/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQBuffers;
import org.apache.activemq.api.core.ActiveMQInterruptedException;
import org.apache.activemq.core.journal.EncodingSupport;
import org.apache.activemq.core.journal.IOAsyncTask;
import org.apache.activemq.core.journal.impl.dataformat.ByteArrayEncoding;
import org.apache.activemq.journal.ActiveMQJournalLogger;

public class TimedBuffer
{
   // Constants -----------------------------------------------------

   // The number of tries on sleep before switching to spin
   public static final int MAX_CHECKS_ON_SLEEP = 20;

   // Attributes ----------------------------------------------------

   private TimedBufferObserver bufferObserver;

   // If the TimedBuffer is idle - i.e. no records are being added, then it's pointless the timer flush thread
   // in spinning and checking the time - and using up CPU in the process - this semaphore is used to
   // prevent that
   private final Semaphore spinLimiter = new Semaphore(1);

   private CheckTimer timerRunnable = new CheckTimer();

   private final int bufferSize;

   private final ActiveMQBuffer buffer;

   private int bufferLimit = 0;

   private List<IOAsyncTask> callbacks;

   private volatile int timeout;

   // used to measure sync requests. When a sync is requested, it shouldn't take more than timeout to happen
   private volatile boolean pendingSync = false;

   private Thread timerThread;

   private volatile boolean started;

   // We use this flag to prevent flush occurring between calling checkSize and addBytes
   // CheckSize must always be followed by it's corresponding addBytes otherwise the buffer
   // can get in an inconsistent state
   private boolean delayFlush;

   // for logging write rates

   private final boolean logRates;

   private final AtomicLong bytesFlushed = new AtomicLong(0);

   private final AtomicLong flushesDone = new AtomicLong(0);

   private Timer logRatesTimer;

   private TimerTask logRatesTimerTask;

   private boolean useSleep = true;

   // no need to be volatile as every access is synchronized
   private boolean spinning = false;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public TimedBuffer(final int size, final int timeout, final boolean logRates)
   {
      bufferSize = size;

      this.logRates = logRates;

      if (logRates)
      {
         logRatesTimer = new Timer(true);
      }
      // Setting the interval for nano-sleeps

      buffer = ActiveMQBuffers.fixedBuffer(bufferSize);

      buffer.clear();

      bufferLimit = 0;

      callbacks = new ArrayList<IOAsyncTask>();

      this.timeout = timeout;
   }

   // for Debug purposes
   public synchronized boolean isUseSleep()
   {
      return useSleep;
   }

   public synchronized void setUseSleep(boolean useSleep)
   {
      this.useSleep = useSleep;
   }

   public synchronized void start()
   {
      if (started)
      {
         return;
      }

      // Need to start with the spin limiter acquired
      try
      {
         spinLimiter.acquire();
      }
      catch (InterruptedException e)
      {
         throw new ActiveMQInterruptedException(e);
      }

      timerRunnable = new CheckTimer();

      timerThread = new Thread(timerRunnable, "activemq-buffer-timeout");

      timerThread.start();

      if (logRates)
      {
         logRatesTimerTask = new LogRatesTimerTask();

         logRatesTimer.scheduleAtFixedRate(logRatesTimerTask, 2000, 2000);
      }

      started = true;
   }

   public void stop()
   {
      if (!started)
      {
         return;
      }

      flush();

      bufferObserver = null;

      timerRunnable.close();

      spinLimiter.release();

      if (logRates)
      {
         logRatesTimerTask.cancel();
      }

      while (timerThread.isAlive())
      {
         try
         {
            timerThread.join();
         }
         catch (InterruptedException e)
         {
            throw new ActiveMQInterruptedException(e);
         }
      }

      started = false;
   }

   public synchronized void setObserver(final TimedBufferObserver observer)
   {
      if (bufferObserver != null)
      {
         flush();
      }

      bufferObserver = observer;
   }

   /**
    * Verify if the size fits the buffer
    *
    * @param sizeChecked
    */
   public synchronized boolean checkSize(final int sizeChecked)
   {
      if (!started)
      {
         throw new IllegalStateException("TimedBuffer is not started");
      }

      if (sizeChecked > bufferSize)
      {
         throw new IllegalStateException("Can't write records bigger than the bufferSize(" + bufferSize +
                                            ") on the journal");
      }

      if (bufferLimit == 0 || buffer.writerIndex() + sizeChecked > bufferLimit)
      {
         // Either there is not enough space left in the buffer for the sized record
         // Or a flush has just been performed and we need to re-calcualate bufferLimit

         flush();

         delayFlush = true;

         final int remainingInFile = bufferObserver.getRemainingBytes();

         if (sizeChecked > remainingInFile)
         {
            return false;
         }
         else
         {
            // There is enough space in the file for this size

            // Need to re-calculate buffer limit

            bufferLimit = Math.min(remainingInFile, bufferSize);

            return true;
         }
      }
      else
      {
         delayFlush = true;

         return true;
      }
   }

   public synchronized void addBytes(final ActiveMQBuffer bytes, final boolean sync, final IOAsyncTask callback)
   {
      addBytes(new ByteArrayEncoding(bytes.toByteBuffer().array()), sync, callback);
   }

   public synchronized void addBytes(final EncodingSupport bytes, final boolean sync, final IOAsyncTask callback)
   {
      if (!started)
      {
         throw new IllegalStateException("TimedBuffer is not started");
      }

      delayFlush = false;

      bytes.encode(buffer);

      callbacks.add(callback);

      if (sync)
      {
         pendingSync = true;

         startSpin();
      }

   }

   public void flush()
   {
      flush(false);
   }

   /**
    * force means the Journal is moving to a new file. Any pending write need to be done immediately
    * or data could be lost
    */
   public void flush(final boolean force)
   {
      synchronized (this)
      {
         if (!started)
         {
            throw new IllegalStateException("TimedBuffer is not started");
         }

         if ((force || !delayFlush) && buffer.writerIndex() > 0)
         {
            int pos = buffer.writerIndex();

            if (logRates)
            {
               bytesFlushed.addAndGet(pos);
            }

            ByteBuffer bufferToFlush = bufferObserver.newBuffer(bufferSize, pos);

            // Putting a byteArray on a native buffer is much faster, since it will do in a single native call.
            // Using bufferToFlush.put(buffer) would make several append calls for each byte
            // We also transfer the content of this buffer to the native file's buffer

            bufferToFlush.put(buffer.toByteBuffer().array(), 0, pos);

            bufferObserver.flushBuffer(bufferToFlush, pendingSync, callbacks);

            stopSpin();

            pendingSync = false;

            // swap the instance as the previous callback list is being used asynchronously
            callbacks = new LinkedList<IOAsyncTask>();

            buffer.clear();

            bufferLimit = 0;

            flushesDone.incrementAndGet();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class LogRatesTimerTask extends TimerTask
   {
      private boolean closed;

      private long lastExecution;

      private long lastBytesFlushed;

      private long lastFlushesDone;

      @Override
      public synchronized void run()
      {
         if (!closed)
         {
            long now = System.currentTimeMillis();

            long bytesF = bytesFlushed.get();
            long flushesD = flushesDone.get();

            if (lastExecution != 0)
            {
               double rate = 1000 * (double) (bytesF - lastBytesFlushed) / (now - lastExecution);
               ActiveMQJournalLogger.LOGGER.writeRate(rate, (long) (rate / (1024 * 1024)));
               double flushRate = 1000 * (double) (flushesD - lastFlushesDone) / (now - lastExecution);
               ActiveMQJournalLogger.LOGGER.flushRate(flushRate);
            }

            lastExecution = now;

            lastBytesFlushed = bytesF;

            lastFlushesDone = flushesD;
         }
      }

      @Override
      public synchronized boolean cancel()
      {
         closed = true;

         return super.cancel();
      }
   }

   private class CheckTimer implements Runnable
   {
      private volatile boolean closed = false;

      int checks = 0;
      int failedChecks = 0;
      long timeBefore = 0;

      final int sleepMillis = timeout / 1000000; // truncates
      final int sleepNanos = timeout % 1000000;


      public void run()
      {
         long lastFlushTime = 0;

         while (!closed)
         {
            // We flush on the timer if there are pending syncs there and we've waited at least one
            // timeout since the time of the last flush.
            // Effectively flushing "resets" the timer
            // On the timeout verification, notice that we ignore the timeout check if we are using sleep

            if (pendingSync)
            {
               if (isUseSleep())
               {
                  // if using sleep, we will always flush
                  flush();
                  lastFlushTime = System.nanoTime();
               }
               else if (bufferObserver != null && System.nanoTime() > lastFlushTime + timeout)
               {
                  // if not using flush we will spin and do the time checks manually
                  flush();
                  lastFlushTime = System.nanoTime();
               }

            }

            sleepIfPossible();

            try
            {
               spinLimiter.acquire();

               Thread.yield();

               spinLimiter.release();
            }
            catch (InterruptedException e)
            {
               throw new ActiveMQInterruptedException(e);
            }
         }
      }

      /**
       * We will attempt to use sleep only if the system supports nano-sleep
       * we will on that case verify up to MAX_CHECKS if nano sleep is behaving well.
       * if more than 50% of the checks have failed we will cancel the sleep and just use regular spin
       */
      private void sleepIfPossible()
      {
         if (isUseSleep())
         {
            if (checks < MAX_CHECKS_ON_SLEEP)
            {
               timeBefore = System.nanoTime();
            }

            try
            {
               sleep(sleepMillis, sleepNanos);
            }
            catch (InterruptedException e)
            {
               throw new ActiveMQInterruptedException(e);
            }
            catch (Exception e)
            {
               setUseSleep(false);
               ActiveMQJournalLogger.LOGGER.warn(e.getMessage() + ", disabling sleep on TimedBuffer, using spin now", e);
            }

            if (checks < MAX_CHECKS_ON_SLEEP)
            {
               long realTimeSleep = System.nanoTime() - timeBefore;

               // I'm letting the real time to be up to 50% than the requested sleep.
               if (realTimeSleep > timeout * 1.5)
               {
                  failedChecks++;
               }

               if (++checks >= MAX_CHECKS_ON_SLEEP)
               {
                  if (failedChecks > MAX_CHECKS_ON_SLEEP * 0.5)
                  {
                     ActiveMQJournalLogger.LOGGER.debug("Thread.sleep with nano seconds is not working as expected, Your kernel possibly doesn't support real time. the Journal TimedBuffer will spin for timeouts");
                     setUseSleep(false);
                  }
               }
            }
         }
      }

      public void close()
      {
         closed = true;
      }
   }

   /**
    * Sub classes (tests basically) can use this to override how the sleep is being done
    *
    * @param sleepMillis
    * @param sleepNanos
    * @throws InterruptedException
    */
   protected void sleep(int sleepMillis, int sleepNanos) throws InterruptedException
   {
      Thread.sleep(sleepMillis, sleepNanos);
   }

   /**
    * Sub classes (tests basically) can use this to override disabling spinning
    */
   protected void stopSpin()
   {
      if (spinning)
      {
         try
         {
            // We acquire the spinLimiter semaphore - this prevents the timer flush thread unnecessarily spinning
            // when the buffer is inactive
            spinLimiter.acquire();
         }
         catch (InterruptedException e)
         {
            throw new ActiveMQInterruptedException(e);
         }

         spinning = false;
      }
   }


   /**
    * Sub classes (tests basically) can use this to override disabling spinning
    */
   protected void startSpin()
   {
      if (!spinning)
      {
         spinLimiter.release();

         spinning = true;
      }
   }


}
