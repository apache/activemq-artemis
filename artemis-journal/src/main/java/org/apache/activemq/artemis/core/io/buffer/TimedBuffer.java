/*
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
package org.apache.activemq.artemis.core.io.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.apache.activemq.artemis.utils.critical.CriticalComponentImpl;
import org.jboss.logging.Logger;

public final class TimedBuffer extends CriticalComponentImpl {

   protected static final int CRITICAL_PATHS = 6;
   protected static final int CRITICAL_PATH_FLUSH = 0;
   protected static final int CRITICAL_PATH_STOP = 1;
   protected static final int CRITICAL_PATH_START = 2;
   protected static final int CRITICAL_PATH_CHECK_SIZE = 3;
   protected static final int CRITICAL_PATH_ADD_BYTES = 4;
   protected static final int CRITICAL_PATH_SET_OBSERVER = 5;

   private static final Logger logger = Logger.getLogger(TimedBuffer.class);

   private static final double MAX_TIMEOUT_ERROR_FACTOR = 1.5;

   // Constants -----------------------------------------------------

   // The number of tries on sleep before switching to spin
   private static final int MAX_CHECKS_ON_SLEEP = 20;

   // Attributes ----------------------------------------------------

   private TimedBufferObserver bufferObserver;

   // If the TimedBuffer is idle - i.e. no records are being added, then it's pointless the timer flush thread
   // in spinning and checking the time - and using up CPU in the process - this semaphore is used to
   // prevent that
   private final Semaphore spinLimiter = new Semaphore(1);

   private CheckTimer timerRunnable;

   private final int bufferSize;

   private final ActiveMQBuffer buffer;

   private int bufferLimit = 0;

   private List<IOCallback> callbacks;

   private final int timeout;

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

   // no need to be volatile as every access is synchronized
   private boolean spinning = false;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public TimedBuffer(CriticalAnalyzer analyzer, final int size, final int timeout, final boolean logRates) {
      super(analyzer, CRITICAL_PATHS);
      bufferSize = size;

      this.logRates = logRates;

      if (logRates) {
         logRatesTimer = new Timer(true);
      }
      // Setting the interval for nano-sleeps

      //prefer off heap buffer to allow further humongous allocations and reduce GC overhead
      //NOTE: it is used ByteBuffer::allocateDirect instead of Unpooled::directBuffer, because the latter could allocate
      //direct ByteBuffers with no Cleaner!
      buffer = new ChannelBufferWrapper(Unpooled.wrappedBuffer(ByteBuffer.allocateDirect(size)));

      buffer.clear();

      bufferLimit = 0;

      callbacks = new ArrayList<>();

      this.timeout = timeout;
   }

   public void start() {
      enterCritical(CRITICAL_PATH_START);
      try {
         synchronized (this) {
            if (started) {
               return;
            }

            // Need to start with the spin limiter acquired
            try {
               spinLimiter.acquire();
            } catch (InterruptedException e) {
               throw new ActiveMQInterruptedException(e);
            }

            timerRunnable = new CheckTimer();

            timerThread = new Thread(timerRunnable, "activemq-buffer-timeout");

            timerThread.start();

            if (logRates) {
               logRatesTimerTask = new LogRatesTimerTask();

               logRatesTimer.scheduleAtFixedRate(logRatesTimerTask, 2000, 2000);
            }

            started = true;
         }
      } finally {
         leaveCritical(CRITICAL_PATH_START);
      }
   }

   public void stop() {
      enterCritical(CRITICAL_PATH_STOP);
      Thread localTimer = null;
      try {
         // add critical analyzer here.... <<<<
         synchronized (this) {
            try {
               if (!started) {
                  return;
               }

               flush();

               bufferObserver = null;

               timerRunnable.close();

               spinLimiter.release();

               if (logRates) {
                  logRatesTimerTask.cancel();
               }

               localTimer = timerThread;
               timerThread = null;

            } finally {
               started = false;
            }
         }
         if (localTimer != null) {
            while (localTimer.isAlive()) {
               try {
                  localTimer.join(1000);
                  if (localTimer.isAlive()) {
                     localTimer.interrupt();
                  }
               } catch (InterruptedException e) {
                  throw new ActiveMQInterruptedException(e);
               }
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_STOP);
      }
   }

   public void setObserver(final TimedBufferObserver observer) {
      enterCritical(CRITICAL_PATH_SET_OBSERVER);
      try {
         synchronized (this) {
            if (bufferObserver != null) {
               flush();
            }

            bufferObserver = observer;
         }
      } finally {
         leaveCritical(CRITICAL_PATH_SET_OBSERVER);
      }
   }

   /**
    * Verify if the size fits the buffer
    *
    * @param sizeChecked
    */
   public boolean checkSize(final int sizeChecked) {
      enterCritical(CRITICAL_PATH_CHECK_SIZE);
      try {
         synchronized (this) {
            if (!started) {
               throw new IllegalStateException("TimedBuffer is not started");
            }

            if (sizeChecked > bufferSize) {
               throw new IllegalStateException("Can't write records (size=" + sizeChecked + ") bigger than the bufferSize(" + bufferSize + ") on the journal");
            }

            if (bufferLimit == 0 || buffer.writerIndex() + sizeChecked > bufferLimit) {
               // Either there is not enough space left in the buffer for the sized record
               // Or a flush has just been performed and we need to re-calculate bufferLimit

               flush();

               delayFlush = true;

               final int remainingInFile = bufferObserver.getRemainingBytes();

               if (sizeChecked > remainingInFile) {
                  return false;
               } else {
                  // There is enough space in the file for this size

                  // Need to re-calculate buffer limit

                  bufferLimit = Math.min(remainingInFile, bufferSize);

                  return true;
               }
            } else {
               delayFlush = true;

               return true;
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_CHECK_SIZE);
      }
   }

   public void addBytes(final ActiveMQBuffer bytes, final boolean sync, final IOCallback callback) {
      enterCritical(CRITICAL_PATH_ADD_BYTES);
      try {
         synchronized (this) {
            if (!started) {
               throw new IllegalStateException("TimedBuffer is not started");
            }

            delayFlush = false;

            //it doesn't modify the reader index of bytes as in the original version
            final int readableBytes = bytes.readableBytes();
            final int writerIndex = buffer.writerIndex();
            buffer.setBytes(writerIndex, bytes, bytes.readerIndex(), readableBytes);
            buffer.writerIndex(writerIndex + readableBytes);

            callbacks.add(callback);

            if (sync) {
               pendingSync = true;

               startSpin();
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_ADD_BYTES);
      }
   }

   public void addBytes(final EncodingSupport bytes, final boolean sync, final IOCallback callback) {
      enterCritical(CRITICAL_PATH_ADD_BYTES);
      try {
         synchronized (this) {
            if (!started) {
               throw new IllegalStateException("TimedBuffer is not started");
            }

            delayFlush = false;

            bytes.encode(buffer);

            callbacks.add(callback);

            if (sync) {
               pendingSync = true;

               startSpin();
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_ADD_BYTES);
      }

   }

   public void flush() {
      flushBatch();
   }

   /**
    * Attempts to flush if {@code !delayFlush} and {@code buffer} is filled by any data.
    *
    * @return {@code true} when are flushed any bytes, {@code false} otherwise
    */
   public boolean flushBatch() {
      enterCritical(CRITICAL_PATH_FLUSH);
      try {
         synchronized (this) {
            if (!started) {
               throw new IllegalStateException("TimedBuffer is not started");
            }

            if (!delayFlush && buffer.writerIndex() > 0) {
               int pos = buffer.writerIndex();

               if (logRates) {
                  bytesFlushed.addAndGet(pos);
               }

               bufferObserver.flushBuffer(buffer.byteBuf(), pendingSync, callbacks);

               stopSpin();

               pendingSync = false;

               // swap the instance as the previous callback list is being used asynchronously
               callbacks = new ArrayList<>();

               buffer.clear();

               bufferLimit = 0;

               flushesDone.incrementAndGet();

               return pos > 0;
            } else {
               return false;
            }
         }
      } finally {
         leaveCritical(CRITICAL_PATH_FLUSH);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class LogRatesTimerTask extends TimerTask {

      private boolean closed;

      private long lastExecution;

      private long lastBytesFlushed;

      private long lastFlushesDone;

      @Override
      public synchronized void run() {
         if (!closed) {
            long now = System.currentTimeMillis();

            long bytesF = bytesFlushed.get();
            long flushesD = flushesDone.get();

            if (lastExecution != 0) {
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
      public synchronized boolean cancel() {
         closed = true;

         return super.cancel();
      }
   }

   private class CheckTimer implements Runnable {

      private volatile boolean closed = false;

      int checks = 0;
      int failedChecks = 0;

      @Override
      public void run() {
         long lastFlushTime = System.nanoTime();
         boolean useSleep = true;

         while (!closed) {
            // We flush on the timer if there are pending syncs there and we've waited at least one
            // timeout since the time of the last flush.
            // Effectively flushing "resets" the timer
            // On the timeout verification, notice that we ignore the timeout check if we are using sleep

            if (pendingSync) {
               if (useSleep) {
                  // if using sleep, we will always flush
                  lastFlushTime = System.nanoTime();
                  if (flushBatch()) {
                     //it could wait until the timeout is expired
                     final long timeFromTheLastFlush = System.nanoTime() - lastFlushTime;

                     // example: Say the device took 20% of the time to write..
                     //          We only need to wait 80% more..
                     //          timeFromTheLastFlush would be the difference
                     //          And if the device took more than that time, there's no need to wait at all.
                     final long timeToSleep = timeout - timeFromTheLastFlush;
                     if (timeToSleep > 0) {
                        useSleep = sleepIfPossible(timeToSleep);
                     }
                  }
               } else if (bufferObserver != null && System.nanoTime() - lastFlushTime > timeout) {
                  lastFlushTime = System.nanoTime();
                  // if not using flush we will spin and do the time checks manually
                  flush();
               }
            }

            try {
               spinLimiter.acquire();

               Thread.yield();

               spinLimiter.release();
            } catch (InterruptedException e) {
               throw new ActiveMQInterruptedException(e);
            }
         }
      }

      /**
       * We will attempt to use sleep only if the system supports nano-sleep.
       * we will on that case verify up to MAX_CHECKS if nano sleep is behaving well.
       * if more than 50% of the checks have failed we will cancel the sleep and just use regular spin
       */
      private boolean sleepIfPossible(long nanosToSleep) {
         boolean useSleep = true;
         try {
            final long startSleep = System.nanoTime();
            sleep(nanosToSleep);
            if (checks < MAX_CHECKS_ON_SLEEP) {
               final long elapsedSleep = System.nanoTime() - startSleep;
               // I'm letting the real time to be up to 50% than the requested sleep.
               if (elapsedSleep > (nanosToSleep * MAX_TIMEOUT_ERROR_FACTOR)) {
                  failedChecks++;
               }

               if (++checks >= MAX_CHECKS_ON_SLEEP) {
                  if (failedChecks > MAX_CHECKS_ON_SLEEP * 0.5) {
                     logger.debug("LockSupport.parkNanos with nano seconds is not working as expected, Your kernel possibly doesn't support real time. the Journal TimedBuffer will spin for timeouts");
                     useSleep = false;
                  }
               }
            }
         } catch (Exception e) {
            useSleep = false;
            // I don't think we need to individualize a logger code here, this is unlikely to happen anyways
            logger.warn(e.getMessage() + ", disabling sleep on TimedBuffer, using spin now", e);
         }
         return useSleep;
      }

      public void close() {
         closed = true;
      }
   }

   /**
    * Sub classes (tests basically) can use this to override how the sleep is being done
    *
    * @param sleepNanos
    */
   protected void sleep(long sleepNanos) {
      LockSupport.parkNanos(sleepNanos);
   }

   /**
    * Sub classes (tests basically) can use this to override disabling spinning
    */
   protected void stopSpin() {
      if (spinning) {
         try {
            // We acquire the spinLimiter semaphore - this prevents the timer flush thread unnecessarily spinning
            // when the buffer is inactive
            spinLimiter.acquire();
         } catch (InterruptedException e) {
            throw new ActiveMQInterruptedException(e);
         }

         spinning = false;
      }
   }

   /**
    * Sub classes (tests basically) can use this to override disabling spinning
    */
   protected void startSpin() {
      if (!spinning) {
         spinLimiter.release();

         spinning = true;
      }
   }

}