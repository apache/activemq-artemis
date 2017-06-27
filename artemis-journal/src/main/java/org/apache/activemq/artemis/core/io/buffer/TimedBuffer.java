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
import java.util.Collections;
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

public final class TimedBuffer {
   // Constants -----------------------------------------------------

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

   private List<IOCallback> callbacks;

   private final int timeout;

   private final AtomicLong pendingSyncs = new AtomicLong();

   private Thread timerThread;

   private volatile boolean started;

   // We use this flag to prevent flush occurring between calling checkSize and addBytes
   // CheckSize must always be followed by it's corresponding addBytes otherwise the buffer
   // can get in an inconsistent state
   private boolean delayFlush;

   // for logging write rates

   private final boolean logRates;

   private long bytesFlushed = 0;

   private final AtomicLong flushesDone = new AtomicLong(0);

   private Timer logRatesTimer;

   private TimerTask logRatesTimerTask;

   // no need to be volatile as every access is synchronized
   private boolean spinning = false;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public TimedBuffer(final int size, final int timeout, final boolean logRates) {
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

      callbacks = null;

      this.timeout = timeout;
   }

   public synchronized void start() {
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

   public void stop() {
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

      while (timerThread.isAlive()) {
         try {
            timerThread.join();
         } catch (InterruptedException e) {
            throw new ActiveMQInterruptedException(e);
         }
      }

      started = false;
   }

   public synchronized void setObserver(final TimedBufferObserver observer) {
      if (bufferObserver != null) {
         flush();
      }

      bufferObserver = observer;
   }

   /**
    * Verify if the size fits the buffer
    *
    * @param sizeChecked
    */
   public synchronized boolean checkSize(final int sizeChecked) {
      if (!started) {
         throw new IllegalStateException("TimedBuffer is not started");
      }

      if (sizeChecked > bufferSize) {
         throw new IllegalStateException("Can't write records bigger than the bufferSize(" + bufferSize +
                                            ") on the journal");
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

   public synchronized void addBytes(final ActiveMQBuffer bytes, final boolean sync, final IOCallback callback) {
      if (!started) {
         throw new IllegalStateException("TimedBuffer is not started");
      }

      delayFlush = false;

      //it doesn't modify the reader index of bytes as in the original version
      final int readableBytes = bytes.readableBytes();
      final int writerIndex = buffer.writerIndex();
      buffer.setBytes(writerIndex, bytes, bytes.readerIndex(), readableBytes);
      buffer.writerIndex(writerIndex + readableBytes);

      if (callbacks == null) {
         callbacks = new ArrayList<>();
      }
      callbacks.add(callback);

      if (sync) {
         final long currentPendingSyncs = pendingSyncs.get();
         pendingSyncs.lazySet(currentPendingSyncs + 1);
         startSpin();
      }
   }

   public synchronized void addBytes(final EncodingSupport bytes, final boolean sync, final IOCallback callback) {
      if (!started) {
         throw new IllegalStateException("TimedBuffer is not started");
      }

      delayFlush = false;

      bytes.encode(buffer);

      if (callbacks == null) {
         callbacks = new ArrayList<>();
      }
      callbacks.add(callback);

      if (sync) {
         final long currentPendingSyncs = pendingSyncs.get();
         pendingSyncs.lazySet(currentPendingSyncs + 1);
         startSpin();
      }

   }

   public void flush() {
      flush(false);
   }

   /**
    * force means the Journal is moving to a new file. Any pending write need to be done immediately
    * or data could be lost
    */
   private void flush(final boolean force) {
      synchronized (this) {
         if (!started) {
            throw new IllegalStateException("TimedBuffer is not started");
         }

         if ((force || !delayFlush) && buffer.writerIndex() > 0) {
            final int pos = buffer.writerIndex();

            final ByteBuffer bufferToFlush = bufferObserver.newBuffer(bufferSize, pos);
            //bufferObserver::newBuffer doesn't necessary return a buffer with limit == pos or limit == bufferSize!!
            bufferToFlush.limit(pos);
            //perform memcpy under the hood due to the off heap buffer
            buffer.getBytes(0, bufferToFlush);

            final List<IOCallback> ioCallbacks = callbacks == null ? Collections.emptyList() : callbacks;
            bufferObserver.flushBuffer(bufferToFlush, pendingSyncs.get() > 0, ioCallbacks);

            stopSpin();

            pendingSyncs.lazySet(0);

            callbacks = null;

            buffer.clear();

            bufferLimit = 0;

            if (logRates) {
               logFlushed(pos);
            }
         }
      }
   }

   private void logFlushed(int bytes) {
      this.bytesFlushed += bytes;
      //more lightweight than XADD if single writer
      final long currentFlushesDone = flushesDone.get();
      //flushesDone::lazySet write-Release bytesFlushed
      flushesDone.lazySet(currentFlushesDone + 1L);
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

            final long flushesDone = TimedBuffer.this.flushesDone.get();
            //flushesDone::get read-Acquire bytesFlushed
            final long bytesFlushed = TimedBuffer.this.bytesFlushed;
            if (lastExecution != 0) {
               final double rate = 1000 * (double) (bytesFlushed - lastBytesFlushed) / (now - lastExecution);
               ActiveMQJournalLogger.LOGGER.writeRate(rate, (long) (rate / (1024 * 1024)));
               final double flushRate = 1000 * (double) (flushesDone - lastFlushesDone) / (now - lastExecution);
               ActiveMQJournalLogger.LOGGER.flushRate(flushRate);
            }

            lastExecution = now;

            lastBytesFlushed = bytesFlushed;

            lastFlushesDone = flushesDone;
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

      @Override
      public void run() {
         int waitTimes = 0;
         long lastFlushTime = 0;
         long estimatedOptimalBatch = Runtime.getRuntime().availableProcessors();
         final Semaphore spinLimiter = TimedBuffer.this.spinLimiter;
         final long timeout = TimedBuffer.this.timeout;

         while (!closed) {
            boolean flushed = false;
            final long currentPendingSyncs = pendingSyncs.get();

            if (currentPendingSyncs > 0) {
               if (bufferObserver != null) {
                  final boolean checkpoint = System.nanoTime() > lastFlushTime + timeout;
                  if (checkpoint || currentPendingSyncs >= estimatedOptimalBatch) {
                     flush();
                     if (checkpoint) {
                        estimatedOptimalBatch = currentPendingSyncs;
                     } else {
                        estimatedOptimalBatch = Math.max(estimatedOptimalBatch, currentPendingSyncs);
                     }
                     lastFlushTime = System.nanoTime();
                     //a flush has been requested
                     flushed = true;
                  }
               }
            }

            if (flushed) {
               waitTimes = 0;
            } else {
               //instead of interruptible sleeping, perform progressive parks depending on the load
               waitTimes = TimedBuffer.wait(waitTimes, spinLimiter);
            }
         }
      }

      public void close() {
         closed = true;
      }
   }

   private static int wait(int waitTimes, Semaphore spinLimiter) {
      if (waitTimes < 10) {
         //doesn't make sense to spin loop here, because of the lock around flush/addBytes operations!
         Thread.yield();
         waitTimes++;
      } else if (waitTimes < 20) {
         LockSupport.parkNanos(1L);
         waitTimes++;
      } else if (waitTimes < 50) {
         LockSupport.parkNanos(10L);
         waitTimes++;
      } else if (waitTimes < 100) {
         LockSupport.parkNanos(100L);
         waitTimes++;
      } else if (waitTimes < 1000) {
         LockSupport.parkNanos(1000L);
         waitTimes++;
      } else {
         LockSupport.parkNanos(100_000L);
         try {
            spinLimiter.acquire();
            spinLimiter.release();
         } catch (InterruptedException e) {
            throw new ActiveMQInterruptedException(e);
         }
      }
      return waitTimes;
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
