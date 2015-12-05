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
package org.apache.activemq.artemis.core.io.aio;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.core.io.AbstractSequentialFileFactory;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.jlibaio.LibaioFile;
import org.apache.activemq.artemis.jlibaio.SubmitInfo;
import org.apache.activemq.artemis.jlibaio.util.CallbackCache;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;

public final class AIOSequentialFileFactory extends AbstractSequentialFileFactory {

   private static final boolean trace = ActiveMQJournalLogger.LOGGER.isTraceEnabled();

   private final ReuseBuffersController buffersControl = new ReuseBuffersController();

   private volatile boolean reuseBuffers = true;

   private ExecutorService pollerExecutor;

   volatile LibaioContext<AIOSequentialCallback> libaioContext;

   private final CallbackCache<AIOSequentialCallback> callbackPool;

   private final AtomicBoolean running = new AtomicBoolean(false);

   // This method exists just to make debug easier.
   // I could replace log.trace by log.info temporarily while I was debugging
   // Journal
   private static void trace(final String message) {
      ActiveMQJournalLogger.LOGGER.trace(message);
   }

   public AIOSequentialFileFactory(final File journalDir, int maxIO) {
      this(journalDir, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, maxIO, false, null);
   }

   public AIOSequentialFileFactory(final File journalDir, final IOCriticalErrorListener listener, int maxIO) {
      this(journalDir, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, maxIO, false, listener);
   }

   public AIOSequentialFileFactory(final File journalDir,
                                   final int bufferSize,
                                   final int bufferTimeout,
                                   final int maxIO,
                                   final boolean logRates) {
      this(journalDir, bufferSize, bufferTimeout, maxIO, logRates, null);
   }

   public AIOSequentialFileFactory(final File journalDir,
                                   final int bufferSize,
                                   final int bufferTimeout,
                                   final int maxIO,
                                   final boolean logRates,
                                   final IOCriticalErrorListener listener) {
      super(journalDir, true, bufferSize, bufferTimeout, maxIO, logRates, listener);
      callbackPool = new CallbackCache<>(maxIO);
   }

   public AIOSequentialCallback getCallback() {
      AIOSequentialCallback callback = callbackPool.get();
      if (callback == null) {
         callback = new AIOSequentialCallback();
      }

      return callback;
   }

   public void enableBufferReuse() {
      this.reuseBuffers = true;
   }

   public void disableBufferReuse() {
      this.reuseBuffers = false;
   }

   @Override
   public SequentialFile createSequentialFile(final String fileName) {
      return new AIOSequentialFile(this, bufferSize, bufferTimeout, journalDir, fileName, writeExecutor);
   }

   @Override
   public boolean isSupportsCallbacks() {
      return true;
   }

   public static boolean isSupported() {
      return LibaioContext.isLoaded();
   }

   @Override
   public ByteBuffer allocateDirectBuffer(final int size) {

      int blocks = size / 512;
      if (size % 512 != 0) {
         blocks++;
      }

      // The buffer on AIO has to be a multiple of 512
      ByteBuffer buffer = LibaioContext.newAlignedBuffer(blocks * 512, 512);

      buffer.limit(size);

      return buffer;
   }

   @Override
   public void releaseDirectBuffer(final ByteBuffer buffer) {
      LibaioContext.freeBuffer(buffer);
   }

   @Override
   public ByteBuffer newBuffer(int size) {
      if (size % 512 != 0) {
         size = (size / 512 + 1) * 512;
      }

      return buffersControl.newBuffer(size);
   }

   @Override
   public void clearBuffer(final ByteBuffer directByteBuffer) {
      directByteBuffer.position(0);
      libaioContext.memsetBuffer(directByteBuffer);
   }

   @Override
   public int getAlignment() {
      return 512;
   }

   // For tests only
   @Override
   public ByteBuffer wrapBuffer(final byte[] bytes) {
      ByteBuffer newbuffer = newBuffer(bytes.length);
      newbuffer.put(bytes);
      return newbuffer;
   }

   @Override
   public int calculateBlockSize(final int position) {
      int alignment = getAlignment();

      int pos = (position / alignment + (position % alignment != 0 ? 1 : 0)) * alignment;

      return pos;
   }

   /* (non-Javadoc)
    * @see org.apache.activemq.artemis.core.io.SequentialFileFactory#releaseBuffer(java.nio.ByteBuffer)
    */
   @Override
   public synchronized void releaseBuffer(final ByteBuffer buffer) {
      LibaioContext.freeBuffer(buffer);
   }

   @Override
   public void start() {
      if (running.compareAndSet(false, true)) {
         super.start();

         this.libaioContext = new LibaioContext(maxIO, true);

         this.running.set(true);

         pollerExecutor = Executors.newCachedThreadPool(AccessController.doPrivileged(new PrivilegedAction<ActiveMQThreadFactory>() {
            @Override
            public ActiveMQThreadFactory run() {
               return new ActiveMQThreadFactory("ActiveMQ-AIO-poller-pool" + System.identityHashCode(this), true, AIOSequentialFileFactory.class.getClassLoader());
            }
         }));

         pollerExecutor.execute(new PollerRunnable());
      }

   }

   @Override
   public void stop() {
      if (this.running.compareAndSet(true, false)) {
         buffersControl.stop();

         libaioContext.close();
         libaioContext = null;

         if (pollerExecutor != null) {
            pollerExecutor.shutdown();

            try {
               if (!pollerExecutor.awaitTermination(AbstractSequentialFileFactory.EXECUTOR_TIMEOUT, TimeUnit.SECONDS)) {
                  ActiveMQJournalLogger.LOGGER.timeoutOnPollerShutdown(new Exception("trace"));
               }
            }
            catch (InterruptedException e) {
               throw new ActiveMQInterruptedException(e);
            }
         }

         super.stop();
      }
   }

   @Override
   protected void finalize() {
      stop();
   }

   /**
    * The same callback is used for Runnable executor.
    * This way we can save some memory over the pool.
    */
   public class AIOSequentialCallback implements SubmitInfo, Runnable, Comparable<AIOSequentialCallback> {

      IOCallback callback;
      boolean error = false;
      AIOSequentialFile sequentialFile;
      ByteBuffer buffer;
      LibaioFile libaioFile;
      String errorMessage;
      int errorCode = -1;
      long writeSequence;

      long position;
      int bytes;

      @Override
      public String toString() {
         return "AIOSequentialCallback{" +
            "error=" + error +
            ", errorMessage='" + errorMessage + '\'' +
            ", errorCode=" + errorCode +
            ", writeSequence=" + writeSequence +
            ", position=" + position +
            '}';
      }

      public AIOSequentialCallback initWrite(long positionToWrite, int bytesToWrite) {
         this.position = positionToWrite;
         this.bytes = bytesToWrite;
         return this;
      }

      @Override
      public void run() {
         try {
            libaioFile.write(position, bytes, buffer, this);
         }
         catch (IOException e) {
            callback.onError(-1, e.getMessage());
         }
      }

      @Override
      public int compareTo(AIOSequentialCallback other) {
         if (this == other || this.writeSequence == other.writeSequence) {
            return 0;
         }
         else if (other.writeSequence < this.writeSequence) {
            return 1;
         }
         else {
            return -1;
         }
      }

      public AIOSequentialCallback init(long writeSequence,
                                        IOCallback IOCallback,
                                        LibaioFile libaioFile,
                                        AIOSequentialFile sequentialFile,
                                        ByteBuffer usedBuffer) {
         this.callback = IOCallback;
         this.sequentialFile = sequentialFile;
         this.error = false;
         this.buffer = usedBuffer;
         this.libaioFile = libaioFile;
         this.writeSequence = writeSequence;
         this.errorMessage = null;
         return this;
      }

      @Override
      public void onError(int errno, String message) {
         this.error = true;
         this.errorCode = errno;
         this.errorMessage = message;
      }

      /**
       * this is called by libaio.
       */
      @Override
      public void done() {
         this.sequentialFile.done(this);
      }

      /**
       * This is callbed by the AIOSequentialFile, after determined the callbacks were returned in sequence
       */
      public void sequentialDone() {

         if (error) {
            callback.onError(errorCode, errorMessage);
            errorMessage = null;
         }
         else {
            if (callback != null) {
               callback.done();
            }

            if (buffer != null && reuseBuffers) {
               buffersControl.bufferDone(buffer);
            }

            callbackPool.put(AIOSequentialCallback.this);
         }
      }
   }

   private class PollerRunnable implements Runnable {

      @Override
      public void run() {
         libaioContext.poll();
      }
   }

   /**
    * Class that will control buffer-reuse
    */
   private class ReuseBuffersController {

      private volatile long bufferReuseLastTime = System.currentTimeMillis();

      private final ConcurrentLinkedQueue<ByteBuffer> reuseBuffersQueue = new ConcurrentLinkedQueue<>();

      private boolean stopped = false;

      public ByteBuffer newBuffer(final int size) {
         // if a new buffer wasn't requested in 10 seconds, we clear the queue
         // This is being done this way as we don't need another Timeout Thread
         // just to cleanup this
         if (bufferSize > 0 && System.currentTimeMillis() - bufferReuseLastTime > 10000) {
            if (AIOSequentialFileFactory.trace) {
               AIOSequentialFileFactory.trace("Clearing reuse buffers queue with " + reuseBuffersQueue.size() +
                                                 " elements");
            }

            bufferReuseLastTime = System.currentTimeMillis();

            clearPoll();
         }

         // if a buffer is bigger than the configured-bufferSize, we just create a new
         // buffer.
         if (size > bufferSize) {
            return LibaioContext.newAlignedBuffer(size, 512);
         }
         else {
            // We need to allocate buffers following the rules of the storage
            // being used (AIO/NIO)
            int alignedSize = calculateBlockSize(size);

            // Try getting a buffer from the queue...
            ByteBuffer buffer = reuseBuffersQueue.poll();

            if (buffer == null) {
               // if empty create a new one.
               buffer = LibaioContext.newAlignedBuffer(size, 512);

               buffer.limit(alignedSize);
            }
            else {
               clearBuffer(buffer);

               // set the limit of the buffer to the bufferSize being required
               buffer.limit(alignedSize);
            }

            buffer.rewind();

            return buffer;
         }
      }

      public synchronized void stop() {
         stopped = true;
         clearPoll();
      }

      public synchronized void clearPoll() {
         ByteBuffer reusedBuffer;

         while ((reusedBuffer = reuseBuffersQueue.poll()) != null) {
            releaseBuffer(reusedBuffer);
         }
      }

      public void bufferDone(final ByteBuffer buffer) {
         synchronized (this) {

            if (stopped) {
               releaseBuffer(buffer);
            }
            else {
               bufferReuseLastTime = System.currentTimeMillis();

               // If a buffer has any other than the configured bufferSize, the buffer
               // will be just sent to GC
               if (buffer.capacity() == bufferSize) {
                  reuseBuffersQueue.offer(buffer);
               }
               else {
                  releaseBuffer(buffer);
               }
            }
         }
      }
   }

   @Override
   public String toString() {
      return AIOSequentialFileFactory.class.getSimpleName() + "(buffersControl.stopped=" + buffersControl.stopped +
         "):" + super.toString();
   }
}
