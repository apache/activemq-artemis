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
package org.apache.activemq.artemis.jlibaio;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used as an aggregator for the {@link LibaioFile}.
 * <br>
 * It holds native data, and it will share a libaio queue that can be used by multiple files.
 * <br>
 * You need to use the poll methods to read the result of write and read submissions.
 * <br>
 * You also need to use the special buffer created by {@link LibaioFile} as you need special alignments
 * when dealing with O_DIRECT files.
 * <br>
 * A Single controller can server multiple files. There's no need to create one controller per file.
 * <br>
 * <a href="https://ext4.wiki.kernel.org/index.php/Clarifying_Direct_IO's_Semantics">Interesting reading for this.</a>
 */
public class LibaioContext<Callback extends SubmitInfo> implements Closeable {

   private static final AtomicLong totalMaxIO = new AtomicLong(0);

   /**
    * This definition needs to match Version.h on the native sources.
    * <br>
    * Or else the native module won't be loaded because of version mismatches
    */
   private static final int EXPECTED_NATIVE_VERSION = 7;

   private static boolean loaded = false;

   private static final AtomicBoolean shuttingDown = new AtomicBoolean(false);

   private static final AtomicInteger contexts = new AtomicInteger(0);

   public static boolean isLoaded() {
      return loaded;
   }

   private static boolean loadLibrary(final String name) {
      try {
         System.loadLibrary(name);
         if (getNativeVersion() != EXPECTED_NATIVE_VERSION) {
            NativeLogger.LOGGER.incompatibleNativeLibrary();
            return false;
         } else {
            return true;
         }
      } catch (Throwable e) {
         NativeLogger.LOGGER.debug(name + " -> error loading the native library", e);
         return false;
      }

   }

   static {
      String[] libraries = new String[]{"artemis-native-64", "artemis-native-32"};

      for (String library : libraries) {
         if (loadLibrary(library)) {
            loaded = true;
            Runtime.getRuntime().addShutdownHook(new Thread() {
               @Override
               public void run() {
                  shuttingDown.set(true);
                  checkShutdown();
               }
            });
            break;
         } else {
            NativeLogger.LOGGER.debug("Library " + library + " not found!");
         }
      }

      if (!loaded) {
         NativeLogger.LOGGER.debug("Couldn't locate LibAIO Wrapper");
      }
   }

   private static void checkShutdown() {
      if (contexts.get() == 0 && shuttingDown.get()) {
         shutdownHook();
      }
   }

   private static native void shutdownHook();

   /**
    * This is used to validate leaks on tests.
    *
    * @return the number of allocated aio, to be used on test checks.
    */
   public static long getTotalMaxIO() {
      return totalMaxIO.get();
   }

   /**
    * It will reset all the positions on the buffer to 0, using memset.
    *
    * @param buffer a native buffer.
    *               s
    */
   public void memsetBuffer(ByteBuffer buffer) {
      memsetBuffer(buffer, buffer.limit());
   }

   /**
    * This is used on tests validating for leaks.
    */
   public static void resetMaxAIO() {
      totalMaxIO.set(0);
   }

   /**
    * the native ioContext including the structure created.
    */
   private final ByteBuffer ioContext;

   private final AtomicBoolean closed = new AtomicBoolean(false);

   final Semaphore ioSpace;

   final int queueSize;

   final boolean useFdatasync;

   /**
    * The queue size here will use resources defined on the kernel parameter
    * <a href="https://www.kernel.org/doc/Documentation/sysctl/fs.txt">fs.aio-max-nr</a> .
    *
    * @param queueSize    the size to be initialize on libaio
    *                     io_queue_init which can't be higher than /proc/sys/fs/aio-max-nr.
    * @param useSemaphore should block on a semaphore avoiding using more submits than what's available.
    * @param useFdatasync should use fdatasync before calling callbacks.
    */
   public LibaioContext(int queueSize, boolean useSemaphore, boolean useFdatasync) {
      try {
         contexts.incrementAndGet();
         this.ioContext = newContext(queueSize);
         this.useFdatasync = useFdatasync;
      } catch (Exception e) {
         throw e;
      }
      this.queueSize = queueSize;
      totalMaxIO.addAndGet(queueSize);
      if (useSemaphore) {
         this.ioSpace = new Semaphore(queueSize);
      } else {
         this.ioSpace = null;
      }
   }

   /**
    * Documented at {@link LibaioFile#write(long, int, java.nio.ByteBuffer, SubmitInfo)}
    *
    * @param fd          the file descriptor
    * @param position    the write position
    * @param size        number of bytes to use
    * @param bufferWrite the native buffer
    * @param callback    a callback
    * @throws IOException in case of error
    */
   public void submitWrite(int fd,
                           long position,
                           int size,
                           ByteBuffer bufferWrite,
                           Callback callback) throws IOException {
      if (closed.get()) {
         throw new IOException("Libaio Context is closed!");
      }
      try {
         if (ioSpace != null) {
            ioSpace.acquire();
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new IOException(e.getMessage(), e);
      }
      submitWrite(fd, this.ioContext, position, size, bufferWrite, callback);
   }

   public void submitRead(int fd,
                          long position,
                          int size,
                          ByteBuffer bufferWrite,
                          Callback callback) throws IOException {
      if (closed.get()) {
         throw new IOException("Libaio Context is closed!");
      }
      try {
         if (ioSpace != null) {
            ioSpace.acquire();
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new IOException(e.getMessage(), e);
      }
      submitRead(fd, this.ioContext, position, size, bufferWrite, callback);
   }

   /**
    * This is used to close the libaio queues and cleanup the native data used.
    * <br>
    * It is unsafe to close the controller while you have pending writes or files open as
    * this could cause core dumps or VM crashes.
    */
   @Override
   public void close() {
      if (!closed.getAndSet(true)) {

         if (ioSpace != null) {
            try {
               ioSpace.tryAcquire(queueSize, 10, TimeUnit.SECONDS);
            } catch (Exception e) {
               NativeLogger.LOGGER.error(e);
            }
         }
         totalMaxIO.addAndGet(-queueSize);

         if (ioContext != null) {
            deleteContext(ioContext);
         }
         contexts.decrementAndGet();
         checkShutdown();
      }
   }

   @Override
   protected void finalize() throws Throwable {
      super.finalize();
      close();
   }

   /**
    * It will open a file. If you set the direct flag = false then you won't need to use the special buffer.
    * Notice: This will create an empty file if the file doesn't already exist.
    *
    * @param file   the file to be open.
    * @param direct will set ODIRECT.
    * @return It will return a LibaioFile instance.
    * @throws IOException in case of error.
    */
   public LibaioFile<Callback> openFile(File file, boolean direct) throws IOException {
      return openFile(file.getPath(), direct);
   }

   /**
    * It will open a file. If you set the direct flag = false then you won't need to use the special buffer.
    * Notice: This will create an empty file if the file doesn't already exist.
    *
    * @param file   the file to be open.
    * @param direct should use O_DIRECT when opening the file.
    * @return a new open file.
    * @throws IOException in case of error.
    */
   public LibaioFile<Callback> openFile(String file, boolean direct) throws IOException {
      checkNotNull(file, "path");
      checkNotNull(ioContext, "IOContext");

      // note: the native layer will throw an IOException in case of errors
      int res = LibaioContext.open(file, direct);

      return new LibaioFile<>(res, this);
   }

   /**
    * It will open a file disassociated with any sort of factory.
    * This is useful when you won't use reading / writing through libaio like locking files.
    *
    * @param file   a file name
    * @param direct will use O_DIRECT
    * @return a new file
    * @throws IOException in case of error.
    */
   public static LibaioFile openControlFile(String file, boolean direct) throws IOException {
      checkNotNull(file, "path");

      // note: the native layer will throw an IOException in case of errors
      int res = LibaioContext.open(file, direct);

      return new LibaioFile<>(res, null);
   }

   /**
    * Checks that the given argument is not null. If it is, throws {@link NullPointerException}.
    * Otherwise, returns the argument.
    */
   private static <T> T checkNotNull(T arg, String text) {
      if (arg == null) {
         throw new NullPointerException(text);
      }
      return arg;
   }

   /**
    * It will poll the libaio queue for results. It should block until min is reached
    * Results are placed on the callback.
    * <br>
    * This shouldn't be called concurrently. You should provide your own synchronization if you need more than one
    * Thread polling for any reason.
    * <br>
    * Notice that the native layer will invoke {@link SubmitInfo#onError(int, String)} in case of failures,
    * but it won't call done method for you.
    *
    * @param callbacks area to receive the callbacks passed on submission.The size of this callback has to
    *                  be greater than the parameter max.
    * @param min       the minimum number of elements to receive. It will block until this is achieved.
    * @param max       The maximum number of elements to receive.
    * @return Number of callbacks returned.
    * @see LibaioFile#write(long, int, java.nio.ByteBuffer, SubmitInfo)
    * @see LibaioFile#read(long, int, java.nio.ByteBuffer, SubmitInfo)
    */
   public int poll(Callback[] callbacks, int min, int max) {
      int released = poll(ioContext, callbacks, min, max);
      if (ioSpace != null) {
         if (released > 0) {
            ioSpace.release(released);
         }
      }
      return released;
   }

   /**
    * It will start polling and will keep doing until the context is closed.
    * This will call callbacks on {@link SubmitInfo#onError(int, String)} and
    * {@link SubmitInfo#done()}.
    * In case of error, both {@link SubmitInfo#onError(int, String)} and
    * {@link SubmitInfo#done()} are called.
    */
   public void poll() {
      if (!closed.get()) {
         blockedPoll(ioContext, useFdatasync);
      }
   }

   /**
    * Called from the native layer
    */
   private void done(SubmitInfo info) {
      info.done();
      if (ioSpace != null) {
         ioSpace.release();
      }
   }

   /**
    * This is the queue for libaio, initialized with queueSize.
    */
   private native ByteBuffer newContext(int queueSize);

   /**
    * Internal method to be used when closing the controller.
    */
   private native void deleteContext(ByteBuffer buffer);

   /**
    * it will return a file descriptor.
    *
    * @param path   the file name.
    * @param direct translates as O_DIRECT On open
    * @return a fd from open C call.
    */
   public static native int open(String path, boolean direct);

   public static native void close(int fd);

   /**
    */

   /**
    * Buffers for O_DIRECT need to use posix_memalign.
    * <br>
    * Documented at {@link LibaioFile#newBuffer(int)}.
    *
    * @param size      needs to be % alignment
    * @param alignment the alignment used at the dispositive
    * @return a new native buffer used with posix_memalign
    */
   public static native ByteBuffer newAlignedBuffer(int size, int alignment);

   /**
    * This will call posix free to release the inner buffer allocated at {@link #newAlignedBuffer(int, int)}.
    *
    * @param buffer a native buffer allocated with {@link #newAlignedBuffer(int, int)}.
    */
   public static native void freeBuffer(ByteBuffer buffer);

   /**
    * Documented at {@link LibaioFile#write(long, int, java.nio.ByteBuffer, SubmitInfo)}.
    */
   native void submitWrite(int fd,
                           ByteBuffer libaioContext,
                           long position,
                           int size,
                           ByteBuffer bufferWrite,
                           Callback callback) throws IOException;

   /**
    * Documented at {@link LibaioFile#read(long, int, java.nio.ByteBuffer, SubmitInfo)}.
    */
   native void submitRead(int fd,
                          ByteBuffer libaioContext,
                          long position,
                          int size,
                          ByteBuffer bufferWrite,
                          Callback callback) throws IOException;

   /**
    * Note: this shouldn't be done concurrently.
    * This method will block until the min condition is satisfied on the poll.
    * <p/>
    * The callbacks will include the original callback sent at submit (read or write).
    */
   native int poll(ByteBuffer libaioContext, Callback[] callbacks, int min, int max);

   /**
    * This method will block as long as the context is open.
    */
   native void blockedPoll(ByteBuffer libaioContext, boolean useFdatasync);

   static native int getNativeVersion();

   public static native boolean lock(int fd);

   public static native void memsetBuffer(ByteBuffer buffer, int size);

   static native long getSize(int fd);

   static native int getBlockSizeFD(int fd);

   public static int getBlockSize(File path) {
      return getBlockSize(path.getAbsolutePath());
   }

   public static native int getBlockSize(String path);

   static native void fallocate(int fd, long size);

   static native void fill(int fd, long size);

   static native void writeInternal(int fd, long position, long size, ByteBuffer bufferWrite) throws IOException;
}
