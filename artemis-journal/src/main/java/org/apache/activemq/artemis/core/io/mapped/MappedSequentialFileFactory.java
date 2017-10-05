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
package org.apache.activemq.artemis.core.io.mapped;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.buffer.TimedBuffer;
import org.apache.activemq.artemis.utils.Env;

public final class MappedSequentialFileFactory implements SequentialFileFactory {

   private final File directory;
   private int capacity;
   private final IOCriticalErrorListener criticalErrorListener;
   private final TimedBuffer timedBuffer;
   private boolean useDataSync;
   private boolean bufferPooling;
   //pools only the biggest one -> optimized for the common case
   private final ThreadLocal<ByteBuffer> bytesPool;
   private final int bufferSize;

   private MappedSequentialFileFactory(File directory,
                                       int capacity,
                                       final boolean buffered,
                                       final int bufferSize,
                                       final int bufferTimeout,
                                       IOCriticalErrorListener criticalErrorListener) {
      this.directory = directory;
      this.capacity = capacity;
      this.criticalErrorListener = criticalErrorListener;
      this.useDataSync = true;
      if (buffered && bufferTimeout > 0 && bufferSize > 0) {
         timedBuffer = new TimedBuffer(bufferSize, bufferTimeout, false);
      } else {
         timedBuffer = null;
      }
      this.bufferSize = bufferSize;
      this.bufferPooling = true;
      this.bytesPool = new ThreadLocal<>();
   }

   public MappedSequentialFileFactory capacity(int capacity) {
      this.capacity = capacity;
      return this;
   }

   public int capacity() {
      return capacity;
   }

   public static MappedSequentialFileFactory buffered(File directory,
                                                      int capacity,
                                                      final int bufferSize,
                                                      final int bufferTimeout,
                                                      IOCriticalErrorListener criticalErrorListener) {
      return new MappedSequentialFileFactory(directory, capacity, true, bufferSize, bufferTimeout, criticalErrorListener);
   }

   public static MappedSequentialFileFactory unbuffered(File directory,
                                                        int capacity,
                                                        IOCriticalErrorListener criticalErrorListener) {
      return new MappedSequentialFileFactory(directory, capacity, false, 0, 0, criticalErrorListener);
   }

   @Override
   public SequentialFile createSequentialFile(String fileName) {
      final MappedSequentialFile mappedSequentialFile = new MappedSequentialFile(this, directory, new File(directory, fileName), capacity, criticalErrorListener);
      if (this.timedBuffer == null) {
         return mappedSequentialFile;
      } else {
         return new TimedSequentialFile(this, mappedSequentialFile);
      }
   }

   @Override
   public MappedSequentialFileFactory setDatasync(boolean enabled) {
      this.useDataSync = enabled;
      return this;
   }

   @Override
   public boolean isDatasync() {
      return useDataSync;
   }

   @Override
   public long getBufferSize() {
      return bufferSize;
   }

   @Override
   public int getMaxIO() {
      return 1;
   }

   @Override
   public List<String> listFiles(final String extension) throws Exception {
      final FilenameFilter extensionFilter = (file, name) -> name.endsWith("." + extension);
      final String[] fileNames = directory.list(extensionFilter);
      if (fileNames == null) {
         return Collections.EMPTY_LIST;
      }
      return Arrays.asList(fileNames);
   }

   @Override
   public boolean isSupportsCallbacks() {
      return timedBuffer != null;
   }

   @Override
   public void onIOError(Exception exception, String message, SequentialFile file) {
      if (criticalErrorListener != null) {
         criticalErrorListener.onIOException(exception, message, file);
      }
   }

   @Override
   public ByteBuffer allocateDirectBuffer(final int size) {
      final int requiredCapacity = (int) BytesUtils.align(size, Env.osPageSize());
      final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(requiredCapacity);
      byteBuffer.limit(size);
      return byteBuffer;
   }

   @Override
   public void releaseDirectBuffer(ByteBuffer buffer) {
      PlatformDependent.freeDirectBuffer(buffer);
   }

   public MappedSequentialFileFactory enableBufferReuse() {
      this.bufferPooling = true;
      return this;
   }

   public MappedSequentialFileFactory disableBufferReuse() {
      this.bufferPooling = false;
      return this;
   }

   @Override
   public ByteBuffer newBuffer(final int size) {
      if (!this.bufferPooling) {
         return allocateDirectBuffer(size);
      } else {
         final int requiredCapacity = (int) BytesUtils.align(size, Env.osPageSize());
         ByteBuffer byteBuffer = bytesPool.get();
         if (byteBuffer == null || requiredCapacity > byteBuffer.capacity()) {
            //do not free the old one (if any) until the new one will be released into the pool!
            byteBuffer = ByteBuffer.allocateDirect(requiredCapacity);
         } else {
            bytesPool.set(null);
            PlatformDependent.setMemory(PlatformDependent.directBufferAddress(byteBuffer), size, (byte) 0);
            byteBuffer.clear();
         }
         byteBuffer.limit(size);
         return byteBuffer;
      }
   }

   @Override
   public void releaseBuffer(ByteBuffer buffer) {
      if (this.bufferPooling) {
         if (buffer.isDirect()) {
            final ByteBuffer byteBuffer = bytesPool.get();
            if (byteBuffer != buffer) {
               //replace with the current pooled only if greater or null
               if (byteBuffer == null || buffer.capacity() > byteBuffer.capacity()) {
                  if (byteBuffer != null) {
                     //free the smaller one
                     PlatformDependent.freeDirectBuffer(byteBuffer);
                  }
                  bytesPool.set(buffer);
               } else {
                  PlatformDependent.freeDirectBuffer(buffer);
               }
            }
         }
      }
   }

   @Override
   public void activateBuffer(SequentialFile file) {
      if (timedBuffer != null) {
         file.setTimedBuffer(timedBuffer);
      }
   }

   @Override
   public void deactivateBuffer() {
      if (timedBuffer != null) {
         // When moving to a new file, we need to make sure any pending buffer will be transferred to the buffer
         timedBuffer.flush();
         timedBuffer.setObserver(null);
      }
   }

   @Override
   public ByteBuffer wrapBuffer(final byte[] bytes) {
      return ByteBuffer.wrap(bytes);
   }

   @Override
   public int getAlignment() {
      return 1;
   }

   @Override
   @Deprecated
   public MappedSequentialFileFactory setAlignment(int alignment) {
      throw new UnsupportedOperationException("alignment can't be changed!");
   }

   @Override
   public int calculateBlockSize(int bytes) {
      return bytes;
   }

   @Override
   public File getDirectory() {
      return this.directory;
   }

   @Override
   public void clearBuffer(final ByteBuffer buffer) {
      if (buffer.isDirect()) {
         BytesUtils.zerosDirect(buffer);
      } else if (buffer.hasArray()) {
         final byte[] array = buffer.array();
         //SIMD OPTIMIZATION
         Arrays.fill(array, (byte) 0);
      } else {
         final int capacity = buffer.capacity();
         for (int i = 0; i < capacity; i++) {
            buffer.put(i, (byte) 0);
         }
      }
      buffer.rewind();
   }

   @Override
   public void start() {
      if (timedBuffer != null) {
         timedBuffer.start();
      }
   }

   @Override
   public void stop() {
      if (timedBuffer != null) {
         timedBuffer.stop();
      }
   }

   @Override
   public void createDirs() throws Exception {
      boolean ok = directory.mkdirs();
      if (!ok) {
         throw new IOException("Failed to create directory " + directory);
      }
   }

   @Override
   public void flush() {
      if (timedBuffer != null) {
         timedBuffer.flush();
      }
   }
}
