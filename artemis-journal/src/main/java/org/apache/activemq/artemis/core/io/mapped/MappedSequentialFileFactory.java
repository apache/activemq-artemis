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
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;

public final class MappedSequentialFileFactory implements SequentialFileFactory {

   private static long DEFAULT_BLOCK_SIZE = 64L << 20;
   private final File directory;
   private final IOCriticalErrorListener criticalErrorListener;
   private long chunkBytes;
   private long overlapBytes;
   private boolean useDataSync;

   public MappedSequentialFileFactory(File directory, IOCriticalErrorListener criticalErrorListener) {
      this.directory = directory;
      this.criticalErrorListener = criticalErrorListener;
      this.chunkBytes = DEFAULT_BLOCK_SIZE;
      this.overlapBytes = DEFAULT_BLOCK_SIZE / 4;
   }

   public MappedSequentialFileFactory(File directory) {
      this.directory = directory;
      this.criticalErrorListener = null;
      this.chunkBytes = DEFAULT_BLOCK_SIZE;
      this.overlapBytes = DEFAULT_BLOCK_SIZE / 4;
   }

   public long chunkBytes() {
      return chunkBytes;
   }

   public MappedSequentialFileFactory chunkBytes(long chunkBytes) {
      this.chunkBytes = chunkBytes;
      return this;
   }

   public long overlapBytes() {
      return overlapBytes;
   }

   public MappedSequentialFileFactory overlapBytes(long overlapBytes) {
      this.overlapBytes = overlapBytes;
      return this;
   }

   @Override
   public SequentialFile createSequentialFile(String fileName) {
      return new MappedSequentialFile(this, directory, new File(directory, fileName), chunkBytes, overlapBytes, criticalErrorListener);
   }

   @Override
   public SequentialFileFactory setDatasync(boolean enabled) {
      this.useDataSync = enabled;
      return this;
   }

   @Override
   public boolean isDatasync() {
      return useDataSync;
   }

   @Override
   public int getMaxIO() {
      return 0;
   }

   @Override
   public List<String> listFiles(final String extension) throws Exception {
      final FilenameFilter extensionFilter = new FilenameFilter() {
         @Override
         public boolean accept(final File file, final String name) {
            return name.endsWith("." + extension);
         }
      };
      final String[] fileNames = directory.list(extensionFilter);
      if (fileNames == null) {
         return Collections.EMPTY_LIST;
      }
      return Arrays.asList(fileNames);
   }

   @Override
   public boolean isSupportsCallbacks() {
      return false;
   }

   @Override
   public void onIOError(Exception exception, String message, SequentialFile file) {
      if (criticalErrorListener != null) {
         criticalErrorListener.onIOException(exception, message, file);
      }
   }

   @Override
   public ByteBuffer allocateDirectBuffer(final int size) {
      return ByteBuffer.allocateDirect(size).order(ByteOrder.nativeOrder());
   }

   @Override
   public void releaseDirectBuffer(final ByteBuffer buffer) {
      PlatformDependent.freeDirectBuffer(buffer);
   }

   @Override
   public ByteBuffer newBuffer(final int size) {
      return ByteBuffer.allocateDirect(size).order(ByteOrder.nativeOrder());
   }

   @Override
   public void releaseBuffer(ByteBuffer buffer) {
      if (buffer.isDirect()) {
         PlatformDependent.freeDirectBuffer(buffer);
      }
   }

   @Override
   public void activateBuffer(SequentialFile file) {

   }

   @Override
   public void deactivateBuffer() {

   }

   @Override
   public ByteBuffer wrapBuffer(final byte[] bytes) {
      return ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
   }

   @Override
   public int getAlignment() {
      return 1;
   }

   @Override
   public SequentialFileFactory setAlignment(int alignment) {
      // no op
      return this;
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
      buffer.clear();
      if (buffer.isDirect()) {
         BytesUtils.zerosDirect(buffer);
      } else if (buffer.hasArray()) {
         final byte[] array = buffer.array();
         //SIMD OPTIMIZATION
         Arrays.fill(array, (byte) 0);
      } else {
         //TODO VERIFY IF IT COULD HAPPENS
         final int capacity = buffer.capacity();
         for (int i = 0; i < capacity; i++) {
            buffer.put(i, (byte) 0);
         }
      }
   }

   @Override
   public void start() {

   }

   @Override
   public void stop() {

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

   }
}
