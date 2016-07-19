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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;

import io.netty.util.internal.PlatformDependent;

final class MappedByteBufferCache implements AutoCloseable {

   public static final int PAGE_SIZE = Integer.parseInt(System.getProperty("os_page_size", "4096"));
   private static final Object FILE_LOCK = new Object();
   private final RandomAccessFile raf;
   private final FileChannel fileChannel;
   private final long chunkBytes;
   private final long overlapBytes;
   private final ArrayList<WeakReference<MappedByteBuffer>> byteBuffers;
   private final File file;
   private final long mappedSize;
   private boolean closed;

   private MappedByteBufferCache(File file, RandomAccessFile raf, long chunkBytes, long overlapBytes, long alignment) {
      this.byteBuffers = new ArrayList<>();
      this.file = file;
      this.raf = raf;
      this.fileChannel = raf.getChannel();
      this.chunkBytes = BytesUtils.align(chunkBytes, alignment);
      this.overlapBytes = BytesUtils.align(overlapBytes, alignment);
      this.closed = false;
      this.mappedSize = this.chunkBytes + this.overlapBytes;
   }

   public static MappedByteBufferCache of(File file, long chunkSize, long overlapSize) throws FileNotFoundException {
      final RandomAccessFile raf = new RandomAccessFile(file, "rw");
      return new MappedByteBufferCache(file, raf, chunkSize, overlapSize, PAGE_SIZE);
   }

   public static boolean inside(long position, long mappedPosition, long mappedLimit) {
      return mappedPosition <= position && position < mappedLimit;
   }

   public File file() {
      return file;
   }

   public long chunkBytes() {
      return chunkBytes;
   }

   public long overlapBytes() {
      return overlapBytes;
   }

   public int indexFor(long position) {
      final int chunk = (int) (position / chunkBytes);
      return chunk;
   }

   public long mappedPositionFor(int index) {
      return index * chunkBytes;
   }

   public long mappedLimitFor(long mappedPosition) {
      return mappedPosition + chunkBytes;
   }

   public MappedByteBuffer acquireMappedByteBuffer(final int index) throws IOException, IllegalArgumentException, IllegalStateException {
      if (closed)
         throw new IOException("Closed");
      if (index < 0)
         throw new IOException("Attempt to access a negative index: " + index);
      while (byteBuffers.size() <= index) {
         byteBuffers.add(null);
      }
      final WeakReference<MappedByteBuffer> mbbRef = byteBuffers.get(index);
      if (mbbRef != null) {
         final MappedByteBuffer mbb = mbbRef.get();
         if (mbb != null) {
            return mbb;
         }
      }
      return mapAndAcquire(index);
   }

   //METHOD BUILT TO SEPARATE THE SLOW PATH TO ENSURE INLINING OF THE MOST OCCURRING CASE
   private MappedByteBuffer mapAndAcquire(final int index) throws IOException {
      final long chunkStartPosition = mappedPositionFor(index);
      final long minSize = chunkStartPosition + mappedSize;
      if (fileChannel.size() < minSize) {
         try {
            synchronized (FILE_LOCK) {
               try (FileLock lock = fileChannel.lock()) {
                  final long size = fileChannel.size();
                  if (size < minSize) {
                     raf.setLength(minSize);
                  }
               }
            }
         }
         catch (IOException ioe) {
            throw new IOException("Failed to resize to " + minSize, ioe);
         }
      }

      final MappedByteBuffer mbb = fileChannel.map(FileChannel.MapMode.READ_WRITE, chunkStartPosition, mappedSize);
      mbb.order(ByteOrder.nativeOrder());
      byteBuffers.set(index, new WeakReference<>(mbb));
      return mbb;
   }

   public long fileSize() throws IOException {
      if (closed)
         throw new IllegalStateException("Closed");
      return fileChannel.size();
   }

   public void closeAndResize(long length) {
      if (!closed) {
         //TO_FIX: unmap in this way is not portable BUT required on Windows that can't resize a memmory mapped file!
         final int mappedBuffers = this.byteBuffers.size();
         for (int i = 0; i < mappedBuffers; i++) {
            final WeakReference<MappedByteBuffer> mbbRef = byteBuffers.get(i);
            if (mbbRef != null) {
               final MappedByteBuffer mbb = mbbRef.get();
               if (mbb != null) {
                  try {
                     PlatformDependent.freeDirectBuffer(mbb);
                  }
                  catch (Throwable t) {
                     //TO_FIX: force releasing of the other buffers
                  }
               }
            }
         }
         this.byteBuffers.clear();
         try {
            if (fileChannel.size() != length) {
               try {
                  synchronized (FILE_LOCK) {
                     try (FileLock lock = fileChannel.lock()) {
                        final long size = fileChannel.size();
                        if (size != length) {
                           raf.setLength(length);
                        }
                     }
                  }
               }
               catch (IOException ioe) {
                  throw new IllegalStateException("Failed to resize to " + length, ioe);
               }
            }
         }
         catch (IOException ex) {
            throw new IllegalStateException("Failed to get size", ex);
         }
         finally {
            try {
               fileChannel.close();
            }
            catch (IOException e) {
               throw new IllegalStateException("Failed to close channel", e);
            }
            finally {
               try {
                  raf.close();
               }
               catch (IOException e) {
                  throw new IllegalStateException("Failed to close RandomAccessFile", e);
               }
            }
            closed = true;
         }
      }
   }

   public boolean isClosed() {
      return closed;
   }

   @Override
   public void close() {
      if (!closed) {
         //TO_FIX: unmap in this way is not portable BUT required on Windows that can't resize a memory mapped file!
         final int mappedBuffers = this.byteBuffers.size();
         for (int i = 0; i < mappedBuffers; i++) {
            final WeakReference<MappedByteBuffer> mbbRef = byteBuffers.get(i);
            if (mbbRef != null) {
               final MappedByteBuffer mbb = mbbRef.get();
               if (mbb != null) {
                  try {
                     PlatformDependent.freeDirectBuffer(mbb);
                  }
                  catch (Throwable t) {
                     //TO_FIX: force releasing of the other buffers
                  }
               }
            }
         }
         this.byteBuffers.clear();
         try {
            fileChannel.close();
         }
         catch (IOException e) {
            throw new IllegalStateException("Failed to close channel", e);
         }
         finally {
            try {
               raf.close();
            }
            catch (IOException e) {
               throw new IllegalStateException("Failed to close RandomAccessFile", e);
            }
         }
         closed = true;
      }
   }
}