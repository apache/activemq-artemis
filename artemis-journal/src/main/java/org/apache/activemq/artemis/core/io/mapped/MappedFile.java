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
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledUnsafeDirectByteBufWrapper;
import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.journal.EncodingSupport;

final class MappedFile implements AutoCloseable {

   private final MappedByteBufferCache cache;
   private final UnpooledUnsafeDirectByteBufWrapper byteBufWrapper;
   private final ChannelBufferWrapper channelBufferWrapper;
   private MappedByteBuffer lastMapped;
   private long lastMappedStart;
   private long lastMappedLimit;
   private long position;
   private long length;

   private MappedFile(MappedByteBufferCache cache) throws IOException {
      this.cache = cache;
      this.lastMapped = null;
      this.lastMappedStart = -1;
      this.lastMappedLimit = -1;
      this.position = 0;
      this.length = this.cache.fileSize();
      this.byteBufWrapper = new UnpooledUnsafeDirectByteBufWrapper();
      this.channelBufferWrapper = new ChannelBufferWrapper(this.byteBufWrapper, false);
   }

   public static MappedFile of(File file, long chunckSize, long overlapSize) throws IOException {
      return new MappedFile(MappedByteBufferCache.of(file, chunckSize, overlapSize));
   }

   public MappedByteBufferCache cache() {
      return cache;
   }

   private int checkOffset(long offset, int bytes) throws BufferUnderflowException, IOException {
      if (!MappedByteBufferCache.inside(offset, lastMappedStart, lastMappedLimit)) {
         return updateOffset(offset, bytes);
      } else {
         final int bufferPosition = (int) (offset - lastMappedStart);
         return bufferPosition;
      }
   }

   private int updateOffset(long offset, int bytes) throws BufferUnderflowException, IOException {
      try {
         final int index = cache.indexFor(offset);
         final long mappedPosition = cache.mappedPositionFor(index);
         final long mappedLimit = cache.mappedLimitFor(mappedPosition);
         if (offset + bytes > mappedLimit) {
            throw new IOException("mapping overflow!");
         }
         lastMapped = cache.acquireMappedByteBuffer(index);
         lastMappedStart = mappedPosition;
         lastMappedLimit = mappedLimit;
         final int bufferPosition = (int) (offset - mappedPosition);
         return bufferPosition;
      } catch (IllegalStateException e) {
         throw new IOException(e);
      } catch (IllegalArgumentException e) {
         throw new BufferUnderflowException();
      }
   }

   public void force() {
      if (lastMapped != null) {
         lastMapped.force();
      }
   }

   /**
    * Reads a sequence of bytes from this file into the given buffer.
    * <p>
    * <p> Bytes are read starting at this file's specified position.
    */
   public int read(long position, ByteBuf dst, int dstStart, int dstLength) throws IOException {
      final int bufferPosition = checkOffset(position, dstLength);
      final long srcAddress = PlatformDependent.directBufferAddress(lastMapped) + bufferPosition;
      if (dst.hasMemoryAddress()) {
         final long dstAddress = dst.memoryAddress() + dstStart;
         PlatformDependent.copyMemory(srcAddress, dstAddress, dstLength);
      } else if (dst.hasArray()) {
         final byte[] dstArray = dst.array();
         PlatformDependent.copyMemory(srcAddress, dstArray, dstStart, dstLength);
      } else {
         throw new IllegalArgumentException("unsupported byte buffer");
      }
      position += dstLength;
      if (position > this.length) {
         this.length = position;
      }
      return dstLength;
   }

   /**
    * Reads a sequence of bytes from this file into the given buffer.
    * <p>
    * <p> Bytes are read starting at this file's specified position.
    */
   public int read(long position, ByteBuffer dst, int dstStart, int dstLength) throws IOException {
      final int bufferPosition = checkOffset(position, dstLength);
      final long srcAddress = PlatformDependent.directBufferAddress(lastMapped) + bufferPosition;
      if (dst.isDirect()) {
         final long dstAddress = PlatformDependent.directBufferAddress(dst) + dstStart;
         PlatformDependent.copyMemory(srcAddress, dstAddress, dstLength);
      } else {
         final byte[] dstArray = dst.array();
         PlatformDependent.copyMemory(srcAddress, dstArray, dstStart, dstLength);
      }
      position += dstLength;
      if (position > this.length) {
         this.length = position;
      }
      return dstLength;
   }

   /**
    * Reads a sequence of bytes from this file into the given buffer.
    * <p>
    * <p> Bytes are read starting at this file's current position, and
    * then the position is updated with the number of bytes actually read.
    */
   public int read(ByteBuf dst, int dstStart, int dstLength) throws IOException {
      final int remaining = (int) Math.min(this.length - this.position, Integer.MAX_VALUE);
      final int read = Math.min(remaining, dstLength);
      final int bufferPosition = checkOffset(position, read);
      final long srcAddress = PlatformDependent.directBufferAddress(lastMapped) + bufferPosition;
      if (dst.hasMemoryAddress()) {
         final long dstAddress = dst.memoryAddress() + dstStart;
         PlatformDependent.copyMemory(srcAddress, dstAddress, read);
      } else if (dst.hasArray()) {
         final byte[] dstArray = dst.array();
         PlatformDependent.copyMemory(srcAddress, dstArray, dstStart, read);
      } else {
         throw new IllegalArgumentException("unsupported byte buffer");
      }
      position += read;
      return read;
   }

   /**
    * Reads a sequence of bytes from this file into the given buffer.
    * <p>
    * <p> Bytes are read starting at this file's current position, and
    * then the position is updated with the number of bytes actually read.
    */
   public int read(ByteBuffer dst, int dstStart, int dstLength) throws IOException {
      final int remaining = (int) Math.min(this.length - this.position, Integer.MAX_VALUE);
      final int read = Math.min(remaining, dstLength);
      final int bufferPosition = checkOffset(position, read);
      final long srcAddress = PlatformDependent.directBufferAddress(lastMapped) + bufferPosition;
      if (dst.isDirect()) {
         final long dstAddress = PlatformDependent.directBufferAddress(dst) + dstStart;
         PlatformDependent.copyMemory(srcAddress, dstAddress, read);
      } else {
         final byte[] dstArray = dst.array();
         PlatformDependent.copyMemory(srcAddress, dstArray, dstStart, read);
      }
      position += read;
      return read;
   }

   /**
    * Writes an encoded sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's current position,
    */
   public void write(EncodingSupport encodingSupport) throws IOException {
      final int encodedSize = encodingSupport.getEncodeSize();
      final int bufferPosition = checkOffset(position, encodedSize);
      this.byteBufWrapper.wrap(this.lastMapped, bufferPosition, encodedSize);
      try {
         encodingSupport.encode(this.channelBufferWrapper);
      } finally {
         this.byteBufWrapper.reset();
      }
      position += encodedSize;
      if (position > this.length) {
         this.length = position;
      }
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's current position,
    */
   public void write(ByteBuf src, int srcStart, int srcLength) throws IOException {
      final int bufferPosition = checkOffset(position, srcLength);
      final long destAddress = PlatformDependent.directBufferAddress(lastMapped) + bufferPosition;
      if (src.hasMemoryAddress()) {
         final long srcAddress = src.memoryAddress() + srcStart;
         PlatformDependent.copyMemory(srcAddress, destAddress, srcLength);
      } else if (src.hasArray()) {
         final byte[] srcArray = src.array();
         PlatformDependent.copyMemory(srcArray, srcStart, destAddress, srcLength);
      } else {
         throw new IllegalArgumentException("unsupported byte buffer");
      }
      position += srcLength;
      if (position > this.length) {
         this.length = position;
      }
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's current position,
    */
   public void write(ByteBuffer src, int srcStart, int srcLength) throws IOException {
      final int bufferPosition = checkOffset(position, srcLength);
      final long destAddress = PlatformDependent.directBufferAddress(lastMapped) + bufferPosition;
      if (src.isDirect()) {
         final long srcAddress = PlatformDependent.directBufferAddress(src) + srcStart;
         PlatformDependent.copyMemory(srcAddress, destAddress, srcLength);
      } else {
         final byte[] srcArray = src.array();
         PlatformDependent.copyMemory(srcArray, srcStart, destAddress, srcLength);
      }
      position += srcLength;
      if (position > this.length) {
         this.length = position;
      }
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's specified position,
    */
   public void write(long position, ByteBuf src, int srcStart, int srcLength) throws IOException {
      final int bufferPosition = checkOffset(position, srcLength);
      final long destAddress = PlatformDependent.directBufferAddress(lastMapped) + bufferPosition;
      if (src.hasMemoryAddress()) {
         final long srcAddress = src.memoryAddress() + srcStart;
         PlatformDependent.copyMemory(srcAddress, destAddress, srcLength);
      } else if (src.hasArray()) {
         final byte[] srcArray = src.array();
         PlatformDependent.copyMemory(srcArray, srcStart, destAddress, srcLength);
      } else {
         throw new IllegalArgumentException("unsupported byte buffer");
      }
      position += srcLength;
      if (position > this.length) {
         this.length = position;
      }
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's specified position,
    */
   public void write(long position, ByteBuffer src, int srcStart, int srcLength) throws IOException {
      final int bufferPosition = checkOffset(position, srcLength);
      final long destAddress = PlatformDependent.directBufferAddress(lastMapped) + bufferPosition;
      if (src.isDirect()) {
         final long srcAddress = PlatformDependent.directBufferAddress(src) + srcStart;
         PlatformDependent.copyMemory(srcAddress, destAddress, srcLength);
      } else {
         final byte[] srcArray = src.array();
         PlatformDependent.copyMemory(srcArray, srcStart, destAddress, srcLength);
      }
      position += srcLength;
      if (position > this.length) {
         this.length = position;
      }
   }

   /**
    * Writes a sequence of bytes to this file from the given buffer.
    * <p>
    * <p> Bytes are written starting at this file's current position,
    */
   public void zeros(long offset, int count) throws IOException {
      while (count > 0) {
         //do not need to validate the bytes count
         final int bufferPosition = checkOffset(offset, 0);
         final int endZerosPosition = (int)Math.min((long)bufferPosition + count, lastMapped.capacity());
         final int zeros = endZerosPosition - bufferPosition;
         final long destAddress = PlatformDependent.directBufferAddress(lastMapped) + bufferPosition;
         PlatformDependent.setMemory(destAddress, zeros, (byte) 0);
         offset += zeros;
         count -= zeros;
         //TODO need to call force on each write?
         //this.force();
      }
      if (offset > this.length) {
         this.length = offset;
      }
   }

   public long position() {
      return position;
   }

   public long position(long newPosition) {
      final long oldPosition = this.position;
      this.position = newPosition;
      return oldPosition;
   }

   public long length() {
      return length;
   }

   @Override
   public void close() {
      cache.close();
   }

   public void closeAndResize(long length) {
      cache.closeAndResize(length);
   }
}
