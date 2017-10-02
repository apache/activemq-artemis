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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.io.buffer.UnpooledUnsafeDirectByteBufWrapper;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.utils.Env;

final class MappedFile implements AutoCloseable {

   private static final int OS_PAGE_SIZE = Env.osPageSize();
   private final MappedByteBuffer buffer;
   private final FileChannel channel;
   private final long address;
   private final UnpooledUnsafeDirectByteBufWrapper byteBufWrapper;
   private final ChannelBufferWrapper channelBufferWrapper;
   private int position;
   private int length;

   private MappedFile(FileChannel channel, MappedByteBuffer byteBuffer, int position, int length) throws IOException {
      this.channel = channel;
      this.buffer = byteBuffer;
      this.position = position;
      this.length = length;
      this.byteBufWrapper = new UnpooledUnsafeDirectByteBufWrapper();
      this.channelBufferWrapper = new ChannelBufferWrapper(this.byteBufWrapper, false);
      this.address = PlatformDependent.directBufferAddress(buffer);
   }

   public static MappedFile of(File file, int position, int capacity) throws IOException {
      final MappedByteBuffer buffer;
      final int length;
      final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
      length = (int) channel.size();
      if (length != capacity && length != 0) {
         channel.close();
         throw new IllegalStateException("the file is not " + capacity + " bytes long!");
      }
      buffer = channel.map(FileChannel.MapMode.READ_WRITE, position, capacity);
      return new MappedFile(channel, buffer, 0, length);
   }

   public FileChannel channel() {
      return channel;
   }

   public MappedByteBuffer mapped() {
      return buffer;
   }

   public long address() {
      return this.address;
   }

   public void force() {
      this.buffer.force();
   }

   /**
    * Reads a sequence of bytes from this file into the given buffer.
    * <p>
    * <p> Bytes are read starting at this file's specified position.
    */
   public int read(int position, ByteBuf dst, int dstStart, int dstLength) throws IOException {
      final long srcAddress = this.address + position;
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
   public int read(int position, ByteBuffer dst, int dstStart, int dstLength) throws IOException {
      final long srcAddress = this.address + position;
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
      final int remaining = this.length - this.position;
      final int read = Math.min(remaining, dstLength);
      final long srcAddress = this.address + position;
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
      final int remaining = this.length - this.position;
      final int read = Math.min(remaining, dstLength);
      final long srcAddress = this.address + position;
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
      this.byteBufWrapper.wrap(this.buffer, this.position, encodedSize);
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
      final long destAddress = this.address + position;
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
      final long destAddress = this.address + position;
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
   public void write(int position, ByteBuf src, int srcStart, int srcLength) throws IOException {
      final long destAddress = this.address + position;
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
   public void write(int position, ByteBuffer src, int srcStart, int srcLength) throws IOException {
      final long destAddress = this.address + position;
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
   public void zeros(int position, final int count) throws IOException {
      //zeroes memory in reverse direction in OS_PAGE_SIZE batches
      //to gain sympathy by the page cache LRU policy
      final long start = this.address + position;
      final long end = start + count;
      int toZeros = count;
      final long lastGap = (int) (end & (OS_PAGE_SIZE - 1));
      final long lastStartPage = end - lastGap;
      long lastZeroed = end;
      if (start <= lastStartPage) {
         if (lastGap > 0) {
            PlatformDependent.setMemory(lastStartPage, lastGap, (byte) 0);
            lastZeroed = lastStartPage;
            toZeros -= lastGap;
         }
      }
      //any that will enter has lastZeroed OS page aligned
      while (toZeros >= OS_PAGE_SIZE) {
         assert BytesUtils.isAligned(lastZeroed, OS_PAGE_SIZE);/**/
         final long startPage = lastZeroed - OS_PAGE_SIZE;
         PlatformDependent.setMemory(startPage, OS_PAGE_SIZE, (byte) 0);
         lastZeroed = startPage;
         toZeros -= OS_PAGE_SIZE;
      }
      //there is anything left in the first OS page?
      if (toZeros > 0) {
         PlatformDependent.setMemory(start, toZeros, (byte) 0);
      }

      position += count;
      if (position > this.length) {
         this.length = position;
      }
   }

   public int position() {
      return position;
   }

   public void position(int position) {
      this.position = position;
   }

   public long length() {
      return length;
   }

   @Override
   public void close() {
      try {
         channel.close();
      } catch (IOException e) {
         throw new IllegalStateException(e);
      } finally {
         //unmap in a deterministic way: do not rely on GC to do it
         PlatformDependent.freeDirectBuffer(this.buffer);
      }
   }
}
