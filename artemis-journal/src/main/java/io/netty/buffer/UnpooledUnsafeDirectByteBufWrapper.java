/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.netty.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import io.netty.util.internal.PlatformDependent;

/**
 * A NIO direct {@link ByteBuffer} wrapper.
 * Only ByteBuffer's manipulation operations are supported.
 * Is best suited only for encoding/decoding purposes.
 */
public final class UnpooledUnsafeDirectByteBufWrapper extends AbstractReferenceCountedByteBuf {

   private ByteBuffer buffer;
   private long memoryAddress;

   /**
    * Creates a new direct buffer by wrapping the specified initial buffer.
    */
   public UnpooledUnsafeDirectByteBufWrapper() {
      super(0);
      this.buffer = null;
      this.memoryAddress = 0L;
   }

   public void wrap(ByteBuffer buffer, int srcIndex, int length) {
      if (buffer != null) {
         this.buffer = buffer;
         this.memoryAddress = PlatformDependent.directBufferAddress(buffer) + srcIndex;
         clear();
         maxCapacity(length);
      } else {
         reset();
      }
   }

   public void reset() {
      this.buffer = null;
      this.memoryAddress = 0L;
      clear();
      maxCapacity(0);
   }

   @Override
   public boolean isDirect() {
      return true;
   }

   @Override
   public int capacity() {
      return maxCapacity();
   }

   @Override
   public ByteBuf capacity(int newCapacity) {
      if (newCapacity != maxCapacity()) {
         throw new IllegalArgumentException("can't set a capacity different from the max allowed one");
      }
      return this;
   }

   @Override
   public ByteBufAllocator alloc() {
      return null;
   }

   @Override
   public ByteOrder order() {
      return ByteOrder.BIG_ENDIAN;
   }

   @Override
   public boolean hasArray() {
      return false;
   }

   @Override
   public byte[] array() {
      throw new UnsupportedOperationException("direct buffer");
   }

   @Override
   public int arrayOffset() {
      throw new UnsupportedOperationException("direct buffer");
   }

   @Override
   public boolean hasMemoryAddress() {
      return true;
   }

   @Override
   public long memoryAddress() {
      return memoryAddress;
   }

   @Override
   protected byte _getByte(int index) {
      return UnsafeByteBufUtil.getByte(addr(index));
   }

   @Override
   protected short _getShort(int index) {
      return UnsafeByteBufUtil.getShort(addr(index));
   }

   @Override
   protected short _getShortLE(int index) {
      return UnsafeByteBufUtil.getShortLE(addr(index));
   }

   @Override
   protected int _getUnsignedMedium(int index) {
      return UnsafeByteBufUtil.getUnsignedMedium(addr(index));
   }

   @Override
   protected int _getUnsignedMediumLE(int index) {
      return UnsafeByteBufUtil.getUnsignedMediumLE(addr(index));
   }

   @Override
   protected int _getInt(int index) {
      return UnsafeByteBufUtil.getInt(addr(index));
   }

   @Override
   protected int _getIntLE(int index) {
      return UnsafeByteBufUtil.getIntLE(addr(index));
   }

   @Override
   protected long _getLong(int index) {
      return UnsafeByteBufUtil.getLong(addr(index));
   }

   @Override
   protected long _getLongLE(int index) {
      return UnsafeByteBufUtil.getLongLE(addr(index));
   }

   @Override
   public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
      UnsafeByteBufUtil.getBytes(this, addr(index), index, dst, dstIndex, length);
      return this;
   }

   @Override
   public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
      UnsafeByteBufUtil.getBytes(this, addr(index), index, dst, dstIndex, length);
      return this;
   }

   @Override
   public ByteBuf getBytes(int index, ByteBuffer dst) {
      UnsafeByteBufUtil.getBytes(this, addr(index), index, dst);
      return this;
   }

   @Override
   public ByteBuf readBytes(ByteBuffer dst) {
      int length = dst.remaining();
      checkReadableBytes(length);
      getBytes(readerIndex, dst);
      readerIndex += length;
      return this;
   }

   @Override
   protected void _setByte(int index, int value) {
      UnsafeByteBufUtil.setByte(addr(index), value);
   }

   @Override
   protected void _setShort(int index, int value) {
      UnsafeByteBufUtil.setShort(addr(index), value);
   }

   @Override
   protected void _setShortLE(int index, int value) {
      UnsafeByteBufUtil.setShortLE(addr(index), value);
   }

   @Override
   protected void _setMedium(int index, int value) {
      UnsafeByteBufUtil.setMedium(addr(index), value);
   }

   @Override
   protected void _setMediumLE(int index, int value) {
      UnsafeByteBufUtil.setMediumLE(addr(index), value);
   }

   @Override
   protected void _setInt(int index, int value) {
      UnsafeByteBufUtil.setInt(addr(index), value);
   }

   @Override
   protected void _setIntLE(int index, int value) {
      UnsafeByteBufUtil.setIntLE(addr(index), value);
   }

   @Override
   protected void _setLong(int index, long value) {
      UnsafeByteBufUtil.setLong(addr(index), value);
   }

   @Override
   protected void _setLongLE(int index, long value) {
      UnsafeByteBufUtil.setLongLE(addr(index), value);
   }

   @Override
   public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
      UnsafeByteBufUtil.setBytes(this, addr(index), index, src, srcIndex, length);
      return this;
   }

   @Override
   public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
      UnsafeByteBufUtil.setBytes(this, addr(index), index, src, srcIndex, length);
      return this;
   }

   @Override
   public ByteBuf setBytes(int index, ByteBuffer src) {
      UnsafeByteBufUtil.setBytes(this, addr(index), index, src);
      return this;
   }

   @Override
   @Deprecated
   public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   @Deprecated
   public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   @Deprecated
   public int getBytes(int index, FileChannel out, long position, int length) throws IOException {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   @Deprecated
   public int readBytes(GatheringByteChannel out, int length) throws IOException {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   @Deprecated
   public int readBytes(FileChannel out, long position, int length) throws IOException {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   @Deprecated
   public int setBytes(int index, InputStream in, int length) throws IOException {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   @Deprecated
   public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   @Deprecated
   public int setBytes(int index, FileChannel in, long position, int length) throws IOException {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   public int nioBufferCount() {
      return 1;
   }

   @Override
   @Deprecated
   public ByteBuffer[] nioBuffers(int index, int length) {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   @Deprecated
   public ByteBuf copy(int index, int length) {

      throw new UnsupportedOperationException("unsupported!");

   }

   @Override
   @Deprecated
   public ByteBuffer internalNioBuffer(int index, int length) {
      throw new UnsupportedOperationException("cannot access directly the wrapped buffer!");
   }

   @Override
   @Deprecated
   public ByteBuffer nioBuffer(int index, int length) {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   @Deprecated
   protected void deallocate() {
      //NO_OP
   }

   @Override
   public ByteBuf unwrap() {
      return null;
   }

   private long addr(int index) {
      return memoryAddress + index;
   }

   @Override
   @Deprecated
   protected SwappedByteBuf newSwappedByteBuf() {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   public ByteBuf setZero(int index, int length) {
      UnsafeByteBufUtil.setZero(this, addr(index), index, length);
      return this;
   }

   @Override
   public ByteBuf writeZero(int length) {
      ensureWritable(length);
      int wIndex = writerIndex;
      setZero(wIndex, length);
      writerIndex = wIndex + length;
      return this;
   }
}

