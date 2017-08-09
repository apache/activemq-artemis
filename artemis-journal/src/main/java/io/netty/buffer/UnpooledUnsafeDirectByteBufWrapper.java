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
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import io.netty.util.internal.PlatformDependent;

/**
 * A NIO {@link ByteBuffer}, byte[] and address direct access wrapper.
 * Only content manipulation operations are supported.
 * Is best suited only for encoding/decoding purposes.
 */
public final class UnpooledUnsafeDirectByteBufWrapper extends AbstractReferenceCountedByteBuf {

   private static final byte ZERO = 0;
   private ByteBuffer buffer;
   private int arrayOffset;
   private byte[] array;
   private long memoryAddress;

   public UnpooledUnsafeDirectByteBufWrapper() {
      super(0);
      this.buffer = null;
      this.arrayOffset = -1;
      this.array = null;
      this.memoryAddress = 0L;
   }

   public void wrap(long address, int length) {
      this.memoryAddress = address;
      this.arrayOffset = -1;
      this.array = null;
      this.buffer = null;
      clear();
      maxCapacity(length);
   }

   public void wrap(byte[] array, int srcIndex, int length) {
      if (array != null) {
         this.memoryAddress = 0L;
         this.arrayOffset = srcIndex;
         this.array = array;
         this.buffer = null;
         clear();
         maxCapacity(length);
      } else {
         reset();
      }
   }

   public void wrap(ByteBuffer buffer, int srcIndex, int length) {
      if (buffer != null) {
         this.buffer = buffer;
         if (buffer.isDirect()) {
            this.memoryAddress = PlatformDependent.directBufferAddress(buffer) + srcIndex;
            this.arrayOffset = -1;
            this.array = null;
         } else {
            this.arrayOffset = srcIndex;
            this.array = buffer.array();
            this.memoryAddress = 0L;
         }
         clear();
         maxCapacity(length);
      } else {
         reset();
      }
   }

   public void reset() {
      this.buffer = null;
      this.memoryAddress = 0L;
      this.arrayOffset = -1;
      this.array = null;
      clear();
      maxCapacity(0);
   }

   @Override
   public boolean isDirect() {
      return buffer != null ? buffer.isDirect() : false;
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
      return array != null;
   }

   @Override
   public byte[] array() {
      return array;
   }

   @Override
   public int arrayOffset() {
      return arrayOffset;
   }

   @Override
   public boolean hasMemoryAddress() {
      return memoryAddress != 0;
   }

   @Override
   public long memoryAddress() {
      return memoryAddress;
   }

   @Override
   protected byte _getByte(int index) {
      if (hasMemoryAddress()) {
         return UnsafeByteBufUtil.getByte(addr(index));
      } else {
         return UnsafeByteBufUtil.getByte(array, idx(index));
      }
   }

   @Override
   protected short _getShort(int index) {
      if (hasMemoryAddress()) {
         return UnsafeByteBufUtil.getShort(addr(index));
      } else {
         return UnsafeByteBufUtil.getShort(array, idx(index));
      }
   }

   @Override
   protected short _getShortLE(int index) {
      if (hasMemoryAddress()) {
         return UnsafeByteBufUtil.getShortLE(addr(index));
      } else {
         return UnsafeByteBufUtil.getShortLE(array, idx(index));
      }
   }

   @Override
   protected int _getUnsignedMedium(int index) {
      if (hasMemoryAddress()) {
         return UnsafeByteBufUtil.getUnsignedMedium(addr(index));
      } else {
         return UnsafeByteBufUtil.getUnsignedMedium(array, idx(index));
      }
   }

   @Override
   protected int _getUnsignedMediumLE(int index) {
      if (hasMemoryAddress()) {
         return UnsafeByteBufUtil.getUnsignedMediumLE(addr(index));
      } else {
         return UnsafeByteBufUtil.getUnsignedMediumLE(array, idx(index));
      }
   }

   @Override
   protected int _getInt(int index) {
      if (hasMemoryAddress()) {
         return UnsafeByteBufUtil.getInt(addr(index));
      } else {
         return UnsafeByteBufUtil.getInt(array, idx(index));
      }
   }

   @Override
   protected int _getIntLE(int index) {
      if (hasMemoryAddress()) {
         return UnsafeByteBufUtil.getIntLE(addr(index));
      } else {
         return UnsafeByteBufUtil.getIntLE(array, idx(index));
      }
   }

   @Override
   protected long _getLong(int index) {
      if (hasMemoryAddress()) {
         return UnsafeByteBufUtil.getLong(addr(index));
      } else {
         return UnsafeByteBufUtil.getLong(array, idx(index));
      }
   }

   @Override
   protected long _getLongLE(int index) {
      if (hasMemoryAddress()) {
         return UnsafeByteBufUtil.getLongLE(addr(index));
      } else {
         return UnsafeByteBufUtil.getLongLE(array, idx(index));
      }
   }

   @Override
   public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.getBytes(this, addr(index), index, dst, dstIndex, length);
      } else {
         final int idx = idx(index);
         checkDstIndex(idx, length, dstIndex, dst.capacity());
         getBytes(array, idx, dst, dstIndex, length);
      }
      return this;
   }

   private static void getBytes(byte[] array, int idx, ByteBuf dst, int dstIndex, int length) {
      if (dst.hasMemoryAddress()) {
         PlatformDependent.copyMemory(array, idx, dst.memoryAddress() + dstIndex, length);
      } else if (dst.hasArray()) {
         System.arraycopy(array, idx, dst.array(), dst.arrayOffset() + dstIndex, length);
      } else {
         dst.setBytes(dstIndex, array, idx, length);
      }
   }

   @Override
   public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.getBytes(this, addr(index), index, dst, dstIndex, length);
      } else {
         final int idx = idx(index);
         checkDstIndex(idx, length, dstIndex, dst.length);
         System.arraycopy(array, idx, dst, dstIndex, length);
      }
      return this;
   }

   @Override
   public ByteBuf getBytes(int index, ByteBuffer dst) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.getBytes(this, addr(index), index, dst);
      } else {
         final int idx = idx(index);
         checkIndex(idx, dst.remaining());
         getBytes(array, idx, dst);
      }
      return this;
   }

   private static void getBytes(byte[] array, int idx, ByteBuffer dst) {
      if (dst.remaining() == 0) {
         return;
      }
      if (dst.isDirect()) {
         if (dst.isReadOnly()) {
            // We need to check if dst is ready-only so we not write something in it by using Unsafe.
            throw new ReadOnlyBufferException();
         }
         // Copy to direct memory
         final long dstAddress = PlatformDependent.directBufferAddress(dst);
         PlatformDependent.copyMemory(array, idx, dstAddress + dst.position(), dst.remaining());
         dst.position(dst.position() + dst.remaining());
      } else if (dst.hasArray()) {
         // Copy to array
         System.arraycopy(array, idx, dst.array(), dst.arrayOffset() + dst.position(), dst.remaining());
         dst.position(dst.position() + dst.remaining());
      } else {
         dst.put(array, idx, dst.remaining());
      }
   }

   @Override
   public ByteBuf readBytes(ByteBuffer dst) {
      final int length = dst.remaining();
      checkReadableBytes(length);
      getBytes(readerIndex, dst);
      readerIndex += length;
      return this;
   }

   @Override
   protected void _setByte(int index, int value) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setByte(addr(index), value);
      } else {
         UnsafeByteBufUtil.setByte(array, idx(index), value);
      }
   }

   @Override
   protected void _setShort(int index, int value) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setShort(addr(index), value);
      } else {
         UnsafeByteBufUtil.setShort(array, idx(index), value);
      }
   }

   @Override
   protected void _setShortLE(int index, int value) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setShortLE(addr(index), value);
      } else {
         UnsafeByteBufUtil.setShortLE(array, idx(index), value);
      }
   }

   @Override
   protected void _setMedium(int index, int value) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setMedium(addr(index), value);
      } else {
         UnsafeByteBufUtil.setMedium(array, idx(index), value);
      }
   }

   @Override
   protected void _setMediumLE(int index, int value) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setMediumLE(addr(index), value);
      } else {
         UnsafeByteBufUtil.setMediumLE(array, idx(index), value);
      }
   }

   @Override
   protected void _setInt(int index, int value) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setInt(addr(index), value);
      } else {
         UnsafeByteBufUtil.setInt(array, idx(index), value);
      }
   }

   @Override
   protected void _setIntLE(int index, int value) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setIntLE(addr(index), value);
      } else {
         UnsafeByteBufUtil.setIntLE(array, idx(index), value);
      }
   }

   @Override
   protected void _setLong(int index, long value) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setLong(addr(index), value);
      } else {
         UnsafeByteBufUtil.setLong(array, idx(index), value);
      }
   }

   @Override
   protected void _setLongLE(int index, long value) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setLongLE(addr(index), value);
      } else {
         UnsafeByteBufUtil.setLongLE(array, idx(index), value);
      }
   }

   @Override
   public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setBytes(this, addr(index), index, src, srcIndex, length);
      } else {
         final int idx = idx(index);
         checkSrcIndex(idx, length, srcIndex, src.capacity());
         setBytes(array, idx, src, srcIndex, length);
      }
      return this;
   }

   private static void setBytes(byte[] array, int idx, ByteBuf src, int srcIndex, int length) {
      if (src.hasMemoryAddress()) {
         PlatformDependent.copyMemory(src.memoryAddress() + srcIndex, array, idx, length);
      } else if (src.hasArray()) {
         System.arraycopy(src.array(), src.arrayOffset() + srcIndex, array, idx, length);
      } else {
         src.getBytes(srcIndex, array, idx, length);
      }
   }

   @Override
   public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setBytes(this, addr(index), index, src, srcIndex, length);
      } else {
         final int idx = idx(index);
         checkSrcIndex(idx, length, srcIndex, src.length);
         System.arraycopy(src, srcIndex, array, idx, length);
      }
      return this;
   }

   @Override
   public ByteBuf setBytes(int index, ByteBuffer src) {
      if (hasMemoryAddress()) {
         UnsafeByteBufUtil.setBytes(this, addr(index), index, src);
      } else {
         final int idx = idx(index);
         checkSrcIndex(idx, src.remaining(), src.position(), src.capacity());
         setBytes(array, idx(index), src);
      }
      return this;
   }

   private static void setBytes(byte[] array, int idx, ByteBuffer src) {
      final int length = src.remaining();
      if (length == 0) {
         return;
      }
      if (src.isDirect()) {
         // Copy from direct memory
         final long srcAddress = PlatformDependent.directBufferAddress(src);
         PlatformDependent.copyMemory(srcAddress + src.position(), array, idx, length);
         src.position(src.position() + length);
      } else if (src.hasArray()) {
         // Copy from array
         System.arraycopy(src.array(), src.arrayOffset() + src.position(), array, idx, length);
         src.position(src.position() + length);
      } else {
         src.get(array, idx, src.remaining());
      }
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
      return buffer == null ? 0 : 1;
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

   private int idx(int index) {
      return arrayOffset + index;
   }

   @Override
   @Deprecated
   protected SwappedByteBuf newSwappedByteBuf() {
      throw new UnsupportedOperationException("unsupported!");
   }

   @Override
   public ByteBuf setZero(int index, int length) {
      if (hasMemoryAddress()) {
         if (length == 0) {
            return this;
         }
         this.checkIndex(index, length);
         PlatformDependent.setMemory(addr(index), length, ZERO);
      } else {
         //prefer Arrays::fill here?
         UnsafeByteBufUtil.setZero(array, idx(index), length);
      }
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

