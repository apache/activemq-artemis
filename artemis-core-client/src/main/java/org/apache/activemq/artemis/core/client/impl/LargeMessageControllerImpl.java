/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.client.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.UTF8Util;

public final class LargeMessageControllerImpl extends LargeMessageControllerBaseImpl {

   private static final String READ_ONLY_ERROR_MESSAGE = "This is a read-only buffer, setOperations are not supported";
   private long readerIndex = 0;

   public LargeMessageControllerImpl(ClientConsumerInternal consumerInternal, long totalSize, long readTimeout) {
      super(consumerInternal, totalSize, readTimeout);
   }

   public LargeMessageControllerImpl(ClientConsumerInternal consumerInternal,
                                     long totalSize,
                                     long readTimeout,
                                     File cachedFile) {
      super(consumerInternal, totalSize, readTimeout, cachedFile);
   }

   public LargeMessageControllerImpl(ClientConsumerInternal consumerInternal,
                                     long totalSize,
                                     long readTimeout,
                                     File cachedFile,
                                     int bufferSize) {
      super(consumerInternal, totalSize, readTimeout, cachedFile, bufferSize);
   }

   // Channel Buffer Implementation ---------------------------------

   @Override
   public int capacity() {
      return -1;
   }

   @Override
   public byte readByte() {
      return getByte(readerIndex++);
   }

   @Override
   public byte getByte(final int index) {
      return getByte((long) index);
   }

   @Override
   public void getBytes(final int index, final ActiveMQBuffer dst, final int dstIndex, final int length) {
      getBytes((long) index, dst, dstIndex, length);
   }

   private void getBytes(final long index, final ActiveMQBuffer dst, final int dstIndex, final int length) {
      byte[] destBytes = new byte[length];
      getBytes(index, destBytes);
      dst.setBytes(dstIndex, destBytes);
   }

   @Override
   public void getBytes(final int index, final byte[] dst, final int dstIndex, final int length) {
      getBytes((long) index, dst, dstIndex, length);
   }

   private void getBytes(final long index, final byte[] dst, final int dstIndex, final int length) {
      byte[] bytesToGet = new byte[length];

      getBytes(index, bytesToGet);

      System.arraycopy(bytesToGet, 0, dst, dstIndex, length);
   }

   @Override
   public void getBytes(final int index, final ByteBuffer dst) {
      getBytes((long) index, dst);
   }

   private void getBytes(final long index, final ByteBuffer dst) {
      byte[] bytesToGet = new byte[dst.remaining()];
      getBytes(index, bytesToGet);
      dst.put(bytesToGet);
   }

   @Override
   public int getInt(final int index) {
      return getInt((long) index);
   }

   private int getInt(final long index) {
      return (getByte(index) & 0xff) << 24 | (getByte(index + 1) & 0xff) << 16 |
         (getByte(index + 2) & 0xff) << 8 |
         (getByte(index + 3) & 0xff) << 0;
   }

   @Override
   public long getLong(final int index) {
      return getLong((long) index);
   }

   private long getLong(final long index) {
      return ((long) getByte(index) & 0xff) << 56 | ((long) getByte(index + 1) & 0xff) << 48 |
         ((long) getByte(index + 2) & 0xff) << 40 |
         ((long) getByte(index + 3) & 0xff) << 32 |
         ((long) getByte(index + 4) & 0xff) << 24 |
         ((long) getByte(index + 5) & 0xff) << 16 |
         ((long) getByte(index + 6) & 0xff) << 8 |
         ((long) getByte(index + 7) & 0xff) << 0;
   }

   @Override
   public short getShort(final int index) {
      return getShort((long) index);
   }

   private short getShort(final long index) {
      return (short) (getByte(index) << 8 | getByte(index + 1) & 0xFF);
   }

   @Override
   public void setByte(final int index, final byte value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setBytes(final int index, final ActiveMQBuffer src, final int srcIndex, final int length) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setBytes(final int index, final byte[] src, final int srcIndex, final int length) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setBytes(final int index, final ByteBuffer src) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setInt(final int index, final int value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setLong(final int index, final long value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setShort(final int index, final short value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public ByteBuffer toByteBuffer(final int index, final int length) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void release() {
      //no-op
   }

   @Override
   public int readerIndex() {
      return (int) readerIndex;
   }

   @Override
   public void readerIndex(final int readerIndex) {
      try {
         checkForPacket(readerIndex);
      } catch (Exception e) {
         ActiveMQClientLogger.LOGGER.errorReadingIndex(e);
         throw new RuntimeException(e.getMessage(), e);
      }
      this.readerIndex = readerIndex;
   }

   @Override
   public int writerIndex() {
      return (int) getSize();
   }

   @Override
   public void writerIndex(final int writerIndex) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setIndex(final int readerIndex, final int writerIndex) {
      try {
         checkForPacket(readerIndex);
      } catch (Exception e) {
         ActiveMQClientLogger.LOGGER.errorSettingIndex(e);
         throw new RuntimeException(e.getMessage(), e);
      }
      this.readerIndex = readerIndex;
   }

   @Override
   public void clear() {
   }

   @Override
   public boolean readable() {
      return true;
   }

   @Override
   public boolean writable() {
      return false;
   }

   @Override
   public int readableBytes() {
      long readableBytes = getSize() - readerIndex;

      if (readableBytes > Integer.MAX_VALUE) {
         return Integer.MAX_VALUE;
      } else {
         return (int) (getSize() - readerIndex);
      }
   }

   @Override
   public int writableBytes() {
      return 0;
   }

   @Override
   public void markReaderIndex() {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void resetReaderIndex() {
      try {
         checkForPacket(0);
      } catch (Exception e) {
         ActiveMQClientLogger.LOGGER.errorReSettingIndex(e);
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   @Override
   public void markWriterIndex() {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void resetWriterIndex() {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void discardReadBytes() {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public short getUnsignedByte(final int index) {
      return (short) (getByte(index) & 0xFF);
   }

   @Override
   public int getUnsignedShort(final int index) {
      return getShort(index) & 0xFFFF;
   }

   @Override
   public long getUnsignedInt(final int index) {
      return getInt(index) & 0xFFFFFFFFL;
   }

   @Override
   public void getBytes(int index, final byte[] dst) {
      getBytes((long) index, dst);
   }

   private void getBytes(long index, final byte[] dst) {
      // TODO: optimize this by using System.arraycopy
      for (int i = 0; i < dst.length; i++) {
         dst[i] = getByte(index++);
      }
   }

   @Override
   public void getBytes(final int index, final ActiveMQBuffer dst) {
      getBytes(index, dst, dst.writableBytes());
   }

   @Override
   public void getBytes(final int index, final ActiveMQBuffer dst, final int length) {
      if (length > dst.writableBytes()) {
         throw new IndexOutOfBoundsException();
      }
      getBytes(index, dst, dst.writerIndex(), length);
      dst.writerIndex(dst.writerIndex() + length);
   }

   @Override
   public void setBytes(final int index, final byte[] src) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setBytes(final int index, final ActiveMQBuffer src) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setBytes(final int index, final ActiveMQBuffer src, final int length) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public int readUnsignedByte() {
      return (short) (readByte() & 0xFF);
   }

   @Override
   public short readShort() {
      short v = getShort(readerIndex);
      readerIndex += 2;
      return v;
   }

   @Override
   public int readUnsignedShort() {
      return readShort() & 0xFFFF;
   }

   @Override
   public int readInt() {
      int v = getInt(readerIndex);
      readerIndex += 4;
      return v;
   }

   @Override
   public Integer readNullableInt() {
      int b = readByte();
      if (b == DataConstants.NULL) {
         return null;
      } else {
         return readInt();
      }
   }

   @Override
   public long readUnsignedInt() {
      return readInt() & 0xFFFFFFFFL;
   }

   @Override
   public long readLong() {
      long v = getLong(readerIndex);
      readerIndex += 8;
      return v;
   }

   @Override
   public Long readNullableLong() {
      int b = readByte();
      if (b == DataConstants.NULL) {
         return null;
      } else {
         return readLong();
      }
   }

   @Override
   public void readBytes(final byte[] dst, final int dstIndex, final int length) {
      getBytes(readerIndex, dst, dstIndex, length);
      readerIndex += length;
   }

   @Override
   public void readBytes(final byte[] dst) {
      readBytes(dst, 0, dst.length);
   }

   @Override
   public void readBytes(final ActiveMQBuffer dst) {
      readBytes(dst, dst.writableBytes());
   }

   @Override
   public void readBytes(final ActiveMQBuffer dst, final int length) {
      if (length > dst.writableBytes()) {
         throw new IndexOutOfBoundsException();
      }
      readBytes(dst, dst.writerIndex(), length);
      dst.writerIndex(dst.writerIndex() + length);
   }

   @Override
   public void readBytes(final ActiveMQBuffer dst, final int dstIndex, final int length) {
      getBytes(readerIndex, dst, dstIndex, length);
      readerIndex += length;
   }

   @Override
   public void readBytes(final ByteBuffer dst) {
      int length = dst.remaining();
      getBytes(readerIndex, dst);
      readerIndex += length;
   }

   @Override
   public int skipBytes(final int length) {

      long newReaderIndex = readerIndex + length;
      checkForPacket(newReaderIndex);
      readerIndex = newReaderIndex;
      return length;
   }

   @Override
   public void writeByte(final byte value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeShort(final short value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeInt(final int value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeNullableInt(final Integer value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeLong(final long value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeNullableLong(final Long value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeBytes(final byte[] src, final int srcIndex, final int length) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeBytes(final byte[] src) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeBytes(final ActiveMQBuffer src, final int length) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeBytes(final ByteBuffer src) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   /**
    * Transfers the specified source buffer's data to this buffer starting at
    * the current {@code writerIndex} until the source buffer's position
    * reaches its limit, and increases the {@code writerIndex} by the
    * number of the transferred bytes.
    *
    * @param src The source buffer
    * @throws IndexOutOfBoundsException if {@code src.remaining()} is greater than
    *                                   {@code this.writableBytes}
    */
   @Override
   public void writeBytes(ByteBuf src, int srcIndex, int length) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public ByteBuffer toByteBuffer() {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public boolean readBoolean() {
      return readByte() != 0;
   }

   @Override
   public Boolean readNullableBoolean() {
      int b = readByte();
      if (b == DataConstants.NULL) {
         return null;
      } else {
         return readBoolean();
      }
   }

   @Override
   public char readChar() {
      return (char) readShort();
   }

   @Override
   public char getChar(final int index) {
      return (char) getShort(index);
   }

   @Override
   public double getDouble(final int index) {
      return Double.longBitsToDouble(getLong(index));
   }

   @Override
   public float getFloat(final int index) {
      return Float.intBitsToFloat(getInt(index));
   }

   @Override
   public double readDouble() {
      return Double.longBitsToDouble(readLong());
   }

   @Override
   public float readFloat() {
      return Float.intBitsToFloat(readInt());
   }

   @Override
   public SimpleString readNullableSimpleString() {
      int b = readByte();
      if (b == DataConstants.NULL) {
         return null;
      } else {
         return readSimpleString();
      }
   }

   @Override
   public String readNullableString() {
      int b = readByte();
      if (b == DataConstants.NULL) {
         return null;
      } else {
         return readString();
      }
   }

   @Override
   public SimpleString readSimpleString() {
      int len = readInt();
      byte[] data = new byte[len];
      readBytes(data);
      return new SimpleString(data);
   }

   @Override
   public String readString() {
      int len = readInt();

      if (len < 9) {
         char[] chars = new char[len];
         for (int i = 0; i < len; i++) {
            chars[i] = (char) readShort();
         }
         return new String(chars);
      } else if (len < 0xfff) {
         return readUTF();
      } else {
         return readSimpleString().toString();
      }
   }

   @Override
   public String readUTF() {
      return UTF8Util.readUTF(this);
   }

   @Override
   public void writeBoolean(final boolean val) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeNullableBoolean(final Boolean val) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeChar(final char val) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeDouble(final double val) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);

   }

   @Override
   public void writeFloat(final float val) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);

   }

   @Override
   public void writeNullableSimpleString(final SimpleString val) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeNullableString(final String val) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeSimpleString(final SimpleString val) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeString(final String val) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeUTF(final String utf) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public ActiveMQBuffer copy() {
      throw new UnsupportedOperationException();
   }

   @Override
   public ActiveMQBuffer slice(final int index, final int length) {
      throw new UnsupportedOperationException();
   }

   /**
    * from {@link java.io.DataInput} interface
    */
   @Override
   public void readFully(byte[] b) throws IOException {
      readBytes(b);
   }

   /**
    * from {@link java.io.DataInput} interface
    */
   @Override
   public void readFully(byte[] b, int off, int len) throws IOException {
      readBytes(b, off, len);
   }

   /**
    * from {@link java.io.DataInput} interface
    */
   @Override
   public String readLine() throws IOException {
      return ByteUtil.readLine(this);
   }

   @Override
   public ByteBuf byteBuf() {
      return null;
   }

   @Override
   public ActiveMQBuffer copy(final int index, final int length) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public ActiveMQBuffer duplicate() {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public ActiveMQBuffer readSlice(final int length) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setChar(final int index, final char value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setDouble(final int index, final double value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setFloat(final int index, final float value) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public ActiveMQBuffer slice() {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeBytes(final ActiveMQBuffer src, final int srcIndex, final int length) {
      throw new IllegalAccessError(READ_ONLY_ERROR_MESSAGE);
   }

}