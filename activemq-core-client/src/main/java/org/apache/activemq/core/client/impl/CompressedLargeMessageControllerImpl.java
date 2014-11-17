/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.core.client.impl;

import java.io.DataInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQBuffers;
import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.client.HornetQClientLogger;
import org.apache.activemq.utils.DataConstants;
import org.apache.activemq.utils.HornetQBufferInputStream;
import org.apache.activemq.utils.InflaterReader;
import org.apache.activemq.utils.InflaterWriter;
import org.apache.activemq.utils.UTF8Util;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
final class CompressedLargeMessageControllerImpl implements LargeMessageController
{
   private static final String OPERATION_NOT_SUPPORTED = "Operation not supported";

   private final LargeMessageController bufferDelegate;

   public CompressedLargeMessageControllerImpl(final LargeMessageController bufferDelegate)
   {
      this.bufferDelegate = bufferDelegate;
   }

   /**
    *
    */
   public void discardUnusedPackets()
   {
      bufferDelegate.discardUnusedPackets();
   }

   /**
    * Add a buff to the List, or save it to the OutputStream if set
    */
   public void addPacket(byte[] chunk, int flowControlSize, boolean isContinues)
   {
      bufferDelegate.addPacket(chunk, flowControlSize, isContinues);
   }

   public synchronized void cancel()
   {
      bufferDelegate.cancel();
   }

   public synchronized void close()
   {
      bufferDelegate.cancel();
   }

   public void setOutputStream(final OutputStream output) throws ActiveMQException
   {
      bufferDelegate.setOutputStream(new InflaterWriter(output));
   }

   public synchronized void saveBuffer(final OutputStream output) throws ActiveMQException
   {
      setOutputStream(output);
      waitCompletion(0);
   }

   /**
    * @param timeWait Milliseconds to Wait. 0 means forever
    */
   public synchronized boolean waitCompletion(final long timeWait) throws ActiveMQException
   {
      return bufferDelegate.waitCompletion(timeWait);
   }

   @Override
   public int capacity()
   {
      return -1;
   }

   DataInputStream dataInput = null;

   private DataInputStream getStream()
   {
      if (dataInput == null)
      {
         try
         {
            InputStream input = new HornetQBufferInputStream(bufferDelegate);

            dataInput = new DataInputStream(new InflaterReader(input));
         }
         catch (Exception e)
         {
            throw new RuntimeException(e.getMessage(), e);
         }

      }
      return dataInput;
   }

   private void positioningNotSupported()
   {
      throw new IllegalStateException("Position not supported over compressed large messages");
   }

   public byte readByte()
   {
      try
      {
         return getStream().readByte();
      }
      catch (Exception e)
      {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   @Override
   public byte getByte(final int index)
   {
      positioningNotSupported();
      return 0;
   }

   @Override
   public void getBytes(final int index, final ActiveMQBuffer dst, final int dstIndex, final int length)
   {
      positioningNotSupported();
   }

   @Override
   public void getBytes(final int index, final byte[] dst, final int dstIndex, final int length)
   {
      positioningNotSupported();
   }

   @Override
   public void getBytes(final int index, final ByteBuffer dst)
   {
      positioningNotSupported();
   }

   @Override
   public int getInt(final int index)
   {
      positioningNotSupported();
      return 0;
   }

   @Override
   public long getLong(final int index)
   {
      positioningNotSupported();
      return 0;
   }

   @Override
   public short getShort(final int index)
   {
      positioningNotSupported();
      return 0;
   }

   @Override
   public void setByte(final int index, final byte value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void setBytes(final int index, final ActiveMQBuffer src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void setBytes(final int index, final byte[] src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void setBytes(final int index, final ByteBuffer src)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void setInt(final int index, final int value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void setLong(final int index, final long value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void setShort(final int index, final short value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public ByteBuffer toByteBuffer(final int index, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public int readerIndex()
   {
      return 0;
   }

   public void readerIndex(final int readerIndex)
   {
      // TODO
   }

   public int writerIndex()
   {
      // TODO
      return 0;
   }

   public long getSize()
   {
      return this.bufferDelegate.getSize();
   }

   public void writerIndex(final int writerIndex)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setIndex(final int readerIndex, final int writerIndex)
   {
      positioningNotSupported();
   }

   public void clear()
   {
   }

   public boolean readable()
   {
      return true;
   }

   public boolean writable()
   {
      return false;
   }

   public int readableBytes()
   {
      return 1;
   }

   public int writableBytes()
   {
      return 0;
   }

   public void markReaderIndex()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void resetReaderIndex()
   {
      // TODO: reset positioning if possible
   }

   public void markWriterIndex()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void resetWriterIndex()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void discardReadBytes()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public short getUnsignedByte(final int index)
   {
      return (short) (getByte(index) & 0xFF);
   }

   public int getUnsignedShort(final int index)
   {
      return getShort(index) & 0xFFFF;
   }

   public long getUnsignedInt(final int index)
   {
      return getInt(index) & 0xFFFFFFFFL;
   }

   public void getBytes(int index, final byte[] dst)
   {
      // TODO: optimize this by using System.arraycopy
      for (int i = 0; i < dst.length; i++)
      {
         dst[i] = getByte(index++);
      }
   }

   public void getBytes(final int index, final ActiveMQBuffer dst)
   {
      getBytes(index, dst, dst.writableBytes());
   }

   public void getBytes(final int index, final ActiveMQBuffer dst, final int length)
   {
      if (length > dst.writableBytes())
      {
         throw new IndexOutOfBoundsException();
      }
      getBytes(index, dst, dst.writerIndex(), length);
      dst.writerIndex(dst.writerIndex() + length);
   }

   public void setBytes(final int index, final byte[] src)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setBytes(final int index, final ActiveMQBuffer src)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setBytes(final int index, final ActiveMQBuffer src, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public short readUnsignedByte()
   {
      try
      {
         return (short) getStream().readUnsignedByte();
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public short readShort()
   {
      try
      {
         return getStream().readShort();
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public int readUnsignedShort()
   {
      try
      {
         return getStream().readUnsignedShort();
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public int readInt()
   {
      try
      {
         return getStream().readInt();
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public long readUnsignedInt()
   {
      return readInt() & 0xFFFFFFFFL;
   }

   public long readLong()
   {
      try
      {
         return getStream().readLong();
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public void readBytes(final byte[] dst, final int dstIndex, final int length)
   {
      try
      {
         int nReadBytes = getStream().read(dst, dstIndex, length);
         if (nReadBytes < length)
         {
            HornetQClientLogger.LOGGER.compressedLargeMessageError(length, nReadBytes);
         }
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public void readBytes(final byte[] dst)
   {
      readBytes(dst, 0, dst.length);
   }

   public void readBytes(final ActiveMQBuffer dst)
   {
      readBytes(dst, dst.writableBytes());
   }

   public void readBytes(final ActiveMQBuffer dst, final int length)
   {
      if (length > dst.writableBytes())
      {
         throw new IndexOutOfBoundsException();
      }
      readBytes(dst, dst.writerIndex(), length);
      dst.writerIndex(dst.writerIndex() + length);
   }

   public void readBytes(final ActiveMQBuffer dst, final int dstIndex, final int length)
   {
      byte[] destBytes = new byte[length];
      readBytes(destBytes);
      dst.setBytes(dstIndex, destBytes);
   }

   public void readBytes(final ByteBuffer dst)
   {
      byte[] bytesToGet = new byte[dst.remaining()];
      readBytes(bytesToGet);
      dst.put(bytesToGet);
   }

   public void skipBytes(final int length)
   {

      try
      {
         for (int i = 0; i < length; i++)
         {
            getStream().read();
         }
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e.getMessage(), e);
      }
   }

   public void writeByte(final byte value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeShort(final short value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeInt(final int value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeLong(final long value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeBytes(final byte[] src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeBytes(final byte[] src)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeBytes(final ActiveMQBuffer src, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeBytes(final ByteBuffer src)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public ByteBuffer toByteBuffer()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public Object getUnderlyingBuffer()
   {
      return this;
   }

   @Override
   public boolean readBoolean()
   {
      return readByte() != 0;
   }

   @Override
   public char readChar()
   {
      return (char) readShort();
   }

   public char getChar(final int index)
   {
      return (char) getShort(index);
   }

   public double getDouble(final int index)
   {
      return Double.longBitsToDouble(getLong(index));
   }

   public float getFloat(final int index)
   {
      return Float.intBitsToFloat(getInt(index));
   }

   public ActiveMQBuffer readBytes(final int length)
   {
      byte[] bytesToGet = new byte[length];
      readBytes(bytesToGet);
      return ActiveMQBuffers.wrappedBuffer(bytesToGet);
   }

   @Override
   public double readDouble()
   {
      return Double.longBitsToDouble(readLong());
   }

   @Override
   public float readFloat()
   {
      return Float.intBitsToFloat(readInt());
   }

   @Override
   public SimpleString readNullableSimpleString()
   {
      int b = readByte();
      if (b == DataConstants.NULL)
      {
         return null;
      }
      else
      {
         return readSimpleString();
      }
   }

   @Override
   public String readNullableString()
   {
      int b = readByte();
      if (b == DataConstants.NULL)
      {
         return null;
      }
      else
      {
         return readString();
      }
   }

   @Override
   public SimpleString readSimpleString()
   {
      int len = readInt();
      byte[] data = new byte[len];
      readBytes(data);
      return new SimpleString(data);
   }

   @Override
   public String readString()
   {
      int len = readInt();

      if (len < 9)
      {
         char[] chars = new char[len];
         for (int i = 0; i < len; i++)
         {
            chars[i] = (char) readShort();
         }
         return new String(chars);
      }
      else if (len < 0xfff)
      {
         return readUTF();
      }
      else
      {
         return readSimpleString().toString();
      }
   }

   @Override
   public String readUTF()
   {
      return UTF8Util.readUTF(this);
   }

   @Override
   public void writeBoolean(final boolean val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void writeChar(final char val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void writeDouble(final double val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);

   }

   @Override
   public void writeFloat(final float val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);

   }

   @Override
   public void writeNullableSimpleString(final SimpleString val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void writeNullableString(final String val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void writeSimpleString(final SimpleString val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void writeString(final String val)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   @Override
   public void writeUTF(final String utf)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public ActiveMQBuffer copy()
   {
      throw new UnsupportedOperationException();
   }

   public ActiveMQBuffer slice(final int index, final int length)
   {
      throw new UnsupportedOperationException();
   }

   // Inner classes -------------------------------------------------

   public ByteBuf byteBuf()
   {
      return null;
   }

   public ActiveMQBuffer copy(final int index, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public ActiveMQBuffer duplicate()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public ActiveMQBuffer readSlice(final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setChar(final int index, final char value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setDouble(final int index, final double value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void setFloat(final int index, final float value)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public ActiveMQBuffer slice()
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }

   public void writeBytes(final ActiveMQBuffer src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(OPERATION_NOT_SUPPORTED);
   }
}
