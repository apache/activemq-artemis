/**
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
package org.apache.activemq.core.buffers.impl;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.utils.DataConstants;
import org.apache.activemq.utils.UTF8Util;

/**
 * A ChannelBufferWrapper
 *
 * @author Tim Fox
 */
public class ChannelBufferWrapper implements ActiveMQBuffer
{
   protected ByteBuf buffer; // NO_UCD (use final)
   private final boolean releasable;

   public static ByteBuf unwrap(ByteBuf buffer)
   {
      ByteBuf parent;
      while ((parent = buffer.unwrap()) != null &&
              parent != buffer) // this last part is just in case the semantic
      {                         // ever changes where unwrap is returning itself
         buffer = parent;
      }

      return buffer;
   }

   public ChannelBufferWrapper(final ByteBuf buffer)
   {
      this(buffer, false);
   }

   public ChannelBufferWrapper(final ByteBuf buffer, boolean releasable)
   {
      if (!releasable)
      {
         this.buffer = Unpooled.unreleasableBuffer(buffer);
      }
      else
      {
         this.buffer = buffer;
      }
      this.releasable = releasable;
   }

   public boolean readBoolean()
   {
      return readByte() != 0;
   }

   public SimpleString readNullableSimpleString()
   {
      int b = buffer.readByte();
      if (b == DataConstants.NULL)
      {
         return null;
      }
      return readSimpleStringInternal();
   }

   public String readNullableString()
   {
      int b = buffer.readByte();
      if (b == DataConstants.NULL)
      {
         return null;
      }
      return readStringInternal();
   }

   public SimpleString readSimpleString()
   {
      return readSimpleStringInternal();
   }

   private SimpleString readSimpleStringInternal()
   {
      int len = buffer.readInt();
      byte[] data = new byte[len];
      buffer.readBytes(data);
      return new SimpleString(data);
   }

   public String readString()
   {
      return readStringInternal();
   }

   private String readStringInternal()
   {
      int len = buffer.readInt();

      if (len < 9)
      {
         char[] chars = new char[len];
         for (int i = 0; i < len; i++)
         {
            chars[i] = (char)buffer.readShort();
         }
         return new String(chars);
      }
      else if (len < 0xfff)
      {
         return readUTF();
      }
      else
      {
         return readSimpleStringInternal().toString();
      }
   }

   public String readUTF()
   {
      return UTF8Util.readUTF(this);
   }

   public void writeBoolean(final boolean val)
   {
      buffer.writeByte((byte)(val ? -1 : 0));
   }

   public void writeNullableSimpleString(final SimpleString val)
   {
      if (val == null)
      {
         buffer.writeByte(DataConstants.NULL);
      }
      else
      {
         buffer.writeByte(DataConstants.NOT_NULL);
         writeSimpleStringInternal(val);
      }
   }

   public void writeNullableString(final String val)
   {
      if (val == null)
      {
         buffer.writeByte(DataConstants.NULL);
      }
      else
      {
         buffer.writeByte(DataConstants.NOT_NULL);
         writeStringInternal(val);
      }
   }

   public void writeSimpleString(final SimpleString val)
   {
      writeSimpleStringInternal(val);
   }

   private void writeSimpleStringInternal(final SimpleString val)
   {
      byte[] data = val.getData();
      buffer.writeInt(data.length);
      buffer.writeBytes(data);
   }

   public void writeString(final String val)
   {
      writeStringInternal(val);
   }

   private void writeStringInternal(final String val)
   {
      int length = val.length();

      buffer.writeInt(length);

      if (length < 9)
      {
         // If very small it's more performant to store char by char
         for (int i = 0; i < val.length(); i++)
         {
            buffer.writeShort((short)val.charAt(i));
         }
      }
      else if (length < 0xfff)
      {
         // Store as UTF - this is quicker than char by char for most strings
         writeUTF(val);
      }
      else
      {
         // Store as SimpleString, since can't store utf > 0xffff in length
         writeSimpleStringInternal(new SimpleString(val));
      }
   }

   public void writeUTF(final String utf)
   {
      UTF8Util.saveUTF(this, utf);
   }

   public int capacity()
   {
      return buffer.capacity();
   }

   public ByteBuf byteBuf()
   {
      return buffer;
   }

   public void clear()
   {
      buffer.clear();
   }

   public ActiveMQBuffer copy()
   {
      return new ChannelBufferWrapper(buffer.copy(), releasable);
   }

   public ActiveMQBuffer copy(final int index, final int length)
   {
      return new ChannelBufferWrapper(buffer.copy(index, length), releasable);
   }

   public void discardReadBytes()
   {
      buffer.discardReadBytes();
   }

   public ActiveMQBuffer duplicate()
   {
      return new ChannelBufferWrapper(buffer.duplicate(), releasable);
   }

   public byte getByte(final int index)
   {
      return buffer.getByte(index);
   }

   public void getBytes(final int index, final byte[] dst, final int dstIndex, final int length)
   {
      buffer.getBytes(index, dst, dstIndex, length);
   }

   public void getBytes(final int index, final byte[] dst)
   {
      buffer.getBytes(index, dst);
   }

   public void getBytes(final int index, final ByteBuffer dst)
   {
      buffer.getBytes(index, dst);
   }

   public void getBytes(final int index, final ActiveMQBuffer dst, final int dstIndex, final int length)
   {
      buffer.getBytes(index, dst.byteBuf(), dstIndex, length);
   }

   public void getBytes(final int index, final ActiveMQBuffer dst, final int length)
   {
      buffer.getBytes(index, dst.byteBuf(), length);
   }

   public void getBytes(final int index, final ActiveMQBuffer dst)
   {
      buffer.getBytes(index, dst.byteBuf());
   }

   public char getChar(final int index)
   {
      return (char)buffer.getShort(index);
   }

   public double getDouble(final int index)
   {
      return Double.longBitsToDouble(buffer.getLong(index));
   }

   public float getFloat(final int index)
   {
      return Float.intBitsToFloat(buffer.getInt(index));
   }

   public int getInt(final int index)
   {
      return buffer.getInt(index);
   }

   public long getLong(final int index)
   {
      return buffer.getLong(index);
   }

   public short getShort(final int index)
   {
      return buffer.getShort(index);
   }

   public short getUnsignedByte(final int index)
   {
      return buffer.getUnsignedByte(index);
   }

   public long getUnsignedInt(final int index)
   {
      return buffer.getUnsignedInt(index);
   }

   public int getUnsignedShort(final int index)
   {
      return buffer.getUnsignedShort(index);
   }

   public void markReaderIndex()
   {
      buffer.markReaderIndex();
   }

   public void markWriterIndex()
   {
      buffer.markWriterIndex();
   }

   public boolean readable()
   {
      return buffer.isReadable();
   }

   public int readableBytes()
   {
      return buffer.readableBytes();
   }

   public byte readByte()
   {
      return buffer.readByte();
   }

   public void readBytes(final byte[] dst, final int dstIndex, final int length)
   {
      buffer.readBytes(dst, dstIndex, length);
   }

   public void readBytes(final byte[] dst)
   {
      buffer.readBytes(dst);
   }

   public void readBytes(final ByteBuffer dst)
   {
      buffer.readBytes(dst);
   }

   public void readBytes(final ActiveMQBuffer dst, final int dstIndex, final int length)
   {
      buffer.readBytes(dst.byteBuf(), dstIndex, length);
   }

   public void readBytes(final ActiveMQBuffer dst, final int length)
   {
      buffer.readBytes(dst.byteBuf(), length);
   }

   public void readBytes(final ActiveMQBuffer dst)
   {
      buffer.readBytes(dst.byteBuf());
   }

   public ActiveMQBuffer readBytes(final int length)
   {
      return new ChannelBufferWrapper(buffer.readBytes(length), releasable);
   }

   public char readChar()
   {
      return (char)buffer.readShort();
   }

   public double readDouble()
   {
      return Double.longBitsToDouble(buffer.readLong());
   }

   public int readerIndex()
   {
      return buffer.readerIndex();
   }

   public void readerIndex(final int readerIndex)
   {
      buffer.readerIndex(readerIndex);
   }

   public float readFloat()
   {
      return Float.intBitsToFloat(buffer.readInt());
   }

   public int readInt()
   {
      return buffer.readInt();
   }

   public long readLong()
   {
      return buffer.readLong();
   }

   public short readShort()
   {
      return buffer.readShort();
   }

   public ActiveMQBuffer readSlice(final int length)
   {
      return new ChannelBufferWrapper(buffer.readSlice(length), releasable);
   }

   public short readUnsignedByte()
   {
      return buffer.readUnsignedByte();
   }

   public long readUnsignedInt()
   {
      return buffer.readUnsignedInt();
   }

   public int readUnsignedShort()
   {
      return buffer.readUnsignedShort();
   }

   public void resetReaderIndex()
   {
      buffer.resetReaderIndex();
   }

   public void resetWriterIndex()
   {
      buffer.resetWriterIndex();
   }

   public void setByte(final int index, final byte value)
   {
      buffer.setByte(index, value);
   }

   public void setBytes(final int index, final byte[] src, final int srcIndex, final int length)
   {
      buffer.setBytes(index, src, srcIndex, length);
   }

   public void setBytes(final int index, final byte[] src)
   {
      buffer.setBytes(index, src);
   }

   public void setBytes(final int index, final ByteBuffer src)
   {
      buffer.setBytes(index, src);
   }

   public void setBytes(final int index, final ActiveMQBuffer src, final int srcIndex, final int length)
   {
      buffer.setBytes(index, src.byteBuf(), srcIndex, length);
   }

   public void setBytes(final int index, final ActiveMQBuffer src, final int length)
   {
      buffer.setBytes(index, src.byteBuf(), length);
   }

   public void setBytes(final int index, final ActiveMQBuffer src)
   {
      buffer.setBytes(index, src.byteBuf());
   }

   public void setChar(final int index, final char value)
   {
      buffer.setShort(index, (short)value);
   }

   public void setDouble(final int index, final double value)
   {
      buffer.setLong(index, Double.doubleToLongBits(value));
   }

   public void setFloat(final int index, final float value)
   {
      buffer.setInt(index, Float.floatToIntBits(value));
   }

   public void setIndex(final int readerIndex, final int writerIndex)
   {
      buffer.setIndex(readerIndex, writerIndex);
   }

   public void setInt(final int index, final int value)
   {
      buffer.setInt(index, value);
   }

   public void setLong(final int index, final long value)
   {
      buffer.setLong(index, value);
   }

   public void setShort(final int index, final short value)
   {
      buffer.setShort(index, value);
   }

   public void skipBytes(final int length)
   {
      buffer.skipBytes(length);
   }

   public ActiveMQBuffer slice()
   {
      return new ChannelBufferWrapper(buffer.slice(), releasable);
   }

   public ActiveMQBuffer slice(final int index, final int length)
   {
      return new ChannelBufferWrapper(buffer.slice(index, length), releasable);
   }

   public ByteBuffer toByteBuffer()
   {
      return buffer.nioBuffer();
   }

   public ByteBuffer toByteBuffer(final int index, final int length)
   {
      return buffer.nioBuffer(index, length);
   }

   public boolean writable()
   {
      return buffer.isWritable();
   }

   public int writableBytes()
   {
      return buffer.writableBytes();
   }

   public void writeByte(final byte value)
   {
      buffer.writeByte(value);
   }

   public void writeBytes(final byte[] src, final int srcIndex, final int length)
   {
      buffer.writeBytes(src, srcIndex, length);
   }

   public void writeBytes(final byte[] src)
   {
      buffer.writeBytes(src);
   }

   public void writeBytes(final ByteBuffer src)
   {
      buffer.writeBytes(src);
   }

   public void writeBytes(final ActiveMQBuffer src, final int srcIndex, final int length)
   {
      buffer.writeBytes(src.byteBuf(), srcIndex, length);
   }

   public void writeBytes(final ActiveMQBuffer src, final int length)
   {
      buffer.writeBytes(src.byteBuf(), length);
   }

   public void writeChar(final char chr)
   {
      buffer.writeShort((short)chr);
   }

   public void writeDouble(final double value)
   {
      buffer.writeLong(Double.doubleToLongBits(value));
   }

   public void writeFloat(final float value)
   {
      buffer.writeInt(Float.floatToIntBits(value));
   }

   public void writeInt(final int value)
   {
      buffer.writeInt(value);
   }

   public void writeLong(final long value)
   {
      buffer.writeLong(value);
   }

   public int writerIndex()
   {
      return buffer.writerIndex();
   }

   public void writerIndex(final int writerIndex)
   {
      buffer.writerIndex(writerIndex);
   }

   public void writeShort(final short value)
   {
      buffer.writeShort(value);
   }

}
