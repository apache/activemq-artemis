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
package org.apache.activemq.core.protocol.proton.converter.jms;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;

import org.apache.activemq.api.core.HornetQBuffer;
import org.apache.activemq.api.core.Message;
import org.apache.activemq.api.core.Pair;
import org.apache.activemq.core.message.impl.MessageInternal;
import org.apache.activemq.utils.DataConstants;

import static org.apache.activemq.reader.MessageUtil.getBodyBuffer;
import static org.apache.activemq.reader.StreamMessageUtil.streamReadBoolean;
import static org.apache.activemq.reader.StreamMessageUtil.streamReadByte;
import static org.apache.activemq.reader.StreamMessageUtil.streamReadBytes;
import static org.apache.activemq.reader.StreamMessageUtil.streamReadChar;
import static org.apache.activemq.reader.StreamMessageUtil.streamReadDouble;
import static org.apache.activemq.reader.StreamMessageUtil.streamReadFloat;
import static org.apache.activemq.reader.StreamMessageUtil.streamReadInteger;
import static org.apache.activemq.reader.StreamMessageUtil.streamReadLong;
import static org.apache.activemq.reader.StreamMessageUtil.streamReadObject;
import static org.apache.activemq.reader.StreamMessageUtil.streamReadShort;
import static org.apache.activemq.reader.StreamMessageUtil.streamReadString;

public final class ServerJMSStreamMessage extends ServerJMSMessage implements StreamMessage
{
   public static final byte TYPE = Message.STREAM_TYPE;

   private int bodyLength = 0;


   public ServerJMSStreamMessage(MessageInternal message, int deliveryCount)
   {
      super(message, deliveryCount);

   }

   // StreamMessage implementation ----------------------------------

   public boolean readBoolean() throws JMSException
   {
      try
      {
         return streamReadBoolean(message);
      }
      catch (IllegalStateException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public byte readByte() throws JMSException
   {
      try
      {
         return streamReadByte(message);
      }
      catch (IllegalStateException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public short readShort() throws JMSException
   {

      try
      {
         return streamReadShort(message);
      }
      catch (IllegalStateException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public char readChar() throws JMSException
   {

      try
      {
         return streamReadChar(message);
      }
      catch (IllegalStateException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readInt() throws JMSException
   {

      try
      {
         return streamReadInteger(message);
      }
      catch (IllegalStateException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public long readLong() throws JMSException
   {

      try
      {
         return streamReadLong(message);
      }
      catch (IllegalStateException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public float readFloat() throws JMSException
   {

      try
      {
         return streamReadFloat(message);
      }
      catch (IllegalStateException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public double readDouble() throws JMSException
   {

      try
      {
         return streamReadDouble(message);
      }
      catch (IllegalStateException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public String readString() throws JMSException
   {

      try
      {
         return streamReadString(message);
      }
      catch (IllegalStateException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   /**
    * len here is used to control how many more bytes to read
    */
   private int len = 0;

   public int readBytes(final byte[] value) throws JMSException
   {

      try
      {
         Pair<Integer, Integer> pairRead = streamReadBytes(message, len, value);

         len = pairRead.getA();
         return pairRead.getB();
      }
      catch (IllegalStateException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public Object readObject() throws JMSException
   {

      if (getBodyBuffer(message).readerIndex() >= message.getEndOfBodyPosition())
      {
         throw new MessageEOFException("");
      }
      try
      {
         return streamReadObject(message);
      }
      catch (IllegalStateException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public void writeBoolean(final boolean value) throws JMSException
   {

      getBuffer().writeByte(DataConstants.BOOLEAN);
      getBuffer().writeBoolean(value);
   }

   public void writeByte(final byte value) throws JMSException
   {

      getBuffer().writeByte(DataConstants.BYTE);
      getBuffer().writeByte(value);
   }

   public void writeShort(final short value) throws JMSException
   {

      getBuffer().writeByte(DataConstants.SHORT);
      getBuffer().writeShort(value);
   }

   public void writeChar(final char value) throws JMSException
   {

      getBuffer().writeByte(DataConstants.CHAR);
      getBuffer().writeShort((short) value);
   }

   public void writeInt(final int value) throws JMSException
   {

      getBuffer().writeByte(DataConstants.INT);
      getBuffer().writeInt(value);
   }

   public void writeLong(final long value) throws JMSException
   {

      getBuffer().writeByte(DataConstants.LONG);
      getBuffer().writeLong(value);
   }

   public void writeFloat(final float value) throws JMSException
   {

      getBuffer().writeByte(DataConstants.FLOAT);
      getBuffer().writeInt(Float.floatToIntBits(value));
   }

   public void writeDouble(final double value) throws JMSException
   {

      getBuffer().writeByte(DataConstants.DOUBLE);
      getBuffer().writeLong(Double.doubleToLongBits(value));
   }

   public void writeString(final String value) throws JMSException
   {

      getBuffer().writeByte(DataConstants.STRING);
      getBuffer().writeNullableString(value);
   }

   public void writeBytes(final byte[] value) throws JMSException
   {

      getBuffer().writeByte(DataConstants.BYTES);
      getBuffer().writeInt(value.length);
      getBuffer().writeBytes(value);
   }

   public void writeBytes(final byte[] value, final int offset, final int length) throws JMSException
   {

      getBuffer().writeByte(DataConstants.BYTES);
      getBuffer().writeInt(length);
      getBuffer().writeBytes(value, offset, length);
   }

   public void writeObject(final Object value) throws JMSException
   {
      if (value instanceof String)
      {
         writeString((String) value);
      }
      else if (value instanceof Boolean)
      {
         writeBoolean((Boolean) value);
      }
      else if (value instanceof Byte)
      {
         writeByte((Byte) value);
      }
      else if (value instanceof Short)
      {
         writeShort((Short) value);
      }
      else if (value instanceof Integer)
      {
         writeInt((Integer) value);
      }
      else if (value instanceof Long)
      {
         writeLong((Long) value);
      }
      else if (value instanceof Float)
      {
         writeFloat((Float) value);
      }
      else if (value instanceof Double)
      {
         writeDouble((Double) value);
      }
      else if (value instanceof byte[])
      {
         writeBytes((byte[]) value);
      }
      else if (value instanceof Character)
      {
         writeChar((Character) value);
      }
      else if (value == null)
      {
         writeString(null);
      }
      else
      {
         throw new MessageFormatException("Invalid object type: " + value.getClass());
      }
   }

   public void reset() throws JMSException
   {
      getBuffer().resetReaderIndex();
   }

   // HornetQRAMessage overrides ----------------------------------------

   @Override
   public void clearBody() throws JMSException
   {
      super.clearBody();

      getBuffer().clear();
   }

   private HornetQBuffer getBuffer()
   {
      return message.getBodyBuffer();
   }


   public void decode() throws Exception
   {
      super.decode();
   }

   /**
    * Encode the body into the internal message
    */
   public void encode() throws Exception
   {
      super.encode();
      bodyLength = message.getEndOfBodyPosition();
   }

}
