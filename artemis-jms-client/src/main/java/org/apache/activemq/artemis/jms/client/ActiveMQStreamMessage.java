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
package org.apache.activemq.artemis.jms.client;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.utils.DataConstants;

import static org.apache.activemq.artemis.reader.StreamMessageUtil.streamReadBoolean;
import static org.apache.activemq.artemis.reader.StreamMessageUtil.streamReadByte;
import static org.apache.activemq.artemis.reader.StreamMessageUtil.streamReadBytes;
import static org.apache.activemq.artemis.reader.StreamMessageUtil.streamReadChar;
import static org.apache.activemq.artemis.reader.StreamMessageUtil.streamReadDouble;
import static org.apache.activemq.artemis.reader.StreamMessageUtil.streamReadFloat;
import static org.apache.activemq.artemis.reader.StreamMessageUtil.streamReadInteger;
import static org.apache.activemq.artemis.reader.StreamMessageUtil.streamReadLong;
import static org.apache.activemq.artemis.reader.StreamMessageUtil.streamReadObject;
import static org.apache.activemq.artemis.reader.StreamMessageUtil.streamReadShort;
import static org.apache.activemq.artemis.reader.StreamMessageUtil.streamReadString;

/**
 * ActiveMQ Artemis implementation of a JMS StreamMessage.
 */
public final class ActiveMQStreamMessage extends ActiveMQMessage implements StreamMessage
{
   public static final byte TYPE = Message.STREAM_TYPE;

   protected ActiveMQStreamMessage(final ClientSession session)
   {
      super(ActiveMQStreamMessage.TYPE, session);
   }

   protected ActiveMQStreamMessage(final ClientMessage message, final ClientSession session)
   {
      super(message, session);
   }

   public ActiveMQStreamMessage(final StreamMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, ActiveMQStreamMessage.TYPE, session);

      foreign.reset();

      try
      {
         while (true)
         {
            Object obj = foreign.readObject();
            writeObject(obj);
         }
      }
      catch (MessageEOFException e)
      {
         // Ignore
      }
   }

   // For testing only
   public ActiveMQStreamMessage()
   {
      message = new ClientMessageImpl((byte)0, false, 0, 0, (byte)4, 1500);
   }

   // Public --------------------------------------------------------

   @Override
   public byte getType()
   {
      return ActiveMQStreamMessage.TYPE;
   }

   // StreamMessage implementation ----------------------------------

   public boolean readBoolean() throws JMSException
   {
      checkRead();
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
      checkRead();

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
      checkRead();
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
      checkRead();
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
      checkRead();
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
      checkRead();
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
      checkRead();
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
      checkRead();
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
      checkRead();
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
      checkRead();
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
      checkRead();
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
      checkWrite();
      getBuffer().writeByte(DataConstants.BOOLEAN);
      getBuffer().writeBoolean(value);
   }

   public void writeByte(final byte value) throws JMSException
   {
      checkWrite();
      getBuffer().writeByte(DataConstants.BYTE);
      getBuffer().writeByte(value);
   }

   public void writeShort(final short value) throws JMSException
   {
      checkWrite();
      getBuffer().writeByte(DataConstants.SHORT);
      getBuffer().writeShort(value);
   }

   public void writeChar(final char value) throws JMSException
   {
      checkWrite();
      getBuffer().writeByte(DataConstants.CHAR);
      getBuffer().writeShort((short)value);
   }

   public void writeInt(final int value) throws JMSException
   {
      checkWrite();
      getBuffer().writeByte(DataConstants.INT);
      getBuffer().writeInt(value);
   }

   public void writeLong(final long value) throws JMSException
   {
      checkWrite();
      getBuffer().writeByte(DataConstants.LONG);
      getBuffer().writeLong(value);
   }

   public void writeFloat(final float value) throws JMSException
   {
      checkWrite();
      getBuffer().writeByte(DataConstants.FLOAT);
      getBuffer().writeInt(Float.floatToIntBits(value));
   }

   public void writeDouble(final double value) throws JMSException
   {
      checkWrite();
      getBuffer().writeByte(DataConstants.DOUBLE);
      getBuffer().writeLong(Double.doubleToLongBits(value));
   }

   public void writeString(final String value) throws JMSException
   {
      checkWrite();
      getBuffer().writeByte(DataConstants.STRING);
      getBuffer().writeNullableString(value);
   }

   public void writeBytes(final byte[] value) throws JMSException
   {
      checkWrite();
      getBuffer().writeByte(DataConstants.BYTES);
      getBuffer().writeInt(value.length);
      getBuffer().writeBytes(value);
   }

   public void writeBytes(final byte[] value, final int offset, final int length) throws JMSException
   {
      checkWrite();
      getBuffer().writeByte(DataConstants.BYTES);
      getBuffer().writeInt(length);
      getBuffer().writeBytes(value, offset, length);
   }

   public void writeObject(final Object value) throws JMSException
   {
      if (value instanceof String)
      {
         writeString((String)value);
      }
      else if (value instanceof Boolean)
      {
         writeBoolean((Boolean)value);
      }
      else if (value instanceof Byte)
      {
         writeByte((Byte)value);
      }
      else if (value instanceof Short)
      {
         writeShort((Short)value);
      }
      else if (value instanceof Integer)
      {
         writeInt((Integer)value);
      }
      else if (value instanceof Long)
      {
         writeLong((Long)value);
      }
      else if (value instanceof Float)
      {
         writeFloat((Float)value);
      }
      else if (value instanceof Double)
      {
         writeDouble((Double)value);
      }
      else if (value instanceof byte[])
      {
         writeBytes((byte[])value);
      }
      else if (value instanceof Character)
      {
         writeChar((Character)value);
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
      if (!readOnly)
      {
         readOnly = true;
      }
      getBuffer().resetReaderIndex();
   }

   // ActiveMQRAMessage overrides ----------------------------------------

   @Override
   public void clearBody() throws JMSException
   {
      super.clearBody();

      getBuffer().clear();
   }

   @Override
   public void doBeforeSend() throws Exception
   {
      reset();
   }

   private ActiveMQBuffer getBuffer()
   {
      return message.getBodyBuffer();
   }

   @SuppressWarnings("rawtypes")
   @Override
   public boolean isBodyAssignableTo(Class c)
   {
      return false;
   }
}
