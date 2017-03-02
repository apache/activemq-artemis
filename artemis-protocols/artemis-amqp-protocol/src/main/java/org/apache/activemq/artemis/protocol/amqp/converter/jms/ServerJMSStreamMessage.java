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
package org.apache.activemq.artemis.protocol.amqp.converter.jms;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
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

public final class ServerJMSStreamMessage extends ServerJMSMessage implements StreamMessage {

   public static final byte TYPE = Message.STREAM_TYPE;

   private int bodyLength = 0;

   public ServerJMSStreamMessage(ICoreMessage message) {
      super(message);
   }

   // StreamMessage implementation ----------------------------------

   @Override
   public boolean readBoolean() throws JMSException {

      try {
         return streamReadBoolean(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new MessageFormatException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new MessageEOFException("");
      }
   }

   @Override
   public byte readByte() throws JMSException {
      try {
         return streamReadByte(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new MessageFormatException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new MessageEOFException("");
      }
   }

   @Override
   public short readShort() throws JMSException {

      try {
         return streamReadShort(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new MessageFormatException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new MessageEOFException("");
      }
   }

   @Override
   public char readChar() throws JMSException {

      try {
         return streamReadChar(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new MessageFormatException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new MessageEOFException("");
      }
   }

   @Override
   public int readInt() throws JMSException {

      try {
         return streamReadInteger(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new MessageFormatException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new MessageEOFException("");
      }
   }

   @Override
   public long readLong() throws JMSException {

      try {
         return streamReadLong(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new MessageFormatException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new MessageEOFException("");
      }
   }

   @Override
   public float readFloat() throws JMSException {

      try {
         return streamReadFloat(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new MessageFormatException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new MessageEOFException("");
      }
   }

   @Override
   public double readDouble() throws JMSException {

      try {
         return streamReadDouble(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new MessageFormatException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new MessageEOFException("");
      }
   }

   @Override
   public String readString() throws JMSException {

      try {
         return streamReadString(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new MessageFormatException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new MessageEOFException("");
      }
   }

   /**
    * len here is used to control how many more bytes to read
    */
   private int len = 0;

   @Override
   public int readBytes(final byte[] value) throws JMSException {

      try {
         Pair<Integer, Integer> pairRead = streamReadBytes(getReadBodyBuffer(), len, value);

         len = pairRead.getA();
         return pairRead.getB();
      } catch (IllegalStateException e) {
         throw new MessageFormatException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new MessageEOFException("");
      }
   }

   @Override
   public Object readObject() throws JMSException {

      if (getReadBodyBuffer().readerIndex() >= getReadBodyBuffer().writerIndex()) {
         throw new MessageEOFException("");
      }
      try {
         return streamReadObject(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new MessageFormatException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new MessageEOFException("");
      }
   }

   @Override
   public void writeBoolean(final boolean value) throws JMSException {

      getWriteBodyBuffer().writeByte(DataConstants.BOOLEAN);
      getWriteBodyBuffer().writeBoolean(value);
   }

   @Override
   public void writeByte(final byte value) throws JMSException {

      getWriteBodyBuffer().writeByte(DataConstants.BYTE);
      getWriteBodyBuffer().writeByte(value);
   }

   @Override
   public void writeShort(final short value) throws JMSException {

      getWriteBodyBuffer().writeByte(DataConstants.SHORT);
      getWriteBodyBuffer().writeShort(value);
   }

   @Override
   public void writeChar(final char value) throws JMSException {

      getWriteBodyBuffer().writeByte(DataConstants.CHAR);
      getWriteBodyBuffer().writeShort((short) value);
   }

   @Override
   public void writeInt(final int value) throws JMSException {

      getWriteBodyBuffer().writeByte(DataConstants.INT);
      getWriteBodyBuffer().writeInt(value);
   }

   @Override
   public void writeLong(final long value) throws JMSException {

      getWriteBodyBuffer().writeByte(DataConstants.LONG);
      getWriteBodyBuffer().writeLong(value);
   }

   @Override
   public void writeFloat(final float value) throws JMSException {

      getWriteBodyBuffer().writeByte(DataConstants.FLOAT);
      getWriteBodyBuffer().writeInt(Float.floatToIntBits(value));
   }

   @Override
   public void writeDouble(final double value) throws JMSException {

      getWriteBodyBuffer().writeByte(DataConstants.DOUBLE);
      getWriteBodyBuffer().writeLong(Double.doubleToLongBits(value));
   }

   @Override
   public void writeString(final String value) throws JMSException {

      getWriteBodyBuffer().writeByte(DataConstants.STRING);
      getWriteBodyBuffer().writeNullableString(value);
   }

   @Override
   public void writeBytes(final byte[] value) throws JMSException {

      getWriteBodyBuffer().writeByte(DataConstants.BYTES);
      getWriteBodyBuffer().writeInt(value.length);
      getWriteBodyBuffer().writeBytes(value);
   }

   @Override
   public void writeBytes(final byte[] value, final int offset, final int length) throws JMSException {

      getWriteBodyBuffer().writeByte(DataConstants.BYTES);
      getWriteBodyBuffer().writeInt(length);
      getWriteBodyBuffer().writeBytes(value, offset, length);
   }

   @Override
   public void writeObject(final Object value) throws JMSException {
      if (value instanceof String) {
         writeString((String) value);
      } else if (value instanceof Boolean) {
         writeBoolean((Boolean) value);
      } else if (value instanceof Byte) {
         writeByte((Byte) value);
      } else if (value instanceof Short) {
         writeShort((Short) value);
      } else if (value instanceof Integer) {
         writeInt((Integer) value);
      } else if (value instanceof Long) {
         writeLong((Long) value);
      } else if (value instanceof Float) {
         writeFloat((Float) value);
      } else if (value instanceof Double) {
         writeDouble((Double) value);
      } else if (value instanceof byte[]) {
         writeBytes((byte[]) value);
      } else if (value instanceof Character) {
         writeChar((Character) value);
      } else if (value == null) {
         writeString(null);
      } else {
         throw new MessageFormatException("Invalid object type: " + value.getClass());
      }
   }

   @Override
   public void reset() throws JMSException {
      getWriteBodyBuffer().resetReaderIndex();
   }

   // ActiveMQRAMessage overrides ----------------------------------------

   @Override
   public void clearBody() throws JMSException {
      super.clearBody();

      getWriteBodyBuffer().clear();
   }

   @Override
   public void decode() throws Exception {
      super.decode();
   }

   /**
    * Encode the body into the internal message
    */
   @Override
   public void encode() throws Exception {
      super.encode();
      bodyLength = message.getEndOfBodyPosition();
   }

}
