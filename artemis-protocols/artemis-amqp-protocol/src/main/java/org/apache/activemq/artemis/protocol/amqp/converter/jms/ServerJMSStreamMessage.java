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

public final class ServerJMSStreamMessage extends ServerJMSMessage {

   public static final byte TYPE = Message.STREAM_TYPE;

   private int bodyLength = 0;

   public ServerJMSStreamMessage(ICoreMessage message) {
      super(message);
   }

   // StreamMessage implementation ----------------------------------

   public boolean readBoolean() throws Exception {

      try {
         return streamReadBoolean(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new RuntimeException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new RuntimeException("");
      }
   }

   public byte readByte() throws Exception {
      try {
         return streamReadByte(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new RuntimeException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new RuntimeException("");
      }
   }

   public short readShort() throws Exception {

      try {
         return streamReadShort(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new RuntimeException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new RuntimeException("");
      }
   }

   public char readChar() throws Exception {

      try {
         return streamReadChar(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new RuntimeException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new RuntimeException("");
      }
   }

   public int readInt() throws Exception {

      try {
         return streamReadInteger(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new RuntimeException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new RuntimeException("");
      }
   }

   public long readLong() throws Exception {

      try {
         return streamReadLong(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new RuntimeException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new RuntimeException("");
      }
   }

   public float readFloat() throws Exception {

      try {
         return streamReadFloat(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new RuntimeException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new RuntimeException("");
      }
   }

   public double readDouble() throws Exception {

      try {
         return streamReadDouble(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new RuntimeException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new RuntimeException("");
      }
   }

   public String readString() throws Exception {

      try {
         return streamReadString(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new RuntimeException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new RuntimeException("");
      }
   }

   /**
    * len here is used to control how many more bytes to read
    */
   private int len = 0;

   public int readBytes(final byte[] value) throws Exception {

      try {
         Pair<Integer, Integer> pairRead = streamReadBytes(getReadBodyBuffer(), len, value);

         len = pairRead.getA();
         return pairRead.getB();
      } catch (IllegalStateException e) {
         throw new RuntimeException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new RuntimeException("");
      }
   }

   public Object readObject() throws Exception {

      if (getReadBodyBuffer().readerIndex() >= getReadBodyBuffer().writerIndex()) {
         throw new RuntimeException("");
      }
      try {
         return streamReadObject(getReadBodyBuffer());
      } catch (IllegalStateException e) {
         throw new RuntimeException(e.getMessage());
      } catch (IndexOutOfBoundsException e) {
         throw new RuntimeException("");
      }
   }

   public void writeBoolean(final boolean value) throws Exception {

      getWriteBodyBuffer().writeByte(DataConstants.BOOLEAN);
      getWriteBodyBuffer().writeBoolean(value);
   }

   public void writeByte(final byte value) throws Exception {

      getWriteBodyBuffer().writeByte(DataConstants.BYTE);
      getWriteBodyBuffer().writeByte(value);
   }

   public void writeShort(final short value) throws Exception {

      getWriteBodyBuffer().writeByte(DataConstants.SHORT);
      getWriteBodyBuffer().writeShort(value);
   }

   public void writeChar(final char value) throws Exception {

      getWriteBodyBuffer().writeByte(DataConstants.CHAR);
      getWriteBodyBuffer().writeShort((short) value);
   }

   public void writeInt(final int value) throws Exception {

      getWriteBodyBuffer().writeByte(DataConstants.INT);
      getWriteBodyBuffer().writeInt(value);
   }

   public void writeLong(final long value) throws Exception {

      getWriteBodyBuffer().writeByte(DataConstants.LONG);
      getWriteBodyBuffer().writeLong(value);
   }

   public void writeFloat(final float value) throws Exception {

      getWriteBodyBuffer().writeByte(DataConstants.FLOAT);
      getWriteBodyBuffer().writeInt(Float.floatToIntBits(value));
   }

   public void writeDouble(final double value) throws Exception {

      getWriteBodyBuffer().writeByte(DataConstants.DOUBLE);
      getWriteBodyBuffer().writeLong(Double.doubleToLongBits(value));
   }

   public void writeString(final String value) throws Exception {

      getWriteBodyBuffer().writeByte(DataConstants.STRING);
      getWriteBodyBuffer().writeNullableString(value);
   }

   public void writeBytes(final byte[] value) throws Exception {

      getWriteBodyBuffer().writeByte(DataConstants.BYTES);
      getWriteBodyBuffer().writeInt(value.length);
      getWriteBodyBuffer().writeBytes(value);
   }

   public void writeBytes(final byte[] value, final int offset, final int length) throws Exception {

      getWriteBodyBuffer().writeByte(DataConstants.BYTES);
      getWriteBodyBuffer().writeInt(length);
      getWriteBodyBuffer().writeBytes(value, offset, length);
   }

   public void writeObject(final Object value) throws Exception {
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
         throw new RuntimeException("Invalid object type: " + value.getClass());
      }
   }

   public void reset() throws Exception {
      getWriteBodyBuffer().resetReaderIndex();
   }

   // ActiveMQRAMessage overrides ----------------------------------------

   @Override
   public void clearBody() throws Exception {
      super.clearBody();

      getWriteBodyBuffer().clear();
   }

   @Override
   public void decode() throws Exception {
      super.decode();
   }

   /**
    * Encode the body into the internal message
     * @throws java.lang.Exception
    */
   @Override
   public void encode() throws Exception {
      super.encode();
      bodyLength = message.getEndOfBodyPosition();
   }

}
