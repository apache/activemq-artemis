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

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import org.apache.activemq.artemis.api.core.ICoreMessage;

import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesMessageReset;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadBoolean;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadByte;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadBytes;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadChar;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadDouble;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadFloat;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadInt;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadLong;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadShort;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadUTF;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadUnsignedByte;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesReadUnsignedShort;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesWriteBoolean;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesWriteByte;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesWriteBytes;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesWriteChar;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesWriteDouble;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesWriteFloat;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesWriteInt;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesWriteLong;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesWriteObject;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesWriteShort;
import static org.apache.activemq.artemis.reader.BytesMessageUtil.bytesWriteUTF;

public class ServerJMSBytesMessage extends ServerJMSMessage implements BytesMessage {

   public ServerJMSBytesMessage(ICoreMessage message) {
      super(message);
   }

   @Override
   public long getBodyLength() throws JMSException {
      return message.getReadOnlyBodyBuffer().readableBytes();
   }

   @Override
   public boolean readBoolean() throws JMSException {
      return bytesReadBoolean(getReadBodyBuffer());
   }

   @Override
   public byte readByte() throws JMSException {
      return bytesReadByte(getReadBodyBuffer());
   }

   @Override
   public int readUnsignedByte() throws JMSException {
      return bytesReadUnsignedByte(getReadBodyBuffer());
   }

   @Override
   public short readShort() throws JMSException {
      return bytesReadShort(getReadBodyBuffer());
   }

   @Override
   public int readUnsignedShort() throws JMSException {
      return bytesReadUnsignedShort(getReadBodyBuffer());
   }

   @Override
   public char readChar() throws JMSException {
      return bytesReadChar(getReadBodyBuffer());
   }

   @Override
   public int readInt() throws JMSException {
      return bytesReadInt(getReadBodyBuffer());
   }

   @Override
   public long readLong() throws JMSException {
      return bytesReadLong(getReadBodyBuffer());
   }

   @Override
   public float readFloat() throws JMSException {
      return bytesReadFloat(getReadBodyBuffer());
   }

   @Override
   public double readDouble() throws JMSException {
      return bytesReadDouble(getReadBodyBuffer());
   }

   @Override
   public String readUTF() throws JMSException {
      return bytesReadUTF(getReadBodyBuffer());
   }

   @Override
   public int readBytes(byte[] value) throws JMSException {
      return bytesReadBytes(getReadBodyBuffer(), value);
   }

   @Override
   public int readBytes(byte[] value, int length) throws JMSException {
      return bytesReadBytes(getReadBodyBuffer(), value, length);
   }

   @Override
   public void writeBoolean(boolean value) throws JMSException {
      bytesWriteBoolean(getWriteBodyBuffer(), value);

   }

   @Override
   public void writeByte(byte value) throws JMSException {
      bytesWriteByte(getWriteBodyBuffer(), value);
   }

   @Override
   public void writeShort(short value) throws JMSException {
      bytesWriteShort(getWriteBodyBuffer(), value);
   }

   @Override
   public void writeChar(char value) throws JMSException {
      bytesWriteChar(getWriteBodyBuffer(), value);
   }

   @Override
   public void writeInt(int value) throws JMSException {
      bytesWriteInt(getWriteBodyBuffer(), value);
   }

   @Override
   public void writeLong(long value) throws JMSException {
      bytesWriteLong(getWriteBodyBuffer(), value);
   }

   @Override
   public void writeFloat(float value) throws JMSException {
      bytesWriteFloat(getWriteBodyBuffer(), value);
   }

   @Override
   public void writeDouble(double value) throws JMSException {
      bytesWriteDouble(getWriteBodyBuffer(), value);
   }

   @Override
   public void writeUTF(String value) throws JMSException {
      bytesWriteUTF(getWriteBodyBuffer(), value);
   }

   @Override
   public void writeBytes(byte[] value) throws JMSException {
      bytesWriteBytes(getWriteBodyBuffer(), value);
   }

   @Override
   public void writeBytes(byte[] value, int offset, int length) throws JMSException {
      bytesWriteBytes(getWriteBodyBuffer(), value, offset, length);
   }

   @Override
   public void writeObject(Object value) throws JMSException {
      if (!bytesWriteObject(getWriteBodyBuffer(), value)) {
         throw new JMSException("Can't make conversion of " + value + " to any known type");
      }
   }

   @Override
   public void encode() throws Exception {
      super.encode();
      // this is to make sure we encode the body-length before it's persisted
      getBodyLength();
   }

   @Override
   public void decode() throws Exception {
      super.decode();

   }

   @Override
   public void reset() throws JMSException {
      if (!message.isLargeMessage()) {
         bytesMessageReset(getReadBodyBuffer());
         bytesMessageReset(getWriteBodyBuffer());
      }
   }

}
