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

public class ServerJMSBytesMessage extends ServerJMSMessage {

   public ServerJMSBytesMessage(ICoreMessage message) {
      super(message);
   }

   public long getBodyLength() throws Exception {
      return message.getBodyBufferSize();
   }

   public boolean readBoolean() throws Exception {
      return bytesReadBoolean(getReadBodyBuffer());
   }

   public byte readByte() throws Exception {
      return bytesReadByte(getReadBodyBuffer());
   }

   public int readUnsignedByte() throws Exception {
      return bytesReadUnsignedByte(getReadBodyBuffer());
   }

   public short readShort() throws Exception {
      return bytesReadShort(getReadBodyBuffer());
   }

   public int readUnsignedShort() throws Exception {
      return bytesReadUnsignedShort(getReadBodyBuffer());
   }

   public char readChar() throws Exception {
      return bytesReadChar(getReadBodyBuffer());
   }

   public int readInt() throws Exception {
      return bytesReadInt(getReadBodyBuffer());
   }

   public long readLong() throws Exception {
      return bytesReadLong(getReadBodyBuffer());
   }

   public float readFloat() throws Exception {
      return bytesReadFloat(getReadBodyBuffer());
   }

   public double readDouble() throws Exception {
      return bytesReadDouble(getReadBodyBuffer());
   }

   public String readUTF() throws Exception {
      return bytesReadUTF(getReadBodyBuffer());
   }

   public int readBytes(byte[] value) throws Exception {
      return bytesReadBytes(getReadBodyBuffer(), value);
   }

   public int readBytes(byte[] value, int length) throws Exception {
      return bytesReadBytes(getReadBodyBuffer(), value, length);
   }

   public void writeBoolean(boolean value) throws Exception {
      bytesWriteBoolean(getWriteBodyBuffer(), value);

   }

   public void writeByte(byte value) throws Exception {
      bytesWriteByte(getWriteBodyBuffer(), value);
   }

   public void writeShort(short value) throws Exception {
      bytesWriteShort(getWriteBodyBuffer(), value);
   }

   public void writeChar(char value) throws Exception {
      bytesWriteChar(getWriteBodyBuffer(), value);
   }

   public void writeInt(int value) throws Exception {
      bytesWriteInt(getWriteBodyBuffer(), value);
   }

   public void writeLong(long value) throws Exception {
      bytesWriteLong(getWriteBodyBuffer(), value);
   }

   public void writeFloat(float value) throws Exception {
      bytesWriteFloat(getWriteBodyBuffer(), value);
   }

   public void writeDouble(double value) throws Exception {
      bytesWriteDouble(getWriteBodyBuffer(), value);
   }

   public void writeUTF(String value) throws Exception {
      bytesWriteUTF(getWriteBodyBuffer(), value);
   }

   public void writeBytes(byte[] value) throws Exception {
      bytesWriteBytes(getWriteBodyBuffer(), value);
   }

   public void writeBytes(byte[] value, int offset, int length) throws Exception {
      bytesWriteBytes(getWriteBodyBuffer(), value, offset, length);
   }

   public void writeObject(Object value) throws Exception {
      if (!bytesWriteObject(getWriteBodyBuffer(), value)) {
         throw new Exception("Can't make conversion of " + value + " to any known type");
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

   public void reset() throws Exception {
      if (!message.isLargeMessage()) {
         bytesMessageReset(getReadBodyBuffer());
         bytesMessageReset(getWriteBodyBuffer());
      }
   }

}
