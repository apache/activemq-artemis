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

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import org.apache.activemq.core.message.impl.MessageImpl;
import org.apache.activemq.core.message.impl.MessageInternal;

import static org.apache.activemq.reader.BytesMessageUtil.bytesMessageReset;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadBoolean;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadByte;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadBytes;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadChar;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadDouble;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadFloat;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadInt;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadLong;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadShort;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadUTF;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadUnsignedByte;
import static org.apache.activemq.reader.BytesMessageUtil.bytesReadUnsignedShort;
import static org.apache.activemq.reader.BytesMessageUtil.bytesWriteBoolean;
import static org.apache.activemq.reader.BytesMessageUtil.bytesWriteByte;
import static org.apache.activemq.reader.BytesMessageUtil.bytesWriteBytes;
import static org.apache.activemq.reader.BytesMessageUtil.bytesWriteChar;
import static org.apache.activemq.reader.BytesMessageUtil.bytesWriteDouble;
import static org.apache.activemq.reader.BytesMessageUtil.bytesWriteFloat;
import static org.apache.activemq.reader.BytesMessageUtil.bytesWriteInt;
import static org.apache.activemq.reader.BytesMessageUtil.bytesWriteLong;
import static org.apache.activemq.reader.BytesMessageUtil.bytesWriteObject;
import static org.apache.activemq.reader.BytesMessageUtil.bytesWriteShort;
import static org.apache.activemq.reader.BytesMessageUtil.bytesWriteUTF;


/**
 * @author Clebert Suconic
 */

public class ServerJMSBytesMessage extends ServerJMSMessage implements BytesMessage
{
   public ServerJMSBytesMessage(MessageInternal message, int deliveryCount)
   {
      super(message, deliveryCount);
   }

   @Override
   public long getBodyLength() throws JMSException
   {
      return message.getEndOfBodyPosition() - MessageImpl.BODY_OFFSET;
   }

   @Override
   public boolean readBoolean() throws JMSException
   {
      return bytesReadBoolean(message);
   }

   @Override
   public byte readByte() throws JMSException
   {
      return bytesReadByte(message);
   }

   @Override
   public int readUnsignedByte() throws JMSException
   {
      return bytesReadUnsignedByte(message);
   }

   @Override
   public short readShort() throws JMSException
   {
      return bytesReadShort(message);
   }

   @Override
   public int readUnsignedShort() throws JMSException
   {
      return bytesReadUnsignedShort(message);
   }

   @Override
   public char readChar() throws JMSException
   {
      return bytesReadChar(message);
   }

   @Override
   public int readInt() throws JMSException
   {
      return bytesReadInt(message);
   }

   @Override
   public long readLong() throws JMSException
   {
      return bytesReadLong(message);
   }

   @Override
   public float readFloat() throws JMSException
   {
      return bytesReadFloat(message);
   }

   @Override
   public double readDouble() throws JMSException
   {
      return bytesReadDouble(message);
   }

   @Override
   public String readUTF() throws JMSException
   {
      return bytesReadUTF(message);
   }

   @Override
   public int readBytes(byte[] value) throws JMSException
   {
      return bytesReadBytes(message, value);
   }

   @Override
   public int readBytes(byte[] value, int length) throws JMSException
   {
      return bytesReadBytes(message, value, length);
   }

   @Override
   public void writeBoolean(boolean value) throws JMSException
   {
      bytesWriteBoolean(message, value);

   }

   @Override
   public void writeByte(byte value) throws JMSException
   {
      bytesWriteByte(message, value);
   }

   @Override
   public void writeShort(short value) throws JMSException
   {
      bytesWriteShort(message, value);
   }

   @Override
   public void writeChar(char value) throws JMSException
   {
      bytesWriteChar(message, value);
   }

   @Override
   public void writeInt(int value) throws JMSException
   {
      bytesWriteInt(message, value);
   }

   @Override
   public void writeLong(long value) throws JMSException
   {
      bytesWriteLong(message, value);
   }

   @Override
   public void writeFloat(float value) throws JMSException
   {
      bytesWriteFloat(message, value);
   }

   @Override
   public void writeDouble(double value) throws JMSException
   {
      bytesWriteDouble(message, value);
   }

   @Override
   public void writeUTF(String value) throws JMSException
   {
      bytesWriteUTF(message, value);
   }

   @Override
   public void writeBytes(byte[] value) throws JMSException
   {
      bytesWriteBytes(message, value);
   }

   @Override
   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      bytesWriteBytes(message, value, offset, length);
   }

   @Override
   public void writeObject(Object value) throws JMSException
   {
      if (!bytesWriteObject(message, value))
      {
         throw new JMSException("Can't make conversion of " + value + " to any known type");
      }
   }

   public void encode() throws Exception
   {
      super.encode();
      // this is to make sure we encode the body-length before it's persisted
      getBodyLength();
   }


   public void decode() throws Exception
   {
      super.decode();

   }

   @Override
   public void reset() throws JMSException
   {
      bytesMessageReset(message);
   }

}
