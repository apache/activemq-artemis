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
package org.apache.activemq.jms.client;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.Message;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.core.message.impl.MessageImpl;

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
 * ActiveMQ implementation of a JMS {@link BytesMessage}.
 *
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public class ActiveMQBytesMessage extends ActiveMQMessage implements BytesMessage
{
   // Static -------------------------------------------------------
   public static final byte TYPE = Message.BYTES_TYPE;

   // Attributes ----------------------------------------------------

   private int bodyLength;

   // Constructor ---------------------------------------------------

   /**
    * This constructor is used to construct messages prior to sending
    */
   protected ActiveMQBytesMessage(final ClientSession session)
   {
      super(ActiveMQBytesMessage.TYPE, session);
   }

   /**
    * Constructor on receipt at client side
    */
   protected ActiveMQBytesMessage(final ClientMessage message, final ClientSession session)
   {
      super(message, session);
   }

   /**
    * Foreign message constructor
    */
   public ActiveMQBytesMessage(final BytesMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, ActiveMQBytesMessage.TYPE, session);

      foreign.reset();

      byte[] buffer = new byte[1024];
      int n = foreign.readBytes(buffer);
      while (n != -1)
      {
         writeBytes(buffer, 0, n);
         n = foreign.readBytes(buffer);
      }
   }

   // BytesMessage implementation -----------------------------------

   public boolean readBoolean() throws JMSException
   {
      checkRead();
      try
      {
         return bytesReadBoolean(message);
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
         return bytesReadByte(message);
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readUnsignedByte() throws JMSException
   {
      checkRead();
      try
      {
         return bytesReadUnsignedByte(message);
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
         return bytesReadShort(message);
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readUnsignedShort() throws JMSException
   {
      checkRead();
      try
      {
         return bytesReadUnsignedShort(message);
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
         return bytesReadChar(message);
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
         return bytesReadInt(message);
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
         return bytesReadLong(message);
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
         return bytesReadFloat(message);
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
         return bytesReadDouble(message);
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public String readUTF() throws JMSException
   {
      checkRead();
      try
      {
         return bytesReadUTF(message);
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
      catch (Exception e)
      {
         JMSException je = new JMSException("Failed to get UTF");
         je.setLinkedException(e);
         je.initCause(e);
         throw je;
      }
   }

   public int readBytes(final byte[] value) throws JMSException
   {
      checkRead();
      return bytesReadBytes(message, value);
   }

   public int readBytes(final byte[] value, final int length) throws JMSException
   {
      checkRead();
      return bytesReadBytes(message, value, length);

   }

   public void writeBoolean(final boolean value) throws JMSException
   {
      checkWrite();
      bytesWriteBoolean(message, value);
   }

   public void writeByte(final byte value) throws JMSException
   {
      checkWrite();
      bytesWriteByte(message, value);
   }

   public void writeShort(final short value) throws JMSException
   {
      checkWrite();
      bytesWriteShort(message, value);
   }

   public void writeChar(final char value) throws JMSException
   {
      checkWrite();
      bytesWriteChar(message, value);
   }

   public void writeInt(final int value) throws JMSException
   {
      checkWrite();
      bytesWriteInt(message, value);
   }

   public void writeLong(final long value) throws JMSException
   {
      checkWrite();
      bytesWriteLong(message, value);
   }

   public void writeFloat(final float value) throws JMSException
   {
      checkWrite();
      bytesWriteFloat(message, value);
   }

   public void writeDouble(final double value) throws JMSException
   {
      checkWrite();
      bytesWriteDouble(message, value);
   }

   public void writeUTF(final String value) throws JMSException
   {
      checkWrite();
      try
      {
         bytesWriteUTF(message, value);
      }
      catch (Exception e)
      {
         JMSException je = new JMSException("Failed to write UTF");
         je.setLinkedException(e);
         je.initCause(e);
         throw je;
      }

   }

   public void writeBytes(final byte[] value) throws JMSException
   {
      checkWrite();
      bytesWriteBytes(message, value);
   }

   public void writeBytes(final byte[] value, final int offset, final int length) throws JMSException
   {
      checkWrite();
      bytesWriteBytes(message, value, offset, length);
   }

   public void writeObject(final Object value) throws JMSException
   {
      checkWrite();
      if (!bytesWriteObject(message, value))
      {
         throw new MessageFormatException("Invalid object for properties");
      }
   }

   public void reset() throws JMSException
   {
      if (!readOnly)
      {
         readOnly = true;

         bodyLength = message.getBodySize();
      }

      bytesMessageReset(message);
   }

   @Override
   public void doBeforeReceive() throws ActiveMQException
   {
      bodyLength = message.getBodySize();
   }

   // ActiveMQRAMessage overrides ----------------------------------------

   @Override
   public void clearBody() throws JMSException
   {
      super.clearBody();

      try
      {
         getBuffer().clear();
      }
      catch (RuntimeException e)
      {
         JMSException e2 = new JMSException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
   }

   public long getBodyLength() throws JMSException
   {
      checkRead();

      return bodyLength;
   }

   @Override
   public void doBeforeSend() throws Exception
   {
      reset();
   }

   // Public --------------------------------------------------------

   @Override
   public byte getType()
   {
      return ActiveMQBytesMessage.TYPE;
   }

   private ActiveMQBuffer getBuffer()
   {
      return message.getBodyBuffer();
   }

   @Override
   public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes")
                                     Class c)
   {
      return c.isAssignableFrom(byte[].class);
   }

   @Override
   protected <T> T getBodyInternal(Class<T> c)
   {
      if (bodyLength == 0)
         return null;
      byte[] dst = new byte[bodyLength];
      message.getBodyBuffer().getBytes(MessageImpl.BODY_OFFSET, dst);
      return (T)dst;
   }
}
