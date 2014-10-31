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
package org.hornetq.jms.client;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.message.impl.MessageImpl;

/**
 * HornetQ implementation of a JMS {@link BytesMessage}.
 *
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public class HornetQBytesMessage extends HornetQMessage implements BytesMessage
{
   // Static -------------------------------------------------------
   public static final byte TYPE = Message.BYTES_TYPE;

   // Attributes ----------------------------------------------------

   private int bodyLength;

   // Constructor ---------------------------------------------------

   /**
    * This constructor is used to construct messages prior to sending
    */
   protected HornetQBytesMessage(final ClientSession session)
   {
      super(HornetQBytesMessage.TYPE, session);
   }

   /**
    * Constructor on receipt at client side
    */
   protected HornetQBytesMessage(final ClientMessage message, final ClientSession session)
   {
      super(message, session);
   }

   /**
    * Foreign message constructor
    */
   public HornetQBytesMessage(final BytesMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, HornetQBytesMessage.TYPE, session);

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
         return getBuffer().readBoolean();
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
         return getBuffer().readByte();
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
         return getBuffer().readUnsignedByte();
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
         return getBuffer().readShort();
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
         return getBuffer().readUnsignedShort();
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
         return (char)getBuffer().readShort();
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
         return getBuffer().readInt();
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
         return getBuffer().readLong();
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
         return Float.intBitsToFloat(getBuffer().readInt());
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
         return Double.longBitsToDouble(getBuffer().readLong());
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
         return getBuffer().readUTF();
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
      return readBytes(value, value.length);
   }

   public int readBytes(final byte[] value, final int length) throws JMSException
   {
      checkRead();

      if (!getBuffer().readable())
      {
         return -1;
      }

      int read = Math.min(length, getBuffer().readableBytes());

      if (read != 0)
      {
         getBuffer().readBytes(value, 0, read);
      }

      return read;
   }

   public void writeBoolean(final boolean value) throws JMSException
   {
      checkWrite();
      getBuffer().writeBoolean(value);
   }

   public void writeByte(final byte value) throws JMSException
   {
      checkWrite();
      getBuffer().writeByte(value);
   }

   public void writeShort(final short value) throws JMSException
   {
      checkWrite();
      getBuffer().writeShort(value);
   }

   public void writeChar(final char value) throws JMSException
   {
      checkWrite();
      getBuffer().writeShort((short)value);
   }

   public void writeInt(final int value) throws JMSException
   {
      checkWrite();
      getBuffer().writeInt(value);
   }

   public void writeLong(final long value) throws JMSException
   {
      checkWrite();
      getBuffer().writeLong(value);
   }

   public void writeFloat(final float value) throws JMSException
   {
      checkWrite();
      getBuffer().writeInt(Float.floatToIntBits(value));
   }

   public void writeDouble(final double value) throws JMSException
   {
      checkWrite();
      getBuffer().writeLong(Double.doubleToLongBits(value));
   }

   public void writeUTF(final String value) throws JMSException
   {
      checkWrite();
      try
      {
         getBuffer().writeUTF(value);
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
      getBuffer().writeBytes(value);
   }

   public void writeBytes(final byte[] value, final int offset, final int length) throws JMSException
   {
      checkWrite();
      getBuffer().writeBytes(value, offset, length);
   }

   public void writeObject(final Object value) throws JMSException
   {
      if (value == null)
      {
         throw new NullPointerException("Attempt to write a null value");
      }
      if (value instanceof String)
      {
         writeUTF((String)value);
      }
      else if (value instanceof Boolean)
      {
         writeBoolean((Boolean)value);
      }
      else if (value instanceof Character)
      {
         writeChar((Character)value);
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
      else
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

         getBuffer().resetReaderIndex();
      }
      else
      {
         getBuffer().resetReaderIndex();
      }
   }

   @Override
   public void doBeforeReceive() throws HornetQException
   {
      bodyLength = message.getBodySize();
   }

   // HornetQRAMessage overrides ----------------------------------------

   @Override
   public void clearBody()
   {
      super.clearBody();

      getBuffer().clear();
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
      return HornetQBytesMessage.TYPE;
   }

   private HornetQBuffer getBuffer()
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
