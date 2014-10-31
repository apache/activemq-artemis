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

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.utils.DataConstants;

/**
 * HornetQ implementation of a JMS StreamMessage.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Some parts based on JBM 1.x class by:
 *
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public final class HornetQStreamMessage extends HornetQMessage implements StreamMessage
{
   public static final byte TYPE = Message.STREAM_TYPE;

   protected HornetQStreamMessage(final ClientSession session)
   {
      super(HornetQStreamMessage.TYPE, session);
   }

   protected HornetQStreamMessage(final ClientMessage message, final ClientSession session)
   {
      super(message, session);
   }

   public HornetQStreamMessage(final StreamMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, HornetQStreamMessage.TYPE, session);

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
   public HornetQStreamMessage()
   {
      message = new ClientMessageImpl((byte)0, false, 0, 0, (byte)4, 1500);
   }

   // Public --------------------------------------------------------

   @Override
   public byte getType()
   {
      return HornetQStreamMessage.TYPE;
   }

   // StreamMessage implementation ----------------------------------

   public boolean readBoolean() throws JMSException
   {
      checkRead();
      try
      {
         byte type = getBuffer().readByte();

         switch (type)
         {
            case DataConstants.BOOLEAN:
               return getBuffer().readBoolean();
            case DataConstants.STRING:
               String s = getBuffer().readNullableString();
               return Boolean.valueOf(s);
            default:
               throw new MessageFormatException("Invalid conversion, type byte was " + type);
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public byte readByte() throws JMSException
   {
      checkRead();
      int index = getBuffer().readerIndex();
      try
      {
         byte type = getBuffer().readByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return getBuffer().readByte();
            case DataConstants.STRING:
               String s = getBuffer().readNullableString();
               return Byte.parseByte(s);
            default:
               throw new MessageFormatException("Invalid conversion");
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
      catch (NumberFormatException e)
      {
         getBuffer().readerIndex(index);
         throw e;
      }
   }

   public short readShort() throws JMSException
   {
      checkRead();
      try
      {
         byte type = getBuffer().readByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return getBuffer().readByte();
            case DataConstants.SHORT:
               return getBuffer().readShort();
            case DataConstants.STRING:
               String s = getBuffer().readNullableString();
               return Short.parseShort(s);
            default:
               throw new MessageFormatException("Invalid conversion");
         }
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
         byte type = getBuffer().readByte();
         switch (type)
         {
            case DataConstants.CHAR:
               return (char)getBuffer().readShort();
            case DataConstants.STRING:
               String str = getBuffer().readNullableString();
               if (str == null)
               {
                  throw new NullPointerException("Invalid conversion");
               }
               else
               {
                  throw new MessageFormatException("Invalid conversion");
               }
            default:
               throw new MessageFormatException("Invalid conversion");
         }
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
         byte type = getBuffer().readByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return getBuffer().readByte();
            case DataConstants.SHORT:
               return getBuffer().readShort();
            case DataConstants.INT:
               return getBuffer().readInt();
            case DataConstants.STRING:
               String s = getBuffer().readNullableString();
               return Integer.parseInt(s);
            default:
               throw new MessageFormatException("Invalid conversion");
         }
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
         byte type = getBuffer().readByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return getBuffer().readByte();
            case DataConstants.SHORT:
               return getBuffer().readShort();
            case DataConstants.INT:
               return getBuffer().readInt();
            case DataConstants.LONG:
               return getBuffer().readLong();
            case DataConstants.STRING:
               String s = getBuffer().readNullableString();
               return Long.parseLong(s);
            default:
               throw new MessageFormatException("Invalid conversion");
         }
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
         byte type = getBuffer().readByte();
         switch (type)
         {
            case DataConstants.FLOAT:
               return Float.intBitsToFloat(getBuffer().readInt());
            case DataConstants.STRING:
               String s = getBuffer().readNullableString();
               return Float.parseFloat(s);
            default:
               throw new MessageFormatException("Invalid conversion");
         }
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
         byte type = getBuffer().readByte();
         switch (type)
         {
            case DataConstants.FLOAT:
               return Float.intBitsToFloat(getBuffer().readInt());
            case DataConstants.DOUBLE:
               return Double.longBitsToDouble(getBuffer().readLong());
            case DataConstants.STRING:
               String s = getBuffer().readNullableString();
               return Double.parseDouble(s);
            default:
               throw new MessageFormatException("Invalid conversion: " + type);
         }
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
         byte type = getBuffer().readByte();
         switch (type)
         {
            case DataConstants.BOOLEAN:
               return String.valueOf(getBuffer().readBoolean());
            case DataConstants.BYTE:
               return String.valueOf(getBuffer().readByte());
            case DataConstants.SHORT:
               return String.valueOf(getBuffer().readShort());
            case DataConstants.CHAR:
               return String.valueOf((char)getBuffer().readShort());
            case DataConstants.INT:
               return String.valueOf(getBuffer().readInt());
            case DataConstants.LONG:
               return String.valueOf(getBuffer().readLong());
            case DataConstants.FLOAT:
               return String.valueOf(Float.intBitsToFloat(getBuffer().readInt()));
            case DataConstants.DOUBLE:
               return String.valueOf(Double.longBitsToDouble(getBuffer().readLong()));
            case DataConstants.STRING:
               return getBuffer().readNullableString();
            default:
               throw new MessageFormatException("Invalid conversion");
         }
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   private int len;

   public int readBytes(final byte[] value) throws JMSException
   {
      checkRead();
      try
      {
         if (len == -1)
         {
            len = 0;
            return -1;
         }
         else if (len == 0)
         {
            byte type = getBuffer().readByte();
            if (type != DataConstants.BYTES)
            {
               throw new MessageFormatException("Invalid conversion");
            }
            len = getBuffer().readInt();
         }
         int read = Math.min(value.length, len);
         getBuffer().readBytes(value, 0, read);
         len -= read;
         if (len == 0)
         {
            len = -1;
         }
         return read;
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
         byte type = getBuffer().readByte();
         switch (type)
         {
            case DataConstants.BOOLEAN:
               return getBuffer().readBoolean();
            case DataConstants.BYTE:
               return getBuffer().readByte();
            case DataConstants.SHORT:
               return getBuffer().readShort();
            case DataConstants.CHAR:
               return (char)getBuffer().readShort();
            case DataConstants.INT:
               return getBuffer().readInt();
            case DataConstants.LONG:
               return getBuffer().readLong();
            case DataConstants.FLOAT:
               return Float.intBitsToFloat(getBuffer().readInt());
            case DataConstants.DOUBLE:
               return Double.longBitsToDouble(getBuffer().readLong());
            case DataConstants.STRING:
               return getBuffer().readNullableString();
            case DataConstants.BYTES:
               int bufferLen = getBuffer().readInt();
               byte[] bytes = new byte[bufferLen];
               getBuffer().readBytes(bytes);
               return bytes;
            default:
               throw new MessageFormatException("Invalid conversion");
         }
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

   // HornetQRAMessage overrides ----------------------------------------

   @Override
   public void clearBody()
   {
      super.clearBody();

      getBuffer().clear();
   }

   @Override
   public void doBeforeSend() throws Exception
   {
      reset();
   }

   private HornetQBuffer getBuffer()
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
