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
package org.apache.activemq.ra;

import java.util.Arrays;

import javax.jms.JMSException;
import javax.jms.StreamMessage;


/**
 * A wrapper for a message
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public class HornetQRAStreamMessage extends HornetQRAMessage implements StreamMessage
{
   /** Whether trace is enabled */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /**
    * Create a new wrapper
    * @param message the message
    * @param session the session
    */
   public HornetQRAStreamMessage(final StreamMessage message, final HornetQRASession session)
   {
      super(message, session);

      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor(" + message + ", " + session + ")");
      }
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public boolean readBoolean() throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("readBoolean()");
      }

      return ((StreamMessage)message).readBoolean();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public byte readByte() throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("readByte()");
      }

      return ((StreamMessage)message).readByte();
   }

   /**
    * Read
    * @param value The value
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int readBytes(final byte[] value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("readBytes(" + Arrays.toString(value) + ")");
      }

      return ((StreamMessage)message).readBytes(value);
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public char readChar() throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("readChar()");
      }

      return ((StreamMessage)message).readChar();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public double readDouble() throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("readDouble()");
      }

      return ((StreamMessage)message).readDouble();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public float readFloat() throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("readFloat()");
      }

      return ((StreamMessage)message).readFloat();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int readInt() throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("readInt()");
      }

      return ((StreamMessage)message).readInt();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public long readLong() throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("readLong()");
      }

      return ((StreamMessage)message).readLong();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public Object readObject() throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("readObject()");
      }

      return ((StreamMessage)message).readObject();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public short readShort() throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("readShort()");
      }

      return ((StreamMessage)message).readShort();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public String readString() throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("readString()");
      }

      return ((StreamMessage)message).readString();
   }

   /**
    * Reset
    * @exception JMSException Thrown if an error occurs
    */
   public void reset() throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("reset()");
      }

      ((StreamMessage)message).reset();
   }

   /**
    * Write
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void writeBoolean(final boolean value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeBoolean(" + value + ")");
      }

      ((StreamMessage)message).writeBoolean(value);
   }

   /**
    * Write
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void writeByte(final byte value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeByte(" + value + ")");
      }

      ((StreamMessage)message).writeByte(value);
   }

   /**
    * Write
    * @param value The value
    * @param offset The offset
    * @param length The length
    * @exception JMSException Thrown if an error occurs
    */
   public void writeBytes(final byte[] value, final int offset, final int length) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeBytes(" + value + ", " + offset + ", " + length + ")");
      }

      ((StreamMessage)message).writeBytes(value, offset, length);
   }

   /**
    * Write
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void writeBytes(final byte[] value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeBytes(" + value + ")");
      }

      ((StreamMessage)message).writeBytes(value);
   }

   /**
    * Write
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void writeChar(final char value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeChar(" + value + ")");
      }

      ((StreamMessage)message).writeChar(value);
   }

   /**
    * Write
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void writeDouble(final double value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeDouble(" + value + ")");
      }

      ((StreamMessage)message).writeDouble(value);
   }

   /**
    * Write
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void writeFloat(final float value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeFloat(" + value + ")");
      }

      ((StreamMessage)message).writeFloat(value);
   }

   /**
    * Write
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void writeInt(final int value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeInt(" + value + ")");
      }

      ((StreamMessage)message).writeInt(value);
   }

   /**
    * Write
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void writeLong(final long value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeLong(" + value + ")");
      }

      ((StreamMessage)message).writeLong(value);
   }

   /**
    * Write
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void writeObject(final Object value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeObject(" + value + ")");
      }

      ((StreamMessage)message).writeObject(value);
   }

   /**
    * Write
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void writeShort(final short value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeShort(" + value + ")");
      }

      ((StreamMessage)message).writeShort(value);
   }

   /**
    * Write
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void writeString(final String value) throws JMSException
   {
      if (HornetQRAStreamMessage.trace)
      {
         HornetQRALogger.LOGGER.trace("writeString(" + value + ")");
      }

      ((StreamMessage)message).writeString(value);
   }
}
