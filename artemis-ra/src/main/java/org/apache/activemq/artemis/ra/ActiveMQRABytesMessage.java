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
package org.apache.activemq.artemis.ra;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import java.util.Arrays;

/**
 * A wrapper for a message
 */
public class ActiveMQRABytesMessage extends ActiveMQRAMessage implements BytesMessage {

   /**
    * Create a new wrapper
    *
    * @param message the message
    * @param session the session
    */
   public ActiveMQRABytesMessage(final BytesMessage message, final ActiveMQRASession session) {
      super(message, session);

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("constructor(" + message + ", " + session + ")");
      }
   }

   /**
    * Get body length
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public long getBodyLength() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getBodyLength()");
      }

      return ((BytesMessage) message).getBodyLength();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public boolean readBoolean() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readBoolean()");
      }

      return ((BytesMessage) message).readBoolean();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public byte readByte() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readByte()");
      }

      return ((BytesMessage) message).readByte();
   }

   /**
    * Read
    *
    * @param value  The value
    * @param length The length
    * @return The result
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public int readBytes(final byte[] value, final int length) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readBytes(" + Arrays.toString(value) + ", " + length + ")");
      }

      return ((BytesMessage) message).readBytes(value, length);
   }

   /**
    * Read
    *
    * @param value The value
    * @return The result
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public int readBytes(final byte[] value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readBytes(" + Arrays.toString(value) + ")");
      }

      return ((BytesMessage) message).readBytes(value);
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public char readChar() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readChar()");
      }

      return ((BytesMessage) message).readChar();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public double readDouble() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readDouble()");
      }

      return ((BytesMessage) message).readDouble();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public float readFloat() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readFloat()");
      }

      return ((BytesMessage) message).readFloat();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public int readInt() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readInt()");
      }

      return ((BytesMessage) message).readInt();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public long readLong() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readLong()");
      }

      return ((BytesMessage) message).readLong();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public short readShort() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readShort()");
      }

      return ((BytesMessage) message).readShort();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public int readUnsignedByte() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readUnsignedByte()");
      }

      return ((BytesMessage) message).readUnsignedByte();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public int readUnsignedShort() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readUnsignedShort()");
      }

      return ((BytesMessage) message).readUnsignedShort();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public String readUTF() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("readUTF()");
      }

      return ((BytesMessage) message).readUTF();
   }

   /**
    * Reset
    *
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void reset() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("reset()");
      }

      ((BytesMessage) message).reset();
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeBoolean(final boolean value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeBoolean(" + value + ")");
      }

      ((BytesMessage) message).writeBoolean(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeByte(final byte value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeByte(" + value + ")");
      }

      ((BytesMessage) message).writeByte(value);
   }

   /**
    * Write
    *
    * @param value  The value
    * @param offset The offset
    * @param length The length
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeBytes(final byte[] value, final int offset, final int length) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeBytes(" + Arrays.toString(value) + ", " + offset + ", " + length + ")");
      }

      ((BytesMessage) message).writeBytes(value, offset, length);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeBytes(final byte[] value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeBytes(" + Arrays.toString(value) + ")");
      }

      ((BytesMessage) message).writeBytes(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeChar(final char value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeChar(" + value + ")");
      }

      ((BytesMessage) message).writeChar(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeDouble(final double value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeDouble(" + value + ")");
      }

      ((BytesMessage) message).writeDouble(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeFloat(final float value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeFloat(" + value + ")");
      }

      ((BytesMessage) message).writeFloat(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeInt(final int value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeInt(" + value + ")");
      }

      ((BytesMessage) message).writeInt(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeLong(final long value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeLong(" + value + ")");
      }

      ((BytesMessage) message).writeLong(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeObject(final Object value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeObject(" + value + ")");
      }

      ((BytesMessage) message).writeObject(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeShort(final short value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeShort(" + value + ")");
      }

      ((BytesMessage) message).writeShort(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeUTF(final String value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("writeUTF(" + value + ")");
      }

      ((BytesMessage) message).writeUTF(value);
   }
}
