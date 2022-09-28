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

import javax.jms.JMSException;
import javax.jms.StreamMessage;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for a message
 */
public class ActiveMQRAStreamMessage extends ActiveMQRAMessage implements StreamMessage {

   private static final Logger logger = LoggerFactory.getLogger(ActiveMQRAStreamMessage.class);

   /**
    * Create a new wrapper
    *
    * @param message the message
    * @param session the session
    */
   public ActiveMQRAStreamMessage(final StreamMessage message, final ActiveMQRASession session) {
      super(message, session);

      if (logger.isTraceEnabled()) {
         logger.trace("constructor(" + message + ", " + session + ")");
      }
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public boolean readBoolean() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("readBoolean()");
      }

      return ((StreamMessage) message).readBoolean();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public byte readByte() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("readByte()");
      }

      return ((StreamMessage) message).readByte();
   }

   /**
    * Read
    *
    * @param value The value
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public int readBytes(final byte[] value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("readBytes(" + Arrays.toString(value) + ")");
      }

      return ((StreamMessage) message).readBytes(value);
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public char readChar() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("readChar()");
      }

      return ((StreamMessage) message).readChar();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public double readDouble() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("readDouble()");
      }

      return ((StreamMessage) message).readDouble();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public float readFloat() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("readFloat()");
      }

      return ((StreamMessage) message).readFloat();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public int readInt() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("readInt()");
      }

      return ((StreamMessage) message).readInt();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public long readLong() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("readLong()");
      }

      return ((StreamMessage) message).readLong();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Object readObject() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("readObject()");
      }

      return ((StreamMessage) message).readObject();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public short readShort() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("readShort()");
      }

      return ((StreamMessage) message).readShort();
   }

   /**
    * Read
    *
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public String readString() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("readString()");
      }

      return ((StreamMessage) message).readString();
   }

   /**
    * Reset
    *
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void reset() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("reset()");
      }

      ((StreamMessage) message).reset();
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeBoolean(final boolean value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("writeBoolean(" + value + ")");
      }

      ((StreamMessage) message).writeBoolean(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeByte(final byte value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("writeByte(" + value + ")");
      }

      ((StreamMessage) message).writeByte(value);
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
      if (logger.isTraceEnabled()) {
         logger.trace("writeBytes(" + Arrays.toString(value) + ", " + offset + ", " + length + ")");
      }

      ((StreamMessage) message).writeBytes(value, offset, length);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeBytes(final byte[] value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("writeBytes(" + Arrays.toString(value) + ")");
      }

      ((StreamMessage) message).writeBytes(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeChar(final char value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("writeChar(" + value + ")");
      }

      ((StreamMessage) message).writeChar(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeDouble(final double value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("writeDouble(" + value + ")");
      }

      ((StreamMessage) message).writeDouble(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeFloat(final float value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("writeFloat(" + value + ")");
      }

      ((StreamMessage) message).writeFloat(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeInt(final int value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("writeInt(" + value + ")");
      }

      ((StreamMessage) message).writeInt(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeLong(final long value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("writeLong(" + value + ")");
      }

      ((StreamMessage) message).writeLong(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeObject(final Object value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("writeObject(" + value + ")");
      }

      ((StreamMessage) message).writeObject(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeShort(final short value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("writeShort(" + value + ")");
      }

      ((StreamMessage) message).writeShort(value);
   }

   /**
    * Write
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void writeString(final String value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("writeString(" + value + ")");
      }

      ((StreamMessage) message).writeString(value);
   }
}
