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
import javax.jms.MapMessage;
import java.util.Arrays;
import java.util.Enumeration;

/**
 * A wrapper for a message
 */
public class ActiveMQRAMapMessage extends ActiveMQRAMessage implements MapMessage {

   /**
    * Create a new wrapper
    *
    * @param message the message
    * @param session the session
    */
   public ActiveMQRAMapMessage(final MapMessage message, final ActiveMQRASession session) {
      super(message, session);

      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("constructor(" + message + ", " + session + ")");
      }
   }

   /**
    * Get
    *
    * @param name The name
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public boolean getBoolean(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getBoolean(" + name + ")");
      }

      return ((MapMessage) message).getBoolean(name);
   }

   /**
    * Get
    *
    * @param name The name
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public byte getByte(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getByte(" + name + ")");
      }

      return ((MapMessage) message).getByte(name);
   }

   /**
    * Get
    *
    * @param name The name
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public byte[] getBytes(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getBytes(" + name + ")");
      }

      return ((MapMessage) message).getBytes(name);
   }

   /**
    * Get
    *
    * @param name The name
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public char getChar(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getChar(" + name + ")");
      }

      return ((MapMessage) message).getChar(name);
   }

   /**
    * Get
    *
    * @param name The name
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public double getDouble(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getDouble(" + name + ")");
      }

      return ((MapMessage) message).getDouble(name);
   }

   /**
    * Get
    *
    * @param name The name
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public float getFloat(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getFloat(" + name + ")");
      }

      return ((MapMessage) message).getFloat(name);
   }

   /**
    * Get
    *
    * @param name The name
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public int getInt(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getInt(" + name + ")");
      }

      return ((MapMessage) message).getInt(name);
   }

   /**
    * Get
    *
    * @param name The name
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public long getLong(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getLong(" + name + ")");
      }

      return ((MapMessage) message).getLong(name);
   }

   /**
    * Get the map names
    *
    * @return The values
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Enumeration getMapNames() throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getMapNames()");
      }

      return ((MapMessage) message).getMapNames();
   }

   /**
    * Get
    *
    * @param name The name
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Object getObject(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getObject(" + name + ")");
      }

      return ((MapMessage) message).getObject(name);
   }

   /**
    * Get
    *
    * @param name The name
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public short getShort(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getShort(" + name + ")");
      }

      return ((MapMessage) message).getShort(name);
   }

   /**
    * Get
    *
    * @param name The name
    * @return The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public String getString(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("getString(" + name + ")");
      }

      return ((MapMessage) message).getString(name);
   }

   /**
    * Does the item exist
    *
    * @param name The name
    * @return True / false
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public boolean itemExists(final String name) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("itemExists(" + name + ")");
      }

      return ((MapMessage) message).itemExists(name);
   }

   /**
    * Set
    *
    * @param name  The name
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setBoolean(final String name, final boolean value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setBoolean(" + name + ", " + value + ")");
      }

      ((MapMessage) message).setBoolean(name, value);
   }

   /**
    * Set
    *
    * @param name  The name
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setByte(final String name, final byte value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setByte(" + name + ", " + value + ")");
      }

      ((MapMessage) message).setByte(name, value);
   }

   /**
    * Set
    *
    * @param name   The name
    * @param value  The value
    * @param offset The offset
    * @param length The length
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setBytes(final String name, final byte[] value, final int offset, final int length) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setBytes(" + name + ", " + Arrays.toString(value) + ", " + offset + ", " +
                                          length + ")");
      }

      ((MapMessage) message).setBytes(name, value, offset, length);
   }

   /**
    * Set
    *
    * @param name  The name
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setBytes(final String name, final byte[] value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setBytes(" + name + ", " + Arrays.toString(value) + ")");
      }

      ((MapMessage) message).setBytes(name, value);
   }

   /**
    * Set
    *
    * @param name  The name
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setChar(final String name, final char value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setChar(" + name + ", " + value + ")");
      }

      ((MapMessage) message).setChar(name, value);
   }

   /**
    * Set
    *
    * @param name  The name
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setDouble(final String name, final double value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setDouble(" + name + ", " + value + ")");
      }

      ((MapMessage) message).setDouble(name, value);
   }

   /**
    * Set
    *
    * @param name  The name
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setFloat(final String name, final float value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setFloat(" + name + ", " + value + ")");
      }

      ((MapMessage) message).setFloat(name, value);
   }

   /**
    * Set
    *
    * @param name  The name
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setInt(final String name, final int value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setInt(" + name + ", " + value + ")");
      }

      ((MapMessage) message).setInt(name, value);
   }

   /**
    * Set
    *
    * @param name  The name
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setLong(final String name, final long value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setLong(" + name + ", " + value + ")");
      }

      ((MapMessage) message).setLong(name, value);
   }

   /**
    * Set
    *
    * @param name  The name
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setObject(final String name, final Object value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setObject(" + name + ", " + value + ")");
      }

      ((MapMessage) message).setObject(name, value);
   }

   /**
    * Set
    *
    * @param name  The name
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setShort(final String name, final short value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setShort(" + name + ", " + value + ")");
      }

      ((MapMessage) message).setShort(name, value);
   }

   /**
    * Set
    *
    * @param name  The name
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setString(final String name, final String value) throws JMSException {
      if (ActiveMQRALogger.LOGGER.isTraceEnabled()) {
         ActiveMQRALogger.LOGGER.trace("setString(" + name + ", " + value + ")");
      }

      ((MapMessage) message).setString(name, value);
   }
}
