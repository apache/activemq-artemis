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

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.utils.collections.TypedProperties;

import static org.apache.activemq.artemis.reader.MapMessageUtil.readBodyMap;
import static org.apache.activemq.artemis.reader.MapMessageUtil.writeBodyMap;

/**
 * ActiveMQ Artemis implementation of a JMS MapMessage.
 */
public final class ServerJMSMapMessage extends ServerJMSMessage implements MapMessage {
   // Constants -----------------------------------------------------

   public static final byte TYPE = Message.MAP_TYPE;

   // Attributes ----------------------------------------------------

   private final TypedProperties map = new TypedProperties();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /*
    * This constructor is used to construct messages prior to sending
    */
   public ServerJMSMapMessage(ICoreMessage message) {
      super(message);

   }

   // MapMessage implementation -------------------------------------

   @Override
   public void setBoolean(final String name, final boolean value) throws JMSException {
      map.putBooleanProperty(SimpleString.toSimpleString(name), value);
   }

   @Override
   public void setByte(final String name, final byte value) throws JMSException {
      map.putByteProperty(SimpleString.toSimpleString(name), value);
   }

   @Override
   public void setShort(final String name, final short value) throws JMSException {
      map.putShortProperty(SimpleString.toSimpleString(name), value);
   }

   @Override
   public void setChar(final String name, final char value) throws JMSException {
      map.putCharProperty(SimpleString.toSimpleString(name), value);
   }

   @Override
   public void setInt(final String name, final int value) throws JMSException {
      map.putIntProperty(SimpleString.toSimpleString(name), value);
   }

   @Override
   public void setLong(final String name, final long value) throws JMSException {
      map.putLongProperty(SimpleString.toSimpleString(name), value);
   }

   @Override
   public void setFloat(final String name, final float value) throws JMSException {
      map.putFloatProperty(SimpleString.toSimpleString(name), value);
   }

   @Override
   public void setDouble(final String name, final double value) throws JMSException {
      map.putDoubleProperty(SimpleString.toSimpleString(name), value);
   }

   @Override
   public void setString(final String name, final String value) throws JMSException {
      map.putSimpleStringProperty(SimpleString.toSimpleString(name), value == null ? null : SimpleString.toSimpleString(value));
   }

   @Override
   public void setBytes(final String name, final byte[] value) throws JMSException {
      map.putBytesProperty(SimpleString.toSimpleString(name), value);
   }

   @Override
   public void setBytes(final String name, final byte[] value, final int offset, final int length) throws JMSException {
      if (offset + length > value.length) {
         throw new JMSException("Invalid offset/length");
      }
      byte[] newBytes = new byte[length];
      System.arraycopy(value, offset, newBytes, 0, length);
      map.putBytesProperty(SimpleString.toSimpleString(name), newBytes);
   }

   @Override
   public void setObject(final String name, final Object value) throws JMSException {
      try {
         TypedProperties.setObjectProperty(SimpleString.toSimpleString(name), value, map);
      } catch (ActiveMQPropertyConversionException e) {
         throw new MessageFormatException(e.getMessage());
      }
   }

   @Override
   public boolean getBoolean(final String name) throws JMSException {
      try {
         return map.getBooleanProperty(SimpleString.toSimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new MessageFormatException(e.getMessage());
      }
   }

   @Override
   public byte getByte(final String name) throws JMSException {
      try {
         return map.getByteProperty(SimpleString.toSimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new MessageFormatException(e.getMessage());
      }
   }

   @Override
   public short getShort(final String name) throws JMSException {
      try {
         return map.getShortProperty(SimpleString.toSimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new MessageFormatException(e.getMessage());
      }
   }

   @Override
   public char getChar(final String name) throws JMSException {
      try {
         return map.getCharProperty(SimpleString.toSimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new MessageFormatException(e.getMessage());
      }
   }

   @Override
   public int getInt(final String name) throws JMSException {
      try {
         return map.getIntProperty(SimpleString.toSimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new MessageFormatException(e.getMessage());
      }
   }

   @Override
   public long getLong(final String name) throws JMSException {
      try {
         return map.getLongProperty(SimpleString.toSimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new MessageFormatException(e.getMessage());
      }
   }

   @Override
   public float getFloat(final String name) throws JMSException {
      try {
         return map.getFloatProperty(SimpleString.toSimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new MessageFormatException(e.getMessage());
      }
   }

   @Override
   public double getDouble(final String name) throws JMSException {
      try {
         return map.getDoubleProperty(SimpleString.toSimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new MessageFormatException(e.getMessage());
      }
   }

   @Override
   public String getString(final String name) throws JMSException {
      try {
         SimpleString str = map.getSimpleStringProperty(SimpleString.toSimpleString(name));
         if (str == null) {
            return null;
         } else {
            return str.toString();
         }
      } catch (ActiveMQPropertyConversionException e) {
         throw new MessageFormatException(e.getMessage());
      }
   }

   @Override
   public byte[] getBytes(final String name) throws JMSException {
      try {
         return map.getBytesProperty(SimpleString.toSimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new MessageFormatException(e.getMessage());
      }
   }

   @Override
   public Object getObject(final String name) throws JMSException {
      Object val = map.getProperty(SimpleString.toSimpleString(name));

      if (val instanceof SimpleString) {
         val = ((SimpleString) val).toString();
      }

      return val;
   }

   @Override
   public Enumeration getMapNames() throws JMSException {
      Set<SimpleString> simplePropNames = map.getPropertyNames();
      Set<String> propNames = new HashSet<>(simplePropNames.size());

      for (SimpleString str : simplePropNames) {
         propNames.add(str.toString());
      }

      return Collections.enumeration(propNames);
   }

   @Override
   public boolean itemExists(final String name) throws JMSException {
      return map.containsProperty(SimpleString.toSimpleString(name));
   }

   @Override
   public void clearBody() throws JMSException {
      super.clearBody();

      map.clear();
   }

   @Override
   public void encode() throws Exception {
      super.encode();
      writeBodyMap(getWriteBodyBuffer(), map);
   }

   @Override
   public void decode() throws Exception {
      super.decode();
      readBodyMap(getReadBodyBuffer(), map);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

}
