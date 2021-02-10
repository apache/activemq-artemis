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


import java.util.Collections;
import java.util.Enumeration;

import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;

import static org.apache.activemq.artemis.reader.MapMessageUtil.readBodyMap;
import static org.apache.activemq.artemis.reader.MapMessageUtil.writeBodyMap;

/**
 * ActiveMQ Artemis implementation of a JMS MapMessage.
 */
public final class ServerJMSMapMessage extends ServerJMSMessage  {
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

   public void setBoolean(final String name, final boolean value) throws Exception {
      map.putBooleanProperty(new SimpleString(name), value);
   }

   public void setByte(final String name, final byte value) throws Exception {
      map.putByteProperty(new SimpleString(name), value);
   }

   public void setShort(final String name, final short value) throws Exception {
      map.putShortProperty(new SimpleString(name), value);
   }

   public void setChar(final String name, final char value) throws Exception {
      map.putCharProperty(new SimpleString(name), value);
   }

   public void setInt(final String name, final int value) throws Exception {
      map.putIntProperty(new SimpleString(name), value);
   }

   public void setLong(final String name, final long value) throws Exception {
      map.putLongProperty(new SimpleString(name), value);
   }

   public void setFloat(final String name, final float value) throws Exception {
      map.putFloatProperty(new SimpleString(name), value);
   }

   public void setDouble(final String name, final double value) throws Exception {
      map.putDoubleProperty(new SimpleString(name), value);
   }

   public void setString(final String name, final String value) throws Exception {
      map.putSimpleStringProperty(new SimpleString(name), value == null ? null : new SimpleString(value));
   }

   public void setBytes(final String name, final byte[] value) throws Exception {
      map.putBytesProperty(new SimpleString(name), value);
   }

   public void setBytes(final String name, final byte[] value, final int offset, final int length) throws Exception {
      if (offset + length > value.length) {
         throw new Exception("Invalid offset/length");
      }
      byte[] newBytes = new byte[length];
      System.arraycopy(value, offset, newBytes, 0, length);
      map.putBytesProperty(new SimpleString(name), newBytes);
   }

   public void setObject(final String name, final Object value) throws Exception {
      try {
         // primitives and String
         Object val = value;
         if (value instanceof UnsignedInteger) {
            val = ((UnsignedInteger) value).intValue();
         } else if (value instanceof UnsignedShort) {
            val = ((UnsignedShort) value).shortValue();
         } else if (value instanceof UnsignedByte) {
            val = ((UnsignedByte) value).byteValue();
         } else if (value instanceof UnsignedLong) {
            val = ((UnsignedLong) value).longValue();
         }
         TypedProperties.setObjectProperty(new SimpleString(name), val, map);
      } catch (ActiveMQPropertyConversionException e) {
         throw new RuntimeException(e.getMessage());
      }
   }

   public boolean getBoolean(final String name) throws Exception {
      try {
         return map.getBooleanProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new RuntimeException(e.getMessage());
      }
   }

   public byte getByte(final String name) throws Exception {
      try {
         return map.getByteProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new RuntimeException(e.getMessage());
      }
   }

   public short getShort(final String name) throws Exception {
      try {
         return map.getShortProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new RuntimeException(e.getMessage());
      }
   }

   public char getChar(final String name) throws Exception {
      try {
         return map.getCharProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new RuntimeException(e.getMessage());
      }
   }

   public int getInt(final String name) throws Exception {
      try {
         return map.getIntProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new RuntimeException(e.getMessage());
      }
   }

   public long getLong(final String name) throws Exception {
      try {
         return map.getLongProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new RuntimeException(e.getMessage());
      }
   }

   public float getFloat(final String name) throws Exception {
      try {
         return map.getFloatProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new RuntimeException(e.getMessage());
      }
   }

   public double getDouble(final String name) throws Exception {
      try {
         return map.getDoubleProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new RuntimeException(e.getMessage());
      }
   }

   public String getString(final String name) throws Exception {
      try {
         SimpleString str = map.getSimpleStringProperty(new SimpleString(name));
         if (str == null) {
            return null;
         } else {
            return str.toString();
         }
      } catch (ActiveMQPropertyConversionException e) {
         throw new RuntimeException(e.getMessage());
      }
   }

   public byte[] getBytes(final String name) throws Exception {
      try {
         return map.getBytesProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException e) {
         throw new RuntimeException(e.getMessage());
      }
   }

   public Object getObject(final String name) throws Exception {
      Object val = map.getProperty(new SimpleString(name));

      if (val instanceof SimpleString) {
         val = ((SimpleString) val).toString();
      }

      return val;
   }

   public Enumeration getMapNames() throws Exception {
      return Collections.enumeration(map.getMapNames());
   }

   public boolean itemExists(final String name) throws Exception {
      return map.containsProperty(new SimpleString(name));
   }

   @Override
   public void clearBody() throws Exception {
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
