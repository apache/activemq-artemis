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
package org.apache.activemq.artemis.jms.tests.message;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * Foreign message implementation. Used for testing only.
 */
public class SimpleJMSMessage implements Message {

   private boolean ignoreSetDestination;

   // Constructors --------------------------------------------------

   public SimpleJMSMessage() {
      properties.put("JMSXDeliveryCount", new Integer(0));
   }

   /*
    * This constructor is used to simulate an activemq message in which the set of the destination is ignored after receipt.
    */
   public SimpleJMSMessage(final Destination dest) {
      this();
      ignoreSetDestination = true;
      destination = dest;
   }

   // Message implementation ----------------------------------------

   private String messageID;

   public String getJMSMessageID() throws JMSException {
      return messageID;
   }

   public void setJMSMessageID(final String id) throws JMSException {
      messageID = id;
   }

   private long timestamp;

   public long getJMSTimestamp() throws JMSException {
      return timestamp;
   }

   public void setJMSTimestamp(final long timestamp) throws JMSException {
      this.timestamp = timestamp;
   }

   //
   // TODO Is this really the spec?
   //

   private byte[] correlationIDBytes;

   private String correlationIDString;

   private boolean isCorrelationIDBytes;

   public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
      if (!isCorrelationIDBytes) {
         throw new JMSException("CorrelationID is a String for this message");
      }
      return correlationIDBytes;
   }

   public void setJMSCorrelationIDAsBytes(final byte[] correlationID) throws JMSException {
      if (correlationID == null || correlationID.length == 0) {
         throw new JMSException("Please specify a non-zero length byte[]");
      }
      correlationIDBytes = correlationID;
      isCorrelationIDBytes = true;
   }

   public void setJMSCorrelationID(final String correlationID) throws JMSException {
      correlationIDString = correlationID;
      isCorrelationIDBytes = false;
   }

   public String getJMSCorrelationID() throws JMSException {

      return correlationIDString;
   }

   private Destination replyTo;

   public Destination getJMSReplyTo() throws JMSException {
      return replyTo;
   }

   public void setJMSReplyTo(final Destination replyTo) throws JMSException {
      this.replyTo = replyTo;
   }

   private Destination destination;

   public Destination getJMSDestination() throws JMSException {
      return destination;
   }

   public void setJMSDestination(final Destination destination) throws JMSException {
      if (!ignoreSetDestination) {
         this.destination = destination;
      }
   }

   private int deliveryMode = DeliveryMode.PERSISTENT;

   public int getJMSDeliveryMode() throws JMSException {
      return deliveryMode;
   }

   public void setJMSDeliveryMode(final int deliveryMode) throws JMSException {
      this.deliveryMode = deliveryMode;
   }

   private boolean redelivered;

   public boolean getJMSRedelivered() throws JMSException {
      return redelivered;
   }

   public void setJMSRedelivered(final boolean redelivered) throws JMSException {
      this.redelivered = redelivered;
   }

   private String type;

   public String getJMSType() throws JMSException {
      return type;
   }

   public void setJMSType(final String type) throws JMSException {
      this.type = type;
   }

   private long expiration;

   public long getJMSExpiration() throws JMSException {
      return expiration;
   }

   public void setJMSExpiration(final long expiration) throws JMSException {
      this.expiration = expiration;
   }

   private int priority;

   public int getJMSPriority() throws JMSException {
      return priority;
   }

   public void setJMSPriority(final int priority) throws JMSException {
      this.priority = priority;
   }

   private final Map<String, Object> properties = new HashMap<String, Object>();

   public void clearProperties() throws JMSException {
      properties.clear();
   }

   public boolean propertyExists(final String name) throws JMSException {
      return properties.containsKey(name);
   }

   public boolean getBooleanProperty(final String name) throws JMSException {
      Object prop = properties.get(name);
      if (!(prop instanceof Boolean)) {
         throw new JMSException("Not boolean");
      }
      return ((Boolean) properties.get(name)).booleanValue();
   }

   public byte getByteProperty(final String name) throws JMSException {
      Object prop = properties.get(name);
      if (!(prop instanceof Byte)) {
         throw new JMSException("Not byte");
      }
      return ((Byte) properties.get(name)).byteValue();
   }

   public short getShortProperty(final String name) throws JMSException {
      Object prop = properties.get(name);
      if (!(prop instanceof Short)) {
         throw new JMSException("Not short");
      }
      return ((Short) properties.get(name)).shortValue();
   }

   public int getIntProperty(final String name) throws JMSException {
      Object prop = properties.get(name);
      if (!(prop instanceof Integer)) {
         throw new JMSException("Not int");
      }
      return ((Integer) properties.get(name)).intValue();
   }

   public long getLongProperty(final String name) throws JMSException {
      Object prop = properties.get(name);
      if (!(prop instanceof Long)) {
         throw new JMSException("Not long");
      }
      return ((Long) properties.get(name)).longValue();
   }

   public float getFloatProperty(final String name) throws JMSException {
      Object prop = properties.get(name);
      if (!(prop instanceof Float)) {
         throw new JMSException("Not float");
      }
      return ((Float) properties.get(name)).floatValue();
   }

   public double getDoubleProperty(final String name) throws JMSException {
      Object prop = properties.get(name);
      if (!(prop instanceof Double)) {
         throw new JMSException("Not double");
      }
      return ((Double) properties.get(name)).doubleValue();
   }

   public String getStringProperty(final String name) throws JMSException {
      Object prop = properties.get(name);
      if (!(prop instanceof String)) {
         throw new JMSException("Not string");
      }
      return (String) properties.get(name);
   }

   public Object getObjectProperty(final String name) throws JMSException {
      return properties.get(name);
   }

   public Enumeration getPropertyNames() throws JMSException {
      return Collections.enumeration(properties.keySet());
   }

   public void setBooleanProperty(final String name, final boolean value) throws JMSException {
      properties.put(name, new Boolean(value));
   }

   public void setByteProperty(final String name, final byte value) throws JMSException {
      properties.put(name, new Byte(value));
   }

   public void setShortProperty(final String name, final short value) throws JMSException {
      properties.put(name, new Short(value));
   }

   public void setIntProperty(final String name, final int value) throws JMSException {
      properties.put(name, new Integer(value));
   }

   public void setLongProperty(final String name, final long value) throws JMSException {
      properties.put(name, new Long(value));
   }

   public void setFloatProperty(final String name, final float value) throws JMSException {
      properties.put(name, new Float(value));
   }

   public void setDoubleProperty(final String name, final double value) throws JMSException {
      properties.put(name, new Double(value));
   }

   public void setStringProperty(final String name, final String value) throws JMSException {
      properties.put(name, value);
   }

   public void setObjectProperty(final String name, final Object value) throws JMSException {
      properties.put(name, value);
   }

   public void acknowledge() throws JMSException {
   }

   public void clearBody() throws JMSException {
   }

   @Override
   public long getJMSDeliveryTime() throws JMSException {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public <T> T getBody(Class<T> c) throws JMSException {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public boolean isBodyAssignableTo(Class c) throws JMSException {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
