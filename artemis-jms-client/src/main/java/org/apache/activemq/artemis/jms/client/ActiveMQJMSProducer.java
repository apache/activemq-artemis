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
package org.apache.activemq.artemis.jms.client;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.utils.TypedProperties;

/**
 * NOTE: this class forwards {@link #setDisableMessageID(boolean)} and
 * {@link #setDisableMessageTimestamp(boolean)} calls their equivalent at the
 * {@link MessageProducer}. IF the user is using the producer in async mode, this may lead to races.
 * We allow/tolerate this because these are just optional optimizations.
 */
public final class ActiveMQJMSProducer implements JMSProducer {

   private final ActiveMQJMSContext context;
   private final MessageProducer producer;
   private final TypedProperties properties = new TypedProperties();

   //we convert Strings to SimpleStrings so if getProperty is called the wrong object is returned, this list let's us return the
   //correct type
   private final List<SimpleString> stringPropertyNames = new ArrayList<>();

   private volatile CompletionListener completionListener;

   private Destination jmsHeaderReplyTo;
   private String jmsHeaderCorrelationID;
   private byte[] jmsHeaderCorrelationIDAsBytes;
   private String jmsHeaderType;

   ActiveMQJMSProducer(ActiveMQJMSContext context, MessageProducer producer) {
      this.context = context;
      this.producer = producer;
   }

   @Override
   public JMSProducer send(Destination destination, Message message) {
      if (message == null) {
         throw new MessageFormatRuntimeException("null message");
      }

      try {
         if (jmsHeaderCorrelationID != null) {
            message.setJMSCorrelationID(jmsHeaderCorrelationID);
         }
         if (jmsHeaderCorrelationIDAsBytes != null && jmsHeaderCorrelationIDAsBytes.length > 0) {
            message.setJMSCorrelationIDAsBytes(jmsHeaderCorrelationIDAsBytes);
         }
         if (jmsHeaderReplyTo != null) {
            message.setJMSReplyTo(jmsHeaderReplyTo);
         }
         if (jmsHeaderType != null) {
            message.setJMSType(jmsHeaderType);
         }
         // XXX HORNETQ-1209 "JMS 2.0" can this be a foreign msg?
         // if so, then "SimpleString" properties will trigger an error.
         setProperties(message);
         if (completionListener != null) {
            CompletionListener wrapped = new CompletionListenerWrapper(completionListener);
            producer.send(destination, message, wrapped);
         } else {
            producer.send(destination, message);
         }
      } catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
      return this;
   }

   /**
    * Sets all properties we carry onto the message.
    *
    * @param message
    * @throws JMSException
    */
   private void setProperties(Message message) throws JMSException {
      for (SimpleString name : properties.getPropertyNames()) {
         message.setObjectProperty(name.toString(), properties.getProperty(name));
      }
   }

   @Override
   public JMSProducer send(Destination destination, String body) {
      TextMessage message = context.createTextMessage(body);
      send(destination, message);
      return this;
   }

   @Override
   public JMSProducer send(Destination destination, Map<String, Object> body) {
      MapMessage message = context.createMapMessage();
      if (body != null) {
         try {
            for (Entry<String, Object> entry : body.entrySet()) {
               final String name = entry.getKey();
               final Object v = entry.getValue();
               if (v instanceof String) {
                  message.setString(name, (String) v);
               } else if (v instanceof Long) {
                  message.setLong(name, (Long) v);
               } else if (v instanceof Double) {
                  message.setDouble(name, (Double) v);
               } else if (v instanceof Integer) {
                  message.setInt(name, (Integer) v);
               } else if (v instanceof Character) {
                  message.setChar(name, (Character) v);
               } else if (v instanceof Short) {
                  message.setShort(name, (Short) v);
               } else if (v instanceof Boolean) {
                  message.setBoolean(name, (Boolean) v);
               } else if (v instanceof Float) {
                  message.setFloat(name, (Float) v);
               } else if (v instanceof Byte) {
                  message.setByte(name, (Byte) v);
               } else if (v instanceof byte[]) {
                  byte[] array = (byte[]) v;
                  message.setBytes(name, array, 0, array.length);
               } else {
                  message.setObject(name, v);
               }
            }
         } catch (JMSException e) {
            throw new MessageFormatRuntimeException(e.getMessage());
         }
      }
      send(destination, message);
      return this;
   }

   @Override
   public JMSProducer send(Destination destination, byte[] body) {
      BytesMessage message = context.createBytesMessage();
      if (body != null) {
         try {
            message.writeBytes(body);
         } catch (JMSException e) {
            throw new MessageFormatRuntimeException(e.getMessage());
         }
      }
      send(destination, message);
      return this;
   }

   @Override
   public JMSProducer send(Destination destination, Serializable body) {
      ObjectMessage message = context.createObjectMessage(body);
      send(destination, message);
      return this;
   }

   @Override
   public JMSProducer setDisableMessageID(boolean value) {
      try {
         producer.setDisableMessageID(value);
      } catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
      return this;
   }

   @Override
   public boolean getDisableMessageID() {
      try {
         return producer.getDisableMessageID();
      } catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
   }

   @Override
   public JMSProducer setDisableMessageTimestamp(boolean value) {
      try {
         producer.setDisableMessageTimestamp(value);
      } catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
      return this;
   }

   @Override
   public boolean getDisableMessageTimestamp() {
      try {
         return producer.getDisableMessageTimestamp();
      } catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
   }

   @Override
   public JMSProducer setDeliveryMode(int deliveryMode) {
      try {
         producer.setDeliveryMode(deliveryMode);
      } catch (JMSException e) {
         JMSRuntimeException e2 = new JMSRuntimeException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
      return this;
   }

   @Override
   public int getDeliveryMode() {
      try {
         return producer.getDeliveryMode();
      } catch (JMSException e) {
         JMSRuntimeException e2 = new JMSRuntimeException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
   }

   @Override
   public JMSProducer setPriority(int priority) {
      try {
         producer.setPriority(priority);
      } catch (JMSException e) {
         JMSRuntimeException e2 = new JMSRuntimeException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
      return this;
   }

   @Override
   public int getPriority() {
      try {
         return producer.getPriority();
      } catch (JMSException e) {
         JMSRuntimeException e2 = new JMSRuntimeException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
   }

   @Override
   public JMSProducer setTimeToLive(long timeToLive) {
      try {
         producer.setTimeToLive(timeToLive);
         return this;
      } catch (JMSException e) {
         JMSRuntimeException e2 = new JMSRuntimeException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
   }

   @Override
   public long getTimeToLive() {
      long timeToLive = 0;
      try {
         timeToLive = producer.getTimeToLive();
         return timeToLive;
      } catch (JMSException e) {
         JMSRuntimeException e2 = new JMSRuntimeException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
   }

   @Override
   public JMSProducer setDeliveryDelay(long deliveryDelay) {
      try {
         producer.setDeliveryDelay(deliveryDelay);
         return this;
      } catch (JMSException e) {
         JMSRuntimeException e2 = new JMSRuntimeException(e.getMessage());
         e2.initCause(e);
         throw e2;
      }
   }

   @Override
   public long getDeliveryDelay() {
      long deliveryDelay = 0;
      try {
         deliveryDelay = producer.getDeliveryDelay();
      } catch (Exception ignored) {
      }
      return deliveryDelay;
   }

   @Override
   public JMSProducer setAsync(CompletionListener completionListener) {
      this.completionListener = completionListener;
      return this;
   }

   @Override
   public CompletionListener getAsync() {
      return completionListener;
   }

   @Override
   public JMSProducer setProperty(String name, boolean value) {
      checkName(name);
      properties.putBooleanProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, byte value) {
      checkName(name);
      properties.putByteProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, short value) {
      checkName(name);
      properties.putShortProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, int value) {
      checkName(name);
      properties.putIntProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, long value) {
      checkName(name);
      properties.putLongProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, float value) {
      checkName(name);
      properties.putFloatProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, double value) {
      checkName(name);
      properties.putDoubleProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, String value) {
      checkName(name);
      SimpleString key = new SimpleString(name);
      properties.putSimpleStringProperty(key, new SimpleString(value));
      stringPropertyNames.add(key);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, Object value) {
      checkName(name);
      try {
         TypedProperties.setObjectProperty(new SimpleString(name), value, properties);
      } catch (ActiveMQPropertyConversionException amqe) {
         throw new MessageFormatRuntimeException(amqe.getMessage());
      } catch (RuntimeException e) {
         throw new JMSRuntimeException(e.getMessage());
      }
      return this;
   }

   @Override
   public JMSProducer clearProperties() {
      try {
         stringPropertyNames.clear();
         properties.clear();
      } catch (RuntimeException e) {
         throw new JMSRuntimeException(e.getMessage());
      }
      return this;
   }

   @Override
   public boolean propertyExists(String name) {
      return properties.containsProperty(new SimpleString(name));
   }

   @Override
   public boolean getBooleanProperty(String name) {
      try {
         return properties.getBooleanProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException ce) {
         throw new MessageFormatRuntimeException(ce.getMessage());
      } catch (RuntimeException e) {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public byte getByteProperty(String name) {
      try {
         return properties.getByteProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException ce) {
         throw new MessageFormatRuntimeException(ce.getMessage());
      }
   }

   @Override
   public short getShortProperty(String name) {
      try {
         return properties.getShortProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException ce) {
         throw new MessageFormatRuntimeException(ce.getMessage());
      }
   }

   @Override
   public int getIntProperty(String name) {
      try {
         return properties.getIntProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException ce) {
         throw new MessageFormatRuntimeException(ce.getMessage());
      }
   }

   @Override
   public long getLongProperty(String name) {
      try {
         return properties.getLongProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException ce) {
         throw new MessageFormatRuntimeException(ce.getMessage());
      }
   }

   @Override
   public float getFloatProperty(String name) {
      try {
         return properties.getFloatProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException ce) {
         throw new MessageFormatRuntimeException(ce.getMessage());
      }
   }

   @Override
   public double getDoubleProperty(String name) {
      try {
         return properties.getDoubleProperty(new SimpleString(name));
      } catch (ActiveMQPropertyConversionException ce) {
         throw new MessageFormatRuntimeException(ce.getMessage());
      }
   }

   @Override
   public String getStringProperty(String name) {
      try {
         SimpleString prop = properties.getSimpleStringProperty(new SimpleString(name));
         if (prop == null)
            return null;
         return prop.toString();
      } catch (ActiveMQPropertyConversionException ce) {
         throw new MessageFormatRuntimeException(ce.getMessage());
      } catch (RuntimeException e) {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public Object getObjectProperty(String name) {
      try {
         SimpleString key = new SimpleString(name);
         Object property = properties.getProperty(key);
         if (stringPropertyNames.contains(key)) {
            property = property.toString();
         }
         return property;
      } catch (ActiveMQPropertyConversionException ce) {
         throw new MessageFormatRuntimeException(ce.getMessage());
      } catch (RuntimeException e) {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public Set<String> getPropertyNames() {
      try {
         Set<SimpleString> simplePropNames = properties.getPropertyNames();
         Set<String> propNames = new HashSet<>(simplePropNames.size());

         for (SimpleString str : simplePropNames) {
            propNames.add(str.toString());
         }
         return propNames;
      } catch (ActiveMQPropertyConversionException ce) {
         throw new MessageFormatRuntimeException(ce.getMessage());
      } catch (RuntimeException e) {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
      if (correlationID == null || correlationID.length == 0) {
         throw new JMSRuntimeException("Please specify a non-zero length byte[]");
      }
      jmsHeaderCorrelationIDAsBytes = Arrays.copyOf(correlationID, correlationID.length);
      return this;
   }

   @Override
   public byte[] getJMSCorrelationIDAsBytes() {
      return Arrays.copyOf(jmsHeaderCorrelationIDAsBytes, jmsHeaderCorrelationIDAsBytes.length);
   }

   @Override
   public JMSProducer setJMSCorrelationID(String correlationID) {
      jmsHeaderCorrelationID = correlationID;
      return this;
   }

   @Override
   public String getJMSCorrelationID() {
      return jmsHeaderCorrelationID;
   }

   @Override
   public JMSProducer setJMSType(String type) {
      jmsHeaderType = type;
      return this;
   }

   @Override
   public String getJMSType() {
      return jmsHeaderType;
   }

   @Override
   public JMSProducer setJMSReplyTo(Destination replyTo) {
      jmsHeaderReplyTo = replyTo;
      return this;
   }

   @Override
   public Destination getJMSReplyTo() {
      return jmsHeaderReplyTo;
   }

   private void checkName(String name) {
      if (name == null) {
         throw ActiveMQJMSClientBundle.BUNDLE.nameCannotBeNull();
      }
      if (name.equals("")) {
         throw ActiveMQJMSClientBundle.BUNDLE.nameCannotBeEmpty();
      }
   }

   final class CompletionListenerWrapper implements CompletionListener {

      private final CompletionListener wrapped;

      CompletionListenerWrapper(CompletionListener wrapped) {
         this.wrapped = wrapped;
      }

      @Override
      public void onCompletion(Message message) {
         context.getThreadAwareContext().setCurrentThread(true);
         try {
            wrapped.onCompletion(message);
         } finally {
            context.getThreadAwareContext().clearCurrentThread(true);
         }
      }

      @Override
      public void onException(Message message, Exception exception) {
         context.getThreadAwareContext().setCurrentThread(true);
         try {
            wrapped.onException(message, exception);
         } finally {
            context.getThreadAwareContext().clearCurrentThread(true);
         }
      }
   }
}
