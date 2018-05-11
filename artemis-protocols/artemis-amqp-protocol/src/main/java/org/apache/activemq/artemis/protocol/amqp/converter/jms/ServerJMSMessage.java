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

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.reader.MessageUtil;

import static org.apache.activemq.artemis.api.core.FilterConstants.NATIVE_MESSAGE_ID;
import static org.apache.activemq.artemis.api.core.Message.BYTES_TYPE;
import static org.apache.activemq.artemis.api.core.Message.MAP_TYPE;
import static org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.STREAM_TYPE;
import static org.apache.activemq.artemis.api.core.Message.TEXT_TYPE;

public class ServerJMSMessage implements Message {

   protected final ICoreMessage message;
   private ActiveMQBuffer readBodyBuffer;

   public ServerJMSMessage(ICoreMessage message) {
      this.message = message;
   }

   public static ServerJMSMessage wrapCoreMessage(ICoreMessage wrapped) {
      switch (wrapped.getType()) {
         case STREAM_TYPE:
            return new ServerJMSStreamMessage(wrapped);
         case BYTES_TYPE:
            return new ServerJMSBytesMessage(wrapped);
         case MAP_TYPE:
            return new ServerJMSMapMessage(wrapped);
         case TEXT_TYPE:
            return new ServerJMSTextMessage(wrapped);
         case OBJECT_TYPE:
            return new ServerJMSObjectMessage(wrapped);
         default:
            return new ServerJMSMessage(wrapped);
      }
   }

   public ICoreMessage getInnerMessage() {
      return message;
   }

   /**
    * When reading we use a protected copy so multi-threads can work fine
    */
   protected ActiveMQBuffer getReadBodyBuffer() {
      if (readBodyBuffer == null) {
         // to avoid clashes between multiple threads
         readBodyBuffer = message.getDataBuffer();
      }
      return readBodyBuffer;
   }

   /**
    * When writing on the conversion we use the buffer directly
    */
   protected ActiveMQBuffer getWriteBodyBuffer() {
      readBodyBuffer = null; // it invalidates this buffer if anything is written
      return message.getBodyBuffer();
   }

   @Override
   public final String getJMSMessageID() throws JMSException {
      if (message.containsProperty(NATIVE_MESSAGE_ID)) {
         return getStringProperty(NATIVE_MESSAGE_ID);
      }
      return null;
   }

   @Override
   public final void setJMSMessageID(String id) throws JMSException {
      if (id != null) {
         message.putStringProperty(NATIVE_MESSAGE_ID, id);
      }
   }

   @Override
   public final long getJMSTimestamp() throws JMSException {
      return message.getTimestamp();
   }

   @Override
   public final void setJMSTimestamp(long timestamp) throws JMSException {
      message.setTimestamp(timestamp);
   }

   @Override
   public final byte[] getJMSCorrelationIDAsBytes() throws JMSException {
      return MessageUtil.getJMSCorrelationIDAsBytes(message);
   }

   @Override
   public final void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
      try {
         MessageUtil.setJMSCorrelationIDAsBytes(message, correlationID);
      } catch (ActiveMQException e) {
         throw new JMSException(e.getMessage());
      }
   }

   @Override
   public final String getJMSCorrelationID() throws JMSException {
      return MessageUtil.getJMSCorrelationID(message);
   }

   @Override
   public final void setJMSCorrelationID(String correlationID) throws JMSException {
      MessageUtil.setJMSCorrelationID(message, correlationID);
   }

   @Override
   public final Destination getJMSReplyTo() throws JMSException {
      SimpleString reply = MessageUtil.getJMSReplyTo(message);
      if (reply != null) {
         return new ServerDestination(reply);
      } else {
         return null;
      }
   }

   @Override
   public final void setJMSReplyTo(Destination replyTo) throws JMSException {
      MessageUtil.setJMSReplyTo(message, replyTo == null ? null : ((ActiveMQDestination) replyTo).getSimpleAddress());
   }

   @Override
   public final Destination getJMSDestination() throws JMSException {
      SimpleString sdest = message.getAddressSimpleString();

      if (sdest == null) {
         return null;
      } else {
         return new ServerDestination(sdest);
      }
   }

   @Override
   public final void setJMSDestination(Destination destination) throws JMSException {
      if (destination == null) {
         message.setAddress((SimpleString)null);
      } else {
         message.setAddress(((ActiveMQDestination) destination).getSimpleAddress());
      }

   }

   @Override
   public final int getJMSDeliveryMode() throws JMSException {
      return message.isDurable() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
   }

   @Override
   public final void setJMSDeliveryMode(int deliveryMode) throws JMSException {
      if (deliveryMode == DeliveryMode.PERSISTENT) {
         message.setDurable(true);
      } else if (deliveryMode == DeliveryMode.NON_PERSISTENT) {
         message.setDurable(false);
      } else {
         throw new JMSException("Invalid mode " + deliveryMode);
      }
   }

   @Override
   public final boolean getJMSRedelivered() throws JMSException {
      return false;
   }

   @Override
   public final void setJMSRedelivered(boolean redelivered) throws JMSException {
      // no op
   }

   @Override
   public final String getJMSType() throws JMSException {
      return MessageUtil.getJMSType(message);
   }

   @Override
   public final void setJMSType(String type) throws JMSException {
      MessageUtil.setJMSType(message, type);
   }

   @Override
   public final long getJMSExpiration() throws JMSException {
      return message.getExpiration();
   }

   @Override
   public final void setJMSExpiration(long expiration) throws JMSException {
      message.setExpiration(expiration);
   }

   @Override
   public final long getJMSDeliveryTime() throws JMSException {
      // no op
      return 0;
   }

   @Override
   public final void setJMSDeliveryTime(long deliveryTime) throws JMSException {
      // no op
   }

   @Override
   public final int getJMSPriority() throws JMSException {
      return message.getPriority();
   }

   @Override
   public final void setJMSPriority(int priority) throws JMSException {
      message.setPriority((byte) priority);
   }

   @Override
   public final void clearProperties() throws JMSException {
      MessageUtil.clearProperties(message);

   }

   @Override
   public final boolean propertyExists(String name) throws JMSException {
      return MessageUtil.propertyExists(message, name);
   }

   @Override
   public final boolean getBooleanProperty(String name) throws JMSException {
      return message.getBooleanProperty(name);
   }

   @Override
   public final byte getByteProperty(String name) throws JMSException {
      return message.getByteProperty(name);
   }

   @Override
   public final short getShortProperty(String name) throws JMSException {
      return message.getShortProperty(name);
   }

   @Override
   public final int getIntProperty(String name) throws JMSException {
      return message.getIntProperty(name);
   }

   @Override
   public final long getLongProperty(String name) throws JMSException {
      return message.getLongProperty(name);
   }

   @Override
   public final float getFloatProperty(String name) throws JMSException {
      return message.getFloatProperty(name);
   }

   @Override
   public final double getDoubleProperty(String name) throws JMSException {
      return message.getDoubleProperty(name);
   }

   @Override
   public final String getStringProperty(String name) throws JMSException {
      return message.getStringProperty(name);
   }

   @Override
   public final Object getObjectProperty(String name) throws JMSException {
      Object val = message.getObjectProperty(name);
      if (val instanceof SimpleString) {
         val = ((SimpleString) val).toString();
      }
      return val;
   }

   @Override
   public final Enumeration getPropertyNames() throws JMSException {
      return Collections.enumeration(MessageUtil.getPropertyNames(message));
   }

   @Override
   public final void setBooleanProperty(String name, boolean value) throws JMSException {
      message.putBooleanProperty(name, value);
   }

   @Override
   public final void setByteProperty(String name, byte value) throws JMSException {
      message.putByteProperty(name, value);
   }

   @Override
   public final void setShortProperty(String name, short value) throws JMSException {
      message.putShortProperty(name, value);
   }

   @Override
   public final void setIntProperty(String name, int value) throws JMSException {
      message.putIntProperty(name, value);
   }

   @Override
   public final void setLongProperty(String name, long value) throws JMSException {
      message.putLongProperty(name, value);
   }

   @Override
   public final void setFloatProperty(String name, float value) throws JMSException {
      message.putFloatProperty(name, value);
   }

   @Override
   public final void setDoubleProperty(String name, double value) throws JMSException {
      message.putDoubleProperty(name, value);
   }

   @Override
   public final void setStringProperty(String name, String value) throws JMSException {
      message.putStringProperty(name, value);
   }

   @Override
   public final void setObjectProperty(String name, Object value) throws JMSException {
      message.putObjectProperty(name, value);
   }

   @Override
   public final void acknowledge() throws JMSException {
      // no op
   }

   @Override
   public void clearBody() throws JMSException {
      message.getBodyBuffer().clear();
   }

   @Override
   public final <T> T getBody(Class<T> c) throws JMSException {
      // no op.. jms2 not used on the conversion
      return null;
   }

   /**
    * Encode the body into the internal message
    */
   public void encode() throws Exception {
      if (!message.isLargeMessage()) {
         message.getBodyBuffer().resetReaderIndex();
      }
   }

   public void decode() throws Exception {
      if (!message.isLargeMessage()) {
         message.getBodyBuffer().resetReaderIndex();
      }
   }

   @Override
   public final boolean isBodyAssignableTo(Class c) throws JMSException {
      // no op.. jms2 not used on the conversion
      return false;
   }
}
