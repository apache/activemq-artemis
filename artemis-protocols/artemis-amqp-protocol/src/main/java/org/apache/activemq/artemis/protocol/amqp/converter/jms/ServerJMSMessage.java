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

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.reader.MessageUtil;

import static org.apache.activemq.artemis.api.core.FilterConstants.NATIVE_MESSAGE_ID;
import static org.apache.activemq.artemis.api.core.Message.BYTES_TYPE;
import static org.apache.activemq.artemis.api.core.Message.MAP_TYPE;
import static org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.STREAM_TYPE;
import static org.apache.activemq.artemis.api.core.Message.TEXT_TYPE;

import static org.apache.activemq.artemis.protocol.amqp.converter.JMSConstants.DeliveryMode_NON_PERSISTENT;
import static org.apache.activemq.artemis.protocol.amqp.converter.JMSConstants.DeliveryMode_PERSISTENT;

public class ServerJMSMessage  {

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
     * @return
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
     * @return
    */
   protected ActiveMQBuffer getWriteBodyBuffer() {
      readBodyBuffer = null; // it invalidates this buffer if anything is written
      return message.getBodyBuffer();
   }

   public final String getJMSMessageID() throws Exception {
      if (message.containsProperty(NATIVE_MESSAGE_ID)) {
         return getStringProperty(NATIVE_MESSAGE_ID);
      }
      return null;
   }

   public final void setJMSMessageID(String id) throws Exception {
      if (id != null) {
         message.putStringProperty(NATIVE_MESSAGE_ID, id);
      }
   }

   public final long getJMSTimestamp() throws Exception {
      return message.getTimestamp();
   }

   public final void setJMSTimestamp(long timestamp) throws Exception {
      message.setTimestamp(timestamp);
   }

   public final byte[] getJMSCorrelationIDAsBytes() throws Exception {
      return MessageUtil.getJMSCorrelationIDAsBytes(message);
   }

   public final void setJMSCorrelationIDAsBytes(byte[] correlationID) throws Exception {
      if (correlationID == null || correlationID.length == 0) {
         throw new Exception("Please specify a non-zero length byte[]");
      }
      message.setCorrelationID(correlationID);
   }

   public final String getJMSCorrelationID() throws Exception {

      Object correlationID = message.getCorrelationID();
      if (correlationID instanceof String) {

         return ((String) correlationID);
      } else if (correlationID != null) {
         return String.valueOf(correlationID);
      } else {
         return null;
      }
   }

   public final void setJMSCorrelationID(String correlationID) throws Exception {
      message.setCorrelationID(correlationID);
   }

   public final SimpleString getJMSReplyTo() throws Exception {
      return MessageUtil.getJMSReplyTo(message);
   }

   public final void setJMSReplyTo(String replyTo) throws Exception {
      MessageUtil.setJMSReplyTo(message, SimpleString.toSimpleString(replyTo));
   }

   public SimpleString getJMSDestination() throws Exception {
      return message.getAddressSimpleString();
   }

   public final void setJMSDestination(String destination) throws Exception {
      message.setAddress(destination);
   }

   public final int getJMSDeliveryMode() throws Exception {
      return message.isDurable() ? DeliveryMode_PERSISTENT : DeliveryMode_NON_PERSISTENT;
   }

   public final void setJMSDeliveryMode(int deliveryMode) throws Exception {
      switch (deliveryMode) {
         case DeliveryMode_PERSISTENT:
            message.setDurable(true);
            break;
         case DeliveryMode_NON_PERSISTENT:
            message.setDurable(false);
            break;
         default:
            throw new Exception("Invalid mode " + deliveryMode);
      }
   }

   public final boolean getJMSRedelivered() throws Exception {
      return false;
   }

   public final void setJMSRedelivered(boolean redelivered) throws Exception {
      // no op
   }

   public final String getJMSType() throws Exception {
      return MessageUtil.getJMSType(message);
   }

   public final void setJMSType(String type) throws Exception {
      MessageUtil.setJMSType(message, type);
   }

   public final long getJMSExpiration() throws Exception {
      return message.getExpiration();
   }

   public final void setJMSExpiration(long expiration) throws Exception {
      message.setExpiration(expiration);
   }

   public final long getJMSDeliveryTime() throws Exception {
      // no op
      return 0;
   }

   public final void setJMSDeliveryTime(long deliveryTime) throws Exception {
      // no op
   }

   public final int getJMSPriority() throws Exception {
      return message.getPriority();
   }

   public final void setJMSPriority(int priority) throws Exception {
      message.setPriority((byte) priority);
   }

   public final void clearProperties() throws Exception {
      MessageUtil.clearProperties(message);

   }

   public final boolean propertyExists(String name) throws Exception {
      return MessageUtil.propertyExists(message, name);
   }

   public final boolean getBooleanProperty(String name) throws Exception {
      return message.getBooleanProperty(name);
   }

   public final byte getByteProperty(String name) throws Exception {
      return message.getByteProperty(name);
   }

   public final short getShortProperty(String name) throws Exception {
      return message.getShortProperty(name);
   }

   public final int getIntProperty(String name) throws Exception {
      return MessageUtil.getIntProperty(message, name);
   }

   public final long getLongProperty(String name) throws Exception {
      return MessageUtil.getLongProperty(message, name);
   }

   public final float getFloatProperty(String name) throws Exception {
      return message.getFloatProperty(name);
   }

   public final double getDoubleProperty(String name) throws Exception {
      return message.getDoubleProperty(name);
   }

   public final String getStringProperty(String name) throws Exception {
      return MessageUtil.getStringProperty(message, name);
   }

   public final Object getObjectProperty(String name) throws Exception {
      return MessageUtil.getObjectProperty(message, name);
   }

   public final Enumeration getPropertyNames() throws Exception {
      return Collections.enumeration(MessageUtil.getPropertyNames(message));
   }

   public final void setBooleanProperty(String name, boolean value) throws Exception {
      message.putBooleanProperty(name, value);
   }

   public final void setByteProperty(String name, byte value) throws Exception {
      message.putByteProperty(name, value);
   }

   public final void setShortProperty(String name, short value) throws Exception {
      message.putShortProperty(name, value);
   }

   public final void setIntProperty(String name, int value) throws Exception {
      MessageUtil.setIntProperty(message, name, value);
   }

   public final void setLongProperty(String name, long value) throws Exception {
      MessageUtil.setLongProperty(message, name, value);
   }

   public final void setFloatProperty(String name, float value) throws Exception {
      message.putFloatProperty(name, value);
   }

   public final void setDoubleProperty(String name, double value) throws Exception {
      message.putDoubleProperty(name, value);
   }

   public final void setStringProperty(String name, String value) throws Exception {
      MessageUtil.setStringProperty(message, name, value);
   }

   public final void setObjectProperty(String name, Object value) throws Exception {
      MessageUtil.setObjectProperty(message, name, value);
   }

   public final void acknowledge() throws Exception {
      // no op
   }

   public void clearBody() throws Exception {
      message.getBodyBuffer().clear();
   }

   public final <T> T getBody(Class<T> c) throws Exception {
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

   public final boolean isBodyAssignableTo(Class c) throws Exception {
      // no op.. jms2 not used on the conversion
      return false;
   }
}
