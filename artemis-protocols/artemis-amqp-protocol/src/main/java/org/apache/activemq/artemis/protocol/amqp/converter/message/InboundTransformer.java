/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.converter.message;

import static org.apache.activemq.artemis.api.core.Message.HDR_SCHEDULED_DELIVERY_TIME;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_CONTENT_ENCODING;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_CONTENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_FIRST_ACQUIRER;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_FOOTER_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_HEADER;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_MESSAGE_ANNOTATION_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_REPLYTO_GROUP_ID;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerDestination;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Decimal128;
import org.apache.qpid.proton.amqp.Decimal32;
import org.apache.qpid.proton.amqp.Decimal64;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;

public abstract class InboundTransformer {

   protected IDGenerator idGenerator;

   public static final String TRANSFORMER_NATIVE = "native";
   public static final String TRANSFORMER_RAW = "raw";
   public static final String TRANSFORMER_JMS = "jms";

   public InboundTransformer(IDGenerator idGenerator) {
      this.idGenerator = idGenerator;
   }

   public abstract ServerJMSMessage transform(EncodedMessage amqpMessage) throws Exception;

   public abstract String getTransformerName();

   public abstract InboundTransformer getFallbackTransformer();

   @SuppressWarnings("unchecked")
   protected ServerJMSMessage populateMessage(ServerJMSMessage jms, org.apache.qpid.proton.message.Message amqp) throws Exception {
      Header header = amqp.getHeader();
      if (header != null) {
         jms.setBooleanProperty(JMS_AMQP_HEADER, true);

         if (header.getDurable() != null) {
            jms.setJMSDeliveryMode(header.getDurable().booleanValue() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         } else {
            jms.setJMSDeliveryMode(Message.DEFAULT_DELIVERY_MODE);
         }

         if (header.getPriority() != null) {
            jms.setJMSPriority(header.getPriority().intValue());
         } else {
            jms.setJMSPriority(Message.DEFAULT_PRIORITY);
         }

         if (header.getFirstAcquirer() != null) {
            jms.setBooleanProperty(JMS_AMQP_FIRST_ACQUIRER, header.getFirstAcquirer());
         }

         if (header.getDeliveryCount() != null) {
            // AMQP Delivery Count counts only failed delivers where JMS
            // Delivery Count should include the original delivery in the count.
            jms.setLongProperty("JMSXDeliveryCount", header.getDeliveryCount().longValue() + 1);
         }
      } else {
         jms.setJMSPriority((byte) Message.DEFAULT_PRIORITY);
         jms.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
      }

      final MessageAnnotations ma = amqp.getMessageAnnotations();
      if (ma != null) {
         for (Map.Entry<?, ?> entry : ma.getValue().entrySet()) {
            String key = entry.getKey().toString();
            if ("x-opt-delivery-time".equals(key) && entry.getValue() != null) {
               long deliveryTime = ((Number) entry.getValue()).longValue();
               jms.setLongProperty(HDR_SCHEDULED_DELIVERY_TIME.toString(), deliveryTime);
            } else if ("x-opt-delivery-delay".equals(key) && entry.getValue() != null) {
               long delay = ((Number) entry.getValue()).longValue();
               if (delay > 0) {
                  jms.setLongProperty(HDR_SCHEDULED_DELIVERY_TIME.toString(), System.currentTimeMillis() + delay);
               }
            }

            setProperty(jms, JMS_AMQP_MESSAGE_ANNOTATION_PREFIX + key, entry.getValue());
         }
      }

      final ApplicationProperties ap = amqp.getApplicationProperties();
      if (ap != null) {
         for (Map.Entry<Object, Object> entry : (Set<Map.Entry<Object, Object>>) ap.getValue().entrySet()) {
            setProperty(jms, entry.getKey().toString(), entry.getValue());
         }
      }

      final Properties properties = amqp.getProperties();
      if (properties != null) {
         if (properties.getMessageId() != null) {
            jms.setJMSMessageID(AMQPMessageIdHelper.INSTANCE.toBaseMessageIdString(properties.getMessageId()));
         }
         Binary userId = properties.getUserId();
         if (userId != null) {
            // TODO - Better Way to set this?
            jms.setStringProperty("JMSXUserID", new String(userId.getArray(), userId.getArrayOffset(), userId.getLength(), StandardCharsets.UTF_8));
         }
         if (properties.getTo() != null) {
            jms.setJMSDestination(new ServerDestination(properties.getTo()));
         }
         if (properties.getSubject() != null) {
            jms.setJMSType(properties.getSubject());
         }
         if (properties.getReplyTo() != null) {
            jms.setJMSReplyTo(new ServerDestination(properties.getReplyTo()));
         }
         if (properties.getCorrelationId() != null) {
            jms.setJMSCorrelationID(AMQPMessageIdHelper.INSTANCE.toBaseMessageIdString(properties.getCorrelationId()));
         }
         if (properties.getContentType() != null) {
            jms.setStringProperty(JMS_AMQP_CONTENT_TYPE, properties.getContentType().toString());
         }
         if (properties.getContentEncoding() != null) {
            jms.setStringProperty(JMS_AMQP_CONTENT_ENCODING, properties.getContentEncoding().toString());
         }
         if (properties.getCreationTime() != null) {
            jms.setJMSTimestamp(properties.getCreationTime().getTime());
         }
         if (properties.getGroupId() != null) {
            jms.setStringProperty("_AMQ_GROUP_ID", properties.getGroupId());
         }
         if (properties.getGroupSequence() != null) {
            jms.setIntProperty("JMSXGroupSeq", properties.getGroupSequence().intValue());
         }
         if (properties.getReplyToGroupId() != null) {
            jms.setStringProperty(JMS_AMQP_REPLYTO_GROUP_ID, properties.getReplyToGroupId());
         }
         if (properties.getAbsoluteExpiryTime() != null) {
            jms.setJMSExpiration(properties.getAbsoluteExpiryTime().getTime());
         }
      }

      // If the jms expiration has not yet been set...
      if (header != null && jms.getJMSExpiration() == 0) {
         // Then lets try to set it based on the message ttl.
         long ttl = Message.DEFAULT_TIME_TO_LIVE;
         if (header.getTtl() != null) {
            ttl = header.getTtl().longValue();
         }

         if (ttl == 0) {
            jms.setJMSExpiration(0);
         } else {
            jms.setJMSExpiration(System.currentTimeMillis() + ttl);
         }
      }

      final Footer fp = amqp.getFooter();
      if (fp != null) {
         for (Map.Entry<Object, Object> entry : (Set<Map.Entry<Object, Object>>) fp.getValue().entrySet()) {
            String key = entry.getKey().toString();
            setProperty(jms, JMS_AMQP_FOOTER_PREFIX + key, entry.getValue());
         }
      }

      return jms;
   }

   private void setProperty(Message msg, String key, Object value) throws JMSException {
      if (value instanceof UnsignedLong) {
         long v = ((UnsignedLong) value).longValue();
         msg.setLongProperty(key, v);
      } else if (value instanceof UnsignedInteger) {
         long v = ((UnsignedInteger) value).longValue();
         if (Integer.MIN_VALUE <= v && v <= Integer.MAX_VALUE) {
            msg.setIntProperty(key, (int) v);
         } else {
            msg.setLongProperty(key, v);
         }
      } else if (value instanceof UnsignedShort) {
         int v = ((UnsignedShort) value).intValue();
         if (Short.MIN_VALUE <= v && v <= Short.MAX_VALUE) {
            msg.setShortProperty(key, (short) v);
         } else {
            msg.setIntProperty(key, v);
         }
      } else if (value instanceof UnsignedByte) {
         short v = ((UnsignedByte) value).shortValue();
         if (Byte.MIN_VALUE <= v && v <= Byte.MAX_VALUE) {
            msg.setByteProperty(key, (byte) v);
         } else {
            msg.setShortProperty(key, v);
         }
      } else if (value instanceof Symbol) {
         msg.setStringProperty(key, value.toString());
      } else if (value instanceof Decimal128) {
         msg.setDoubleProperty(key, ((Decimal128) value).doubleValue());
      } else if (value instanceof Decimal64) {
         msg.setDoubleProperty(key, ((Decimal64) value).doubleValue());
      } else if (value instanceof Decimal32) {
         msg.setFloatProperty(key, ((Decimal32) value).floatValue());
      } else if (value instanceof Binary) {
         msg.setStringProperty(key, value.toString());
      } else {
         msg.setObjectProperty(key, value);
      }
   }
}
