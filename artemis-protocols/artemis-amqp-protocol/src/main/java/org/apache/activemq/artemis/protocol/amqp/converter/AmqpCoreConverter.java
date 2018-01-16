/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.protocol.amqp.converter;

import static org.apache.activemq.artemis.api.core.Message.HDR_SCHEDULED_DELIVERY_TIME;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_CONTENT_ENCODING;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_CONTENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_FIRST_ACQUIRER;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_FOOTER_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_HEADER;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_HEADER_DURABLE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_HEADER_PRIORITY;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_MESSAGE_ANNOTATION_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_REPLYTO_GROUP_ID;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.OCTET_STREAM_CONTENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.createBytesMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.createMapMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.createMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.createObjectMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.createStreamMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.createTextMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.getCharsetForTextualContent;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.isContentType;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessageObjectPools;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerDestination;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Decimal128;
import org.apache.qpid.proton.amqp.Decimal32;
import org.apache.qpid.proton.amqp.Decimal64;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.WritableBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 *  This class was created just to separate concerns on AMQPConverter.
 *  For better organization of the code.
 * */
public class AmqpCoreConverter {

   @SuppressWarnings("unchecked")
   public static ICoreMessage toCore(AMQPMessage message, CoreMessageObjectPools coreMessageObjectPools) throws Exception {

      Section body = message.getProtonMessage().getBody();
      ServerJMSMessage result;

      if (body == null) {
         if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString(), message.getProtonMessage())) {
            result = createObjectMessage(message.getMessageID(), coreMessageObjectPools);
         } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, message.getProtonMessage()) || isContentType(null, message.getProtonMessage())) {
            result = createBytesMessage(message.getMessageID(), coreMessageObjectPools);
         } else {
            Charset charset = getCharsetForTextualContent(message.getProtonMessage().getContentType());
            if (charset != null) {
               result = createTextMessage(message.getMessageID(), coreMessageObjectPools);
            } else {
               result = createMessage(message.getMessageID(), coreMessageObjectPools);
            }
         }
      } else if (body instanceof Data) {
         Binary payload = ((Data) body).getValue();

         if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString(), message.getProtonMessage())) {
            result = createObjectMessage(message.getMessageID(), payload.getArray(), payload.getArrayOffset(), payload.getLength(), coreMessageObjectPools);
         } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, message.getProtonMessage())) {
            result = createBytesMessage(message.getMessageID(), payload.getArray(), payload.getArrayOffset(), payload.getLength(), coreMessageObjectPools);
         } else {
            Charset charset = getCharsetForTextualContent(message.getProtonMessage().getContentType());
            if (StandardCharsets.UTF_8.equals(charset)) {
               ByteBuffer buf = ByteBuffer.wrap(payload.getArray(), payload.getArrayOffset(), payload.getLength());

               try {
                  CharBuffer chars = charset.newDecoder().decode(buf);
                  result = createTextMessage(message.getMessageID(), String.valueOf(chars), coreMessageObjectPools);
               } catch (CharacterCodingException e) {
                  result = createBytesMessage(message.getMessageID(), payload.getArray(), payload.getArrayOffset(), payload.getLength(), coreMessageObjectPools);
               }
            } else {
               result = createBytesMessage(message.getMessageID(), payload.getArray(), payload.getArrayOffset(), payload.getLength(), coreMessageObjectPools);
            }
         }

      } else if (body instanceof AmqpSequence) {
         AmqpSequence sequence = (AmqpSequence) body;
         ServerJMSStreamMessage m = createStreamMessage(message.getMessageID(), coreMessageObjectPools);
         for (Object item : sequence.getValue()) {
            m.writeObject(item);
         }

         result = m;
      } else if (body instanceof AmqpValue) {
         Object value = ((AmqpValue) body).getValue();
         if (value == null || value instanceof String) {
            result = createTextMessage(message.getMessageID(), (String) value, coreMessageObjectPools);

         } else if (value instanceof Binary) {
            Binary payload = (Binary) value;

            if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString(), message.getProtonMessage())) {
               result = createObjectMessage(message.getMessageID(), payload, coreMessageObjectPools);
            } else {
               result = createBytesMessage(message.getMessageID(), payload.getArray(), payload.getArrayOffset(), payload.getLength(), coreMessageObjectPools);
            }

         } else if (value instanceof List) {
            ServerJMSStreamMessage m = createStreamMessage(message.getMessageID(), coreMessageObjectPools);
            for (Object item : (List<Object>) value) {
               m.writeObject(item);
            }
            result = m;
         } else if (value instanceof Map) {
            result = createMapMessage(message.getMessageID(), (Map<String, Object>) value, coreMessageObjectPools);
         } else {
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(1024);
            try {
               TLSEncode.getEncoder().setByteBuffer(new NettyWritable(buf));
               TLSEncode.getEncoder().writeObject(body);
               result = createBytesMessage(message.getMessageID(), buf.array(), 0, buf.writerIndex(), coreMessageObjectPools);
            } finally {
               buf.release();
               TLSEncode.getEncoder().setByteBuffer((WritableBuffer)null);
            }
         }
      } else {
         throw new RuntimeException("Unexpected body type: " + body.getClass());
      }

      TypedProperties properties = message.getExtraProperties();
      if (properties != null) {
         for (SimpleString str : properties.getPropertyNames()) {
            result.getInnerMessage().putBytesProperty(str, properties.getBytesProperty(str));
         }
      }

      populateMessage(result, message.getProtonMessage());
      result.getInnerMessage().setReplyTo(message.getReplyTo());
      result.getInnerMessage().setDurable(message.isDurable());
      result.getInnerMessage().setPriority(message.getPriority());
      result.getInnerMessage().setAddress(message.getAddressSimpleString());

      result.encode();

      return result != null ? result.getInnerMessage() : null;
   }

   @SuppressWarnings("unchecked")
   protected static ServerJMSMessage populateMessage(ServerJMSMessage jms, org.apache.qpid.proton.message.Message amqp) throws Exception {
      Header header = amqp.getHeader();
      if (header != null) {
         jms.setBooleanProperty(JMS_AMQP_HEADER, true);

         if (header.getDurable() != null) {
            jms.setBooleanProperty(JMS_AMQP_HEADER_DURABLE, true);
            jms.setJMSDeliveryMode(header.getDurable().booleanValue() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         } else {
            jms.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
         }

         if (header.getPriority() != null) {
            jms.setBooleanProperty(JMS_AMQP_HEADER_PRIORITY, true);
            jms.setJMSPriority(header.getPriority().intValue());
         } else {
            jms.setJMSPriority(javax.jms.Message.DEFAULT_PRIORITY);
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
         jms.setJMSPriority((byte) javax.jms.Message.DEFAULT_PRIORITY);
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
         for (Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) ap.getValue().entrySet()) {
            setProperty(jms, entry.getKey(), entry.getValue());
         }
      }

      final Properties properties = amqp.getProperties();
      if (properties != null) {
         if (properties.getMessageId() != null) {
            jms.setJMSMessageID(AMQPMessageIdHelper.INSTANCE.toMessageIdString(properties.getMessageId()));
         }
         Binary userId = properties.getUserId();
         if (userId != null) {
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
            jms.setJMSCorrelationID(AMQPMessageIdHelper.INSTANCE.toCorrelationIdString(properties.getCorrelationId()));
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
         long ttl = javax.jms.Message.DEFAULT_TIME_TO_LIVE;
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

   private static void setProperty(javax.jms.Message msg, String key, Object value) throws JMSException {
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
