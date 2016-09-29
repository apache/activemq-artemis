/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.converter.message;

import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPIllegalStateException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.jboss.logging.Logger;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;

public class JMSMappingOutboundTransformer extends OutboundTransformer {
   private static final Logger logger = Logger.getLogger(JMSMappingOutboundTransformer.class);
   public static final Symbol JMS_DEST_TYPE_MSG_ANNOTATION = Symbol.valueOf("x-opt-jms-dest");
   public static final Symbol JMS_REPLY_TO_TYPE_MSG_ANNOTATION = Symbol.valueOf("x-opt-jms-reply-to");

   public static final byte QUEUE_TYPE = 0x00;
   public static final byte TOPIC_TYPE = 0x01;
   public static final byte TEMP_QUEUE_TYPE = 0x02;
   public static final byte TEMP_TOPIC_TYPE = 0x03;

   public JMSMappingOutboundTransformer(JMSVendor vendor) {
      super(vendor);
   }

   /**
    * Perform the conversion between JMS Message and Proton Message without
    * re-encoding it to array. This is needed because some frameworks may elect
    * to do this on their own way (Netty for instance using Nettybuffers)
    *
    * @param msg
    * @return
    * @throws Exception
    */
   public ProtonJMessage convert(Message msg) throws JMSException, UnsupportedEncodingException {
      Header header = new Header();
      Properties props = new Properties();
      HashMap<Symbol, Object> daMap = null;
      HashMap<Symbol, Object> maMap = null;
      HashMap apMap = null;
      Section body = null;
      HashMap footerMap = null;
      if (msg instanceof BytesMessage) {
         BytesMessage m = (BytesMessage) msg;
         byte[] data = new byte[(int) m.getBodyLength()];
         m.readBytes(data);
         m.reset(); // Need to reset after readBytes or future readBytes
         // calls (ex: redeliveries) will fail and return -1
         body = new Data(new Binary(data));
      }
      if (msg instanceof TextMessage) {
         body = new AmqpValue(((TextMessage) msg).getText());
      }
      if (msg instanceof MapMessage) {
         final HashMap<String, Object> map = new HashMap<>();
         final MapMessage m = (MapMessage) msg;
         final Enumeration<String> names = m.getMapNames();
         while (names.hasMoreElements()) {
            String key = names.nextElement();
            map.put(key, m.getObject(key));
         }
         body = new AmqpValue(map);
      }
      if (msg instanceof StreamMessage) {
         ArrayList<Object> list = new ArrayList<>();
         final StreamMessage m = (StreamMessage) msg;
         try {
            while (true) {
               list.add(m.readObject());
            }
         }
         catch (MessageEOFException e) {
         }

         String amqpType = msg.getStringProperty(AMQPMessageTypes.AMQP_TYPE_KEY);
         if (amqpType.equals(AMQPMessageTypes.AMQP_LIST)) {
            body = new AmqpValue(list);
         }
         else {
            body = new AmqpSequence(list);
         }
      }
      if (msg instanceof ObjectMessage) {
         body = new AmqpValue(((ObjectMessage) msg).getObject());
      }

      if (body == null && msg instanceof ServerJMSMessage) {

         MessageInternal internalMessage = ((ServerJMSMessage) msg).getInnerMessage();
         if (!internalMessage.containsProperty("AMQP_MESSAGE_FORMAT")) {
            int readerIndex = internalMessage.getBodyBuffer().readerIndex();
            try {
               Object s = internalMessage.getBodyBuffer().readNullableSimpleString();
               if (s != null) {
                  body = new AmqpValue(s.toString());
               }
            }
            catch (Throwable ignored) {
               logger.debug("Exception ignored during conversion, should be ok!", ignored.getMessage(), ignored);
            }
            finally {
               internalMessage.getBodyBuffer().readerIndex(readerIndex);
            }
         }
      }

      header.setDurable(msg.getJMSDeliveryMode() == DeliveryMode.PERSISTENT ? true : false);
      header.setPriority(new UnsignedByte((byte) msg.getJMSPriority()));
      if (msg.getJMSType() != null) {
         props.setSubject(msg.getJMSType());
      }
      if (msg.getJMSMessageID() != null) {

         String msgId = msg.getJMSMessageID();

         try {
            props.setMessageId(AMQPMessageIdHelper.INSTANCE.toIdObject(msgId));
         }
         catch (ActiveMQAMQPIllegalStateException e) {
            props.setMessageId(msgId);
         }
      }
      if (msg.getJMSDestination() != null) {
         props.setTo(vendor.toAddress(msg.getJMSDestination()));
         if (maMap == null) {
            maMap = new HashMap<>();
         }
         maMap.put(JMS_DEST_TYPE_MSG_ANNOTATION, destinationType(msg.getJMSDestination()));
      }
      if (msg.getJMSReplyTo() != null) {
         props.setReplyTo(vendor.toAddress(msg.getJMSReplyTo()));
         if (maMap == null) {
            maMap = new HashMap<>();
         }
         maMap.put(JMS_REPLY_TO_TYPE_MSG_ANNOTATION, destinationType(msg.getJMSReplyTo()));
      }
      if (msg.getJMSCorrelationID() != null) {
         String correlationId = msg.getJMSCorrelationID();

         try {
            props.setCorrelationId(AMQPMessageIdHelper.INSTANCE.toIdObject(correlationId));
         }
         catch (ActiveMQAMQPIllegalStateException e) {
            props.setCorrelationId(correlationId);
         }
      }
      if (msg.getJMSExpiration() != 0) {
         long ttl = msg.getJMSExpiration() - System.currentTimeMillis();
         if (ttl < 0) {
            ttl = 1;
         }
         header.setTtl(new UnsignedInteger((int) ttl));

         props.setAbsoluteExpiryTime(new Date(msg.getJMSExpiration()));
      }
      if (msg.getJMSTimestamp() != 0) {
         props.setCreationTime(new Date(msg.getJMSTimestamp()));
      }

      final Enumeration<String> keys = msg.getPropertyNames();
      while (keys.hasMoreElements()) {
         String key = keys.nextElement();
         if (key.equals(messageFormatKey) || key.equals(nativeKey) || key.equals(ServerJMSMessage.NATIVE_MESSAGE_ID)) {
            // skip..
         }
         else if (key.equals(firstAcquirerKey)) {
            header.setFirstAcquirer(msg.getBooleanProperty(key));
         }
         else if (key.startsWith("JMSXDeliveryCount")) {
            // The AMQP delivery-count field only includes prior failed delivery attempts,
            // whereas JMSXDeliveryCount includes the first/current delivery attempt.
            int amqpDeliveryCount = msg.getIntProperty(key) - 1;
            if (amqpDeliveryCount > 0) {
               header.setDeliveryCount(new UnsignedInteger(amqpDeliveryCount));
            }
         }
         else if (key.startsWith("JMSXUserID")) {
            String value = msg.getStringProperty(key);
            props.setUserId(new Binary(value.getBytes(StandardCharsets.UTF_8)));
         }
         else if (key.startsWith("JMSXGroupID") || key.startsWith("_AMQ_GROUP_ID")) {
            String value = msg.getStringProperty(key);
            props.setGroupId(value);
            if (apMap == null) {
               apMap = new HashMap();
            }
            apMap.put(key, value);
         }
         else if (key.startsWith("JMSXGroupSeq")) {
            UnsignedInteger value = new UnsignedInteger(msg.getIntProperty(key));
            props.setGroupSequence(value);
            if (apMap == null) {
               apMap = new HashMap();
            }
            apMap.put(key, value);
         }
         else if (key.startsWith(prefixDeliveryAnnotationsKey)) {
            if (daMap == null) {
               daMap = new HashMap<>();
            }
            String name = key.substring(prefixDeliveryAnnotationsKey.length());
            daMap.put(Symbol.valueOf(name), msg.getObjectProperty(key));
         }
         else if (key.startsWith(prefixMessageAnnotationsKey)) {
            if (maMap == null) {
               maMap = new HashMap<>();
            }
            String name = key.substring(prefixMessageAnnotationsKey.length());
            maMap.put(Symbol.valueOf(name), msg.getObjectProperty(key));
         }
         else if (key.equals(contentTypeKey)) {
            props.setContentType(Symbol.getSymbol(msg.getStringProperty(key)));
         }
         else if (key.equals(contentEncodingKey)) {
            props.setContentEncoding(Symbol.getSymbol(msg.getStringProperty(key)));
         }
         else if (key.equals(replyToGroupIDKey)) {
            props.setReplyToGroupId(msg.getStringProperty(key));
         }
         else if (key.startsWith(prefixFooterKey)) {
            if (footerMap == null) {
               footerMap = new HashMap();
            }
            String name = key.substring(prefixFooterKey.length());
            footerMap.put(name, msg.getObjectProperty(key));
         }
         else if (key.equals(AMQPMessageTypes.AMQP_TYPE_KEY)) {
            // skip
         }
         else {
            if (apMap == null) {
               apMap = new HashMap();
            }
            apMap.put(key, msg.getObjectProperty(key));
         }
      }

      MessageAnnotations ma = null;
      if (maMap != null) {
         ma = new MessageAnnotations(maMap);
      }
      DeliveryAnnotations da = null;
      if (daMap != null) {
         da = new DeliveryAnnotations(daMap);
      }
      ApplicationProperties ap = null;
      if (apMap != null) {
         ap = new ApplicationProperties(apMap);
      }
      Footer footer = null;
      if (footerMap != null) {
         footer = new Footer(footerMap);
      }

      return (ProtonJMessage) org.apache.qpid.proton.message.Message.Factory.create(header, da, ma, props, ap, body, footer);
   }

   private static byte destinationType(Destination destination) {
      if (destination instanceof Queue) {
         if (destination instanceof TemporaryQueue) {
            return TEMP_QUEUE_TYPE;
         }
         else {
            return QUEUE_TYPE;
         }
      }
      else if (destination instanceof Topic) {
         if (destination instanceof TemporaryTopic) {
            return TEMP_TOPIC_TYPE;
         }
         else {
            return TOPIC_TYPE;
         }
      }

      throw new IllegalArgumentException("Unknown Destination Type passed to JMS Transformer.");
   }
}
