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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMapMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSObjectMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSTextMessage;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPIllegalStateException;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.activemq.artemis.reader.MessageUtil;
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
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.api.core.FilterConstants.NATIVE_MESSAGE_ID;
import static org.apache.activemq.artemis.api.core.Message.HDR_SCHEDULED_DELIVERY_TIME;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.EMPTY_BINARY;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_CONTENT_ENCODING;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_CONTENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_DELIVERY_ANNOTATION_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_FIRST_ACQUIRER;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_FOOTER_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_HEADER;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_HEADER_DURABLE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_HEADER_PRIORITY;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_MESSAGE_ANNOTATION_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_NATIVE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_PROPERTIES;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_REPLYTO_GROUP_ID;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_DEST_TYPE_MSG_ANNOTATION;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_REPLY_TO_TYPE_MSG_ANNOTATION;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.QUEUE_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.TEMP_QUEUE_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.TEMP_TOPIC_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.TOPIC_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.toAddress;

public class CoreAmqpConverter {

   private static Logger logger = Logger.getLogger(CoreAmqpConverter.class);

   public static AMQPMessage checkAMQP(Message message) throws Exception {
      if (message instanceof AMQPMessage) {
         return (AMQPMessage)message;
      } else {
         // It will first convert to Core, then to AMQP
         return fromCore(message.toCore());
      }
   }

   public static AMQPMessage fromCore(ICoreMessage coreMessage) throws Exception {
      if (coreMessage == null) {
         return null;
      }

      ServerJMSMessage message = ServerJMSMessage.wrapCoreMessage(coreMessage);
      message.decode();

      long messageFormat = 0;
      Header header = null;
      final Properties properties = new Properties();
      Map<Symbol, Object> daMap = null;
      final Map<Symbol, Object> maMap = new HashMap<>();
      Map<String, Object> apMap = null;
      Map<Object, Object> footerMap = null;

      Section body = convertBody(message, maMap, properties);

      if (message.getInnerMessage().isDurable()) {
         if (header == null) {
            header = new Header();
         }
         header.setDurable(true);
      }
      byte priority = (byte) message.getJMSPriority();
      if (priority != javax.jms.Message.DEFAULT_PRIORITY) {
         if (header == null) {
            header = new Header();
         }
         header.setPriority(UnsignedByte.valueOf(priority));
      }
      String type = message.getJMSType();
      if (type != null) {
         properties.setSubject(type);
      }
      String messageId = message.getJMSMessageID();
      if (messageId != null) {
         try {
            properties.setMessageId(AMQPMessageIdHelper.INSTANCE.toIdObject(messageId));
         } catch (ActiveMQAMQPIllegalStateException e) {
            properties.setMessageId(messageId);
         }
      }
      Destination destination = message.getJMSDestination();
      if (destination != null) {
         properties.setTo(toAddress(destination));
         maMap.put(JMS_DEST_TYPE_MSG_ANNOTATION, destinationType(destination));
      }
      Destination replyTo = message.getJMSReplyTo();
      if (replyTo != null) {
         properties.setReplyTo(toAddress(replyTo));
         maMap.put(JMS_REPLY_TO_TYPE_MSG_ANNOTATION, destinationType(replyTo));
      }
      String correlationId = message.getJMSCorrelationID();
      if (correlationId != null) {
         try {
            properties.setCorrelationId(AMQPMessageIdHelper.INSTANCE.toIdObject(correlationId));
         } catch (ActiveMQAMQPIllegalStateException e) {
            properties.setCorrelationId(correlationId);
         }
      }
      long expiration = message.getJMSExpiration();
      if (expiration != 0) {
         long ttl = expiration - System.currentTimeMillis();
         if (ttl < 0) {
            ttl = 1;
         }

         if (header == null) {
            header = new Header();
         }
         header.setTtl(new UnsignedInteger((int) ttl));

         properties.setAbsoluteExpiryTime(new Date(expiration));
      }
      long timeStamp = message.getJMSTimestamp();
      if (timeStamp != 0) {
         properties.setCreationTime(new Date(timeStamp));
      }

      final Set<String> keySet = MessageUtil.getPropertyNames(message.getInnerMessage());
      for (String key : keySet) {
         if (key.startsWith("JMSX")) {
            if (key.equals("JMSXUserID")) {
               String value = message.getStringProperty(key);
               properties.setUserId(new Binary(value.getBytes(StandardCharsets.UTF_8)));
               continue;
            } else if (key.equals("JMSXGroupID")) {
               String value = message.getStringProperty(key);
               properties.setGroupId(value);
               continue;
            } else if (key.equals("JMSXGroupSeq")) {
               UnsignedInteger value = new UnsignedInteger(message.getIntProperty(key));
               properties.setGroupSequence(value);
               continue;
            }
         } else if (key.startsWith(JMS_AMQP_PREFIX)) {
            // AMQP Message Information stored from a conversion to the Core Message
            if (key.equals(JMS_AMQP_NATIVE)) {
               // skip..internal use only
               continue;
            } else if (key.equals(JMS_AMQP_FIRST_ACQUIRER)) {
               if (header == null) {
                  header = new Header();
               }
               header.setFirstAcquirer(message.getBooleanProperty(key));
               continue;
            } else if (key.equals(JMS_AMQP_HEADER)) {
               if (header == null) {
                  header = new Header();
               }
               continue;
            } else if (key.equals(JMS_AMQP_HEADER_DURABLE)) {
               if (header == null) {
                  header = new Header();
               }
               header.setDurable(message.getInnerMessage().isDurable());
               continue;
            } else if (key.equals(JMS_AMQP_HEADER_PRIORITY)) {
               if (header == null) {
                  header = new Header();
               }
               header.setPriority(UnsignedByte.valueOf(priority));
               continue;
            } else if (key.startsWith(JMS_AMQP_PROPERTIES)) {
               continue;
            } else if (key.startsWith(JMS_AMQP_DELIVERY_ANNOTATION_PREFIX)) {
               if (daMap == null) {
                  daMap = new HashMap<>();
               }
               String name = key.substring(JMS_AMQP_DELIVERY_ANNOTATION_PREFIX.length());
               daMap.put(Symbol.valueOf(name), message.getObjectProperty(key));
               continue;
            } else if (key.startsWith(JMS_AMQP_MESSAGE_ANNOTATION_PREFIX)) {
               String name = key.substring(JMS_AMQP_MESSAGE_ANNOTATION_PREFIX.length());
               maMap.put(Symbol.valueOf(name), message.getObjectProperty(key));
               continue;
            } else if (key.equals(JMS_AMQP_CONTENT_TYPE)) {
               properties.setContentType(Symbol.getSymbol(message.getStringProperty(key)));
               continue;
            } else if (key.equals(JMS_AMQP_CONTENT_ENCODING)) {
               properties.setContentEncoding(Symbol.getSymbol(message.getStringProperty(key)));
               continue;
            } else if (key.equals(JMS_AMQP_REPLYTO_GROUP_ID)) {
               properties.setReplyToGroupId(message.getStringProperty(key));
               continue;
            } else if (key.startsWith(JMS_AMQP_FOOTER_PREFIX)) {
               if (footerMap == null) {
                  footerMap = new HashMap<>();
               }
               String name = key.substring(JMS_AMQP_FOOTER_PREFIX.length());
               footerMap.put(name, message.getObjectProperty(key));
               continue;
            }
         } else if (key.equals("_AMQ_GROUP_ID")) {
            String value = message.getStringProperty(key);
            properties.setGroupId(value);
            continue;
         } else if (key.equals(NATIVE_MESSAGE_ID)) {
            // skip..internal use only
            continue;
         } else if (key.endsWith(HDR_SCHEDULED_DELIVERY_TIME.toString())) {
            // skip..remove annotation from previous inbound transformation
            continue;
         }

         if (apMap == null) {
            apMap = new HashMap<>();
         }

         Object objectProperty = message.getObjectProperty(key);
         if (objectProperty instanceof byte[]) {
            objectProperty = new Binary((byte[]) objectProperty);
         }

         apMap.put(key, objectProperty);
      }

      ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1024);

      try {
         EncoderImpl encoder = TLSEncode.getEncoder();
         encoder.setByteBuffer(new NettyWritable(buffer));

         if (header != null) {
            encoder.writeObject(header);
         }
         if (daMap != null) {
            encoder.writeObject(new DeliveryAnnotations(daMap));
         }
         if (maMap != null) {
            encoder.writeObject(new MessageAnnotations(maMap));
         }
         if (properties != null) {
            encoder.writeObject(properties);
         }
         if (apMap != null) {
            encoder.writeObject(new ApplicationProperties(apMap));
         }
         if (body != null) {
            encoder.writeObject(body);
         }
         if (footerMap != null) {
            encoder.writeObject(new Footer(footerMap));
         }

         byte[] data = new byte[buffer.writerIndex()];
         buffer.readBytes(data);

         AMQPMessage amqpMessage = new AMQPMessage(messageFormat, data);
         amqpMessage.setMessageID(message.getInnerMessage().getMessageID());
         amqpMessage.setReplyTo(coreMessage.getReplyTo());
         return amqpMessage;

      } finally {
         TLSEncode.getEncoder().setByteBuffer((WritableBuffer) null);
         buffer.release();
      }
   }

   private static Section convertBody(ServerJMSMessage message, Map<Symbol, Object> maMap, Properties properties) throws JMSException {

      Section body = null;

      if (message instanceof ServerJMSBytesMessage) {
         Binary payload = getBinaryFromMessageBody((ServerJMSBytesMessage) message);

         maMap.put(AMQPMessageSupport.JMS_MSG_TYPE, AMQPMessageSupport.JMS_BYTES_MESSAGE);
         if (payload == null) {
            payload = EMPTY_BINARY;
         } else {
            body = new AmqpValue(payload);
         }
      } else if (message instanceof ServerJMSTextMessage) {
         body = new AmqpValue(((TextMessage) message).getText());
         maMap.put(AMQPMessageSupport.JMS_MSG_TYPE, AMQPMessageSupport.JMS_TEXT_MESSAGE);
      } else if (message instanceof ServerJMSMapMessage) {
         body = new AmqpValue(getMapFromMessageBody((ServerJMSMapMessage) message));
         maMap.put(AMQPMessageSupport.JMS_MSG_TYPE, AMQPMessageSupport.JMS_MAP_MESSAGE);
      } else if (message instanceof ServerJMSStreamMessage) {
         maMap.put(AMQPMessageSupport.JMS_MSG_TYPE, AMQPMessageSupport.JMS_STREAM_MESSAGE);
         ArrayList<Object> list = new ArrayList<>();
         final ServerJMSStreamMessage m = (ServerJMSStreamMessage) message;
         try {
            while (true) {
               list.add(m.readObject());
            }
         } catch (MessageEOFException e) {
         }

         body = new AmqpSequence(list);
      } else if (message instanceof ServerJMSObjectMessage) {
         properties.setContentType(AMQPMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);
         maMap.put(AMQPMessageSupport.JMS_MSG_TYPE, AMQPMessageSupport.JMS_OBJECT_MESSAGE);
         Binary payload = getBinaryFromMessageBody((ServerJMSObjectMessage) message);

         if (payload == null) {
            payload = EMPTY_BINARY;
         }

         body = new Data(payload);

         // For a non-AMQP message we tag the outbound content type as containing
         // a serialized Java object so that an AMQP client has a hint as to what
         // we are sending it.
         if (!message.propertyExists(JMS_AMQP_CONTENT_TYPE)) {
            message.setStringProperty(JMS_AMQP_CONTENT_TYPE, SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString());
         }
      } else if (message instanceof ServerJMSMessage) {
         maMap.put(AMQPMessageSupport.JMS_MSG_TYPE, AMQPMessageSupport.JMS_MESSAGE);
         // If this is not an AMQP message that was converted then the original encoding
         // will be unknown so we check for special cases of messages with special data
         // encoded into the server message body.
         ICoreMessage internalMessage = message.getInnerMessage();
         int readerIndex = internalMessage.getBodyBuffer().readerIndex();
         try {
            Object s = internalMessage.getBodyBuffer().readNullableSimpleString();
            if (s != null) {
               body = new AmqpValue(s.toString());
            }
         } catch (Throwable ignored) {
            logger.debug("Exception ignored during conversion", ignored.getMessage(), ignored);
            body = new AmqpValue("Conversion to AMQP error!");
         } finally {
            internalMessage.getBodyBuffer().readerIndex(readerIndex);
         }
      }

      return body;
   }

   private static Binary getBinaryFromMessageBody(ServerJMSBytesMessage message) throws JMSException {
      byte[] data = new byte[(int) message.getBodyLength()];
      message.readBytes(data);
      message.reset(); // Need to reset after readBytes or future readBytes

      return new Binary(data);
   }

   private static Binary getBinaryFromMessageBody(ServerJMSObjectMessage message) throws JMSException {
      message.getInnerMessage().getBodyBuffer().resetReaderIndex();
      int size = message.getInnerMessage().getBodyBuffer().readInt();
      byte[] bytes = new byte[size];
      message.getInnerMessage().getBodyBuffer().readBytes(bytes);

      return new Binary(bytes);
   }

   private static Map<String, Object> getMapFromMessageBody(ServerJMSMapMessage message) throws JMSException {
      final HashMap<String, Object> map = new LinkedHashMap<>();

      @SuppressWarnings("unchecked")
      final Enumeration<String> names = message.getMapNames();
      while (names.hasMoreElements()) {
         String key = names.nextElement();
         Object value = message.getObject(key);
         if (value instanceof byte[]) {
            value = new Binary((byte[]) value);
         }
         map.put(key, value);
      }

      return map;
   }

   private static byte destinationType(Destination destination) {
      if (destination instanceof Queue) {
         if (destination instanceof TemporaryQueue) {
            return TEMP_QUEUE_TYPE;
         } else {
            return QUEUE_TYPE;
         }
      } else if (destination instanceof Topic) {
         if (destination instanceof TemporaryTopic) {
            return TEMP_TOPIC_TYPE;
         } else {
            return TOPIC_TYPE;
         }
      }

      throw new IllegalArgumentException("Unknown Destination Type passed to JMS Transformer.");
   }

}
