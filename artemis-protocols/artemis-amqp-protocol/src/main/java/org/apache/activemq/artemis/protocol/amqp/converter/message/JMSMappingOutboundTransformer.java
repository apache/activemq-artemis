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
package org.apache.activemq.artemis.protocol.amqp.converter.message;

import static org.apache.activemq.artemis.api.core.Message.HDR_SCHEDULED_DELIVERY_TIME;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_DATA;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_NULL;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_SEQUENCE;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_UNKNOWN;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_VALUE_BINARY;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_VALUE_LIST;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_VALUE_STRING;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.EMPTY_BINARY;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_CONTENT_ENCODING;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_CONTENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_DELIVERY_ANNOTATION_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_FIRST_ACQUIRER;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_FOOTER_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_HEADER;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_MESSAGE_ANNOTATION_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_MESSAGE_FORMAT;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_NATIVE;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_ORIGINAL_ENCODING;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_PROPERTIES;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_REPLYTO_GROUP_ID;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.toAddress;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMapMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSObjectMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSTextMessage;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPIllegalStateException;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.utils.IDGenerator;
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
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.jboss.logging.Logger;

public class JMSMappingOutboundTransformer extends OutboundTransformer {

   private static final Logger logger = Logger.getLogger(JMSMappingOutboundTransformer.class);

   public static final Symbol JMS_DEST_TYPE_MSG_ANNOTATION = Symbol.valueOf("x-opt-jms-dest");
   public static final Symbol JMS_REPLY_TO_TYPE_MSG_ANNOTATION = Symbol.valueOf("x-opt-jms-reply-to");

   public static final byte QUEUE_TYPE = 0x00;
   public static final byte TOPIC_TYPE = 0x01;
   public static final byte TEMP_QUEUE_TYPE = 0x02;
   public static final byte TEMP_TOPIC_TYPE = 0x03;

   // For now Proton requires that we create a decoder to create an encoder
   private static class EncoderDecoderPair {
      DecoderImpl decoder = new DecoderImpl();
      EncoderImpl encoder = new EncoderImpl(decoder);
      {
         AMQPDefinedTypes.registerAllTypes(decoder, encoder);
      }
   }

   private static final ThreadLocal<EncoderDecoderPair> tlsCodec = new ThreadLocal<EncoderDecoderPair>() {
      @Override
      protected EncoderDecoderPair initialValue() {
         return new EncoderDecoderPair();
      }
   };

   public JMSMappingOutboundTransformer(IDGenerator idGenerator) {
      super(idGenerator);
   }

   @Override
   public long transform(ServerJMSMessage message, WritableBuffer buffer) throws JMSException, UnsupportedEncodingException {
      if (message == null) {
         return 0;
      }

      long messageFormat = 0;
      Header header = null;
      Properties properties = null;
      Map<Symbol, Object> daMap = null;
      Map<Symbol, Object> maMap = null;
      Map<String, Object> apMap = null;
      Map<Object, Object> footerMap = null;

      Section body = convertBody(message);

      if (message.getInnerMessage().isDurable()) {
         if (header == null) {
            header = new Header();
         }
         header.setDurable(true);
      }
      byte priority = (byte) message.getJMSPriority();
      if (priority != Message.DEFAULT_PRIORITY) {
         if (header == null) {
            header = new Header();
         }
         header.setPriority(UnsignedByte.valueOf(priority));
      }
      String type = message.getJMSType();
      if (type != null) {
         if (properties == null) {
            properties = new Properties();
         }
         properties.setSubject(type);
      }
      String messageId = message.getJMSMessageID();
      if (messageId != null) {
         if (properties == null) {
            properties = new Properties();
         }
         try {
            properties.setMessageId(AMQPMessageIdHelper.INSTANCE.toIdObject(messageId));
         } catch (ActiveMQAMQPIllegalStateException e) {
            properties.setMessageId(messageId);
         }
      }
      Destination destination = message.getJMSDestination();
      if (destination != null) {
         if (properties == null) {
            properties = new Properties();
         }
         properties.setTo(toAddress(destination));
         if (maMap == null) {
            maMap = new HashMap<>();
         }
         maMap.put(JMS_DEST_TYPE_MSG_ANNOTATION, destinationType(destination));
      }
      Destination replyTo = message.getJMSReplyTo();
      if (replyTo != null) {
         if (properties == null) {
            properties = new Properties();
         }
         properties.setReplyTo(toAddress(replyTo));
         if (maMap == null) {
            maMap = new HashMap<>();
         }
         maMap.put(JMS_REPLY_TO_TYPE_MSG_ANNOTATION, destinationType(replyTo));
      }
      String correlationId = message.getJMSCorrelationID();
      if (correlationId != null) {
         if (properties == null) {
            properties = new Properties();
         }
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

         if (properties == null) {
            properties = new Properties();
         }
         properties.setAbsoluteExpiryTime(new Date(expiration));
      }
      long timeStamp = message.getJMSTimestamp();
      if (timeStamp != 0) {
         if (properties == null) {
            properties = new Properties();
         }
         properties.setCreationTime(new Date(timeStamp));
      }

      final Set<String> keySet = MessageUtil.getPropertyNames(message.getInnerMessage());
      for (String key : keySet) {
         if (key.startsWith("JMSX")) {
            if (key.equals("JMSXDeliveryCount")) {
               // The AMQP delivery-count field only includes prior failed delivery attempts,
               // whereas JMSXDeliveryCount includes the first/current delivery attempt.
               int amqpDeliveryCount = message.getDeliveryCount() - 1;
               if (amqpDeliveryCount > 0) {
                  if (header == null) {
                     header = new Header();
                  }
                  header.setDeliveryCount(new UnsignedInteger(amqpDeliveryCount));
               }
               continue;
            } else if (key.equals("JMSXUserID")) {
               String value = message.getStringProperty(key);
               if (properties == null) {
                  properties = new Properties();
               }
               properties.setUserId(new Binary(value.getBytes(StandardCharsets.UTF_8)));
               continue;
            } else if (key.equals("JMSXGroupID")) {
               String value = message.getStringProperty(key);
               if (properties == null) {
                  properties = new Properties();
               }
               properties.setGroupId(value);
               continue;
            } else if (key.equals("JMSXGroupSeq")) {
               UnsignedInteger value = new UnsignedInteger(message.getIntProperty(key));
               if (properties == null) {
                  properties = new Properties();
               }
               properties.setGroupSequence(value);
               continue;
            }
         } else if (key.startsWith(JMS_AMQP_PREFIX)) {
            // AMQP Message Information stored from a conversion to the Core Message
            if (key.equals(JMS_AMQP_MESSAGE_FORMAT)) {
               messageFormat = message.getLongProperty(JMS_AMQP_MESSAGE_FORMAT);
               continue;
            } else if (key.equals(JMS_AMQP_NATIVE)) {
               // skip..internal use only
               continue;
            } else if (key.equals(JMS_AMQP_ORIGINAL_ENCODING)) {
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
            } else if (key.startsWith(JMS_AMQP_PROPERTIES)) {
               if (properties == null) {
                  properties = new Properties();
               }
               continue;
            } else if (key.startsWith(JMS_AMQP_DELIVERY_ANNOTATION_PREFIX)) {
               if (daMap == null) {
                  daMap = new HashMap<>();
               }
               String name = key.substring(JMS_AMQP_DELIVERY_ANNOTATION_PREFIX.length());
               daMap.put(Symbol.valueOf(name), message.getObjectProperty(key));
               continue;
            } else if (key.startsWith(JMS_AMQP_MESSAGE_ANNOTATION_PREFIX)) {
               if (maMap == null) {
                  maMap = new HashMap<>();
               }
               String name = key.substring(JMS_AMQP_MESSAGE_ANNOTATION_PREFIX.length());
               maMap.put(Symbol.valueOf(name), message.getObjectProperty(key));
               continue;
            } else if (key.equals(JMS_AMQP_CONTENT_TYPE)) {
               if (properties == null) {
                  properties = new Properties();
               }
               properties.setContentType(Symbol.getSymbol(message.getStringProperty(key)));
               continue;
            } else if (key.equals(JMS_AMQP_CONTENT_ENCODING)) {
               if (properties == null) {
                  properties = new Properties();
               }
               properties.setContentEncoding(Symbol.getSymbol(message.getStringProperty(key)));
               continue;
            } else if (key.equals(JMS_AMQP_REPLYTO_GROUP_ID)) {
               if (properties == null) {
                  properties = new Properties();
               }
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
            if (properties == null) {
               properties = new Properties();
            }
            properties.setGroupId(value);
            continue;
         } else if (key.equals(ServerJMSMessage.NATIVE_MESSAGE_ID)) {
            // skip..internal use only
            continue;
         } else if (key.endsWith(HDR_SCHEDULED_DELIVERY_TIME.toString())) {
            // skip..remove annotation from previous inbound transformation
            continue;
         } else if (key.equals(AMQPMessageTypes.AMQP_TYPE_KEY)) {
            // skip..internal use only - TODO - Remove this deprecated value in future release.
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

      EncoderImpl encoder = tlsCodec.get().encoder;
      encoder.setByteBuffer(buffer);

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

      return messageFormat;
   }

   private Section convertBody(ServerJMSMessage message) throws JMSException {

      Section body = null;
      short orignalEncoding = AMQP_UNKNOWN;

      try {
         orignalEncoding = message.getShortProperty(JMS_AMQP_ORIGINAL_ENCODING);
      } catch (Exception ex) {
         // Ignore and stick with UNKNOWN
      }

      if (message instanceof ServerJMSBytesMessage) {
         Binary payload = getBinaryFromMessageBody((ServerJMSBytesMessage) message);

         if (payload == null) {
            payload = EMPTY_BINARY;
         }

         switch (orignalEncoding) {
            case AMQP_NULL:
               break;
            case AMQP_VALUE_BINARY:
               body = new AmqpValue(payload);
               break;
            case AMQP_DATA:
            case AMQP_UNKNOWN:
            default:
               body = new Data(payload);
               break;
         }
      } else if (message instanceof ServerJMSTextMessage) {
         switch (orignalEncoding) {
            case AMQP_NULL:
               break;
            case AMQP_DATA:
               body = new Data(getBinaryFromMessageBody((ServerJMSTextMessage) message));
               break;
            case AMQP_VALUE_STRING:
            case AMQP_UNKNOWN:
            default:
               body = new AmqpValue(((TextMessage) message).getText());
               break;
         }
      } else if (message instanceof ServerJMSMapMessage) {
         body = new AmqpValue(getMapFromMessageBody((ServerJMSMapMessage) message));
      } else if (message instanceof ServerJMSStreamMessage) {
         ArrayList<Object> list = new ArrayList<>();
         final ServerJMSStreamMessage m = (ServerJMSStreamMessage) message;
         try {
            while (true) {
               list.add(m.readObject());
            }
         } catch (MessageEOFException e) {
         }

         // Deprecated encoding markers - TODO - Remove on future release
         if (orignalEncoding == AMQP_UNKNOWN) {
            String amqpType = message.getStringProperty(AMQPMessageTypes.AMQP_TYPE_KEY);
            if (amqpType != null) {
               if (amqpType.equals(AMQPMessageTypes.AMQP_LIST)) {
                  orignalEncoding = AMQP_VALUE_LIST;
               } else {
                  orignalEncoding = AMQP_SEQUENCE;
               }
            }
         }

         switch (orignalEncoding) {
            case AMQP_SEQUENCE:
               body = new AmqpSequence(list);
               break;
            case AMQP_VALUE_LIST:
            case AMQP_UNKNOWN:
            default:
               body = new AmqpValue(list);
               break;
         }
      } else if (message instanceof ServerJMSObjectMessage) {
         Binary payload = getBinaryFromMessageBody((ServerJMSObjectMessage) message);

         if (payload == null) {
            payload = EMPTY_BINARY;
         }

         switch (orignalEncoding) {
            case AMQP_VALUE_BINARY:
               body = new AmqpValue(payload);
               break;
            case AMQP_DATA:
            case AMQP_UNKNOWN:
            default:
               body = new Data(payload);
               break;
         }

         // For a non-AMQP message we tag the outbound content type as containing
         // a serialized Java object so that an AMQP client has a hint as to what
         // we are sending it.
         if (!message.propertyExists(JMS_AMQP_CONTENT_TYPE)) {
            message.setStringProperty(JMS_AMQP_CONTENT_TYPE, SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);
         }
      } else if (message instanceof ServerJMSMessage) {
         // If this is not an AMQP message that was converted then the original encoding
         // will be unknown so we check for special cases of messages with special data
         // encoded into the server message body.
         if (orignalEncoding == AMQP_UNKNOWN) {
            MessageInternal internalMessage = message.getInnerMessage();
            int readerIndex = internalMessage.getBodyBuffer().readerIndex();
            try {
               Object s = internalMessage.getBodyBuffer().readNullableSimpleString();
               if (s != null) {
                  body = new AmqpValue(s.toString());
               }
            } catch (Throwable ignored) {
               logger.debug("Exception ignored during conversion, should be ok!", ignored.getMessage(), ignored);
            } finally {
               internalMessage.getBodyBuffer().readerIndex(readerIndex);
            }
         }
      }

      return body;
   }

   private Binary getBinaryFromMessageBody(ServerJMSBytesMessage message) throws JMSException {
      byte[] data = new byte[(int) message.getBodyLength()];
      message.readBytes(data);
      message.reset(); // Need to reset after readBytes or future readBytes

      return new Binary(data);
   }

   private Binary getBinaryFromMessageBody(ServerJMSTextMessage message) throws JMSException {
      Binary result = null;
      String text = message.getText();
      if (text != null) {
         result = new Binary(text.getBytes(StandardCharsets.UTF_8));
      }

      return result;
   }

   private Binary getBinaryFromMessageBody(ServerJMSObjectMessage message) throws JMSException {
      message.getInnerMessage().getBodyBuffer().resetReaderIndex();
      int size = message.getInnerMessage().getBodyBuffer().readInt();
      byte[] bytes = new byte[size];
      message.getInnerMessage().getBodyBuffer().readBytes(bytes);

      return new Binary(bytes);
   }

   private Map<String, Object> getMapFromMessageBody(ServerJMSMapMessage message) throws JMSException {
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
