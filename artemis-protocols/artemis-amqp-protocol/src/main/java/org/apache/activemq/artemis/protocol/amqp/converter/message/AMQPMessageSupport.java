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

import static org.apache.activemq.artemis.api.core.Message.BYTES_TYPE;
import static org.apache.activemq.artemis.api.core.Message.DEFAULT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.MAP_TYPE;
import static org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.STREAM_TYPE;
import static org.apache.activemq.artemis.api.core.Message.TEXT_TYPE;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.artemis.core.buffers.impl.ResetLimitWrappedActiveMQBuffer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMapMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSObjectMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSTextMessage;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInvalidContentTypeException;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;

/**
 * Support class containing constant values and static methods that are used to map to / from
 * AMQP Message types being sent or received.
 */
public final class AMQPMessageSupport {

   // Message Properties used to map AMQP to JMS and back

   public static final String JMS_AMQP_PREFIX = "JMS_AMQP_";
   public static final int JMS_AMQP_PREFIX_LENGTH = JMS_AMQP_PREFIX.length();

   public static final String MESSAGE_FORMAT = "MESSAGE_FORMAT";
   public static final String ORIGINAL_ENCODING = "ORIGINAL_ENCODING";
   public static final String NATIVE = "NATIVE";
   public static final String HEADER = "HEADER";
   public static final String PROPERTIES = "PROPERTIES";

   public static final String FIRST_ACQUIRER = "FirstAcquirer";
   public static final String CONTENT_TYPE = "ContentType";
   public static final String CONTENT_ENCODING = "ContentEncoding";
   public static final String REPLYTO_GROUP_ID = "ReplyToGroupID";

   public static final String DELIVERY_ANNOTATION_PREFIX = "DA_";
   public static final String MESSAGE_ANNOTATION_PREFIX = "MA_";
   public static final String FOOTER_PREFIX = "FT_";

   public static final String JMS_AMQP_HEADER = JMS_AMQP_PREFIX + HEADER;
   public static final String JMS_AMQP_PROPERTIES = JMS_AMQP_PREFIX + PROPERTIES;
   public static final String JMS_AMQP_ORIGINAL_ENCODING = JMS_AMQP_PREFIX + ORIGINAL_ENCODING;
   public static final String JMS_AMQP_MESSAGE_FORMAT = JMS_AMQP_PREFIX + MESSAGE_FORMAT;
   public static final String JMS_AMQP_NATIVE = JMS_AMQP_PREFIX + NATIVE;
   public static final String JMS_AMQP_FIRST_ACQUIRER = JMS_AMQP_PREFIX + FIRST_ACQUIRER;
   public static final String JMS_AMQP_CONTENT_TYPE = JMS_AMQP_PREFIX + CONTENT_TYPE;
   public static final String JMS_AMQP_CONTENT_ENCODING = JMS_AMQP_PREFIX + CONTENT_ENCODING;
   public static final String JMS_AMQP_REPLYTO_GROUP_ID = JMS_AMQP_PREFIX + REPLYTO_GROUP_ID;
   public static final String JMS_AMQP_DELIVERY_ANNOTATION_PREFIX = JMS_AMQP_PREFIX + DELIVERY_ANNOTATION_PREFIX;
   public static final String JMS_AMQP_MESSAGE_ANNOTATION_PREFIX = JMS_AMQP_PREFIX + MESSAGE_ANNOTATION_PREFIX;
   public static final String JMS_AMQP_FOOTER_PREFIX = JMS_AMQP_PREFIX + FOOTER_PREFIX;

   // Message body type definitions
   public static final Binary EMPTY_BINARY = new Binary(new byte[0]);
   public static final Data EMPTY_BODY = new Data(EMPTY_BINARY);

   public static final short AMQP_UNKNOWN = 0;
   public static final short AMQP_NULL = 1;
   public static final short AMQP_DATA = 2;
   public static final short AMQP_SEQUENCE = 3;
   public static final short AMQP_VALUE_NULL = 4;
   public static final short AMQP_VALUE_STRING = 5;
   public static final short AMQP_VALUE_BINARY = 6;
   public static final short AMQP_VALUE_MAP = 7;
   public static final short AMQP_VALUE_LIST = 8;

   /**
    * Content type used to mark Data sections as containing a serialized java object.
    */
   public static final String SERIALIZED_JAVA_OBJECT_CONTENT_TYPE = "application/x-java-serialized-object";

   /**
    * Content type used to mark Data sections as containing arbitrary bytes.
    */
   public static final String OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";

   /**
    * Lookup and return the correct Proton Symbol instance based on the given key.
    *
    * @param key
    *        the String value name of the Symbol to locate.
    *
    * @return the Symbol value that matches the given key.
    */
   public static Symbol getSymbol(String key) {
      return Symbol.valueOf(key);
   }

   /**
    * Safe way to access message annotations which will check internal structure and either
    * return the annotation if it exists or null if the annotation or any annotations are
    * present.
    *
    * @param key
    *        the String key to use to lookup an annotation.
    * @param message
    *        the AMQP message object that is being examined.
    *
    * @return the given annotation value or null if not present in the message.
    */
   public static Object getMessageAnnotation(String key, Message message) {
      if (message != null && message.getMessageAnnotations() != null) {
         Map<Symbol, Object> annotations = message.getMessageAnnotations().getValue();
         return annotations.get(AMQPMessageSupport.getSymbol(key));
      }

      return null;
   }

   /**
    * Check whether the content-type field of the properties section (if present) in the given
    * message matches the provided string (where null matches if there is no content type
    * present.
    *
    * @param contentType
    *        content type string to compare against, or null if none
    * @param message
    *        the AMQP message object that is being examined.
    *
    * @return true if content type matches
    */
   public static boolean isContentType(String contentType, Message message) {
      if (contentType == null) {
         return message.getContentType() == null;
      } else {
         return contentType.equals(message.getContentType());
      }
   }

   /**
    * @param contentType
    *        the contentType of the received message
    * @return the character set to use, or null if not to treat the message as text
    */
   public static Charset getCharsetForTextualContent(String contentType) {
      try {
         return AMQPContentTypeSupport.parseContentTypeForTextualCharset(contentType);
      } catch (ActiveMQAMQPInvalidContentTypeException e) {
         return null;
      }
   }

   public static ServerJMSMessage wrapMessage(int messageType, ServerMessage wrapped, int deliveryCount) {
      switch (messageType) {
         case STREAM_TYPE:
            return new ServerJMSStreamMessage(wrapped, deliveryCount);
         case BYTES_TYPE:
            return new ServerJMSBytesMessage(wrapped, deliveryCount);
         case MAP_TYPE:
            return new ServerJMSMapMessage(wrapped, deliveryCount);
         case TEXT_TYPE:
            return new ServerJMSTextMessage(wrapped, deliveryCount);
         case OBJECT_TYPE:
            return new ServerJMSObjectMessage(wrapped, deliveryCount);
         default:
            return new ServerJMSMessage(wrapped, deliveryCount);
      }
   }

   public static String toAddress(Destination destination) {
      if (destination instanceof ActiveMQDestination) {
         return ((ActiveMQDestination) destination).getAddress();
      }
      return null;
   }

   public static ServerJMSBytesMessage createBytesMessage(IDGenerator idGenerator) {
      return new ServerJMSBytesMessage(newMessage(idGenerator, BYTES_TYPE), 0);
   }

   public static ServerJMSMessage createBytesMessage(IDGenerator idGenerator, byte[] array, int arrayOffset, int length) throws JMSException {
      ServerJMSBytesMessage message = createBytesMessage(idGenerator);
      message.writeBytes(array, arrayOffset, length);
      return message;
   }

   public static ServerJMSStreamMessage createStreamMessage(IDGenerator idGenerator) {
      return new ServerJMSStreamMessage(newMessage(idGenerator, STREAM_TYPE), 0);
   }

   public static ServerJMSMessage createMessage(IDGenerator idGenerator) {
      return new ServerJMSMessage(newMessage(idGenerator, DEFAULT_TYPE), 0);
   }

   public static ServerJMSTextMessage createTextMessage(IDGenerator idGenerator) {
      return new ServerJMSTextMessage(newMessage(idGenerator, TEXT_TYPE), 0);
   }

   public static ServerJMSTextMessage createTextMessage(IDGenerator idGenerator, String text) throws JMSException {
      ServerJMSTextMessage message = createTextMessage(idGenerator);
      message.setText(text);
      return message;
   }

   public static ServerJMSObjectMessage createObjectMessage(IDGenerator idGenerator) {
      return new ServerJMSObjectMessage(newMessage(idGenerator, OBJECT_TYPE), 0);
   }

   public static ServerJMSMessage createObjectMessage(IDGenerator idGenerator, Binary serializedForm) throws JMSException {
      ServerJMSObjectMessage message = createObjectMessage(idGenerator);
      message.setSerializedForm(serializedForm);
      return message;
   }

   public static ServerJMSMessage createObjectMessage(IDGenerator idGenerator, byte[] array, int offset, int length) throws JMSException {
      ServerJMSObjectMessage message = createObjectMessage(idGenerator);
      message.setSerializedForm(new Binary(array, offset, length));
      return message;
   }

   public static ServerJMSMapMessage createMapMessage(IDGenerator idGenerator) {
      return new ServerJMSMapMessage(newMessage(idGenerator, MAP_TYPE), 0);
   }

   public static ServerJMSMapMessage createMapMessage(IDGenerator idGenerator, Map<String, Object> content) throws JMSException {
      ServerJMSMapMessage message = createMapMessage(idGenerator);
      final Set<Map.Entry<String, Object>> set = content.entrySet();
      for (Map.Entry<String, Object> entry : set) {
         Object value = entry.getValue();
         if (value instanceof Binary) {
            Binary binary = (Binary) value;
            value = Arrays.copyOfRange(binary.getArray(), binary.getArrayOffset(), binary.getLength());
         }
         message.setObject(entry.getKey(), value);
      }
      return message;
   }

   private static ServerMessageImpl newMessage(IDGenerator idGenerator, byte messageType) {
      ServerMessageImpl message = new ServerMessageImpl(idGenerator.generateID(), 512);
      message.setType(messageType);
      ((ResetLimitWrappedActiveMQBuffer) message.getBodyBuffer()).setMessage(null);
      return message;
   }
}
