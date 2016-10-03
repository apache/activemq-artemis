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

import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_DATA;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_NULL;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_SEQUENCE;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_VALUE_BINARY;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_VALUE_LIST;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_VALUE_MAP;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_VALUE_NULL;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_VALUE_STRING;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_MESSAGE_FORMAT;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_ORIGINAL_ENCODING;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.OCTET_STREAM_CONTENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.createBytesMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.createMapMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.createMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.createObjectMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.createStreamMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.createTextMessage;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.getCharsetForTextualContent;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.isContentType;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

public class JMSMappingInboundTransformer extends InboundTransformer {

   public JMSMappingInboundTransformer(IDGenerator idGenerator) {
      super(idGenerator);
   }

   @Override
   public String getTransformerName() {
      return TRANSFORMER_JMS;
   }

   @Override
   public InboundTransformer getFallbackTransformer() {
      return new AMQPNativeInboundTransformer(idGenerator);
   }

   @Override
   public ServerJMSMessage transform(EncodedMessage encodedMessage) throws Exception {
      ServerJMSMessage transformedMessage = null;

      try {
         Message amqpMessage = encodedMessage.decode();
         transformedMessage = createServerMessage(amqpMessage);
         populateMessage(transformedMessage, amqpMessage);
      } catch (Exception ex) {
         InboundTransformer transformer = this.getFallbackTransformer();

         while (transformer != null) {
            try {
               transformedMessage = transformer.transform(encodedMessage);
               break;
            } catch (Exception e) {
               transformer = transformer.getFallbackTransformer();
            }
         }
      }

      // Regardless of the transformer that finally decoded the message we need to ensure that
      // the AMQP Message Format value is preserved for application on retransmit.
      if (transformedMessage != null && encodedMessage.getMessageFormat() != 0) {
         transformedMessage.setLongProperty(JMS_AMQP_MESSAGE_FORMAT, encodedMessage.getMessageFormat());
      }

      return transformedMessage;
   }

   @SuppressWarnings("unchecked")
   private ServerJMSMessage createServerMessage(Message message) throws Exception {

      Section body = message.getBody();
      ServerJMSMessage result;

      if (body == null) {
         if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, message)) {
            result = createObjectMessage(idGenerator);
         } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, message) || isContentType(null, message)) {
            result = createBytesMessage(idGenerator);
         } else {
            Charset charset = getCharsetForTextualContent(message.getContentType());
            if (charset != null) {
               result = createTextMessage(idGenerator);
            } else {
               result = createMessage(idGenerator);
            }
         }

         result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_NULL);
      } else if (body instanceof Data) {
         Binary payload = ((Data) body).getValue();

         if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, message)) {
            result = createObjectMessage(idGenerator, payload.getArray(), payload.getArrayOffset(), payload.getLength());
         } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, message)) {
            result = createBytesMessage(idGenerator, payload.getArray(), payload.getArrayOffset(), payload.getLength());
         } else {
            Charset charset = getCharsetForTextualContent(message.getContentType());
            if (StandardCharsets.UTF_8.equals(charset)) {
               ByteBuffer buf = ByteBuffer.wrap(payload.getArray(), payload.getArrayOffset(), payload.getLength());

               try {
                  CharBuffer chars = charset.newDecoder().decode(buf);
                  result = createTextMessage(idGenerator, String.valueOf(chars));
               } catch (CharacterCodingException e) {
                  result = createBytesMessage(idGenerator, payload.getArray(), payload.getArrayOffset(), payload.getLength());
               }
            } else {
               result = createBytesMessage(idGenerator, payload.getArray(), payload.getArrayOffset(), payload.getLength());
            }
         }

         result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_DATA);
      } else if (body instanceof AmqpSequence) {
         AmqpSequence sequence = (AmqpSequence) body;
         ServerJMSStreamMessage m = createStreamMessage(idGenerator);
         for (Object item : sequence.getValue()) {
            m.writeObject(item);
         }

         result = m;
         result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_SEQUENCE);
      } else if (body instanceof AmqpValue) {
         Object value = ((AmqpValue) body).getValue();
         if (value == null || value instanceof String) {
            result = createTextMessage(idGenerator, (String) value);

            result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, value == null ? AMQP_VALUE_NULL : AMQP_VALUE_STRING);
         } else if (value instanceof Binary) {
            Binary payload = (Binary) value;

            if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, message)) {
               result = createObjectMessage(idGenerator, payload);
            } else {
               result = createBytesMessage(idGenerator, payload.getArray(), payload.getArrayOffset(), payload.getLength());
            }

            result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
         } else if (value instanceof List) {
            ServerJMSStreamMessage m = createStreamMessage(idGenerator);
            for (Object item : (List<Object>) value) {
               m.writeObject(item);
            }
            result = m;
            result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_LIST);
         } else if (value instanceof Map) {
            result = createMapMessage(idGenerator, (Map<String, Object>) value);
            result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_MAP);
         } else {
            // Trigger fall-back to native encoder which generates BytesMessage with the
            // original message stored in the message body.
            throw new ActiveMQAMQPInternalErrorException("Unable to encode to ActiveMQ JMS Message");
         }
      } else {
         throw new RuntimeException("Unexpected body type: " + body.getClass());
      }

      return result;
   }
}
