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
package org.apache.activemq.artemis.integration.kafka.protocol.core.jms;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import java.util.Map;

import org.apache.activemq.artemis.integration.kafka.protocol.core.CoreMessageSerializer;
import org.apache.activemq.artemis.jms.client.ActiveMQBytesMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQMapMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQObjectMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQStreamMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQTextMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedSerializer;

public class CoreJmsMessageSerializer implements ExtendedSerializer<Message> {

   CoreMessageSerializer coreMessageSerializer = new CoreMessageSerializer();


   @Override
   public byte[] serialize(String topic, Headers headers, Message message) {
      if (message == null) return null;
      try {
         ActiveMQMessage activeMQJmsMessage;
         // First convert from foreign message if appropriate
         if (!(message instanceof ActiveMQMessage)) {
            // JMS 1.1 Sect. 3.11.4: A provider must be prepared to accept, from a client,
            // a message whose implementation is not one of its own.

            if (message instanceof BytesMessage) {
               activeMQJmsMessage = new ActiveMQBytesMessage((BytesMessage) message, null);
            } else if (message instanceof MapMessage) {
               activeMQJmsMessage = new ActiveMQMapMessage((MapMessage) message, null);
            } else if (message instanceof ObjectMessage) {
               activeMQJmsMessage = new ActiveMQObjectMessage((ObjectMessage) message, null, null);
            } else if (message instanceof StreamMessage) {
               activeMQJmsMessage = new ActiveMQStreamMessage((StreamMessage) message, null);
            } else if (message instanceof TextMessage) {
               activeMQJmsMessage = new ActiveMQTextMessage((TextMessage) message, null);
            } else {
               activeMQJmsMessage = new ActiveMQMessage(message, null);
            }
         } else {
            activeMQJmsMessage = (ActiveMQMessage) message;
         }
         activeMQJmsMessage.doBeforeSend();
         return coreMessageSerializer.serialize(topic, headers, activeMQJmsMessage.getCoreMessage());
      } catch (JMSException e) {
         throw new SerializationException(e);
      } catch (Exception e) {
         throw new SerializationException(e);
      }
   }


   @Override
   public byte[] serialize(String topic, Message message) {
      if (message == null) return null;
      return serialize(topic, null, message);
   }

   @Override
   public void configure(Map<String, ?> configs, boolean isKey) {
      coreMessageSerializer.configure(configs, isKey);
   }

   @Override
   public void close() {
      coreMessageSerializer.close();
   }

}