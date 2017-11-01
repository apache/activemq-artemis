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

import javax.jms.Message;

import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.integration.kafka.protocol.core.CoreMessageDeserializer;
import org.apache.activemq.artemis.jms.client.ActiveMQMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedDeserializer;

public class CoreJmsMessageDeserializer implements ExtendedDeserializer<Message> {

   CoreMessageDeserializer coreMessageDeserializer = new CoreMessageDeserializer();

   @Override
   public Message deserialize(String topic, Headers headers, byte[] bytes) {
      if (bytes == null) return null;
      ClientMessageImpl clientMessage = new ClientMessageImpl();
      clientMessage.initBuffer(bytes.length);
      coreMessageDeserializer.internalDeserialize(topic, clientMessage, headers, bytes);
      ActiveMQMessage activeMQMessage = ActiveMQMessage.createMessage(clientMessage, null);
      try {
         activeMQMessage.doBeforeReceive();
      } catch (ActiveMQException e) {
         throw new SerializationException(e);
      }
      return activeMQMessage;
   }

   @Override
   public void configure(Map<String, ?> configs, boolean isKey) {
      coreMessageDeserializer.configure(configs, isKey);
   }

   @Override
   public Message deserialize(String topic, byte[] bytes) {
      if (bytes == null) return null;
      return deserialize(topic, bytes);
   }

   @Override
   public void close() {
      coreMessageDeserializer.close();
   }
}
