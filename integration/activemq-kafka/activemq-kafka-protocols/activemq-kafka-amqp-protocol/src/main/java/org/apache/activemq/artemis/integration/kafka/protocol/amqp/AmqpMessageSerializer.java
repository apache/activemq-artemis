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
package org.apache.activemq.artemis.integration.kafka.protocol.amqp;

import java.util.Map;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.integration.kafka.protocol.amqp.proton.ProtonMessageSerializer;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.CoreAmqpConverter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class AmqpMessageSerializer implements Serializer<Message> {

   ProtonMessageSerializer protonMessageSerializer = new ProtonMessageSerializer();

   @Override
   public byte[] serialize(String topic, Message message) {
      if (message == null) return null;
      try {
         AMQPMessage amqpMessage = CoreAmqpConverter.checkAMQP(message);
         return protonMessageSerializer.serialize(topic, amqpMessage.getProtonMessage());
      } catch (Exception e) {
         throw new SerializationException(e);
      }
   }

   @Override
   public void configure(Map<String, ?> configs, boolean isKey) {
      protonMessageSerializer.configure(configs, isKey);
   }

   @Override
   public void close() {
      protonMessageSerializer.close();
   }
}
