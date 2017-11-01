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
import org.apache.activemq.artemis.integration.kafka.protocol.amqp.proton.ProtonMessageDeserializer;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.kafka.common.serialization.Deserializer;

public class AmqpMessageDeserializer implements Deserializer<Message> {

   ProtonMessageDeserializer protonMessageDeserializer = new ProtonMessageDeserializer();

   @Override
   public void configure(Map<String, ?> configs, boolean isKey) {
      protonMessageDeserializer.configure(configs, isKey);
   }

   @Override
   public Message deserialize(String topic, byte[] bytes) {
      if (bytes == null) return null;
      return new AMQPMessage(protonMessageDeserializer.deserialize(topic, bytes));
   }

   @Override
   public void close() {
      protonMessageDeserializer.close();
   }

}
