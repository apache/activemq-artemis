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
package org.apache.activemq.artemis.integration.kafka.protocol.amqp.proton;

import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedDeserializer;
import org.apache.qpid.proton.message.Message;

public class ProtonMessageDeserializer implements ExtendedDeserializer<Message> {

   @Override
   public void configure(Map<String, ?> configs, boolean isKey) {
   }

   @Override
   public Message deserialize(String topic, Headers headers, byte[] bytes) {
      if (bytes == null) return null;
      Message message = Message.Factory.create();
      message.decode(bytes, 0, bytes.length);
      return message;
   }

   @Override
   public Message deserialize(String topic, byte[] bytes) {
      return deserialize(topic, null, bytes);
   }

   @Override
   public void close() {

   }

}
