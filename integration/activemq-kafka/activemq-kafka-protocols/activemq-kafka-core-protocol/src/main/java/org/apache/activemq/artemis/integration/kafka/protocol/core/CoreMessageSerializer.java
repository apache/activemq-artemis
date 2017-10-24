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
package org.apache.activemq.artemis.integration.kafka.protocol.core;

import static org.apache.activemq.artemis.api.core.Message.TEXT_TYPE;

import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class CoreMessageSerializer implements ExtendedSerializer<Message> {

   private StringSerializer stringSerializer = new StringSerializer();

   @Override
   public void configure(Map<String, ?> configs, boolean isKey) {
      stringSerializer.configure(configs, isKey);
   }

   @Override
   public byte[] serialize(String topic, Headers headers, Message message) {
      if (message == null) return null;
      ICoreMessage coreMessage = message.toCore();

      addHeader(headers, "messageID", coreMessage.getMessageID());
      addHeader(headers, "address", coreMessage.getAddress());
      addHeader(headers, "type", coreMessage.getType());
      addHeader(headers, "durable", coreMessage.isDurable());
      addHeader(headers, "expiration", coreMessage.getExpiration());
      addHeader(headers, "timestamp", coreMessage.getPriority());
      addHeader(headers, "priority", coreMessage.getPriority());

      coreMessage.getPropertyNames()
         .forEach(
            key -> addHeader(headers, key.toString(), coreMessage.getObjectProperty(key))
         );

      if (coreMessage.getType() == TEXT_TYPE) {
         return stringSerializer.serialize(topic, TextMessageUtil.readBodyText(coreMessage.getBodyBuffer()).toString());
      } else {
         return serialize(topic, coreMessage);
      }
   }

   private void addHeader(Headers headers, String name, Object value) {
      if (value != null)
         headers.add(name, stringSerializer.serialize(null, String.valueOf(value)));
   }

   @Override
   public byte[] serialize(String topic, Message message) {
      if (message == null) return null;
      ICoreMessage coreMessage = message.toCore();
      ActiveMQBuffer byteBuf = coreMessage.getBodyBuffer();
      byte[] bytes = new byte[byteBuf.readableBytes()];
      int readerIndex = byteBuf.readerIndex();
      byteBuf.getBytes(readerIndex, bytes);
      return bytes;
   }

   @Override
   public void close() {
      stringSerializer.close();
   }
}
