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
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;


public class CoreMessageDeserializer implements ExtendedDeserializer<Message> {

   private StringDeserializer stringDeserializer = new StringDeserializer();

   @Override
   public void configure(Map<String, ?> configs, boolean isKey) {
      stringDeserializer.configure(configs, isKey);
   }

   @Override
   public Message deserialize(String topic, Headers headers, byte[] bytes) {
      CoreMessage coreMessage = new CoreMessage();
      coreMessage.initBuffer(bytes.length);
      return internalDeserialize(topic, coreMessage, headers, bytes);
   }

   @Override
   public Message deserialize(String topic, byte[] bytes) {
      return deserialize(topic, null, bytes);
   }

   public <C extends CoreMessage> C internalDeserialize(String topic, C coreMessage, Headers headers, byte[] bytes) {
      if (headers != null) {
         headers.forEach(header -> {
            String value = stringDeserializer.deserialize(null, header.value());
            String key = header.key();
            switch (key) {
               case "messageID":
                  coreMessage.setMessageID(Long.valueOf(value));
                  break;
               case "address":
                  coreMessage.setAddress(value);
                  break;
               case "type":
                  coreMessage.setType(Byte.valueOf(value));
                  break;
               case "durable":
                  coreMessage.setDurable(Boolean.valueOf(value));
                  break;
               case "expiration":
                  coreMessage.setExpiration(Long.valueOf(value));
                  break;
               case "timestamp":
                  coreMessage.setTimestamp(Long.valueOf(value));
                  break;
               case "priority":
                  coreMessage.setPriority(Byte.valueOf(value));
                  break;
               default:
                  coreMessage.putStringProperty(key, value);
            }

         });
      }
      if (coreMessage.getType() == TEXT_TYPE) {
         String body = stringDeserializer.deserialize(topic, bytes);
         TextMessageUtil.writeBodyText(coreMessage.getBodyBuffer(), SimpleString.toSimpleString(body));
      } else {
         ActiveMQBuffer byteBuf = coreMessage.getBodyBuffer();
         byteBuf.writeBytes(bytes);
      }
      return coreMessage;
   }

   @Override
   public void close() {
      stringDeserializer.close();
   }

}