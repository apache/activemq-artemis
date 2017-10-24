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

import java.nio.BufferOverflowException;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.qpid.proton.message.Message;

public class ProtonMessageSerializer implements ExtendedSerializer<Message> {

   private int bufferSize = 4096;
   public static final String SERIALIZER_BUFFER_SIZE = "serializer.buffer.size";


   @Override
   public byte[] serialize(String topic, Headers headers, Message message) {
      if (message == null) return null;
      try {
         byte[] encodedMessage = new byte[bufferSize];
         while (true) {
            try {
               int encoded = message.encode(encodedMessage, 0, encodedMessage.length);
               return Arrays.copyOfRange(encodedMessage, 0, encoded);
            } catch (BufferOverflowException var5) {
               encodedMessage = new byte[encodedMessage.length * 2];
            }
         }
      } catch (Exception e) {
         throw new SerializationException(e);
      }
   }

   @Override
   public void configure(Map<String, ?> configs, boolean isKey) {
      Object bufferSizeValue = configs.get(ProtonMessageSerializer.SERIALIZER_BUFFER_SIZE);
      if (bufferSizeValue != null) {
         bufferSize = Integer.valueOf(bufferSizeValue.toString());
      }

   }

   @Override
   public byte[] serialize(String topic, Message message) {
      if (message == null) return null;
      return serialize(topic, null, message);
   }

   @Override
   public void close() {

   }
}
