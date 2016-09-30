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

package org.apache.activemq.artemis.core.protocol.hornetq.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;

public class HQPropertiesConverter {

   private static Map<SimpleString, SimpleString> hqAmqDictionary;
   private static Map<SimpleString, SimpleString> amqHqDictionary;

   static {
      Map<SimpleString, SimpleString> d = new HashMap<>();

      // Add entries for outgoing messages
      d.put(new SimpleString("_HQ_ACTUAL_EXPIRY"), new SimpleString("_AMQ_ACTUAL_EXPIRY"));
      d.put(new SimpleString("_HQ_ORIG_ADDRESS"), new SimpleString("_AMQ_ORIG_ADDRESS"));
      d.put(new SimpleString("_HQ_ORIG_QUEUE"), new SimpleString("_AMQ_ORIG_QUEUE"));
      d.put(new SimpleString("_HQ_ORIG_MESSAGE_ID"), new SimpleString("_AMQ_ORIG_MESSAGE_ID"));
      d.put(new SimpleString("_HQ_GROUP_ID"), new SimpleString("_AMQ_GROUP_ID"));
      d.put(new SimpleString("_HQ_LARGE_COMPRESSED"), new SimpleString("_AMQ_LARGE_COMPRESSED"));
      d.put(new SimpleString("_HQ_LARGE_SIZE"), new SimpleString("_AMQ_LARGE_SIZE"));
      d.put(new SimpleString("_HQ_SCHED_DELIVERY"), new SimpleString("_AMQ_SCHED_DELIVERY"));
      d.put(new SimpleString("_HQ_DUPL_ID"), new SimpleString("_AMQ_DUPL_ID"));
      d.put(new SimpleString("_HQ_LVQ_NAME"), new SimpleString("_AMQ_LVQ_NAME"));

      hqAmqDictionary = Collections.unmodifiableMap(d);

      d = new HashMap<>();
      // inverting the direction
      for (Map.Entry<SimpleString, SimpleString> entry : hqAmqDictionary.entrySet()) {
         d.put(entry.getValue(), entry.getKey());
      }

      amqHqDictionary = Collections.unmodifiableMap(d);
   }

   public static void replaceAMQProperties(final Message message) {
      replaceDict(message, amqHqDictionary);
   }

   public static void replaceHQProperties(final Message message) {
      replaceDict(message, hqAmqDictionary);
   }

   private static void replaceDict(final Message message, Map<SimpleString, SimpleString> dictionary) {
      for (SimpleString property : new HashSet<>(message.getPropertyNames())) {
         SimpleString replaceTo = dictionary.get(property);
         if (replaceTo != null) {
            message.putObjectProperty(replaceTo, message.removeProperty(property));
         }
      }
   }

   public static boolean isMessagePacket(Packet packet) {
      int type = packet.getType();
      return type == PacketImpl.SESS_SEND ||
         type == PacketImpl.SESS_SEND_LARGE ||
         type == PacketImpl.SESS_RECEIVE_LARGE_MSG ||
         type == PacketImpl.SESS_RECEIVE_MSG;
   }

}
