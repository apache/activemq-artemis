/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;

public abstract class QueueAbstractPacket extends PacketImpl {

   protected SimpleString queueName;
   protected SimpleString oldVersionQueueName;

   protected SimpleString address;
   protected SimpleString oldVersionAddresseName;

   public SimpleString getQueueName(int clientVersion) {

      if (clientVersion < ADDRESSING_CHANGE_VERSION) {
         if (oldVersionQueueName == null) {
            oldVersionQueueName = convertName(queueName);
         }

         return oldVersionQueueName;
      } else {
         return queueName;
      }
   }

   public SimpleString getAddress(int clientVersion) {

      if (clientVersion < ADDRESSING_CHANGE_VERSION) {
         if (oldVersionAddresseName == null) {
            oldVersionAddresseName = convertName(address);
         }

         return oldVersionAddresseName;
      } else {
         return address;
      }
   }

   /**
    * It converts the given {@code queueNames} using the JMS prefix found on {@link #address} when {@code clientVersion < }{@link #ADDRESSING_CHANGE_VERSION}.
    * If no conversion has occurred, it returns {@code queueNames}.
    *
    * @param clientVersion version of the client
    * @param queueNames    names of the queues to be converted
    * @return the converted queues names or {@code queueNames} when no conversion has occurred
    */
   public final List<SimpleString> convertQueueNames(int clientVersion, List<SimpleString> queueNames) {
      if (clientVersion < ADDRESSING_CHANGE_VERSION) {
         return applyAddressPrefixTo(queueNames);
      } else {
         return queueNames;
      }
   }

   private List<SimpleString> applyAddressPrefixTo(List<SimpleString> queueNames) {
      final int names = queueNames.size();
      if (names == 0) {
         return Collections.emptyList();
      } else {
         final SimpleString address = this.address;
         final SimpleString prefix = jmsPrefixOf(address);
         if (prefix != null) {
            final List<SimpleString> prefixedQueueNames = new ArrayList<>(names);
            for (int i = 0; i < names; i++) {
               final SimpleString oldQueueNames = queueNames.get(i);
               final SimpleString prefixedQueueName = prefix.concat(oldQueueNames);
               prefixedQueueNames.add(prefixedQueueName);
            }
            return prefixedQueueNames;
         } else {
            return queueNames;
         }
      }
   }

   private static SimpleString jmsPrefixOf(SimpleString address) {
      if (address.startsWith(OLD_QUEUE_PREFIX)) {
         return OLD_QUEUE_PREFIX;
      } else if (address.startsWith(OLD_TOPIC_PREFIX)) {
         return OLD_TOPIC_PREFIX;
      } else {
         return null;
      }
   }

   public QueueAbstractPacket(byte type) {
      super(type);
   }
}
