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

package org.apache.activemq.artemis.core.protocol.mqtt;

/**
 * MQTT Acks only hold message ID information.  From this we must infer the internal message ID and consumer.
 */
class MQTTMessageInfo {

   private long serverMessageId;

   private long consumerId;

   private String address;

   MQTTMessageInfo(long serverMessageId, long consumerId, String address) {
      this.serverMessageId = serverMessageId;
      this.consumerId = consumerId;
      this.address = address;
   }

   long getServerMessageId() {
      return serverMessageId;
   }

   long getConsumerId() {
      return consumerId;
   }

   String getAddress() {
      return address;
   }

   @Override
   public String toString() {
      return ("ServerMessageId: " + serverMessageId + " ConsumerId: " + consumerId + " addr: " + address);
   }
}
