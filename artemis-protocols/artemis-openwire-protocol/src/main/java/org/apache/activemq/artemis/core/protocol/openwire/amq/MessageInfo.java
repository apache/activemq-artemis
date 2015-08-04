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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

import org.apache.activemq.command.MessageId;

public class MessageInfo {

   public MessageId amqId;
   public long nativeId;
   public int size;
   //mark message that is acked within a local tx
   public boolean localAcked;

   public MessageInfo(MessageId amqId, long nativeId, int size) {
      this.amqId = amqId;
      this.nativeId = nativeId;
      this.size = size;
   }

   @Override
   public String toString() {
      return "native mid: " + this.nativeId + " amqId: " + amqId + " local acked: " + localAcked;
   }

   public void setLocalAcked(boolean ack) {
      localAcked = ack;
   }

   public boolean isLocalAcked() {
      return localAcked;
   }
}
