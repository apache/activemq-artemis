/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.utils.DataConstants;

public class SessionReceiveMessage_1X extends SessionReceiveMessage {

   public SessionReceiveMessage_1X(long consumerID, ICoreMessage message, int deliveryCount) {
      super(consumerID, message, deliveryCount);
   }

   public SessionReceiveMessage_1X(CoreMessage message) {
      super(message);
   }

   @Override
   public void encodeRest(ActiveMQBuffer buffer) {
      message.sendBuffer_1X(buffer.byteBuf());
      buffer.writeLong(consumerID);
      buffer.writeInt(deliveryCount);
   }

   @Override
   protected void receiveMessage(ByteBuf buffer) {
      message.receiveBuffer_1X(buffer);
   }

   @Override
   public int expectedEncodeSize() {
      return super.expectedEncodeSize() + DataConstants.SIZE_INT;
   }

}
