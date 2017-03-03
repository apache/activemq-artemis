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
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.DataConstants;

public class SessionReceiveMessage extends MessagePacket {
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long consumerID;

   private int deliveryCount;

   public SessionReceiveMessage(final long consumerID, final Message message, final int deliveryCount) {
      super(SESS_RECEIVE_MSG, message);

      this.consumerID = consumerID;

      this.deliveryCount = deliveryCount;
   }

   public SessionReceiveMessage(final Message message) {
      super(SESS_RECEIVE_MSG, message);
   }

   // Public --------------------------------------------------------

   public long getConsumerID() {
      return consumerID;
   }

   public int getDeliveryCount() {
      return deliveryCount;
   }

   @Override
   protected ActiveMQBuffer createPacket(RemotingConnection connection, boolean usePooled) {
      return internalCreatePacket(message.getEncodeSize() + PACKET_HEADERS_SIZE + DataConstants.SIZE_LONG + DataConstants.SIZE_INT, connection, usePooled);
   }

   @Override
   public void encodeRest(ActiveMQBuffer buffer) {
      message.sendBuffer(buffer.byteBuf(), deliveryCount);
      buffer.writeLong(consumerID);
      buffer.writeInt(deliveryCount);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      // Buffer comes in after having read standard headers and positioned at Beginning of body part

      message.receiveBuffer(copyMessageBuffer(buffer.byteBuf(), DataConstants.SIZE_LONG + DataConstants.SIZE_INT));

      buffer.readerIndex(buffer.capacity() - DataConstants.SIZE_LONG - DataConstants.SIZE_INT);
      this.consumerID = buffer.readLong();
      this.deliveryCount = buffer.readInt();

   }
   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int) (consumerID ^ (consumerID >>> 32));
      result = prime * result + deliveryCount;
      return result;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", consumerID=" + consumerID);
      buff.append(", deliveryCount=" + deliveryCount);
      buff.append("]");
      return buff.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionReceiveMessage))
         return false;
      SessionReceiveMessage other = (SessionReceiveMessage) obj;
      if (consumerID != other.consumerID)
         return false;
      if (deliveryCount != other.deliveryCount)
         return false;
      return true;
   }

}
