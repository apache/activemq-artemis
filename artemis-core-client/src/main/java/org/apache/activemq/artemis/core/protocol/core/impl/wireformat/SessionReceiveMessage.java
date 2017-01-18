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
import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.DataConstants;

public class SessionReceiveMessage extends MessagePacket {
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long consumerID;

   private int deliveryCount;

   public SessionReceiveMessage(final long consumerID, final MessageInternal message, final int deliveryCount) {
      super(SESS_RECEIVE_MSG, message);

      this.consumerID = consumerID;

      this.deliveryCount = deliveryCount;
   }

   public SessionReceiveMessage(final MessageInternal message) {
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
   public ActiveMQBuffer encode(final RemotingConnection connection) {
      ActiveMQBuffer buffer = message.getEncodedBuffer();

      ActiveMQBuffer bufferWrite = connection.createTransportBuffer(buffer.writerIndex(), true);
      bufferWrite.writeBytes(buffer, 0, bufferWrite.capacity());
      bufferWrite.setIndex(buffer.readerIndex(), buffer.writerIndex());

      // Sanity check
      if (bufferWrite.writerIndex() != message.getEndOfMessagePosition()) {
         throw new IllegalStateException("Wrong encode position");
      }

      bufferWrite.writeLong(consumerID);
      bufferWrite.writeInt(deliveryCount);

      size = bufferWrite.writerIndex();

      // Write standard headers

      int len = size - DataConstants.SIZE_INT;
      bufferWrite.setInt(0, len);
      bufferWrite.setByte(DataConstants.SIZE_INT, getType());
      bufferWrite.setLong(DataConstants.SIZE_INT + DataConstants.SIZE_BYTE, channelID);

      // Position reader for reading by Netty
      bufferWrite.setIndex(0, size);

      return bufferWrite;
   }

   @Override
   public void decode(final ActiveMQBuffer buffer) {
      channelID = buffer.readLong();

      message.decodeFromBuffer(buffer);

      consumerID = buffer.readLong();

      deliveryCount = buffer.readInt();

      size = buffer.readerIndex();

      // Need to position buffer for reading

      buffer.setIndex(PACKET_HEADERS_SIZE + DataConstants.SIZE_INT, message.getEndOfBodyPosition());
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
