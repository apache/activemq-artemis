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
package org.apache.activemq.core.protocol.core.impl.wireformat;

import org.apache.activemq.api.core.HornetQBuffer;
import org.apache.activemq.core.message.impl.MessageInternal;
import org.apache.activemq.spi.core.protocol.RemotingConnection;
import org.apache.activemq.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionReceiveMessage extends MessagePacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long consumerID;

   private int deliveryCount;

   public SessionReceiveMessage(final long consumerID, final MessageInternal message, final int deliveryCount)
   {
      super(SESS_RECEIVE_MSG, message);

      this.consumerID = consumerID;

      this.deliveryCount = deliveryCount;
   }

   public SessionReceiveMessage(final MessageInternal message)
   {
      super(SESS_RECEIVE_MSG, message);
   }

   // Public --------------------------------------------------------

   public long getConsumerID()
   {
      return consumerID;
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }

   @Override
   public HornetQBuffer encode(final RemotingConnection connection)
   {
      HornetQBuffer buffer = message.getEncodedBuffer();

      // Sanity check
      if (buffer.writerIndex() != message.getEndOfMessagePosition())
      {
         throw new IllegalStateException("Wrong encode position");
      }

      buffer.writeLong(consumerID);
      buffer.writeInt(deliveryCount);

      size = buffer.writerIndex();

      // Write standard headers

      int len = size - DataConstants.SIZE_INT;
      buffer.setInt(0, len);
      buffer.setByte(DataConstants.SIZE_INT, getType());
      buffer.setLong(DataConstants.SIZE_INT + DataConstants.SIZE_BYTE, channelID);

      // Position reader for reading by Netty
      buffer.setIndex(0, size);

      return buffer;
   }

   @Override
   public void decode(final HornetQBuffer buffer)
   {
      channelID = buffer.readLong();

      message.decodeFromBuffer(buffer);

      consumerID = buffer.readLong();

      deliveryCount = buffer.readInt();

      size = buffer.readerIndex();

      // Need to position buffer for reading

      buffer.setIndex(PACKET_HEADERS_SIZE + DataConstants.SIZE_INT, message.getEndOfBodyPosition());
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int)(consumerID ^ (consumerID >>> 32));
      result = prime * result + deliveryCount;
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionReceiveMessage))
         return false;
      SessionReceiveMessage other = (SessionReceiveMessage)obj;
      if (consumerID != other.consumerID)
         return false;
      if (deliveryCount != other.deliveryCount)
         return false;
      return true;
   }

}
