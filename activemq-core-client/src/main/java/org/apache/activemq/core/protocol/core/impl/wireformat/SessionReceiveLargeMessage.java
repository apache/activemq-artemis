/**
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
package org.apache.activemq.core.protocol.core.impl.wireformat;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.core.message.impl.MessageInternal;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;

/**
 * A SessionReceiveLargeMessage
 *
 * @author Clebert Suconic
 *
 *
 */
public class SessionReceiveLargeMessage extends PacketImpl
{
   private final MessageInternal message;

   /** Since we receive the message before the entire message was received, */
   private long largeMessageSize;

   private long consumerID;

   private int deliveryCount;

   // To be used on decoding at the client while receiving a large message
   public SessionReceiveLargeMessage(final MessageInternal message)
   {
      super(SESS_RECEIVE_LARGE_MSG);
      this.message = message;
   }

   public SessionReceiveLargeMessage(final long consumerID,
                                     final MessageInternal message,
                                     final long largeMessageSize,
                                     final int deliveryCount)
   {
      super(SESS_RECEIVE_LARGE_MSG);

      this.consumerID = consumerID;

      this.message = message;

      this.deliveryCount = deliveryCount;

      this.largeMessageSize = largeMessageSize;
   }

   public MessageInternal getLargeMessage()
   {
      return message;
   }

   public long getConsumerID()
   {
      return consumerID;
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }

   /**
    * @return the largeMessageSize
    */
   public long getLargeMessageSize()
   {
      return largeMessageSize;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer)
   {
      buffer.writeLong(consumerID);
      buffer.writeInt(deliveryCount);
      buffer.writeLong(largeMessageSize);
      message.encodeHeadersAndProperties(buffer);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer)
   {
      consumerID = buffer.readLong();
      deliveryCount = buffer.readInt();
      largeMessageSize = buffer.readLong();
      message.decodeHeadersAndProperties(buffer);
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int)(consumerID ^ (consumerID >>> 32));
      result = prime * result + deliveryCount;
      result = prime * result + (int)(largeMessageSize ^ (largeMessageSize >>> 32));
      result = prime * result + ((message == null) ? 0 : message.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionReceiveLargeMessage))
         return false;
      SessionReceiveLargeMessage other = (SessionReceiveLargeMessage)obj;
      if (consumerID != other.consumerID)
         return false;
      if (deliveryCount != other.deliveryCount)
         return false;
      if (largeMessageSize != other.largeMessageSize)
         return false;
      if (message == null)
      {
         if (other.message != null)
            return false;
      }
      else if (!message.equals(other.message))
         return false;
      return true;
   }

}
