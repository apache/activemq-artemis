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
package org.apache.activemq6.core.protocol.core.impl.wireformat;

import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.core.protocol.core.impl.PacketImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class SessionAcknowledgeMessage extends PacketImpl
{
   private long consumerID;

   private long messageID;

   private boolean requiresResponse;

   public SessionAcknowledgeMessage(final long consumerID, final long messageID, final boolean requiresResponse)
   {
      super(SESS_ACKNOWLEDGE);

      this.consumerID = consumerID;

      this.messageID = messageID;

      this.requiresResponse = requiresResponse;
   }

   public SessionAcknowledgeMessage()
   {
      super(SESS_ACKNOWLEDGE);
   }

   // Public --------------------------------------------------------

   public long getConsumerID()
   {
      return consumerID;
   }

   public long getMessageID()
   {
      return messageID;
   }

   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(consumerID);

      buffer.writeLong(messageID);

      buffer.writeBoolean(requiresResponse);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      consumerID = buffer.readLong();

      messageID = buffer.readLong();

      requiresResponse = buffer.readBoolean();
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int)(consumerID ^ (consumerID >>> 32));
      result = prime * result + (int)(messageID ^ (messageID >>> 32));
      result = prime * result + (requiresResponse ? 1231 : 1237);
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionAcknowledgeMessage))
         return false;
      SessionAcknowledgeMessage other = (SessionAcknowledgeMessage)obj;
      if (consumerID != other.consumerID)
         return false;
      if (messageID != other.messageID)
         return false;
      if (requiresResponse != other.requiresResponse)
         return false;
      return true;
   }
}
