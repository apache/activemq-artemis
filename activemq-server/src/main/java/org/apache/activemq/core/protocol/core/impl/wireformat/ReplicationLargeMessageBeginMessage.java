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
 * A ReplicationLargeMessageBeingMessage
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationLargeMessageBeginMessage extends PacketImpl
{

   long messageId;

   public ReplicationLargeMessageBeginMessage(final long messageId)
   {
      this();
      this.messageId = messageId;
   }

   public ReplicationLargeMessageBeginMessage()
   {
      super(PacketImpl.REPLICATION_LARGE_MESSAGE_BEGIN);
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(messageId);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      messageId = buffer.readLong();
   }

   /**
    * @return the messageId
    */
   public long getMessageId()
   {
      return messageId;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int)(messageId ^ (messageId >>> 32));
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (getClass() != obj.getClass())
         return false;
      ReplicationLargeMessageBeginMessage other = (ReplicationLargeMessageBeginMessage)obj;
      if (messageId != other.messageId)
         return false;
      return true;
   }
}
