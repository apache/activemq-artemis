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
public class SessionConsumerCloseMessage extends PacketImpl
{

   private long consumerID;

   public SessionConsumerCloseMessage(final long objectID)
   {
      super(SESS_CONSUMER_CLOSE);

      consumerID = objectID;
   }

   public SessionConsumerCloseMessage()
   {
      super(SESS_CONSUMER_CLOSE);
   }

   // Public --------------------------------------------------------

   public long getConsumerID()
   {
      return consumerID;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(consumerID);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      consumerID = buffer.readLong();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", consumerID=" + consumerID + "]";
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int)(consumerID ^ (consumerID >>> 32));
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionConsumerCloseMessage))
         return false;
      SessionConsumerCloseMessage other = (SessionConsumerCloseMessage)obj;
      if (consumerID != other.consumerID)
         return false;
      return true;
   }
}
