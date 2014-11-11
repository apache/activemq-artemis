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
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 *
 */
public class SessionConsumerFlowCreditMessage extends PacketImpl
{
   private long consumerID;
   private int credits;

   public SessionConsumerFlowCreditMessage(final long consumerID, final int credits)
   {
      super(SESS_FLOWTOKEN);
      this.consumerID = consumerID;
      this.credits = credits;
   }

   public SessionConsumerFlowCreditMessage()
   {
      super(SESS_FLOWTOKEN);
   }

   // Public --------------------------------------------------------

   public long getConsumerID()
   {
      return consumerID;
   }

   public int getCredits()
   {
      return credits;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(consumerID);
      buffer.writeInt(credits);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      consumerID = buffer.readLong();
      credits = buffer.readInt();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", consumerID=" + consumerID + ", credits=" + credits + "]";
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int)(consumerID ^ (consumerID >>> 32));
      result = prime * result + credits;
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionConsumerFlowCreditMessage))
         return false;
      SessionConsumerFlowCreditMessage other = (SessionConsumerFlowCreditMessage)obj;
      if (consumerID != other.consumerID)
         return false;
      if (credits != other.credits)
         return false;
      return true;
   }
}
