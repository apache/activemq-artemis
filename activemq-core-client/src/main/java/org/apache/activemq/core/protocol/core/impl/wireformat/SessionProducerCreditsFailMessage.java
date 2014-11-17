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
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;

/**
 * @author Justin Bertram
 *
 */
public class SessionProducerCreditsFailMessage extends PacketImpl
{
   private int credits;

   private SimpleString address;

   public SessionProducerCreditsFailMessage(final int credits, final SimpleString address)
   {
      super(SESS_PRODUCER_FAIL_CREDITS);

      this.credits = credits;

      this.address = address;
   }

   public SessionProducerCreditsFailMessage()
   {
      super(SESS_PRODUCER_FAIL_CREDITS);
   }

   public int getCredits()
   {
      return credits;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(credits);
      buffer.writeSimpleString(address);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      credits = buffer.readInt();
      address = buffer.readSimpleString();
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((address == null) ? 0 : address.hashCode());
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
      if (!(obj instanceof SessionProducerCreditsFailMessage))
         return false;
      SessionProducerCreditsFailMessage other = (SessionProducerCreditsFailMessage)obj;
      if (address == null)
      {
         if (other.address != null)
            return false;
      }
      else if (!address.equals(other.address))
         return false;
      if (credits != other.credits)
         return false;
      return true;
   }
}
