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
import org.apache.activemq.core.protocol.core.impl.PacketImpl;

/**
 *
 * A ReattachSessionMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ReattachSessionMessage extends PacketImpl
{
   private String name;

   private int lastConfirmedCommandID;

   public ReattachSessionMessage(final String name, final int lastConfirmedCommandID)
   {
      super(REATTACH_SESSION);

      this.name = name;

      this.lastConfirmedCommandID = lastConfirmedCommandID;
   }

   public ReattachSessionMessage()
   {
      super(REATTACH_SESSION);
   }

   public String getName()
   {
      return name;
   }

   public int getLastConfirmedCommandID()
   {
      return lastConfirmedCommandID;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeString(name);
      buffer.writeInt(lastConfirmedCommandID);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      name = buffer.readString();
      lastConfirmedCommandID = buffer.readInt();
   }

   @Override
   public final boolean isRequiresConfirmations()
   {
      return false;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + lastConfirmedCommandID;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof ReattachSessionMessage))
         return false;
      ReattachSessionMessage other = (ReattachSessionMessage)obj;
      if (lastConfirmedCommandID != other.lastConfirmedCommandID)
         return false;
      if (name == null)
      {
         if (other.name != null)
            return false;
      }
      else if (!name.equals(other.name))
         return false;
      return true;
   }
}
