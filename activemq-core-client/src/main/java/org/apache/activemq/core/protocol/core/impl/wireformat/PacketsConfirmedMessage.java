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

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;

/**
 *
 * A PacketsConfirmedMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class PacketsConfirmedMessage extends PacketImpl
{

   private int commandID;

   public PacketsConfirmedMessage(final int commandID)
   {
      super(PACKETS_CONFIRMED);

      this.commandID = commandID;
   }

   public PacketsConfirmedMessage()
   {
      super(PACKETS_CONFIRMED);
   }

   // Public --------------------------------------------------------

   public int getCommandID()
   {
      return commandID;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer)
   {
      buffer.writeInt(commandID);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer)
   {
      commandID = buffer.readInt();
   }


   @Override
   public final boolean isRequiresConfirmations()
   {
      return false;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", commandID=" + commandID + "]";
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + commandID;
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
      {
         return true;
      }
      if (!super.equals(obj))
      {
         return false;
      }
      if (!(obj instanceof PacketsConfirmedMessage))
      {
         return false;
      }
      PacketsConfirmedMessage other = (PacketsConfirmedMessage)obj;
      if (commandID != other.commandID)
      {
         return false;
      }
      return true;
   }
}
