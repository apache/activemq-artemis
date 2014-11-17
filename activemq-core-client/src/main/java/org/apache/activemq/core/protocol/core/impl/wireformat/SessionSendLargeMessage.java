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
import org.apache.activemq6.core.message.impl.MessageInternal;
import org.apache.activemq6.core.protocol.core.impl.PacketImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert Suconic</a>
 *
 */
public class SessionSendLargeMessage extends PacketImpl
{


   /** Used only if largeMessage */
   private final MessageInternal largeMessage;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendLargeMessage(final MessageInternal largeMessage)
   {
      super(SESS_SEND_LARGE);

      this.largeMessage = largeMessage;
   }

   // Public --------------------------------------------------------

   public MessageInternal getLargeMessage()
   {
      return largeMessage;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      largeMessage.encodeHeadersAndProperties(buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      largeMessage.decodeHeadersAndProperties(buffer);
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((largeMessage == null) ? 0 : largeMessage.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionSendLargeMessage))
         return false;
      SessionSendLargeMessage other = (SessionSendLargeMessage)obj;
      if (largeMessage == null)
      {
         if (other.largeMessage != null)
            return false;
      }
      else if (!largeMessage.equals(other.largeMessage))
         return false;
      return true;
   }

}
