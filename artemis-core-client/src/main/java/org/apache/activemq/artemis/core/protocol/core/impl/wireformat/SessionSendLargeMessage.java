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
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;

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
   public void encodeRest(final ActiveMQBuffer buffer)
   {
      largeMessage.encodeHeadersAndProperties(buffer);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer)
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
