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
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 *
 */
public class CreateSessionResponseMessage extends PacketImpl
{
   private int serverVersion;

   public CreateSessionResponseMessage(final int serverVersion)
   {
      super(CREATESESSION_RESP);

      this.serverVersion = serverVersion;
   }

   public CreateSessionResponseMessage()
   {
      super(CREATESESSION_RESP);
   }

   @Override
   public boolean isResponse()
   {
      return true;
   }

   public int getServerVersion()
   {
      return serverVersion;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(serverVersion);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      serverVersion = buffer.readInt();
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
      result = prime * result + serverVersion;
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof CreateSessionResponseMessage))
         return false;
      CreateSessionResponseMessage other = (CreateSessionResponseMessage)obj;
      if (serverVersion != other.serverVersion)
         return false;
      return true;
   }
}
