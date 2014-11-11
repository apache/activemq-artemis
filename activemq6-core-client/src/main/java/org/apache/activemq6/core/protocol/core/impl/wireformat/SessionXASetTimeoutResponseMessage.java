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
 *
 */
public class SessionXASetTimeoutResponseMessage extends PacketImpl
{
   private boolean ok;

   public SessionXASetTimeoutResponseMessage(final boolean ok)
   {
      super(SESS_XA_SET_TIMEOUT_RESP);

      this.ok = ok;
   }

   public SessionXASetTimeoutResponseMessage()
   {
      super(SESS_XA_SET_TIMEOUT_RESP);
   }

   // Public --------------------------------------------------------

   @Override
   public boolean isResponse()
   {
      return true;
   }

   public boolean isOK()
   {
      return ok;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeBoolean(ok);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      ok = buffer.readBoolean();
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (ok ? 1231 : 1237);
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionXASetTimeoutResponseMessage))
         return false;
      SessionXASetTimeoutResponseMessage other = (SessionXASetTimeoutResponseMessage)obj;
      if (ok != other.ok)
         return false;
      return true;
   }
}
