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

import javax.transaction.xa.Xid;

import org.apache.activemq.api.core.HornetQBuffer;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.utils.XidCodecSupport;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAPrepareMessage extends PacketImpl
{

   private Xid xid;

   public SessionXAPrepareMessage(final Xid xid)
   {
      super(SESS_XA_PREPARE);

      this.xid = xid;
   }

   public SessionXAPrepareMessage()
   {
      super(SESS_XA_PREPARE);
   }


   public Xid getXid()
   {
      return xid;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      XidCodecSupport.encodeXid(xid, buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      xid = XidCodecSupport.decodeXid(buffer);
   }

   @Override
   public boolean isAsyncExec()
   {
      return true;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((xid == null) ? 0 : xid.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionXAPrepareMessage))
         return false;
      SessionXAPrepareMessage other = (SessionXAPrepareMessage)obj;
      if (xid == null)
      {
         if (other.xid != null)
            return false;
      }
      else if (!xid.equals(other.xid))
         return false;
      return true;
   }
}
