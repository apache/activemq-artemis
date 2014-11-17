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
 */
public class SessionXAEndMessage extends PacketImpl
{
   private Xid xid;

   private boolean failed;

   public SessionXAEndMessage(final Xid xid, final boolean failed)
   {
      super(SESS_XA_END);

      this.xid = xid;

      this.failed = failed;
   }

   public SessionXAEndMessage()
   {
      super(SESS_XA_END);
   }

   public boolean isFailed()
   {
      return failed;
   }

   public Xid getXid()
   {
      return xid;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      XidCodecSupport.encodeXid(xid, buffer);
      buffer.writeBoolean(failed);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      xid = XidCodecSupport.decodeXid(buffer);
      failed = buffer.readBoolean();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", xid=" + xid + ", failed=" + failed + "]";
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (failed ? 1231 : 1237);
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
      if (!(obj instanceof SessionXAEndMessage))
         return false;
      SessionXAEndMessage other = (SessionXAEndMessage)obj;
      if (failed != other.failed)
         return false;
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
