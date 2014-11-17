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

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.apache.activemq.api.core.HornetQBuffer;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.utils.XidCodecSupport;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class SessionXAGetInDoubtXidsResponseMessage extends PacketImpl
{
   private List<Xid> xids;

   public SessionXAGetInDoubtXidsResponseMessage(final List<Xid> xids)
   {
      super(SESS_XA_INDOUBT_XIDS_RESP);

      this.xids = xids;
   }

   public SessionXAGetInDoubtXidsResponseMessage()
   {
      super(SESS_XA_INDOUBT_XIDS_RESP);
   }

   @Override
   public boolean isResponse()
   {
      return true;
   }

   public List<Xid> getXids()
   {
      return xids;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(xids.size());

      for (Xid xid : xids)
      {
         XidCodecSupport.encodeXid(xid, buffer);
      }
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      int len = buffer.readInt();
      xids = new ArrayList<Xid>(len);
      for (int i = 0; i < len; i++)
      {
         Xid xid = XidCodecSupport.decodeXid(buffer);

         xids.add(xid);
      }
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((xids == null) ? 0 : xids.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionXAGetInDoubtXidsResponseMessage))
         return false;
      SessionXAGetInDoubtXidsResponseMessage other = (SessionXAGetInDoubtXidsResponseMessage)obj;
      if (xids == null)
      {
         if (other.xids != null)
            return false;
      }
      else if (!xids.equals(other.xids))
         return false;
      return true;
   }
}
