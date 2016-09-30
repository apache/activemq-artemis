/*
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

import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.utils.XidCodecSupport;

public class SessionXAStartMessage extends PacketImpl {
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Xid xid;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAStartMessage(final Xid xid) {
      super(SESS_XA_START);

      this.xid = xid;
   }

   public SessionXAStartMessage() {
      super(SESS_XA_START);
   }

   // Public --------------------------------------------------------

   public Xid getXid() {
      return xid;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      XidCodecSupport.encodeXid(xid, buffer);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      xid = XidCodecSupport.decodeXid(buffer);
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((xid == null) ? 0 : xid.hashCode());
      return result;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", xid=" + xid);
      buff.append("]");
      return buff.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionXAStartMessage))
         return false;
      SessionXAStartMessage other = (SessionXAStartMessage) obj;
      if (xid == null) {
         if (other.xid != null)
            return false;
      } else if (!xid.equals(other.xid))
         return false;
      return true;
   }

}
