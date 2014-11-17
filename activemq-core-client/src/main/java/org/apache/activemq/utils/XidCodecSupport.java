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
package org.apache.activemq.utils;

import javax.transaction.xa.Xid;

import org.apache.activemq.api.core.HornetQBuffer;
import org.apache.activemq.core.transaction.impl.XidImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class XidCodecSupport
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static void encodeXid(final Xid xid, final HornetQBuffer out)
   {
      out.writeInt(xid.getFormatId());
      out.writeInt(xid.getBranchQualifier().length);
      out.writeBytes(xid.getBranchQualifier());
      out.writeInt(xid.getGlobalTransactionId().length);
      out.writeBytes(xid.getGlobalTransactionId());
   }

   public static Xid decodeXid(final HornetQBuffer in)
   {
      int formatID = in.readInt();
      byte[] bq = new byte[in.readInt()];
      in.readBytes(bq);
      byte[] gtxid = new byte[in.readInt()];
      in.readBytes(gtxid);
      Xid xid = new XidImpl(bq, formatID, gtxid);
      return xid;
   }

   public static int getXidEncodeLength(final Xid xid)
   {
      return DataConstants.SIZE_INT * 3 + xid.getBranchQualifier().length + xid.getGlobalTransactionId().length;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
