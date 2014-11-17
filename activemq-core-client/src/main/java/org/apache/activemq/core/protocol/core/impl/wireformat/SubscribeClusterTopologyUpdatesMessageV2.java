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

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SubscribeClusterTopologyUpdatesMessageV2 extends SubscribeClusterTopologyUpdatesMessage
{

   private int clientVersion;

   public SubscribeClusterTopologyUpdatesMessageV2(final boolean clusterConnection, int clientVersion)
   {
      super(SUBSCRIBE_TOPOLOGY_V2, clusterConnection);

      this.clientVersion = clientVersion;
   }

   public SubscribeClusterTopologyUpdatesMessageV2()
   {
      super(SUBSCRIBE_TOPOLOGY_V2);
   }

   // Public --------------------------------------------------------



   @Override
   public void encodeRest(final ActiveMQBuffer buffer)
   {
      super.encodeRest(buffer);
      buffer.writeInt(clientVersion);
   }

   /**
    * @return the clientVersion
    */
   public int getClientVersion()
   {
      return clientVersion;
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer)
   {
      super.decodeRest(buffer);
      clientVersion = buffer.readInt();
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + clientVersion;
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SubscribeClusterTopologyUpdatesMessageV2))
         return false;
      SubscribeClusterTopologyUpdatesMessageV2 other = (SubscribeClusterTopologyUpdatesMessageV2)obj;
      if (clientVersion != other.clientVersion)
         return false;
      return true;
   }
}
