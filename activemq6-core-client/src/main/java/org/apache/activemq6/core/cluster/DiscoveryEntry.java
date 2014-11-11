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
package org.apache.activemq6.core.cluster;

import org.apache.activemq6.api.core.TransportConfiguration;

/**
 * A DiscoveryEntry
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class DiscoveryEntry
{
   private final String nodeID;
   private final TransportConfiguration connector;
   private final long lastUpdate;


   public DiscoveryEntry(final String nodeID, final TransportConfiguration connector, final long lastUpdate)
   {
      this.nodeID = nodeID;
      this.connector = connector;
      this.lastUpdate = lastUpdate;
   }

   public String getNodeID()
   {
      return nodeID;
   }

   public TransportConfiguration getConnector()
   {
      return connector;
   }

   public long getLastUpdate()
   {
      return lastUpdate;
   }

   @Override
   public String toString()
   {
      return "DiscoveryEntry[nodeID=" + nodeID + ", connector=" + connector + ", lastUpdate=" + lastUpdate + "]";
   }
}
