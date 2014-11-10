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
package org.hornetq.tests.integration.cluster.topology;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class HAClientTopologyWithDiscoveryTest extends TopologyClusterTestBase
{
   protected final String groupAddress = getUDPDiscoveryAddress();

   protected final int groupPort = getUDPDiscoveryPort();

   @Override
   protected boolean isNetty()
   {
      return false;
   }

   @Override
   protected void setupCluster() throws Exception
   {
      setupCluster(false);
   }

   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      setupDiscoveryClusterConnection("cluster0", 0, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      setupDiscoveryClusterConnection("cluster1", 1, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      setupDiscoveryClusterConnection("cluster2", 2, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      setupDiscoveryClusterConnection("cluster3", 3, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      setupDiscoveryClusterConnection("cluster4", 4, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
   }

   @Override
   protected void setupServers() throws Exception
   {
      setupLiveServerWithDiscovery(0, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupLiveServerWithDiscovery(1, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupLiveServerWithDiscovery(2, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupLiveServerWithDiscovery(3, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupLiveServerWithDiscovery(4, groupAddress, groupPort, isFileStorage(), isNetty(), false);
   }

   @Override
   protected ServerLocator createHAServerLocator()
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithHA(new DiscoveryGroupConfiguration()
         .setBroadcastEndpointFactoryConfiguration(new UDPBroadcastGroupConfiguration()
            .setGroupAddress(groupAddress)
            .setGroupPort(groupPort)));
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      addServerLocator(locator);
      return locator;
   }
}
