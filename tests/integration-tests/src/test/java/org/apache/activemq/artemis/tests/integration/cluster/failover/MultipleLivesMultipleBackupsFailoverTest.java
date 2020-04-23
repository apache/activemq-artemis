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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.tests.integration.cluster.util.SameProcessActiveMQServer;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.junit.Test;

/**
 */
public class MultipleLivesMultipleBackupsFailoverTest extends MultipleBackupsFailoverTestBase {

   protected Map<Integer, TestableServer> servers = new HashMap<>();
   private ServerLocator locator2;
   private ServerLocator locator;
   private final boolean sharedStore = true;

   @Test
   public void testMultipleFailovers2LiveServers() throws Exception {
      NodeManager nodeManager1 = new InVMNodeManager(!sharedStore);
      NodeManager nodeManager2 = new InVMNodeManager(!sharedStore);
      createLiveConfig(nodeManager1, 0, 3, 4, 5);
      createBackupConfig(nodeManager1, 0, 1, true, new int[]{0, 2}, 3, 4, 5);
      createBackupConfig(nodeManager1, 0, 2, true, new int[]{0, 1}, 3, 4, 5);
      createLiveConfig(nodeManager2, 3, 0);
      createBackupConfig(nodeManager2, 3, 4, true, new int[]{3, 5}, 0, 1, 2);
      createBackupConfig(nodeManager2, 3, 5, true, new int[]{3, 4}, 0, 1, 2);

      servers.get(0).start();
      waitForServerToStart(servers.get(0).getServer());

      servers.get(3).start();
      waitForServerToStart(servers.get(3).getServer());

      servers.get(1).start();
      waitForServerToStart(servers.get(1).getServer());

      servers.get(2).start();

      servers.get(4).start();
      waitForServerToStart(servers.get(4).getServer());

      servers.get(5).start();

      waitForServerToStart(servers.get(4).getServer());

      locator = getServerLocator(0).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setReconnectAttempts(15);

      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 4, servers.get(0).getServer());
      ClientSession session = sendAndConsume(sf, true);

      instanceLog.debug(((ServerLocatorInternal) locator).getTopology().describe());
      Thread.sleep(500);
      servers.get(0).crash(session);

      int liveAfter0 = waitForNewLive(10000, true, servers, 1, 2);

      locator2 = getServerLocator(3).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setReconnectAttempts(15);

      ClientSessionFactoryInternal sf2 = createSessionFactoryAndWaitForTopology(locator2, 4);
      ClientSession session2 = sendAndConsume(sf2, true);

      System.setProperty("foo", "bar");
      servers.get(3).crash(session2);
      int liveAfter3 = waitForNewLive(10000, true, servers, 4, 5);
      locator.close();
      locator2.close();
      if (liveAfter0 == 2) {
         Thread.sleep(500);
         servers.get(1).stop();
         Thread.sleep(500);
         servers.get(2).stop();
      } else {
         Thread.sleep(500);
         servers.get(2).stop();
         Thread.sleep(500);
         servers.get(1).stop();
      }

      if (liveAfter3 == 4) {
         Thread.sleep(500);
         servers.get(5).stop();
         Thread.sleep(500);
         servers.get(4).stop();
      } else {
         Thread.sleep(500);
         servers.get(4).stop();
         Thread.sleep(500);
         servers.get(5).stop();
      }
   }

   protected void createBackupConfig(NodeManager nodeManager,
                                     int liveNode,
                                     int nodeid,
                                     boolean createClusterConnections,
                                     int[] otherBackupNodes,
                                     int... otherClusterNodes) throws Exception {
      Configuration config1 = super.createDefaultInVMConfig().clearAcceptorConfigurations().addAcceptorConfiguration(createTransportConfiguration(isNetty(), true, generateParams(nodeid, isNetty()))).setHAPolicyConfiguration(sharedStore ? new SharedStoreSlavePolicyConfiguration() : new ReplicaPolicyConfiguration()).setBindingsDirectory(getBindingsDir() + "_" + liveNode).setJournalDirectory(getJournalDir() + "_" + liveNode).setPagingDirectory(getPageDir() + "_" + liveNode).setLargeMessagesDirectory(getLargeMessagesDir() + "_" + liveNode);

      for (int node : otherBackupNodes) {
         TransportConfiguration liveConnector = createTransportConfiguration(isNetty(), false, generateParams(node, isNetty()));
         config1.addConnectorConfiguration(liveConnector.getName(), liveConnector);
      }

      TransportConfiguration backupConnector = createTransportConfiguration(isNetty(), false, generateParams(nodeid, isNetty()));
      config1.addConnectorConfiguration(backupConnector.getName(), backupConnector);

      String[] clusterNodes = new String[otherClusterNodes.length];
      for (int i = 0; i < otherClusterNodes.length; i++) {
         TransportConfiguration connector = createTransportConfiguration(isNetty(), false, generateParams(otherClusterNodes[i], isNetty()));
         config1.addConnectorConfiguration(connector.getName(), connector);
         clusterNodes[i] = connector.getName();
      }
      config1.addClusterConfiguration(basicClusterConnectionConfig(backupConnector.getName(), clusterNodes));

      servers.put(nodeid, new SameProcessActiveMQServer(createInVMFailoverServer(true, config1, nodeManager, liveNode)));
   }

   protected void createLiveConfig(NodeManager nodeManager, int liveNode, int... otherLiveNodes) throws Exception {
      TransportConfiguration liveConnector = createTransportConfiguration(isNetty(), false, generateParams(liveNode, isNetty()));

      Configuration config0 = super.createDefaultInVMConfig().clearAcceptorConfigurations().addAcceptorConfiguration(createTransportConfiguration(isNetty(), true, generateParams(liveNode, isNetty()))).setHAPolicyConfiguration(sharedStore ? new SharedStoreMasterPolicyConfiguration() : new ReplicatedPolicyConfiguration()).setBindingsDirectory(getBindingsDir() + "_" + liveNode).setJournalDirectory(getJournalDir() + "_" + liveNode).setPagingDirectory(getPageDir() + "_" + liveNode).setLargeMessagesDirectory(getLargeMessagesDir() + "_" + liveNode).addConnectorConfiguration(liveConnector.getName(), liveConnector);

      String[] pairs = new String[otherLiveNodes.length];
      for (int i = 0; i < otherLiveNodes.length; i++) {
         TransportConfiguration otherLiveConnector = createTransportConfiguration(isNetty(), false, generateParams(otherLiveNodes[i], isNetty()));
         config0.addConnectorConfiguration(otherLiveConnector.getName(), otherLiveConnector);
         pairs[i] = otherLiveConnector.getName();
      }
      config0.addClusterConfiguration(basicClusterConnectionConfig(liveConnector.getName(), pairs));

      servers.put(liveNode, new SameProcessActiveMQServer(createInVMFailoverServer(true, config0, nodeManager, liveNode)));
   }

   @Override
   protected boolean isNetty() {
      return false;
   }
}
