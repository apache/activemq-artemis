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
package org.hornetq.tests.integration.cluster.failover;


import java.util.HashMap;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMemberImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.After;
import org.junit.Test;

public class ColocatedFailoverCheckPairingsTest extends ServiceTestBase
{
   TestableServer liveServer1;

   protected TestableServer liveServer2;

   protected Configuration liveConfiguration1;

   protected Configuration liveConfiguration2;

   protected NodeManager nodeManagerLive1;

   protected NodeManager nodeManagerLive2;

   @Override
   public void setUp() throws Exception
   {
      super.setUp();
   }

   @Test
   public void testPairings() throws Exception
   {
      createConfigs();

      liveServer1.start();
      liveServer2.start();
      waitForServer(liveServer1.getServer());
      waitForServer(liveServer2.getServer());

      ServerLocator locator = HornetQClient.createServerLocator(true, getConnectorTransportConfiguration(1));

      waitForTopology(liveServer1.getServer(), 2, 2);
      waitForTopology(liveServer2.getServer(), 2, 2);

      ClientSessionFactory sessionFactory = locator.createSessionFactory();

      Topology topology = sessionFactory.getServerLocator().getTopology();
      System.out.println(topology.describe());
      TopologyMemberImpl m1 = topology.getMember(liveServer1.getServer().getNodeID().toString());
      TopologyMemberImpl m2 = topology.getMember(liveServer2.getServer().getNodeID().toString());

      assertPairedCorrectly(m1, m2);
   }

   protected void assertPairedCorrectly(TopologyMemberImpl m1, TopologyMemberImpl m2)
   {
      assertEquals(m1.getLive(), m2.getBackup());
      assertEquals(m2.getLive(), m1.getBackup());
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      liveServer1.stop();
      liveServer2.stop();
      super.tearDown();
   }

   protected void createConfigs() throws Exception
   {
      nodeManagerLive1 = new InVMNodeManager(false);
      nodeManagerLive2 = new InVMNodeManager(false);

      TransportConfiguration liveConnector1 = getConnectorTransportConfiguration(1);
      liveConfiguration1 = super.createDefaultConfig();
      liveConfiguration1.getAcceptorConfigurations().clear();
      liveConfiguration1.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(1));
      liveConfiguration1.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);
      liveConfiguration1.getHAPolicy().setFailbackDelay(1000);
      liveConfiguration1.setJournalDirectory(getTestDir() + "/live1");
      liveConfiguration1.getQueueConfigurations().add(new CoreQueueConfiguration("jms.queue.testQueue", "jms.queue.testQueue", null, true));

      TransportConfiguration liveConnector2 = getConnectorTransportConfiguration(2);
      basicClusterConnectionConfig(liveConfiguration1, liveConnector1.getName(), liveConnector2.getName());
      liveConfiguration1.getConnectorConfigurations().put(liveConnector1.getName(), liveConnector1);
      liveConfiguration1.getConnectorConfigurations().put(liveConnector2.getName(), liveConnector2);

      Configuration backupConfiguration1 = liveConfiguration1.copy();
      backupConfiguration1.setJournalDirectory(getTestDir() + "/live2");
      backupConfiguration1.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_SHARED_STORE);
      liveConfiguration1.getBackupServerConfigurations().add(backupConfiguration1);

      liveServer1 = createTestableServer(liveConfiguration1, nodeManagerLive1, nodeManagerLive2, 1);

      liveConfiguration2 = super.createDefaultConfig();
      liveConfiguration2.getAcceptorConfigurations().clear();
      liveConfiguration2.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(2));
      liveConfiguration2.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);
      liveConfiguration2.getHAPolicy().setFailbackDelay(1000);
      liveConfiguration2.setJournalDirectory(getTestDir() + "/live2");
      liveConfiguration2.getQueueConfigurations().add(new CoreQueueConfiguration("jms.queue.testQueue", "jms.queue.testQueue", null, true));

      basicClusterConnectionConfig(liveConfiguration2, liveConnector2.getName(), liveConnector1.getName());
      liveConfiguration2.getConnectorConfigurations().put(liveConnector1.getName(), liveConnector1);
      liveConfiguration2.getConnectorConfigurations().put(liveConnector2.getName(), liveConnector2);

      Configuration backupConfiguration2 = liveConfiguration2.copy();
      backupConfiguration2.setJournalDirectory(getTestDir() + "/live1");
      backupConfiguration2.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_SHARED_STORE);
      liveConfiguration2.getBackupServerConfigurations().add(backupConfiguration2);

      liveServer2 = createTestableServer(liveConfiguration2, nodeManagerLive2, nodeManagerLive1, 2);
   }

   protected TransportConfiguration getAcceptorTransportConfiguration(int node)
   {
      HashMap<String, Object> params = new HashMap<>();
      params.put("server-id", "" + node);
      return new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params);
   }

   protected TransportConfiguration getConnectorTransportConfiguration(int node)
   {
      HashMap<String, Object> params = new HashMap<>();
      params.put("server-id", "" + node);
      return new TransportConfiguration(INVM_CONNECTOR_FACTORY, params);
   }

   protected TestableServer createTestableServer(Configuration config, NodeManager liveNodeManager, NodeManager backupNodeManager, int id)
   {
      return new SameProcessHornetQServer(
         createColocatedInVMFailoverServer(true, config, liveNodeManager, backupNodeManager, id));
   }
}
