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

package org.apache.activemq.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.api.config.HornetQDefaultConfiguration;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.HornetQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.client.impl.Topology;
import org.apache.activemq.core.client.impl.TopologyMemberImpl;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.CoreQueueConfiguration;
import org.apache.activemq.core.config.ScaleDownConfiguration;
import org.apache.activemq.core.config.ha.ColocatedPolicyConfiguration;
import org.apache.activemq.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.impl.HornetQServerImpl;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class AutomaticColocatedQuorumVoteTest extends ServiceTestBase
{
   private final boolean replicated;

   @Parameterized.Parameters(name = "replicated={0}")
   public static Collection getParameters()
   {
      return Arrays.asList(new Object[][]
      {
         {true},
         {false}
      });
   }

   public AutomaticColocatedQuorumVoteTest(boolean replicated)
   {
      this.replicated = replicated;
   }
   @Test
   public void testSimpleDistributionBackupStrategyFull() throws Exception
   {
      HornetQServer server0 = createServer(0, 1, false);
      HornetQServer server1 = createServer(1, 0, false);
      TransportConfiguration liveConnector0 = getConnectorTransportConfiguration("liveConnector" + 0, 0);
      TransportConfiguration liveConnector1 = getConnectorTransportConfiguration("liveConnector" + 1, 1);

      try
      (
         ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(liveConnector0)
      )
      {
         server0.start();
         server1.start();
         ClientSessionFactory sessionFactory0 = serverLocator.createSessionFactory(liveConnector0);
         waitForRemoteBackup(sessionFactory0, 10);
         ClientSessionFactory sessionFactory1 = serverLocator.createSessionFactory(liveConnector1);
         waitForRemoteBackup(sessionFactory1, 10);
         Topology topology = serverLocator.getTopology();
         Collection<TopologyMemberImpl> members = topology.getMembers();
         assertEquals(members.size(), 2);
         Map<String,HornetQServer> backupServers0 = server0.getClusterManager().getHAManager().getBackupServers();
         assertEquals(backupServers0.size(), 1);
         Map<String,HornetQServer> backupServers1 = server1.getClusterManager().getHAManager().getBackupServers();
         assertEquals(backupServers1.size(), 1);
         HornetQServer backupServer0 = backupServers0.values().iterator().next();
         HornetQServer backupServer1 = backupServers1.values().iterator().next();
         waitForRemoteBackupSynchronization(backupServer0);
         waitForRemoteBackupSynchronization(backupServer1);
         assertEquals(server0.getNodeID(), backupServer1.getNodeID());
         assertEquals(server1.getNodeID(), backupServer0.getNodeID());
         Set<TransportConfiguration> backupAcceptors0 = backupServer0.getConfiguration().getAcceptorConfigurations();
         assertEquals(1, backupAcceptors0.size());
         assertEquals("5545", backupAcceptors0.iterator().next().getParams().get("port"));
         Set<TransportConfiguration> backupAcceptors1 = backupServer1.getConfiguration().getAcceptorConfigurations();
         assertEquals(1, backupAcceptors1.size());
         assertEquals("5546", backupAcceptors1.iterator().next().getParams().get("port"));
         Map<String, TransportConfiguration> connectorConfigurations0 = backupServer0.getConfiguration().getConnectorConfigurations();
         assertEquals(2, connectorConfigurations0.size());
         assertEquals("5545", connectorConfigurations0.get("liveConnector0").getParams().get("port"));
         assertEquals("5446", connectorConfigurations0.get("remoteConnector0").getParams().get("port"));
         Map<String, TransportConfiguration> connectorConfigurations1 = backupServer1.getConfiguration().getConnectorConfigurations();
         assertEquals(2, connectorConfigurations1.size());
         assertEquals("5546", connectorConfigurations1.get("liveConnector1").getParams().get("port"));
         assertEquals("5445", connectorConfigurations1.get("remoteConnector1").getParams().get("port"));
         if (!replicated)
         {
            assertEquals(server0.getConfiguration().getJournalDirectory(), backupServer1.getConfiguration().getJournalDirectory());
            assertEquals(server0.getConfiguration().getBindingsDirectory(), backupServer1.getConfiguration().getBindingsDirectory());
            assertEquals(server0.getConfiguration().getLargeMessagesDirectory(), backupServer1.getConfiguration().getLargeMessagesDirectory());
            assertEquals(server0.getConfiguration().getPagingDirectory(), backupServer1.getConfiguration().getPagingDirectory());
            assertEquals(server1.getConfiguration().getJournalDirectory(), backupServer0.getConfiguration().getJournalDirectory());
            assertEquals(server1.getConfiguration().getBindingsDirectory(), backupServer0.getConfiguration().getBindingsDirectory());
            assertEquals(server1.getConfiguration().getLargeMessagesDirectory(), backupServer0.getConfiguration().getLargeMessagesDirectory());
            assertEquals(server1.getConfiguration().getPagingDirectory(), backupServer0.getConfiguration().getPagingDirectory());
         }
         else
         {
            assertNotEquals(server0.getConfiguration().getJournalDirectory(), backupServer1.getConfiguration().getJournalDirectory());
            assertNotEquals(server0.getConfiguration().getBindingsDirectory(), backupServer1.getConfiguration().getBindingsDirectory());
            assertNotEquals(server0.getConfiguration().getLargeMessagesDirectory(), backupServer1.getConfiguration().getLargeMessagesDirectory());
            assertNotEquals(server0.getConfiguration().getPagingDirectory(), backupServer1.getConfiguration().getPagingDirectory());
            assertNotEquals(server1.getConfiguration().getJournalDirectory(), backupServer0.getConfiguration().getJournalDirectory());
            assertNotEquals(server1.getConfiguration().getBindingsDirectory(), backupServer0.getConfiguration().getBindingsDirectory());
            assertNotEquals(server1.getConfiguration().getLargeMessagesDirectory(), backupServer0.getConfiguration().getLargeMessagesDirectory());
            assertNotEquals(server1.getConfiguration().getPagingDirectory(), backupServer0.getConfiguration().getPagingDirectory());
         }
      }
      finally
      {
         try
         {
            server0.stop();
         }
         catch (Throwable e)
         {
            e.printStackTrace();
         }
         server1.stop();
      }
   }

   @Test
   public void testSimpleDistributionBackupStrategyScaleDown() throws Exception
   {
      HornetQServer server0 = createServer(0, 1, true);
      HornetQServer server1 = createServer(1, 0, true);
      TransportConfiguration liveConnector0 = getConnectorTransportConfiguration("liveConnector" + 0, 0);
      TransportConfiguration liveConnector1 = getConnectorTransportConfiguration("liveConnector" + 1, 1);

      try
      (
            ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(liveConnector0)
      )
      {
         server0.start();
         server1.start();
         ClientSessionFactory sessionFactory0 = serverLocator.createSessionFactory(liveConnector0);
         waitForRemoteBackup(sessionFactory0, 10);
         ClientSessionFactory sessionFactory1 = serverLocator.createSessionFactory(liveConnector1);
         waitForRemoteBackup(sessionFactory1, 10);
         Topology topology = serverLocator.getTopology();
         Collection<TopologyMemberImpl> members = topology.getMembers();
         assertEquals(members.size(), 2);
         Map<String,HornetQServer> backupServers0 = server0.getClusterManager().getHAManager().getBackupServers();
         assertEquals(backupServers0.size(), 1);
         Map<String,HornetQServer> backupServers1 = server1.getClusterManager().getHAManager().getBackupServers();
         assertEquals(backupServers1.size(), 1);
         HornetQServer backupServer0 = backupServers0.values().iterator().next();
         HornetQServer backupServer1 = backupServers1.values().iterator().next();
         waitForRemoteBackupSynchronization(backupServer0);
         waitForRemoteBackupSynchronization(backupServer1);
         assertEquals(server0.getNodeID(), backupServer1.getNodeID());
         assertEquals(server1.getNodeID(), backupServer0.getNodeID());
         Set<TransportConfiguration> backupAcceptors0 = backupServer0.getConfiguration().getAcceptorConfigurations();
         assertEquals(0, backupAcceptors0.size());
         Set<TransportConfiguration> backupAcceptors1 = backupServer1.getConfiguration().getAcceptorConfigurations();
         assertEquals(0, backupAcceptors1.size());
         Map<String, TransportConfiguration> connectorConfigurations0 = backupServer0.getConfiguration().getConnectorConfigurations();
         assertEquals(2, connectorConfigurations0.size());
         assertEquals("5445", connectorConfigurations0.get("liveConnector0").getParams().get("port"));
         assertEquals("5446", connectorConfigurations0.get("remoteConnector0").getParams().get("port"));
         Map<String, TransportConfiguration> connectorConfigurations1 = backupServer1.getConfiguration().getConnectorConfigurations();
         assertEquals(2, connectorConfigurations1.size());
         assertEquals("5446", connectorConfigurations1.get("liveConnector1").getParams().get("port"));
         assertEquals("5445", connectorConfigurations1.get("remoteConnector1").getParams().get("port"));
         if (!replicated)
         {
            assertEquals(server0.getConfiguration().getJournalDirectory(), backupServer1.getConfiguration().getJournalDirectory());
            assertEquals(server0.getConfiguration().getBindingsDirectory(), backupServer1.getConfiguration().getBindingsDirectory());
            assertEquals(server0.getConfiguration().getLargeMessagesDirectory(), backupServer1.getConfiguration().getLargeMessagesDirectory());
            assertEquals(server0.getConfiguration().getPagingDirectory(), backupServer1.getConfiguration().getPagingDirectory());
            assertEquals(server1.getConfiguration().getJournalDirectory(), backupServer0.getConfiguration().getJournalDirectory());
            assertEquals(server1.getConfiguration().getBindingsDirectory(), backupServer0.getConfiguration().getBindingsDirectory());
            assertEquals(server1.getConfiguration().getLargeMessagesDirectory(), backupServer0.getConfiguration().getLargeMessagesDirectory());
            assertEquals(server1.getConfiguration().getPagingDirectory(), backupServer0.getConfiguration().getPagingDirectory());
         }
         else
         {
            assertNotEquals(server0.getConfiguration().getJournalDirectory(), backupServer1.getConfiguration().getJournalDirectory());
            assertNotEquals(server0.getConfiguration().getBindingsDirectory(), backupServer1.getConfiguration().getBindingsDirectory());
            assertNotEquals(server0.getConfiguration().getLargeMessagesDirectory(), backupServer1.getConfiguration().getLargeMessagesDirectory());
            assertNotEquals(server0.getConfiguration().getPagingDirectory(), backupServer1.getConfiguration().getPagingDirectory());
            assertNotEquals(server1.getConfiguration().getJournalDirectory(), backupServer0.getConfiguration().getJournalDirectory());
            assertNotEquals(server1.getConfiguration().getBindingsDirectory(), backupServer0.getConfiguration().getBindingsDirectory());
            assertNotEquals(server1.getConfiguration().getLargeMessagesDirectory(), backupServer0.getConfiguration().getLargeMessagesDirectory());
            assertNotEquals(server1.getConfiguration().getPagingDirectory(), backupServer0.getConfiguration().getPagingDirectory());
         }
      }
      finally
      {
         try
         {
            server0.stop();
         }
         catch (Throwable e)
         {
            e.printStackTrace();
         }
         server1.stop();
      }
   }

   @Test
   public void testSimpleDistributionOfBackupsMaxBackupsExceeded() throws Exception
   {
      HornetQServer server0 = createServer(0, 1, false);
      HornetQServer server1 = createServer(1, 0, false);
      HornetQServer server2 = createServer(2, 0, false);
      HornetQServer server3 = createServer(3, 0, false);
      TransportConfiguration liveConnector0 = getConnectorTransportConfiguration("liveConnector" + 0, 0);
      TransportConfiguration liveConnector1 = getConnectorTransportConfiguration("liveConnector" + 1, 1);
      TransportConfiguration liveConnector2 = getConnectorTransportConfiguration("liveConnector" + 2, 2);
      TransportConfiguration liveConnector3 = getConnectorTransportConfiguration("liveConnector" + 3, 3);


      try
      (
         ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(liveConnector0)
      )
      {
         server0.start();
         server1.start();
         ClientSessionFactory sessionFactory0 = serverLocator.createSessionFactory(liveConnector0);
         waitForRemoteBackup(sessionFactory0, 10);
         ClientSessionFactory sessionFactory1 = serverLocator.createSessionFactory(liveConnector1);
         waitForRemoteBackup(sessionFactory1, 10);
         Topology topology = serverLocator.getTopology();
         Collection<TopologyMemberImpl> members = topology.getMembers();
         assertEquals(members.size(), 2);
         Map<String,HornetQServer> backupServers0 = server0.getClusterManager().getHAManager().getBackupServers();
         assertEquals(backupServers0.size(), 1);
         Map<String,HornetQServer> backupServers1 = server1.getClusterManager().getHAManager().getBackupServers();
         assertEquals(backupServers1.size(), 1);
         HornetQServer backupServer0 = backupServers0.values().iterator().next();
         HornetQServer backupServer1 = backupServers1.values().iterator().next();
         waitForRemoteBackupSynchronization(backupServer0);
         waitForRemoteBackupSynchronization(backupServer1);
         assertEquals(server0.getNodeID(), backupServer1.getNodeID());
         assertEquals(server1.getNodeID(), backupServer0.getNodeID());
         server2.start();
         //just give server2 time to try both server 0 and 1
         ClientSessionFactory sessionFactory2 = serverLocator.createSessionFactory(liveConnector2);
         server3.start();
         ClientSessionFactory sessionFactory3 = serverLocator.createSessionFactory(liveConnector3);
         waitForRemoteBackup(sessionFactory2, 10);
         waitForRemoteBackup(sessionFactory3, 10);
         assertEquals(members.size(), 2);
         Map<String,HornetQServer> backupServers2 = server2.getClusterManager().getHAManager().getBackupServers();
         assertEquals(backupServers2.size(), 1);
         Map<String,HornetQServer> backupServers3 = server3.getClusterManager().getHAManager().getBackupServers();
         assertEquals(backupServers3.size(), 1);
         HornetQServer backupServer2 = backupServers2.values().iterator().next();
         HornetQServer backupServer3 = backupServers3.values().iterator().next();
         waitForRemoteBackupSynchronization(backupServer2);
         waitForRemoteBackupSynchronization(backupServer3);
         assertEquals(server0.getNodeID(), backupServer1.getNodeID());
         assertEquals(server1.getNodeID(), backupServer0.getNodeID());
         assertEquals(server2.getNodeID(), backupServer3.getNodeID());
         assertEquals(server3.getNodeID(), backupServer2.getNodeID());
      }
      finally
      {
         server0.stop();
         server1.stop();
         server2.stop();
         server3.stop();
      }
   }

   private HornetQServer createServer(int node, int remoteNode, boolean scaleDown) throws Exception
   {
      TransportConfiguration liveConnector = getConnectorTransportConfiguration("liveConnector" + node, node);
      TransportConfiguration remoteConnector = getConnectorTransportConfiguration("remoteConnector" + node, remoteNode);
      TransportConfiguration liveAcceptor = getAcceptorTransportConfiguration(node);
      Configuration liveConfiguration = getConfiguration("server" + node, scaleDown, liveConnector, liveAcceptor, remoteConnector);
      HornetQServer server = new HornetQServerImpl(liveConfiguration);
      server.setIdentity("server" + node);
      return server;
   }

   private Configuration getConfiguration(String identity, boolean scaleDown, TransportConfiguration liveConnector, TransportConfiguration liveAcceptor, TransportConfiguration... otherLiveNodes) throws Exception
   {
      Configuration configuration = createDefaultConfig()
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(liveAcceptor)
         .addConnectorConfiguration(liveConnector.getName(), liveConnector)
         .setJournalDirectory(HornetQDefaultConfiguration.getDefaultJournalDir() + identity)
         .setBindingsDirectory(HornetQDefaultConfiguration.getDefaultBindingsDirectory() + identity)
         .setLargeMessagesDirectory(HornetQDefaultConfiguration.getDefaultLargeMessagesDir() + identity)
         .setPagingDirectory(HornetQDefaultConfiguration.getDefaultPagingDir() + identity)
         .addQueueConfiguration(new CoreQueueConfiguration()
                                   .setAddress("jms.queue.testQueue")
                                   .setName("jms.queue.testQueue"));

      List<String> transportConfigurationList = new ArrayList<>();

      final ColocatedPolicyConfiguration haPolicy = new ColocatedPolicyConfiguration();
      for (TransportConfiguration otherLiveNode : otherLiveNodes)
      {
         configuration.addConnectorConfiguration(otherLiveNode.getName(), otherLiveNode);
         transportConfigurationList.add(otherLiveNode.getName());
         haPolicy.getExcludedConnectors().add(otherLiveNode.getName());
      }
      configuration.addClusterConfiguration(basicClusterConnectionConfig(liveConnector.getName(), transportConfigurationList));
      haPolicy.setBackupPortOffset(100);
      haPolicy.setBackupRequestRetries(-1);
      haPolicy.setBackupRequestRetryInterval(500);
      haPolicy.setMaxBackups(1);
      haPolicy.setRequestBackup(true);
      configuration.setHAPolicyConfiguration(haPolicy);
      if (!replicated)
      {
         SharedStoreMasterPolicyConfiguration ssmc = new SharedStoreMasterPolicyConfiguration();
         SharedStoreSlavePolicyConfiguration sssc = new SharedStoreSlavePolicyConfiguration();
         haPolicy.setLiveConfig(ssmc);
         haPolicy.setBackupConfig(sssc);
         if (scaleDown)
         {
            sssc.setScaleDownConfiguration(new ScaleDownConfiguration());
         }
      }
      else
      {
         ReplicatedPolicyConfiguration rpc = new ReplicatedPolicyConfiguration();
         ReplicaPolicyConfiguration rpc2 = new ReplicaPolicyConfiguration();
         haPolicy.setLiveConfig(rpc);
         haPolicy.setBackupConfig(rpc2);
         if (scaleDown)
         {
            rpc2.setScaleDownConfiguration(new ScaleDownConfiguration());
         }
      }

      return configuration;
   }

   private TransportConfiguration getAcceptorTransportConfiguration(int node)
   {
      HashMap<String, Object> params = new HashMap<>();
      params.put("port", "" + (5445 + node));
      return new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);
   }

   private TransportConfiguration getConnectorTransportConfiguration(String name, int node)
   {
      HashMap<String, Object> params = new HashMap<>();
      params.put("port", "" + (5445 + node));
      return new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params, name);
   }
}
