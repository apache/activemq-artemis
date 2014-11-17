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
package org.apache.activemq6.tests.integration.cluster.util;
import org.apache.activemq6.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq6.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq6.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq6.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq6.api.core.Pair;
import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.client.HornetQClient;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.config.ClusterConnectionConfiguration;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.server.HornetQServers;
import org.apache.activemq6.core.server.NodeManager;
import org.apache.activemq6.core.server.impl.InVMNodeManager;
import org.apache.activemq6.tests.util.ServiceTestBase;

/**
 * @author Clebert Suconic
 */

public class MultiServerTestBase extends ServiceTestBase
{


   protected HornetQServer[] servers;

   protected HornetQServer[] backupServers;

   protected NodeManager[] nodeManagers;

   protected int getNumberOfServers()
   {
      return 5;
   }

   protected boolean useBackups()
   {
      return true;
   }

   protected boolean useRealFiles()
   {
      return true;
   }

   protected boolean useNetty()
   {
      return false;
   }

   protected boolean useSharedStorage()
   {
      return true;
   }


   protected final ServerLocator createLocator(final boolean ha, final int serverID)
   {
      TransportConfiguration targetConfig = createTransportConfiguration(useNetty(), false, generateParams(serverID, useNetty()));
      if (ha)
      {
         ServerLocator locator = HornetQClient.createServerLocatorWithHA(targetConfig);
         return addServerLocator(locator);
      }
      else
      {
         ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(targetConfig);
         return addServerLocator(locator);
      }
   }


   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      servers = new HornetQServer[getNumberOfServers()];

      if (useBackups())
      {
         backupServers = new HornetQServer[getNumberOfServers()];
      }

      if (useBackups())
      {
         nodeManagers = new NodeManager[getNumberOfServers()];
      }


      for (int i = 0; i < getNumberOfServers(); i++)
      {
         Pair<HornetQServer, NodeManager> nodeServer = setupLiveServer(i, useRealFiles(), useSharedStorage());
         this.servers[i] = nodeServer.getA();

         if (useBackups())
         {
            this.nodeManagers[i] = nodeServer.getB();

            // The backup id here will be only useful if doing replication
            // this base class doesn't currently support replication, but I will set things here for later anyways
            this.backupServers[i] = setupBackupServer(i + getNumberOfServers(), i, nodeServer.getB());
         }
      }
   }

   protected void startServers() throws Exception
   {
      for (HornetQServer server : servers)
      {
         server.start();
      }

      for (HornetQServer server: servers)
      {
         waitForServer(server);
      }

      if (backupServers != null)
      {
         for (HornetQServer server : backupServers)
         {
            server.start();
         }

         for (HornetQServer server: backupServers)
         {
            waitForServer(server);
         }

      }

      for (HornetQServer server : servers)
      {
         waitForTopology(server, getNumberOfServers(), useBackups() ? getNumberOfServers() : 0);
      }
   }

   public void startServers(int ... serverID) throws Exception
   {
      for (int s : serverID)
      {
         servers[s].start();
         waitForServer(servers[s]);
      }
   }

   public void startBackups(int ... serverID) throws Exception
   {
      for (int s : serverID)
      {
         backupServers[s].start();
         waitForServer(backupServers[s]);
      }

   }

   protected Pair<HornetQServer, NodeManager> setupLiveServer(final int node,
                                                              final boolean realFiles,
                                                              final boolean sharedStorage) throws Exception
   {
      NodeManager nodeManager = null;
      TransportConfiguration serverConfigAcceptor = createTransportConfiguration(useNetty(), true, generateParams(node, useNetty()));
      TransportConfiguration thisConnector = createTransportConfiguration(useNetty(), false, generateParams(node, useNetty()));

      if (sharedStorage)
      {
         nodeManager = new InVMNodeManager(false);
      }

      Configuration configuration = createBasicConfig(node)
         .setJournalMaxIO_AIO(1000)
         .setThreadPoolMaxSize(10)
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(serverConfigAcceptor)
         .addConnectorConfiguration("thisConnector", thisConnector)
         .setHAPolicyConfiguration(sharedStorage ? new SharedStoreMasterPolicyConfiguration() : new ReplicatedPolicyConfiguration());

      List<String> targetServersOnConnection = new ArrayList<String>();

      for (int targetNode = 0; targetNode < getNumberOfServers(); targetNode++)
      {
         if (targetNode == node)
         {
            continue;
         }
         String targetConnectorName = "target-" + targetNode;
         TransportConfiguration targetServer = createTransportConfiguration(useNetty(), false, generateParams(targetNode, useNetty()));
         configuration.getConnectorConfigurations().put(targetConnectorName, targetServer);
         targetServersOnConnection.add(targetConnectorName);

         // The connector towards a backup.. just to have a reference so bridges can connect to backups on their configs
         String backupConnectorName = "backup-" + targetNode;

         TransportConfiguration backupConnector = createTransportConfiguration(useNetty(), false, generateParams(targetNode + getNumberOfServers(), useNetty()));

         configuration.getConnectorConfigurations().put(backupConnectorName, backupConnector);
      }

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration()
         .setName("localCluster" + node)
         .setAddress("cluster-queues")
         .setConnectorName("thisConnector")
         .setRetryInterval(100)
         .setConfirmationWindowSize(1024)
         .setStaticConnectors(targetServersOnConnection);

      configuration.getClusterConfigurations().add(clusterConf);

      HornetQServer server;

      if (sharedStorage)
      {
         server = createInVMFailoverServer(realFiles, configuration, nodeManager, node);
      }
      else
      {
         server = createServer(realFiles, configuration);
      }

      server.setIdentity(this.getClass().getSimpleName() + "/Live(" + node + ")");


      addServer(server);

      return new Pair<HornetQServer, NodeManager>(server, nodeManager);
   }

   protected HornetQServer setupBackupServer(final int node,
                                             final int liveNode,
                                             final NodeManager nodeManager) throws Exception
   {
      TransportConfiguration serverConfigAcceptor = createTransportConfiguration(useNetty(), true, generateParams(node, useNetty()));
      TransportConfiguration thisConnector = createTransportConfiguration(useNetty(), false, generateParams(node, useNetty()));

      Configuration configuration = createBasicConfig(useSharedStorage() ? liveNode : node)
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(serverConfigAcceptor)
         .addConnectorConfiguration("thisConnector", thisConnector)
         .setHAPolicyConfiguration(useSharedStorage() ? new SharedStoreSlavePolicyConfiguration() : new ReplicaPolicyConfiguration());

      List<String> targetServersOnConnection = new ArrayList<String>();

      for (int targetNode = 0; targetNode < getNumberOfServers(); targetNode++)
      {
//         if (targetNode == node)
//         {
//            // moving on from itself
//            continue;
//         }
         String targetConnectorName = "targetConnector-" + targetNode;
         TransportConfiguration targetServer = createTransportConfiguration(useNetty(), false, generateParams(targetNode, useNetty()));
         configuration.addConnectorConfiguration(targetConnectorName, targetServer);
         targetServersOnConnection.add(targetConnectorName);
      }

      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration()
         .setName("localCluster" + node)
         .setAddress("cluster-queues")
         .setConnectorName("thisConnector")
         .setRetryInterval(100)
         .setConfirmationWindowSize(1024)
         .setStaticConnectors(targetServersOnConnection);

      configuration.getClusterConfigurations().add(clusterConf);

      HornetQServer server;

      if (useSharedStorage())
      {
         server = createInVMFailoverServer(true, configuration, nodeManager, liveNode);
      }
      else
      {
         server = addServer(HornetQServers.newHornetQServer(configuration, useRealFiles()));
      }
      server.setIdentity(this.getClass().getSimpleName() + "/Backup(" + node + " of live " + liveNode + ")");
      return server;
   }


}
