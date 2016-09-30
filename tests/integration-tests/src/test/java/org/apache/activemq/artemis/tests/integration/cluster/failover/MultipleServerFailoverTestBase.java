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

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.integration.cluster.util.SameProcessActiveMQServer;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.junit.Before;

public abstract class MultipleServerFailoverTestBase extends ActiveMQTestBase {
   // Constants -----------------------------------------------------

   protected static final SimpleString ADDRESS = new SimpleString("jms.queues.FailoverTestAddress");

   // Attributes ----------------------------------------------------

   protected List<TestableServer> liveServers = new ArrayList<>();

   protected List<TestableServer> backupServers = new ArrayList<>();

   protected List<Configuration> backupConfigs = new ArrayList<>();

   protected List<Configuration> liveConfigs = new ArrayList<>();

   protected List<NodeManager> nodeManagers;

   public abstract int getLiveServerCount();

   public abstract int getBackupServerCount();

   public abstract boolean isNetty();

   public abstract boolean isSharedStore();

   public abstract String getNodeGroupName();

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      liveServers = new ArrayList<>();
      backupServers = new ArrayList<>();
      backupConfigs = new ArrayList<>();
      liveConfigs = new ArrayList<>();

      for (int i = 0; i < getLiveServerCount(); i++) {
         HAPolicyConfiguration haPolicyConfiguration = null;

         if (isSharedStore()) {
            haPolicyConfiguration = new SharedStoreMasterPolicyConfiguration();
         } else {
            haPolicyConfiguration = new ReplicatedPolicyConfiguration();
            if (getNodeGroupName() != null) {
               ((ReplicatedPolicyConfiguration) haPolicyConfiguration).setGroupName(getNodeGroupName() + "-" + i);
            }
         }

         Configuration configuration = createDefaultConfig(isNetty()).clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(true, i)).setHAPolicyConfiguration(haPolicyConfiguration);

         if (!isSharedStore()) {
            configuration.setBindingsDirectory(getBindingsDir(i, false));
            configuration.setJournalDirectory(getJournalDir(i, false));
            configuration.setPagingDirectory(getPageDir(i, false));
            configuration.setLargeMessagesDirectory(getLargeMessagesDir(i, false));
         } else {
            //todo
         }

         TransportConfiguration livetc = getConnectorTransportConfiguration(true, i);
         configuration.addConnectorConfiguration(livetc.getName(), livetc);
         List<String> connectors = new ArrayList<>();
         for (int j = 0; j < getLiveServerCount(); j++) {
            if (j != i) {
               TransportConfiguration staticTc = getConnectorTransportConfiguration(true, j);
               configuration.getConnectorConfigurations().put(staticTc.getName(), staticTc);
               connectors.add(staticTc.getName());
            }
         }

         String[] input = new String[connectors.size()];
         connectors.toArray(input);
         configuration.addClusterConfiguration(basicClusterConnectionConfig(livetc.getName(), input));
         liveConfigs.add(configuration);
         ActiveMQServer server = createServer(true, configuration);
         TestableServer activeMQServer = new SameProcessActiveMQServer(server);
         activeMQServer.setIdentity("Live-" + i);
         liveServers.add(activeMQServer);
      }
      for (int i = 0; i < getBackupServerCount(); i++) {
         HAPolicyConfiguration haPolicyConfiguration = null;

         if (isSharedStore()) {
            haPolicyConfiguration = new SharedStoreSlavePolicyConfiguration();
         } else {
            haPolicyConfiguration = new ReplicaPolicyConfiguration();
            if (getNodeGroupName() != null) {
               ((ReplicaPolicyConfiguration) haPolicyConfiguration).setGroupName(getNodeGroupName() + "-" + i);
            }
         }

         Configuration configuration = createDefaultConfig(isNetty()).clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(false, i)).setHAPolicyConfiguration(haPolicyConfiguration);

         if (!isSharedStore()) {
            configuration.setBindingsDirectory(getBindingsDir(i, true));
            configuration.setJournalDirectory(getJournalDir(i, true));
            configuration.setPagingDirectory(getPageDir(i, true));
            configuration.setLargeMessagesDirectory(getLargeMessagesDir(i, true));
         } else {
            //todo
         }

         TransportConfiguration backuptc = getConnectorTransportConfiguration(false, i);
         configuration.addConnectorConfiguration(backuptc.getName(), backuptc);
         List<String> connectors = new ArrayList<>();
         for (int j = 0; j < getBackupServerCount(); j++) {
            TransportConfiguration staticTc = getConnectorTransportConfiguration(true, j);
            configuration.addConnectorConfiguration(staticTc.getName(), staticTc);
            connectors.add(staticTc.getName());
         }
         for (int j = 0; j < getBackupServerCount(); j++) {
            if (j != i) {
               TransportConfiguration staticTc = getConnectorTransportConfiguration(false, j);
               configuration.getConnectorConfigurations().put(staticTc.getName(), staticTc);
               connectors.add(staticTc.getName());
            }
         }
         String[] input = new String[connectors.size()];
         connectors.toArray(input);
         configuration.addClusterConfiguration(basicClusterConnectionConfig(backuptc.getName(), input));
         backupConfigs.add(configuration);
         ActiveMQServer server = createServer(true, configuration);
         TestableServer testableServer = new SameProcessActiveMQServer(server);
         testableServer.setIdentity("Backup-" + i);
         backupServers.add(testableServer);
      }
   }

   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live, int node) {
      TransportConfiguration transportConfiguration;
      if (isNetty()) {
         transportConfiguration = TransportConfigurationUtils.getNettyAcceptor(live, node, (live ? "live-" : "backup-") + node);
      } else {
         transportConfiguration = TransportConfigurationUtils.getInVMAcceptor(live, node, (live ? "live-" : "backup-") + node);
      }
      return transportConfiguration;
   }

   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live, int node) {
      TransportConfiguration transportConfiguration;
      if (isNetty()) {
         transportConfiguration = TransportConfigurationUtils.getNettyConnector(live, node, (live ? "live-" : "backup-") + node);
      } else {
         transportConfiguration = TransportConfigurationUtils.getInVMConnector(live, node, (live ? "live-" : "backup-") + node);
      }
      return transportConfiguration;
   }

   protected ServerLocatorInternal getServerLocator(int node) throws Exception {
      return (ServerLocatorInternal) addServerLocator(ActiveMQClient.createServerLocatorWithHA(getConnectorTransportConfiguration(true, node))).setRetryInterval(50).setReconnectAttempts(-1).setInitialConnectAttempts(-1);
   }

   protected ServerLocatorInternal getBackupServerLocator(int node) throws Exception {
      return (ServerLocatorInternal) addServerLocator(ActiveMQClient.createServerLocatorWithHA(getConnectorTransportConfiguration(false, node))).setRetryInterval(50).setReconnectAttempts(-1).setInitialConnectAttempts(-1);
   }

   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         int ackBatchSize) throws Exception {
      return addClientSession(sf.createSession(autoCommitSends, autoCommitAcks, ackBatchSize));
   }

   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf.createSession(autoCommitSends, autoCommitAcks));
   }

   protected ClientSession createSession(ClientSessionFactory sf) throws Exception {
      return addClientSession(sf.createSession());
   }

   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf.createSession(xa, autoCommitSends, autoCommitAcks));
   }

   protected void waitForDistribution(SimpleString address, ActiveMQServer server, int messageCount) throws Exception {
      ActiveMQServerLogger.LOGGER.debug("waiting for distribution of messages on server " + server);

      long start = System.currentTimeMillis();

      long timeout = 5000;

      Queue q = (Queue) server.getPostOffice().getBinding(address).getBindable();

      do {

         if (getMessageCount(q) >= messageCount) {
            return;
         }

         Thread.sleep(10);
      } while (System.currentTimeMillis() - start < timeout);

      throw new Exception();
   }
}
