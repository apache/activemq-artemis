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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.core.server.impl.SharedNothingBackupActivation;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.integration.cluster.util.SameProcessActiveMQServer;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.ReplicatedBackupUtils;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReplicaTimeoutTest extends ActiveMQTestBase {

   protected ServerLocator locator;

   protected static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   @Before
   public void setup() {
      locator = addServerLocator(ActiveMQClient.createServerLocatorWithHA(getConnectorTransportConfiguration(true), getConnectorTransportConfiguration(false))).setRetryInterval(50);
   }

   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

   protected NodeManager createReplicatedBackupNodeManager(Configuration backupConfig) {
      return new InVMNodeManager(true, backupConfig.getJournalLocation());
   }

   protected TestableServer createTestableServer(Configuration config, NodeManager nodeManager) throws Exception {
      boolean isBackup = config.getHAPolicyConfiguration() instanceof ReplicaPolicyConfiguration || config.getHAPolicyConfiguration() instanceof SharedStoreSlavePolicyConfiguration;
      return new SameProcessActiveMQServer(createInVMFailoverServer(true, config, nodeManager, isBackup ? 2 : 1));
   }

   protected ClientSessionFactoryInternal createSessionFactoryAndWaitForTopology(ServerLocator locator,
                                                                                 int topologyMembers) throws Exception {
      CountDownLatch countDownLatch = new CountDownLatch(topologyMembers);

      locator.addClusterTopologyListener(new FailoverTestBase.LatchClusterTopologyListener(countDownLatch));

      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();
      addSessionFactory(sf);

      Assert.assertTrue("topology members expected " + topologyMembers, countDownLatch.await(5, TimeUnit.SECONDS));
      return sf;
   }

   protected ClientSessionFactoryInternal createSessionFactory() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setReconnectAttempts(300).setRetryInterval(100);

      return createSessionFactoryAndWaitForTopology(locator, 2);
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks));
   }

   protected void crash(TestableServer liveServer,
                        TestableServer backupServer,
                        ClientSession... sessions) throws Exception {
      if (sessions.length > 0) {
         for (ClientSession session : sessions) {
            waitForRemoteBackup(session.getSessionFactory(), 5, true, backupServer.getServer());
         }
      } else {
         waitForRemoteBackup(null, 5, true, backupServer.getServer());
      }
      liveServer.crash(true, true, sessions);
   }

   @Test//(timeout = 120000)
   public void testFailbackTimeout() throws Exception {
      AssertionLoggerHandler.startCapture();
      try {
         TestableServer backupServer = null;
         TestableServer liveServer = null;
         ClientSessionFactory sf = null;
         try {
            final TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
            final TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
            final TransportConfiguration backupAcceptor = getAcceptorTransportConfiguration(false);

            Configuration backupConfig = createDefaultInVMConfig();
            Configuration liveConfig = createDefaultInVMConfig();

            ReplicatedBackupUtils.configureReplicationPair(backupConfig, backupConnector, backupAcceptor, liveConfig, liveConnector, null);
            ((ReplicatedPolicyConfiguration) liveConfig.getHAPolicyConfiguration()).setInitialReplicationSyncTimeout(1000);
            ((ReplicaPolicyConfiguration) backupConfig.getHAPolicyConfiguration()).setInitialReplicationSyncTimeout(1000);

            backupConfig.setBindingsDirectory(getBindingsDir(0, true)).setJournalDirectory(getJournalDir(0, true)).
               setPagingDirectory(getPageDir(0, true)).setLargeMessagesDirectory(getLargeMessagesDir(0, true)).setSecurityEnabled(false);
            liveConfig.setBindingsDirectory(getBindingsDir(0, false)).setJournalDirectory(getJournalDir(0, false)).
               setPagingDirectory(getPageDir(0, false)).setLargeMessagesDirectory(getLargeMessagesDir(0, false)).setSecurityEnabled(false);

            ((ReplicatedPolicyConfiguration) liveConfig.getHAPolicyConfiguration()).setCheckForLiveServer(true);
            ((ReplicaPolicyConfiguration) backupConfig.getHAPolicyConfiguration()).setMaxSavedReplicatedJournalsSize(2).setAllowFailBack(true);
            ((ReplicaPolicyConfiguration) backupConfig.getHAPolicyConfiguration()).setRestartBackup(false);

            NodeManager nodeManager = createReplicatedBackupNodeManager(backupConfig);

            backupServer = createTestableServer(backupConfig, nodeManager);

            liveConfig.clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(true));

            liveServer = createTestableServer(liveConfig, nodeManager);

            AtomicBoolean ignoreIntercept = new AtomicBoolean(false);

            final TestableServer theBackup = backupServer;

            liveServer.start();
            backupServer.start();

            Wait.assertTrue(backupServer.getServer()::isReplicaSync);

            sf = createSessionFactory();

            ClientSession session = createSession(sf, true, true);

            session.createQueue(new QueueConfiguration(ADDRESS));

            crash(liveServer, backupServer, session);

            Wait.assertTrue(backupServer.getServer()::isActive);

            ignoreIntercept.set(true);

            ((ActiveMQServerImpl) backupServer.getServer()).setAfterActivationCreated(new Runnable() {
               @Override
               public void run() {
                  //theBackup.getServer().getActivation()

                  SharedNothingBackupActivation activation = (SharedNothingBackupActivation) theBackup.getServer().getActivation();
                  activation.getReplicationEndpoint().addOutgoingInterceptorForReplication(new Interceptor() {
                     @Override
                     public boolean intercept(Packet packet, RemotingConnection connection) throws ActiveMQException {
                        if (ignoreIntercept.get() && packet.getType() == PacketImpl.REPLICATION_RESPONSE_V2) {
                           return false;
                        }
                        return true;
                     }
                  });
               }
            });

            liveServer.start();

            Assert.assertTrue(Wait.waitFor(() -> AssertionLoggerHandler.findText("AMQ229114")));

            Wait.assertFalse(liveServer.getServer()::isStarted);

         } finally {
            if (sf != null) {
               sf.close();
            }
            try {
               liveServer.getServer().stop();
            } catch (Throwable ignored) {
            }
            try {
               backupServer.getServer().stop();
            } catch (Throwable ignored) {
            }
         }
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

}
