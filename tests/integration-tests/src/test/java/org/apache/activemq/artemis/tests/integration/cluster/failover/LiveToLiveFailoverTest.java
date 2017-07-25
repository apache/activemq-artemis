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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.config.ScaleDownConfiguration;
import org.apache.activemq.artemis.core.config.ha.ColocatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

public class LiveToLiveFailoverTest extends FailoverTest {

   private InVMNodeManager nodeManager0;
   private InVMNodeManager nodeManager1;
   private ClientSessionFactoryInternal sf2;

   @Override
   public void setUp() throws Exception {
      super.setUp();
   }

   @Override
   protected void createConfigs() throws Exception {
      nodeManager0 = new InVMNodeManager(false);
      nodeManager1 = new InVMNodeManager(false);
      TransportConfiguration liveConnector0 = getConnectorTransportConfiguration(true, 0);
      TransportConfiguration liveConnector1 = getConnectorTransportConfiguration(true, 1);

      backupConfig = super.createDefaultInVMConfig(1).clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(true, 1)).setHAPolicyConfiguration(new ColocatedPolicyConfiguration().setRequestBackup(true).setLiveConfig(new SharedStoreMasterPolicyConfiguration()).setBackupConfig(new SharedStoreSlavePolicyConfiguration().setScaleDownConfiguration(new ScaleDownConfiguration().addConnector(liveConnector1.getName())))).addConnectorConfiguration(liveConnector0.getName(), liveConnector0).addConnectorConfiguration(liveConnector1.getName(), liveConnector1).addClusterConfiguration(basicClusterConnectionConfig(liveConnector1.getName(), liveConnector0.getName()));

      backupServer = createColocatedTestableServer(backupConfig, nodeManager1, nodeManager0, 1);

      liveConfig = super.createDefaultInVMConfig(0).clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(true, 0)).setHAPolicyConfiguration(new ColocatedPolicyConfiguration().setRequestBackup(true).setBackupRequestRetryInterval(1000).setLiveConfig(new SharedStoreMasterPolicyConfiguration()).setBackupConfig(new SharedStoreSlavePolicyConfiguration().setScaleDownConfiguration(new ScaleDownConfiguration()))).addConnectorConfiguration(liveConnector0.getName(), liveConnector0).addConnectorConfiguration(liveConnector1.getName(), liveConnector1).addClusterConfiguration(basicClusterConnectionConfig(liveConnector0.getName(), liveConnector1.getName()));

      liveServer = createColocatedTestableServer(liveConfig, nodeManager0, nodeManager1, 0);
   }

   @Override
   protected void setLiveIdentity() {
      liveServer.setIdentity(this.getClass().getSimpleName() + "/liveServer0");
   }

   @Override
   protected void setBackupIdentity() {
      backupServer.setIdentity(this.getClass().getSimpleName() + "/liveServer1");
   }

   @Override
   protected void waitForBackup() {
      Map<String, ActiveMQServer> backupServers0 = liveServer.getServer().getClusterManager().getHAManager().getBackupServers();
      Map<String, ActiveMQServer> backupServers1 = backupServer.getServer().getClusterManager().getHAManager().getBackupServers();
      final long toWait = 10000;
      final long time = System.currentTimeMillis();
      while (true) {
         if (backupServers0.size() == 1 && backupServers1.size() == 1) {
            break;
         }
         if (System.currentTimeMillis() > (time + toWait)) {
            fail("backup started? ( live server0 backups = " + backupServers0.size() + " live server1 backups = " + backupServers1.size() + ")");
         }
         try {
            Thread.sleep(100);
         } catch (InterruptedException e) {
            fail(e.getMessage());
         }
      }
      waitForRemoteBackupSynchronization(backupServers0.values().iterator().next());
      waitForRemoteBackupSynchronization(backupServers1.values().iterator().next());
   }

   @Override
   protected final ClientSessionFactoryInternal createSessionFactoryAndWaitForTopology(ServerLocator locator,
                                                                                       int topologyMembers) throws Exception {
      CountDownLatch countDownLatch = new CountDownLatch(topologyMembers * 2);

      locator.addClusterTopologyListener(new LatchClusterTopologyListener(countDownLatch));

      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory();
      addSessionFactory(sf);

      assertTrue("topology members expected " + topologyMembers, countDownLatch.await(5, TimeUnit.SECONDS));

      closeSessionFactory(sf);

      sf = (ClientSessionFactoryInternal) locator.createSessionFactory(liveServer.getServer().getNodeID().toString());
      addSessionFactory(sf);

      if (sf2 == null) {
         sf2 = (ClientSessionFactoryInternal) locator.createSessionFactory(backupServer.getServer().getNodeID().toString());

         ClientSession session2 = createSession(sf2, false, false);
         session2.createQueue(ADDRESS, ADDRESS, null, true);
         addSessionFactory(sf2);
      }

      return sf;
   }

   protected final ClientSessionFactoryInternal createSessionFactoryAndWaitForTopology(ServerLocator locator,
                                                                                       TransportConfiguration transportConfiguration,
                                                                                       int topologyMembers) throws Exception {
      CountDownLatch countDownLatch = new CountDownLatch(topologyMembers * 2);

      locator.addClusterTopologyListener(new LatchClusterTopologyListener(countDownLatch));

      ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) locator.createSessionFactory(transportConfiguration);
      addSessionFactory(sf);

      assertTrue("topology members expected " + topologyMembers, countDownLatch.await(5, TimeUnit.SECONDS));

      closeSessionFactory(sf);

      sf = (ClientSessionFactoryInternal) locator.createSessionFactory(liveServer.getServer().getNodeID().toString());
      addSessionFactory(sf);

      if (sf2 == null) {
         sf2 = (ClientSessionFactoryInternal) locator.createSessionFactory(backupServer.getServer().getNodeID().toString());

         ClientSession session2 = createSession(sf2, false, false);
         session2.createQueue(ADDRESS, ADDRESS, null, true);
         addSessionFactory(sf2);
      }
      return sf;
   }

   @Override
   protected void createClientSessionFactory() throws Exception {
      if (liveServer.getServer().isStarted()) {
         sf = (ClientSessionFactoryInternal) createSessionFactory(locator);
         sf = (ClientSessionFactoryInternal) locator.createSessionFactory(liveServer.getServer().getNodeID().toString());
      } else {
         sf = (ClientSessionFactoryInternal) createSessionFactory(locator);
      }
   }

   @Override
   protected void createSessionFactory() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setReconnectAttempts(300).setRetryInterval(100);

      sf = createSessionFactoryAndWaitForTopology(locator, getConnectorTransportConfiguration(true, 0), 2);

      if (sf2 == null) {
         sf2 = (ClientSessionFactoryInternal) locator.createSessionFactory(backupServer.getServer().getNodeID().toString());
         addSessionFactory(sf2);
         ClientSession session2 = createSession(sf2, false, false);
         session2.createQueue(ADDRESS, ADDRESS, null, true);
      }
   }

   private TransportConfiguration getConnectorTransportConfiguration(boolean live, int server) {
      return TransportConfigurationUtils.getInVMConnector(live, server);
   }

   private TransportConfiguration getAcceptorTransportConfiguration(boolean live, int server) {
      return TransportConfigurationUtils.getInVMAcceptor(live, server);
   }

   @Test
   public void scaleDownDelay() throws Exception {
      createSessionFactory();

      ClientSession session = createSession(sf, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      //send enough messages to ensure that when the client fails over scaledown hasn't complete
      sendMessages(session, producer, 1000);

      crash(session);

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      receiveDurableMessages(consumer);

      session.close();

      sf.close();

      Assert.assertEquals(0, sf.numSessions());

      Assert.assertEquals(0, sf.numConnections());
   }

   // https://jira.jboss.org/jira/browse/HORNETQ-285
   @Override
   @Test
   public void testFailoverOnInitialConnection() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setFailoverOnInitialConnection(true).setReconnectAttempts(300).setRetryInterval(100);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      // Crash live server
      crash();

      ClientSession session = createSession(sf);

      //session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      sendMessages(session, producer, NUM_MESSAGES);

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      receiveMessages(consumer);

      session.close();
   }

   @Override
   @Test
   public void testCreateNewFactoryAfterFailover() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setFailoverOnInitialConnection(true);
      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = sendAndConsume(sf, true);

      crash(true, session);

      session.close();

      long timeout;
      timeout = System.currentTimeMillis() + 5000;
      while (timeout > System.currentTimeMillis()) {
         try {
            createClientSessionFactory();
            break;
         } catch (Exception e) {
            // retrying
            Thread.sleep(100);
         }
      }

      session = sendAndConsume(sf, false);
   }


   @Override
   public void testTimeoutOnFailoverTransactionCommitTimeoutCommunication() throws Exception {
   }


      //invalid tests for Live to Live failover
   //all the timeout ones aren't as we don't migrate timeouts, any failback or server restart
   //or replicating tests aren't either
   @Override
   public void testLiveAndBackupBackupComesBackNewFactory() throws Exception {
   }

   @Override
   public void testLiveAndBackupLiveComesBackNewFactory() {
   }

   @Override
   public void testTimeoutOnFailoverConsumeBlocked() throws Exception {
   }

   @Override
   public void testFailoverMultipleSessionsWithConsumers() throws Exception {
      //
   }

   @Override
   public void testTimeoutOnFailover() throws Exception {
   }

   @Override
   public void testTimeoutOnFailoverTransactionRollback() throws Exception {
   }

   @Override
   public void testTimeoutOnFailoverConsume() throws Exception {
   }

   @Override
   public void testTimeoutOnFailoverTransactionCommit() throws Exception {
   }

   @Override
   public void testFailBack() throws Exception {
   }

   @Override
   public void testFailBackLiveRestartsBackupIsGone() throws Exception {
   }

   @Override
   public void testLiveAndBackupLiveComesBack() throws Exception {
   }

   @Override
   public void testSimpleFailover() throws Exception {
   }

   @Override
   public void testFailThenReceiveMoreMessagesAfterFailover2() throws Exception {
   }

   @Override
   public void testWithoutUsingTheBackup() throws Exception {
   }

   //todo check to see which failing tests are valid,
   @Override
   public void testSimpleSendAfterFailoverDurableNonTemporary() throws Exception {
   }

   @Override
   public void testCommitOccurredUnblockedAndResendNoDuplicates() throws Exception {
   }

   /*@Override
   public void testCommitDidNotOccurUnblockedAndResend() throws Exception
   {
   }



   @Override
   public void testLiveAndBackupLiveComesBackNewFactory() throws Exception
   {
   }

   @Override
   public void testXAMessagesSentSoRollbackOnEnd() throws Exception
   {
   }

   @Override
   public void testLiveAndBackupBackupComesBackNewFactory() throws Exception
   {
   }

   @Override
   public void testXAMessagesSentSoRollbackOnEnd2() throws Exception
   {
   }

   @Override
   public void testXAMessagesSentSoRollbackOnCommit() throws Exception
   {
   }

   @Override
   public void testTransactedMessagesSentSoRollback() throws Exception
   {
   }

   @Override
   public void testXAMessagesSentSoRollbackOnPrepare() throws Exception
   {
   }

   @Override
   public void testNonTransactedWithZeroConsumerWindowSize() throws Exception
   {
   }*/
}
