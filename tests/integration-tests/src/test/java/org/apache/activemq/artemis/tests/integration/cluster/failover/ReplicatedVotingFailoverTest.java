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

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class ReplicatedVotingFailoverTest extends FailoverTestBase {

   boolean testBackupFailsVoteFails = false;
   @Rule
   public TestRule watcher = new TestWatcher() {
      @Override
      protected void starting(Description description) {
         testBackupFailsVoteFails = description.getMethodName().equals("testBackupFailsVoteFails");
      }

   };

   protected void beforeWaitForRemoteBackupSynchronization() {
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

   @Test
   public void testBackupFailsVoteSuccess() throws Exception {
      try {
         beforeWaitForRemoteBackupSynchronization();

         waitForRemoteBackupSynchronization(backupServer.getServer());

         backupServer.stop();

         ServerLocator locator = createInVMLocator(0);
         ClientSessionFactory sessionFactory = locator.createSessionFactory();
         ClientSession session = sessionFactory.createSession();
         addClientSession(session);
         ClientProducer producer = session.createProducer("testAddress");
         producer.send(session.createMessage(true));
         assertTrue(liveServer.isActive());


      } finally {
         try {
            liveServer.getServer().stop();
         } catch (Throwable ignored) {
         }
         try {
            backupServer.getServer().stop();
         } catch (Throwable ignored) {
         }
      }
   }

   @Test
   public void testBackupFailsVoteFails() throws Exception {
      try {
         beforeWaitForRemoteBackupSynchronization();

         waitForRemoteBackupSynchronization(backupServer.getServer());

         backupServer.stop();

         try {
            ServerLocator locator = createInVMLocator(0);
            ClientSessionFactory sessionFactory = locator.createSessionFactory();
            ClientSession session = sessionFactory.createSession();
            addClientSession(session);
            ClientProducer producer = session.createProducer("testAddress");
            producer.send(session.createMessage(true));
         } catch (Exception e) {
            //expected
         }
         waitForServerToStop(liveServer.getServer());
         assertFalse(liveServer.isStarted());


      } finally {
         try {
            liveServer.getServer().stop();
         } catch (Throwable ignored) {
         }
         try {
            backupServer.getServer().stop();
         } catch (Throwable ignored) {
         }
      }
   }

   @Override
   protected void createConfigs() throws Exception {
      createReplicatedConfigs();
   }

   @Override
   protected void setupHAPolicyConfiguration() {
      ((ReplicatedPolicyConfiguration) liveConfig.getHAPolicyConfiguration()).setCheckForLiveServer(true);
      ((ReplicatedPolicyConfiguration) liveConfig.getHAPolicyConfiguration()).setVoteOnReplicationFailure(true);
      ((ReplicaPolicyConfiguration) backupConfig.getHAPolicyConfiguration()).setMaxSavedReplicatedJournalsSize(2).setAllowFailBack(true);
      ((ReplicaPolicyConfiguration) backupConfig.getHAPolicyConfiguration()).setRestartBackup(false);
      if (testBackupFailsVoteFails) {
         ((ReplicatedPolicyConfiguration) liveConfig.getHAPolicyConfiguration()).setQuorumSize(2);
      }
   }

   @Override
   protected void crash(boolean waitFailure, ClientSession... sessions) throws Exception {
      if (sessions.length > 0) {
         for (ClientSession session : sessions) {
            waitForRemoteBackup(session.getSessionFactory(), 5, true, backupServer.getServer());
         }
      } else {
         waitForRemoteBackup(null, 5, true, backupServer.getServer());
      }
      super.crash(waitFailure, sessions);
   }

   @Override
   protected void crash(ClientSession... sessions) throws Exception {
      if (sessions.length > 0) {
         for (ClientSession session : sessions) {
            waitForRemoteBackup(session.getSessionFactory(), 5, true, backupServer.getServer());
         }
      } else {
         waitForRemoteBackup(null, 5, true, backupServer.getServer());
      }
      super.crash(sessions);
   }
}
