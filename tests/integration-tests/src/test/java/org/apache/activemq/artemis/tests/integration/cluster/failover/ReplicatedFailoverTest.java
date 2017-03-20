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

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicatedPolicy;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class ReplicatedFailoverTest extends FailoverTest {

   boolean isReplicatedFailbackTest = false;
   @Rule
   public TestRule watcher = new TestWatcher() {
      @Override
      protected void starting(Description description) {
         isReplicatedFailbackTest = description.getMethodName().equals("testReplicatedFailback");
      }

   };

   protected void beforeWaitForRemoteBackupSynchronization() {
   }

   @Test(timeout = 120000)
   /*
   * default maxSavedReplicatedJournalsSize is 2, this means the backup will fall back to replicated only twice, after this
   * it is stopped permanently
   *
   * */ public void testReplicatedFailback() throws Exception {
      try {
         beforeWaitForRemoteBackupSynchronization();

         waitForRemoteBackupSynchronization(backupServer.getServer());

         createSessionFactory();

         ClientSession session = createSession(sf, true, true);

         session.createQueue(ADDRESS, ADDRESS, null, true);

         crash(session);

         ReplicatedPolicy haPolicy = (ReplicatedPolicy) liveServer.getServer().getHAPolicy();

         haPolicy.setCheckForLiveServer(true);

         liveServer.start();

         waitForRemoteBackupSynchronization(liveServer.getServer());

         waitForRemoteBackupSynchronization(backupServer.getServer());

         waitForServerToStart(liveServer.getServer());

         session = createSession(sf, true, true);

         crash(session);

         ReplicatedPolicyConfiguration replicatedPolicyConfiguration = (ReplicatedPolicyConfiguration) liveServer.getServer().getConfiguration().getHAPolicyConfiguration();

         replicatedPolicyConfiguration.setCheckForLiveServer(true);

         liveServer.start();

         waitForRemoteBackupSynchronization(liveServer.getServer());

         waitForRemoteBackupSynchronization(backupServer.getServer());

         waitForServerToStart(liveServer.getServer());

         session = createSession(sf, true, true);

         crash(session);

         replicatedPolicyConfiguration = (ReplicatedPolicyConfiguration) liveServer.getServer().getConfiguration().getHAPolicyConfiguration();

         replicatedPolicyConfiguration.setCheckForLiveServer(true);

         liveServer.start();

         waitForServerToStart(liveServer.getServer());

         backupServer.getServer().waitForActivation(5, TimeUnit.SECONDS);

         waitForRemoteBackupSynchronization(liveServer.getServer());

         waitForServerToStart(backupServer.getServer());

         assertTrue(backupServer.getServer().isStarted());

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
   }

   @Override
   protected void createConfigs() throws Exception {
      createReplicatedConfigs();
   }

   @Override
   protected void setupHAPolicyConfiguration() {
      if (isReplicatedFailbackTest) {
         ((ReplicatedPolicyConfiguration) liveConfig.getHAPolicyConfiguration()).setCheckForLiveServer(true);
         ((ReplicaPolicyConfiguration) backupConfig.getHAPolicyConfiguration()).setMaxSavedReplicatedJournalsSize(2).setAllowFailBack(true);
         ((ReplicaPolicyConfiguration) backupConfig.getHAPolicyConfiguration()).setRestartBackup(false);
      } else {
         super.setupHAPolicyConfiguration();
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
