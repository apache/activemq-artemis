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
package org.apache.activemq.artemis.tests.integration.replication;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.DistributedPrimitiveManagerConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationPrimaryPolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.quorum.file.FileBasedPrimitiveManager;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PluggableQuorumReplicationTest extends SharedNothingReplicationTest {

   private DistributedPrimitiveManagerConfiguration managerConfiguration;
   @Rule
   public TemporaryFolder tmpFolder = new TemporaryFolder();

   @Before
   public void init() throws IOException {
      managerConfiguration = new DistributedPrimitiveManagerConfiguration(FileBasedPrimitiveManager.class.getName(), Collections.singletonMap("locks-folder", tmpFolder.newFolder("manager").toString()));
   }

   @Override
   protected HAPolicyConfiguration createReplicationLiveConfiguration() {
      ReplicationPrimaryPolicyConfiguration haPolicy = ReplicationPrimaryPolicyConfiguration.withDefault();
      haPolicy.setDistributedManagerConfiguration(managerConfiguration);
      return haPolicy;
   }

   @Override
   protected HAPolicyConfiguration createReplicationBackupConfiguration() {
      ReplicationBackupPolicyConfiguration haPolicy = ReplicationBackupPolicyConfiguration.withDefault();
      haPolicy.setDistributedManagerConfiguration(managerConfiguration);
      haPolicy.setClusterName("cluster");
      return haPolicy;
   }

   @Test
   public void testUnReplicatedOrderedTransitionCheckForLiveDefaultFalse() throws Exception {
      // start live
      Configuration liveConfiguration = createLiveConfiguration();
      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.setIdentity("LIVE");
      liveServer.start();

      Wait.waitFor(liveServer::isStarted);

      final CountDownLatch replicated = new CountDownLatch(1);

      final CountDownLatch unReplicated = new CountDownLatch(1);

      final String[] backUpNodeIdHolder = new String[1];
      ServerLocator locator = ServerLocatorImpl.newLocator("(tcp://localhost:61616,tcp://localhost:61617)?ha=true");
      locator.setCallTimeout(60_000L);
      locator.setConnectionTTL(60_000L);
      locator.addClusterTopologyListener(new ClusterTopologyListener() {
         @Override
         public void nodeUP(TopologyMember member, boolean last) {
            if (member.getBackup() != null) {
               replicated.countDown();
               backUpNodeIdHolder[0] = member.getNodeId();
            }
         }

         @Override
         public void nodeDown(long eventUID, String nodeID) {
            if (nodeID.equals(backUpNodeIdHolder[0])) {
               unReplicated.countDown();
               backUpNodeIdHolder[0] = "down";
            }
         }
      });

      ClientSessionFactory csf = locator.createSessionFactory();
      ClientSession clientSession = csf.createSession();
      clientSession.createQueue(new QueueConfiguration("slow").setRoutingType(RoutingType.ANYCAST));
      clientSession.close();

      // start backup
      Configuration backupConfiguration = createBackupConfiguration();
      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.setIdentity("BACKUP");
      backupServer.start();

      Wait.waitFor(backupServer::isStarted);

      Assert.assertTrue("can not replicate in 30 seconds", replicated.await(30, TimeUnit.SECONDS));


      liveServer.stop();

      // backup will take over and run un replicated

      csf = locator.createSessionFactory();
      clientSession = csf.createSession();
      clientSession.createQueue(new QueueConfiguration("slow_un_replicated").setRoutingType(RoutingType.ANYCAST));
      clientSession.close();


      Assert.assertTrue("can not un replicate in 30 seconds", unReplicated.await(30, TimeUnit.SECONDS));

      backupServer.stop(false);

      // now only backup should be able to start as it has run un_replicated

      // live activation should fail as it is now stale!
      final AtomicReference<Exception> failedActivation = new AtomicReference<>();
      liveServer.registerActivationFailureListener(failedActivation::set);

      liveServer.start();

      assertTrue(Wait.waitFor(() -> failedActivation.get() != null && failedActivation.get().getMessage().contains("coordinated")));
      liveServer.stop();

      // restart backup but with a new instance, as going live has changed its policy and role
      ActiveMQServer restartedBackupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      restartedBackupServer.start();

      Wait.waitFor(restartedBackupServer::isStarted);
      assertEquals(3, restartedBackupServer.getNodeManager().getNodeActivationSequence());

      csf = locator.createSessionFactory();
      clientSession = csf.createSession();
      clientSession.createQueue(new QueueConfiguration("backup_as_un_replicated").setRoutingType(RoutingType.ANYCAST));
      clientSession.close();

      // live restarted won't check for live by default so will fail to get lock and error out
      ActiveMQServer restartedLiveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      restartedLiveServer.setIdentity("LIVE");

      final AtomicReference<Exception> failedActivationNoLock = new AtomicReference<>();
      restartedLiveServer.registerActivationFailureListener(failedActivationNoLock::set);

      restartedLiveServer.start();
      assertTrue(Wait.waitFor(() -> failedActivationNoLock.get() != null && failedActivationNoLock.get().getMessage().contains("Failed to become live")));

      // but what happens next to this server, it is a zombie?
      restartedLiveServer.stop();

      // all done
      restartedBackupServer.stop();
      clientSession.close();
      locator.close();
   }

   @Test
   public void testUnReplicatedOrderedTransitionWithCheckForLiveTrue() throws Exception {
      // start live
      final Configuration liveConfiguration = createLiveConfiguration();
      ((ReplicationPrimaryPolicyConfiguration)liveConfiguration.getHAPolicyConfiguration()).setCheckForLiveServer(true);

      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.setIdentity("LIVE");
      liveServer.start();

      Wait.waitFor(liveServer::isStarted);

      final CountDownLatch replicated = new CountDownLatch(1);

      final CountDownLatch unReplicated = new CountDownLatch(1);

      final String[] backUpNodeIdHolder = new String[1];
      ServerLocator locator = ServerLocatorImpl.newLocator("(tcp://localhost:61616,tcp://localhost:61617)?ha=true");
      locator.setCallTimeout(60_000L);
      locator.setConnectionTTL(60_000L);
      locator.addClusterTopologyListener(new ClusterTopologyListener() {
         @Override
         public void nodeUP(TopologyMember member, boolean last) {
            if (member.getBackup() != null) {
               replicated.countDown();
               backUpNodeIdHolder[0] = member.getNodeId();
            }
         }

         @Override
         public void nodeDown(long eventUID, String nodeID) {
            if (nodeID.equals(backUpNodeIdHolder[0])) {
               unReplicated.countDown();
               backUpNodeIdHolder[0] = "down";
            }
         }
      });

      ClientSessionFactory csf = locator.createSessionFactory();
      ClientSession clientSession = csf.createSession();
      clientSession.createQueue(new QueueConfiguration("slow").setRoutingType(RoutingType.ANYCAST));
      clientSession.close();

      // start backup
      Configuration backupConfiguration = createBackupConfiguration();
      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.setIdentity("BACKUP");
      backupServer.start();

      Wait.waitFor(backupServer::isStarted);

      Assert.assertTrue("can not replicate in 30 seconds", replicated.await(30, TimeUnit.SECONDS));

      liveServer.stop();

      // backup will take over and run un replicated

      csf = locator.createSessionFactory();
      clientSession = csf.createSession();
      clientSession.createQueue(new QueueConfiguration("slow_un_replicated").setRoutingType(RoutingType.ANYCAST));
      clientSession.close();

      Assert.assertTrue("can not un replicate in 30 seconds", unReplicated.await(30, TimeUnit.SECONDS));
      assertTrue(Wait.waitFor(() -> 2L == backupServer.getNodeManager().getNodeActivationSequence()));

      backupServer.stop(false);

      // now only backup should be able to start as it has run un_replicated

      // live activation should fail as it is now stale!
      final AtomicReference<Exception> failedActivation = new AtomicReference<>();
      liveServer.registerActivationFailureListener(failedActivation::set);

      liveServer.start();

      assertTrue(Wait.waitFor(() -> failedActivation.get() != null && failedActivation.get().getMessage().contains("coordinated")));
      liveServer.stop();

      // restart backup but with a new instance, as going live has changed its policy and role
      ActiveMQServer restartedBackupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      restartedBackupServer.start();

      Wait.waitFor(restartedBackupServer::isStarted);
      assertEquals(3L, restartedBackupServer.getNodeManager().getNodeActivationSequence());

      csf = locator.createSessionFactory();
      clientSession = csf.createSession();
      clientSession.createQueue(new QueueConfiguration("backup_as_un_replicated").setRoutingType(RoutingType.ANYCAST));
      clientSession.close();

      backUpNodeIdHolder[0] = ""; // reset to capture topology node up on in_sync_replication

      // verify the live restart as a backup to the restartedBackupServer that has taken on the live role, no failback
      ActiveMQServer restartedLiveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      restartedLiveServer.setIdentity("LIVE");

      restartedLiveServer.start();

      csf = locator.createSessionFactory();
      clientSession = csf.createSession();
      clientSession.createQueue(new QueueConfiguration("backup_as_replicated").setRoutingType(RoutingType.ANYCAST));
      clientSession.close();

      assertTrue(Wait.waitFor(restartedLiveServer::isReplicaSync));
      assertTrue(Wait.waitFor(() -> 3L == restartedLiveServer.getNodeManager().getNodeActivationSequence()));

      restartedBackupServer.stop(true);

      assertTrue("original live is un_replicated live again", Wait.waitFor(() -> "down".equals(backUpNodeIdHolder[0])));

      assertTrue(Wait.waitFor(() -> 4L == restartedLiveServer.getNodeManager().getNodeActivationSequence()));

      restartedLiveServer.stop(true);
      clientSession.close();
      locator.close();
   }

   @Test
   public void testBackupFailoverAndPrimaryFailback() throws Exception {
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);

      // start live
      Configuration liveConfiguration = createLiveConfiguration();
      ((ReplicationPrimaryPolicyConfiguration)liveConfiguration.getHAPolicyConfiguration()).setCheckForLiveServer(true);

      ActiveMQServer primaryInstance = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      primaryInstance.setIdentity("PRIMARY");
      primaryInstance.start();

      // primary initially UN REPLICATED
      Assert.assertEquals(1L, primaryInstance.getNodeManager().getNodeActivationSequence());

      // start backup
      Configuration backupConfiguration = createBackupConfiguration();
      ((ReplicationBackupPolicyConfiguration)backupConfiguration.getHAPolicyConfiguration()).setAllowFailBack(true);

      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.setIdentity("BACKUP");
      backupServer.start();

      Wait.waitFor(backupServer::isStarted);

      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> backupServer.isReplicaSync(), timeout);

      // primary REPLICATED, backup matches (has replicated) activation sequence
      Assert.assertEquals(1L, primaryInstance.getNodeManager().getNodeActivationSequence());
      Assert.assertEquals(1L, backupServer.getNodeManager().getNodeActivationSequence());

      primaryInstance.stop();

      // backup UN REPLICATED (new version)
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> 2L == backupServer.getNodeManager().getNodeActivationSequence(), timeout);

      // just to let the console logging breath!
      TimeUnit.SECONDS.sleep(1);

      // expect backup to restart on replication sync as primary requests failback
      ActiveMQServer restartedPrimaryForFailBack = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      restartedPrimaryForFailBack.setIdentity("PRIMARY");
      restartedPrimaryForFailBack.start();

      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> backupServer.isReplicaSync(), timeout);

      // restarted primary will run un replicated (increment sequence) while backup restarts to revert to backup role.
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> {
         try {
            return 3L == restartedPrimaryForFailBack.getNodeManager().getNodeActivationSequence();
         } catch (NullPointerException ok) {
            return false;
         }
      }, timeout);

      // the backup should then resume with an insync replica view of that version
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> 3L == backupServer.getNodeManager().getNodeActivationSequence(), timeout);
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> restartedPrimaryForFailBack.isReplicaSync(), timeout);
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> backupServer.isReplicaSync(), timeout);

      // just to let the console logging breath!
      TimeUnit.SECONDS.sleep(5);


      // stop backup to verify primary goes on with new sequence as un replicated
      backupServer.stop();

      // just to let the console logging breath!
      TimeUnit.SECONDS.sleep(5);

      // live goes un replicated
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> {
         try {
            return 4L == restartedPrimaryForFailBack.getNodeManager().getNodeActivationSequence();
         } catch (NullPointerException ok) {
            return false;
         }
      }, timeout * 10);

      restartedPrimaryForFailBack.stop();
   }


   @Test
   public void testPrimaryIncrementActivationSequenceOnUnReplicated() throws Exception {
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);

      // start live
      Configuration liveConfiguration = createLiveConfiguration();

      ActiveMQServer primaryInstance = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      primaryInstance.setIdentity("PRIMARY");
      primaryInstance.start();

      // primary UN REPLICATED
      Assert.assertEquals(1L, primaryInstance.getNodeManager().getNodeActivationSequence());

      // start backup
      Configuration backupConfiguration = createBackupConfiguration();

      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.setIdentity("BACKUP");
      backupServer.start();

      Wait.waitFor(backupServer::isStarted);

      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> backupServer.isReplicaSync(), timeout);

      // primary REPLICATED, backup matches (has replicated) activation sequence
      Assert.assertEquals(1L, primaryInstance.getNodeManager().getNodeActivationSequence());
      Assert.assertEquals(1L, backupServer.getNodeManager().getNodeActivationSequence());

      // transition to un replicated once backup goes away
      backupServer.stop();

      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> 2L == primaryInstance.getNodeManager().getNodeActivationSequence(), timeout);

      // done
      primaryInstance.stop();
   }

}
