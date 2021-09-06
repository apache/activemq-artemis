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

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.DistributedPrimitiveManagerConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationPrimaryPolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.MutableLong;
import org.apache.activemq.artemis.quorum.file.FileBasedPrimitiveManager;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PluggableQuorumReplicationTest extends SharedNothingReplicationTest {

   private DistributedPrimitiveManagerConfiguration managerConfiguration;

   @Before
   public void init() throws IOException {
      managerConfiguration = new DistributedPrimitiveManagerConfiguration(FileBasedPrimitiveManager.class.getName(), Collections.singletonMap("locks-folder", temporaryFolder.newFolder("manager").toString()));
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
   public void testUnReplicatedOrderedTransition() throws Exception {
      // start live
      final Configuration liveConfiguration = createLiveConfiguration();

      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.setIdentity("LIVE");
      liveServer.start();

      Wait.waitFor(liveServer::isStarted);

      ServerLocator locator = ServerLocatorImpl.newLocator("(tcp://localhost:61616,tcp://localhost:61617)?ha=true");
      locator.setCallTimeout(60_000L);
      locator.setConnectionTTL(60_000L);

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

      waitForTopology(liveServer, 1, 1, 30000);
      waitForTopology(backupServer, 1, 1, 30000);

      liveServer.stop();

      // backup will take over and run un replicated

      csf = locator.createSessionFactory();
      clientSession = csf.createSession();
      clientSession.createQueue(new QueueConfiguration("slow_un_replicated").setRoutingType(RoutingType.ANYCAST));
      clientSession.close();

      waitForTopology(backupServer, 1, 0, 30000);
      assertTrue(Wait.waitFor(() -> 2L == backupServer.getNodeManager().getNodeActivationSequence()));

      backupServer.stop(false);

      // now only backup should be able to start as it has run un_replicated
      liveServer.start();
      Wait.assertFalse(liveServer::isActive);
      liveServer.stop();

      // restart backup
      backupServer.start();

      Wait.waitFor(backupServer::isStarted);
      assertEquals(3L, backupServer.getNodeManager().getNodeActivationSequence());

      csf = locator.createSessionFactory();
      clientSession = csf.createSession();
      clientSession.createQueue(new QueueConfiguration("backup_as_un_replicated").setRoutingType(RoutingType.ANYCAST));
      clientSession.close();

      // verify the live restart as a backup to the restarted backupServer that has taken on the live role, no failback
      liveServer.start();

      csf = locator.createSessionFactory();
      clientSession = csf.createSession();
      clientSession.createQueue(new QueueConfiguration("backup_as_replicated").setRoutingType(RoutingType.ANYCAST));
      clientSession.close();

      assertTrue(Wait.waitFor(liveServer::isReplicaSync));
      assertTrue(Wait.waitFor(() -> 3L == liveServer.getNodeManager().getNodeActivationSequence()));

      backupServer.stop(true);

      waitForTopology(liveServer, 1, 0, 30000);
      assertTrue(Wait.waitFor(() -> 4L == liveServer.getNodeManager().getNodeActivationSequence()));

      liveServer.stop(true);
      clientSession.close();
      locator.close();
   }

   @Test
   public void testBackupFailoverAndPrimaryFailback() throws Exception {
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);

      // start live
      Configuration liveConfiguration = createLiveConfiguration();

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
      TimeUnit.MILLISECONDS.sleep(100);

      // restart primary that will request failback
      ActiveMQServer restartedPrimaryForFailBack = primaryInstance; //addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      restartedPrimaryForFailBack.start();

      // first step is backup getting replicated
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
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> restartedPrimaryForFailBack.isReplicaSync(), timeout);
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> backupServer.isReplicaSync(), timeout);
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> 3L == backupServer.getNodeManager().getNodeActivationSequence(), timeout);

      // just to let the console logging breath!
      TimeUnit.MILLISECONDS.sleep(100);

      // stop backup to verify primary goes on with new sequence as un replicated
      backupServer.stop();

      // just to let the console logging breath!
      TimeUnit.MILLISECONDS.sleep(100);

      // live goes un replicated
      org.apache.activemq.artemis.utils.Wait.assertTrue(() -> {
         try {
            return 4L == restartedPrimaryForFailBack.getNodeManager().getNodeActivationSequence();
         } catch (NullPointerException ok) {
            return false;
         }
      }, timeout);

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


   @Test
   public void testBackupStartsFirst() throws Exception {

      // start backup
      Configuration backupConfiguration = createBackupConfiguration();
      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.setIdentity("BACKUP");
      backupServer.start();

      // start live
      final Configuration liveConfiguration = createLiveConfiguration();

      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.setIdentity("LIVE");
      liveServer.start();

      Wait.waitFor(liveServer::isStarted);

      assertTrue(Wait.waitFor(backupServer::isStarted));
      assertTrue(Wait.waitFor(backupServer::isReplicaSync));
      assertTrue(liveServer.isReplicaSync());
   }

   @Test
   public void testBackupOutOfSequenceReleasesLock() throws Exception {

      // start backup
      Configuration backupConfiguration = createBackupConfiguration();
      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.setIdentity("BACKUP");
      backupServer.start();

      // start live
      final Configuration liveConfiguration = createLiveConfiguration();

      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.setIdentity("LIVE");
      liveServer.start();

      Wait.waitFor(liveServer::isStarted);

      assertTrue(Wait.waitFor(backupServer::isStarted));
      assertTrue(Wait.waitFor(backupServer::isReplicaSync));
      assertTrue(liveServer.isReplicaSync());

      backupServer.stop();

      TimeUnit.SECONDS.sleep(1);

      liveServer.stop();
      // backup can get lock but does not have the sequence to start, will try and be a backup

      backupServer.start();

      // live server should be active
      liveServer.start();
      Wait.waitFor(liveServer::isStarted);

      assertTrue(Wait.waitFor(backupServer::isStarted));
      assertTrue(Wait.waitFor(backupServer::isReplicaSync));
      assertTrue(liveServer.isReplicaSync());
   }


   @Test
   public void testBackupOutOfSequenceCheckActivationSequence() throws Exception {

      // start backup
      Configuration backupConfiguration = createBackupConfiguration();
      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.setIdentity("BACKUP");
      backupServer.start();

      // start live
      final Configuration liveConfiguration = createLiveConfiguration();

      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.setIdentity("LIVE");
      liveServer.start();

      Wait.waitFor(liveServer::isStarted);

      assertTrue(Wait.waitFor(backupServer::isStarted));
      assertTrue(Wait.waitFor(backupServer::isReplicaSync));
      assertTrue(liveServer.isReplicaSync());

      backupServer.stop();

      TimeUnit.SECONDS.sleep(1);

      final String coordinatedId = liveServer.getNodeID().toString();
      liveServer.stop();

      // backup can get lock but does not have the sequence to start, will try and be a backup
      // to verify it can short circuit with a dirty read we grab the lock for a little while
      DistributedPrimitiveManager distributedPrimitiveManager = DistributedPrimitiveManager.newInstanceOf(
         managerConfiguration.getClassName(),
         managerConfiguration.getProperties());
      distributedPrimitiveManager.start();
      final DistributedLock lock = distributedPrimitiveManager.getDistributedLock(coordinatedId);
      assertTrue(lock.tryLock());
      CountDownLatch preActivate = new CountDownLatch(1);
      backupServer.registerActivateCallback(new ActivateCallback() {
         @Override
         public void preActivate() {
            ActivateCallback.super.preActivate();
            preActivate.countDown();
         }
      });
      backupServer.start();

      // it should be able to do a dirty read of the sequence id and not have to wait to get a lock
      assertTrue(preActivate.await(1, TimeUnit.SECONDS));

      // release the lock
      distributedPrimitiveManager.stop();

      // live server should be active
      liveServer.start();
      Wait.waitFor(liveServer::isStarted);

      assertTrue(Wait.waitFor(backupServer::isReplicaSync));
      assertTrue(liveServer.isReplicaSync());
   }

   @Test
   public void testSelfRepairPrimary() throws Exception {
      // start live
      final Configuration liveConfiguration = createLiveConfiguration();
      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.setIdentity("LIVE");

      liveServer.start();
      final String coordinatedId = liveServer.getNodeID().toString();
      Wait.waitFor(liveServer::isStarted);
      liveServer.stop();

      liveServer.start();
      Wait.waitFor(liveServer::isStarted);
      Assert.assertEquals(2, liveServer.getNodeManager().getNodeActivationSequence());
      liveServer.stop();

      // backup can get lock but does not have the sequence to start, will try and be a backup
      // to verify it can short circuit with a dirty read we grab the lock for a little while
      DistributedPrimitiveManager distributedPrimitiveManager = DistributedPrimitiveManager
         .newInstanceOf(managerConfiguration.getClassName(), managerConfiguration.getProperties());
      distributedPrimitiveManager.start();
      try (DistributedLock lock = distributedPrimitiveManager.getDistributedLock(coordinatedId)) {
         assertTrue(lock.tryLock());
         distributedPrimitiveManager.getMutableLong(coordinatedId).compareAndSet(2, -2);
      }
      liveServer.start();
      Wait.waitFor(liveServer::isStarted);
      Assert.assertEquals(3, liveServer.getNodeManager().getNodeActivationSequence());
      Assert.assertEquals(3, distributedPrimitiveManager.getMutableLong(coordinatedId).get());

      distributedPrimitiveManager.stop();

      Configuration backupConfiguration = createBackupConfiguration();
      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.setIdentity("BACKUP");
      backupServer.start();
      Wait.waitFor(backupServer::isReplicaSync);
      Assert.assertEquals(3, backupServer.getNodeManager().getNodeActivationSequence());
      backupServer.stop();
   }

   @Test
   public void testPrimaryPeers() throws Exception {
      final String PEER_NODE_ID = "some-shared-id-001";

      final Configuration liveConfiguration = createLiveConfiguration();
      ((ReplicationPrimaryPolicyConfiguration)liveConfiguration.getHAPolicyConfiguration()).setCoordinationId(PEER_NODE_ID);

      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.setIdentity("LIVE");
      liveServer.start();

      Wait.waitFor(liveServer::isStarted);

      ServerLocator locator = ServerLocatorImpl.newLocator("(tcp://localhost:61616,tcp://localhost:61617)?ha=true");
      locator.setCallTimeout(60_000L);
      locator.setConnectionTTL(60_000L);
      final ClientSessionFactory keepLocatorAliveSLF = locator.createSessionFactory();

      ClientSessionFactory csf = locator.createSessionFactory();
      sendTo(csf, "live_un_replicated");
      csf.close();

      // start peer, will backup
      Configuration peerLiveConfiguration = createBackupConfiguration(); // to get acceptors and locators ports that won't clash
      peerLiveConfiguration.setHAPolicyConfiguration(createReplicationLiveConfiguration());
      ((ReplicationPrimaryPolicyConfiguration)peerLiveConfiguration.getHAPolicyConfiguration()).setCoordinationId(PEER_NODE_ID);
      peerLiveConfiguration.setName("localhost::live-peer");

      ActiveMQServer livePeerServer = addServer(ActiveMQServers.newActiveMQServer(peerLiveConfiguration));
      livePeerServer.setIdentity("LIVE-PEER");
      livePeerServer.start();

      Wait.waitFor(livePeerServer::isStarted);

      waitForTopology(liveServer, 1, 1, 30000);
      waitForTopology(livePeerServer, 1, 1, 30000);

      liveServer.stop();

      // livePeerServer will take over and run un replicated

      csf = locator.createSessionFactory();
      receiveFrom(csf, "live_un_replicated");
      sendTo(csf, "peer_un_replicated");
      csf.close();

      waitForTopology(livePeerServer, 1, 0, 30000);

      assertTrue(Wait.waitFor(() -> 2L == livePeerServer.getNodeManager().getNodeActivationSequence()));

      livePeerServer.stop(false);

      liveServer.start();

      Wait.assertTrue(() -> !liveServer.isActive());

      // restart backup
      livePeerServer.start();

      Wait.waitFor(livePeerServer::isStarted);

      assertEquals(3L, livePeerServer.getNodeManager().getNodeActivationSequence());

      csf = locator.createSessionFactory();
      receiveFrom(csf, "peer_un_replicated");
      sendTo(csf, "backup_as_un_replicated");
      csf.close();

      // verify the live restart as a backup to the restarted PeerLiveServer that has taken on the live role
      liveServer.start();

      csf = locator.createSessionFactory();
      receiveFrom(csf, "backup_as_un_replicated");
      sendTo(csf, "backup_as_replicated");
      csf.close();

      assertTrue(Wait.waitFor(liveServer::isReplicaSync));
      assertTrue(Wait.waitFor(() -> 3L == liveServer.getNodeManager().getNodeActivationSequence()));

      waitForTopology(liveServer, 1, 1, 30000);
      waitForTopology(livePeerServer, 1, 1, 30000);

      livePeerServer.stop(true);

      assertTrue(Wait.waitFor(() -> 4L == liveServer.getNodeManager().getNodeActivationSequence()));

      csf = locator.createSessionFactory();
      receiveFrom(csf, "backup_as_replicated");
      csf.close();

      waitForTopology(liveServer, 1, 0, 30000);

      liveServer.stop(true);
      keepLocatorAliveSLF.close();
      locator.close();
   }

   @Test
   public void testUnavailableSelfHeal() throws Exception {

      // start backup
      Configuration backupConfiguration = createBackupConfiguration();
      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.setIdentity("BACKUP");
      backupServer.start();

      // start live
      final Configuration liveConfiguration = createLiveConfiguration();

      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.setIdentity("LIVE");
      liveServer.start();

      Wait.waitFor(liveServer::isStarted);

      assertTrue(Wait.waitFor(backupServer::isStarted));
      assertTrue(Wait.waitFor(backupServer::isReplicaSync));
      assertTrue(liveServer.isReplicaSync());

      final String coordinatedId = liveServer.getNodeID().toString();

      backupServer.stop();
      TimeUnit.MILLISECONDS.sleep(500);
      liveServer.stop();

      // some  manual intervention to force an unavailable
      // simulate live failing in activation local sequence update on un replicated run when backup stops.

      DistributedPrimitiveManager distributedPrimitiveManager = DistributedPrimitiveManager.newInstanceOf(managerConfiguration.getClassName(), managerConfiguration.getProperties());
      distributedPrimitiveManager.start();
      final MutableLong activationSequence = distributedPrimitiveManager.getMutableLong(coordinatedId);
      Assert.assertTrue(activationSequence.compareAndSet(2, -2));

      // live server should activate after self healing its outstanding claim
      liveServer.start();
      Wait.waitFor(liveServer::isStarted);
      Assert.assertEquals(3, liveServer.getNodeManager().getNodeActivationSequence());
      Assert.assertEquals(3, activationSequence.get());
   }

   @Test
   public void testUnavailableAutoRepair() throws Exception {
      // start backup
      Configuration backupConfiguration = createBackupConfiguration();
      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.setIdentity("BACKUP");
      backupServer.start();

      // start live
      final Configuration liveConfiguration = createLiveConfiguration();

      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.setIdentity("LIVE");
      liveServer.start();

      Wait.waitFor(liveServer::isStarted);

      assertTrue(Wait.waitFor(backupServer::isStarted));
      assertTrue(Wait.waitFor(backupServer::isReplicaSync));
      assertTrue(liveServer.isReplicaSync());

      final String coordinatedId = liveServer.getNodeID().toString();

      backupServer.stop();
      TimeUnit.MILLISECONDS.sleep(500);
      liveServer.stop();

      // some  manual intervention to force an unavailable
      // simulate live failing in activation local sequence update on un replicated run when backup stops.

      DistributedPrimitiveManager distributedPrimitiveManager = DistributedPrimitiveManager.newInstanceOf(
         managerConfiguration.getClassName(),
         managerConfiguration.getProperties());
      distributedPrimitiveManager.start();
      final MutableLong coordinatedActivationSequence = distributedPrimitiveManager.getMutableLong(coordinatedId);
      Assert.assertTrue(coordinatedActivationSequence.compareAndSet(2, -2));

      // case: 2, the fail to write locally 2 but the write actually failing
      // need to put 1 in the local activation sequence of the live
      FileLockNodeManager fileLockNodeManager = new FileLockNodeManager(liveConfiguration.getNodeManagerLockLocation().getAbsoluteFile(), true);
      fileLockNodeManager.start();
      assertEquals(2, fileLockNodeManager.readNodeActivationSequence());
      fileLockNodeManager.writeNodeActivationSequence(1);
      fileLockNodeManager.stop();

      // should delay pending resolution of the uncommitted claim
      backupServer.start();
      CountDownLatch liveStarting = new CountDownLatch(1);
      // should delay pending resolution of the uncommitted claim
      // IMPORTANT: primary activation run on the start caller thread!! We need another thread here
      final Thread liveServerStarterThread = new Thread(() -> {
         liveStarting.countDown();
         try {
            liveServer.start();
         } catch (Throwable e) {
            e.printStackTrace();
         }
      });
      liveServerStarterThread.start();
      liveStarting.await();
      TimeUnit.MILLISECONDS.sleep(500);
      // both are candidates and one of them failed to commit the claim
      // let them compete on retry
      // one of the two can activate
      Wait.waitFor(() -> liveServer.isStarted() || backupServer.isStarted());

      assertTrue(Wait.waitFor(backupServer::isReplicaSync));
      assertTrue(liveServer.isReplicaSync());

      assertEquals(3, backupServer.getNodeManager().getNodeActivationSequence());
      assertEquals(3, liveServer.getNodeManager().getNodeActivationSequence());

   }

   private void sendTo(ClientSessionFactory clientSessionFactory, String addr) throws Exception {
      ClientSession clientSession = clientSessionFactory.createSession(true, true);
      clientSession.createQueue(new QueueConfiguration(addr).setRoutingType(RoutingType.ANYCAST).setDurable(true));
      ClientProducer producer = clientSession.createProducer(addr);
      ClientMessage message = clientSession.createMessage(true);
      message.putStringProperty("K", addr);
      message.putLongProperty("delay", 0L); // so slow interceptor does not get us
      producer.send(message);
      producer.close();
      clientSession.close();
   }

   private void receiveFrom(ClientSessionFactory clientSessionFactory, String addr) throws Exception {
      ClientSession clientSession = clientSessionFactory.createSession(true, true);
      clientSession.start();
      ClientConsumer consumer = clientSession.createConsumer(addr);
      Message message = consumer.receive(4000);
      assertNotNull(message);
      assertTrue(message.getStringProperty("K").equals(addr));
      consumer.close();
      clientSession.close();
   }
}
