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
package org.apache.activemq.artemis.core.config.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.server.cluster.ha.ColocatedPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.LiveOnlyPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicaPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicatedPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicationBackupPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicationPrimaryPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ScaleDownPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreMasterPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreSlavePolicy;
import org.apache.activemq.artemis.core.server.impl.Activation;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ColocatedActivation;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.core.server.impl.LiveOnlyActivation;
import org.apache.activemq.artemis.core.server.impl.ReplicationBackupActivation;
import org.apache.activemq.artemis.core.server.impl.ReplicationPrimaryActivation;
import org.apache.activemq.artemis.core.server.impl.SharedNothingBackupActivation;
import org.apache.activemq.artemis.core.server.impl.SharedNothingLiveActivation;
import org.apache.activemq.artemis.core.server.impl.SharedStoreBackupActivation;
import org.apache.activemq.artemis.core.server.impl.SharedStoreLiveActivation;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.MutableLong;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;

public class HAPolicyConfigurationTest extends ActiveMQTestBase {

   @Test
   public void shouldNotUseJdbcNodeManagerWithoutHAPolicy() throws Exception {
      Configuration configuration = createConfiguration("database-store-no-hapolicy-config.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      assertEquals(StoreConfiguration.StoreType.DATABASE, server.getConfiguration().getStoreConfiguration().getStoreType());
      assertEquals(HAPolicyConfiguration.TYPE.LIVE_ONLY, server.getConfiguration().getHAPolicyConfiguration().getType());
      try {
         server.start();
         assertThat(server.getNodeManager(), instanceOf(FileLockNodeManager.class));
      } finally {
         server.stop();
      }
   }

   @Test
   public void liveOnlyTest() throws Exception {
      Configuration configuration = createConfiguration("live-only-hapolicy-config.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof LiveOnlyActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof LiveOnlyPolicy);
         LiveOnlyPolicy liveOnlyPolicy = (LiveOnlyPolicy) haPolicy;
         ScaleDownPolicy scaleDownPolicy = liveOnlyPolicy.getScaleDownPolicy();
         assertNotNull(scaleDownPolicy);
         assertEquals(scaleDownPolicy.getGroupName(), "boo!");
         assertEquals(scaleDownPolicy.getDiscoveryGroup(), "wahey");
         List<String> connectors = scaleDownPolicy.getConnectors();
         assertNotNull(connectors);
         assertEquals(connectors.size(), 0);
      } finally {
         server.stop();
      }
   }

   @Test
   public void liveOnlyTest2() throws Exception {
      Configuration configuration = createConfiguration("live-only-hapolicy-config2.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof LiveOnlyActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof LiveOnlyPolicy);
         LiveOnlyPolicy liveOnlyPolicy = (LiveOnlyPolicy) haPolicy;
         ScaleDownPolicy scaleDownPolicy = liveOnlyPolicy.getScaleDownPolicy();
         assertNotNull(scaleDownPolicy);
         assertFalse(scaleDownPolicy.isEnabled());
         assertEquals(scaleDownPolicy.getGroupName(), "boo!");
         assertEquals(scaleDownPolicy.getDiscoveryGroup(), null);
         List<String> connectors = scaleDownPolicy.getConnectors();
         assertNotNull(connectors);
         assertEquals(connectors.size(), 2);
         assertTrue(connectors.contains("sd-connector1"));
         assertTrue(connectors.contains("sd-connector2"));
      } finally {
         server.stop();
      }
   }

   @Test
   public void liveOnlyTest3() throws Exception {
      liveOnlyTest("live-only-hapolicy-config3.xml");
   }

   @Test
   public void liveOnlyTest4() throws Exception {
      liveOnlyTest("live-only-hapolicy-config4.xml");
   }

   @Test
   public void liveOnlyTest5() throws Exception {
      liveOnlyTest("live-only-hapolicy-config5.xml");
   }

   public static class FakeDistributedPrimitiveManager implements DistributedPrimitiveManager {

      private final Map<String, String> config;
      private boolean started;
      private DistributedLock lock;

      public FakeDistributedPrimitiveManager(Map<String, String> config) {
         this.config = config;
         this.started = false;
      }

      public Map<String, String> getConfig() {
         return config;
      }

      @Override
      public void addUnavailableManagerListener(UnavailableManagerListener listener) {
         // no op
      }

      @Override
      public void removeUnavailableManagerListener(UnavailableManagerListener listener) {
         // no op
      }

      @Override
      public boolean start(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
         started = true;
         return true;
      }

      @Override
      public void start() throws InterruptedException, ExecutionException {
         started = true;
      }

      @Override
      public boolean isStarted() {
         return started;
      }

      @Override
      public void stop() {
         started = false;
         if (lock != null) {
            lock.close();
         }
         lock = null;
      }

      @Override
      public DistributedLock getDistributedLock(String lockId) {
         if (!started) {
            throw new IllegalStateException("need to start first");
         }
         if (lock == null) {
            lock = new DistributedLock() {

               private boolean held;

               @Override
               public String getLockId() {
                  return lockId;
               }

               @Override
               public boolean isHeldByCaller() throws UnavailableStateException {
                  return held;
               }

               @Override
               public boolean tryLock() throws UnavailableStateException, InterruptedException {
                  if (held) {
                     return false;
                  }
                  held = true;
                  return true;
               }

               @Override
               public void unlock() throws UnavailableStateException {
                  held = false;
               }

               @Override
               public void addListener(UnavailableLockListener listener) {

               }

               @Override
               public void removeListener(UnavailableLockListener listener) {

               }

               @Override
               public void close() {
                  held = false;
               }
            };
         } else if (!lock.getLockId().equals(lockId)) {
            throw new IllegalStateException("This shouldn't happen");
         }
         return lock;
      }

      @Override
      public MutableLong getMutableLong(String mutableLongId) throws InterruptedException, ExecutionException, TimeoutException {
         // TODO
         return null;
      }

      @Override
      public void close() {
         stop();
      }
   }

   private static void validateManagerConfig(Map<String, String> config) {
      assertEquals("127.0.0.1:6666", config.get("connect-string"));
      assertEquals("16000", config.get("session-ms"));
      assertEquals("2000", config.get("connection-ms"));
      assertEquals("2", config.get("retries"));
      assertEquals("2000", config.get("retries-ms"));
      assertEquals("test", config.get("namespace"));
      assertEquals("10", config.get("session-percent"));
      assertEquals(7, config.size());
   }

   @Test
   public void PrimaryReplicationTest() throws Exception {
      Configuration configuration = createConfiguration("primary-hapolicy-config.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof ReplicationPrimaryActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ReplicationPrimaryPolicy);
         ReplicationPrimaryPolicy policy = (ReplicationPrimaryPolicy) haPolicy;
         assertFalse(policy.isAllowAutoFailBack());
         assertEquals(9876, policy.getInitialReplicationSyncTimeout());
         assertFalse(policy.canScaleDown());
         assertFalse(policy.isBackup());
         assertFalse(policy.isSharedStore());
         assertTrue(policy.isCheckForLiveServer());
         assertTrue(policy.isWaitForActivation());
         assertEquals("purple", policy.getGroupName());
         assertEquals("purple", policy.getBackupGroupName());
         assertEquals("abcdefg", policy.getClusterName());
         assertFalse(policy.useQuorumManager());
         // check failback companion backup policy
         ReplicationBackupPolicy failbackPolicy = policy.getBackupPolicy();
         assertNotNull(failbackPolicy);
         assertSame(policy, failbackPolicy.getLivePolicy());
         assertEquals(policy.getGroupName(), failbackPolicy.getGroupName());
         assertEquals(policy.getBackupGroupName(), failbackPolicy.getBackupGroupName());
         assertEquals(policy.getClusterName(), failbackPolicy.getClusterName());
         assertEquals(failbackPolicy.getMaxSavedReplicatedJournalsSize(), ActiveMQDefaultConfiguration.getDefaultMaxSavedReplicatedJournalsSize());
         assertEquals(1, failbackPolicy.getVoteRetries());
         assertEquals(1000, failbackPolicy.getVoteRetryWait());
         assertTrue(failbackPolicy.isTryFailback());
         assertTrue(failbackPolicy.isBackup());
         assertFalse(failbackPolicy.isSharedStore());
         assertTrue(failbackPolicy.isWaitForActivation());
         assertFalse(failbackPolicy.useQuorumManager());
         assertEquals(12345, failbackPolicy.getRetryReplicationWait());
         // check scale-down properties
         assertFalse(failbackPolicy.canScaleDown());
         assertNull(failbackPolicy.getScaleDownClustername());
         assertNull(failbackPolicy.getScaleDownGroupName());
         // validate manager
         DistributedPrimitiveManager manager = ((ReplicationPrimaryActivation) activation).getDistributedManager();
         assertNotNull(manager);
         assertEquals(FakeDistributedPrimitiveManager.class.getName(), manager.getClass().getName());
         MatcherAssert.assertThat(manager, IsInstanceOf.instanceOf(FakeDistributedPrimitiveManager.class));
         FakeDistributedPrimitiveManager forwardingManager = (FakeDistributedPrimitiveManager) manager;
         // validate manager config
         validateManagerConfig(forwardingManager.getConfig());
      } finally {
         server.stop();
      }
   }

   @Test
   public void BackupReplicationTest() throws Exception {
      Configuration configuration = createConfiguration("backup-hapolicy-config.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof ReplicationBackupActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ReplicationBackupPolicy);
         ReplicationBackupPolicy policy = (ReplicationBackupPolicy) haPolicy;
         assertEquals("tiddles", policy.getGroupName());
         assertEquals("tiddles", policy.getBackupGroupName());
         assertEquals("33rrrrr", policy.getClusterName());
         assertEquals(22, policy.getMaxSavedReplicatedJournalsSize());
         assertEquals(1, policy.getVoteRetries());
         assertEquals(1000, policy.getVoteRetryWait());
         assertFalse(policy.isTryFailback());
         assertTrue(policy.isBackup());
         assertFalse(policy.isSharedStore());
         assertTrue(policy.isWaitForActivation());
         assertFalse(policy.useQuorumManager());
         assertEquals(12345, policy.getRetryReplicationWait());
         // check scale-down properties
         assertFalse(policy.canScaleDown());
         assertNull(policy.getScaleDownClustername());
         assertNull(policy.getScaleDownGroupName());
         // check failover companion live policy
         ReplicationPrimaryPolicy failoverLivePolicy = policy.getLivePolicy();
         assertNotNull(failoverLivePolicy);
         assertSame(policy, failoverLivePolicy.getBackupPolicy());
         assertFalse(failoverLivePolicy.isAllowAutoFailBack());
         assertEquals(9876, failoverLivePolicy.getInitialReplicationSyncTimeout());
         assertFalse(failoverLivePolicy.canScaleDown());
         assertFalse(failoverLivePolicy.isBackup());
         assertFalse(failoverLivePolicy.isSharedStore());
         assertFalse(failoverLivePolicy.isCheckForLiveServer());
         assertTrue(failoverLivePolicy.isWaitForActivation());
         assertEquals(policy.getGroupName(), failoverLivePolicy.getGroupName());
         assertEquals(policy.getClusterName(), failoverLivePolicy.getClusterName());
         assertEquals(policy.getBackupGroupName(), failoverLivePolicy.getBackupGroupName());
         assertFalse(failoverLivePolicy.useQuorumManager());
         // check scale-down properties
         assertFalse(failoverLivePolicy.canScaleDown());
         assertNull(failoverLivePolicy.getScaleDownClustername());
         assertNull(failoverLivePolicy.getScaleDownGroupName());
         // validate manager
         DistributedPrimitiveManager manager = ((ReplicationBackupActivation) activation).getDistributedManager();
         assertNotNull(manager);
         assertEquals(FakeDistributedPrimitiveManager.class.getName(), manager.getClass().getName());
         MatcherAssert.assertThat(manager, IsInstanceOf.instanceOf(FakeDistributedPrimitiveManager.class));
         FakeDistributedPrimitiveManager forwardingManager = (FakeDistributedPrimitiveManager) manager;
         // validate manager config
         validateManagerConfig(forwardingManager.getConfig());
      } finally {
         server.stop();
      }
   }

   @Test
   public void ReplicatedTest() throws Exception {
      Configuration configuration = createConfiguration("replicated-hapolicy-config.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedNothingLiveActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ReplicatedPolicy);
         ReplicatedPolicy replicatedPolicy = (ReplicatedPolicy) haPolicy;
         assertEquals(replicatedPolicy.getGroupName(), "purple");
         assertTrue(replicatedPolicy.isCheckForLiveServer());
         assertEquals(replicatedPolicy.getClusterName(), "abcdefg");
         assertEquals(replicatedPolicy.getInitialReplicationSyncTimeout(), 9876);
         assertEquals(replicatedPolicy.getRetryReplicationWait(), 12345);
      } finally {
         server.stop();
      }
   }

   @Test
   public void ReplicaTest() throws Exception {
      Configuration configuration = createConfiguration("replica-hapolicy-config.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedNothingBackupActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ReplicaPolicy);
         ReplicaPolicy replicaPolicy = (ReplicaPolicy) haPolicy;
         assertEquals(replicaPolicy.getGroupName(), "tiddles");
         assertEquals(replicaPolicy.getMaxSavedReplicatedJournalsSize(), 22);
         assertEquals(replicaPolicy.getClusterName(), "33rrrrr");
         assertFalse(replicaPolicy.isRestartBackup());
         assertTrue(replicaPolicy.isAllowFailback());
         assertEquals(replicaPolicy.getInitialReplicationSyncTimeout(), 9876);
         assertEquals(replicaPolicy.getRetryReplicationWait(), 12345);
         ScaleDownPolicy scaleDownPolicy = replicaPolicy.getScaleDownPolicy();
         assertNotNull(scaleDownPolicy);
         assertEquals(scaleDownPolicy.getGroupName(), "boo!");
         assertEquals(scaleDownPolicy.getDiscoveryGroup(), "wahey");
         List<String> connectors = scaleDownPolicy.getConnectors();
         assertNotNull(connectors);
         assertEquals(connectors.size(), 0);
      } finally {
         server.stop();
      }
   }

   @Test
   public void ReplicaTest2() throws Exception {
      Configuration configuration = createConfiguration("replica-hapolicy-config2.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedNothingBackupActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ReplicaPolicy);
         ReplicaPolicy replicaPolicy = (ReplicaPolicy) haPolicy;
         assertEquals(replicaPolicy.getGroupName(), "tiddles");
         assertEquals(replicaPolicy.getMaxSavedReplicatedJournalsSize(), 22);
         assertEquals(replicaPolicy.getClusterName(), "33rrrrr");
         assertFalse(replicaPolicy.isRestartBackup());
         ScaleDownPolicy scaleDownPolicy = replicaPolicy.getScaleDownPolicy();
         assertNotNull(scaleDownPolicy);
         assertEquals(scaleDownPolicy.getGroupName(), "boo!");
         assertEquals(scaleDownPolicy.getDiscoveryGroup(), null);
         List<String> connectors = scaleDownPolicy.getConnectors();
         assertNotNull(connectors);
         assertEquals(connectors.size(), 2);
         assertTrue(connectors.contains("sd-connector1"));
         assertTrue(connectors.contains("sd-connector2"));
      } finally {
         server.stop();
      }
   }

   @Test
   public void ReplicaTest3() throws Exception {
      Configuration configuration = createConfiguration("replica-hapolicy-config3.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedNothingBackupActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ReplicaPolicy);
         ReplicaPolicy replicaPolicy = (ReplicaPolicy) haPolicy;
         assertEquals(replicaPolicy.getGroupName(), "tiddles");
         assertEquals(replicaPolicy.getMaxSavedReplicatedJournalsSize(), 22);
         assertEquals(replicaPolicy.getClusterName(), "33rrrrr");
         assertFalse(replicaPolicy.isRestartBackup());
         ScaleDownPolicy scaleDownPolicy = replicaPolicy.getScaleDownPolicy();
         assertNull(scaleDownPolicy);
      } finally {
         server.stop();
      }
   }

   @Test
   public void SharedStoreMasterTest() throws Exception {
      Configuration configuration = createConfiguration("shared-store-master-hapolicy-config.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedStoreLiveActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof SharedStoreMasterPolicy);
         SharedStoreMasterPolicy masterPolicy = (SharedStoreMasterPolicy) haPolicy;
         assertFalse(masterPolicy.isFailoverOnServerShutdown());
      } finally {
         server.stop();
      }
   }

   @Test
   public void SharedStoreSlaveTest() throws Exception {
      Configuration configuration = createConfiguration("shared-store-slave-hapolicy-config.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedStoreBackupActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof SharedStoreSlavePolicy);
         SharedStoreSlavePolicy sharedStoreSlavePolicy = (SharedStoreSlavePolicy) haPolicy;
         assertFalse(sharedStoreSlavePolicy.isFailoverOnServerShutdown());
         assertFalse(sharedStoreSlavePolicy.isRestartBackup());
         ScaleDownPolicy scaleDownPolicy = sharedStoreSlavePolicy.getScaleDownPolicy();
         assertNotNull(scaleDownPolicy);
         assertEquals(scaleDownPolicy.getGroupName(), "boo!");
         assertEquals(scaleDownPolicy.getDiscoveryGroup(), "wahey");
         List<String> connectors = scaleDownPolicy.getConnectors();
         assertNotNull(connectors);
         assertEquals(connectors.size(), 0);
      } finally {
         server.stop();
      }
   }

   @Test
   public void SharedStoreSlaveTest2() throws Exception {
      Configuration configuration = createConfiguration("shared-store-slave-hapolicy-config2.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedStoreBackupActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof SharedStoreSlavePolicy);
         SharedStoreSlavePolicy sharedStoreSlavePolicy = (SharedStoreSlavePolicy) haPolicy;
         assertTrue(sharedStoreSlavePolicy.isFailoverOnServerShutdown());
         assertTrue(sharedStoreSlavePolicy.isRestartBackup());
         ScaleDownPolicy scaleDownPolicy = sharedStoreSlavePolicy.getScaleDownPolicy();
         assertNotNull(scaleDownPolicy);
         assertEquals(scaleDownPolicy.getGroupName(), "boo!");
         assertEquals(scaleDownPolicy.getDiscoveryGroup(), null);
         List<String> connectors = scaleDownPolicy.getConnectors();
         assertNotNull(connectors);
         assertEquals(connectors.size(), 2);
         assertTrue(connectors.contains("sd-connector1"));
         assertTrue(connectors.contains("sd-connector2"));
      } finally {
         server.stop();
      }
   }

   @Test
   public void SharedStoreSlaveTest3() throws Exception {
      Configuration configuration = createConfiguration("shared-store-slave-hapolicy-config3.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedStoreBackupActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof SharedStoreSlavePolicy);
         SharedStoreSlavePolicy sharedStoreSlavePolicy = (SharedStoreSlavePolicy) haPolicy;
         assertTrue(sharedStoreSlavePolicy.isFailoverOnServerShutdown());
         assertTrue(sharedStoreSlavePolicy.isRestartBackup());
         ScaleDownPolicy scaleDownPolicy = sharedStoreSlavePolicy.getScaleDownPolicy();
         assertNull(scaleDownPolicy);
      } finally {
         server.stop();
      }
   }

   @Test
   public void colocatedTest() throws Exception {
      Configuration configuration = createConfiguration("colocated-hapolicy-config.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof ColocatedActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ColocatedPolicy);
         ColocatedPolicy colocatedPolicy = (ColocatedPolicy) haPolicy;
         ReplicatedPolicy livePolicy = (ReplicatedPolicy) colocatedPolicy.getLivePolicy();
         assertNotNull(livePolicy);

         assertEquals(livePolicy.getGroupName(), "purple");
         assertTrue(livePolicy.isCheckForLiveServer());
         assertEquals(livePolicy.getClusterName(), "abcdefg");
         ReplicaPolicy backupPolicy = (ReplicaPolicy) colocatedPolicy.getBackupPolicy();
         assertNotNull(backupPolicy);
         assertEquals(backupPolicy.getGroupName(), "tiddles");
         assertEquals(backupPolicy.getMaxSavedReplicatedJournalsSize(), 22);
         assertEquals(backupPolicy.getClusterName(), "33rrrrr");
         assertFalse(backupPolicy.isRestartBackup());
      } finally {
         server.stop();
      }
   }

   @Test
   public void colocatedTestNullBackup() throws Exception {
      Configuration configuration = createConfiguration("colocated-hapolicy-config-null-backup.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof ColocatedActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ColocatedPolicy);
         ColocatedPolicy colocatedPolicy = (ColocatedPolicy) haPolicy;
         ReplicatedPolicy livePolicy = (ReplicatedPolicy) colocatedPolicy.getLivePolicy();
         assertNotNull(livePolicy);

         assertEquals(livePolicy.getGroupName(), "purple");
         assertEquals(livePolicy.getGroupName(), livePolicy.getBackupGroupName());
         assertEquals(livePolicy.getBackupGroupName(), haPolicy.getBackupGroupName());
         assertTrue(livePolicy.isCheckForLiveServer());
         assertEquals(livePolicy.getClusterName(), "abcdefg");
         ReplicaPolicy backupPolicy = (ReplicaPolicy) colocatedPolicy.getBackupPolicy();
         assertNotNull(backupPolicy);
      } finally {
         server.stop();
      }
   }

   @Test
   public void colocatedTest2() throws Exception {
      Configuration configuration = createConfiguration("colocated-hapolicy-config2.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof ColocatedActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ColocatedPolicy);
         ColocatedPolicy colocatedPolicy = (ColocatedPolicy) haPolicy;
         SharedStoreMasterPolicy livePolicy = (SharedStoreMasterPolicy) colocatedPolicy.getLivePolicy();
         assertNotNull(livePolicy);

         assertFalse(livePolicy.isFailoverOnServerShutdown());
         SharedStoreSlavePolicy backupPolicy = (SharedStoreSlavePolicy) colocatedPolicy.getBackupPolicy();
         assertNotNull(backupPolicy);
         assertFalse(backupPolicy.isFailoverOnServerShutdown());
         assertFalse(backupPolicy.isRestartBackup());
      } finally {
         server.stop();
      }
   }

   @Test
   public void colocatedTest2nullbackup() throws Exception {
      Configuration configuration = createConfiguration("colocated-hapolicy-config2-null-backup.xml");
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof ColocatedActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ColocatedPolicy);
         ColocatedPolicy colocatedPolicy = (ColocatedPolicy) haPolicy;
         SharedStoreMasterPolicy livePolicy = (SharedStoreMasterPolicy) colocatedPolicy.getLivePolicy();
         assertNotNull(livePolicy);

         assertFalse(livePolicy.isFailoverOnServerShutdown());
         SharedStoreSlavePolicy backupPolicy = (SharedStoreSlavePolicy) colocatedPolicy.getBackupPolicy();
         assertNotNull(backupPolicy);
      } finally {
         server.stop();
      }
   }

   private void liveOnlyTest(String file) throws Exception {
      Configuration configuration = createConfiguration(file);
      ActiveMQServerImpl server = new ActiveMQServerImpl(configuration);
      try {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof LiveOnlyActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof LiveOnlyPolicy);
         LiveOnlyPolicy liveOnlyPolicy = (LiveOnlyPolicy) haPolicy;
         ScaleDownPolicy scaleDownPolicy = liveOnlyPolicy.getScaleDownPolicy();
         assertNull(scaleDownPolicy);
      } finally {
         server.stop();
      }
   }

   protected Configuration createConfiguration(String fileName) throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(fileName);
      deploymentManager.addDeployable(fc);

      deploymentManager.readConfiguration();

      // we need this otherwise the data folder will be located under activemq-server and not on the temporary directory
      fc.setPagingDirectory(getTestDir() + "/" + fc.getPagingDirectory());
      fc.setLargeMessagesDirectory(getTestDir() + "/" + fc.getLargeMessagesDirectory());
      fc.setJournalDirectory(getTestDir() + "/" + fc.getJournalDirectory());
      fc.setBindingsDirectory(getTestDir() + "/" + fc.getBindingsDirectory());

      return fc;
   }
}
