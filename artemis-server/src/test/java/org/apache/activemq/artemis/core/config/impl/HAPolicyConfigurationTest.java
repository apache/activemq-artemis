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

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.server.cluster.ha.ColocatedPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.LiveOnlyPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicaPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicatedPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ScaleDownPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreMasterPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreSlavePolicy;
import org.apache.activemq.artemis.core.server.impl.Activation;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ColocatedActivation;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.core.server.impl.LiveOnlyActivation;
import org.apache.activemq.artemis.core.server.impl.SharedNothingBackupActivation;
import org.apache.activemq.artemis.core.server.impl.SharedNothingLiveActivation;
import org.apache.activemq.artemis.core.server.impl.SharedStoreBackupActivation;
import org.apache.activemq.artemis.core.server.impl.SharedStoreLiveActivation;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
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
