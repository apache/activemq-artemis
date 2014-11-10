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
package org.hornetq.core.config.impl;

import org.hornetq.core.server.cluster.ha.ColocatedPolicy;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.core.server.cluster.ha.LiveOnlyPolicy;
import org.hornetq.core.server.cluster.ha.ReplicaPolicy;
import org.hornetq.core.server.cluster.ha.ReplicatedPolicy;
import org.hornetq.core.server.cluster.ha.ScaleDownPolicy;
import org.hornetq.core.server.cluster.ha.SharedStoreMasterPolicy;
import org.hornetq.core.server.cluster.ha.SharedStoreSlavePolicy;
import org.hornetq.core.server.impl.ColocatedActivation;
import org.hornetq.core.server.impl.LiveOnlyActivation;
import org.hornetq.core.server.impl.SharedNothingBackupActivation;
import org.hornetq.core.server.impl.SharedNothingLiveActivation;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.impl.Activation;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.SharedStoreBackupActivation;
import org.hornetq.core.server.impl.SharedStoreLiveActivation;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.Test;

import java.util.List;

public class HAPolicyConfigurationTest extends UnitTestCase
{
   @Test
   public void liveOnlyTest() throws Exception
   {
      Configuration configuration = createConfiguration("live-only-hapolicy-config.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
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
      }
      finally
      {
         server.stop();
      }
   }

   @Test
   public void liveOnlyTest2() throws Exception
   {
      Configuration configuration = createConfiguration("live-only-hapolicy-config2.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
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
      }
      finally
      {
         server.stop();
      }
   }

   @Test
   public void liveOnlyTest3() throws Exception
   {
      liveOnlyTest("live-only-hapolicy-config3.xml");
   }

   @Test
   public void liveOnlyTest4() throws Exception
   {
      liveOnlyTest("live-only-hapolicy-config4.xml");
   }
   @Test
   public void liveOnlyTest5() throws Exception
   {
      liveOnlyTest("live-only-hapolicy-config5.xml");
   }

   @Test
   public void ReplicatedTest() throws Exception
   {
      Configuration configuration = createConfiguration("replicated-hapolicy-config.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedNothingLiveActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ReplicatedPolicy);
         ReplicatedPolicy replicatedPolicy = (ReplicatedPolicy) haPolicy;
         assertEquals(replicatedPolicy.getGroupName(), "purple");
         assertTrue(replicatedPolicy.isCheckForLiveServer());
         assertEquals(replicatedPolicy.getClusterName(), "abcdefg");
      }
      finally
      {
         server.stop();
      }
   }

   @Test
   public void ReplicaTest() throws Exception
   {
      Configuration configuration = createConfiguration("replica-hapolicy-config.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
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
         assertEquals(scaleDownPolicy.getDiscoveryGroup(), "wahey");
         List<String> connectors = scaleDownPolicy.getConnectors();
         assertNotNull(connectors);
         assertEquals(connectors.size(), 0);
      }
      finally
      {
         server.stop();
      }
   }

   @Test
   public void ReplicaTest2() throws Exception
   {
      Configuration configuration = createConfiguration("replica-hapolicy-config2.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
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
      }
      finally
      {
         server.stop();
      }
   }

   @Test
   public void ReplicaTest3() throws Exception
   {
      Configuration configuration = createConfiguration("replica-hapolicy-config3.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
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
      }
      finally
      {
         server.stop();
      }
   }

   @Test
   public void SharedStoreMasterTest() throws Exception
   {
      Configuration configuration = createConfiguration("shared-store-master-hapolicy-config.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedStoreLiveActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof SharedStoreMasterPolicy);
         SharedStoreMasterPolicy masterPolicy = (SharedStoreMasterPolicy) haPolicy;
         assertEquals(masterPolicy.getFailbackDelay(), 3456);
         assertFalse(masterPolicy.isFailoverOnServerShutdown());
      }
      finally
      {
         server.stop();
      }
   }

   @Test
   public void SharedStoreSlaveTest() throws Exception
   {
      Configuration configuration = createConfiguration("shared-store-slave-hapolicy-config.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedStoreBackupActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof SharedStoreSlavePolicy);
         SharedStoreSlavePolicy replicaPolicy = (SharedStoreSlavePolicy) haPolicy;
         assertEquals(replicaPolicy.getFailbackDelay(), 9876);
         assertFalse(replicaPolicy.isFailoverOnServerShutdown());
         assertFalse(replicaPolicy.isRestartBackup());
         ScaleDownPolicy scaleDownPolicy = replicaPolicy.getScaleDownPolicy();
         assertNotNull(scaleDownPolicy);
         assertEquals(scaleDownPolicy.getGroupName(), "boo!");
         assertEquals(scaleDownPolicy.getDiscoveryGroup(), "wahey");
         List<String> connectors = scaleDownPolicy.getConnectors();
         assertNotNull(connectors);
         assertEquals(connectors.size(), 0);
      }
      finally
      {
         server.stop();
      }
   }

   @Test
   public void SharedStoreSlaveTest2() throws Exception
   {
      Configuration configuration = createConfiguration("shared-store-slave-hapolicy-config2.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedStoreBackupActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof SharedStoreSlavePolicy);
         SharedStoreSlavePolicy replicaPolicy = (SharedStoreSlavePolicy) haPolicy;
         assertEquals(replicaPolicy.getFailbackDelay(), 5678);
         assertTrue(replicaPolicy.isFailoverOnServerShutdown());
         assertTrue(replicaPolicy.isRestartBackup());
         ScaleDownPolicy scaleDownPolicy = replicaPolicy.getScaleDownPolicy();
         assertNotNull(scaleDownPolicy);
         assertEquals(scaleDownPolicy.getGroupName(), "boo!");
         assertEquals(scaleDownPolicy.getDiscoveryGroup(), null);
         List<String> connectors = scaleDownPolicy.getConnectors();
         assertNotNull(connectors);
         assertEquals(connectors.size(), 2);
         assertTrue(connectors.contains("sd-connector1"));
         assertTrue(connectors.contains("sd-connector2"));
      }
      finally
      {
         server.stop();
      }
   }

   @Test
   public void SharedStoreSlaveTest3() throws Exception
   {
      Configuration configuration = createConfiguration("shared-store-slave-hapolicy-config3.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof SharedStoreBackupActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof SharedStoreSlavePolicy);
         SharedStoreSlavePolicy replicaPolicy = (SharedStoreSlavePolicy) haPolicy;
         assertEquals(replicaPolicy.getFailbackDelay(), 5678);
         assertTrue(replicaPolicy.isFailoverOnServerShutdown());
         assertTrue(replicaPolicy.isRestartBackup());
         ScaleDownPolicy scaleDownPolicy = replicaPolicy.getScaleDownPolicy();
         assertNull(scaleDownPolicy);
      }
      finally
      {
         server.stop();
      }
   }

   @Test
   public void colocatedTest() throws Exception
   {
      Configuration configuration = createConfiguration("colocated-hapolicy-config.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
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
      }
      finally
      {
         server.stop();
      }
   }


   @Test
   public void colocatedTest2() throws Exception
   {
      Configuration configuration = createConfiguration("colocated-hapolicy-config2.xml");
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof ColocatedActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof ColocatedPolicy);
         ColocatedPolicy colocatedPolicy = (ColocatedPolicy) haPolicy;
         SharedStoreMasterPolicy livePolicy = (SharedStoreMasterPolicy) colocatedPolicy.getLivePolicy();
         assertNotNull(livePolicy);

         assertFalse(livePolicy.isFailoverOnServerShutdown());
         assertEquals(livePolicy.getFailbackDelay(), 1234);
         SharedStoreSlavePolicy backupPolicy = (SharedStoreSlavePolicy) colocatedPolicy.getBackupPolicy();
         assertNotNull(backupPolicy);
         assertEquals(backupPolicy.getFailbackDelay(), 44);
         assertFalse(backupPolicy.isFailoverOnServerShutdown());
         assertFalse(backupPolicy.isRestartBackup());
      }
      finally
      {
         server.stop();
      }
   }

   private void liveOnlyTest(String file) throws Exception
   {
      Configuration configuration = createConfiguration(file);
      HornetQServerImpl server = new HornetQServerImpl(configuration);
      try
      {
         server.start();
         Activation activation = server.getActivation();
         assertTrue(activation instanceof LiveOnlyActivation);
         HAPolicy haPolicy = server.getHAPolicy();
         assertTrue(haPolicy instanceof LiveOnlyPolicy);
         LiveOnlyPolicy liveOnlyPolicy = (LiveOnlyPolicy) haPolicy;
         ScaleDownPolicy scaleDownPolicy = liveOnlyPolicy.getScaleDownPolicy();
         assertNull(scaleDownPolicy);
      }
      finally
      {
         server.stop();
      }
   }


   protected Configuration createConfiguration(String fileName) throws Exception
   {
      FileConfiguration fc = new FileConfiguration(fileName);

      fc.start();

      // we need this otherwise the data folder will be located under hornetq-server and not on the temporary directory
      fc.setPagingDirectory(getTestDir() + "/" + fc.getPagingDirectory());
      fc.setLargeMessagesDirectory(getTestDir() + "/" + fc.getLargeMessagesDirectory());
      fc.setJournalDirectory(getTestDir() + "/" + fc.getJournalDirectory());
      fc.setBindingsDirectory(getTestDir() + "/" + fc.getBindingsDirectory());

      return fc;
   }
}
