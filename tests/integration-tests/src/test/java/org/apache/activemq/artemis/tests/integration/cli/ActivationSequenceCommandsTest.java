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
package org.apache.activemq.artemis.tests.integration.cli;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.cli.commands.activation.ActivationSequenceList;
import org.apache.activemq.artemis.cli.commands.activation.ActivationSequenceSet;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.DistributedPrimitiveManagerConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationPrimaryPolicyConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.MutableLong;
import org.apache.activemq.artemis.quorum.file.FileBasedPrimitiveManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ActivationSequenceCommandsTest extends ActiveMQTestBase {

   @Rule
   public TemporaryFolder brokersFolder = new TemporaryFolder();

   protected DistributedPrimitiveManagerConfiguration managerConfiguration;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      managerConfiguration = new DistributedPrimitiveManagerConfiguration(FileBasedPrimitiveManager.class.getName(),
                                                                          Collections.singletonMap("locks-folder", temporaryFolder.newFolder("manager").toString()));
   }

   @After
   @Override
   public void tearDown() throws Exception {
      super.tearDown();
   }

   protected Configuration createPrimaryConfiguration() throws Exception {
      Configuration conf = new ConfigurationImpl();
      conf.setName("localhost::primary");

      File primaryDir = brokersFolder.newFolder("primary");
      conf.setBrokerInstance(primaryDir);

      conf.addAcceptorConfiguration("primary", "tcp://localhost:61616");
      conf.addConnectorConfiguration("backup", "tcp://localhost:61617");
      conf.addConnectorConfiguration("primary", "tcp://localhost:61616");

      conf.setClusterUser("mycluster");
      conf.setClusterPassword("mypassword");

      conf.setHAPolicyConfiguration(createReplicationPrimaryConfiguration());

      ClusterConnectionConfiguration ccconf = new ClusterConnectionConfiguration();
      ccconf.setStaticConnectors(new ArrayList<>()).getStaticConnectors().add("backup");
      ccconf.setName("cluster");
      ccconf.setConnectorName("primary");
      conf.addClusterConfiguration(ccconf);

      conf.setSecurityEnabled(false).setJMXManagementEnabled(false).setJournalType(JournalType.MAPPED).setJournalFileSize(1024 * 512).setConnectionTTLOverride(60_000L);

      return conf;
   }

   protected Configuration createBackupConfiguration() throws Exception {
      Configuration conf = new ConfigurationImpl();
      conf.setName("localhost::backup");

      File backupDir = brokersFolder.newFolder("backup");
      conf.setBrokerInstance(backupDir);

      conf.setHAPolicyConfiguration(createReplicationBackupConfiguration());

      conf.addAcceptorConfiguration("backup", "tcp://localhost:61617");
      conf.addConnectorConfiguration("live", "tcp://localhost:61616");
      conf.addConnectorConfiguration("backup", "tcp://localhost:61617");

      conf.setClusterUser("mycluster");
      conf.setClusterPassword("mypassword");

      ClusterConnectionConfiguration ccconf = new ClusterConnectionConfiguration();
      ccconf.setStaticConnectors(new ArrayList<>()).getStaticConnectors().add("live");
      ccconf.setName("cluster");
      ccconf.setConnectorName("backup");
      conf.addClusterConfiguration(ccconf);

      conf.setSecurityEnabled(false).setJMXManagementEnabled(false).setJournalType(JournalType.MAPPED).setJournalFileSize(1024 * 512).setConnectionTTLOverride(60_000L);

      return conf;
   }

   protected HAPolicyConfiguration createReplicationPrimaryConfiguration() {
      ReplicationPrimaryPolicyConfiguration haPolicy = ReplicationPrimaryPolicyConfiguration.withDefault();
      haPolicy.setDistributedManagerConfiguration(managerConfiguration);
      return haPolicy;
   }

   protected HAPolicyConfiguration createReplicationBackupConfiguration() {
      ReplicationBackupPolicyConfiguration haPolicy = ReplicationBackupPolicyConfiguration.withDefault();
      haPolicy.setDistributedManagerConfiguration(managerConfiguration);
      haPolicy.setClusterName("cluster");
      return haPolicy;
   }

   @Test
   public void restorePrimaryCoordinatedSequence() throws Exception {
      // start live
      final Configuration primaryConfiguration = createPrimaryConfiguration();
      final ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(primaryConfiguration));
      liveServer.setIdentity("PRIMARY");
      liveServer.start();
      Wait.waitFor(liveServer::isStarted);
      final String nodeID = liveServer.getNodeID().toString();
      liveServer.stop();
      restoreCoordinatedSequence(primaryConfiguration, liveServer, nodeID, 1);
   }

   @Test
   public void restoreBackupCoordinatedSequence() throws Exception {
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);

      // start live
      Configuration liveConfiguration = createPrimaryConfiguration();

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

      final String nodeId = backupServer.getNodeManager().getNodeId().toString();

      backupServer.stop();
      restoreCoordinatedSequence(backupConfiguration, backupServer, nodeId, 2);
   }

   @Test
   public void restorePeerCoordinatedSequence() throws Exception {      // start live
      final Configuration primaryConfiguration = createPrimaryConfiguration();
      ((ReplicationPrimaryPolicyConfiguration) primaryConfiguration.getHAPolicyConfiguration()).setCoordinationId("peer-id");
      final ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(primaryConfiguration));
      liveServer.setIdentity("PRIMARY");
      liveServer.start();
      Wait.waitFor(liveServer::isStarted);
      final String nodeID = liveServer.getNodeID().toString();
      liveServer.stop();
      restoreCoordinatedSequence(primaryConfiguration, liveServer, nodeID, 1);
   }

   private void restoreCoordinatedSequence(Configuration primaryConfiguration,
                                           ActiveMQServer liveServer,
                                           String nodeID,
                                           final long expectedStartCoordinatedSequence) throws Exception {
      final ActivationSequenceList sequenceList = new ActivationSequenceList();
      ActivationSequenceList.ListResult list = ActivationSequenceList.execute(sequenceList, primaryConfiguration, null);
      Assert.assertEquals(expectedStartCoordinatedSequence, list.coordinatedActivationSequence.longValue());
      Assert.assertEquals(expectedStartCoordinatedSequence, list.localActivationSequence.longValue());
      try (DistributedPrimitiveManager distributedPrimitiveManager = DistributedPrimitiveManager
         .newInstanceOf(managerConfiguration.getClassName(), managerConfiguration.getProperties())) {
         distributedPrimitiveManager.start();
         try (DistributedLock lock = distributedPrimitiveManager.getDistributedLock(nodeID);
              MutableLong coordinatedActivationSequence = distributedPrimitiveManager.getMutableLong(nodeID)) {
            Assert.assertTrue(lock.tryLock());
            final long activationSequence = coordinatedActivationSequence.get();
            Assert.assertEquals(expectedStartCoordinatedSequence, activationSequence);
            coordinatedActivationSequence.set(0);
         }
         sequenceList.remote = true;
         Assert.assertEquals(0, ActivationSequenceList.execute(sequenceList, primaryConfiguration, null)
            .coordinatedActivationSequence.longValue());
         final ActivationSequenceSet sequenceSet = new ActivationSequenceSet();
         sequenceSet.remote = true;
         sequenceSet.value = expectedStartCoordinatedSequence;
         ActivationSequenceSet.execute(sequenceSet, primaryConfiguration, null);
         liveServer.start();
         Wait.waitFor(liveServer::isStarted);
         Assert.assertTrue(liveServer.isActive());
         Assert.assertEquals(expectedStartCoordinatedSequence + 1, liveServer.getNodeManager().getNodeActivationSequence());
         liveServer.stop();
      }
   }
}
