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
package org.apache.activemq.artemis.tests.integration.security;

import java.util.Collections;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.core.config.ha.DistributedPrimitiveManagerConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationPrimaryPolicyConfiguration;
import org.apache.activemq.artemis.quorum.file.FileBasedPrimitiveManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQBasicSecurityManager;
import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTestBase;
import org.apache.activemq.artemis.tests.util.ReplicatedBackupUtils;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

public class PluggableQuorumBasicSecurityManagerFailoverTest extends FailoverTestBase {

   @Override
   protected void createConfigs() throws Exception {
      createPluggableReplicatedConfigs();
   }

   @Override
   protected void createPluggableReplicatedConfigs() throws Exception {
      final TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      final TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      final TransportConfiguration backupAcceptor = getAcceptorTransportConfiguration(false);

      backupConfig = createDefaultInVMConfig();
      liveConfig = createDefaultInVMConfig();

      DistributedPrimitiveManagerConfiguration managerConfiguration =
         new DistributedPrimitiveManagerConfiguration(FileBasedPrimitiveManager.class.getName(),
                                                      Collections.singletonMap("locks-folder",
                                                                               tmpFolder.newFolder("manager").toString()));

      ReplicatedBackupUtils.configurePluggableQuorumReplicationPair(backupConfig, backupConnector, backupAcceptor,
                                                                    liveConfig, liveConnector, null,
                                                                    managerConfiguration, managerConfiguration);

      backupConfig
         .setSecurityEnabled(true)
         .setBindingsDirectory(getBindingsDir(0, true))
         .setJournalDirectory(getJournalDir(0, true))
         .setPagingDirectory(getPageDir(0, true))
         .setLargeMessagesDirectory(getLargeMessagesDir(0, true));

      setupHAPolicyConfiguration();
      backupNodeManager = createReplicatedBackupNodeManager(backupConfig);

      backupServer = createTestableServer(backupConfig, backupNodeManager);

      backupServer.getServer().setSecurityManager(new ActiveMQBasicSecurityManager());

      liveConfig
         .setSecurityEnabled(true)
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(getAcceptorTransportConfiguration(true));

      nodeManager = createNodeManager(liveConfig);
      liveServer = createTestableServer(liveConfig, nodeManager);

      liveServer.getServer().setSecurityManager(new ActiveMQBasicSecurityManager());
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

   @Override
   protected void setupHAPolicyConfiguration() {
      ((ReplicationPrimaryPolicyConfiguration) liveConfig.getHAPolicyConfiguration()).setCheckForLiveServer(true);
      ((ReplicationBackupPolicyConfiguration) backupConfig.getHAPolicyConfiguration()).setMaxSavedReplicatedJournalsSize(2).setAllowFailBack(true);
   }

   @Test
   public void testFailover() throws Exception {

      liveServer.getServer().getActiveMQServerControl().addUser("foo", "bar", "baz", false);

      ClientSessionFactory cf = createSessionFactory(getServerLocator());
      ClientSession session = null;

      try {
         session = cf.createSession("foo", "bar", false, true, true, false, 0);
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception");
      }

      crash(session);
      waitForServerToStart(backupServer.getServer());

      try {
         cf = createSessionFactory(getServerLocator());
         session = cf.createSession("foo", "bar", false, true, true, false, 0);
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception");
      }
   }
}

