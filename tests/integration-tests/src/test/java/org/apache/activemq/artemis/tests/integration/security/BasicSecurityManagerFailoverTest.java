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

import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.spi.core.security.ActiveMQBasicSecurityManager;
import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTestBase;
import org.apache.activemq.artemis.tests.util.ReplicatedBackupUtils;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class BasicSecurityManagerFailoverTest extends FailoverTestBase {

   private boolean replicated;

   @Parameterized.Parameters(name = "replicated={0}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{true}, {false}});
   }

   public BasicSecurityManagerFailoverTest(boolean replicated) {
      this.replicated = replicated;
   }

   @Override
   protected void createConfigs() throws Exception {
      if (replicated) {
         createReplicatedConfigs();
      } else {
         createSharedStoreConfigs();
      }
   }

   protected void createSharedStoreConfigs() throws Exception {
      nodeManager = createNodeManager();
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);

      backupConfig = super
         .createDefaultInVMConfig()
         .setSecurityEnabled(true)
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(getAcceptorTransportConfiguration(false))
         .setHAPolicyConfiguration(new SharedStoreSlavePolicyConfiguration())
         .addConnectorConfiguration(liveConnector.getName(), liveConnector)
         .addConnectorConfiguration(backupConnector.getName(), backupConnector)
         .addClusterConfiguration(createBasicClusterConfig(backupConnector.getName(), liveConnector.getName()));

      backupServer = createTestableServer(backupConfig);

      backupServer.getServer().setSecurityManager(new ActiveMQBasicSecurityManager());

      liveConfig = super
         .createDefaultInVMConfig()
         .setSecurityEnabled(true)
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(getAcceptorTransportConfiguration(true))
         .setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration())
         .addClusterConfiguration(createBasicClusterConfig(liveConnector.getName()))
         .addConnectorConfiguration(liveConnector.getName(), liveConnector);

      liveServer = createTestableServer(liveConfig);

      liveServer.getServer().setSecurityManager(new ActiveMQBasicSecurityManager());
   }

   @Override
   protected void createReplicatedConfigs() throws Exception {
      final TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      final TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      final TransportConfiguration backupAcceptor = getAcceptorTransportConfiguration(false);

      backupConfig = createDefaultInVMConfig();
      liveConfig = createDefaultInVMConfig();

      ReplicatedBackupUtils.configureReplicationPair(backupConfig, backupConnector, backupAcceptor, liveConfig, liveConnector, null);

      backupConfig
         .setSecurityEnabled(true)
         .setBindingsDirectory(getBindingsDir(0, true))
         .setJournalDirectory(getJournalDir(0, true))
         .setPagingDirectory(getPageDir(0, true))
         .setLargeMessagesDirectory(getLargeMessagesDir(0, true));

      setupHAPolicyConfiguration();
      nodeManager = createReplicatedBackupNodeManager(backupConfig);

      backupServer = createTestableServer(backupConfig);

      backupServer.getServer().setSecurityManager(new ActiveMQBasicSecurityManager());

      liveConfig
         .setSecurityEnabled(true)
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(getAcceptorTransportConfiguration(true));

      liveServer = createTestableServer(liveConfig);

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
