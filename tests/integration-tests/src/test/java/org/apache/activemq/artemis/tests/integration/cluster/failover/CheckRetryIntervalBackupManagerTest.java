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
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.server.cluster.BackupManager;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.tests.util.CountDownSessionFailureListener;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.apache.activemq.artemis.utils.Wait;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

public class CheckRetryIntervalBackupManagerTest extends FailoverTestBase {

   private static final Logger log = Logger.getLogger(CheckRetryIntervalBackupManagerTest.class);

   private volatile CountDownSessionFailureListener listener;

   private volatile ClientSessionFactoryInternal sf;

   private final Object lockFail = new Object();

   @Override
   protected void createConfigs() throws Exception {
      nodeManager = createNodeManager();
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);

      backupConfig = super.createDefaultInVMConfig().clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(false)).setHAPolicyConfiguration(new SharedStoreSlavePolicyConfiguration()).addConnectorConfiguration(liveConnector.getName(), liveConnector).addConnectorConfiguration(backupConnector.getName(), backupConnector).addClusterConfiguration(createBasicClusterConfig(backupConnector.getName(), liveConnector.getName()).setRetryInterval(333));

      backupServer = createTestableServer(backupConfig);

      liveConfig = super.createDefaultInVMConfig().clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(true)).setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration()).addClusterConfiguration(createBasicClusterConfig(liveConnector.getName()).setRetryInterval(333)).addConnectorConfiguration(liveConnector.getName(), liveConnector);

      liveServer = createTestableServer(liveConfig);
   }

   @Test
   public void testValidateRetryInterval() {
      ActiveMQServerImpl server = (ActiveMQServerImpl) backupServer.getServer();
      for (BackupManager.BackupConnector backupConnector : server.getBackupManager().getBackupConnectors()) {

         Wait.assertTrue(() -> backupConnector.getBackupServerLocator() != null);
         Assert.assertEquals(333, backupConnector.getBackupServerLocator().getRetryInterval());
         Assert.assertEquals(-1, backupConnector.getBackupServerLocator().getReconnectAttempts());
      }
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

}
