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
import org.apache.activemq.artemis.core.config.ScaleDownConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

public class SharedStoreBackupTest extends FailoverTestBase {

   @Test
   public void testStartSharedBackupWithScalingDownPolicyDisabled() throws Exception {
      liveServer.stop();
      // wait max 10s for backup to activate
      Assert.assertTrue("Backup did not activate in 10s timeout.", waitForBackupToBecomeActive(backupServer, 10000));
   }

   /**
    * Returns true if backup started in given timeout. False otherwise.
    *
    * @param backupServer backup server
    * @param waitTimeout  timeout in milliseconds
    * @return returns true if backup started in given timeout. False otherwise
    */
   private boolean waitForBackupToBecomeActive(TestableServer backupServer, long waitTimeout) throws Exception {

      long startTime = System.currentTimeMillis();
      boolean isBackupStarted;
      // wait given timeout for backup to activate
      while (!(isBackupStarted = backupServer.isActive()) && System.currentTimeMillis() - startTime < waitTimeout) {
         Thread.sleep(300);
      }
      return isBackupStarted;
   }

   @Override
   protected void createConfigs() throws Exception {
      nodeManager = new InVMNodeManager(false);
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      backupConfig = super.createDefaultConfig(false).clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(false)).setHAPolicyConfiguration(new SharedStoreSlavePolicyConfiguration().setScaleDownConfiguration(new ScaleDownConfiguration().setEnabled(false)).setRestartBackup(false)).addConnectorConfiguration(liveConnector.getName(), liveConnector).addConnectorConfiguration(backupConnector.getName(), backupConnector).addClusterConfiguration(basicClusterConnectionConfig(backupConnector.getName(), liveConnector.getName()));

      backupServer = createTestableServer(backupConfig);

      liveConfig = super.createDefaultConfig(false).clearAcceptorConfigurations().addAcceptorConfiguration(getAcceptorTransportConfiguration(true)).setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration().setFailoverOnServerShutdown(true)).addClusterConfiguration(basicClusterConnectionConfig(liveConnector.getName())).addConnectorConfiguration(liveConnector.getName(), liveConnector);

      liveServer = createTestableServer(liveConfig);
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
