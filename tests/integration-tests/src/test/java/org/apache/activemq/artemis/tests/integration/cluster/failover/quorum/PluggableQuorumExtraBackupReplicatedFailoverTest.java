/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.cluster.failover.quorum;

import java.util.Arrays;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationPrimaryPolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTestBase;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PluggableQuorumExtraBackupReplicatedFailoverTest extends FailoverTestBase {

   private static final String GROUP_NAME = "foo";

   @Parameterized.Parameter
   public boolean useGroupName;

   @Parameterized.Parameters(name = "useGroupName={0}")
   public static Iterable<Object[]> getParams() {
      return Arrays.asList(new Object[][]{{false}, {true}});
   }

   @Override
   protected void createConfigs() throws Exception {
      createPluggableReplicatedConfigs();
   }

   @Override
   protected void setupHAPolicyConfiguration() {
      if (useGroupName) {
         ((ReplicationPrimaryPolicyConfiguration) liveConfig.getHAPolicyConfiguration()).setGroupName(GROUP_NAME);
         ((ReplicationBackupPolicyConfiguration) backupConfig.getHAPolicyConfiguration()).setGroupName(GROUP_NAME);
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

   @Test
   public void testExtraBackupReplicates() throws Exception {
      Configuration secondBackupConfig = backupConfig.copy();
      String secondBackupGroupName = ((ReplicationBackupPolicyConfiguration) secondBackupConfig.getHAPolicyConfiguration()).getGroupName();
      Assert.assertEquals(((ReplicationBackupPolicyConfiguration) backupConfig.getHAPolicyConfiguration()).getGroupName(),
                          secondBackupGroupName);
      if (useGroupName) {
         Assert.assertEquals(GROUP_NAME, secondBackupGroupName);
      } else {
         Assert.assertNull(secondBackupGroupName);
      }
      TestableServer secondBackupServer = createTestableServer(secondBackupConfig);
      secondBackupConfig.setBindingsDirectory(getBindingsDir(1, true))
         .setJournalDirectory(getJournalDir(1, true))
         .setPagingDirectory(getPageDir(1, true))
         .setLargeMessagesDirectory(getLargeMessagesDir(1, true))
         .setSecurityEnabled(false);

      waitForRemoteBackupSynchronization(backupServer.getServer());

      secondBackupServer.start();
      Thread.sleep(5000);
      backupServer.stop();
      waitForSync(secondBackupServer.getServer());
      waitForRemoteBackupSynchronization(secondBackupServer.getServer());

   }

   private void waitForSync(ActiveMQServer server) throws Exception {
      Wait.waitFor(server::isReplicaSync);
   }

}
