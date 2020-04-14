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
package org.apache.activemq.artemis.tests.integration.cluster.failover.quorum;

import java.io.IOException;
import java.util.Collections;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.DistributedPrimitiveManagerConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationPrimaryPolicyConfiguration;
import org.apache.activemq.artemis.quorum.file.FileBasedPrimitiveManager;
import org.apache.activemq.artemis.tests.integration.cluster.failover.ReplicaTimeoutTest;
import org.apache.activemq.artemis.tests.util.ReplicatedBackupUtils;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class PluggableQuorumReplicaTimeoutTest extends ReplicaTimeoutTest {

   @Rule
   public TemporaryFolder tmpFolder = new TemporaryFolder();

   @Override
   protected void configureReplicationPair(Configuration backupConfig,
                                           Configuration liveConfig,
                                           TransportConfiguration backupConnector,
                                           TransportConfiguration backupAcceptor,
                                           TransportConfiguration liveConnector) throws IOException {
      DistributedPrimitiveManagerConfiguration managerConfiguration = new DistributedPrimitiveManagerConfiguration(FileBasedPrimitiveManager.class.getName(), Collections.singletonMap("locks-folder", tmpFolder.newFolder("manager").toString()));

      ReplicatedBackupUtils.configurePluggableQuorumReplicationPair(backupConfig, backupConnector, backupAcceptor,
                                                                    liveConfig, liveConnector, null,
                                                                    managerConfiguration, managerConfiguration);
      ReplicationPrimaryPolicyConfiguration primaryConfiguration = ((ReplicationPrimaryPolicyConfiguration) liveConfig.getHAPolicyConfiguration());
      primaryConfiguration.setInitialReplicationSyncTimeout(1000);
      primaryConfiguration.setCheckForLiveServer(true);
      ReplicationBackupPolicyConfiguration backupConfiguration = ((ReplicationBackupPolicyConfiguration) backupConfig.getHAPolicyConfiguration());
      backupConfiguration.setInitialReplicationSyncTimeout(1000);
      backupConfiguration.setMaxSavedReplicatedJournalsSize(2)
         .setAllowFailBack(true);
   }

   @Override
   protected boolean expectLiveSuicide() {
      return false;
   }
}
