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
package org.apache.activemq.artemis.tests.util;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.DistributedPrimitiveManagerConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationPrimaryPolicyConfiguration;

public final class ReplicatedBackupUtils {

   public static final String LIVE_NODE_NAME = "amqLIVE";
   public static final String BACKUP_NODE_NAME = "amqBackup";

   private ReplicatedBackupUtils() {
      // Utility class
   }

   public static void configureReplicationPair(Configuration backupConfig,
                                               TransportConfiguration backupConnector,
                                               TransportConfiguration backupAcceptor,
                                               Configuration liveConfig,
                                               TransportConfiguration liveConnector,
                                               TransportConfiguration liveAcceptor) {
      if (backupAcceptor != null) {
         backupConfig.clearAcceptorConfigurations().addAcceptorConfiguration(backupAcceptor);
      }

      if (liveAcceptor != null) {
         liveConfig.clearAcceptorConfigurations().addAcceptorConfiguration(liveAcceptor);
      }

      backupConfig.addConnectorConfiguration(BACKUP_NODE_NAME, backupConnector).addConnectorConfiguration(LIVE_NODE_NAME, liveConnector).addClusterConfiguration(ActiveMQTestBase.basicClusterConnectionConfig(BACKUP_NODE_NAME, LIVE_NODE_NAME)).setHAPolicyConfiguration(new ReplicaPolicyConfiguration());

      liveConfig.setName(LIVE_NODE_NAME).addConnectorConfiguration(LIVE_NODE_NAME, liveConnector).addConnectorConfiguration(BACKUP_NODE_NAME, backupConnector).setSecurityEnabled(false).addClusterConfiguration(ActiveMQTestBase.basicClusterConnectionConfig(LIVE_NODE_NAME, BACKUP_NODE_NAME)).setHAPolicyConfiguration(new ReplicatedPolicyConfiguration());
   }


   public static void configurePluggableQuorumReplicationPair(Configuration backupConfig,
                                               TransportConfiguration backupConnector,
                                               TransportConfiguration backupAcceptor,
                                               Configuration liveConfig,
                                               TransportConfiguration liveConnector,
                                               TransportConfiguration liveAcceptor,
                                               DistributedPrimitiveManagerConfiguration primaryManagerConfiguration,
                                               DistributedPrimitiveManagerConfiguration backupManagerConfiguration) {
      if (backupAcceptor != null) {
         backupConfig.clearAcceptorConfigurations().addAcceptorConfiguration(backupAcceptor);
      }

      if (liveAcceptor != null) {
         liveConfig.clearAcceptorConfigurations().addAcceptorConfiguration(liveAcceptor);
      }

      backupConfig.addConnectorConfiguration(BACKUP_NODE_NAME, backupConnector).addConnectorConfiguration(LIVE_NODE_NAME, liveConnector).addClusterConfiguration(ActiveMQTestBase.basicClusterConnectionConfig(BACKUP_NODE_NAME, LIVE_NODE_NAME))
         .setHAPolicyConfiguration(ReplicationBackupPolicyConfiguration.withDefault()
                                      .setDistributedManagerConfiguration(backupManagerConfiguration));

      liveConfig.setName(LIVE_NODE_NAME).addConnectorConfiguration(LIVE_NODE_NAME, liveConnector).addConnectorConfiguration(BACKUP_NODE_NAME, backupConnector).setSecurityEnabled(false).addClusterConfiguration(ActiveMQTestBase.basicClusterConnectionConfig(LIVE_NODE_NAME, BACKUP_NODE_NAME))
         .setHAPolicyConfiguration(ReplicationPrimaryPolicyConfiguration.withDefault()
                                      .setDistributedManagerConfiguration(primaryManagerConfiguration));
   }
}
