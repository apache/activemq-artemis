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
package org.hornetq.tests.util;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.ha.ReplicaPolicyConfiguration;
import org.hornetq.core.config.ha.ReplicatedPolicyConfiguration;

public final class ReplicatedBackupUtils
{
   public static final String LIVE_NODE_NAME = "hqLIVE";
   public static final String BACKUP_NODE_NAME = "hqBackup";
   private ReplicatedBackupUtils()
   {
      // Utility class
   }

   public static void configureReplicationPair(Configuration backupConfig,
                                               TransportConfiguration backupConnector,
                                               TransportConfiguration backupAcceptor,
                                               Configuration liveConfig,
                                               TransportConfiguration liveConnector)
   {
      if (backupAcceptor != null)
      {
         backupConfig.clearAcceptorConfigurations().addAcceptorConfiguration(backupAcceptor);
      }

      backupConfig.addConnectorConfiguration(BACKUP_NODE_NAME, backupConnector)
         .addConnectorConfiguration(LIVE_NODE_NAME, liveConnector)
         .addClusterConfiguration(UnitTestCase.basicClusterConnectionConfig(BACKUP_NODE_NAME, LIVE_NODE_NAME))
         .setHAPolicyConfiguration(new ReplicaPolicyConfiguration());

      liveConfig.setName(LIVE_NODE_NAME)
         .addConnectorConfiguration(LIVE_NODE_NAME, liveConnector)
         .addConnectorConfiguration(BACKUP_NODE_NAME, backupConnector)
         .setSecurityEnabled(false)
         .addClusterConfiguration(UnitTestCase.basicClusterConnectionConfig(LIVE_NODE_NAME, BACKUP_NODE_NAME))
         .setHAPolicyConfiguration(new ReplicatedPolicyConfiguration());
   }
}
