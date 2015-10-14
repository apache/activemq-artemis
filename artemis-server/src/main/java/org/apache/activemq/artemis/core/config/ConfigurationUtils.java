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
package org.apache.activemq.artemis.core.config;

import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.core.config.ha.ColocatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.LiveOnlyPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.cluster.ha.BackupPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ColocatedPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.LiveOnlyPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicaPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicatedPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ScaleDownPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreMasterPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreSlavePolicy;

public final class ConfigurationUtils {

   private ConfigurationUtils() {
      // Utility class
   }

   public static ClusterConnectionConfiguration getReplicationClusterConfiguration(Configuration conf,
                                                                                   String replicationCluster) throws ActiveMQIllegalStateException {
      if (replicationCluster == null || replicationCluster.isEmpty())
         return conf.getClusterConfigurations().get(0);
      for (ClusterConnectionConfiguration clusterConf : conf.getClusterConfigurations()) {
         if (replicationCluster.equals(clusterConf.getName()))
            return clusterConf;
      }
      throw new ActiveMQIllegalStateException("Missing cluster-configuration for replication-clustername '" + replicationCluster + "'.");
   }

   public static HAPolicy getHAPolicy(HAPolicyConfiguration conf) throws ActiveMQIllegalStateException {
      if (conf == null) {
         return new LiveOnlyPolicy();
      }

      switch (conf.getType()) {
         case LIVE_ONLY: {
            LiveOnlyPolicyConfiguration pc = (LiveOnlyPolicyConfiguration) conf;
            return new LiveOnlyPolicy(getScaleDownPolicy(pc.getScaleDownConfiguration()));
         }
         case REPLICATED: {
            ReplicatedPolicyConfiguration pc = (ReplicatedPolicyConfiguration) conf;
            return new ReplicatedPolicy(pc.isCheckForLiveServer(), pc.getGroupName(), pc.getClusterName(), pc.getInitialReplicationSyncTimeout());
         }
         case REPLICA: {
            ReplicaPolicyConfiguration pc = (ReplicaPolicyConfiguration) conf;
            return new ReplicaPolicy(pc.getClusterName(), pc.getMaxSavedReplicatedJournalsSize(), pc.getGroupName(), pc.isRestartBackup(), pc.isAllowFailBack(), pc.getInitialReplicationSyncTimeout(), getScaleDownPolicy(pc.getScaleDownConfiguration()));
         }
         case SHARED_STORE_MASTER: {
            SharedStoreMasterPolicyConfiguration pc = (SharedStoreMasterPolicyConfiguration) conf;
            return new SharedStoreMasterPolicy(pc.isFailoverOnServerShutdown());
         }
         case SHARED_STORE_SLAVE: {
            SharedStoreSlavePolicyConfiguration pc = (SharedStoreSlavePolicyConfiguration) conf;
            return new SharedStoreSlavePolicy(pc.isFailoverOnServerShutdown(), pc.isRestartBackup(), pc.isAllowFailBack(), getScaleDownPolicy(pc.getScaleDownConfiguration()));
         }
         case COLOCATED: {
            ColocatedPolicyConfiguration pc = (ColocatedPolicyConfiguration) conf;

            HAPolicyConfiguration backupConf = pc.getBackupConfig();
            BackupPolicy backupPolicy;
            if (backupConf == null) {
               backupPolicy = new ReplicaPolicy();
            }
            else {
               backupPolicy = (BackupPolicy) getHAPolicy(backupConf);
            }
            HAPolicyConfiguration liveConf = pc.getLiveConfig();
            HAPolicy livePolicy;
            if (liveConf == null) {
               livePolicy = new ReplicatedPolicy();
            }
            else {
               livePolicy = getHAPolicy(liveConf);
            }
            return new ColocatedPolicy(pc.isRequestBackup(), pc.getBackupRequestRetries(), pc.getBackupRequestRetryInterval(), pc.getMaxBackups(), pc.getBackupPortOffset(), pc.getExcludedConnectors(), livePolicy, backupPolicy);
         }

      }
      throw ActiveMQMessageBundle.BUNDLE.unsupportedHAPolicyConfiguration(conf);
   }

   public static ScaleDownPolicy getScaleDownPolicy(ScaleDownConfiguration scaleDownConfiguration) {
      if (scaleDownConfiguration != null) {
         if (scaleDownConfiguration.getDiscoveryGroup() != null) {
            return new ScaleDownPolicy(scaleDownConfiguration.getDiscoveryGroup(), scaleDownConfiguration.getGroupName(), scaleDownConfiguration.getClusterName(), scaleDownConfiguration.isEnabled());
         }
         else {
            return new ScaleDownPolicy(scaleDownConfiguration.getConnectors(), scaleDownConfiguration.getGroupName(), scaleDownConfiguration.getClusterName(), scaleDownConfiguration.isEnabled());
         }
      }
      return null;
   }

   // A method to check the passed Configuration object and warn users if semantically unwise parameters are present
   public static void validateConfiguration(Configuration configuration) {
      // Warn if connection-ttl-override/connection-ttl == check-period
      compareTTLWithCheckPeriod(configuration);
   }

   private static void compareTTLWithCheckPeriod(Configuration configuration) {
      for (ClusterConnectionConfiguration c : configuration.getClusterConfigurations())
         compareTTLWithCheckPeriod(c.getName(), c.getConnectionTTL(), configuration.getConnectionTTLOverride(), c.getClientFailureCheckPeriod());

      for (BridgeConfiguration c : configuration.getBridgeConfigurations())
         compareTTLWithCheckPeriod(c.getName(), c.getConnectionTTL(), configuration.getConnectionTTLOverride(), c.getClientFailureCheckPeriod());
   }

   private static void compareTTLWithCheckPeriod(String name,
                                                 long connectionTTL,
                                                 long connectionTTLOverride,
                                                 long checkPeriod) {
      if (connectionTTLOverride == checkPeriod)
         ActiveMQServerLogger.LOGGER.connectionTTLEqualsCheckPeriod(name, "connection-ttl-override", "check-period");

      if (connectionTTL == checkPeriod)
         ActiveMQServerLogger.LOGGER.connectionTTLEqualsCheckPeriod(name, "connection-ttl", "check-period");
   }
}
