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
package org.apache.activemq.core.config;

import org.apache.activemq.api.core.HornetQIllegalStateException;
import org.apache.activemq.core.config.ha.ColocatedPolicyConfiguration;
import org.apache.activemq.core.config.ha.LiveOnlyPolicyConfiguration;
import org.apache.activemq.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.core.server.HornetQMessageBundle;
import org.apache.activemq.core.server.cluster.ha.BackupPolicy;
import org.apache.activemq.core.server.cluster.ha.ColocatedPolicy;
import org.apache.activemq.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.core.server.cluster.ha.LiveOnlyPolicy;
import org.apache.activemq.core.server.cluster.ha.ReplicaPolicy;
import org.apache.activemq.core.server.cluster.ha.ReplicatedPolicy;
import org.apache.activemq.core.server.cluster.ha.ScaleDownPolicy;
import org.apache.activemq.core.server.cluster.ha.SharedStoreMasterPolicy;
import org.apache.activemq.core.server.cluster.ha.SharedStoreSlavePolicy;

public final class ConfigurationUtils
{

   private ConfigurationUtils()
   {
      // Utility class
   }

   public static ClusterConnectionConfiguration getReplicationClusterConfiguration(Configuration conf, String replicationCluster) throws HornetQIllegalStateException
   {
      if (replicationCluster == null || replicationCluster.isEmpty())
         return conf.getClusterConfigurations().get(0);
      for (ClusterConnectionConfiguration clusterConf : conf.getClusterConfigurations())
      {
         if (replicationCluster.equals(clusterConf.getName()))
            return clusterConf;
      }
      throw new HornetQIllegalStateException("Missing cluster-configuration for replication-clustername '" + replicationCluster + "'.");
   }

   public static HAPolicy getHAPolicy(HAPolicyConfiguration conf) throws HornetQIllegalStateException
   {
      if (conf == null)
      {
         return new LiveOnlyPolicy();
      }

      switch (conf.getType())
      {
         case LIVE_ONLY:
         {
            LiveOnlyPolicyConfiguration pc = (LiveOnlyPolicyConfiguration) conf;
            return new LiveOnlyPolicy(getScaleDownPolicy(pc.getScaleDownConfiguration()));
         }
         case REPLICATED:
         {
            ReplicatedPolicyConfiguration pc = (ReplicatedPolicyConfiguration) conf;
            return new ReplicatedPolicy(pc.isCheckForLiveServer(), pc.getGroupName(), pc.getClusterName());
         }
         case REPLICA:
         {
            ReplicaPolicyConfiguration pc = (ReplicaPolicyConfiguration) conf;
            return new ReplicaPolicy(pc.getClusterName(), pc.getMaxSavedReplicatedJournalsSize(), pc.getGroupName(), pc.isRestartBackup(), pc.isAllowFailBack(), pc.getFailbackDelay(), getScaleDownPolicy(pc.getScaleDownConfiguration()));
         }
         case SHARED_STORE_MASTER:
         {
            SharedStoreMasterPolicyConfiguration pc = (SharedStoreMasterPolicyConfiguration) conf;
            return new SharedStoreMasterPolicy(pc.getFailbackDelay(), pc.isFailoverOnServerShutdown());
         }
         case SHARED_STORE_SLAVE:
         {
            SharedStoreSlavePolicyConfiguration pc = (SharedStoreSlavePolicyConfiguration) conf;
            return new SharedStoreSlavePolicy(pc.getFailbackDelay(), pc.isFailoverOnServerShutdown(), pc.isRestartBackup(), pc.isAllowFailBack(), getScaleDownPolicy(pc.getScaleDownConfiguration()));
         }
         case COLOCATED:
         {
            ColocatedPolicyConfiguration pc = (ColocatedPolicyConfiguration) conf;

            HAPolicyConfiguration backupConf = pc.getBackupConfig();
            BackupPolicy backupPolicy;
            if (backupConf == null)
            {
               backupPolicy = new ReplicaPolicy();
            }
            else
            {
               backupPolicy = (BackupPolicy) getHAPolicy(backupConf);
            }
            HAPolicyConfiguration liveConf = pc.getLiveConfig();
            HAPolicy livePolicy;
            if (liveConf == null)
            {
               livePolicy = new ReplicatedPolicy();
            }
            else
            {
               livePolicy = getHAPolicy(liveConf);
            }
            return new ColocatedPolicy(pc.isRequestBackup(),
                  pc.getBackupRequestRetries(),
                  pc.getBackupRequestRetryInterval(),
                  pc.getMaxBackups(),
                  pc.getBackupPortOffset(),
                  pc.getExcludedConnectors(),
                  livePolicy,
                  backupPolicy);
         }

      }
      throw HornetQMessageBundle.BUNDLE.unsupportedHAPolicyConfiguration(conf);
   }

   public static ScaleDownPolicy getScaleDownPolicy(ScaleDownConfiguration scaleDownConfiguration)
   {
      if (scaleDownConfiguration != null)
      {
         if (scaleDownConfiguration.getDiscoveryGroup() != null)
         {
            return new ScaleDownPolicy(scaleDownConfiguration.getDiscoveryGroup(), scaleDownConfiguration.getGroupName(),
                  scaleDownConfiguration.getClusterName(), scaleDownConfiguration.isEnabled());
         }
         else
         {
            return new ScaleDownPolicy(scaleDownConfiguration.getConnectors(), scaleDownConfiguration.getGroupName(),
                  scaleDownConfiguration.getClusterName(), scaleDownConfiguration.isEnabled());
         }
      }
      return null;
   }
}
