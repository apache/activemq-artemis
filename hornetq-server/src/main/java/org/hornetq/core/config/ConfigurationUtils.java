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
package org.hornetq.core.config;

import org.hornetq.api.core.HornetQIllegalStateException;
public final class ConfigurationUtils
{

   private ConfigurationUtils()
   {
      // Utility class
   }

   public static ClusterConnectionConfiguration getReplicationClusterConfiguration(Configuration conf) throws HornetQIllegalStateException
   {
      final String replicationCluster = conf.getHAPolicy().getReplicationClustername();
      if (replicationCluster == null || replicationCluster.isEmpty())
         return conf.getClusterConfigurations().get(0);
      for (ClusterConnectionConfiguration clusterConf : conf.getClusterConfigurations())
      {
         if (replicationCluster.equals(clusterConf.getName()))
            return clusterConf;
      }
      throw new HornetQIllegalStateException("Missing cluster-configuration for replication-clustername '" + replicationCluster + "'.");
   }
}
