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
package org.apache.activemq.artemis.core.config.ha;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;

public class ReplicationPrimaryPolicyConfiguration implements HAPolicyConfiguration {

   private boolean checkForLiveServer = ActiveMQDefaultConfiguration.isDefaultCheckForLiveServer();

   private String groupName = null;

   private String clusterName = null;

   private long initialReplicationSyncTimeout = ActiveMQDefaultConfiguration.getDefaultInitialReplicationSyncTimeout();

   private int voteRetries = ActiveMQDefaultConfiguration.getDefaultVoteRetries();

   /**
    * TODO: move if into {@link ActiveMQDefaultConfiguration} when the configuration is stable.
    */
   private long voteRetryWait = 2000;

   private Long retryReplicationWait = ActiveMQDefaultConfiguration.getDefaultRetryReplicationWait();

   private DistributedPrimitiveManagerConfiguration distributedManagerConfiguration = null;

   public static ReplicationPrimaryPolicyConfiguration withDefault() {
      return new ReplicationPrimaryPolicyConfiguration();
   }

   private ReplicationPrimaryPolicyConfiguration() {
   }

   @Override
   public TYPE getType() {
      return TYPE.PRIMARY;
   }

   public boolean isCheckForLiveServer() {
      return checkForLiveServer;
   }

   public ReplicationPrimaryPolicyConfiguration setCheckForLiveServer(boolean checkForLiveServer) {
      this.checkForLiveServer = checkForLiveServer;
      return this;
   }

   public String getGroupName() {
      return groupName;
   }

   public ReplicationPrimaryPolicyConfiguration setGroupName(String groupName) {
      this.groupName = groupName;
      return this;
   }

   public String getClusterName() {
      return clusterName;
   }

   public ReplicationPrimaryPolicyConfiguration setClusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
   }

   public long getInitialReplicationSyncTimeout() {
      return initialReplicationSyncTimeout;
   }

   public ReplicationPrimaryPolicyConfiguration setInitialReplicationSyncTimeout(long initialReplicationSyncTimeout) {
      this.initialReplicationSyncTimeout = initialReplicationSyncTimeout;
      return this;
   }

   public int getVoteRetries() {
      return voteRetries;
   }

   public ReplicationPrimaryPolicyConfiguration setVoteRetries(int voteRetries) {
      this.voteRetries = voteRetries;
      return this;
   }

   public ReplicationPrimaryPolicyConfiguration setVoteRetryWait(long voteRetryWait) {
      this.voteRetryWait = voteRetryWait;
      return this;
   }

   public long getVoteRetryWait() {
      return voteRetryWait;
   }

   public void setRetryReplicationWait(Long retryReplicationWait) {
      this.retryReplicationWait = retryReplicationWait;
   }

   public Long getRetryReplicationWait() {
      return retryReplicationWait;
   }

   public ReplicationPrimaryPolicyConfiguration setDistributedManagerConfiguration(DistributedPrimitiveManagerConfiguration configuration) {
      this.distributedManagerConfiguration = configuration;
      return this;
   }

   public DistributedPrimitiveManagerConfiguration getDistributedManagerConfiguration() {
      return distributedManagerConfiguration;
   }
}
