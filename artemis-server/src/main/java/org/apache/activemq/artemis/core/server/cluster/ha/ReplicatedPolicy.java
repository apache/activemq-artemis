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
package org.apache.activemq.artemis.core.server.cluster.ha;

import java.util.Map;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.server.NetworkHealthCheck;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.LiveActivation;
import org.apache.activemq.artemis.core.server.impl.SharedNothingLiveActivation;

public class ReplicatedPolicy implements HAPolicy<LiveActivation> {

   private boolean checkForLiveServer = ActiveMQDefaultConfiguration.isDefaultCheckForLiveServer();

   private String groupName = null;

   private String clusterName;

   private long initialReplicationSyncTimeout = ActiveMQDefaultConfiguration.getDefaultInitialReplicationSyncTimeout();

   /*
   * these are only set by the ReplicaPolicy after failover to decide if the live server can failback, these should not
   * be exposed in configuration.
   * */
   private boolean allowAutoFailBack = ActiveMQDefaultConfiguration.isDefaultAllowAutoFailback();

   /*
   * whether or not this live broker should vote to remain live
   * */
   private boolean voteOnReplicationFailure;

   /*
   * what quorum size to use for voting
   * */
   private int quorumSize;

   private int voteRetries;

   private long voteRetryWait;

   /*
   * this are only used as the policy when the server is started as a live after a failover
   * */
   private ReplicaPolicy replicaPolicy;

   private final NetworkHealthCheck networkHealthCheck;

   public ReplicatedPolicy(NetworkHealthCheck networkHealthCheck) {
      replicaPolicy = new ReplicaPolicy(networkHealthCheck, this);
      this.networkHealthCheck = networkHealthCheck;
   }

   public ReplicatedPolicy(boolean checkForLiveServer,
                           String groupName,
                           String clusterName,
                           long initialReplicationSyncTimeout,
                           NetworkHealthCheck networkHealthCheck,
                           boolean voteOnReplicationFailure,
                           int quorumSize,
                           int voteRetries,
                           long voteRetryWait) {
      this.checkForLiveServer = checkForLiveServer;
      this.groupName = groupName;
      this.clusterName = clusterName;
      this.initialReplicationSyncTimeout = initialReplicationSyncTimeout;
      this.networkHealthCheck = networkHealthCheck;
      this.voteOnReplicationFailure = voteOnReplicationFailure;
      this.quorumSize = quorumSize;
      this.voteRetries = voteRetries;
      this.voteRetryWait = voteRetryWait;
   }

   public ReplicatedPolicy(boolean checkForLiveServer,
                           boolean allowAutoFailBack,
                           long initialReplicationSyncTimeout,
                           String groupName,
                           String clusterName,
                           ReplicaPolicy replicaPolicy,
                           NetworkHealthCheck networkHealthCheck,
                           boolean voteOnReplicationFailure,
                           int quorumSize,
                           int voteRetries,
                           long voteRetryWait) {
      this.checkForLiveServer = checkForLiveServer;
      this.clusterName = clusterName;
      this.groupName = groupName;
      this.allowAutoFailBack = allowAutoFailBack;
      this.initialReplicationSyncTimeout = initialReplicationSyncTimeout;
      this.replicaPolicy = replicaPolicy;
      this.networkHealthCheck = networkHealthCheck;
      this.voteOnReplicationFailure = voteOnReplicationFailure;
      this.quorumSize = quorumSize;
   }

   public boolean isCheckForLiveServer() {
      return checkForLiveServer;
   }

   public void setCheckForLiveServer(boolean checkForLiveServer) {
      this.checkForLiveServer = checkForLiveServer;
   }

   public boolean isAllowAutoFailBack() {
      return allowAutoFailBack;
   }

   @Deprecated
   public long getFailbackDelay() {
      return -1;
   }

   @Deprecated
   public void setFailbackDelay(long failbackDelay) {
   }

   public long getInitialReplicationSyncTimeout() {
      return initialReplicationSyncTimeout;
   }

   public void setInitialReplicationSyncTimeout(long initialReplicationSyncTimeout) {
      this.initialReplicationSyncTimeout = initialReplicationSyncTimeout;
   }

   public String getClusterName() {
      return clusterName;
   }

   public void setClusterName(String clusterName) {
      this.clusterName = clusterName;
   }

   public ReplicaPolicy getReplicaPolicy() {
      if (replicaPolicy == null) {
         replicaPolicy = new ReplicaPolicy(networkHealthCheck, this);
         replicaPolicy.setQuorumSize(quorumSize);
         replicaPolicy.setVoteOnReplicationFailure(voteOnReplicationFailure);
         replicaPolicy.setVoteRetries(voteRetries);
         replicaPolicy.setVoteRetryWait(voteRetryWait);
         if (clusterName != null && clusterName.length() > 0) {
            replicaPolicy.setClusterName(clusterName);
         }
         if (groupName != null && groupName.length() > 0) {
            replicaPolicy.setGroupName(groupName);
         }
      }
      return replicaPolicy;
   }

   public void setReplicaPolicy(ReplicaPolicy replicaPolicy) {
      this.replicaPolicy = replicaPolicy;
   }

   /*
   * these 2 methods are the same, leaving both as the second is correct but the first is needed until more refactoring is done
   * */
   @Override
   public String getBackupGroupName() {
      return groupName;
   }

   public String getGroupName() {
      return groupName;
   }

   @Override
   public String getScaleDownGroupName() {
      return null;
   }

   public void setGroupName(String groupName) {
      this.groupName = groupName;
   }

   @Override
   public boolean isSharedStore() {
      return false;
   }

   @Override
   public boolean isBackup() {
      return false;
   }

   @Override
   public boolean canScaleDown() {
      return false;
   }

   @Override
   public String getScaleDownClustername() {
      return null;
   }

   public void setAllowAutoFailBack(boolean allowAutoFailBack) {
      this.allowAutoFailBack = allowAutoFailBack;
   }

   public boolean isVoteOnReplicationFailure() {
      return voteOnReplicationFailure;
   }

   @Override
   public LiveActivation createActivation(ActiveMQServerImpl server,
                                          boolean wasLive,
                                          Map<String, Object> activationParams,
                                          ActiveMQServerImpl.ShutdownOnCriticalErrorListener shutdownOnCriticalIO) {
      return new SharedNothingLiveActivation(server, this);
   }

   public int getQuorumSize() {
      return quorumSize;
   }

   public void setQuorumSize(int quorumSize) {
      this.quorumSize = quorumSize;
   }
}
