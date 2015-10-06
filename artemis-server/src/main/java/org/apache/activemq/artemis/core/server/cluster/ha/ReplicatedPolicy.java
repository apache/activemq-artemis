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

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.LiveActivation;
import org.apache.activemq.artemis.core.server.impl.SharedNothingLiveActivation;

import java.util.Map;

public class ReplicatedPolicy implements HAPolicy<LiveActivation> {

   private boolean checkForLiveServer = ActiveMQDefaultConfiguration.isDefaultCheckForLiveServer();

   private String groupName = null;

   private String clusterName;

   /*
   * these are only set by the ReplicaPolicy after failover to decide if the live server can failback, these should not
   * be exposed in configuration.
   * */
   private boolean allowAutoFailBack = ActiveMQDefaultConfiguration.isDefaultAllowAutoFailback();

   private long failbackDelay = ActiveMQDefaultConfiguration.getDefaultFailbackDelay();

   /*
   * this are only used as the policy when the server is started as a live after a failover
   * */
   private ReplicaPolicy replicaPolicy;

   public ReplicatedPolicy() {
      replicaPolicy = new ReplicaPolicy(clusterName, -1, groupName, this);
   }

   public ReplicatedPolicy(boolean checkForLiveServer, String groupName, String clusterName) {
      this.checkForLiveServer = checkForLiveServer;
      this.groupName = groupName;
      this.clusterName = clusterName;
      /*
      * we create this with sensible defaults in case we start after a failover
      * */
   }

   public ReplicatedPolicy(boolean checkForLiveServer,
                           boolean allowAutoFailBack,
                           long failbackDelay,
                           String groupName,
                           String clusterName,
                           ReplicaPolicy replicaPolicy) {
      this.checkForLiveServer = checkForLiveServer;
      this.clusterName = clusterName;
      this.groupName = groupName;
      this.allowAutoFailBack = allowAutoFailBack;
      this.failbackDelay = failbackDelay;
      this.replicaPolicy = replicaPolicy;
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

   public long getFailbackDelay() {
      return failbackDelay;
   }

   public void setFailbackDelay(long failbackDelay) {
      this.failbackDelay = failbackDelay;
   }

   public String getClusterName() {
      return clusterName;
   }

   public void setClusterName(String clusterName) {
      this.clusterName = clusterName;
   }

   public ReplicaPolicy getReplicaPolicy() {
      if (replicaPolicy == null) {
         replicaPolicy = new ReplicaPolicy(clusterName, -1, groupName, this);
      }
      return replicaPolicy;
   }

   public void setReplicaPolicy(ReplicaPolicy replicaPolicy) {
      this.replicaPolicy = replicaPolicy;
   }

   /*
   * these 2 methods are the same, leaving both as the second is correct but the first is needed until more refactoring is done
   * */
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

   @Override
   public LiveActivation createActivation(ActiveMQServerImpl server,
                                          boolean wasLive,
                                          Map<String, Object> activationParams,
                                          ActiveMQServerImpl.ShutdownOnCriticalErrorListener shutdownOnCriticalIO) {
      return new SharedNothingLiveActivation(server, this);
   }
}
