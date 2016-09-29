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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ColocatedActivation;
import org.apache.activemq.artemis.core.server.impl.LiveActivation;

public class ColocatedPolicy implements HAPolicy<LiveActivation> {

   /*live stuff*/
   private boolean requestBackup = ActiveMQDefaultConfiguration.isDefaultHapolicyRequestBackup();

   private int backupRequestRetries = ActiveMQDefaultConfiguration.getDefaultHapolicyBackupRequestRetries();

   private long backupRequestRetryInterval = ActiveMQDefaultConfiguration.getDefaultHapolicyBackupRequestRetryInterval();

   private int maxBackups = ActiveMQDefaultConfiguration.getDefaultHapolicyMaxBackups();

   private int backupPortOffset = ActiveMQDefaultConfiguration.getDefaultHapolicyBackupPortOffset();

   /*backup stuff*/
   private List<String> excludedConnectors = new ArrayList<>();

   private BackupPolicy backupPolicy;

   private HAPolicy<LiveActivation> livePolicy;

   public ColocatedPolicy(boolean requestBackup,
                          int backupRequestRetries,
                          long backupRequestRetryInterval,
                          int maxBackups,
                          int backupPortOffset,
                          List<String> excludedConnectors,
                          HAPolicy livePolicy,
                          BackupPolicy backupPolicy) {
      this.requestBackup = requestBackup;
      this.backupRequestRetries = backupRequestRetries;
      this.backupRequestRetryInterval = backupRequestRetryInterval;
      this.maxBackups = maxBackups;
      this.backupPortOffset = backupPortOffset;
      this.excludedConnectors = excludedConnectors;
      this.livePolicy = livePolicy;
      this.backupPolicy = backupPolicy;
   }

   @Override
   public String getBackupGroupName() {
      return null;
   }

   @Override
   public String getScaleDownGroupName() {
      return null;
   }

   @Override
   public boolean isSharedStore() {
      return backupPolicy.isSharedStore();
   }

   @Override
   public boolean isBackup() {
      return false;
   }

   @Override
   public LiveActivation createActivation(ActiveMQServerImpl server,
                                          boolean wasLive,
                                          Map<String, Object> activationParams,
                                          ActiveMQServerImpl.ShutdownOnCriticalErrorListener shutdownOnCriticalIO) throws Exception {
      return new ColocatedActivation(server, this, livePolicy.createActivation(server, wasLive, activationParams, shutdownOnCriticalIO));
   }

   @Override
   public boolean canScaleDown() {
      return false;
   }

   @Override
   public String getScaleDownClustername() {
      return null;
   }

   public boolean isRequestBackup() {
      return requestBackup;
   }

   public void setRequestBackup(boolean requestBackup) {
      this.requestBackup = requestBackup;
   }

   public int getBackupRequestRetries() {
      return backupRequestRetries;
   }

   public void setBackupRequestRetries(int backupRequestRetries) {
      this.backupRequestRetries = backupRequestRetries;
   }

   public long getBackupRequestRetryInterval() {
      return backupRequestRetryInterval;
   }

   public void setBackupRequestRetryInterval(long backupRequestRetryInterval) {
      this.backupRequestRetryInterval = backupRequestRetryInterval;
   }

   public int getMaxBackups() {
      return maxBackups;
   }

   public void setMaxBackups(int maxBackups) {
      this.maxBackups = maxBackups;
   }

   public int getBackupPortOffset() {
      return backupPortOffset;
   }

   public void setBackupPortOffset(int backupPortOffset) {
      this.backupPortOffset = backupPortOffset;
   }

   public List<String> getExcludedConnectors() {
      return excludedConnectors;
   }

   public void setExcludedConnectors(List<String> excludedConnectors) {
      this.excludedConnectors = excludedConnectors;
   }

   public HAPolicy<LiveActivation> getLivePolicy() {
      return livePolicy;
   }

   public void setLivePolicy(HAPolicy<LiveActivation> livePolicy) {
      this.livePolicy = livePolicy;
   }

   public BackupPolicy getBackupPolicy() {
      return backupPolicy;
   }

   public void setBackupPolicy(BackupPolicy backupPolicy) {
      this.backupPolicy = backupPolicy;
   }
}
