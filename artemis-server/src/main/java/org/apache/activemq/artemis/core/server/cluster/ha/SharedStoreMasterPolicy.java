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
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.LiveActivation;
import org.apache.activemq.artemis.core.server.impl.SharedStoreLiveActivation;

public class SharedStoreMasterPolicy implements HAPolicy<LiveActivation> {

   private boolean failoverOnServerShutdown = ActiveMQDefaultConfiguration.isDefaultFailoverOnServerShutdown();
   private boolean waitForActivation = ActiveMQDefaultConfiguration.isDefaultWaitForActivation();

   private SharedStoreSlavePolicy sharedStoreSlavePolicy;

   public SharedStoreMasterPolicy() {
   }

   public SharedStoreMasterPolicy(boolean failoverOnServerShutdown, boolean waitForActivation) {
      this.failoverOnServerShutdown = failoverOnServerShutdown;
      this.waitForActivation = waitForActivation;
   }

   @Deprecated
   public long getFailbackDelay() {
      return -1;
   }

   @Deprecated
   public void setFailbackDelay(long failbackDelay) {
   }

   public boolean isFailoverOnServerShutdown() {
      return failoverOnServerShutdown;
   }

   public void setFailoverOnServerShutdown(boolean failoverOnServerShutdown) {
      this.failoverOnServerShutdown = failoverOnServerShutdown;
   }

   @Override
   public boolean isWaitForActivation() {
      return waitForActivation;
   }

   public void setWaitForActivation(boolean waitForActivation) {
      this.waitForActivation = waitForActivation;
   }

   public SharedStoreSlavePolicy getSharedStoreSlavePolicy() {
      return sharedStoreSlavePolicy;
   }

   public void setSharedStoreSlavePolicy(SharedStoreSlavePolicy sharedStoreSlavePolicy) {
      this.sharedStoreSlavePolicy = sharedStoreSlavePolicy;
   }

   @Override
   public boolean isSharedStore() {
      return true;
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
   public LiveActivation createActivation(ActiveMQServerImpl server,
                                          boolean wasLive,
                                          Map<String, Object> activationParams,
                                          ActiveMQServerImpl.ShutdownOnCriticalErrorListener shutdownOnCriticalIO) {
      return new SharedStoreLiveActivation(server, this);
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
   public String getScaleDownClustername() {
      return null;
   }
}
