/**
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
package org.apache.activemq.core.server.cluster.ha;

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.core.server.impl.Activation;
import org.apache.activemq.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.core.server.impl.SharedStoreBackupActivation;

import java.util.Map;

public class SharedStoreSlavePolicy extends BackupPolicy
{
   private long failbackDelay = ActiveMQDefaultConfiguration.getDefaultFailbackDelay();

   private boolean failoverOnServerShutdown = ActiveMQDefaultConfiguration.isDefaultFailoverOnServerShutdown();

   private boolean allowAutoFailBack = ActiveMQDefaultConfiguration.isDefaultAllowAutoFailback();

   //this is how we act once we have failed over
   private SharedStoreMasterPolicy sharedStoreMasterPolicy;

   public SharedStoreSlavePolicy()
   {
   }

   public SharedStoreSlavePolicy(long failbackDelay, boolean failoverOnServerShutdown, boolean restartBackup, boolean allowAutoFailBack, ScaleDownPolicy scaleDownPolicy)
   {
      this.failbackDelay = failbackDelay;
      this.failoverOnServerShutdown = failoverOnServerShutdown;
      this.restartBackup = restartBackup;
      this.allowAutoFailBack = allowAutoFailBack;
      this.scaleDownPolicy = scaleDownPolicy;
      sharedStoreMasterPolicy = new SharedStoreMasterPolicy(failbackDelay, failoverOnServerShutdown);
   }

   public long getFailbackDelay()
   {
      return failbackDelay;
   }

   public void setFailbackDelay(long failbackDelay)
   {
      this.failbackDelay = failbackDelay;
   }

   public boolean isFailoverOnServerShutdown()
   {
      return failoverOnServerShutdown;
   }

   public void setFailoverOnServerShutdown(boolean failoverOnServerShutdown)
   {
      this.failoverOnServerShutdown = failoverOnServerShutdown;
   }

   public SharedStoreMasterPolicy getSharedStoreMasterPolicy()
   {
      return sharedStoreMasterPolicy;
   }

   public void setSharedStoreMasterPolicy(SharedStoreMasterPolicy sharedStoreMasterPolicy)
   {
      this.sharedStoreMasterPolicy = sharedStoreMasterPolicy;
   }

   @Override
   public boolean isSharedStore()
   {
      return true;
   }

   @Override
   public boolean canScaleDown()
   {
      return scaleDownPolicy != null;
   }

   public boolean isAllowAutoFailBack()
   {
      return allowAutoFailBack;
   }

   public void setAllowAutoFailBack(boolean allowAutoFailBack)
   {
      this.allowAutoFailBack = allowAutoFailBack;
   }

   @Override
   public Activation createActivation(ActiveMQServerImpl server, boolean wasLive, Map<String, Object> activationParams, ActiveMQServerImpl.ShutdownOnCriticalErrorListener shutdownOnCriticalIO)
   {
      return new SharedStoreBackupActivation(server, this);
   }

   @Override
   public String getBackupGroupName()
   {
      return null;
   }
}
