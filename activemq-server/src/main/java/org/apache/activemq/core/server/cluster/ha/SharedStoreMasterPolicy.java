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
package org.apache.activemq.core.server.cluster.ha;

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.core.server.impl.LiveActivation;
import org.apache.activemq.core.server.impl.SharedStoreLiveActivation;

import java.util.Map;

public class SharedStoreMasterPolicy implements HAPolicy<LiveActivation>
{
   private long failbackDelay = ActiveMQDefaultConfiguration.getDefaultFailbackDelay();

   private boolean failoverOnServerShutdown = ActiveMQDefaultConfiguration.isDefaultFailoverOnServerShutdown();

   private SharedStoreSlavePolicy sharedStoreSlavePolicy;

   public SharedStoreMasterPolicy()
   {
   }

   public SharedStoreMasterPolicy(long failbackDelay, boolean failoverOnServerShutdown)
   {
      this.failbackDelay = failbackDelay;
      this.failoverOnServerShutdown = failoverOnServerShutdown;
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

   public SharedStoreSlavePolicy getSharedStoreSlavePolicy()
   {
      return sharedStoreSlavePolicy;
   }

   public void setSharedStoreSlavePolicy(SharedStoreSlavePolicy sharedStoreSlavePolicy)
   {
      this.sharedStoreSlavePolicy = sharedStoreSlavePolicy;
   }

   @Override
   public boolean isSharedStore()
   {
      return true;
   }

   @Override
   public boolean isBackup()
   {
      return false;
   }

   @Override
   public boolean canScaleDown()
   {
      return false;
   }

   @Override
   public LiveActivation createActivation(ActiveMQServerImpl server, boolean wasLive, Map<String, Object> activationParams, ActiveMQServerImpl.ShutdownOnCriticalErrorListener shutdownOnCriticalIO)
   {
      return  new SharedStoreLiveActivation(server, this);
   }

   @Override
   public String getBackupGroupName()
   {
      return null;
   }

   @Override
   public String getScaleDownGroupName()
   {
      return null;
   }

   @Override
   public String getScaleDownClustername()
   {
      return null;
   }
}
