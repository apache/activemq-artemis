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
package org.apache.activemq6.core.config.ha;

import org.apache.activemq6.api.config.HornetQDefaultConfiguration;
import org.apache.activemq6.core.config.HAPolicyConfiguration;
import org.apache.activemq6.core.config.ScaleDownConfiguration;

public class SharedStoreSlavePolicyConfiguration implements HAPolicyConfiguration
{
   private long failbackDelay = HornetQDefaultConfiguration.getDefaultFailbackDelay();

   private boolean failoverOnServerShutdown = HornetQDefaultConfiguration.isDefaultFailoverOnServerShutdown();

   private boolean restartBackup = HornetQDefaultConfiguration.isDefaultRestartBackup();

   private boolean allowFailBack = HornetQDefaultConfiguration.isDefaultAllowAutoFailback();

   private ScaleDownConfiguration scaleDownConfiguration;

   public SharedStoreSlavePolicyConfiguration()
   {
   }

   @Override
   public TYPE getType()
   {
      return TYPE.SHARED_STORE_SLAVE;
   }

   public boolean isRestartBackup()
   {
      return restartBackup;
   }

   public SharedStoreSlavePolicyConfiguration setRestartBackup(boolean restartBackup)
   {
      this.restartBackup = restartBackup;
      return this;
   }

   public ScaleDownConfiguration getScaleDownConfiguration()
   {
      return scaleDownConfiguration;
   }

   public SharedStoreSlavePolicyConfiguration setScaleDownConfiguration(ScaleDownConfiguration scaleDownConfiguration)
   {
      this.scaleDownConfiguration = scaleDownConfiguration;
      return this;
   }

   public boolean isAllowFailBack()
   {
      return allowFailBack;
   }

   public SharedStoreSlavePolicyConfiguration setAllowFailBack(boolean allowFailBack)
   {
      this.allowFailBack = allowFailBack;
      return this;
   }

   public boolean isFailoverOnServerShutdown()
   {
      return failoverOnServerShutdown;
   }

   public SharedStoreSlavePolicyConfiguration setFailoverOnServerShutdown(boolean failoverOnServerShutdown)
   {
      this.failoverOnServerShutdown = failoverOnServerShutdown;
      return this;
   }

   public long getFailbackDelay()
   {
      return failbackDelay;
   }

   public SharedStoreSlavePolicyConfiguration setFailbackDelay(long failbackDelay)
   {
      this.failbackDelay = failbackDelay;
      return this;
   }

}
