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
package org.apache.activemq.core.config.ha;

import org.apache.activemq.api.config.HornetQDefaultConfiguration;
import org.apache.activemq.core.config.HAPolicyConfiguration;

public class SharedStoreMasterPolicyConfiguration implements HAPolicyConfiguration
{
   private long failbackDelay = HornetQDefaultConfiguration.getDefaultFailbackDelay();

   private boolean failoverOnServerShutdown = HornetQDefaultConfiguration.isDefaultFailoverOnServerShutdown();

   public SharedStoreMasterPolicyConfiguration()
   {
   }

   @Override
   public TYPE getType()
   {
      return TYPE.SHARED_STORE_MASTER;
   }

   public long getFailbackDelay()
   {
      return failbackDelay;
   }

   public SharedStoreMasterPolicyConfiguration setFailbackDelay(long failbackDelay)
   {
      this.failbackDelay = failbackDelay;
      return this;
   }

   public boolean isFailoverOnServerShutdown()
   {
      return failoverOnServerShutdown;
   }

   public SharedStoreMasterPolicyConfiguration setFailoverOnServerShutdown(boolean failoverOnServerShutdown)
   {
      this.failoverOnServerShutdown = failoverOnServerShutdown;
      return this;
   }
}
