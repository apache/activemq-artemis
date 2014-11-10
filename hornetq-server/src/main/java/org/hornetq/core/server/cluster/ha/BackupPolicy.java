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
package org.hornetq.core.server.cluster.ha;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.core.server.impl.Activation;

public abstract class BackupPolicy implements HAPolicy<Activation>
{
   protected ScaleDownPolicy scaleDownPolicy;
   protected boolean restartBackup = HornetQDefaultConfiguration.isDefaultRestartBackup();

   public ScaleDownPolicy getScaleDownPolicy()
   {
      return scaleDownPolicy;
   }

   public void setScaleDownPolicy(ScaleDownPolicy scaleDownPolicy)
   {
      this.scaleDownPolicy = scaleDownPolicy;
   }


   @Override
   public boolean isBackup()
   {
      return true;
   }

   @Override
   public String getScaleDownClustername()
   {
      return null;
   }

   @Override
   public String getScaleDownGroupName()
   {
      return getScaleDownPolicy() != null ? getScaleDownPolicy().getGroupName() : null;
   }

   public boolean isRestartBackup()
   {
      return restartBackup;
   }

   public void setRestartBackup(boolean restartBackup)
   {
      this.restartBackup = restartBackup;
   }
}
