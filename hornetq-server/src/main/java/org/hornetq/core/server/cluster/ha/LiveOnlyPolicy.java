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

import org.hornetq.core.server.impl.Activation;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.LiveOnlyActivation;

import java.util.Map;

public class LiveOnlyPolicy implements HAPolicy<Activation>
{
   private ScaleDownPolicy scaleDownPolicy;

   public LiveOnlyPolicy()
   {
   }

   public LiveOnlyPolicy(ScaleDownPolicy scaleDownPolicy)
   {
      this.scaleDownPolicy = scaleDownPolicy;
   }

   @Override
   public Activation createActivation(HornetQServerImpl server, boolean wasLive, Map<String, Object> activationParams, HornetQServerImpl.ShutdownOnCriticalErrorListener shutdownOnCriticalIO)
   {
      return new LiveOnlyActivation(server, this);
   }

   @Override
   public String getBackupGroupName()
   {
      return null;
   }

   @Override
   public String getScaleDownGroupName()
   {
      return scaleDownPolicy == null ? null : scaleDownPolicy.getGroupName();
   }

   @Override
   public String getScaleDownClustername()
   {
      return null;
   }

   @Override
   public boolean isSharedStore()
   {
      return false;
   }

   @Override
   public boolean isBackup()
   {
      return false;
   }

   @Override
   public boolean canScaleDown()
   {
      return scaleDownPolicy != null;
   }

   public ScaleDownPolicy getScaleDownPolicy()
   {
      return scaleDownPolicy;
   }

   public void setScaleDownPolicy(ScaleDownPolicy scaleDownPolicy)
   {
      this.scaleDownPolicy = scaleDownPolicy;
   }
}
