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
package org.apache.activemq6.tests.unit.core.deployers.impl;

import org.apache.activemq6.core.deployers.Deployer;
import org.apache.activemq6.core.deployers.DeploymentManager;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
*/
class FakeDeploymentManager implements DeploymentManager
{
   private boolean started;

   public boolean isStarted()
   {
      return started;
   }

   public void start() throws Exception
   {
      started = true;
   }

   public void stop() throws Exception
   {
      started = false;
   }

   public void registerDeployer(final Deployer deployer) throws Exception
   {
   }

   public void unregisterDeployer(final Deployer deployer) throws Exception
   {
   }
}
