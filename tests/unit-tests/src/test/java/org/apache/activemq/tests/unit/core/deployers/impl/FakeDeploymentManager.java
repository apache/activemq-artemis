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
package org.apache.activemq.tests.unit.core.deployers.impl;

import org.apache.activemq.core.deployers.Deployer;
import org.apache.activemq.core.deployers.DeploymentManager;

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
