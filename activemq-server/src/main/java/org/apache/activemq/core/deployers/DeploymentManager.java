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
package org.apache.activemq.core.deployers;

import org.apache.activemq.core.server.HornetQComponent;

/**
 * This class manages any configuration files available. It will notify any deployers registered with it on changes.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface DeploymentManager extends HornetQComponent
{
   /**
    * registers a deployable object which will handle the deployment of URL's
    * @param deployer The deployable object
    * @throws Exception
    */
   void registerDeployer(Deployer deployer) throws Exception;

   /**
    * unregisters a deployable object which will handle the deployment of URL's
    * @param deployer The deployable object
    * @throws Exception
    */
   void unregisterDeployer(Deployer deployer) throws Exception;
}
