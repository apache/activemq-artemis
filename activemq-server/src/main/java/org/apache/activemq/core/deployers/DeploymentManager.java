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
package org.apache.activemq.core.deployers;

import org.apache.activemq.core.server.ActiveMQComponent;

/**
 * This class manages any configuration files available. It will notify any deployers registered with it on changes.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface DeploymentManager extends ActiveMQComponent
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
