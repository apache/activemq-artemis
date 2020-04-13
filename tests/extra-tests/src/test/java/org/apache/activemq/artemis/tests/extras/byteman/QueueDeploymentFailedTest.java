/*
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
package org.apache.activemq.artemis.tests.extras.byteman;

import java.util.UUID;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class QueueDeploymentFailedTest extends ActiveMQTestBase {

   @Test
   @BMRule(name = "blow up queue deployment",
      targetClass = "org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl",
      targetMethod = "createQueue(SimpleString,RoutingType,SimpleString,SimpleString,SimpleString,boolean,boolean,boolean,boolean,boolean,int,boolean,boolean,boolean,int,long,boolean",
      targetLocation = "EXIT",
      action = "throw new IllegalStateException(\"test exception\")")
   public void testQueueDeploymentFailure() throws Exception {
      ActiveMQServer server = createServer(false, createDefaultNettyConfig());
      String address = UUID.randomUUID().toString();
      server.getConfiguration().addAddressConfiguration(new CoreAddressConfiguration().setName(address).addRoutingType(RoutingType.ANYCAST).addQueueConfiguration(new QueueConfiguration(UUID.randomUUID().toString()).setRoutingType(RoutingType.ANYCAST).setAddress(address)));
      server.start();
      assertTrue(server.getRemotingService().isStarted());
   }
}
