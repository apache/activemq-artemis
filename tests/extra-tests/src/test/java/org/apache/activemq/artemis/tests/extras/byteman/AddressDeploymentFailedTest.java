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

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class AddressDeploymentFailedTest extends ActiveMQTestBase {

   @Test
   @BMRule(name = "blow up address deployment",
      targetClass = "org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl",
      targetMethod = "addOrUpdateAddressInfo(AddressInfo)",
      targetLocation = "EXIT",
      action = "throw new IllegalStateException(\"test exception\")")
   public void testAddressDeploymentFailure() throws Exception {
      ActiveMQServer server = createServer(false, createDefaultNettyConfig());
      server.getConfiguration().addAddressConfiguration(new CoreAddressConfiguration().setName(UUID.randomUUID().toString()).addRoutingType(RoutingType.ANYCAST));
      server.start();
      assertTrue(server.getRemotingService().isStarted());
   }
}
