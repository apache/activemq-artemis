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

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class LatencyTest extends ActiveMQTestBase {

   /*
   * simple test to make sure connect still works with some network latency  built into netty
   * */
   @Test
   @BMRules(
      rules = {@BMRule(
         name = "trace ClientBootstrap.connect",
         targetClass = "org.jboss.netty.bootstrap.ClientBootstrap",
         targetMethod = "connect",
         targetLocation = "ENTRY",
         action = "System.out.println(\"netty connecting\")"), @BMRule(
         name = "sleep OioWorker.run",
         targetClass = "org.jboss.netty.channel.socket.oio.OioWorker",
         targetMethod = "run",
         targetLocation = "ENTRY",
         action = "Thread.sleep(500)")})
   public void testLatency() throws Exception {
      ActiveMQServer server = createServer(createDefaultNettyConfig());
      server.start();
      ServerLocator locator = createNettyNonHALocator();
      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = factory.createSession();
      session.close();
      server.stop();
   }
}
