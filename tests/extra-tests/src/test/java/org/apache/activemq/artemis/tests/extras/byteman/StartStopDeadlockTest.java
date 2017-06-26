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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * This test validates a deadlock identified by https://bugzilla.redhat.com/show_bug.cgi?id=959616
 */
@RunWith(BMUnitRunner.class)
public class StartStopDeadlockTest extends ActiveMQTestBase {

   /*
   * simple test to make sure connect still works with some network latency  built into netty
   * */
   @Test
   @BMRules(

      rules = {@BMRule(
         name = "Server.start wait-init",
         targetClass = "org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl",
         targetMethod = "initialisePart2",
         targetLocation = "ENTRY",
         condition = "incrementCounter(\"server-Init\") == 2",
         action = "System.out.println(\"server backup init\"), waitFor(\"start-init\")"), @BMRule(
         name = "JMSServer.stop wait-init",
         targetClass = "org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl",
         targetMethod = "stop",
         targetLocation = "ENTRY",
         action = "signalWake(\"start-init\", true)"), @BMRule(
         name = "StartStopDeadlockTest tearDown",
         targetClass = "org.apache.activemq.artemis.tests.extras.byteman.StartStopDeadlockTest",
         targetMethod = "tearDown",
         targetLocation = "ENTRY",
         action = "deleteCounter(\"server-Init\")")})
   public void testDeadlock() throws Exception {

      // A live server that will always be crashed
      Configuration confLive = createDefaultNettyConfig().setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration());
      final ActiveMQServer serverLive = addServer(ActiveMQServers.newActiveMQServer(confLive));
      serverLive.start();

      // A backup that will be waiting to be activated
      Configuration config = createDefaultNettyConfig().setHAPolicyConfiguration(new SharedStoreSlavePolicyConfiguration());

      final ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, true));

      final JMSServerManagerImpl jmsServer = new JMSServerManagerImpl(server);
      final InVMNamingContext context = new InVMNamingContext();
      jmsServer.setRegistry(new JndiBindingRegistry(context));

      jmsServer.start();

      final AtomicInteger errors = new AtomicInteger(0);
      final CountDownLatch align = new CountDownLatch(2);
      final CountDownLatch startLatch = new CountDownLatch(1);

      Thread tCrasher = new Thread("tStart") {
         @Override
         public void run() {
            try {
               align.countDown();
               startLatch.await();
               System.out.println("Crashing....");
               serverLive.fail(true);
            } catch (Exception e) {
               errors.incrementAndGet();
               e.printStackTrace();
            }
         }
      };

      Thread tStop = new Thread("tStop") {
         @Override
         public void run() {
            try {
               align.countDown();
               startLatch.await();
               jmsServer.stop();
            } catch (Exception e) {
               errors.incrementAndGet();
               e.printStackTrace();
            }
         }
      };

      tCrasher.start();
      tStop.start();
      align.await();
      startLatch.countDown();

      tCrasher.join();
      tStop.join();

      assertEquals(0, errors.get());
   }
}
