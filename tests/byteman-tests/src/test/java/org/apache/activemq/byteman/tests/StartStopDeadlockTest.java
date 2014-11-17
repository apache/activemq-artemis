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
package org.apache.activemq.byteman.tests;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.HornetQServers;
import org.apache.activemq.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.tests.unit.util.InVMNamingContext;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * This test validates a deadlock identified by https://bugzilla.redhat.com/show_bug.cgi?id=959616
 *
 * @author Clebert
 */
@RunWith(BMUnitRunner.class)
public class StartStopDeadlockTest extends ServiceTestBase
{
   /*
   * simple test to make sure connect still works with some network latency  built into netty
   * */
   @Test
   @BMRules
      (

         rules =
            {
               @BMRule
                  (
                     name = "Server.start wait-init",
                     targetClass = "org.apache.activemq.core.server.impl.HornetQServerImpl",
                     targetMethod = "initialisePart2",
                     targetLocation = "ENTRY",
                     condition = "incrementCounter(\"server-Init\") == 2",
                     action = "System.out.println(\"server backup init\"), waitFor(\"start-init\")"
                  ),
               @BMRule(
                  name = "JMSServer.stop wait-init",
                  targetClass = "org.apache.activemq.jms.server.impl.JMSServerManagerImpl",
                  targetMethod = "stop",
                  targetLocation = "ENTRY",
                  action = "signalWake(\"start-init\", true)"
               ),
               @BMRule(
                  name = "StartStopDeadlockTest tearDown",
                  targetClass = "org.apache.activemq.byteman.tests.StartStopDeadlockTest",
                  targetMethod = "tearDown",
                  targetLocation = "ENTRY",
                  action = "deleteCounter(\"server-Init\")"
               )
            }
      )
   public void testDeadlock() throws Exception
   {

      // A live server that will always be crashed
      Configuration confLive = createDefaultConfig(true)
         .setSecurityEnabled(false)
         .setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration())
         .addConnectorConfiguration("invm", new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      final HornetQServer serverLive = HornetQServers.newHornetQServer(confLive);
      serverLive.start();
      addServer(serverLive);


      // A backup that will be waiting to be activated
      Configuration conf = createDefaultConfig(true)
         .setSecurityEnabled(false)
         .setHAPolicyConfiguration(new SharedStoreSlavePolicyConfiguration())
         .addConnectorConfiguration("invm", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      final HornetQServer server = HornetQServers.newHornetQServer(conf, true);
      addServer(server);

      final JMSServerManagerImpl jmsServer = new JMSServerManagerImpl(server);
      final InVMNamingContext context = new InVMNamingContext();
      jmsServer.setContext(context);

      jmsServer.start();

      final AtomicInteger errors = new AtomicInteger(0);
      final CountDownLatch align = new CountDownLatch(2);
      final CountDownLatch startLatch = new CountDownLatch(1);


      Thread tCrasher = new Thread("tStart")
      {
         @Override
         public void run()
         {
            try
            {
               align.countDown();
               startLatch.await();
               System.out.println("Crashing....");
               serverLive.stop(true);
            }
            catch (Exception e)
            {
               errors.incrementAndGet();
               e.printStackTrace();
            }
         }
      };

      Thread tStop = new Thread("tStop")
      {
         @Override
         public void run()
         {
            try
            {
               align.countDown();
               startLatch.await();
               jmsServer.stop();
            }
            catch (Exception e)
            {
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

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
   }
}
