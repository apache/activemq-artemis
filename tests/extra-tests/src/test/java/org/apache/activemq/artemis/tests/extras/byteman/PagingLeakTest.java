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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.paging.cursor.impl.PagePositionImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class PagingLeakTest extends ActiveMQTestBase {

   private static final AtomicInteger pagePosInstances = new AtomicInteger(0);

   public static void newPosition() {
      pagePosInstances.incrementAndGet();
   }

   public static void deletePosition() {
      pagePosInstances.decrementAndGet();
   }

   @Before
   public void setup() {
      pagePosInstances.set(0);
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "newPosition",
         targetClass = "org.apache.activemq.artemis.core.paging.cursor.impl.PagePositionImpl",
         targetMethod = "<init>()",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.PagingLeakTest.newPosition()"), @BMRule(
         name = "finalPosition",
         targetClass = "org.apache.activemq.artemis.core.paging.cursor.impl.PagePositionImpl",
         targetMethod = "finalize",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.PagingLeakTest.deletePosition()")})
   public void testValidateLeak() throws Throwable {
      System.out.println("location::" + getBindingsDir());

      List<PagePositionImpl> positions = new ArrayList<>();

      for (int i = 0; i < 300; i++) {
         positions.add(new PagePositionImpl(3, 3));
      }

      long timeout = System.currentTimeMillis() + 5000;
      while (pagePosInstances.get() != 300 && timeout > System.currentTimeMillis()) {
         forceGC();
      }

      // This is just to validate the rules are correctly applied on byteman
      assertEquals("You have changed something on PagePositionImpl in such way that these byteman rules are no longer working", 300, pagePosInstances.get());

      positions.clear();

      Wait.waitFor(new Wait.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            forceGC();
            return pagePosInstances.get() == 0;
         }
      }, 5000, 100);

      // This is just to validate the rules are correctly applied on byteman
      assertEquals("You have changed something on PagePositionImpl in such way that these byteman rules are no longer working", 0, pagePosInstances.get());

      final ArrayList<Exception> errors = new ArrayList<>();
      // A backup that will be waiting to be activated
      Configuration config = createDefaultNettyConfig();

      config.setJournalBufferTimeout_AIO(10).setJournalBufferTimeout_NIO(10);

      final ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(config, true));

      server.start();

      AddressSettings settings = new AddressSettings().setPageSizeBytes(2 * 1024).setMaxSizeBytes(10 * 1024).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      server.getAddressSettingsRepository().addMatch("#", settings);

      final SimpleString address = new SimpleString("pgdAddress");

      class Consumer extends Thread {

         final ServerLocator locator;
         final ClientSessionFactory sf;
         final ClientSession session;
         final ClientConsumer consumer;

         final int sleepTime;
         final int maxConsumed;

         Consumer(int sleepTime, String suffix, int maxConsumed) throws Exception {
            server.createQueue(address, RoutingType.MULTICAST, address.concat(suffix), null, true, false);

            this.sleepTime = sleepTime;
            locator = createInVMLocator(0);
            sf = locator.createSessionFactory();
            session = sf.createSession(true, true);
            consumer = session.createConsumer(address.concat(suffix));

            this.maxConsumed = maxConsumed;
         }

         @Override
         public void run() {
            try {
               session.start();

               long lastTime = System.currentTimeMillis();

               for (long i = 0; i < maxConsumed; i++) {
                  ClientMessage msg = consumer.receive(5000);

                  if (msg == null) {
                     errors.add(new Exception("didn't receive a message"));
                     return;
                  }

                  msg.acknowledge();

                  if (sleepTime > 0) {

                     Thread.sleep(sleepTime);
                  }

                  if (i % 1000 == 0) {
                     System.out.println("Consumed " + i + " events in " + (System.currentTimeMillis() - lastTime));
                     lastTime = System.currentTimeMillis();
                  }
               }
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }

      int numberOfMessages = 500;

      Consumer consumer1 = new Consumer(10, "-1", 150);
      Consumer consumer2 = new Consumer(0, "-2", numberOfMessages);

      final ServerLocator locator = createInVMLocator(0);
      final ClientSessionFactory sf = locator.createSessionFactory();
      final ClientSession session = sf.createSession(true, true);
      final ClientProducer producer = session.createProducer(address);

      byte[] b = new byte[1024];

      for (long i = 0; i < numberOfMessages; i++) {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(b);
         producer.send(msg);

         if (i == 100) {
            System.out.println("Starting consumers!!!");
            consumer1.start();
            consumer2.start();
         }

         if (i % 250 == 0) {
            validateInstances();
         }

      }

      System.out.println("Sent " + numberOfMessages);

      consumer1.join();
      consumer2.join();

      validateInstances();
      Throwable elast = null;

      for (Throwable e : errors) {
         e.printStackTrace();
         elast = e;
      }

      if (elast != null) {
         throw elast;
      }

   }

   private void validateInstances() {
      forceGC();
      int count2 = pagePosInstances.get();
      Assert.assertTrue("There is a leak, you shouldn't have this many instances (" + count2 + ")", count2 < 5000);
   }

}
