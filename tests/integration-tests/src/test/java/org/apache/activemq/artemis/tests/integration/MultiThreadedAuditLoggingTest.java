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
package org.apache.activemq.artemis.tests.integration;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.spi.core.security.ActiveMQBasicSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.logmanager.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiThreadedAuditLoggingTest extends ActiveMQTestBase {

   protected ActiveMQServer server;
   private static final int MESSAGE_COUNT = 10;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(true, createDefaultInVMConfig().setSecurityEnabled(true));
      server.setSecurityManager(new ActiveMQBasicSecurityManager());
      server.start();
      Set<Role> roles = new HashSet<>();
      roles.add(new Role("queue1", true, true, true, true, true, true, true, true, true, true));
      server.getSecurityRepository().addMatch("queue1", roles);
      roles = new HashSet<>();
      roles.add(new Role("queue2", true, true, true, true, true, true, true, true, true, true));
      server.getSecurityRepository().addMatch("queue2", roles);
      server.getActiveMQServerControl().addUser("queue1", "queue1", "queue1", true);
      server.getActiveMQServerControl().addUser("queue2", "queue2", "queue2", true);
   }

   private static final Logger logManager = org.jboss.logmanager.Logger.getLogger("org.apache.activemq.audit.message");
   private static java.util.logging.Level previousLevel = logManager.getLevel();

   @BeforeClass
   public static void prepareLogger() {
      logManager.setLevel(Level.INFO);
      AssertionLoggerHandler.startCapture();
   }

   @AfterClass
   public static void clearLogger() {
      AssertionLoggerHandler.stopCapture();
      logManager.setLevel(previousLevel);
   }

   class SomeConsumer extends Thread {

      boolean failed = false;
      protected ClientSession session;
      protected ClientSessionFactory sf;
      protected ServerLocator locator;
      String queue;

      SomeConsumer(String queue) throws Exception {
         this.queue = queue;
         locator = createInVMNonHALocator();
         sf = createSessionFactory(locator);
         session = addClientSession(sf.createSession(queue, queue, false, true, true, false, 0));
      }

      @Override
      public void run() {
         ClientConsumer consumer = null;

         try {
            try {
               session.createQueue(new QueueConfiguration(queue).setRoutingType(RoutingType.ANYCAST));
            } catch (Exception e) {
               // ignore
            }
            consumer = session.createConsumer(queue);
            session.start();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
               consumer.receive();
            }
         } catch (Throwable e) {
            failed = true;
         } finally {
            try {
               if (consumer != null) {
                  consumer.close();
               }
               if (session != null) {
                  session.close();
               }
               if (sf != null) {
                  sf.close();
               }
               if (locator != null) {
                  locator.close();
               }
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }
   }

   class SomeProducer extends Thread {

      boolean failed = false;
      protected ClientSession session;
      protected ClientSessionFactory sf;
      protected ServerLocator locator;
      String queue;

      SomeProducer(String queue) throws Exception {
         this.queue = queue;
         locator = createInVMNonHALocator();
         sf = createSessionFactory(locator);
         session = addClientSession(sf.createSession(queue, queue, false, true, true, false, 0));
      }

      @Override
      public void run() {
         final String data = "Simple Text " + UUID.randomUUID().toString();
         ClientProducer producer = null;

         try {
            try {
               session.createQueue(new QueueConfiguration(queue).setRoutingType(RoutingType.ANYCAST));
            } catch (Exception e) {
               // ignore
            }
            producer = session.createProducer(queue);
            ClientMessage message = session.createMessage(false);
            message.getBodyBuffer().writeString(data);
            for (int i = 0; i < MESSAGE_COUNT; i++) {
               producer.send(message);
            }
         } catch (Throwable e) {
            failed = true;
         } finally {
            try {
               if (producer != null) {
                  producer.close();
               }
               if (session != null) {
                  session.close();
               }
               if (sf != null) {
                  sf.close();
               }
               if (locator != null) {
                  locator.close();
               }
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }
   }

   @Test
   public void testConcurrentLogging() throws Exception {

      int nThreads = 6;
      SomeConsumer[] consumers = new SomeConsumer[nThreads];
      SomeProducer[] producers = new SomeProducer[nThreads];

      for (int j = 0; j < 8; j++) {

         for (int i = 0; i < nThreads / 2; i++) {
            consumers[i] = new SomeConsumer("queue1");
         }

         for (int i = nThreads / 2; i < nThreads; i++) {
            consumers[i] = new SomeConsumer("queue2");
         }

         for (int i = 0; i < nThreads; i++) {
            consumers[i].start();
         }

         for (int i = 0; i < nThreads / 2; i++) {
            producers[i] = new SomeProducer("queue1");
         }

         for (int i = nThreads / 2; i < nThreads; i++) {
            producers[i] = new SomeProducer("queue2");
         }

         for (int i = 0; i < nThreads; i++) {
            producers[i].start();
         }

         for (SomeConsumer consumer : consumers) {
            consumer.join();
            Assert.assertFalse(consumer.failed);
         }

         for (SomeProducer producer : producers) {
            producer.join();
            Assert.assertFalse(producer.failed);
         }

         assertFalse(AssertionLoggerHandler.matchText(".*User queue1\\(queue1\\).* is consuming a message from queue2"));
         assertFalse(AssertionLoggerHandler.matchText(".*User queue2\\(queue2\\).* is consuming a message from queue1"));
         assertTrue(AssertionLoggerHandler.matchText(".*User queue2\\(queue2\\).* is consuming a message from queue2"));
         assertTrue(AssertionLoggerHandler.matchText(".*User queue1\\(queue1\\).* is consuming a message from queue1"));
      }
   }
}
