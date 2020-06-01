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

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class OrphanedConsumerTest extends ActiveMQTestBase {
   private static final Logger log = Logger.getLogger(OrphanedConsumerTest.class);

   private static void debugLog(String message) {
      log.debug(message);
   }

   private static boolean conditionActive = true;

   public static final boolean isConditionActive() {
      return conditionActive;
   }

   public static final void setConditionActive(boolean _conditionActive) {
      conditionActive = _conditionActive;
   }

   public static void throwException() throws Exception {
      throw new InterruptedException("nice.. I interrupted this!");
   }

   private ActiveMQServer server;

   private ServerLocator locator;

   static ActiveMQServer staticServer;

   /**
    * {@link #leavingCloseOnTestCountersWhileClosing()} will set this in case of any issues.
    * the test must then validate for this being null
    */
   static AssertionError verification;

   /**
    * This static method is an entry point for the byteman rules on {@link #testOrphanedConsumers()}
    */
   public static void leavingCloseOnTestCountersWhileClosing() {
      if (staticServer.getSessions().size() == 0) {
         verification = new AssertionError("The session was closed before the consumers, this may cause issues on management leaving Orphaned Consumers!");
      }
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      setConditionActive(true);
      /** I'm using the internal method here because closing
       *  this locator on tear down would hang.
       *  as we are tweaking with the internal state and making it fail */
      locator = internalCreateNonHALocator(true);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      super.tearDown();
      setConditionActive(false);

      staticServer = null;
   }

   /**
    * This is like being two tests in one:
    * I - validating that any exception during the close wouldn't stop connection from being closed
    * II - validating that the connection is only removed at the end of the process and you wouldn't see
    * inconsistencies on management
    *
    * @throws Exception
    */
   @Test
   @BMRules(
      rules = {@BMRule(
         name = "closeExit",
         targetClass = "org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl",
         targetMethod = "close",
         targetLocation = "AT EXIT",
         condition = "org.apache.activemq.artemis.tests.extras.byteman.OrphanedConsumerTest.isConditionActive()",
         action = "org.apache.activemq.artemis.tests.extras.byteman.OrphanedConsumerTest.debugLog(\"throwing stuff\");throw new InterruptedException()"), @BMRule(
         name = "closeEnter",
         targetClass = "org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl",
         targetMethod = "close",
         targetLocation = "ENTRY",
         condition = "org.apache.activemq.artemis.tests.extras.byteman.OrphanedConsumerTest.isConditionActive()",
         action = "org.apache.activemq.artemis.tests.extras.byteman.OrphanedConsumerTest.leavingCloseOnTestCountersWhileClosing()")})
   public void testOrphanedConsumers() throws Exception {
      internalTestOrphanedConsumers(false);
   }

   /**
    * This is like being two tests in one:
    * I - validating that any exception during the close wouldn't stop connection from being closed
    * II - validating that the connection is only removed at the end of the process and you wouldn't see
    * inconsistencies on management
    *
    * @throws Exception
    */
   @Test
   @BMRules(
      rules = {@BMRule(
         name = "closeExit",
         targetClass = "org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl",
         targetMethod = "close",
         targetLocation = "AT EXIT",
         condition = "org.apache.activemq.artemis.tests.extras.byteman.OrphanedConsumerTest.isConditionActive()",
         action = "org.apache.activemq.artemis.tests.extras.byteman.OrphanedConsumerTest.debugLog(\"throwing stuff\");throw new InterruptedException()"), @BMRule(
         name = "closeEnter",
         targetClass = "org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl",
         targetMethod = "close",
         targetLocation = "ENTRY",
         condition = "org.apache.activemq.artemis.tests.extras.byteman.OrphanedConsumerTest.isConditionActive()",
         action = "org.apache.activemq.artemis.tests.extras.byteman.OrphanedConsumerTest.leavingCloseOnTestCountersWhileClosing()")})
   public void testOrphanedConsumersByManagement() throws Exception {
      internalTestOrphanedConsumers(true);
   }

   /**
    * @param useManagement true = it will use a management operation to make the connection failure, false through ping
    * @throws Exception
    */
   private void internalTestOrphanedConsumers(boolean useManagement) throws Exception {
      final int NUMBER_OF_MESSAGES = 2;
      server = createServer(true, true);
      server.start();
      staticServer = server;

      // We are not interested on consumer-window-size on this test
      // We want that every message is delivered
      // as we asserting for number of consumers available and round-robin on delivery
      locator.setConsumerWindowSize(-1).setBlockOnNonDurableSend(false).setBlockOnDurableSend(false).setBlockOnAcknowledge(true).setConnectionTTL(1000).setClientFailureCheckPeriod(100).setReconnectAttempts(0);

      ClientSessionFactoryImpl sf = (ClientSessionFactoryImpl) createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true, 0);

      session.createQueue(new QueueConfiguration("queue1").setAddress("queue"));
      session.createQueue(new QueueConfiguration("queue2").setAddress("queue"));

      ClientProducer prod = session.createProducer("queue");

      ClientConsumer consumer = session.createConsumer("queue1");
      ClientConsumer consumer2 = session.createConsumer("queue2");

      Queue queue1 = server.locateQueue(new SimpleString("queue1"));

      Queue queue2 = server.locateQueue(new SimpleString("queue2"));

      session.start();

      if (!useManagement) {
         sf.stopPingingAfterOne();

         for (long timeout = System.currentTimeMillis() + 6000; timeout > System.currentTimeMillis() && server.getConnectionCount() != 0; ) {
            Thread.sleep(100);
         }

         // an extra second to avoid races of something closing the session while we are asserting it
         Thread.sleep(1000);
      } else {
         server.getActiveMQServerControl().closeConnectionsForAddress("127.0.0.1");
      }

      if (verification != null) {
         throw verification;
      }

      assertEquals(0, queue1.getConsumerCount());
      assertEquals(0, queue2.getConsumerCount());

      setConditionActive(false);

      locator = internalCreateNonHALocator(true).setBlockOnNonDurableSend(false).setBlockOnDurableSend(false).setBlockOnAcknowledge(true).setReconnectAttempts(0).setConsumerWindowSize(-1);

      sf = (ClientSessionFactoryImpl) locator.createSessionFactory();
      session = sf.createSession(true, true, 0);

      session.start();

      prod = session.createProducer("queue");

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         ClientMessage message = session.createMessage(true);
         message.putIntProperty("i", i);
         prod.send(message);
      }

      consumer = session.createConsumer("queue1");
      consumer2 = session.createConsumer("queue2");

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         assertNotNull(consumer.receive(5000));
         assertNotNull(consumer2.receive(5000));
      }

      session.close();
   }

}
