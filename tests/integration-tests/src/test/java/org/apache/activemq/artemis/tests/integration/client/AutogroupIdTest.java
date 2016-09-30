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
package org.apache.activemq.artemis.tests.integration.client;

import java.util.concurrent.CountDownLatch;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AutogroupIdTest extends ActiveMQTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");

   private final SimpleString groupTestQ = new SimpleString("testGroupQueue");

   private ActiveMQServer server;

   private ServerLocator locator;

   /* auto group id tests*/

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false);

      server.start();

      waitForServerToStart(server);

      locator = createInVMNonHALocator();
   }

   /*
   * tests when the autogroupid is set only 1 consumer (out of 2) gets all the messages from a single producer
   * */

   @Test
   public void testGroupIdAutomaticallySet() throws Exception {
      locator.setAutoGroup(true);
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(groupTestQ, groupTestQ, null, false);

      ClientProducer producer = session.createProducer(groupTestQ);

      final CountDownLatch latch = new CountDownLatch(100);

      MyMessageHandler myMessageHandler = new MyMessageHandler(latch);
      MyMessageHandler myMessageHandler2 = new MyMessageHandler(latch);

      ClientConsumer consumer = session.createConsumer(groupTestQ);
      consumer.setMessageHandler(myMessageHandler);
      ClientConsumer consumer2 = session.createConsumer(groupTestQ);
      consumer2.setMessageHandler(myMessageHandler2);

      log.info("starting session");

      session.start();

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         producer.send(session.createMessage(false));
      }
      waitForLatch(latch);

      session.close();

      log.info(myMessageHandler2.messagesReceived);

      Assert.assertEquals(100, myMessageHandler.messagesReceived);
      Assert.assertEquals(0, myMessageHandler2.messagesReceived);

   }

   /*
   * tests when the autogroupid is set only 2 consumers (out of 3) gets all the messages from 2 producers
   * */
   @Test
   public void testGroupIdAutomaticallySetMultipleProducers() throws Exception {
      locator.setAutoGroup(true);
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(groupTestQ, groupTestQ, null, false);

      ClientProducer producer = session.createProducer(groupTestQ);
      ClientProducer producer2 = session.createProducer(groupTestQ);

      final CountDownLatch latch = new CountDownLatch(200);

      MyMessageHandler myMessageHandler = new MyMessageHandler(latch);
      MyMessageHandler myMessageHandler2 = new MyMessageHandler(latch);
      MyMessageHandler myMessageHandler3 = new MyMessageHandler(latch);

      ClientConsumer consumer = session.createConsumer(groupTestQ);
      consumer.setMessageHandler(myMessageHandler);
      ClientConsumer consumer2 = session.createConsumer(groupTestQ);
      consumer2.setMessageHandler(myMessageHandler2);
      ClientConsumer consumer3 = session.createConsumer(groupTestQ);
      consumer3.setMessageHandler(myMessageHandler3);

      session.start();

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         producer.send(session.createMessage(false));
      }
      for (int i = 0; i < numMessages; i++) {
         producer2.send(session.createMessage(false));
      }
      waitForLatch(latch);

      session.close();

      Assert.assertEquals(myMessageHandler.messagesReceived, 100);
      Assert.assertEquals(myMessageHandler2.messagesReceived, 100);
      Assert.assertEquals(myMessageHandler3.messagesReceived, 0);
   }

   /*
   * tests that even though we have a grouping round robin distributor we don't pin the consumer as autogroup is false
   * */
   @Test
   public void testGroupIdAutomaticallyNotSet() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(groupTestQ, groupTestQ, null, false);

      ClientProducer producer = session.createProducer(groupTestQ);

      final CountDownLatch latch = new CountDownLatch(100);

      MyMessageHandler myMessageHandler = new MyMessageHandler(latch);
      MyMessageHandler myMessageHandler2 = new MyMessageHandler(latch);

      ClientConsumer consumer = session.createConsumer(groupTestQ);
      consumer.setMessageHandler(myMessageHandler);
      ClientConsumer consumer2 = session.createConsumer(groupTestQ);
      consumer2.setMessageHandler(myMessageHandler2);

      session.start();

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         producer.send(session.createMessage(false));
      }
      waitForLatch(latch);

      session.close();

      Assert.assertEquals(50, myMessageHandler.messagesReceived);
      Assert.assertEquals(50, myMessageHandler2.messagesReceived);

   }

   private static class MyMessageHandler implements MessageHandler {

      int messagesReceived = 0;

      private final CountDownLatch latch;

      private MyMessageHandler(final CountDownLatch latch) {
         this.latch = latch;
      }

      @Override
      public void onMessage(final ClientMessage message) {
         messagesReceived++;
         try {
            message.acknowledge();
         } catch (ActiveMQException e) {
            e.printStackTrace();
         }
         latch.countDown();
      }
   }
}
