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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientProducerCreditManagerImpl;
import org.apache.activemq.artemis.core.client.impl.ClientProducerCredits;
import org.apache.activemq.artemis.core.client.impl.ClientProducerInternal;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ProducerFlowControlTest extends ActiveMQTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private ServerLocator locator;

   private ClientSessionFactory sf;

   private ClientSession session;

   private ActiveMQServer server;

   protected boolean isNetty() {
      return false;
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      locator = createFactory(isNetty());
   }

   // TODO need to test crashing a producer with unused credits returns them to the pool

   @Test
   public void testFlowControlSingleConsumer() throws Exception {
      testFlowControl(1000, 500, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   @Test
   public void testFlowControlAnon() throws Exception {
      testFlowControl(1000, 500, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, true);
   }

   @Test
   public void testFlowControlSingleConsumerLargeMaxSize() throws Exception {
      testFlowControl(1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   @Test
   public void testFlowControlMultipleConsumers() throws Exception {
      testFlowControl(1000, 500, -1, 1024, 1024, 1024, 5, 1, 0, false);
   }

   @Test
   public void testFlowControlZeroConsumerWindowSize() throws Exception {
      testFlowControl(1000, 500, 10 * 1024, 1024, 0, 1024, 1, 1, 0, false);
   }

   @Test
   public void testFlowControlZeroAckBatchSize() throws Exception {
      testFlowControl(1000, 500, 10 * 1024, 1024, 1024, 0, 1, 1, 0, false);
   }

   @Test
   public void testFlowControlSingleConsumerSlowConsumer() throws Exception {
      testFlowControl(100, 500, 1024, 512, 512, 512, 1, 1, 10, false);
   }

   @Test
   public void testFlowControlSmallMessages() throws Exception {
      testFlowControl(1000, 0, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   @Test
   public void testFlowControlLargerMessagesSmallWindowSize() throws Exception {
      testFlowControl(1000, 10 * 1024, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   @Test
   public void testFlowControlMultipleProducers() throws Exception {
      testFlowControl(1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 5, 0, false);
   }

   @Test
   public void testFlowControlMultipleProducersAndConsumers() throws Exception {
      testFlowControl(500, 500, 100 * 1024, 1024, 1024, 1024, 1, 3, 3, false);
   }

   @Test
   public void testFlowControlMultipleProducersAnon() throws Exception {
      testFlowControl(1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 5, 0, true);
   }

   @Test
   public void testFlowControlLargeMessages2() throws Exception {
      testFlowControl(1000, 10000, -1, 1024, 0, 0, 1, 1, 0, false, 1000, true);
   }

   @Test
   public void testFlowControlLargeMessages3() throws Exception {
      testFlowControl(1000, 10000, 100 * 1024, 1024, 1024, 0, 1, 1, 0, false, 1000, true);
   }

   @Test
   public void testFlowControlLargeMessages4() throws Exception {
      testFlowControl(1000, 10000, 100 * 1024, 1024, 1024, 1024, 1, 1, 0, false, 1000, true);
   }

   @Test
   public void testFlowControlLargeMessages5() throws Exception {
      testFlowControl(1000, 10000, 100 * 1024, 1024, -1, 1024, 1, 1, 0, false, 1000, true);
   }

   @Test
   public void testFlowControlLargeMessages6() throws Exception {
      testFlowControl(1000, 10000, 100 * 1024, 1024, 1024, 1024, 1, 1, 0, true, 1000, true);
   }

   @Test
   public void testFlowControlLargeMessages7() throws Exception {
      testFlowControl(1000, 10000, 100 * 1024, 1024, 1024, 1024, 2, 2, 0, true, 1000, true);
   }

   private void testFlowControl(final int numMessages,
                                final int messageSize,
                                final int maxSize,
                                final int producerWindowSize,
                                final int consumerWindowSize,
                                final int ackBatchSize,
                                final int numConsumers,
                                final int numProducers,
                                final long consumerDelay,
                                final boolean anon) throws Exception {
      testFlowControl(numMessages, messageSize, maxSize, producerWindowSize, consumerWindowSize, ackBatchSize, numConsumers, numProducers, consumerDelay, anon, -1, false);
   }

   private void testFlowControl(final int numMessages,
                                final int messageSize,
                                final int maxSize,
                                final int producerWindowSize,
                                final int consumerWindowSize,
                                final int ackBatchSize,
                                final int numConsumers,
                                final int numProducers,
                                final long consumerDelay,
                                final boolean anon,
                                final int minLargeMessageSize,
                                final boolean realFiles) throws Exception {
      final SimpleString address = new SimpleString("testaddress");

      server = createServer(realFiles, isNetty());

      AddressSettings addressSettings = new AddressSettings().setMaxSizeBytes(maxSize).setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();
      waitForServerToStart(server);

      locator.setProducerWindowSize(producerWindowSize).setConsumerWindowSize(consumerWindowSize).setAckBatchSize(ackBatchSize);

      if (minLargeMessageSize != -1) {
         locator.setMinLargeMessageSize(minLargeMessageSize);
      }

      sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true, true);

      session.start();

      final String queueName = "testqueue";

      for (int i = 0; i < numConsumers; i++) {
         session.createQueue(address, new SimpleString(queueName + i), null, false);
      }

      final byte[] bytes = RandomUtil.randomBytes(messageSize);

      class MyHandler implements MessageHandler {

         int count = 0;

         final CountDownLatch latch = new CountDownLatch(1);

         volatile Exception exception;

         @Override
         public void onMessage(final ClientMessage message) {
            try {
               byte[] bytesRead = new byte[messageSize];

               message.getBodyBuffer().readBytes(bytesRead);

               ActiveMQTestBase.assertEqualsByteArrays(bytes, bytesRead);

               message.acknowledge();

               if (++count == numMessages * numProducers) {
                  latch.countDown();
               }

               if (consumerDelay > 0) {
                  Thread.sleep(consumerDelay);
               }

            } catch (Exception e) {
               ProducerFlowControlTest.log.error("Failed to handle message", e);

               exception = e;

               latch.countDown();
            }
         }
      }

      MyHandler[] handlers = new MyHandler[numConsumers];

      for (int i = 0; i < numConsumers; i++) {
         handlers[i] = new MyHandler();

         ClientConsumer consumer = session.createConsumer(new SimpleString(queueName + i));

         consumer.setMessageHandler(handlers[i]);
      }

      ClientProducer[] producers = new ClientProducer[numProducers];

      for (int i = 0; i < numProducers; i++) {
         if (anon) {
            producers[i] = session.createProducer();
         } else {
            producers[i] = session.createProducer(address);
         }
      }

      long start = System.currentTimeMillis();

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.getBodyBuffer().writeBytes(bytes);

         for (int j = 0; j < numProducers; j++) {
            if (anon) {
               producers[j].send(address, message);
            } else {
               producers[j].send(message);
            }

         }
      }

      for (int i = 0; i < numConsumers; i++) {
         Assert.assertTrue(handlers[i].latch.await(5, TimeUnit.MINUTES));

         Assert.assertNull(handlers[i].exception);
      }

      long end = System.currentTimeMillis();

      double rate = 1000 * (double) numMessages / (end - start);

      ProducerFlowControlTest.log.info("rate is " + rate + " msgs / sec");
   }

   @Test
   public void testClosingSessionUnblocksBlockedProducer() throws Exception {
      final SimpleString address = new SimpleString("testaddress");

      server = createServer(false, isNetty());

      AddressSettings addressSettings = new AddressSettings().setMaxSizeBytes(1024).setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();
      waitForServerToStart(server);

      locator.setProducerWindowSize(1024).setConsumerWindowSize(1024).setAckBatchSize(1024);

      sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true, true);

      final SimpleString queueName = new SimpleString("testqueue");

      session.createQueue(address, queueName, null, false);

      ClientProducer producer = session.createProducer(address);

      byte[] bytes = new byte[2000];

      ClientMessage message = session.createMessage(false);

      message.getBodyBuffer().writeBytes(bytes);

      final AtomicBoolean closed = new AtomicBoolean(false);

      Thread t = new Thread(new Runnable() {
         @Override
         public void run() {
            try {
               Thread.sleep(500);

               closed.set(true);

               session.close();
            } catch (Exception e) {
            }
         }
      });

      t.start();

      try {
         // This will block
         for (int i = 0; i < 10; i++) {
            producer.send(message);
         }
      } catch (ActiveMQObjectClosedException expected) {
      }

      Assert.assertTrue(closed.get());

      t.join();
   }

   @Test
   public void testFlowControlMessageNotRouted() throws Exception {
      final SimpleString address = new SimpleString("testaddress");

      server = createServer(false, isNetty());

      AddressSettings addressSettings = new AddressSettings().setMaxSizeBytes(1024).setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();
      waitForServerToStart(server);

      locator.setProducerWindowSize(1024).setConsumerWindowSize(1024).setAckBatchSize(1024);

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true, true);

      ClientProducer producer = session.createProducer(address);

      byte[] bytes = new byte[100];

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.getBodyBuffer().writeBytes(bytes);

         producer.send(message);
      }
   }

   // Not technically a flow control test, but what the hell
   @Test
   public void testMultipleConsumers() throws Exception {
      server = createServer(false, isNetty());

      server.start();
      waitForServerToStart(server);

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);
      session.createQueue("address", "queue2", null, false);
      session.createQueue("address", "queue3", null, false);
      session.createQueue("address", "queue4", null, false);
      session.createQueue("address", "queue5", null, false);

      ClientConsumer consumer1 = session.createConsumer("queue1");
      ClientConsumer consumer2 = session.createConsumer("queue2");
      ClientConsumer consumer3 = session.createConsumer("queue3");
      ClientConsumer consumer4 = session.createConsumer("queue4");
      ClientConsumer consumer5 = session.createConsumer("queue5");

      ClientProducer producer = session.createProducer("address");

      byte[] bytes = new byte[2000];

      ClientMessage message = session.createMessage(false);

      message.getBodyBuffer().writeBytes(bytes);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++) {
         producer.send(message);
      }

      session.start();

      for (int i = 0; i < numMessages; i++) {
         ClientMessage msg = consumer1.receive(1000);

         Assert.assertNotNull(msg);

         msg = consumer2.receive(5000);

         Assert.assertNotNull(msg);

         msg = consumer3.receive(5000);

         Assert.assertNotNull(msg);

         msg = consumer4.receive(5000);

         Assert.assertNotNull(msg);

         msg = consumer5.receive(5000);

         Assert.assertNotNull(msg);
      }
   }

   @Test
   public void testProducerCreditsCaching1() throws Exception {
      server = createServer(false, isNetty());

      server.start();
      waitForServerToStart(server);

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducerCredits credits = null;

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE * 2; i++) {
         ClientProducer prod = session.createProducer("address");

         ClientProducerCredits newCredits = ((ClientProducerInternal) prod).getProducerCredits();

         if (credits != null) {
            Assert.assertTrue(newCredits == credits);
         }

         credits = newCredits;

         Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
         Assert.assertEquals(0, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());
      }
   }

   @Test
   public void testProducerCreditsCaching2() throws Exception {
      server = createServer(false, isNetty());

      server.start();
      waitForServerToStart(server);
      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducerCredits credits = null;

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE * 2; i++) {
         ClientProducer prod = session.createProducer("address");

         ClientProducerCredits newCredits = ((ClientProducerInternal) prod).getProducerCredits();

         if (credits != null) {
            Assert.assertTrue(newCredits == credits);
         }

         credits = newCredits;

         prod.close();

         Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
         Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());
      }
   }

   @Test
   public void testProducerCreditsCaching3() throws Exception {
      server = createServer(false, isNetty());

      server.start();
      waitForServerToStart(server);

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducerCredits credits = null;

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++) {
         ClientProducer prod = session.createProducer("address" + i);

         ClientProducerCredits newCredits = ((ClientProducerInternal) prod).getProducerCredits();

         if (credits != null) {
            Assert.assertFalse(newCredits == credits);
         }

         credits = newCredits;

         Assert.assertEquals(i + 1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
         Assert.assertEquals(0, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());
      }
   }

   @Test
   public void testProducerCreditsCaching4() throws Exception {
      server = createServer(false, isNetty());

      server.start();
      waitForServerToStart(server);
      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducerCredits credits = null;

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++) {
         ClientProducer prod = session.createProducer("address" + i);

         ClientProducerCredits newCredits = ((ClientProducerInternal) prod).getProducerCredits();

         if (credits != null) {
            Assert.assertFalse(newCredits == credits);
         }

         credits = newCredits;

         prod.close();

         Assert.assertEquals(i + 1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
         Assert.assertEquals(i + 1, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());
      }
   }

   @Test
   public void testProducerCreditsCaching5() throws Exception {
      server = createServer(false, isNetty());

      server.start();
      waitForServerToStart(server);

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducerCredits credits = null;

      List<ClientProducerCredits> creditsList = new ArrayList<>();

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++) {
         ClientProducer prod = session.createProducer("address" + i);

         ClientProducerCredits newCredits = ((ClientProducerInternal) prod).getProducerCredits();

         if (credits != null) {
            Assert.assertFalse(newCredits == credits);
         }

         credits = newCredits;

         Assert.assertEquals(i + 1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
         Assert.assertEquals(0, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());

         creditsList.add(credits);
      }

      Iterator<ClientProducerCredits> iter = creditsList.iterator();

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++) {
         ClientProducer prod = session.createProducer("address" + i);

         ClientProducerCredits newCredits = ((ClientProducerInternal) prod).getProducerCredits();

         Assert.assertTrue(newCredits == iter.next());

         Assert.assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
         Assert.assertEquals(0, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());
      }

      for (int i = 0; i < 10; i++) {
         session.createProducer("address" + (i + ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE));

         Assert.assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE + i + 1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
         Assert.assertEquals(0, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());
      }
   }

   @Test
   public void testProducerCreditsCaching6() throws Exception {
      server = createServer(false, isNetty());

      server.start();
      waitForServerToStart(server);
      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++) {
         ClientProducer prod = session.createProducer((String) null);

         prod.send("address", session.createMessage(false));

         Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
         Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());
      }
   }

   @Test
   public void testProducerCreditsCaching7() throws Exception {
      server = createServer(false, isNetty());

      server.start();
      waitForServerToStart(server);

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++) {
         ClientProducer prod = session.createProducer((String) null);

         prod.send("address" + i, session.createMessage(false));

         Assert.assertEquals(i + 1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
         Assert.assertEquals(i + 1, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());
      }

      for (int i = 0; i < 10; i++) {
         ClientProducer prod = session.createProducer((String) null);

         prod.send("address" + i, session.createMessage(false));

         Assert.assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
         Assert.assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());
      }

      for (int i = 0; i < 10; i++) {
         ClientProducer prod = session.createProducer((String) null);

         prod.send("address2-" + i, session.createMessage(false));

         Assert.assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
         Assert.assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());
      }
   }

   @Test
   public void testProducerCreditsRefCounting() throws Exception {
      server = createServer(false, isNetty());

      server.start();
      waitForServerToStart(server);

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducer prod1 = session.createProducer("address");
      Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
      Assert.assertEquals(0, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());

      ClientProducer prod2 = session.createProducer("address");
      Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
      Assert.assertEquals(0, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());

      ClientProducer prod3 = session.createProducer("address");
      Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
      Assert.assertEquals(0, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());

      prod1.close();

      Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
      Assert.assertEquals(0, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());

      prod2.close();

      Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
      Assert.assertEquals(0, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());

      prod3.close();

      Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().creditsMapSize());
      Assert.assertEquals(1, ((ClientSessionInternal) session).getProducerCreditManager().unReferencedCreditsSize());
   }

}
