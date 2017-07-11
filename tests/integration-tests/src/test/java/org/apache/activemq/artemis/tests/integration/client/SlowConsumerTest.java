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

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.SlowConsumerPolicy;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.TimeUtils;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class SlowConsumerTest extends ActiveMQTestBase {

   private static final Logger logger = Logger.getLogger(SlowConsumerTest.class);

   int threshold = 10;
   private boolean isNetty = false;
   private boolean isPaging = false;

   // this will ensure that all tests in this class are run twice,
   // once with "true" passed to the class' constructor and once with "false"
   @Parameterized.Parameters(name = "netty={0}, paging={1}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{true, false}, {false, false}, {true, true}, {false, true}});
   }

   public SlowConsumerTest(boolean isNetty, boolean isPaging) {
      this.isNetty = isNetty;
      this.isPaging = isPaging;
   }

   private ActiveMQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   private ServerLocator locator;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(true, isNetty);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setSlowConsumerCheckPeriod(1);
      addressSettings.setSlowConsumerThreshold(threshold);
      addressSettings.setSlowConsumerPolicy(SlowConsumerPolicy.KILL);

      if (isPaging) {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
         addressSettings.setMaxSizeBytes(10 * 1024);
         addressSettings.setPageSizeBytes(1024);
      } else {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
         addressSettings.setMaxSizeBytes(-1);
         addressSettings.setPageSizeBytes(1024);

      }

      server.start();

      server.getAddressSettingsRepository().addMatch(QUEUE.toString(), addressSettings);

      server.createQueue(QUEUE, RoutingType.ANYCAST, QUEUE, null, true, false).getPageSubscription().getPagingStore().startPaging();

      locator = createFactory(isNetty);
   }

   @Test
   public void testSlowConsumerKilled() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, true, true, false));

      ClientProducer producer = addClientProducer(session.createProducer(QUEUE));

      assertPaging();

      final int numMessages = 25;

      for (int i = 0; i < numMessages; i++) {
         producer.send(createTextMessage(session, "m" + i));
      }

      ClientConsumer consumer = addClientConsumer(session.createConsumer(QUEUE));
      session.start();

      Thread.sleep(3000);

      try {
         consumer.receiveImmediate();
         fail();
      } catch (ActiveMQObjectClosedException e) {
         assertEquals(e.getType(), ActiveMQExceptionType.OBJECT_CLOSED);
      }
   }

   private void assertPaging() throws Exception {
      Queue queue = server.locateQueue(QUEUE);
      if (isPaging) {
         Assert.assertTrue(queue.getPageSubscription().isPaging());
      } else {
         Assert.assertFalse(queue.getPageSubscription().isPaging());
      }
   }

   @Test
   public void testSlowConsumerNotification() throws Exception {

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, true, true, false));

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setSlowConsumerCheckPeriod(2);
      addressSettings.setSlowConsumerThreshold(10);
      addressSettings.setSlowConsumerPolicy(SlowConsumerPolicy.NOTIFY);
      if (!isPaging) {
         addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
         addressSettings.setMaxSizeBytes(-1);
      }

      server.getAddressSettingsRepository().removeMatch(QUEUE.toString());
      server.getAddressSettingsRepository().addMatch(QUEUE.toString(), addressSettings);

      assertPaging();

      ClientProducer producer = addClientProducer(session.createProducer(QUEUE));

      final int numMessages = 25;

      for (int i = 0; i < numMessages; i++) {
         producer.send(createTextMessage(session, "m" + i));
      }

      SimpleString notifQueue = RandomUtil.randomSimpleString();

      session.createQueue(ActiveMQDefaultConfiguration.getDefaultManagementNotificationAddress(), notifQueue, null, false);

      ClientConsumer notifConsumer = session.createConsumer(notifQueue.toString(), ManagementHelper.HDR_NOTIFICATION_TYPE + "='" + CoreNotificationType.CONSUMER_SLOW + "'");

      final CountDownLatch notifLatch = new CountDownLatch(1);

      notifConsumer.setMessageHandler(new MessageHandler() {
         @Override
         public void onMessage(ClientMessage message) {
            assertEquals(CoreNotificationType.CONSUMER_SLOW.toString(), message.getObjectProperty(ManagementHelper.HDR_NOTIFICATION_TYPE).toString());
            assertEquals(QUEUE.toString(), message.getObjectProperty(ManagementHelper.HDR_ADDRESS).toString());
            assertEquals(Integer.valueOf(1), message.getIntProperty(ManagementHelper.HDR_CONSUMER_COUNT));
            if (isNetty) {
               assertTrue(message.getSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS).toString().startsWith("/127.0.0.1"));
            } else {
               assertEquals(SimpleString.toSimpleString("invm:0"), message.getSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS));
            }
            assertNotNull(message.getSimpleStringProperty(ManagementHelper.HDR_CONNECTION_NAME));
            assertNotNull(message.getSimpleStringProperty(ManagementHelper.HDR_CONSUMER_NAME));
            assertNotNull(message.getSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME));
            try {
               message.acknowledge();
            } catch (ActiveMQException e) {
               e.printStackTrace();
            }
            notifLatch.countDown();
         }
      });

      ClientConsumer consumer = addClientConsumer(session.createConsumer(QUEUE));
      session.start();

      assertTrue(notifLatch.await(15, TimeUnit.SECONDS));
   }

   @Test
   public void testSlowConsumerSpared() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(true, true));

      ClientProducer producer = addClientProducer(session.createProducer(QUEUE));

      final int numMessages = 5;

      for (int i = 0; i < numMessages; i++) {
         producer.send(createTextMessage(session, "m" + i));
      }

      assertPaging();

      ClientConsumer consumer = addClientConsumer(session.createConsumer(QUEUE));
      session.start();

      Thread.sleep(3000);

      for (int i = 0; i < numMessages; i++) {
         assertNotNull(consumer.receive(500));
      }
   }

   @Test
   public void testFastThenSlowConsumerSpared() throws Exception {
      locator.setAckBatchSize(0);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(true, true));

      final ClientSession producerSession = addClientSession(sf.createSession(true, true));

      final ClientProducer producer = addClientProducer(producerSession.createProducer(QUEUE));

      final AtomicLong messagesProduced = new AtomicLong(0);

      Thread t = new Thread(new Runnable() {
         @Override
         public void run() {
            long start = System.currentTimeMillis();
            ClientMessage m = createTextMessage(producerSession, "m", true);

            // send messages as fast as possible for 3 seconds
            while (System.currentTimeMillis() < (start + 3000)) {
               try {
                  producer.send(m);
                  messagesProduced.incrementAndGet();
               } catch (ActiveMQException e) {
                  e.printStackTrace();
                  return;
               }
            }

            start = System.currentTimeMillis();

            // send 1 msg/second for 10 seconds
            while (System.currentTimeMillis() < (start + 10000)) {
               try {
                  producer.send(m);
                  messagesProduced.incrementAndGet();
                  Thread.sleep(1000);
               } catch (Exception e) {
                  e.printStackTrace();
                  return;
               }
            }
         }
      });

      t.start();

      assertPaging();

      ClientConsumer consumer = addClientConsumer(session.createConsumer(QUEUE));
      session.start();

      ClientMessage m = null;
      long messagesConsumed = 0;

      do {
         m = consumer.receive(1500);
         if (m != null) {
            m.acknowledge();
            messagesConsumed++;
         }
      }
      while (m != null);

      assertEquals(messagesProduced.longValue(), messagesConsumed);
   }

   @Test
   public void testSlowWildcardConsumer() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setSlowConsumerCheckPeriod(2);
      addressSettings.setSlowConsumerThreshold(10);
      addressSettings.setSlowConsumerPolicy(SlowConsumerPolicy.KILL);

      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, true, true, false));
      session.createQueue(addressAB, queueName1, null, false);
      session.createQueue(addressAC, queueName2, null, false);
      session.createQueue(address, queueName, null, false);
      ClientProducer producer = session.createProducer(addressAB);
      ClientProducer producer2 = session.createProducer(addressAC);

      final int numMessages = 20;

      for (int i = 0; i < numMessages; i++) {
         producer.send(createTextMessage(session, "m1" + i));
         producer2.send(createTextMessage(session, "m2" + i));
      }

      ClientConsumer consumer = addClientConsumer(session.createConsumer(queueName));
      session.start();

      Thread.sleep(3000);

      try {
         consumer.receiveImmediate();
         fail();
      } catch (ActiveMQObjectClosedException e) {
         assertEquals(e.getType(), ActiveMQExceptionType.OBJECT_CLOSED);
      }
   }

   /**
    * This test creates 3 consumers on one queue. A producer sends
    * messages at a rate of 2 messages per second. Each consumer
    * consumes messages at rate of 1 message per second. The slow
    * consumer threshold is 1 message per second.
    * Based on the above settings, slow consumer removal will not
    * be performed (2 < 3*1), so no consumer will be removed during the
    * test, and all messages will be received.
    */
   @Test
   public void testMultipleConsumersOneQueue() throws Exception {
      locator.setAckBatchSize(0);

      Queue queue = server.locateQueue(QUEUE);

      ClientSessionFactory sf1 = createSessionFactory(locator);
      ClientSessionFactory sf2 = createSessionFactory(locator);
      ClientSessionFactory sf3 = createSessionFactory(locator);
      ClientSessionFactory sf4 = createSessionFactory(locator);

      final int messages = 10 * threshold;

      FixedRateProducer producer = new FixedRateProducer(threshold * 2, sf1, QUEUE, messages);

      final Set<FixedRateConsumer> consumers = new ConcurrentHashSet<>();
      final Set<ClientMessage> receivedMessages = new ConcurrentHashSet<>();

      consumers.add(new FixedRateConsumer(threshold, receivedMessages, sf2, QUEUE, 1));
      consumers.add(new FixedRateConsumer(threshold, receivedMessages, sf3, QUEUE, 2));
      consumers.add(new FixedRateConsumer(threshold, receivedMessages, sf4, QUEUE, 3));

      try {
         producer.start();

         for (FixedRateConsumer consumer : consumers) {
            consumer.start();
         }

         producer.join(10000);

         assertTrue(TimeUtils.waitOnBoolean(true, 10000, () -> receivedMessages.size() == messages));

         Assert.assertEquals(3, queue.getConsumerCount());

      } finally {
         producer.stopRunning();
         Assert.assertFalse(producer.failed);
         for (FixedRateConsumer consumer : consumers) {
            consumer.stopRunning();
            Assert.assertFalse(consumer.failed);
         }
         logger.debug("***report messages received: " + receivedMessages.size());
         logger.debug("***consumers left: " + consumers.size());
      }
   }

   private class FixedRateProducer extends FixedRateClient {

      int messages;
      ClientProducer producer;

      FixedRateProducer(int rate, ClientSessionFactory sf, SimpleString queue, int messages) throws ActiveMQException {
         super(sf, queue, rate);
         this.messages = messages;
      }

      @Override
      protected void prepareWork() throws ActiveMQException {
         super.prepareWork();
         this.producer = session.createProducer(queue);
      }

      @Override
      protected void doWork(int count) throws Exception {

         if (count < messages) {
            ClientMessage m = createTextMessage(session, "msg" + count);
            producer.send(m);
            logger.debug("producer sent a message " + count);
         } else {
            this.working = false;
         }
      }

      @Override
      public String toString() {
         return "Producer";
      }
   }

   private class FixedRateConsumer extends FixedRateClient {

      Set<FixedRateConsumer> consumers;
      ClientConsumer consumer;
      final Set<ClientMessage> receivedMessages;
      int id;

      FixedRateConsumer(int rate,
                        Set<ClientMessage> receivedMessages,
                        ClientSessionFactory sf,
                        SimpleString queue,
                        int id) throws ActiveMQException {
         super(sf, queue, rate);
         this.id = id;
         this.receivedMessages = receivedMessages;
      }

      @Override
      protected void prepareWork() throws ActiveMQException {
         super.prepareWork();
         this.consumer = session.createConsumer(queue);
         this.session.start();
      }

      @Override
      protected void doWork(int count) throws Exception {
         ClientMessage m = this.consumer.receive(1000);
         logger.debug("consumer " + id + " got m: " + m);
         if (m != null) {
            receivedMessages.add(m);
            m.acknowledge();
            logger.debug(" consumer " + id + " acked " + m.getClass().getName() + "now total received: " + receivedMessages.size());
         }
      }

      @Override
      protected void handleError(int count, Exception e) {
         failed = true;
         System.err.println("Got error receiving message " + count + " remove self " + this.id);
         e.printStackTrace();
      }

      @Override
      public String toString() {
         return "Consumer " + id;
      }

   }

   private abstract class FixedRateClient extends Thread {

      protected ClientSessionFactory sf;
      protected SimpleString queue;
      protected ClientSession session;
      protected final int sleepTime;
      protected volatile boolean working;
      boolean failed;

      FixedRateClient(ClientSessionFactory sf, SimpleString queue, int rate) throws ActiveMQException {
         this.sf = sf;
         this.queue = queue;
         this.sleepTime = 1000 / rate;
      }

      protected void prepareWork() throws ActiveMQException {
         this.session = addClientSession(sf.createSession(true, true));
      }

      @Override
      public void run() {
         working = true;
         try {
            prepareWork();
         } catch (ActiveMQException e) {
            logger.debug("got error in prepareWork(), aborting...");
            e.printStackTrace();
            return;
         }
         int count = 0;
         while (working) {
            try {
               doWork(count);
               Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
               // expected, nothing to be done
            } catch (Exception e) {
               failed = true;
               handleError(count, e);
               working = false;
            } finally {
               count++;
            }
         }
      }

      protected abstract void doWork(int count) throws Exception;

      protected void handleError(int count, Exception e) {
      }

      public void stopRunning() {
         working = false;
         try {
            session.close();
            this.interrupt();
            join(5000);
            if (isAlive()) {
               fail("Interrupt is not working on Working Thread");
            }
         } catch (InterruptedException e) {
            e.printStackTrace();
         } catch (Exception e) {
            handleError(0, e);
         }
      }
   }

}
