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
package org.apache.activemq.artemis.tests.integration.paging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

public class MessagesExpiredPagingTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String ADDRESS = "MessagesExpiredPagingTest";

   private static final int NUMBER_OF_QUEUES = 10;

   private AtomicBoolean running = new AtomicBoolean(true);

   ActiveMQServer server;

   protected static final int PAGE_MAX = 100 * 1024;

   protected static final int PAGE_SIZE = 10 * 1024;


   Queue[] queues = new Queue[NUMBER_OF_QUEUES];
   ExecutorService expiresExecutor;


   @After
   @Override
   public void tearDown() throws Exception {
      running.set(false);
      expiresExecutor.shutdown();
      Assert.assertTrue(expiresExecutor.awaitTermination(10, TimeUnit.SECONDS));
      super.tearDown();
   }

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      expiresExecutor = Executors.newFixedThreadPool(NUMBER_OF_QUEUES);

      Configuration config = createDefaultConfig(0, true).setJournalSyncNonTransactional(false);

      config.setMessageExpiryScanPeriod(-1);
      server = createServer(true, config, PAGE_SIZE, PAGE_MAX);

      server.getAddressSettingsRepository().clear();

      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(PAGE_SIZE).setMaxSizeBytes(PAGE_MAX).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE).setAutoCreateAddresses(false).setAutoCreateQueues(false);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);


      server.start();


      server.addAddressInfo(new AddressInfo(ADDRESS).addRoutingType(RoutingType.MULTICAST));

      for (int i = 0; i < NUMBER_OF_QUEUES; i++) {
         Queue queue = server.createQueue(new QueueConfiguration("q" + i).setRoutingType(RoutingType.MULTICAST).setAddress(ADDRESS));
         queues[i] = queue;
         expiresExecutor.execute(() -> {
            Thread.currentThread().setName("Expiry on " + queue.getName() + ".." + Thread.currentThread().getName());
            while (running.get()) {
               try {
                  // I am exagerating calls into expireReferences
                  // as I am trying to break things.
                  // Notice I'm not using the scan expiry from PostOffice, but I'm calling this from multiple places trying to really break stuff.
                  queue.expireReferences();
                  Thread.sleep(10);
               } catch (Throwable e) {
                  logger.warn(e.getMessage(), e);
               }
            }
         });
      }
   }

   @Test
   public void testSendReceiveCORELarge() throws Exception {
      testSendReceive("CORE", 50, 20, 10, 500 * 1024);
   }

   @Test
   public void testSendReceiveCORE() throws Exception {
      testSendReceive("CORE", 5000, 1000, 100, 0);
   }

   @Test
   public void testSendReceiveAMQP() throws Exception {
      testSendReceive("AMQP", 5000, 1000, 100, 0);
   }

   @Test
   public void testSendReceiveAMQPLarge() throws Exception {
      testSendReceive("AMQP", 50, 20, 10, 500 * 1024);
   }

   @Test
   public void testSendReceiveOpenWire() throws Exception {
      testSendReceive("OPENWIRE", 5000, 1000, 100, 0);
   }

   public void testSendReceive(String protocol, int numberOfMessages, int numberOfMessageSecondWave, int pagingInterval, int bodySize) throws Exception {
      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      String extraBody;
      {
         StringBuffer buffer = new StringBuffer();
         for (int i = 0; i < bodySize; i++) {
            buffer.append("*");
         }
         extraBody = buffer.toString();
      }


      Consumer[] consumers = new Consumer[NUMBER_OF_QUEUES];

      for (int i = 0; i < NUMBER_OF_QUEUES; i++) {
         consumers[i] = new Consumer(factory, "q" + i, bodySize);
         consumers[i].start();
      }

      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Topic jmsTopic = session.createTopic(ADDRESS);

         MessageProducer producer = session.createProducer(jmsTopic);
         for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("hello" + extraBody));
         }

         // just validating basic queue consumption working
         for (Consumer c : consumers) {
            Wait.assertEquals(10, c.consumed::get);
         }

         for (Consumer c : consumers) {
            c.consumedDelta.set(0);
         }
         producer.setTimeToLive(10);
         for (int i = 0; i < numberOfMessages; i++) {
            if (i > 0 && i % pagingInterval == 0) {
               for (Consumer c : consumers) {
                  producer.setTimeToLive(TimeUnit.HOURS.toMillis(1));
                  producer.send(session.createTextMessage("hello" + extraBody));
                  Wait.assertTrue(() -> c.consumedDelta.get() > 0);
                  producer.setTimeToLive(10);
                  c.consumedDelta.set(0);
               }
               queues[0].getPagingStore().forceAnotherPage();
            }
            producer.send(session.createTextMessage("hello" + extraBody));
         }

         producer.setTimeToLive(300);
         for (int i = 0; i < numberOfMessageSecondWave; i++) {
            if (i > 0 && i % pagingInterval == 0) {
               queues[0].getPagingStore().forceAnotherPage();
            }
            producer.send(session.createTextMessage("hello" + extraBody));
         }

         producer.setTimeToLive(TimeUnit.HOURS.toMillis(1));
         producer.send(session.createTextMessage("hello again" + extraBody)); // something not expiring

         for (Consumer c : consumers) {
            Wait.assertTrue(() -> c.consumedDelta.get() > 0);
         }

         running.set(false);


         // first check just to make sure topics are being consumed regularly
         for (Consumer c : consumers) {
            c.join(5000);
            Assert.assertFalse(c.isAlive());
            Assert.assertEquals(0, c.errors.get());
         }
      }
   }


   private class Consumer extends Thread {
      final ConnectionFactory factory;
      final int minimalSize;
      AtomicInteger consumedDelta = new AtomicInteger(0);
      AtomicInteger consumed = new AtomicInteger(0);
      AtomicInteger errors = new AtomicInteger(0);
      final String queueName;


      Consumer(ConnectionFactory factory, String queueName, int minimalSize) {
         this.factory = factory;
         this.queueName = queueName;
         this.minimalSize = minimalSize;
      }

      @Override
      public void run() {
         try {
            int rec = 0;
            try (Connection connection = factory.createConnection()) {
               Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               javax.jms.Queue jmsQueue = session.createQueue(ADDRESS + "::" + queueName);
               MessageConsumer consumer = session.createConsumer(jmsQueue);
               connection.start();
               while (running.get()) {
                  TextMessage message = (TextMessage)consumer.receive(500);
                  if (message != null) {
                     Assert.assertTrue(message.getText().length() > minimalSize);
                     consumed.incrementAndGet();
                     consumedDelta.incrementAndGet();
                     Thread.sleep(2);
                  }
                  if (rec++ > 10) {
                     rec = 0;
                     consumer.close();
                     consumer = session.createConsumer(jmsQueue);
                  }
               }
            }
         } catch (Throwable e) {
            errors.incrementAndGet();
         }

      }
   }
}