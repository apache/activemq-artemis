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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeTestAccessor;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoCreateTest extends ActiveMQTestBase {
   private static final Logger logger = LoggerFactory.getLogger(AutoCreateTest.class);

   public final SimpleString addressA = new SimpleString("addressA");
   public final SimpleString queueA = new SimpleString("queueA");

   private ActiveMQServer server;

   @After
   public void clearLogg() {
      AssertionLoggerHandler.stopCapture();
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(true, true);
      AddressSettings settings = new AddressSettings().setAutoCreateAddresses(true).setAutoDeleteAddresses(true).setAutoCreateQueues(true).setAutoDeleteQueues(true);

      server.getConfiguration().getAddressSettings().clear();
      server.getConfiguration().getAddressSettings().put("#", settings);
   }

   @Test
   public void testAutoCreateDeleteRecreate() throws Exception {
      // This test is about validating the behaviour of queues recreates with the default configuration
      Assert.assertEquals("Supposed to use default configuration on this test", ActiveMQDefaultConfiguration.getDefaultAddressQueueScanPeriod(), server.getConfiguration().getAddressQueueScanPeriod());
      server.start();
      int THREADS = 40;
      ExecutorService executor = Executors.newFixedThreadPool(THREADS);
      try {

         String QUEUE_NAME = getName();

         AtomicInteger errors = new AtomicInteger(0);

         for (int i = 0; i < 50; i++) {
            ConnectionFactory cf = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
            logger.debug("*******************************************************************************************************************************");
            logger.debug("run {}", i);
            CyclicBarrier barrier = new CyclicBarrier(THREADS + 1);
            CountDownLatch done = new CountDownLatch(THREADS);
            Runnable consumerThread = () -> {
               try (Connection connection = cf.createConnection()) {
                  Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                  barrier.await(10, TimeUnit.SECONDS);
                  Queue queue = session.createQueue(QUEUE_NAME);
                  MessageConsumer consumer = session.createConsumer(queue);
                  connection.start();
               } catch (Throwable e) {
                  logger.warn(e.getMessage(), e);
                  errors.incrementAndGet();
               } finally {
                  done.countDown();
               }
            };

            for (int t = 0; t < THREADS; t++) {
               executor.execute(consumerThread);
            }

            barrier.await(10, TimeUnit.SECONDS);
            Assert.assertTrue(done.await(10, TimeUnit.SECONDS));
            Assert.assertEquals(0, errors.get());

            try (Connection connection = cf.createConnection()) {
               Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               Queue queue = session.createQueue(QUEUE_NAME);
               MessageConsumer consumer = session.createConsumer(queue);
               connection.start();

               MessageProducer producer = session.createProducer(queue);
               producer.send(session.createTextMessage("hello"));

               Assert.assertNotNull(consumer.receive(5000));
            }
         }
      } finally {
         executor.shutdownNow();
      }
   }

   @Test
   public void testSweep() throws Exception {

      AssertionLoggerHandler.startCapture();
      server.getConfiguration().setAddressQueueScanPeriod(-1); // disabling scanner, we will perform it manually
      server.start();
      String QUEUE_NAME = "autoCreateAndRecreate";

      ConnectionFactory cf = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(QUEUE_NAME);
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();
      }

      try (Connection connection = cf.createConnection()) {
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(QUEUE_NAME);
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(QUEUE_NAME);
         PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
         Assert.assertTrue(serverQueue.isSwept());
         MessageConsumer consumer = session.createConsumer(queue);
         // no need to wait reaping to wait reaping to set it false. A simple add consumer on the queue should clear this
         Wait.assertFalse(serverQueue::isSwept);
         connection.start();
      }

      AddressInfo info = server.getPostOffice().getAddressInfo(SimpleString.toSimpleString(QUEUE_NAME));
      Assert.assertNotNull(info);
      Assert.assertTrue(info.isAutoCreated());

      PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224112"));
      PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
      Assert.assertTrue(AssertionLoggerHandler.findText("AMQ224112"));
      Assert.assertTrue("Queue name should be mentioned on logs", AssertionLoggerHandler.findText(QUEUE_NAME));
      PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
      Assert.assertTrue(AssertionLoggerHandler.findText("AMQ224113")); // we need another sweep to remove it
   }

   @Test
   public void testSweepAddress() throws Exception {
      AssertionLoggerHandler.startCapture();
      server.getConfiguration().setAddressQueueScanPeriod(-1); // disabling scanner, we will perform it manually
      AddressSettings settings = new AddressSettings().setAutoDeleteQueues(true).setAutoDeleteAddresses(true).setAutoDeleteAddressesDelay(10).setAutoDeleteQueuesDelay(10);
      server.getConfiguration().getAddressSettings().clear();
      server.getConfiguration().getAddressSettings().put("#", settings);
      server.start();
      String ADDRESS_NAME = getName();

      AddressInfo info = new AddressInfo(ADDRESS_NAME).addRoutingType(RoutingType.MULTICAST).setAutoCreated(true);
      server.getPostOffice().addAddressInfo(info);
      info = server.getPostOffice().getAddressInfo(SimpleString.toSimpleString(ADDRESS_NAME));

      ConnectionFactory cf = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(ADDRESS_NAME);
         session.createConsumer(topic);
      }

      { // just a namespace area
         final AddressInfo infoRef = info;
         Wait.assertTrue(() -> infoRef.getBindingRemovedTimestamp() != -1);
      }

      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224113"));
      Thread.sleep(50);
      Assert.assertFalse(info.isSwept());
      PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224113"));
      Assert.assertTrue(info.isSwept());
      PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
      Assert.assertTrue(AssertionLoggerHandler.findText("AMQ224113"));
   }


   @Test
   public void testNegativeSweepAddress() throws Exception {
      AssertionLoggerHandler.startCapture();
      server.getConfiguration().setAddressQueueScanPeriod(-1); // disabling scanner, we will perform it manually
      AddressSettings settings = new AddressSettings().setAutoDeleteQueues(true).setAutoDeleteAddresses(true).setAutoDeleteAddressesDelay(10).setAutoDeleteQueuesDelay(10);
      server.getConfiguration().getAddressSettings().clear();
      server.getConfiguration().getAddressSettings().put("#", settings);
      server.start();
      String ADDRESS_NAME = getName();

      AddressInfo info = new AddressInfo(ADDRESS_NAME).addRoutingType(RoutingType.MULTICAST).setAutoCreated(true);
      server.getPostOffice().addAddressInfo(info);
      info = server.getPostOffice().getAddressInfo(SimpleString.toSimpleString(ADDRESS_NAME));

      ConnectionFactory cf = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(ADDRESS_NAME);
         session.createConsumer(topic);
      }

      { // just a namespace area
         final AddressInfo infoRef = info;
         Wait.assertTrue(() -> infoRef.getBindingRemovedTimestamp() != -1);
      }

      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224113"));
      Thread.sleep(50);
      PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224113"));
      Assert.assertTrue(info.isSwept());
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(ADDRESS_NAME);
         session.createConsumer(topic);
         PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224113"));
         Assert.assertFalse(info.isSwept()); // it should be cleared because there is a consumer now
      }
   }

   @Test
   public void testNegativeSweepBecauseOfConsumer() throws Exception {

      AssertionLoggerHandler.startCapture();
      server.getConfiguration().setAddressQueueScanPeriod(-1); // disabling scanner, we will perform it manually
      server.start();
      String QUEUE_NAME = getName();

      ConnectionFactory cf = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(QUEUE_NAME);
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();
      }

      AddressInfo info = server.getPostOffice().getAddressInfo(SimpleString.toSimpleString(QUEUE_NAME));
      Assert.assertNotNull(info);
      Assert.assertTrue(info.isAutoCreated());


      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(QUEUE_NAME);
         PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(QUEUE_NAME);
         Assert.assertTrue(serverQueue.isSwept());
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224112"));
         MessageConsumer consumer = session.createConsumer(queue);
         PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
         Assert.assertFalse(serverQueue.isSwept());
         PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224113")); // we need another sweep to remove it
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224112"));
      }
   }

   @Test
   public void testNegativeSweepBecauseOfSend() throws Exception {

      AssertionLoggerHandler.startCapture();
      server.getConfiguration().setAddressQueueScanPeriod(-1); // disabling scanner, we will perform it manually
      server.start();
      String QUEUE_NAME = getName();

      ConnectionFactory cf = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(QUEUE_NAME);
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();
      }

      AddressInfo info = server.getPostOffice().getAddressInfo(SimpleString.toSimpleString(QUEUE_NAME));
      Assert.assertNotNull(info);
      Assert.assertTrue(info.isAutoCreated());


      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(QUEUE_NAME);
         PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(QUEUE_NAME);
         Assert.assertTrue(serverQueue.isSwept());
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224112"));
         MessageProducer producer = session.createProducer(queue);
         producer.send(session.createTextMessage("hello"));
         Wait.assertEquals(1, serverQueue::getMessageCount);
         PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
         Assert.assertFalse(serverQueue.isSwept());
         PostOfficeTestAccessor.reapAddresses((PostOfficeImpl) server.getPostOffice());
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224113")); // we need another sweep to remove it
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224112"));
      }
   }

   @Test
   public void testCleanupAfterRebootOpenWire() throws Exception {
      testCleanupAfterReboot("OPENWIRE", false);
   }

   @Test
   public void testCleanupAfterRebootCore() throws Exception {
      // there is no need to duplicate the test between usedelay and not.
      // doing it in one of the protocols should be enough
      testCleanupAfterReboot("CORE", true);
   }

   @Test
   public void testCleanupAfterRebootAMQP() throws Exception {
      testCleanupAfterReboot("AMQP", false);
   }

   public void testCleanupAfterReboot(String protocol, boolean useDelay) throws Exception {

      if (useDelay) {
         // setting up a delay, to make things a bit more challenging
         server.getAddressSettingsRepository().addMatch(getName(), new AddressSettings().setAutoCreateAddresses(true).setAutoDeleteAddressesDelay(TimeUnit.DAYS.toMillis(1)).setAutoDeleteQueuesDelay(TimeUnit.DAYS.toMillis(1)));
      }

      AssertionLoggerHandler.startCapture();
      server.getConfiguration().setAddressQueueScanPeriod(-1); // disabling scanner, we will perform it manually
      server.start();
      String QUEUE_NAME = getName();

      ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(QUEUE_NAME);
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();
      }

      AddressInfo info = server.getPostOffice().getAddressInfo(SimpleString.toSimpleString(QUEUE_NAME));
      Assert.assertNotNull(info);
      Assert.assertTrue(info.isAutoCreated());

      server.stop();
      server.start();

      Assert.assertTrue(AssertionLoggerHandler.findText("AMQ224113"));
      Assert.assertTrue(AssertionLoggerHandler.findText("AMQ224112"));

      AssertionLoggerHandler.clear();

      String randomString = "random " + RandomUtil.randomString();

      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(QUEUE_NAME);
         MessageProducer producer = session.createProducer(queue);
         producer.send(session.createTextMessage(randomString));
      }

      info = server.getPostOffice().getAddressInfo(SimpleString.toSimpleString(QUEUE_NAME));
      Assert.assertNotNull(info);
      Assert.assertTrue(info.isAutoCreated());

      server.stop();
      server.start();

      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224113")); // this time around the queue had messages, it has to exist
      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ224112"));

      info = server.getPostOffice().getAddressInfo(SimpleString.toSimpleString(QUEUE_NAME));
      Assert.assertNotNull(info);
      Assert.assertTrue(info.isAutoCreated());

      { // just a namespace
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(QUEUE_NAME);
         Wait.assertEquals(1, serverQueue::getMessageCount);
      }


      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         connection.start();
         Queue queue = session.createQueue(QUEUE_NAME);
         MessageConsumer consumer = session.createConsumer(queue);
         TextMessage message = (TextMessage)consumer.receive(5000);
         Assert.assertEquals(randomString, message.getText());
      }

   }



}
