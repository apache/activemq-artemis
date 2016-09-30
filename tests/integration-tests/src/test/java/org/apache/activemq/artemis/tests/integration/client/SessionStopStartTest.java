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
import java.util.concurrent.TimeUnit;

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

public class SessionStopStartTest extends ActiveMQTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private ActiveMQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false);
      server.start();
      locator = createInVMNonHALocator();
   }

   @Test
   public void testStopStartConsumerSyncReceiveImmediate() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages / 2; i++) {
         ClientMessage cm = consumer.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }
      session.stop();
      ClientMessage cm = consumer.receiveImmediate();
      Assert.assertNull(cm);

      session.start();
      for (int i = 0; i < numMessages / 2; i++) {
         cm = consumer.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }

      session.close();
   }

   @Test
   public void testStopStartConsumerSyncReceive() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages / 2; i++) {
         ClientMessage cm = consumer.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }
      session.stop();
      long time = System.currentTimeMillis();
      ClientMessage cm = consumer.receive(1000);
      long taken = System.currentTimeMillis() - time;
      Assert.assertTrue(taken >= 1000);
      Assert.assertNull(cm);

      session.start();
      for (int i = 0; i < numMessages / 2; i++) {
         cm = consumer.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }

      session.close();
   }

   @Test
   public void testStopStartConsumerAsyncSyncStoppedByHandler() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      final CountDownLatch latch = new CountDownLatch(10);

      // Message should be in consumer

      class MyHandler implements MessageHandler {

         boolean failed;

         boolean started = true;

         int count = 0;

         @Override
         public void onMessage(final ClientMessage message) {

            try {
               if (!started) {
                  failed = true;
               }

               count++;

               if (count == 10) {
                  message.acknowledge();
                  session.stop();
                  started = false;
               }

               latch.countDown();
            } catch (Exception e) {
            }
         }
      }

      MyHandler handler = new MyHandler();

      consumer.setMessageHandler(handler);

      waitForLatch(latch);

      Assert.assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      Assert.assertNull(consumer.getLastException());
      consumer.setMessageHandler(null);
      session.start();
      for (int i = 0; i < 90; i++) {
         ClientMessage msg = consumer.receive(1000);
         if (msg == null) {
            System.out.println("ClientConsumerTest.testStopConsumer");
         }
         Assert.assertNotNull("message " + i, msg);
         msg.acknowledge();
      }

      Assert.assertNull(consumer.receiveImmediate());

      session.close();
   }

   @Test
   public void testStopStartConsumerAsyncSync() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      final CountDownLatch latch = new CountDownLatch(10);

      // Message should be in consumer

      class MyHandler implements MessageHandler {

         boolean failed;

         boolean started = true;

         @Override
         public void onMessage(final ClientMessage message) {

            try {
               if (!started) {
                  failed = true;
               }

               latch.countDown();

               if (latch.getCount() == 0) {

                  message.acknowledge();
                  started = false;
                  consumer.setMessageHandler(null);
               }

            } catch (Exception e) {
            }
         }
      }

      MyHandler handler = new MyHandler();

      consumer.setMessageHandler(handler);

      waitForLatch(latch);

      try {
         session.stop();
      } catch (Exception e) {
         SessionStopStartTest.log.warn(e.getMessage(), e);
         throw e;
      }

      Assert.assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      Assert.assertNull(consumer.getLastException());
      consumer.setMessageHandler(null);
      session.start();
      for (int i = 0; i < 90; i++) {
         ClientMessage msg = consumer.receive(1000);
         if (msg == null) {
            System.out.println("ClientConsumerTest.testStopConsumer");
         }
         Assert.assertNotNull("message " + i, msg);
         msg.acknowledge();
      }

      Assert.assertNull(consumer.receiveImmediate());

      session.close();
   }

   @Test
   public void testStopStartConsumerAsyncASyncStoppeeByHandler() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      CountDownLatch latch = new CountDownLatch(10);

      // Message should be in consumer

      class MyHandler implements MessageHandler {

         int messageReceived = 0;

         boolean failed;

         boolean started = true;

         private final CountDownLatch latch;

         private boolean stop = true;

         MyHandler(final CountDownLatch latch) {
            this.latch = latch;
         }

         MyHandler(final CountDownLatch latch, final boolean stop) {
            this(latch);
            this.stop = stop;
         }

         @Override
         public void onMessage(final ClientMessage message) {

            try {
               if (!started) {
                  failed = true;
               }
               messageReceived++;
               latch.countDown();

               if (stop && latch.getCount() == 0) {

                  message.acknowledge();
                  session.stop();
                  started = false;
               }

            } catch (Exception e) {
            }
         }
      }

      MyHandler handler = new MyHandler(latch);

      consumer.setMessageHandler(handler);

      waitForLatch(latch);

      Thread.sleep(100);

      Assert.assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      Assert.assertNull(consumer.getLastException());
      latch = new CountDownLatch(90);
      handler = new MyHandler(latch, false);
      consumer.setMessageHandler(handler);
      session.start();
      Assert.assertTrue("message received " + handler.messageReceived, latch.await(5, TimeUnit.SECONDS));

      Thread.sleep(100);

      Assert.assertFalse(handler.failed);
      Assert.assertNull(consumer.getLastException());
      session.close();
   }

   @Test
   public void testStopStartConsumerAsyncASync() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      CountDownLatch latch = new CountDownLatch(10);

      // Message should be in consumer

      class MyHandler implements MessageHandler {

         int messageReceived = 0;

         boolean failed;

         boolean started = true;

         private final CountDownLatch latch;

         private boolean stop = true;

         MyHandler(final CountDownLatch latch) {
            this.latch = latch;
         }

         MyHandler(final CountDownLatch latch, final boolean stop) {
            this(latch);
            this.stop = stop;
         }

         @Override
         public void onMessage(final ClientMessage message) {

            try {
               if (!started) {
                  failed = true;
               }
               messageReceived++;
               latch.countDown();

               if (stop && latch.getCount() == 0) {

                  message.acknowledge();
                  consumer.setMessageHandler(null);
                  started = false;
               }

            } catch (Exception e) {
            }
         }
      }

      MyHandler handler = new MyHandler(latch);

      consumer.setMessageHandler(handler);

      waitForLatch(latch);

      Thread.sleep(100);

      Assert.assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      Assert.assertNull(consumer.getLastException());
      latch = new CountDownLatch(90);
      handler = new MyHandler(latch, false);
      consumer.setMessageHandler(handler);
      session.start();
      Assert.assertTrue("message received " + handler.messageReceived, latch.await(5, TimeUnit.SECONDS));

      Thread.sleep(100);

      Assert.assertFalse(handler.failed);
      Assert.assertNull(consumer.getLastException());
      session.close();
   }

   private int getMessageEncodeSize(final SimpleString address) throws Exception {
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession(false, true, true);
      ClientMessage message = session.createMessage(false);
      // we need to set the destination so we can calculate the encodesize correctly
      message.setAddress(address);
      int encodeSize = message.getEncodeSize();
      session.close();
      cf.close();
      return encodeSize;
   }

   @Test
   public void testStopStartMultipleConsumers() throws Exception {
      locator.setConsumerWindowSize(getMessageEncodeSize(QUEUE) * 33);
      ClientSessionFactory sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      ClientConsumer consumer2 = session.createConsumer(QUEUE);
      ClientConsumer consumer3 = session.createConsumer(QUEUE);

      session.start();

      ClientMessage cm = consumer.receive(5000);
      Assert.assertNotNull(cm);
      cm.acknowledge();
      cm = consumer2.receive(5000);
      Assert.assertNotNull(cm);
      cm.acknowledge();
      cm = consumer3.receive(5000);
      Assert.assertNotNull(cm);
      cm.acknowledge();

      session.stop();
      cm = consumer.receiveImmediate();
      Assert.assertNull(cm);
      cm = consumer2.receiveImmediate();
      Assert.assertNull(cm);
      cm = consumer3.receiveImmediate();
      Assert.assertNull(cm);

      session.start();
      cm = consumer.receive(5000);
      Assert.assertNotNull(cm);
      cm = consumer2.receive(5000);
      Assert.assertNotNull(cm);
      cm = consumer3.receive(5000);
      Assert.assertNotNull(cm);
      session.close();
   }

   @Test
   public void testStopStartAlreadyStartedSession() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages / 2; i++) {
         ClientMessage cm = consumer.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }

      session.start();
      for (int i = 0; i < numMessages / 2; i++) {
         ClientMessage cm = consumer.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }

      session.close();
   }

   @Test
   public void testStopAlreadyStoppedSession() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         message.putIntProperty(new SimpleString("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages / 2; i++) {
         ClientMessage cm = consumer.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }
      session.stop();
      ClientMessage cm = consumer.receiveImmediate();
      Assert.assertNull(cm);

      session.stop();
      cm = consumer.receiveImmediate();
      Assert.assertNull(cm);

      session.start();
      for (int i = 0; i < numMessages / 2; i++) {
         cm = consumer.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }

      session.close();
   }

}
