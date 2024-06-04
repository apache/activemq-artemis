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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MessageHandlerTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private final SimpleString QUEUE = SimpleString.of("ConsumerTestQueue");

   private ServerLocator locator;

   private ClientSessionFactory sf;

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false);

      server.start();

      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);

   }

   @Test
   public void testSetMessageHandlerWithMessagesPending() throws Exception {
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QueueConfiguration.of(QUEUE).setDurable(false));

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE, null, true);

      session.start();

      Thread.sleep(100);

      // Message should be in consumer

      class MyHandler implements MessageHandler {

         @Override
         public void onMessage(final ClientMessage message) {
            try {
               Thread.sleep(10);

               message.acknowledge();
            } catch (Exception e) {
            }
         }
      }

      consumer.setMessageHandler(new MyHandler());

      // Let a few messages get processed
      Thread.sleep(100);

      // Now set null

      consumer.setMessageHandler(null);

      // Give a bit of time for some queued executors to run

      Thread.sleep(500);

      // Make sure no exceptions were thrown from onMessage
      assertNull(consumer.getLastException());

      session.close();
   }

   @Test
   public void testSetResetMessageHandler() throws Exception {
      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QueueConfiguration.of(QUEUE).setDurable(false));

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);

         message.putIntProperty(SimpleString.of("i"), i);

         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      CountDownLatch latch = new CountDownLatch(50);

      // Message should be in consumer

      class MyHandler implements MessageHandler {

         int messageReceived = 0;

         boolean failed;

         boolean started = true;

         private final CountDownLatch latch;

         MyHandler(final CountDownLatch latch) {
            this.latch = latch;
         }

         @Override
         public void onMessage(final ClientMessage message) {

            try {
               if (!started) {
                  failed = true;
               }

               messageReceived++;

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

      MyHandler handler = new MyHandler(latch);

      consumer.setMessageHandler(handler);

      session.start();

      waitForLatch(latch);

      Thread.sleep(100);

      assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      assertNull(consumer.getLastException());
      latch = new CountDownLatch(50);
      handler = new MyHandler(latch);
      consumer.setMessageHandler(handler);
      session.start();
      assertTrue(latch.await(5, TimeUnit.SECONDS), "message received " + handler.messageReceived);

      Thread.sleep(100);

      assertFalse(handler.failed);
      assertNull(consumer.getLastException());
      session.close();
   }

   @Test
   public void testSetUnsetMessageHandler() throws Exception {
      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QueueConfiguration.of(QUEUE).setDurable(false));

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         message.putIntProperty(SimpleString.of("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      CountDownLatch latch = new CountDownLatch(50);

      // Message should be in consumer

      class MyHandler implements MessageHandler {

         int messageReceived = 0;

         boolean failed;

         boolean started = true;

         private final CountDownLatch latch;

         MyHandler(final CountDownLatch latch) {
            this.latch = latch;
         }

         @Override
         public void onMessage(final ClientMessage message) {

            try {
               if (!started) {
                  failed = true;
               }
               messageReceived++;
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

      MyHandler handler = new MyHandler(latch);

      consumer.setMessageHandler(handler);

      waitForLatch(latch);

      Thread.sleep(100);

      assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      assertNull(consumer.getLastException());
      consumer.setMessageHandler(null);
      ClientMessage cm = consumer.receiveImmediate();
      assertNotNull(cm);

      session.close();
   }

   @Test
   public void testSetUnsetResetMessageHandler() throws Exception {
      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QueueConfiguration.of(QUEUE).setDurable(false));

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         message.putIntProperty(SimpleString.of("i"), i);
         producer.send(message);
      }

      final ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      CountDownLatch latch = new CountDownLatch(50);

      // Message should be in consumer

      class MyHandler implements MessageHandler {

         int messageReceived = 0;

         boolean failed;

         boolean started = true;

         private final CountDownLatch latch;

         MyHandler(final CountDownLatch latch) {
            this.latch = latch;
         }

         @Override
         public void onMessage(final ClientMessage message) {

            try {
               if (!started) {
                  failed = true;
               }
               messageReceived++;
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

      MyHandler handler = new MyHandler(latch);

      consumer.setMessageHandler(handler);

      waitForLatch(latch);

      Thread.sleep(100);

      assertFalse(handler.failed);

      // Make sure no exceptions were thrown from onMessage
      assertNull(consumer.getLastException());
      consumer.setMessageHandler(null);
      ClientMessage cm = consumer.receiveImmediate();
      assertNotNull(cm);
      latch = new CountDownLatch(49);
      handler = new MyHandler(latch);
      consumer.setMessageHandler(handler);
      session.start();
      assertTrue(latch.await(5, TimeUnit.SECONDS), "message received " + handler.messageReceived);

      Thread.sleep(100);

      assertFalse(handler.failed);
      assertNull(consumer.getLastException());
      session.close();
   }
}
