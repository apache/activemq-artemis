/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.integration.mqtt.imported.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic send and receive scenarios using only AMQP sender and receiver links.
 */
public class AmqpSendReceiveTest extends AmqpClientTestSupport {

   protected static final Logger LOG = LoggerFactory.getLogger(AmqpSendReceiveTest.class);

   @Test(timeout = 60000)
   public void testSimpleSendOneReceiveOne() throws Exception {

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());

      AmqpMessage message = new AmqpMessage();

      message.setMessageId("msg" + 1);
      message.setMessageAnnotation("serialNo", 1);
      message.setText("Test-Message");

      sender.send(message);
      sender.close();

      LOG.info("Attempting to read message with receiver");
      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(2);
      AmqpMessage received = receiver.receive(10, TimeUnit.SECONDS);
      assertNotNull("Should have read message", received);
      assertEquals("msg1", received.getMessageId());
      received.accept();

      receiver.close();

      connection.close();
   }

   @Test(timeout = 60000)
   public void testCloseBusyReceiver() throws Exception {
      final int MSG_COUNT = 20;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();
      AmqpSender sender = session.createSender(getTestName());

      for (int i = 0; i < MSG_COUNT; i++) {
         AmqpMessage message = new AmqpMessage();

         message.setMessageId("msg" + i);
         message.setMessageAnnotation("serialNo", i);
         message.setText("Test-Message");

         System.out.println("Sending message: " + message.getMessageId());

         sender.send(message);
      }

      sender.close();

      Queue queue = getProxyToQueue(getTestName());
      assertEquals(MSG_COUNT, queue.getMessageCount());

      AmqpReceiver receiver1 = session.createReceiver(getTestName());
      receiver1.flow(MSG_COUNT);
      AmqpMessage received = receiver1.receive(5, TimeUnit.SECONDS);
      assertNotNull("Should have got a message", received);
      assertEquals("msg0", received.getMessageId());
      receiver1.close();

      AmqpReceiver receiver2 = session.createReceiver(getTestName());
      receiver2.flow(200);
      for (int i = 0; i < MSG_COUNT; ++i) {
         received = receiver2.receive(5, TimeUnit.SECONDS);
         assertNotNull("Should have got a message", received);
         System.out.println("Read message: " + received.getMessageId());
         assertEquals("msg" + i, received.getMessageId());
      }

      receiver2.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testReceiveWithJMSSelectorFilter() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpMessage message1 = new AmqpMessage();
      message1.setGroupId("abcdefg");
      message1.setApplicationProperty("sn", 100);

      AmqpMessage message2 = new AmqpMessage();
      message2.setGroupId("hijklm");
      message2.setApplicationProperty("sn", 200);

      AmqpSender sender = session.createSender(getTestName());
      sender.send(message1);
      sender.send(message2);
      sender.close();

      AmqpReceiver receiver = session.createReceiver(getTestName(), "sn = 100");
      receiver.flow(2);
      AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull("Should have read a message", received);
      assertEquals(100, received.getApplicationProperty("sn"));
      assertEquals("abcdefg", received.getGroupId());
      received.accept();

      assertNull(receiver.receive(1, TimeUnit.SECONDS));

      receiver.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testAdvancedLinkFlowControl() throws Exception {
      final int MSG_COUNT = 20;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());

      for (int i = 0; i < MSG_COUNT; i++) {
         AmqpMessage message = new AmqpMessage();

         message.setMessageId("msg" + i);
         message.setMessageAnnotation("serialNo", i);
         message.setText("Test-Message");

         sender.send(message);
      }

      sender.close();

      LOG.info("Attempting to read first two messages with receiver #1");
      AmqpReceiver receiver1 = session.createReceiver(getTestName());
      receiver1.flow(2);
      AmqpMessage message1 = receiver1.receive(10, TimeUnit.SECONDS);
      AmqpMessage message2 = receiver1.receive(10, TimeUnit.SECONDS);
      assertNotNull("Should have read message 1", message1);
      assertNotNull("Should have read message 2", message2);
      assertEquals("msg0", message1.getMessageId());
      assertEquals("msg1", message2.getMessageId());
      message1.accept();
      message2.accept();

      LOG.info("Attempting to read next two messages with receiver #2");
      AmqpReceiver receiver2 = session.createReceiver(getTestName());
      receiver2.flow(2);
      AmqpMessage message3 = receiver2.receive(10, TimeUnit.SECONDS);
      AmqpMessage message4 = receiver2.receive(10, TimeUnit.SECONDS);
      assertNotNull("Should have read message 3", message3);
      assertNotNull("Should have read message 4", message4);
      assertEquals("msg2", message3.getMessageId());
      assertEquals("msg3", message4.getMessageId());
      message3.accept();
      message4.accept();

      LOG.info("Attempting to read remaining messages with receiver #1");
      receiver1.flow(MSG_COUNT - 4);
      for (int i = 4; i < MSG_COUNT; i++) {
         AmqpMessage message = receiver1.receive(10, TimeUnit.SECONDS);
         assertNotNull("Should have read a message", message);
         assertEquals("msg" + i, message.getMessageId());
         message.accept();
      }

      receiver1.close();
      receiver2.close();

      connection.close();
   }

   @Test(timeout = 60000)
   public void testDispatchOrderWithPrefetchOfOne() throws Exception {
      final int MSG_COUNT = 20;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());

      for (int i = 0; i < MSG_COUNT; i++) {
         AmqpMessage message = new AmqpMessage();

         message.setMessageId("msg" + i);
         message.setMessageAnnotation("serialNo", i);
         message.setText("Test-Message");

         sender.send(message);
      }

      sender.close();

      AmqpReceiver receiver1 = session.createReceiver(getTestName());
      receiver1.flow(1);

      AmqpReceiver receiver2 = session.createReceiver(getTestName());
      receiver2.flow(1);

      AmqpMessage message1 = receiver1.receive(10, TimeUnit.SECONDS);
      AmqpMessage message2 = receiver2.receive(10, TimeUnit.SECONDS);
      assertNotNull("Should have read message 1", message1);
      assertNotNull("Should have read message 2", message2);
      assertEquals("msg0", message1.getMessageId());
      assertEquals("msg1", message2.getMessageId());
      message1.accept();
      message2.accept();

      receiver1.flow(1);
      AmqpMessage message3 = receiver1.receive(10, TimeUnit.SECONDS);
      receiver2.flow(1);
      AmqpMessage message4 = receiver2.receive(10, TimeUnit.SECONDS);
      assertNotNull("Should have read message 3", message3);
      assertNotNull("Should have read message 4", message4);
      assertEquals("msg2", message3.getMessageId());
      assertEquals("msg3", message4.getMessageId());
      message3.accept();
      message4.accept();

      LOG.info("*** Attempting to read remaining messages with both receivers");
      int splitCredit = (MSG_COUNT - 4) / 2;

      LOG.info("**** Receiver #1 granting creadit[{}] for its block of messages", splitCredit);
      receiver1.flow(splitCredit);
      for (int i = 0; i < splitCredit; i++) {
         AmqpMessage message = receiver1.receive(10, TimeUnit.SECONDS);
         assertNotNull("Receiver #1 should have read a message", message);
         LOG.info("Receiver #1 read message: {}", message.getMessageId());
         message.accept();
      }

      LOG.info("**** Receiver #2 granting creadit[{}] for its block of messages", splitCredit);
      receiver2.flow(splitCredit);
      for (int i = 0; i < splitCredit; i++) {
         AmqpMessage message = receiver2.receive(10, TimeUnit.SECONDS);
         assertNotNull("Receiver #2 should have read message[" + i + "]", message);
         LOG.info("Receiver #2 read message: {}", message.getMessageId());
         message.accept();
      }

      receiver1.close();
      receiver2.close();

      connection.close();
   }

   @Test(timeout = 60000)
   public void testReceiveMessageAndRefillCreditBeforeAccept() throws Exception {
      AmqpClient client = createAmqpClient();

      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      final String address = getTestName();

      AmqpReceiver receiver = session.createReceiver(address);
      AmqpSender sender = session.createSender(address);

      for (int i = 0; i < 2; i++) {
         AmqpMessage message = new AmqpMessage();
         message.setMessageId("msg" + i);
         sender.send(message);
      }
      sender.close();

      receiver.flow(1);
      AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(received);

      receiver.flow(1);
      received.accept();

      received = receiver.receive(10, TimeUnit.SECONDS);
      assertNotNull(received);
      received.accept();

      receiver.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testReceiveMessageAndRefillCreditBeforeAcceptOnQueueAsync() throws Exception {
      final AmqpClient client = createAmqpClient();
      final LinkedList<Throwable> errors = new LinkedList<>();
      final CountDownLatch receiverReady = new CountDownLatch(1);
      ExecutorService executorService = Executors.newCachedThreadPool();

      final String address = getTestName();

      executorService.submit(new Runnable() {
         @Override
         public void run() {
            try {
               LOG.info("Starting consumer connection");
               AmqpConnection connection = addConnection(client.connect());
               AmqpSession session = connection.createSession();
               AmqpReceiver receiver = session.createReceiver(address);
               receiver.flow(1);
               receiverReady.countDown();
               AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
               assertNotNull(received);

               receiver.flow(1);
               received.accept();

               received = receiver.receive(5, TimeUnit.SECONDS);
               assertNotNull(received);
               received.accept();

               receiver.close();
               connection.close();

            } catch (Exception error) {
               errors.add(error);
            }
         }
      });

      // producer
      executorService.submit(new Runnable() {
         @Override
         public void run() {
            try {
               receiverReady.await(20, TimeUnit.SECONDS);
               AmqpConnection connection = addConnection(client.connect());
               AmqpSession session = connection.createSession();

               AmqpSender sender = session.createSender(address);
               for (int i = 0; i < 2; i++) {
                  AmqpMessage message = new AmqpMessage();
                  message.setMessageId("msg" + i);
                  sender.send(message);
               }
               sender.close();
               connection.close();
            } catch (Exception ignored) {
               ignored.printStackTrace();
            }
         }
      });

      executorService.shutdown();
      executorService.awaitTermination(20, TimeUnit.SECONDS);
      assertTrue("no errors: " + errors, errors.isEmpty());
   }

   @Test(timeout = 60000)
   public void testMessageDurabliltyFollowsSpec() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());
      AmqpReceiver receiver1 = session.createReceiver(getTestName());

      Queue queue = getProxyToQueue(getTestName());

      // Create default message that should be sent as non-durable
      AmqpMessage message1 = new AmqpMessage();
      message1.setText("Test-Message -> non-durable");
      message1.setDurable(false);
      message1.setMessageId("ID:Message:1");
      sender.send(message1);

      assertEquals(1, queue.getMessageCount());
      receiver1.flow(1);
      message1 = receiver1.receive(50, TimeUnit.SECONDS);
      assertNotNull("Should have read a message", message1);
      assertFalse("First message sent should not be durable", message1.isDurable());
      message1.accept();

      // Create default message that should be sent as non-durable
      AmqpMessage message2 = new AmqpMessage();
      message2.setText("Test-Message -> durable");
      message2.setDurable(true);
      message2.setMessageId("ID:Message:2");
      sender.send(message2);

      assertEquals(1, queue.getMessageCount());
      receiver1.flow(1);
      message2 = receiver1.receive(50, TimeUnit.SECONDS);
      assertNotNull("Should have read a message", message2);
      assertTrue("Second message sent should be durable", message2.isDurable());
      message2.accept();

      sender.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testReceiveMessageBeyondAckedAmountQueue() throws Exception {
      final int MSG_COUNT = 50;

      AmqpClient client = createAmqpClient();

      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      final String address = getTestName();

      AmqpReceiver receiver = session.createReceiver(address);
      AmqpSender sender = session.createSender(address);

      final Queue destinationView = getProxyToQueue(address);

      for (int i = 0; i < MSG_COUNT; i++) {
         AmqpMessage message = new AmqpMessage();
         message.setMessageId("msg" + i);
         sender.send(message);
      }

      List<AmqpMessage> pendingAcks = new ArrayList<>();

      for (int i = 0; i < MSG_COUNT; i++) {
         receiver.flow(1);
         AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(received);
         pendingAcks.add(received);
      }

      // Send one more to check in-flight stays at zero with no credit and all
      // pending messages settled.
      AmqpMessage message = new AmqpMessage();
      message.setMessageId("msg-final");
      sender.send(message);

      for (AmqpMessage pendingAck : pendingAcks) {
         pendingAck.accept();
      }

      assertTrue("Should be no inflight messages: " + destinationView.getDeliveringCount(), Wait.waitFor(new Wait.Condition() {

         @Override
         public boolean isSatisified() throws Exception {
            return destinationView.getDeliveringCount() == 0;
         }
      }));

      sender.close();
      receiver.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testTwoPresettledReceiversReceiveAllMessages() throws Exception {
      final int MSG_COUNT = 100;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      final String address = getTestName();

      AmqpSender sender = session.createSender(address);
      AmqpReceiver receiver1 = session.createReceiver(address, null, false, true);
      AmqpReceiver receiver2 = session.createReceiver(address, null, false, true);

      for (int i = 0; i < MSG_COUNT; i++) {
         AmqpMessage message = new AmqpMessage();
         message.setMessageId("msg" + i);
         sender.send(message);
      }

      LOG.info("Attempting to read first two messages with receiver #1");
      receiver1.flow(2);
      AmqpMessage message1 = receiver1.receive(10, TimeUnit.SECONDS);
      AmqpMessage message2 = receiver1.receive(10, TimeUnit.SECONDS);
      assertNotNull("Should have read message 1", message1);
      assertNotNull("Should have read message 2", message2);
      assertEquals("msg0", message1.getMessageId());
      assertEquals("msg1", message2.getMessageId());
      message1.accept();
      message2.accept();

      LOG.info("Attempting to read next two messages with receiver #2");
      receiver2.flow(2);
      AmqpMessage message3 = receiver2.receive(10, TimeUnit.SECONDS);
      AmqpMessage message4 = receiver2.receive(10, TimeUnit.SECONDS);
      assertNotNull("Should have read message 3", message3);
      assertNotNull("Should have read message 4", message4);
      assertEquals("msg2", message3.getMessageId());
      assertEquals("msg3", message4.getMessageId());
      message3.accept();
      message4.accept();

      LOG.info("*** Attempting to read remaining messages with both receivers");
      int splitCredit = (MSG_COUNT - 4) / 2;

      LOG.info("**** Receiver #1 granting credit[{}] for its block of messages", splitCredit);
      receiver1.flow(splitCredit);
      for (int i = 0; i < splitCredit; i++) {
         AmqpMessage message = receiver1.receive(10, TimeUnit.SECONDS);
         assertNotNull("Receiver #1 should have read a message", message);
         LOG.info("Receiver #1 read message: {}", message.getMessageId());
         message.accept();
      }

      LOG.info("**** Receiver #2 granting credit[{}] for its block of messages", splitCredit);
      receiver2.flow(splitCredit);
      for (int i = 0; i < splitCredit; i++) {
         AmqpMessage message = receiver2.receive(10, TimeUnit.SECONDS);
         assertNotNull("Receiver #2 should have read a message[" + i + "]", message);
         LOG.info("Receiver #2 read message: {}", message.getMessageId());
         message.accept();
      }

      receiver1.close();
      receiver2.close();

      connection.close();
   }
}
