/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Before;
import org.junit.Test;

/**
 * Test various aspects of Transaction support.
 */
public class AmqpTransactionTest extends AmqpClientTestSupport {

   @Before
   public void createQueue() throws Exception {
      server.createQueue(SimpleString.toSimpleString(getTestName()), SimpleString.toSimpleString(getTestName()), null, true, false);
   }

   @Test(timeout = 30000)
   public void testBeginAndCommitTransaction() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();
      assertNotNull(session);

      session.begin();
      assertTrue(session.isInTransaction());
      session.commit();

      connection.close();
   }

   @Test(timeout = 30000)
   public void testBeginAndRollbackTransaction() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();
      assertNotNull(session);

      session.begin();
      assertTrue(session.isInTransaction());
      session.rollback();

      connection.close();

      System.err.println("Closed");
   }

   @Test(timeout = 60000)
   public void testSendMessageToQueueWithCommit() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());
      final Queue queue = getProxyToQueue(getTestName());

      session.begin();

      AmqpMessage message = new AmqpMessage();
      message.setText("Test-Message");
      sender.send(message);

      assertEquals(0, queue.getMessageCount());

      session.commit();

      assertEquals(1, queue.getMessageCount());

      sender.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testSendMessageToQueueWithRollback() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());
      final Queue queue = getProxyToQueue(getTestName());

      session.begin();

      AmqpMessage message = new AmqpMessage();
      message.setText("Test-Message");
      sender.send(message);

      assertEquals(0, queue.getMessageCount());

      session.rollback();

      assertEquals(0, queue.getMessageCount());

      sender.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testReceiveMessageWithCommit() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());
      final Queue queue = getProxyToQueue(getTestName());

      AmqpMessage message = new AmqpMessage();
      message.setText("Test-Message");
      sender.send(message);

      assertEquals(1, queue.getMessageCount());

      AmqpReceiver receiver = session.createReceiver(getTestName());

      session.begin();

      receiver.flow(1);
      AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(received);
      received.accept();

      session.commit();

      assertEquals(0, queue.getMessageCount());

      sender.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testReceiveAfterConnectionClose() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());
      final Queue queue = getProxyToQueue(getTestName());

      AmqpMessage message = new AmqpMessage();
      message.setText("Test-Message");
      sender.send(message);

      assertEquals(1, queue.getMessageCount());

      AmqpReceiver receiver = session.createReceiver(getTestName());

      session.begin();

      receiver.flow(1);
      AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(received);
      received.accept();

      // this will force a rollback on the TX (It should at least)
      connection.close();

      connection = addConnection(client.connect());
      session = connection.createSession();
      receiver = session.createReceiver(getTestName());
      session.begin();
      receiver.flow(1);

      received = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(received);
      received.accept();

      session.commit();

      assertEquals(0, queue.getMessageCount());

      connection.close();
   }

   @Test(timeout = 60000)
   public void testReceiveMessageWithRollback() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());
      final Queue queue = getProxyToQueue(getTestName());

      AmqpMessage message = new AmqpMessage();
      message.setText("Test-Message");
      sender.send(message);

      assertEquals(1, queue.getMessageCount());

      AmqpReceiver receiver = session.createReceiver(getTestName());

      session.begin();

      receiver.flow(1);
      AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(received);
      received.accept();

      session.rollback();

      assertEquals(1, queue.getMessageCount());

      sender.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testMultipleSessionReceiversInSingleTXNWithCommit() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      // Load up the Queue with some messages
      {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getTestName());
         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message");
         sender.send(message);
         sender.send(message);
         sender.send(message);
         sender.close();
      }

      // Root TXN session controls all TXN send lifetimes.
      AmqpSession txnSession = connection.createSession();

      // Create some sender sessions
      AmqpSession session1 = connection.createSession();
      AmqpSession session2 = connection.createSession();
      AmqpSession session3 = connection.createSession();

      // Sender linked to each session
      AmqpReceiver receiver1 = session1.createReceiver(getTestName());
      AmqpReceiver receiver2 = session2.createReceiver(getTestName());
      AmqpReceiver receiver3 = session3.createReceiver(getTestName());

      final Queue queue = getProxyToQueue(getTestName());
      assertEquals(3, queue.getMessageCount());

      // Begin the transaction that all senders will operate in.
      txnSession.begin();

      assertTrue(txnSession.isInTransaction());

      receiver1.flow(1);
      receiver2.flow(1);
      receiver3.flow(1);

      AmqpMessage message1 = receiver1.receive(5, TimeUnit.SECONDS);
      AmqpMessage message2 = receiver2.receive(5, TimeUnit.SECONDS);
      AmqpMessage message3 = receiver3.receive(5, TimeUnit.SECONDS);

      message1.accept(txnSession);
      message2.accept(txnSession);
      message3.accept(txnSession);

      assertEquals(3, queue.getMessageCount());

      txnSession.commit();

      assertEquals(0, queue.getMessageCount());
   }

   @Test(timeout = 60000)
   public void testMultipleSessionReceiversInSingleTXNWithRollback() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      // Load up the Queue with some messages
      {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getTestName());
         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message");
         sender.send(message);
         sender.send(message);
         sender.send(message);
         sender.close();
      }

      // Root TXN session controls all TXN send lifetimes.
      AmqpSession txnSession = connection.createSession();

      // Create some sender sessions
      AmqpSession session1 = connection.createSession();
      AmqpSession session2 = connection.createSession();
      AmqpSession session3 = connection.createSession();

      // Sender linked to each session
      AmqpReceiver receiver1 = session1.createReceiver(getTestName());
      AmqpReceiver receiver2 = session2.createReceiver(getTestName());
      AmqpReceiver receiver3 = session3.createReceiver(getTestName());

      final Queue queue = getProxyToQueue(getTestName());
      assertEquals(3, queue.getMessageCount());

      // Begin the transaction that all senders will operate in.
      txnSession.begin();

      assertTrue(txnSession.isInTransaction());

      receiver1.flow(1);
      receiver2.flow(1);
      receiver3.flow(1);

      AmqpMessage message1 = receiver1.receive(5, TimeUnit.SECONDS);
      AmqpMessage message2 = receiver2.receive(5, TimeUnit.SECONDS);
      AmqpMessage message3 = receiver3.receive(5, TimeUnit.SECONDS);

      message1.accept(txnSession);
      message2.accept(txnSession);
      message3.accept(txnSession);

      assertEquals(3, queue.getMessageCount());

      txnSession.rollback();

      assertEquals(3, queue.getMessageCount());
   }

   @Test(timeout = 60000)
   public void testMultipleSessionSendersInSingleTXNWithCommit() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      // Root TXN session controls all TXN send lifetimes.
      AmqpSession txnSession = connection.createSession();

      // Create some sender sessions
      AmqpSession session1 = connection.createSession();
      AmqpSession session2 = connection.createSession();
      AmqpSession session3 = connection.createSession();

      // Sender linked to each session
      AmqpSender sender1 = session1.createSender(getTestName());
      AmqpSender sender2 = session2.createSender(getTestName());
      AmqpSender sender3 = session3.createSender(getTestName());

      final Queue queue = getProxyToQueue(getTestName());
      assertEquals(0, queue.getMessageCount());

      // Begin the transaction that all senders will operate in.
      txnSession.begin();

      AmqpMessage message = new AmqpMessage();
      message.setText("Test-Message");

      assertTrue(txnSession.isInTransaction());

      sender1.send(message, txnSession.getTransactionId());
      sender2.send(message, txnSession.getTransactionId());
      sender3.send(message, txnSession.getTransactionId());

      assertEquals(0, queue.getMessageCount());

      txnSession.commit();

      assertEquals(3, queue.getMessageCount());
   }

   @Test(timeout = 60000)
   public void testMultipleSessionSendersInSingleTXNWithRollback() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      // Root TXN session controls all TXN send lifetimes.
      AmqpSession txnSession = connection.createSession();

      // Create some sender sessions
      AmqpSession session1 = connection.createSession();
      AmqpSession session2 = connection.createSession();
      AmqpSession session3 = connection.createSession();

      // Sender linked to each session
      AmqpSender sender1 = session1.createSender(getTestName());
      AmqpSender sender2 = session2.createSender(getTestName());
      AmqpSender sender3 = session3.createSender(getTestName());

      final Queue queue = getProxyToQueue(getTestName());
      assertEquals(0, queue.getMessageCount());

      // Begin the transaction that all senders will operate in.
      txnSession.begin();

      AmqpMessage message = new AmqpMessage();
      message.setText("Test-Message");

      assertTrue(txnSession.isInTransaction());

      sender1.send(message, txnSession.getTransactionId());
      sender2.send(message, txnSession.getTransactionId());
      sender3.send(message, txnSession.getTransactionId());

      assertEquals(0, queue.getMessageCount());

      txnSession.rollback();

      assertEquals(0, queue.getMessageCount());
   }

   //----- Tests Ported from AmqpNetLite client -----------------------------//

   @Test(timeout = 60000)
   public void testSendersCommitAndRollbackWithMultipleSessionsInSingleTX() throws Exception {
      final int NUM_MESSAGES = 5;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      // Root TXN session controls all TXN send lifetimes.
      AmqpSession txnSession = connection.createSession();

      // Normal Session which won't create an TXN itself
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());

      // Commit TXN work from a sender.
      txnSession.begin();
      for (int i = 0; i < NUM_MESSAGES; ++i) {
         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message");
         sender.send(message, txnSession.getTransactionId());
      }
      txnSession.commit();

      // Rollback an additional batch of TXN work from a sender.
      txnSession.begin();
      for (int i = 0; i < NUM_MESSAGES; ++i) {
         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message");
         sender.send(message, txnSession.getTransactionId());
      }
      txnSession.rollback();

      // Commit more TXN work from a sender.
      txnSession.begin();
      for (int i = 0; i < NUM_MESSAGES; ++i) {
         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message");
         sender.send(message, txnSession.getTransactionId());
      }
      txnSession.commit();

      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(NUM_MESSAGES * 2);
      for (int i = 0; i < NUM_MESSAGES * 2; ++i) {
         AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(message);
         message.accept(txnSession);
      }

      connection.close();
   }

   @Test(timeout = 60000)
   public void testReceiversCommitAndRollbackWithMultipleSessionsInSingleTX() throws Exception {
      final int NUM_MESSAGES = 10;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      try {

         // Root TXN session controls all TXN send lifetimes.
         AmqpSession txnSession = connection.createSession();

         // Normal Session which won't create an TXN itself
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getTestName());

         for (int i = 0; i < NUM_MESSAGES + 1; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            message.setApplicationProperty("msgId", i);
            sender.send(message, txnSession.getTransactionId());
         }

         // Read all messages from the Queue, do not accept them yet.
         AmqpReceiver receiver = session.createReceiver(getTestName());
         ArrayList<AmqpMessage> messages = new ArrayList<>(NUM_MESSAGES);
         receiver.flow((NUM_MESSAGES + 2) * 2);
         for (int i = 0; i < NUM_MESSAGES; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            messages.add(message);
         }

         // Commit half the consumed messages
         txnSession.begin();
         for (int i = 0; i < NUM_MESSAGES / 2; ++i) {
            messages.get(i).accept(txnSession);
         }
         txnSession.commit();

         // Rollback the other half the consumed messages
         txnSession.begin();
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; ++i) {
            messages.get(i).accept(txnSession);
         }
         txnSession.rollback();

         {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals(NUM_MESSAGES, message.getApplicationProperty("msgId"));
            message.release();
         }

         // Commit the other half the consumed messages
         // This is a variation from the .NET client tests which doesn't settle the
         // messages in the TX until commit is called but on ActiveMQ they will be
         // redispatched regardless and not stay in the acquired state.
         txnSession.begin();
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            message.accept();
         }
         txnSession.commit();

         // The final message should still be pending.
         {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            receiver.flow(1);
            assertNotNull(message);
            assertEquals(NUM_MESSAGES, message.getApplicationProperty("msgId"));
            message.release();
         }

      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testCommitAndRollbackWithMultipleSessionsInSingleTX() throws Exception {
      final int NUM_MESSAGES = 10;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      // Root TXN session controls all TXN send lifetimes.
      AmqpSession txnSession = connection.createSession();

      // Normal Session which won't create an TXN itself
      AmqpSession session = connection.createSession();
      AmqpSender sender = session.createSender(getTestName());

      for (int i = 0; i < NUM_MESSAGES; ++i) {
         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message");
         message.setApplicationProperty("msgId", i);
         sender.send(message, txnSession.getTransactionId());
      }

      // Read all messages from the Queue, do not accept them yet.
      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(2);
      AmqpMessage message1 = receiver.receive(5, TimeUnit.SECONDS);
      AmqpMessage message2 = receiver.receive(5, TimeUnit.SECONDS);

      // Accept the first one in a TXN and send a new message in that TXN as well
      txnSession.begin();
      {
         message1.accept(txnSession);

         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message");
         message.setApplicationProperty("msgId", NUM_MESSAGES);

         sender.send(message, txnSession.getTransactionId());
      }
      txnSession.commit();

      // Accept the second one in a TXN and send a new message in that TXN as well but rollback
      txnSession.begin();
      {
         message2.accept(txnSession);

         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message");
         message.setApplicationProperty("msgId", NUM_MESSAGES + 1);
         sender.send(message, txnSession.getTransactionId());
      }
      txnSession.rollback();

      // Variation here from .NET code, the client settles the accepted message where
      // the .NET client does not and instead releases here to have it redelivered.

      receiver.flow(NUM_MESSAGES);
      for (int i = 1; i <= NUM_MESSAGES; ++i) {
         AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(message);
         assertEquals(i, message.getApplicationProperty("msgId"));
         message.accept();
      }

      // Should be nothing left.
      assertNull(receiver.receive(1, TimeUnit.SECONDS));

      connection.close();
   }

   @Test(timeout = 60000)
   public void testReceiversCommitAndRollbackWithMultipleSessionsInSingleTXNoSettlement() throws Exception {
      final int NUM_MESSAGES = 10;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = client.connect();

      try {

         // Root TXN session controls all TXN send lifetimes.
         AmqpSession txnSession = connection.createSession();

         // Normal Session which won't create an TXN itself
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getTestName());

         for (int i = 0; i < NUM_MESSAGES + 1; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            message.setApplicationProperty("msgId", i);
            sender.send(message, txnSession.getTransactionId());
         }

         // Read all messages from the Queue, do not accept them yet.
         AmqpReceiver receiver = session.createReceiver(getTestName());
         ArrayList<AmqpMessage> messages = new ArrayList<>(NUM_MESSAGES);
         receiver.flow((NUM_MESSAGES + 2) * 2);
         for (int i = 0; i < NUM_MESSAGES; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            System.out.println("Read message: " + message.getApplicationProperty("msgId"));
            assertNotNull(message);
            messages.add(message);
         }

         // Commit half the consumed messages [0, 1, 2, 3, 4]
         txnSession.begin();
         for (int i = 0; i < NUM_MESSAGES / 2; ++i) {
            System.out.println("Commit: Accepting message: " + messages.get(i).getApplicationProperty("msgId"));
            messages.get(i).accept(txnSession, false);
         }
         txnSession.commit();

         // Rollback the other half the consumed messages [5, 6, 7, 8, 9]
         txnSession.begin();
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; ++i) {
            System.out.println("Rollback: Accepting message: " + messages.get(i).getApplicationProperty("msgId"));
            messages.get(i).accept(txnSession, false);
         }
         txnSession.rollback();

         // After rollback messages should still be acquired so we read last sent message [10]
         {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            System.out.println("Read message: " + message.getApplicationProperty("msgId"));
            assertNotNull(message);
            assertEquals(NUM_MESSAGES, message.getApplicationProperty("msgId"));
            message.release();
         }

         // Commit the other half the consumed messages [5, 6, 7, 8, 9] which should still be acquired
         txnSession.begin();
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; ++i) {
            messages.get(i).accept(txnSession);
         }
         txnSession.commit();

         // The final message [10] should still be pending as we released it previously and committed
         // the previously accepted but not settled messages [5, 6, 7, 8, 9] in a new TX
         {
            receiver.flow(1);
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            System.out.println("Read message: " + message.getApplicationProperty("msgId"));
            assertNotNull(message);
            assertEquals(NUM_MESSAGES, message.getApplicationProperty("msgId"));
            message.accept();
         }

         // We should have now drained the Queue
         receiver.flow(1);
         AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
         if (message != null) {
            System.out.println("Read message: " + message.getApplicationProperty("msgId"));
         }
         assertNull(message);
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testCommitAndRollbackWithMultipleSessionsInSingleTXNoSettlement() throws Exception {
      final int NUM_MESSAGES = 10;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = client.connect();

      // Root TXN session controls all TXN send lifetimes.
      AmqpSession txnSession = connection.createSession();

      // Normal Session which won't create an TXN itself
      AmqpSession session = connection.createSession();
      AmqpSender sender = session.createSender(getTestName());

      for (int i = 0; i < NUM_MESSAGES; ++i) {
         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message");
         message.setApplicationProperty("msgId", i);
         sender.send(message, txnSession.getTransactionId());
      }

      // Read all messages from the Queue, do not accept them yet.
      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(2);
      AmqpMessage message1 = receiver.receive(5, TimeUnit.SECONDS);
      AmqpMessage message2 = receiver.receive(5, TimeUnit.SECONDS);

      // Accept the first one in a TXN and send a new message in that TXN as well
      txnSession.begin();
      {
         // This will result in message [0[ being consumed once we commit.
         message1.accept(txnSession, false);
         System.out.println("Commit: accepting message: " + message1.getApplicationProperty("msgId"));

         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message");
         message.setApplicationProperty("msgId", NUM_MESSAGES);

         sender.send(message, txnSession.getTransactionId());
      }
      txnSession.commit();

      // Accept the second one in a TXN and send a new message in that TXN as well but rollback
      txnSession.begin();
      {
         message2.accept(txnSession, false);
         System.out.println("Rollback: accepting message: " + message2.getApplicationProperty("msgId"));

         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message");
         message.setApplicationProperty("msgId", NUM_MESSAGES + 1);
         sender.send(message, txnSession.getTransactionId());
      }
      txnSession.rollback();

      // This releases message [1]
      message2.release();

      // Should be ten message available for dispatch given that we sent and committed one, and
      // releases another we had previously received.
      receiver.flow(10);
      for (int i = 1; i <= NUM_MESSAGES; ++i) {
         AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull("Expected a message for: " + i, message);
         System.out.println("Accepting message: " + message.getApplicationProperty("msgId"));
         assertEquals(i, message.getApplicationProperty("msgId"));
         message.accept();
      }

      // Should be nothing left.
      receiver.flow(1);
      assertNull(receiver.receive(1, TimeUnit.SECONDS));

      connection.close();
   }
}
