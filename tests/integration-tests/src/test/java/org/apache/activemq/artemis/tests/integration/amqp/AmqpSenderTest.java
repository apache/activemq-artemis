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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test broker behavior when creating AMQP senders
 */
public class AmqpSenderTest extends AmqpClientTestSupport {

   @Override
   protected void addAdditionalAcceptors(ActiveMQServer server) throws Exception {
   }

   @Test(timeout = 60000)
   public void testSenderSettlementModeSettledIsHonored() throws Exception {
      doTestSenderSettlementModeIsHonored(SenderSettleMode.SETTLED);
   }

   @Test(timeout = 60000)
   public void testSenderSettlementModeUnsettledIsHonored() throws Exception {
      doTestSenderSettlementModeIsHonored(SenderSettleMode.UNSETTLED);
   }

   @Test(timeout = 60000)
   public void testSenderSettlementModeMixedIsHonored() throws Exception {
      doTestSenderSettlementModeIsHonored(SenderSettleMode.MIXED);
   }

   public void doTestSenderSettlementModeIsHonored(SenderSettleMode settleMode) throws Exception {
      AmqpClient client = createAmqpClient();

      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender("queue://" + getTestName(), settleMode, ReceiverSettleMode.FIRST);

      Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);
      assertEquals(0, queueView.getMessageCount());

      assertEquals(settleMode, sender.getEndpoint().getRemoteSenderSettleMode());

      AmqpMessage message = new AmqpMessage();
      message.setText("Test-Message");
      sender.send(message);

      sender.close();

      connection.close();
   }

   @Test(timeout = 60000)
   public void testReceiverSettlementModeSetToFirst() throws Exception {
      doTestReceiverSettlementModeForcedToFirst(ReceiverSettleMode.FIRST);
   }

   @Test(timeout = 60000)
   public void testReceiverSettlementModeSetToSecond() throws Exception {
      doTestReceiverSettlementModeForcedToFirst(ReceiverSettleMode.SECOND);
   }

   /*
    * The Broker does not currently support ReceiverSettleMode of SECOND so we ensure that it
    * always drops that back to FIRST to let the client know. The client will need to check and
    * react accordingly.
    */
   private void doTestReceiverSettlementModeForcedToFirst(ReceiverSettleMode modeToUse) throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender("queue://" + getTestName(), SenderSettleMode.UNSETTLED, modeToUse);

      Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);
      assertEquals(0, queueView.getMessageCount());

      assertEquals(ReceiverSettleMode.FIRST, sender.getEndpoint().getRemoteReceiverSettleMode());

      sender.close();

      connection.close();
   }

   @Test(timeout = 60000)
   public void testUnsettledSender() throws Exception {
      final int MSG_COUNT = 1000;

      final CountDownLatch settled = new CountDownLatch(MSG_COUNT);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      connection.setStateInspector(new AmqpValidator() {

         @Override
         public void inspectDeliveryUpdate(Sender sender, Delivery delivery) {
            if (delivery.remotelySettled()) {
               settled.countDown();
            }
         }
      });

      AmqpSession session = connection.createSession();
      AmqpSender sender = session.createSender(getQueueName(), false);

      for (int i = 1; i <= MSG_COUNT; ++i) {
         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message: " + i);
         sender.send(message);

      }

      Queue queueView = getProxyToQueue(getQueueName());
      Wait.assertTrue("All messages should arrive", () -> queueView.getMessageCount() == MSG_COUNT);

      sender.close();

      assertTrue("Remote should have settled all deliveries", settled.await(5, TimeUnit.MINUTES));

      connection.close();
   }


   @Test(timeout = 60000)
   public void testMixDurableAndNonDurable() throws Exception {
      final int MSG_COUNT = 2000;

      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", getBrokerAmqpConnectionURI().toString() + "?jms.forceAsyncSend=true");
      Connection connection = factory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = session.createQueue(getQueueName());
      MessageProducer sender = session.createProducer(queue);

      boolean durable = false;
      for (int i = 1; i <= MSG_COUNT; ++i) {
         javax.jms.Message message = session.createMessage();
         message.setIntProperty("i", i);
         sender.setDeliveryMode(durable ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         durable = !durable; // flipping the switch
         sender.send(message);
      }

      connection.start();
      MessageConsumer receiver = session.createConsumer(queue);

      for (int i = 1; i <= MSG_COUNT; ++i) {
         javax.jms.Message message = receiver.receive(10000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getIntProperty("i"));
      }

      Assert.assertNull(receiver.receiveNoWait());

      connection.close();
   }

   @Test(timeout = 60000)
   public void testPresettledSender() throws Exception {
      final int MSG_COUNT = 1000;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName(), true);

      for (int i = 1; i <= MSG_COUNT; ++i) {
         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message: " + i);
         sender.send(message);

      }

      Queue queueView = getProxyToQueue(getQueueName());
      Wait.assertTrue("All messages should arrive", () -> queueView.getMessageCount() == MSG_COUNT);

      sender.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testDuplicateDetection() throws Exception {
      final int MSG_COUNT = 10;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName(), true);

      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.setPresettle(true);
      receiver.flow(10);
      Assert.assertNull("somehow the queue had messages from a previous test", receiver.receiveNoWait());

      for (int i = 1; i <= MSG_COUNT; ++i) {
         AmqpMessage message = new AmqpMessage();
         message.setApplicationProperty(Message.HDR_DUPLICATE_DETECTION_ID.toString(), "123");
         sender.send(message);
      }

      AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
      Assert.assertNull(receiver.receiveNoWait());

      sender.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testSenderCreditReplenishment() throws Exception {
      AtomicInteger counter = new AtomicInteger();
      CountDownLatch initialCredit = new CountDownLatch(1);
      CountDownLatch refreshedCredit = new CountDownLatch(1);

      AmqpClient client = createAmqpClient(guestUser, guestPass);
      client.setValidator(new AmqpValidator() {
         @Override
         public void inspectCredit(Sender sender) {
            int count = counter.incrementAndGet();
            switch (count) {
               case 1:
                  assertEquals("Unexpected initial credit", AmqpSupport.AMQP_CREDITS_DEFAULT, sender.getCredit());
                  initialCredit.countDown();
                  break;
               case 2:
                  assertEquals("Unexpected replenished credit", AmqpSupport.AMQP_CREDITS_DEFAULT, sender.getCredit());
                  refreshedCredit.countDown();
                  break;
               default:
                  throw new IllegalStateException("Unexpected additional flow: " + count);
            }
         }
      });
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getQueueName());

         // Wait for initial credit to arrive and be checked
         assertTrue("Expected credit did not arrive", initialCredit.await(3000, TimeUnit.MILLISECONDS));

         // Send just enough messages not to cause credit replenishment
         final int msgCount = AmqpSupport.AMQP_CREDITS_DEFAULT - AmqpSupport.AMQP_LOW_CREDITS_DEFAULT;
         for (int i = 1; i <= msgCount - 1; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message: " + i);
            sender.send(message);
         }

         // Wait and check more credit hasn't flowed yet
         assertFalse("Expected credit not to have been refreshed yet", refreshedCredit.await(50, TimeUnit.MILLISECONDS));

         // Send a final message needed to provoke the replenishment flow, wait for to arrive
         AmqpMessage message = new AmqpMessage();
         message.setText("Test-Message: " + msgCount);
         sender.send(message);

         assertTrue("Expected credit refresh did not occur", refreshedCredit.await(3000, TimeUnit.MILLISECONDS));

         connection.close();
      } finally {
         connection.getStateInspector().assertValid();
      }
   }
}