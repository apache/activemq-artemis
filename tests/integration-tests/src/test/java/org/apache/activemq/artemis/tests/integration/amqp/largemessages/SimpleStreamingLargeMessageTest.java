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
package org.apache.activemq.artemis.tests.integration.amqp.largemessages;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test broker behavior when creating AMQP senders
 */
@RunWith(value = Parameterized.class)
public class SimpleStreamingLargeMessageTest extends AmqpClientTestSupport {

   private String smallFrameAcceptor = new String("tcp://localhost:" + (AMQP_PORT + 8));

   int frameSize;
   int minLargeMessageSize;

   @Parameterized.Parameters(name = "frameSize = {0}, minLargeMessage = {1}")
   public static Iterable<? extends Object> testParameters() {
      // The reason I use two frames sizes here
      // is because a message that wasn't broken into frames
      // but still beyond 50K, should still be considered large when storing
      return Arrays.asList(new Object[][]{{512, 50000}, {1024 * 1024, 50000},
         // we disable large message for at least one parameter to compare results between large and non large messages
         {1024 * 1024, 50000000}});
   }

   public SimpleStreamingLargeMessageTest(int frameSize, int minLargeMessageSize) {
      this.frameSize = frameSize;
      this.minLargeMessageSize = minLargeMessageSize;
   }

   @Override
   protected void addAdditionalAcceptors(ActiveMQServer server) throws Exception {
      server.getConfiguration().addAcceptorConfiguration("flow", smallFrameAcceptor + "?protocols=AMQP;useEpoll=false;maxFrameSize=" + frameSize + ";amqpMinLargeMessageSize=" + minLargeMessageSize);
   }

   @Test(timeout = 60000)
   public void testSendNonPersistent() throws Exception {
      testSend(false, false);
   }

   @Test(timeout = 60000)
   public void testSendPersistent() throws Exception {
      testSend(true, false);
   }

   @Test(timeout = 60000)
   public void testSendPersistentRestartServer() throws Exception {
      testSend(true, true);
   }

   public void testSend(boolean persistent, boolean restartServer) throws Exception {
      try {
         int size = 100 * 1024;
         AmqpClient client = createAmqpClient(new URI(smallFrameAcceptor));
         AmqpConnection connection = client.createConnection();
         addConnection(connection);
         connection.setMaxFrameSize(2 * 1024);
         connection.connect();

         AmqpSession session = connection.createSession();

         AmqpSender sender = session.createSender(getQueueName());

         Queue queueView = getProxyToQueue(getQueueName());
         assertNotNull(queueView);
         assertEquals(0, queueView.getMessageCount());

         session.begin();
         for (int m = 0; m < 10; m++) {
            AmqpMessage message = new AmqpMessage();
            message.setDurable(persistent);
            byte[] bytes = new byte[size];
            for (int i = 0; i < bytes.length; i++) {
               bytes[i] = (byte) 'z';
            }

            message.setBytes(bytes);
            sender.send(message);
         }
         session.commit();

         AMQPLargeMessagesTestUtil.validateAllTemporaryBuffers(server);

         if (restartServer) {
            connection.close();
            server.stop();
            server.start();

            connection = client.createConnection();
            addConnection(connection);
            connection.setMaxFrameSize(2 * 1024);
            connection.connect();
            session = connection.createSession();
         }

         queueView = getProxyToQueue(getQueueName());
         Wait.assertEquals(10, queueView::getMessageCount);

         AmqpReceiver receiver = session.createReceiver(getQueueName());
         receiver.flow(10);
         for (int i = 0; i < 10; i++) {
            AmqpMessage msgReceived = receiver.receive(10, TimeUnit.SECONDS);
            Assert.assertNotNull(msgReceived);
            Data body = (Data) msgReceived.getWrappedMessage().getBody();
            byte[] bodyArray = body.getValue().getArray();
            for (int bI = 0; bI < size; bI++) {
               Assert.assertEquals((byte) 'z', bodyArray[bI]);
            }
            msgReceived.accept(true);
         }

         receiver.flow(1);
         Assert.assertNull(receiver.receiveNoWait());

         receiver.close();

         connection.close();

         Wait.assertEquals(0, queueView::getMessageCount);
         validateNoFilesOnLargeDir();
      } catch (Exception e) {
         e.printStackTrace();
         throw e;
      }
   }

   @Test
   public void testSendWithPropertiesAndFilterPersistentRestart() throws Exception {
      testSendWithPropertiesAndFilter(true, true);

   }

   @Test
   public void testSendWithPropertiesAndFilterPersistentNoRestart() throws Exception {
      testSendWithPropertiesAndFilter(true, false);

   }

   @Test
   public void testSendWithPropertiesNonPersistent() throws Exception {
      testSendWithPropertiesAndFilter(false, false);

   }

   public void testSendWithPropertiesAndFilter(boolean persistent, boolean restartServer) throws Exception {
      try {

         int size = 100 * 1024;
         AmqpClient client = createAmqpClient(new URI(smallFrameAcceptor));
         AmqpConnection connection = client.createConnection();
         addConnection(connection);
         connection.setMaxFrameSize(2 * 1024);
         connection.connect();

         AmqpSession session = connection.createSession();

         AmqpSender sender = session.createSender(getQueueName());

         Queue queueView = getProxyToQueue(getQueueName());
         assertNotNull(queueView);
         assertEquals(0, queueView.getMessageCount());

         session.begin();
         int oddID = 0;
         for (int m = 0; m < 10; m++) {
            AmqpMessage message = new AmqpMessage();
            message.setDurable(persistent);
            boolean odd = (m % 2 == 0);
            message.setApplicationProperty("i", m);
            message.setApplicationProperty("oddString", odd ? "odd" : "even");
            message.setApplicationProperty("odd", odd);
            if (odd) {
               message.setApplicationProperty("oddID", oddID++);
            }

            byte[] bytes = new byte[size];
            for (int i = 0; i < bytes.length; i++) {
               bytes[i] = (byte) 'z';
            }

            message.setBytes(bytes);
            sender.send(message);
            if (m == 5) {
               // we will send half transactionally, half normally
               session.commit();
            }
         }

         AMQPLargeMessagesTestUtil.validateAllTemporaryBuffers(server);

         if (restartServer) {
            connection.close();
            server.stop();
            server.start();

            connection = client.createConnection();
            addConnection(connection);
            connection.setMaxFrameSize(2 * 1024);
            connection.connect();
            session = connection.createSession();
         }

         queueView = getProxyToQueue(getQueueName());
         Wait.assertEquals(10, queueView::getMessageCount);

         AmqpReceiver receiver = session.createReceiver(getQueueName(), "odd=true");
         receiver.flow(10);
         for (int i = 0; i < 5; i++) {
            AmqpMessage msgReceived = receiver.receive(10, TimeUnit.SECONDS);
            Assert.assertNotNull(msgReceived);
            Assert.assertTrue((boolean)msgReceived.getApplicationProperty("odd"));
            Assert.assertEquals(i, (int)msgReceived.getApplicationProperty("oddID"));
            Data body = (Data) msgReceived.getWrappedMessage().getBody();
            byte[] bodyArray = body.getValue().getArray();
            for (int bI = 0; bI < size; bI++) {
               Assert.assertEquals((byte) 'z', bodyArray[bI]);
            }
            msgReceived.accept(true);
         }

         receiver.flow(1);
         Assert.assertNull(receiver.receiveNoWait());

         receiver.close();
         connection.close();

         validateNoFilesOnLargeDir(getLargeMessagesDir(), 5);
      } catch (Exception e) {
         e.printStackTrace();
         throw e;
      }

   }


   @Test
   public void testSingleMessage() throws Exception {
      try {

         int size = 100 * 1024;
         AmqpClient client = createAmqpClient(new URI(smallFrameAcceptor));
         AmqpConnection connection = client.createConnection();
         addConnection(connection);
         connection.setMaxFrameSize(2 * 1024);
         connection.connect();

         AmqpSession session = connection.createSession();

         AmqpSender sender = session.createSender(getQueueName());

         Queue queueView = getProxyToQueue(getQueueName());
         assertNotNull(queueView);
         assertEquals(0, queueView.getMessageCount());

         session.begin();
         int oddID = 0;
         for (int m = 0; m < 1; m++) {
            AmqpMessage message = new AmqpMessage();
            message.setDurable(true);
            boolean odd = (m % 2 == 0);
            message.setApplicationProperty("i", m);
            message.setApplicationProperty("oddString", odd ? "odd" : "even");
            message.setApplicationProperty("odd", odd);
            if (odd) {
               message.setApplicationProperty("oddID", oddID++);
            }

            byte[] bytes = new byte[size];
            for (int i = 0; i < bytes.length; i++) {
               bytes[i] = (byte) 'z';
            }

            message.setBytes(bytes);
            sender.send(message);
         }

         session.commit();

         Queue queue = server.locateQueue(SimpleString.toSimpleString(getQueueName()));

         Wait.assertEquals(1, queue::getMessageCount);

         LinkedListIterator<MessageReference> browserIterator = queue.browserIterator();

         while (browserIterator.hasNext()) {
            MessageReference ref = browserIterator.next();
            org.apache.activemq.artemis.api.core.Message message = ref.getMessage();

            Assert.assertNotNull(message);
            Assert.assertTrue(message instanceof LargeServerMessage);

            Assert.assertFalse(((LargeServerMessage)message).hasPendingRecord());
         }
         browserIterator.close();

         connection.close();

         server.stop();

         server.start();

         connection = client.createConnection();
         addConnection(connection);
         connection.setMaxFrameSize(2 * 1024);
         connection.connect();
         session = connection.createSession();

         AmqpReceiver receiver = session.createReceiver(getQueueName());
         receiver.flow(1);
         for (int i = 0; i < 1; i++) {
            AmqpMessage msgReceived = receiver.receive(10, TimeUnit.SECONDS);
            Assert.assertNotNull(msgReceived);
            Assert.assertTrue((boolean)msgReceived.getApplicationProperty("odd"));
            Assert.assertEquals(i, (int)msgReceived.getApplicationProperty("oddID"));
            Data body = (Data) msgReceived.getWrappedMessage().getBody();
            byte[] bodyArray = body.getValue().getArray();
            for (int bI = 0; bI < size; bI++) {
               Assert.assertEquals((byte) 'z', bodyArray[bI]);
            }
            msgReceived.accept(true);
         }

         receiver.flow(1);
         Assert.assertNull(receiver.receiveNoWait());

         receiver.close();
         connection.close();

         validateNoFilesOnLargeDir(getLargeMessagesDir(), 0);
      } catch (Exception e) {
         e.printStackTrace();
         throw e;
      }

   }

   @Test
   public void testJMSPersistentTX() throws Exception {

      boolean persistent = true;
      boolean tx = true;

      jmsTest(persistent, tx);
   }

   @Test
   public void testJMSPersistentNonTX() throws Exception {

      boolean persistent = true;
      boolean tx = false;

      jmsTest(persistent, tx);
   }

   @Test
   public void testJMSNonPersistentTX() throws Exception {

      boolean persistent = false;
      boolean tx = true;

      jmsTest(persistent, tx);
   }

   @Test
   public void testJMSNonPersistentNonTX() throws Exception {

      boolean persistent = false;
      boolean tx = false;

      jmsTest(persistent, tx);
   }

   private void jmsTest(boolean persistent, boolean tx) throws JMSException {
      int MESSAGE_SIZE = 100 * 1024;
      int MESSAGES = 10;
      String producerUri = "amqp://localhost:5672";
      final JmsConnectionFactory producerFactory = new JmsConnectionFactory(producerUri);
      try (Connection producerConnection = producerFactory.createConnection(); Session producerSession = producerConnection.createSession(tx, tx ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE)) {
         producerConnection.start();
         final Destination queue = producerSession.createQueue(getQueueName());
         String consumerUri = "amqp://localhost:5672";
         final JmsConnectionFactory consumerConnectionFactory = new JmsConnectionFactory(consumerUri);
         try (Connection consumerConnection = consumerConnectionFactory.createConnection(); Session consumerSession = consumerConnection.createSession(tx, tx ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE); MessageConsumer consumer = consumerSession.createConsumer(queue); MessageProducer producer = producerSession.createProducer(queue)) {
            if (persistent) {
               producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            } else {
               producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            }
            consumerConnection.start();
            final byte[] largeMessageContent = new byte[MESSAGE_SIZE];
            final byte[] receivedContent = new byte[largeMessageContent.length];
            ThreadLocalRandom.current().nextBytes(largeMessageContent);
            for (int i = 0; i < MESSAGES; i++) {
               final BytesMessage sentMessage = producerSession.createBytesMessage();
               sentMessage.writeBytes(largeMessageContent);
               producer.send(sentMessage);
               if (tx) {
                  producerSession.commit();
               }
               final Message receivedMessage = consumer.receive(5000);
               Assert.assertNotNull("A message should be received in 5000 ms", receivedMessage);
               if (tx) {
                  consumerSession.commit();
               }
               Assert.assertThat(receivedMessage, IsInstanceOf.instanceOf(sentMessage.getClass()));
               Assert.assertEquals(largeMessageContent.length, ((BytesMessage) receivedMessage).readBytes(receivedContent));
               Assert.assertArrayEquals(largeMessageContent, receivedContent);
            }
         }
      }
   }

}