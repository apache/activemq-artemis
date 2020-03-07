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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AmqpInterceptor;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test basic send and receive scenarios using only AMQP sender and receiver links.
 */
public class AmqpSendReceiveInterceptorTest extends AmqpClientTestSupport {

   @Test(timeout = 60000)
   public void testCreateQueueReceiver() throws Exception {
      final CountDownLatch latch = new CountDownLatch(1);
      server.getRemotingService().addIncomingInterceptor(new AmqpInterceptor() {
         @Override
         public boolean intercept(AMQPMessage message, RemotingConnection connection) throws ActiveMQException {
            latch.countDown();
            return true;
         }
      });
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());
      AmqpMessage message = new AmqpMessage();

      message.setMessageId("msg" + 1);
      message.setText("Test-Message");
      sender.send(message);

      assertTrue(latch.await(5, TimeUnit.SECONDS));
      final CountDownLatch latch2 = new CountDownLatch(1);
      server.getRemotingService().addOutgoingInterceptor(new AmqpInterceptor() {
         @Override
         public boolean intercept(AMQPMessage packet, RemotingConnection connection) throws ActiveMQException {
            latch2.countDown();
            return true;
         }
      });
      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(2);
      AmqpMessage amqpMessage = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(amqpMessage);
      assertEquals(latch2.getCount(), 0);
      sender.close();
      receiver.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testRejectMessageWithIncomingInterceptor() throws Exception {
      final CountDownLatch latch = new CountDownLatch(1);
      server.getRemotingService().addIncomingInterceptor(new AmqpInterceptor() {
         @Override
         public boolean intercept(AMQPMessage message, RemotingConnection connection) throws ActiveMQException {
            latch.countDown();
            return false;
         }
      });
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());
      AmqpMessage message = new AmqpMessage();

      message.setMessageId("msg" + 1);
      message.setText("Test-Message");
      try {
         sender.send(message);
         fail("Sending message should have thrown exception here.");
      } catch (Exception e) {
         assertEquals("Interceptor rejected message [condition = failed]", e.getMessage());
      }

      assertTrue(latch.await(5, TimeUnit.SECONDS));
      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(2);
      AmqpMessage amqpMessage = receiver.receive(5, TimeUnit.SECONDS);
      assertNull(amqpMessage);
      sender.close();
      receiver.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testRejectMessageWithOutgoingInterceptor() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());
      AmqpMessage message = new AmqpMessage();

      message.setMessageId("msg" + 1);
      message.setText("Test-Message");
      sender.send(message);

      final CountDownLatch latch = new CountDownLatch(1);
      server.getRemotingService().addOutgoingInterceptor(new AmqpInterceptor() {
         @Override
         public boolean intercept(AMQPMessage packet, RemotingConnection connection) throws ActiveMQException {
            latch.countDown();
            return false;
         }
      });
      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(2);
      AmqpMessage amqpMessage = receiver.receive(5, TimeUnit.SECONDS);
      assertNull(amqpMessage);
      assertEquals(latch.getCount(), 0);
      sender.close();
      receiver.close();
      connection.close();
   }

   private static final String ADDRESS = "address";
   private static final String MESSAGE_ID = "messageId";
   private static final String CORRELATION_ID = "correlationId";
   private static final String MESSAGE_TEXT = "messageText";
   private static final String DURABLE = "durable";
   private static final String PRIORITY = "priority";
   private static final String REPLY_TO = "replyTo";
   private static final String TIME_TO_LIVE = "timeToLive";


   private boolean checkMessageProperties(AMQPMessage message, Map<String, Object> expectedProperties) {
      assertNotNull(message);
      assertNotNull(server.getNodeID());

      assertNotNull(message.getConnectionID());
      assertEquals(message.getAddress(), expectedProperties.get(ADDRESS));
      assertEquals(message.isDurable(), expectedProperties.get(DURABLE));

      Properties props = message.getProperties();
      assertEquals(props.getCorrelationId(), expectedProperties.get(CORRELATION_ID));
      assertEquals(props.getReplyTo(), expectedProperties.get(REPLY_TO));
      assertEquals(props.getMessageId(), expectedProperties.get(MESSAGE_ID));

      Header header = message.getHeader();
      assertEquals(header.getDurable(), expectedProperties.get(DURABLE));
      assertEquals(header.getTtl().toString(), expectedProperties.get(TIME_TO_LIVE).toString());
      assertEquals(header.getPriority().toString(), expectedProperties.get(PRIORITY).toString());
      return true;
   }

   @Test(timeout = 60000)
   public void testCheckInterceptedMessageProperties() throws Exception {
      final CountDownLatch latch = new CountDownLatch(1);

      final String addressQueue = getTestName();
      final String messageId = "lala200";
      final String correlationId = "lala-corrId";
      final String msgText = "Test intercepted message";
      final boolean durableMsg = false;
      final short priority = 8;
      final long timeToLive = 10000;
      final String replyTo = "reply-to-myQueue";

      Map<String, Object> expectedProperties = new HashMap<>();
      expectedProperties.put(ADDRESS, addressQueue);
      expectedProperties.put(MESSAGE_ID, messageId);
      expectedProperties.put(CORRELATION_ID, correlationId);
      expectedProperties.put(MESSAGE_TEXT, msgText);
      expectedProperties.put(DURABLE, durableMsg);
      expectedProperties.put(PRIORITY, priority);
      expectedProperties.put(REPLY_TO, replyTo);
      expectedProperties.put(TIME_TO_LIVE, timeToLive);

      server.getRemotingService().addIncomingInterceptor(new AmqpInterceptor() {
         @Override
         public boolean intercept(AMQPMessage message, RemotingConnection connection) throws ActiveMQException {
            latch.countDown();
            return checkMessageProperties(message, expectedProperties);
         }
      });

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());
      AmqpMessage message = new AmqpMessage();

      message.setMessageId(messageId);
      message.setCorrelationId(correlationId);
      message.setText(msgText);
      message.setDurable(durableMsg);
      message.setPriority(priority);
      message.setReplyToAddress(replyTo);
      message.setTimeToLive(timeToLive);

      sender.send(message);

      assertTrue(latch.await(2, TimeUnit.SECONDS));
      final CountDownLatch latch2 = new CountDownLatch(1);
      server.getRemotingService().addOutgoingInterceptor(new AmqpInterceptor() {
         @Override
         public boolean intercept(AMQPMessage packet, RemotingConnection connection) throws ActiveMQException {
            latch2.countDown();
            return checkMessageProperties(packet, expectedProperties);
         }
      });
      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(2);
      AmqpMessage amqpMessage = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(amqpMessage);
      assertEquals(latch2.getCount(), 0);
      sender.close();
      receiver.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testCheckRemotingConnection() throws Exception {
      final CountDownLatch latch = new CountDownLatch(1);
      final boolean[] passed = {false};
      server.getRemotingService().addIncomingInterceptor(new AmqpInterceptor() {
         @Override
         public boolean intercept(AMQPMessage message, RemotingConnection connection) throws ActiveMQException {
            passed[0] = connection != null;
            latch.countDown();
            return true;
         }
      });

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());
      AmqpMessage message = new AmqpMessage();
      message.setMessageId("msg" + 1);
      message.setText("Test-Message");
      sender.send(message);

      assertTrue(latch.await(2, TimeUnit.SECONDS));
      assertTrue("connection not set", passed[0]);

      final CountDownLatch latch2 = new CountDownLatch(1);
      server.getRemotingService().addOutgoingInterceptor(new AmqpInterceptor() {
         @Override
         public boolean intercept(AMQPMessage packet, RemotingConnection connection) throws ActiveMQException {
            passed[0] = connection != null;
            latch2.countDown();
            return true;
         }
      });
      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(2);
      AmqpMessage amqpMessage = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(amqpMessage);
      assertEquals(latch2.getCount(), 0);
      assertTrue("connection not set", passed[0]);
      sender.close();
      receiver.close();
      connection.close();
   }
}
