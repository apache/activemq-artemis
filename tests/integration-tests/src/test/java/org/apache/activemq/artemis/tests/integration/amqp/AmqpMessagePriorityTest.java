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

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.util.Wait;
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
 * Tests for handling of the AMQP message priority header.
 */
public class AmqpMessagePriorityTest extends AmqpClientTestSupport {

   protected static final Logger LOG = LoggerFactory.getLogger(AmqpMessagePriorityTest.class);

   @Test(timeout = 60000)
   public void testMessageDefaultPriority() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      AmqpMessage message = new AmqpMessage();
      message.setMessageId("MessageID:1");
      message.setPriority((short) 4);

      sender.send(message);
      sender.close();

      Queue queueView = getProxyToQueue(getQueueName());
      Wait.assertEquals(1L, queueView::getMessageCount, 5000, 10);

      AmqpReceiver receiver = session.createReceiver(getQueueName());

      receiver.flow(1);
      AmqpMessage receive = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(receive);
      assertEquals((short) 4, receive.getPriority());
      receiver.close();

      assertEquals(1, queueView.getMessageCount());

      connection.close();
   }

   @Test(timeout = 60000)
   public void testMessagePriorityPreservedAfterServerRestart() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      AmqpMessage message = new AmqpMessage();
      message.setDurable(true);
      message.setMessageId("MessageID:1");
      message.setPriority((short) 7);

      sender.send(message);
      sender.close();
      connection.close();

      server.stop();
      server.start();

      client = createAmqpClient();
      connection = addConnection(client.connect());
      session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(getQueueName());

      Queue queueView = getProxyToQueue(getQueueName());
      assertEquals(1, queueView.getMessageCount());

      receiver.flow(1);
      AmqpMessage receive = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(receive);
      assertEquals((short) 7, receive.getPriority());
      receiver.close();

      assertEquals(1, queueView.getMessageCount());

      connection.close();
   }

   @Test(timeout = 60000)
   public void testMessageNonDefaultPriority() throws Exception {

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      AmqpMessage message = new AmqpMessage();
      message.setMessageId("MessageID:1");
      message.setPriority((short) 0);

      sender.send(message);
      sender.close();

      Queue queueView = getProxyToQueue(getQueueName());
      Wait.assertEquals(1L, queueView::getMessageCount, 5000, 10);

      AmqpReceiver receiver = session.createReceiver(getQueueName());

      receiver.flow(1);
      AmqpMessage receive = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(receive);
      assertEquals((short) 0, receive.getPriority());
      receiver.close();

      assertEquals(1, queueView.getMessageCount());

      connection.close();
   }

   @Test(timeout = 60000)
   public void testMessageWithVeryHighPriority() throws Exception {

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      AmqpMessage message = new AmqpMessage();
      message.setMessageId("MessageID:1");
      message.setPriority((short) 99);

      sender.send(message);
      sender.close();

      Queue queueView = getProxyToQueue(getQueueName());
      Wait.assertEquals(1L, queueView::getMessageCount, 5000, 10);

      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(1);
      AmqpMessage receive = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(receive);
      assertEquals(99, receive.getPriority());
      receiver.close();

      assertEquals(1, queueView.getMessageCount());

      connection.close();
   }

   @Test(timeout = 60000)
   public void testMessageNoPriority() throws Exception {

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      AmqpMessage message = new AmqpMessage();
      message.setMessageId("MessageID:1");

      sender.send(message);
      sender.close();

      Queue queueView = getProxyToQueue(getQueueName());
      Wait.assertEquals(1L, queueView::getMessageCount, 5000, 10);

      AmqpReceiver receiver = session.createReceiver(getQueueName());

      receiver.flow(1);
      AmqpMessage receive = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(receive);
      assertEquals((short) 4, receive.getPriority());
      receiver.close();

      assertEquals(1, queueView.getMessageCount());

      connection.close();
   }

   @Test(timeout = 60000)
   public void testMessagePriorityOrdering() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      for (short i = 0; i <= 9; ++i) {
         AmqpMessage message = new AmqpMessage();
         message.setMessageId("MessageID:" + i);
         message.setPriority(i);
         sender.send(message);
      }

      sender.close();

      Queue queueView = getProxyToQueue(getQueueName());
      Wait.assertEquals(10L, queueView::getMessageCount, 5000, 10);

      AmqpReceiver receiver = session.createReceiver(getQueueName());

      receiver.flow(10);
      for (int i = 9; i >= 0; --i) {
         AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(received);
         assertEquals((short) i, received.getPriority());
         received.accept();
      }
      receiver.close();

      Wait.assertEquals(0L, queueView::getMessageCount, 5000, 10);

      connection.close();
   }
}
