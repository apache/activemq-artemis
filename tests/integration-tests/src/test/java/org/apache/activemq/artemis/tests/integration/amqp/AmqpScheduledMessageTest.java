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
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;

/**
 * Test for scheduled message support using AMQP message annotations.
 */
public class AmqpScheduledMessageTest extends AmqpClientTestSupport {

   @Test(timeout = 60000)
   public void testSendWithDeliveryTimeIsScheduled() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getTestName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      long deliveryTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
      message.setMessageAnnotation("x-opt-delivery-time", deliveryTime);
      message.setText("Test-Message");
      sender.send(message);
      sender.close();

      assertEquals(1, queueView.getScheduledCount());

      // Now try and get the message
      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(1);
      AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
      assertNull(received);

      connection.close();
   }

   @Test(timeout = 60000)
   public void testSendRecvWithDeliveryTime() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getTestName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      long deliveryTime = System.currentTimeMillis() + 6000;
      message.setMessageAnnotation("x-opt-delivery-time", deliveryTime);
      message.setText("Test-Message");
      sender.send(message);
      sender.close();

      assertEquals(1, queueView.getScheduledCount());

      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(1);

      // Now try and get the message, should not due to being scheduled.
      AmqpMessage received = receiver.receive(2, TimeUnit.SECONDS);
      assertNull(received);

      // Now try and get the message, should get it now
      received = receiver.receive(10, TimeUnit.SECONDS);
      assertNotNull(received);
      received.accept();

      connection.close();
   }

   @Test
   public void testScheduleWithDelay() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getTestName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getTestName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      long delay = 6000;
      message.setMessageAnnotation("x-opt-delivery-delay", delay);
      message.setText("Test-Message");
      sender.send(message);
      sender.close();

      assertEquals(1, queueView.getScheduledCount());

      AmqpReceiver receiver = session.createReceiver(getTestName());
      receiver.flow(1);

      // Now try and get the message, should not due to being scheduled.
      AmqpMessage received = receiver.receive(2, TimeUnit.SECONDS);
      assertNull(received);

      // Now try and get the message, should get it now
      received = receiver.receive(10, TimeUnit.SECONDS);
      assertNotNull(received);
      received.accept();

      connection.close();
   }
}
