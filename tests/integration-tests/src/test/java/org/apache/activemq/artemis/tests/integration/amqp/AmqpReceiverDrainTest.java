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
 * Tests various behaviors of broker side drain support.
 */
public class AmqpReceiverDrainTest extends AmqpClientTestSupport {

   @Test(timeout = 60000)
   public void testReceiverCanDrainMessages() throws Exception {
      int MSG_COUNT = 20;
      sendMessages(getQueueName(), MSG_COUNT);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = client.connect();
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(getQueueName());

      Queue queueView = getProxyToQueue(getQueueName());
      assertEquals(MSG_COUNT, queueView.getMessageCount());

      receiver.drain(MSG_COUNT);
      for (int i = 0; i < MSG_COUNT; ++i) {
         AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(message);
         message.accept();
      }
      receiver.close();

      assertEquals(0, queueView.getMessageCount());

      connection.close();
   }

   @Test(timeout = 60000)
   public void testPullWithNoMessageGetDrained() throws Exception {

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = client.connect();
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(getQueueName());

      receiver.flow(10);

      Queue queueView = getProxyToQueue(getQueueName());
      assertEquals(0, queueView.getMessageCount());
      assertEquals(0, queueView.getDeliveringCount());

      assertEquals(10, receiver.getReceiver().getRemoteCredit());

      assertNull(receiver.pull(1, TimeUnit.SECONDS));

      assertEquals(0, receiver.getReceiver().getRemoteCredit());

      connection.close();
   }

   @Test(timeout = 60000)
   public void testPullOneFromRemote() throws Exception {
      int MSG_COUNT = 20;
      sendMessages(getQueueName(), MSG_COUNT);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = client.connect();
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(getQueueName());

      Queue queueView = getProxyToQueue(getQueueName());
      assertEquals(MSG_COUNT, queueView.getMessageCount());

      assertEquals(0, receiver.getReceiver().getRemoteCredit());

      AmqpMessage message = receiver.pull(5, TimeUnit.SECONDS);
      assertNotNull(message);
      message.accept();

      assertEquals(0, receiver.getReceiver().getRemoteCredit());

      receiver.close();

      assertEquals(MSG_COUNT - 1, queueView.getMessageCount());
      assertEquals(1, queueView.getMessagesAcknowledged());

      connection.close();
   }

   @Test(timeout = 60000)
   public void testMultipleZeroResultPulls() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = client.connect();
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(getQueueName());

      receiver.flow(10);

      Queue queueView = getProxyToQueue(getQueueName());
      assertEquals(0, queueView.getMessageCount());

      assertEquals(10, receiver.getReceiver().getRemoteCredit());

      assertNull(receiver.pull(1, TimeUnit.SECONDS));

      assertEquals(0, receiver.getReceiver().getRemoteCredit());

      assertNull(receiver.pull(1, TimeUnit.SECONDS));
      assertNull(receiver.pull(1, TimeUnit.SECONDS));

      assertEquals(0, receiver.getReceiver().getRemoteCredit());

      connection.close();
   }

   public void sendMessages(String destinationName, int count) throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = null;

      try {
         connection = client.connect();
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(destinationName);

         for (int i = 0; i < count; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message-" + i);
            sender.send(message);
         }

         sender.close();
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }
}
