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
import org.junit.Test;

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
      assertEquals(latch.getCount(), 0);
      receiver.close();
      connection.close();
   }
}
