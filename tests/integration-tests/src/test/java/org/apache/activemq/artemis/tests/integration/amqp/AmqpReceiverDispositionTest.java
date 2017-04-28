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

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;

/**
 * Test various behaviors of AMQP receivers with the broker.
 */
public class AmqpReceiverDispositionTest extends AmqpClientTestSupport {

   @Test(timeout = 30000)
   public void testReleasedDisposition() throws Exception {
      sendMessages(getQueueName(), 1);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver1 = session.createReceiver(getQueueName());
      receiver1.flow(1);

      AmqpMessage message = receiver1.receive(5, TimeUnit.SECONDS);

      AmqpReceiver receiver2 = session.createReceiver(getQueueName());

      assertNotNull("did not receive message first time", message);
      assertEquals("MessageID:0", message.getMessageId());

      Message protonMessage = message.getWrappedMessage();
      assertNotNull(protonMessage);
      assertEquals("Unexpected initial value for AMQP delivery-count", 0, protonMessage.getDeliveryCount());

      receiver2.flow(1);
      message.release();

      // Read the message again and validate its state
      message = receiver2.receive(10, TimeUnit.SECONDS);
      assertNotNull("did not receive message again", message);
      assertEquals("MessageID:0", message.getMessageId());

      message.accept();

      protonMessage = message.getWrappedMessage();
      assertNotNull(protonMessage);
      assertEquals("Unexpected updated value for AMQP delivery-count", 0, protonMessage.getDeliveryCount());

      connection.close();
   }

   @Test(timeout = 30000)
   public void testRejectedDisposition() throws Exception {
      sendMessages(getQueueName(), 1);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver1 = session.createReceiver(getQueueName());
      receiver1.flow(1);

      AmqpMessage message = receiver1.receive(5, TimeUnit.SECONDS);
      assertNotNull("did not receive message first time", message);
      assertEquals("MessageID:0", message.getMessageId());

      Message protonMessage = message.getWrappedMessage();
      assertNotNull(protonMessage);
      assertEquals("Unexpected initial value for AMQP delivery-count", 0, protonMessage.getDeliveryCount());

      message.reject();

      // Reject is a terminal outcome and should not be redelivered to the rejecting receiver
      // or any other as it should move to the archived state.
      receiver1.flow(1);
      message = receiver1.receiveNoWait();
      assertNull("Should not receive message again", message);

      // Attempt to Read the message again with another receiver to validate it is archived.
      AmqpReceiver receiver2 = session.createReceiver(getQueueName());
      receiver2.flow(1);
      assertNull(receiver2.receiveNoWait());

      connection.close();
   }

   @Test(timeout = 30000)
   public void testModifiedDispositionWithDeliveryFailedWithoutUndeliverableHereFieldsSet() throws Exception {
      doModifiedDispositionTestImpl(Boolean.TRUE, null);
   }

   @Test(timeout = 30000)
   public void testModifiedDispositionWithoutDeliveryFailedWithoutUndeliverableHereFieldsSet() throws Exception {
      doModifiedDispositionTestImpl(null, null);
   }

   @Test(timeout = 30000)
   public void testModifiedDispositionWithoutDeliveryFailedWithUndeliverableHereFieldsSet() throws Exception {
      doModifiedDispositionTestImpl(null, Boolean.TRUE);
   }

   @Test(timeout = 30000)
   public void testModifiedDispositionWithDeliveryFailedWithUndeliverableHereFieldsSet() throws Exception {
      doModifiedDispositionTestImpl(Boolean.TRUE, Boolean.TRUE);
   }

   private void doModifiedDispositionTestImpl(Boolean deliveryFailed, Boolean undeliverableHere) throws Exception {
      sendMessages(getQueueName(), 1);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver1 = session.createReceiver(getQueueName());
      receiver1.flow(1);

      AmqpMessage message = receiver1.receive(5, TimeUnit.SECONDS);
      assertNotNull("did not receive message first time", message);

      Message protonMessage = message.getWrappedMessage();
      assertNotNull(protonMessage);
      assertEquals("Unexpected initial value for AMQP delivery-count", 0, protonMessage.getDeliveryCount());

      message.modified(deliveryFailed, undeliverableHere);

      // Remote must not redispatch to the client if undeliverable here is true
      if (Boolean.TRUE.equals(undeliverableHere)) {
         receiver1.flow(1);
         message = receiver1.receive(1, TimeUnit.SECONDS);
         assertNull("Should not receive message again", message);
      }

      AmqpReceiver receiver2 = session.createReceiver(getQueueName());
      receiver2.flow(1);

      message = receiver2.receive(5, TimeUnit.SECONDS);
      assertNotNull("did not receive message again", message);

      int expectedDeliveryCount = 0;
      if (Boolean.TRUE.equals(deliveryFailed)) {
         expectedDeliveryCount = 1;
      }

      message.accept();

      Message protonMessage2 = message.getWrappedMessage();
      assertNotNull(protonMessage2);
      assertEquals("Unexpected updated value for AMQP delivery-count", expectedDeliveryCount, protonMessage2.getDeliveryCount());

      connection.close();
   }
}
