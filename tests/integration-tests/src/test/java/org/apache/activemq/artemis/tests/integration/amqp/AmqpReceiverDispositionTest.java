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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test various behaviors of AMQP receivers with the broker.
 */

public class AmqpReceiverDispositionTest {

   @Nested
   public class AmqpReceiverDispositionOrdinaryTests extends AmqpClientTestSupport {
      @Test
      @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
      public void testReleasedDisposition() throws Exception {
         sendMessages(getQueueName(), 1);

         AmqpClient client = createAmqpClient();
         AmqpConnection connection = addConnection(client.connect());
         AmqpSession session = connection.createSession();

         AmqpReceiver receiver1 = session.createReceiver(getQueueName());
         receiver1.flow(1);

         AmqpMessage message = receiver1.receive(5, TimeUnit.SECONDS);

         AmqpReceiver receiver2 = session.createReceiver(getQueueName());

         assertNotNull(message, "did not receive message first time");
         assertEquals("MessageID:0", message.getMessageId());

         Message protonMessage = message.getWrappedMessage();
         assertNotNull(protonMessage);
         assertEquals(0, protonMessage.getDeliveryCount(), "Unexpected initial value for AMQP delivery-count");

         receiver2.flow(1);
         message.release();

         // Read the message again and validate its state
         message = receiver2.receive(10, TimeUnit.SECONDS);
         assertNotNull(message, "did not receive message again");
         assertEquals("MessageID:0", message.getMessageId());

         message.accept();

         protonMessage = message.getWrappedMessage();
         assertNotNull(protonMessage);
         assertEquals(0, protonMessage.getDeliveryCount(), "Unexpected updated value for AMQP delivery-count");

         connection.close();
      }

      @Test
      @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
      public void testRejectedDisposition() throws Exception {
         sendMessages(getQueueName(), 1);

         AmqpClient client = createAmqpClient();
         AmqpConnection connection = addConnection(client.connect());
         AmqpSession session = connection.createSession();

         AmqpReceiver receiver1 = session.createReceiver(getQueueName());
         receiver1.flow(1);

         AmqpMessage message = receiver1.receive(5, TimeUnit.SECONDS);
         assertNotNull(message, "did not receive message first time");
         assertEquals("MessageID:0", message.getMessageId());

         Message protonMessage = message.getWrappedMessage();
         assertNotNull(protonMessage);
         assertEquals(0, protonMessage.getDeliveryCount(), "Unexpected initial value for AMQP delivery-count");

         message.reject();

         // Reject is a terminal outcome and should not be redelivered to the rejecting receiver
         // or any other as it should move to the archived state.
         receiver1.flow(1);
         message = receiver1.receiveNoWait();
         assertNull(message, "Should not receive message again");

         // Attempt to Read the message again with another receiver to validate it is archived.
         AmqpReceiver receiver2 = session.createReceiver(getQueueName());
         receiver2.flow(1);
         assertNull(receiver2.receiveNoWait());

         connection.close();
      }

      @Test
      @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
      public void testModifiedDispositionWithDeliveryFailedWithoutUndeliverableHereFieldsSet() throws Exception {
         doModifiedDispositionTestImpl(Boolean.TRUE, null);
      }

      @Test
      @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
      public void testModifiedDispositionWithoutDeliveryFailedWithoutUndeliverableHereFieldsSet() throws Exception {
         doModifiedDispositionTestImpl(null, null);
      }

      @Test
      @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
      public void testModifiedDispositionWithoutDeliveryFailedWithUndeliverableHereFieldsSet() throws Exception {
         doModifiedDispositionTestImpl(null, Boolean.TRUE);
      }

      @Test
      @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
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
         assertNotNull(message, "did not receive message first time");

         Message protonMessage = message.getWrappedMessage();
         assertNotNull(protonMessage);
         assertEquals(0, protonMessage.getDeliveryCount(), "Unexpected initial value for AMQP delivery-count");

         message.modified(deliveryFailed, undeliverableHere);

         // Remote must not redispatch to the client if undeliverable here is true
         if (Boolean.TRUE.equals(undeliverableHere)) {
            receiver1.flow(1);
            message = receiver1.receive(1, TimeUnit.SECONDS);
            assertNull(message, "Should not receive message again");
         }

         AmqpReceiver receiver2 = session.createReceiver(getQueueName());
         receiver2.flow(1);

         message = receiver2.receive(5, TimeUnit.SECONDS);
         assertNotNull(message, "did not receive message again");

         int expectedDeliveryCount = 0;
         if (Boolean.TRUE.equals(deliveryFailed)) {
            expectedDeliveryCount = 1;
         }

         message.accept();

         Message protonMessage2 = message.getWrappedMessage();
         assertNotNull(protonMessage2);
         assertEquals(expectedDeliveryCount,
                      protonMessage2.getDeliveryCount(),
                      "Unexpected updated value for AMQP delivery-count");

         connection.close();
      }
   }

   @Nested
   public class AmqpReceiverDispositionRejectAsUnmodifiedModeTests extends AmqpClientTestSupport {

      @Override
      protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
         params.put("amqpTreatRejectAsUnmodifiedDeliveryFailed", true);
      }

      @Test
      @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
      public void testRejectedDisposition() throws Exception {
         sendMessages(getQueueName(), 1);

         AmqpClient client = createAmqpClient();
         AmqpConnection connection = addConnection(client.connect());
         AmqpSession session = connection.createSession();

         AmqpReceiver receiver1 = session.createReceiver(getQueueName());
         receiver1.flow(1);

         AmqpMessage message = receiver1.receive(5, TimeUnit.SECONDS);
         assertNotNull(message, "did not receive message first time");
         assertEquals("MessageID:0", message.getMessageId());

         Message protonMessage = message.getWrappedMessage();
         assertNotNull(protonMessage);
         assertEquals(0, protonMessage.getDeliveryCount(), "Unexpected initial value for AMQP delivery-count");

         // Owing to the config, the reject should be treat as if it were a
         // Unmodified delivery-failed=true
         message.reject();
         receiver1.flow(1);

         message = receiver1.receive(5, TimeUnit.SECONDS);
         assertNotNull(message, "did not receive message after reject");
         assertEquals("MessageID:0", message.getMessageId());

         protonMessage = message.getWrappedMessage();
         assertNotNull(protonMessage);
         assertEquals(1, protonMessage.getDeliveryCount(), "Unexpected value for AMQP delivery-count after redelivery");

         connection.close();
      }
   }
}
