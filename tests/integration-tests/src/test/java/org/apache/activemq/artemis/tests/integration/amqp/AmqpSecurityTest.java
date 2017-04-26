/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.junit.Test;

public class AmqpSecurityTest extends AmqpClientTestSupport {

   @Override
   protected boolean isSecurityEnabled() {
      return true;
   }

   @Test(timeout = 60000)
   public void testSaslAuthWithInvalidCredentials() throws Exception {
      AmqpConnection connection = null;
      AmqpClient client = createAmqpClient(fullUser, guestUser);

      try {
         connection = client.connect();
         fail("Should not authenticate when invalid credentials provided");
      } catch (Exception ex) {
         // Expected
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 60000)
   public void testSaslAuthWithAuthzid() throws Exception {
      AmqpConnection connection = null;
      AmqpClient client = createAmqpClient(guestUser, guestPass);
      client.setAuthzid(guestUser);

      try {
         connection = client.connect();
      } catch (Exception ex) {
         fail("Should authenticate even with authzid set");
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 60000)
   public void testSaslAuthWithoutAuthzid() throws Exception {
      AmqpConnection connection = null;
      AmqpClient client = createAmqpClient(guestUser, guestPass);

      try {
         connection = client.connect();
      } catch (Exception ex) {
         fail("Should authenticate even with authzid set");
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 60000)
   public void testSendAndRejected() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      AmqpClient client = createAmqpClient(guestUser, guestPass);
      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectDeliveryUpdate(Sender sender, Delivery delivery) {
            if (!delivery.remotelySettled()) {
               markAsInvalid("delivery is not remotely settled");
            }

            latch.countDown();
         }
      });

      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());
      AmqpMessage message = new AmqpMessage();

      message.setMessageId("msg" + 1);
      message.setMessageAnnotation("serialNo", 1);
      message.setText("Test-Message");

      try {
         sender.send(message);
      } catch (IOException e) {
      }

      assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
      connection.getStateInspector().assertValid();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testSendMessageFailsOnAnonymousRelayWhenNotAuthorizedToSendToAddress() throws Exception {
      AmqpClient client = createAmqpClient(guestUser, guestPass);
      AmqpConnection connection = client.connect();

      try {
         AmqpSession session = connection.createSession();

         AmqpSender sender = session.createAnonymousSender();
         AmqpMessage message = new AmqpMessage();

         message.setAddress(getQueueName());
         message.setMessageId("msg" + 1);
         message.setText("Test-Message");

         try {
            sender.send(message);
            fail("Should not be able to send, message should be rejected");
         } catch (Exception ex) {
            ex.printStackTrace();
         } finally {
            sender.close();
         }
      } finally {
         connection.close();
      }
   }
}
