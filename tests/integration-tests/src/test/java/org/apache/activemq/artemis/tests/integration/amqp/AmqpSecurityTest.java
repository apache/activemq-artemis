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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
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
      AmqpClient client = createAmqpClient(guestUser, guestPass);
      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectOpenedResource(Sender sender) {
            ErrorCondition condition = sender.getRemoteCondition();

            if (condition != null && condition.getCondition() != null) {
               if (!condition.getCondition().equals(AmqpError.UNAUTHORIZED_ACCESS)) {
                  markAsInvalid("Should have been tagged with unauthorized access error");
               }
            } else {
               markAsInvalid("Sender should have been opened with an error");
            }
         }
      });

      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      try {
         try {
            session.createSender(getQueueName());
            fail("Should not be able to consume here.");
         } catch (Exception ex) {
            instanceLog.debug("Caught expected exception");
         }

         connection.getStateInspector().assertValid();
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testSendMessageFailsOnAnonymousRelayWhenNotAuthorizedToSendToAddress() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      AmqpClient client = createAmqpClient(guestUser, guestPass);
      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectDeliveryUpdate(Sender sender, Delivery delivery) {
            DeliveryState state = delivery.getRemoteState();

            if (!delivery.remotelySettled()) {
               markAsInvalid("delivery is not remotely settled");
            }

            if (state instanceof Rejected) {
               Rejected rejected = (Rejected) state;
               if (rejected.getError() == null || rejected.getError().getCondition() == null) {
                  markAsInvalid("Delivery should have been Rejected with an error condition");
               } else {
                  ErrorCondition error = rejected.getError();
                  if (!error.getCondition().equals(AmqpError.UNAUTHORIZED_ACCESS)) {
                     markAsInvalid("Should have been tagged with unauthorized access error");
                  }
               }
            } else {
               markAsInvalid("Delivery should have been Rejected");
            }

            latch.countDown();
         }
      });

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

         assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
         connection.getStateInspector().assertValid();
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testReceiverNotAuthorized() throws Exception {
      AmqpClient client = createAmqpClient(noprivUser, noprivPass);
      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectOpenedResource(Receiver receiver) {
            ErrorCondition condition = receiver.getRemoteCondition();

            if (condition != null && condition.getCondition() != null) {
               if (!condition.getCondition().equals(AmqpError.UNAUTHORIZED_ACCESS)) {
                  markAsInvalid("Should have been tagged with unauthorized access error");
               }
            } else {
               markAsInvalid("Receiver should have been opened with an error");
            }
         }
      });

      AmqpConnection connection = client.connect();

      try {
         AmqpSession session = connection.createSession();

         try {
            session.createReceiver(getQueueName());
            fail("Should not be able to consume here.");
         } catch (Exception ex) {
            instanceLog.debug("Caught expected exception");
         }

         connection.getStateInspector().assertValid();
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testConsumerNotAuthorizedToCreateQueues() throws Exception {
      AmqpClient client = createAmqpClient(noprivUser, noprivPass);
      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectOpenedResource(Sender sender) {
            ErrorCondition condition = sender.getRemoteCondition();

            if (condition != null && condition.getCondition() != null) {
               if (!condition.getCondition().equals(AmqpError.UNAUTHORIZED_ACCESS)) {
                  markAsInvalid("Should have been tagged with unauthorized access error");
               }
            } else {
               markAsInvalid("Sender should have been opened with an error");
            }
         }
      });

      AmqpConnection connection = client.connect();

      try {
         AmqpSession session = connection.createSession();

         try {
            session.createReceiver(getQueueName(getPrecreatedQueueSize() + 1));
            fail("Should not be able to consume here.");
         } catch (Exception ex) {
            instanceLog.debug("Caught expected exception");
         }

         connection.getStateInspector().assertValid();
      } finally {
         connection.close();
      }
   }
}
