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
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;

public class AmqpAnonymousRelayTest extends AmqpClientTestSupport {

   @Override
   protected boolean isAutoCreateQueues() {
      return false;
   }

   @Override
   protected boolean isAutoCreateAddresses() {
      return false;
   }

   @Test(timeout = 60000)
   public void testSendMessageOnAnonymousRelayLinkUsingMessageTo() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();

         AmqpSender sender = session.createAnonymousSender();
         AmqpMessage message = new AmqpMessage();

         message.setAddress(getQueueName());
         message.setMessageId("msg" + 1);
         message.setText("Test-Message");

         sender.send(message);
         sender.close();

         AmqpReceiver receiver = session.createReceiver(getQueueName());
         receiver.flow(1);
         AmqpMessage received = receiver.receive(10, TimeUnit.SECONDS);
         assertNotNull("Should have read message", received);
         assertEquals("msg1", received.getMessageId());
         received.accept();

         receiver.close();
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testSendMessageFailsOnAnonymousRelayLinkWhenNoToValueSet() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();

         AmqpSender sender = session.createAnonymousSender();
         AmqpMessage message = new AmqpMessage();

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

   @Test(timeout = 60000)
   public void testSendMessageFailsOnAnonymousRelayWhenToFieldHasNonExistingAddress() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();

         AmqpSender sender = session.createAnonymousSender();
         AmqpMessage message = new AmqpMessage();

         message.setAddress("exampleQueu-not-in-service");
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
