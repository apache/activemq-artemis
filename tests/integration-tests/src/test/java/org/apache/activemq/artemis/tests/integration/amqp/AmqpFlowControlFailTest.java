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

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;

@RunWith(Enclosed.class)
public class AmqpFlowControlFailTest {

   @RunWith(Parameterized.class)
   public static class AmqpFlowControlFailDispositionTests extends JMSClientTestSupport {

      @Parameterized.Parameter()
      public boolean useModified;

      @Parameterized.Parameter(1)
      public Symbol[] outcomes;

      @Parameterized.Parameter(2)
      public String expectedMessage;


      @Parameterized.Parameters(name = "useModified={0}")
      public static Collection<Object[]> parameters() {
         return Arrays.asList(new Object[][] {
               {true, new Symbol[]{Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL}, "failure at remote"},
               {true, new Symbol[]{Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL}, "[condition = amqp:resource-limit-exceeded]"},
               {false, new Symbol[]{Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL}, "[condition = amqp:resource-limit-exceeded]"},
               {false, new Symbol[]{}, "[condition = amqp:resource-limit-exceeded]"}
         });
      }

      @Override
      protected void configureAddressPolicy(ActiveMQServer server) {
         AmqpFlowControlFailTest.configureAddressPolicy(server);
      }

      @Override
      protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
         params.put("amqpUseModifiedForTransientDeliveryErrors", useModified);
      }


      @Test(timeout = 60000)
      public void testAddressFullDisposition() throws Exception {
         AmqpClient client = createAmqpClient(getBrokerAmqpConnectionURI());
         AmqpConnection connection = addConnection(client.connect());
         try {
            AmqpSession session = connection.createSession();
            AmqpSender sender = session.createSender(getQueueName(), null, null, outcomes);
            boolean rejected = false;
            for (int i = 0; i < 1000; i++) {
               final AmqpMessage message = new AmqpMessage();
               byte[] payload = new byte[10];
               message.setBytes(payload);
               try {
                  sender.send(message);
               } catch (IOException e) {
                  rejected = true;
                  assertTrue(String.format("Unexpected message expected %s to contain %s", e.getMessage(), expectedMessage),
                             e.getMessage().contains(expectedMessage));
               }
            }

            assertTrue("Expected messages to be refused by broker", rejected);
         } finally {
            connection.close();
         }
      }
   }

   public static class AmqpFlowControlFailOrdinaryTests extends JMSClientTestSupport {

      @Override
      protected void configureAddressPolicy(ActiveMQServer server) {
         AmqpFlowControlFailTest.configureAddressPolicy(server);
      }

      @Test(timeout = 60000)
      public void testMesagesNotSent() throws Exception {
         AmqpClient client = createAmqpClient(getBrokerAmqpConnectionURI());
         AmqpConnection connection = addConnection(client.connect());
         int messagesSent = 0;
         try {
            AmqpSession session = connection.createSession();
            AmqpSender sender = session.createSender(getQueueName());
            boolean rejected = false;
            for (int i = 0; i < 1000; i++) {
               final AmqpMessage message = new AmqpMessage();
               byte[] payload = new byte[10];
               message.setBytes(payload);
               try {
                  sender.send(message);
                  messagesSent++;
               } catch (IOException e) {
                  rejected = true;
               }
            }
            assertTrue(rejected);
            rejected = false;
            assertEquals(0, sender.getSender().getCredit());
            AmqpSession session2 = connection.createSession();
            AmqpReceiver receiver = session2.createReceiver(getQueueName());
            receiver.flow(messagesSent);
            for (int i = 0; i < messagesSent; i++) {
               AmqpMessage receive = receiver.receive();
               receive.accept();
            }
            receiver.close();
            session2.close();

            Wait.assertEquals(1000, sender.getSender()::getCredit);
            for (int i = 0; i < 1000; i++) {
               final AmqpMessage message = new AmqpMessage();
               byte[] payload = new byte[100];
               message.setBytes(payload);
               try {
                  sender.send(message);
               } catch (IOException e) {
                  rejected = true;
               }
            }
            assertTrue(rejected);
            assertEquals(0, sender.getSender().getCredit());
         } finally {
            connection.close();
         }
      }
   }

   private static void configureAddressPolicy(final ActiveMQServer server) {
      AddressSettings addressSettings = server.getAddressSettingsRepository().getMatch("#");
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);
      addressSettings.setMaxSizeBytes(1000);
      server.getAddressSettingsRepository().addMatch("#", addressSettings);
   }
}
