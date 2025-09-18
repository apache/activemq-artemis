/**
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

package org.apache.activemq.artemis.tests.integration.openwire;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for escaped address delivery in OpenWire protocol.
 * <p>
 * This test suite verifies that OpenWire destinations with special characters
 * (particularly backslashes) are properly handled during message routing and delivery.
 * It ensures that address escaping works correctly for various destination types
 * including regular queues, FQQN addresses, temporary queues, and literal scheme names.
 */
public class EscapedAddressDeliveryTest extends BasicOpenWireTest {

   private static final String ESCAPED_QUEUE_LOGICAL_NAME = "foo\\foo";
   private static final String ESCAPED_QUEUE_CORE_NAME = "foo\\\\foo";
   private static final String BROKER_CONNECTION_URL = "tcp://localhost:61616";
   private static final int MESSAGE_RECEIVE_TIMEOUT_MS = 3000;

   @Test
   public void shouldDeliverMessageToEscapedAddress() throws Exception {
      final Destination escapedQueueDestination = setupEscapedQueue();
      sendAndReceiveMessage(escapedQueueDestination, "hello");
   }

   @Test
   public void shouldTargetCorrectEscapedCoreAddressWhenProducing() throws Exception {
      final Destination escapedQueueDestination = setupEscapedQueue();

      ActiveMQConnectionFactory connectionFactory = createTestConnectionFactory();
      try (Connection jmsConnection = connectionFactory.createConnection();
           Session jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
           MessageProducer messageProducer = jmsSession.createProducer(escapedQueueDestination)) {

         jmsConnection.start();
         messageProducer.send(jmsSession.createTextMessage("ping"));
      }

      assertTrue(hasBindingForAddress(ESCAPED_QUEUE_CORE_NAME), "Expected a binding under core address: " + ESCAPED_QUEUE_CORE_NAME);
   }

   @Test
   public void shouldPreserveFQQNAddressWhenSendingAndReceiving() throws Exception {
      final String fqqnAddress = "addrA";
      final String fqqnQueueName = "qA";
      final String fullyQualifiedQueueName = fqqnAddress + "::" + fqqnQueueName;

      server.createQueue(QueueConfiguration.of(SimpleString.of(fqqnQueueName))
                                           .setAddress(SimpleString.of(fqqnAddress))
                                           .setRoutingType(RoutingType.ANYCAST)
                                           .setDurable(false));

      Destination fqqnDestination = new ActiveMQQueue(fullyQualifiedQueueName);
      sendAndReceiveMessage(fqqnDestination, "fqqn-ok");

      assertTrue(hasBindingForQueue(fqqnQueueName), "Expected only the pre-created FQQN queue/binding.");
   }

   @Test
   public void shouldDeliverMessageToTemporaryQueue() throws Exception {
      ActiveMQConnectionFactory connectionFactory = createTestConnectionFactory();
      try (Connection jmsConnection = connectionFactory.createConnection();
           Session jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

         jmsConnection.start();
         TemporaryQueue temporaryQueue = jmsSession.createTemporaryQueue();

         try (MessageProducer tempQueueProducer = jmsSession.createProducer(temporaryQueue);
              MessageConsumer tempQueueConsumer = jmsSession.createConsumer(temporaryQueue)) {

            tempQueueProducer.send(jmsSession.createTextMessage("temp-ok"));
            Message receivedTempMessage = tempQueueConsumer.receive(MESSAGE_RECEIVE_TIMEOUT_MS);

            assertNotNull(receivedTempMessage);
            assertEquals("temp-ok", ((TextMessage) receivedTempMessage).getText());
         } finally {
            temporaryQueue.delete();
         }
      }
   }

   @Test
   public void shouldReceiveAdvisoryOnTemporaryQueueRemoval() throws Exception {
      ActiveMQConnectionFactory connectionFactory = createTestConnectionFactory();
      try (Connection jmsConnection = connectionFactory.createConnection();
           Session jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

         jmsConnection.start();

         TemporaryQueue temporaryQueue = jmsSession.createTemporaryQueue();
         Destination tempQueueAdvisoryTopic = org.apache.activemq.advisory.AdvisorySupport.getDestinationAdvisoryTopic(temporaryQueue);

         try (MessageConsumer advisoryMessageConsumer = jmsSession.createConsumer(tempQueueAdvisoryTopic)) {
            temporaryQueue.delete();

            Message tempQueueRemovalAdvisory = advisoryMessageConsumer.receive(MESSAGE_RECEIVE_TIMEOUT_MS);
            assertNotNull(tempQueueRemovalAdvisory, "Expected advisory on temp queue removal");
         }
      }
   }

   @Test
   public void shouldPreserveLiteralSchemeName() throws Exception {
      final String literalSchemeName = "queue://literal";

      server.createQueue(QueueConfiguration.of(literalSchemeName)
                                           .setAddress(literalSchemeName)
                                           .setRoutingType(RoutingType.ANYCAST)
                                           .setDurable(false));

      Destination literalSchemeDestination = new ActiveMQQueue("queue://" + literalSchemeName);
      sendAndReceiveMessage(literalSchemeDestination, "literal-ok");

      assertTrue(hasBindingForAddress(literalSchemeName), "Expected binding under literal core address: " + literalSchemeName);
   }

   private Destination setupEscapedQueue() throws Exception {
      final Destination escapedQueueDestination = new ActiveMQQueue("queue://" + ESCAPED_QUEUE_LOGICAL_NAME);
      server.createQueue(QueueConfiguration.of(ESCAPED_QUEUE_CORE_NAME)
                                           .setAddress(ESCAPED_QUEUE_CORE_NAME)
                                           .setRoutingType(RoutingType.ANYCAST)
                                           .setDurable(false));
      return escapedQueueDestination;
   }

   private ActiveMQConnectionFactory createTestConnectionFactory() {
      return new ActiveMQConnectionFactory(BROKER_CONNECTION_URL);
   }

   private void sendAndReceiveMessage(Destination testDestination, String expectedMessageText) throws Exception {
      ActiveMQConnectionFactory connectionFactory = createTestConnectionFactory();
      try (Connection jmsConnection = connectionFactory.createConnection();
           Session jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
           MessageConsumer messageConsumer = jmsSession.createConsumer(testDestination);
           MessageProducer messageProducer = jmsSession.createProducer(testDestination)) {

         jmsConnection.start();
         messageProducer.send(jmsSession.createTextMessage(expectedMessageText));

         Message receivedMessage = messageConsumer.receive(MESSAGE_RECEIVE_TIMEOUT_MS);
         assertNotNull(receivedMessage, "Expected a message but got null");
         assertEquals(expectedMessageText, ((TextMessage) receivedMessage).getText());
      }
   }

   private boolean hasBindingForAddress(String expectedCoreAddress) throws Exception {
      PostOffice brokerPostOffice = server.getPostOffice();
      return Wait.waitFor(() -> brokerPostOffice.getAllBindings()
                                                .filter(binding -> binding instanceof LocalQueueBinding)
                                                .map(Binding::getAddress)
                                                .anyMatch(bindingAddress -> bindingAddress != null && expectedCoreAddress.equals(bindingAddress.toString())), MESSAGE_RECEIVE_TIMEOUT_MS);
   }

   private boolean hasBindingForQueue(String expectedQueueName) throws Exception {
      PostOffice brokerPostOffice = server.getPostOffice();
      return Wait.waitFor(() -> brokerPostOffice.getAllBindings()
                                                .filter(binding -> binding instanceof LocalQueueBinding)
                                                .map(Binding::getUniqueName)
                                                .anyMatch(queueName -> expectedQueueName.equals(queueName.toString())), MESSAGE_RECEIVE_TIMEOUT_MS);
   }
}