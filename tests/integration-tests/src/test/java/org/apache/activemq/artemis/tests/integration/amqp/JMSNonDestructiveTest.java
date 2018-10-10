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


import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.Test;

public class JMSNonDestructiveTest extends JMSClientTestSupport {

   private static final String NON_DESTRUCTIVE_QUEUE_NAME = "NON_DESTRUCTIVE_QUEUE";
   private static final String NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME = "NON_DESTRUCTIVE_EXPIRY_QUEUE";

   private ConnectionSupplier AMQPConnection = () -> createConnection();
   private ConnectionSupplier CoreConnection = () -> createCoreConnection();

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      server.getConfiguration().setMessageExpiryScanPeriod(100);
      server.getAddressSettingsRepository().addMatch(NON_DESTRUCTIVE_QUEUE_NAME, new AddressSettings().setDefaultNonDestructive(true));
      server.getAddressSettingsRepository().addMatch(NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME, new AddressSettings().setDefaultNonDestructive(true).setExpiryDelay(100L));
   }
   @Override
   protected void createAddressAndQueues(ActiveMQServer server) throws Exception {
      super.createAddressAndQueues(server);

      //Add Non Destructive Queue
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(NON_DESTRUCTIVE_QUEUE_NAME), RoutingType.ANYCAST));
      server.createQueue(SimpleString.toSimpleString(NON_DESTRUCTIVE_QUEUE_NAME), RoutingType.ANYCAST, SimpleString.toSimpleString(NON_DESTRUCTIVE_QUEUE_NAME), null, true, false, -1, false, true);

      //Add Non Destructive Expiry Queue
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME), RoutingType.ANYCAST));
      server.createQueue(SimpleString.toSimpleString(NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME), RoutingType.ANYCAST, SimpleString.toSimpleString(NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME), null, true, false, -1, false, true);

   }


   @Test
   public void testNonDestructiveAMQPProducerAMQPConsumer() throws Exception {
      testNonDestructive(AMQPConnection, AMQPConnection);
   }

   @Test
   public void testNonDestructiveCoreProducerCoreConsumer() throws Exception {
      testNonDestructive(CoreConnection, CoreConnection);
   }

   @Test
   public void testNonDestructiveCoreProducerAMQPConsumer() throws Exception {
      testNonDestructive(CoreConnection, AMQPConnection);
   }

   @Test
   public void testNonDestructiveAMQPProducerCoreConsumer() throws Exception {
      testNonDestructive(AMQPConnection, CoreConnection);
   }

   private interface ConnectionSupplier {
      Connection createConnection() throws JMSException;
   }

   public void testNonDestructive(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      testNonDestructiveSingle(producerConnectionSupplier, consumerConnectionSupplier);
      testNonDestructiveDualConsumer(producerConnectionSupplier, consumerConnectionSupplier);
      testNonDestructiveExpiry(producerConnectionSupplier, consumerConnectionSupplier);
      testNonDestructiveMulitpleMessages(producerConnectionSupplier, consumerConnectionSupplier);
   }


   public void testNonDestructiveSingle(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      sendMessage(producerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME);

      QueueBinding queueBinding = (QueueBinding) server.getPostOffice().getBinding(SimpleString.toSimpleString(NON_DESTRUCTIVE_QUEUE_NAME));
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());


      //Consume Once
      receive(consumerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME);
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      //Consume Again as should be non-destructive
      receive(consumerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME);
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      QueueControl control = (QueueControl) server.getManagementService().getResource(ResourceNames.QUEUE + NON_DESTRUCTIVE_QUEUE_NAME);
      control.removeAllMessages();

      assertEquals("Message count after clearing queue via queue control should be 0", 0, queueBinding.getQueue().getMessageCount());
   }

   public void testNonDestructiveDualConsumer(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      sendMessage(producerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME);

      QueueBinding queueBinding = (QueueBinding) server.getPostOffice().getBinding(SimpleString.toSimpleString(NON_DESTRUCTIVE_QUEUE_NAME));
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());


      //Consume Once
      receiveDualConsumer(consumerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME);
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      //Consume Again as should be non-destructive
      receiveDualConsumer(consumerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME);
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      QueueControl control = (QueueControl) server.getManagementService().getResource(ResourceNames.QUEUE + NON_DESTRUCTIVE_QUEUE_NAME);
      control.removeAllMessages();

      assertEquals("Message count after clearing queue via queue control should be 0", 0, queueBinding.getQueue().getMessageCount());
   }

   public void testNonDestructiveExpiry(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      sendMessage(producerConnectionSupplier, NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME);

      QueueBinding queueBinding = (QueueBinding) server.getPostOffice().getBinding(SimpleString.toSimpleString(NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME));
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());


      //Consume Once
      receive(consumerConnectionSupplier, NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME);
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      Thread.sleep(500);

      //Consume Again this time we expect the message to be expired, so nothing delivered
      receiveNull(consumerConnectionSupplier, NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME);
      assertEquals("Ensure Message count", 0, queueBinding.getQueue().getMessageCount());

      QueueControl control = (QueueControl) server.getManagementService().getResource(ResourceNames.QUEUE + NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME);
      control.removeAllMessages();

      assertEquals("Message count after clearing queue via queue control should be 0", 0, queueBinding.getQueue().getMessageCount());
   }

   public void testNonDestructiveMulitpleMessages(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      sendMessage(producerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME, 0);
      sendMessage(producerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME, 1);
      sendMessage(producerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME, 2);

      QueueBinding queueBinding = (QueueBinding) server.getPostOffice().getBinding(SimpleString.toSimpleString(NON_DESTRUCTIVE_QUEUE_NAME));
      assertEquals("Ensure Message count", 3, queueBinding.getQueue().getMessageCount());


      //Consume Once
      receive(consumerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME, 3);

      //Consume Again as should be non-destructive
      receive(consumerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME, 3);

      QueueControl control = (QueueControl) server.getManagementService().getResource(ResourceNames.QUEUE + NON_DESTRUCTIVE_QUEUE_NAME);
      control.removeAllMessages();

      assertEquals("Message count after clearing queue via queue control should be 0", 0, queueBinding.getQueue().getMessageCount());
   }

   public void testNonDestructiveMulitpleMessagesDualConsumer(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      sendMessage(producerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME, 0);
      sendMessage(producerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME, 1);
      sendMessage(producerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME, 2);

      QueueBinding queueBinding = (QueueBinding) server.getPostOffice().getBinding(SimpleString.toSimpleString(NON_DESTRUCTIVE_QUEUE_NAME));
      assertEquals("Ensure Message count", 3, queueBinding.getQueue().getMessageCount());


      //Consume Once
      receiveDualConsumer(consumerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME, 3);

      //Consume Again as should be non-destructive
      receiveDualConsumer(consumerConnectionSupplier, NON_DESTRUCTIVE_QUEUE_NAME, 3);

      QueueControl control = (QueueControl) server.getManagementService().getResource(ResourceNames.QUEUE + NON_DESTRUCTIVE_QUEUE_NAME);
      control.removeAllMessages();

      assertEquals("Message count after clearing queue via queue control should be 0", 0, queueBinding.getQueue().getMessageCount());
   }

   private void receive(ConnectionSupplier consumerConnectionSupplier, String queueName, int i) throws JMSException {
      try (Connection consumerConnection = consumerConnectionSupplier.createConnection()) {

         Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue consumerQueue = consumerSession.createQueue(queueName);
         MessageConsumer consumer = consumerSession.createConsumer(consumerQueue);
         for (int j = 0; j < i; j++) {
            TextMessage msg = (TextMessage) consumer.receive(200);
            assertNotNull(msg);
            assertEquals(Integer.toString(j), msg.getText());
         }
         TextMessage msg = (TextMessage) consumer.receive(200);
         assertNull(msg);
         consumer.close();
      }
   }

   private void receive(ConnectionSupplier consumerConnectionSupplier, String queueName) throws JMSException {
      try (Connection consumerConnection = consumerConnectionSupplier.createConnection()) {

         Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue consumerQueue = consumerSession.createQueue(queueName);
         MessageConsumer consumer = consumerSession.createConsumer(consumerQueue);
         TextMessage msg = (TextMessage) consumer.receive(200);
         assertNotNull(msg);
         consumer.close();
      }
   }

   private void receiveNull(ConnectionSupplier consumerConnectionSupplier, String queueName) throws JMSException {
      try (Connection consumerConnection = consumerConnectionSupplier.createConnection()) {

         Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue consumerQueue = consumerSession.createQueue(queueName);
         MessageConsumer consumer = consumerSession.createConsumer(consumerQueue);
         TextMessage msg = (TextMessage) consumer.receive(200);
         assertNull(msg);
         consumer.close();
      }
   }

   private void receiveDualConsumer(ConnectionSupplier consumerConnectionSupplier, String queueName) throws JMSException {
      try (Connection consumerConnection = consumerConnectionSupplier.createConnection();
           Connection consumerConnection2 = consumerConnectionSupplier.createConnection()) {

         MessageConsumer consumer = createConsumer(consumerConnection, queueName);
         MessageConsumer consumer2 = createConsumer(consumerConnection2, queueName);

         TextMessage msg = (TextMessage) consumer.receive(200);
         TextMessage msg2 = (TextMessage) consumer2.receive(200);

         assertNotNull(msg);
         assertNotNull(msg2);
         consumer.close();
         consumer2.close();
      }
   }

   private void receiveDualConsumer(ConnectionSupplier consumerConnectionSupplier, String queueName, int i) throws JMSException {
      try (Connection consumerConnection = consumerConnectionSupplier.createConnection();
           Connection consumerConnection2 = consumerConnectionSupplier.createConnection()) {

         MessageConsumer consumer = createConsumer(consumerConnection, queueName);
         MessageConsumer consumer2 = createConsumer(consumerConnection2, queueName);


         for (int j = 0; j < i; j++) {
            TextMessage msg = (TextMessage) consumer.receive(200);
            TextMessage msg2 = (TextMessage) consumer2.receive(200);
            assertNotNull(msg);
            assertNotNull(msg2);
            assertEquals(Integer.toString(j), msg.getText());
            assertEquals(Integer.toString(j), msg2.getText());
         }
         TextMessage msg = (TextMessage) consumer.receive(200);
         assertNull(msg);
         TextMessage msg2 = (TextMessage) consumer2.receive(200);
         assertNull(msg2);
         consumer.close();
         consumer2.close();
      }
   }

   private MessageConsumer createConsumer(Connection connection, String queueName) throws JMSException {
      connection.start();
      Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue consumerQueue = consumerSession.createQueue(queueName);
      return consumerSession.createConsumer(consumerQueue);
   }

   private void sendMessage(ConnectionSupplier producerConnectionSupplier, String queueName) throws JMSException {
      sendMessage(producerConnectionSupplier, queueName, 0);
   }

   private void sendMessage(ConnectionSupplier producerConnectionSupplier, String queueName, int i) throws JMSException {
      try (Connection producerConnection = producerConnectionSupplier.createConnection()) {
         Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue1 = producerSession.createQueue(queueName);
         MessageProducer p = producerSession.createProducer(null);

         TextMessage message1 = producerSession.createTextMessage();
         message1.setText(Integer.toString(i));
         p.send(queue1, message1);
      }
   }
}