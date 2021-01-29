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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.LastValueQueue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.RetryRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class JMSNonDestructiveTest extends JMSClientTestSupport {

   @Rule
   public RetryRule retryRule = new RetryRule(2);

   private static final String NON_DESTRUCTIVE_QUEUE_NAME = "NON_DESTRUCTIVE_QUEUE";
   private static final String NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME = "NON_DESTRUCTIVE_EXPIRY_QUEUE";
   private static final String NON_DESTRUCTIVE_LVQ_QUEUE_NAME = "NON_DESTRUCTIVE_LVQ_QUEUE";
   private static final String NON_DESTRUCTIVE_TOMBSTONE_LVQ_QUEUE_NAME = "NON_DESTRUCTIVE_LVQ_TOMBSTONE_QUEUE";

   private ConnectionSupplier AMQPConnection = () -> createConnection();
   private ConnectionSupplier CoreConnection = () -> createCoreConnection();

   protected final boolean persistenceEnabled;
   protected final long scanPeriod;

   public JMSNonDestructiveTest(boolean persistenceEnabled, long scanPeriod) {
      this.persistenceEnabled = persistenceEnabled;
      this.scanPeriod = scanPeriod;
   }

   @Parameterized.Parameters(name = "persistenceEnabled={0}, scanPeriod={1}")
   public static Collection<Object[]> data() {
      Object[][] params = new Object[][]{{false, 100}, {true, 100}, {true, -1}};
      return Arrays.asList(params);
   }

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      server.getConfiguration().setPersistenceEnabled(persistenceEnabled);
      server.getConfiguration().setMessageExpiryScanPeriod(scanPeriod);
      server.getAddressSettingsRepository().addMatch(NON_DESTRUCTIVE_QUEUE_NAME, new AddressSettings().setDefaultNonDestructive(true));
      server.getAddressSettingsRepository().addMatch(NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME, new AddressSettings().setDefaultNonDestructive(true).setExpiryDelay(100L));
      server.getAddressSettingsRepository().addMatch(NON_DESTRUCTIVE_LVQ_QUEUE_NAME, new AddressSettings().setDefaultLastValueQueue(true).setDefaultNonDestructive(true));
      server.getAddressSettingsRepository().addMatch(NON_DESTRUCTIVE_TOMBSTONE_LVQ_QUEUE_NAME, new AddressSettings().setDefaultLastValueQueue(true).setDefaultNonDestructive(true));
   }
   @Override
   protected void createAddressAndQueues(ActiveMQServer server) throws Exception {
      super.createAddressAndQueues(server);

      //Add Non Destructive Queue
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(NON_DESTRUCTIVE_QUEUE_NAME), RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(NON_DESTRUCTIVE_QUEUE_NAME).setRoutingType(RoutingType.ANYCAST));

      //Add Non Destructive Expiry Queue
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME), RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(NON_DESTRUCTIVE_EXPIRY_QUEUE_NAME).setRoutingType(RoutingType.ANYCAST));

      //Add Non Destructive Last Value Queue
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(NON_DESTRUCTIVE_LVQ_QUEUE_NAME), RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(NON_DESTRUCTIVE_LVQ_QUEUE_NAME).setRoutingType(RoutingType.ANYCAST));

      //Add  Non Destructive Last Value Queue for Tombstone Test
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(NON_DESTRUCTIVE_TOMBSTONE_LVQ_QUEUE_NAME), RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(NON_DESTRUCTIVE_TOMBSTONE_LVQ_QUEUE_NAME).setRoutingType(RoutingType.ANYCAST));

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

   @Test
   public void testNonDestructiveLVQWithConsumerFirstCore() throws Exception {
      testNonDestructiveLVQWithConsumerFirst(CoreConnection);
   }

   @Test
   public void testNonDestructiveLVQWithConsumerFirstAMQP() throws Exception {
      testNonDestructiveLVQWithConsumerFirst(AMQPConnection);
   }

   public void testNonDestructive(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      testNonDestructiveSingle(producerConnectionSupplier, consumerConnectionSupplier);
      testNonDestructiveDualConsumer(producerConnectionSupplier, consumerConnectionSupplier);
      testNonDestructiveExpiry(producerConnectionSupplier, consumerConnectionSupplier);
      testNonDestructiveMulitpleMessages(producerConnectionSupplier, consumerConnectionSupplier);
      testNonDestructiveMulitpleMessagesDualConsumer(producerConnectionSupplier, consumerConnectionSupplier);
      testNonDestructiveLVQ(producerConnectionSupplier, consumerConnectionSupplier);
      testNonDestructiveLVQTombstone(producerConnectionSupplier, consumerConnectionSupplier);

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
      Wait.assertEquals(1, queueBinding.getQueue()::getMessageCount);

      // Wait for expiration
      Wait.waitFor(() -> queueBinding.getQueue().getMessageCount() == 0, 200); // notice the small timeout here is intended,
                  // as it will not suceed if we disable scan as we expect the client to expire destinations

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

   public void testNonDestructiveLVQ(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      sendLVQ(producerConnectionSupplier, NON_DESTRUCTIVE_LVQ_QUEUE_NAME, Message.HDR_LAST_VALUE_NAME.toString());

      QueueBinding queueBinding = (QueueBinding) server.getPostOffice().getBinding(SimpleString.toSimpleString(NON_DESTRUCTIVE_LVQ_QUEUE_NAME));
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      //Simulate a small pause, else both messages could be consumed if consumer is fast enough
      Thread.sleep(10);

      //Consume Once
      receiveLVQ(consumerConnectionSupplier, NON_DESTRUCTIVE_LVQ_QUEUE_NAME, Message.HDR_LAST_VALUE_NAME.toString());
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      //Consume Again as should be non-destructive
      receiveLVQ(consumerConnectionSupplier, NON_DESTRUCTIVE_LVQ_QUEUE_NAME, Message.HDR_LAST_VALUE_NAME.toString());
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      //Send again
      sendLVQ(producerConnectionSupplier, NON_DESTRUCTIVE_LVQ_QUEUE_NAME, Message.HDR_LAST_VALUE_NAME.toString());

      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      //Simulate a small pause, else both messages could be consumed if consumer is fast enough
      Thread.sleep(10);

      //Consume Once More
      receiveLVQ(consumerConnectionSupplier, NON_DESTRUCTIVE_LVQ_QUEUE_NAME, Message.HDR_LAST_VALUE_NAME.toString());

      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      QueueControl control = (QueueControl) server.getManagementService().getResource(ResourceNames.QUEUE + NON_DESTRUCTIVE_LVQ_QUEUE_NAME);
      control.removeAllMessages();

      assertEquals("Message count after clearing queue via queue control should be 0", 0, queueBinding.getQueue().getMessageCount());
   }

   public void testNonDestructiveLVQWithConsumerFirst(ConnectionSupplier connectionSupplier) throws Exception {
      ExecutorService executor = Executors.newFixedThreadPool(1);
      CountDownLatch consumerSetup = new CountDownLatch(1);
      CountDownLatch consumerComplete = new CountDownLatch(1);

      /*
       * Create the consumer before any messages are sent and keep it there so that the first message which arrives
       * on the queue triggers direct delivery. Without the fix in this commit this essentially "poisons" the queue
       * so that consumers can't get messages later.
       */
      executor.submit(() -> {
         try (Connection connection = connectionSupplier.createConnection();
              Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
              MessageConsumer messageConsumer = session.createConsumer(session.createQueue(NON_DESTRUCTIVE_LVQ_QUEUE_NAME))) {
            connection.start();
            consumerSetup.countDown();
            BytesMessage messageReceived = (BytesMessage) messageConsumer.receive(5000);
            assertNotNull(messageReceived);
            consumerComplete.countDown();
         } catch (Exception e) {
            fail(e.getMessage());
         }

         consumerComplete.countDown();
      });

      // wait for the consumer thread to start and get everything setup
      consumerSetup.await(5, TimeUnit.SECONDS);

      try (Connection connection = connectionSupplier.createConnection();
           Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
         MessageProducer producer = session.createProducer(session.createQueue(NON_DESTRUCTIVE_LVQ_QUEUE_NAME));
         BytesMessage message = session.createBytesMessage();
         message.writeUTF("mills " + System.currentTimeMillis());
         message.setStringProperty("_AMQ_LVQ_NAME", "STOCK_NAME");
         producer.send(message);

         // wait for the consumer to close then send another message
         consumerComplete.await(5, TimeUnit.SECONDS);

         message = session.createBytesMessage();
         message.writeUTF("mills " + System.currentTimeMillis());
         message.setStringProperty("_AMQ_LVQ_NAME", "STOCK_NAME");
         producer.send(message);
      }

      try (Connection connection = connectionSupplier.createConnection();
           Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
           MessageConsumer messageConsumer = session.createConsumer(session.createQueue(NON_DESTRUCTIVE_LVQ_QUEUE_NAME))) {
         connection.start();
         BytesMessage messageReceived = (BytesMessage) messageConsumer.receive(5000);
         assertNotNull(messageReceived);
      }

      executor.shutdownNow();
   }

   public void testNonDestructiveLVQTombstone(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      int tombstoneTimeToLive = 500;

      QueueBinding queueBinding = (QueueBinding) server.getPostOffice().getBinding(SimpleString.toSimpleString(NON_DESTRUCTIVE_TOMBSTONE_LVQ_QUEUE_NAME));
      LastValueQueue lastValueQueue = (LastValueQueue)queueBinding.getQueue();
      //Send again
      sendLVQ(producerConnectionSupplier, NON_DESTRUCTIVE_TOMBSTONE_LVQ_QUEUE_NAME, Message.HDR_LAST_VALUE_NAME.toString());

      assertEquals("Ensure Message count", 1, lastValueQueue.getMessageCount());

      //Simulate a small pause, else both messages could be consumed if consumer is fast enough
      Thread.sleep(10);

      //Consume Once More
      receiveLVQ(consumerConnectionSupplier, NON_DESTRUCTIVE_TOMBSTONE_LVQ_QUEUE_NAME, Message.HDR_LAST_VALUE_NAME.toString());

      //Send Tombstone
      sendLVQTombstone(producerConnectionSupplier, NON_DESTRUCTIVE_TOMBSTONE_LVQ_QUEUE_NAME, Message.HDR_LAST_VALUE_NAME.toString(), tombstoneTimeToLive);

      assertEquals("Ensure Message count", 1, lastValueQueue.getMessageCount());

      //Simulate a small pause, else both messages could be consumed if consumer is fast enough
      Thread.sleep(10);

      //Consume Tombstone ensuring Tombstone exists
      receiveLVQTombstone(consumerConnectionSupplier, NON_DESTRUCTIVE_TOMBSTONE_LVQ_QUEUE_NAME, Message.HDR_LAST_VALUE_NAME.toString());

      //Consume Again ensuring Tombstone exists as should not have expired
      receiveLVQTombstone(consumerConnectionSupplier, NON_DESTRUCTIVE_TOMBSTONE_LVQ_QUEUE_NAME, Message.HDR_LAST_VALUE_NAME.toString());

      assertEquals("Ensure Message count", 1, lastValueQueue.getLastValueKeys().size());

      //Ensure enough time elapsed for expiration and expiry thread to have run.
      Thread.sleep(tombstoneTimeToLive * 3);

      // Consume again testing tombstone has been removed
      receiveLVQAssertEmpty(consumerConnectionSupplier, NON_DESTRUCTIVE_TOMBSTONE_LVQ_QUEUE_NAME);
      assertEquals("Ensure Message count", 0, lastValueQueue.getMessageCount());
      assertEquals("Ensure Message count", 0, lastValueQueue.getLastValueKeys().size());

   }

   @Test
   public void testMessageCount() throws Exception {
      sendMessage(CoreConnection, NON_DESTRUCTIVE_QUEUE_NAME);

      QueueBinding queueBinding = (QueueBinding) server.getPostOffice().getBinding(SimpleString.toSimpleString(NON_DESTRUCTIVE_QUEUE_NAME));
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      //Consume Once
      receive(CoreConnection, NON_DESTRUCTIVE_QUEUE_NAME);
      assertEquals("Ensure Message count", 1, queueBinding.getQueue().getMessageCount());

      sendMessage(CoreConnection, NON_DESTRUCTIVE_QUEUE_NAME);
      assertEquals("Ensure Message count", 2, queueBinding.getQueue().getMessageCount());

      //Consume Again as should be non-destructive
      receive(CoreConnection, NON_DESTRUCTIVE_QUEUE_NAME);
      assertEquals("Ensure Message count", 2, queueBinding.getQueue().getMessageCount());

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
         TextMessage msg = (TextMessage) consumer.receiveNoWait();
         assertNull(msg);
         consumer.close();
      }
   }

   private void receive(ConnectionSupplier consumerConnectionSupplier, String queueName) throws JMSException {
      try (Connection consumerConnection = consumerConnectionSupplier.createConnection()) {

         Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue consumerQueue = consumerSession.createQueue(queueName);
         MessageConsumer consumer = consumerSession.createConsumer(consumerQueue);
         TextMessage msg = (TextMessage) consumer.receive(2000);
         assertNotNull(msg);
         consumer.close();
      }
   }

   private void receiveNull(ConnectionSupplier consumerConnectionSupplier, String queueName) throws JMSException {
      try (Connection consumerConnection = consumerConnectionSupplier.createConnection()) {

         Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue consumerQueue = consumerSession.createQueue(queueName);
         MessageConsumer consumer = consumerSession.createConsumer(consumerQueue);
         TextMessage msg = (TextMessage) consumer.receiveNoWait();
         assertNull(msg);
         consumer.close();
      }
   }

   private void receiveDualConsumer(ConnectionSupplier consumerConnectionSupplier, String queueName) throws JMSException {
      try (Connection consumerConnection = consumerConnectionSupplier.createConnection();
           Connection consumerConnection2 = consumerConnectionSupplier.createConnection()) {

         MessageConsumer consumer = createConsumer(consumerConnection, queueName);
         MessageConsumer consumer2 = createConsumer(consumerConnection2, queueName);

         TextMessage msg = (TextMessage) consumer.receive(2000);
         TextMessage msg2 = (TextMessage) consumer2.receive(2000);

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
         TextMessage msg = (TextMessage) consumer.receiveNoWait();
         assertNull(msg);
         TextMessage msg2 = (TextMessage) consumer2.receiveNoWait();
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
      try (Connection connection = producerConnectionSupplier.createConnection();
           Session session = connection.createSession();
           MessageProducer producer = session.createProducer(session.createQueue(queueName))) {
         TextMessage message1 = session.createTextMessage();
         message1.setText(Integer.toString(i));
         producer.send(message1);
      }
   }

   private void receiveLVQ(ConnectionSupplier consumerConnectionSupplier, String queueName, String lastValueKey) throws JMSException {
      try (Connection connection = consumerConnectionSupplier.createConnection();
           Session session = connection.createSession();
           MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {
         TextMessage msg = (TextMessage) consumer.receive(1000);
         assertNotNull(msg);
         assertEquals("KEY", msg.getStringProperty(lastValueKey));
         assertEquals("how are you", msg.getText());
      }
   }

   private void sendLVQ(ConnectionSupplier producerConnectionSupplier, String queueName, String lastValueKey) throws JMSException {
      try (Connection connection = producerConnectionSupplier.createConnection();
           Session session = connection.createSession();
           MessageProducer producer = session.createProducer(session.createQueue(queueName))) {

         TextMessage message1 = session.createTextMessage();
         message1.setStringProperty(lastValueKey, "KEY");
         message1.setText("hello");
         producer.send(message1);

         TextMessage message2 = session.createTextMessage();
         message2.setStringProperty(lastValueKey, "KEY");
         message2.setText("how are you");
         producer.send(message2);
      }
   }

   private void receiveLVQTombstone(ConnectionSupplier consumerConnectionSupplier, String queueName, String lastValueKey) throws JMSException {
      try (Connection connection = consumerConnectionSupplier.createConnection();
           Session session = connection.createSession();
           MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {
         TextMessage msg = (TextMessage) consumer.receive(1000);
         assertNotNull(msg);
         assertEquals("KEY", msg.getStringProperty(lastValueKey));
         assertEquals("tombstone", msg.getText());
      }
   }

   private void receiveLVQAssertEmpty(ConnectionSupplier consumerConnectionSupplier, String queueName) throws JMSException {
      try (Connection connection = consumerConnectionSupplier.createConnection();
           Session session = connection.createSession();
           MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {
         TextMessage msg = (TextMessage) consumer.receiveNoWait();
         assertNull(msg);
      }
   }


   private void sendLVQTombstone(ConnectionSupplier producerConnectionSupplier, String queueName, String lastValueKey, int tombstoneTimeToLive) throws JMSException {
      try (Connection connection = producerConnectionSupplier.createConnection();
           Session session = connection.createSession();
           MessageProducer producer = session.createProducer(session.createQueue(queueName))) {
         TextMessage message1 = session.createTextMessage();
         message1.setStringProperty(lastValueKey, "KEY");
         message1.setText("tombstone");
         producer.send(message1, javax.jms.Message.DEFAULT_DELIVERY_MODE, javax.jms.Message.DEFAULT_PRIORITY, tombstoneTimeToLive);
      }
   }
}