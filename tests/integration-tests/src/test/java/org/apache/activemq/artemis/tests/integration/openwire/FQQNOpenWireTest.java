/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.openwire;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Verify FQQN queues work with openwire/artemis JMS API
 */
@RunWith(Parameterized.class)
public class FQQNOpenWireTest extends OpenWireTestBase {

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> params() {
      return Arrays.asList(new Object[][]{{"OpenWire"}, {"Artemis"}});
   }

   private SimpleString anycastAddress = new SimpleString("address.anycast");
   private SimpleString multicastAddress = new SimpleString("address.multicast");

   private SimpleString anycastQ1 = new SimpleString("q1");
   private SimpleString anycastQ2 = new SimpleString("q2");
   private SimpleString anycastQ3 = new SimpleString("q3");

   private ConnectionFactory factory;

   private ServerLocator locator;

   public FQQNOpenWireTest(String factoryType) {
      if ("OpenWire".equals(factoryType)) {
         factory = new ActiveMQConnectionFactory(urlString);
      } else if ("Artemis".equals(factoryType)) {
         factory = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(urlString);
      }
   }

   @Test
   //there isn't much use of FQQN for topics
   //however we can test query functionality
   public void testTopic() throws Exception {

      Connection connection = factory.createConnection();
      try {
         connection.setClientID("FQQNconn");
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(multicastAddress.toString());

         MessageConsumer consumer1 = session.createConsumer(topic);
         MessageConsumer consumer2 = session.createConsumer(topic);
         MessageConsumer consumer3 = session.createConsumer(topic);

         MessageProducer producer = session.createProducer(topic);

         producer.send(session.createMessage());

         //each consumer receives one
         Message m = consumer1.receive(2000);
         assertNotNull(m);
         m = consumer2.receive(2000);
         assertNotNull(m);
         m = consumer3.receive(2000);
         assertNotNull(m);

         Bindings bindings = server.getPostOffice().getBindingsForAddress(multicastAddress);
         for (Binding b : bindings.getBindings()) {
            instanceLog.debug("checking binidng " + b.getUniqueName() + " " + ((LocalQueueBinding)b).getQueue().getDeliveringMessages());
            SimpleString qName = b.getUniqueName();
            //do FQQN query
            QueueQueryResult result = server.queueQuery(CompositeAddress.toFullyQualified(multicastAddress, qName));
            assertTrue(result.isExists());
            assertEquals(result.getName(), qName);
            //do qname query
            result = server.queueQuery(qName);
            assertTrue(result.isExists());
            assertEquals(result.getName(), qName);
         }
      } finally {
         connection.close();
      }
   }

   @Test
   public void testTopicFQQNSendAndConsumeAutoCreate() throws Exception {
      internalTopicFQQNSendAndConsume(true);
   }

   @Test
   public void testTopicFQQNSendAndConsumeManualCreate() throws Exception {
      internalTopicFQQNSendAndConsume(false);
   }

   private void internalTopicFQQNSendAndConsume(boolean autocreate) throws Exception {
      if (autocreate) {
         server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setAutoCreateAddresses(true).setAutoCreateQueues(true));
      } else {
         server.createQueue(new QueueConfiguration(anycastQ1).setAddress(multicastAddress).setDurable(false));
      }

      try (Connection connection = factory.createConnection()) {
         connection.setClientID("FQQNconn");
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(CompositeAddress.toFullyQualified(multicastAddress, anycastQ1).toString());

         MessageConsumer consumer1 = session.createConsumer(topic);
         MessageConsumer consumer2 = session.createConsumer(topic);
         MessageConsumer consumer3 = session.createConsumer(topic);

         MessageProducer producer = session.createProducer(topic);

         producer.send(session.createMessage());

         //only 1 consumer receives the message as they're all connected to the same FQQN
         Message m = consumer1.receive(2000);
         assertNotNull(m);
         m = consumer2.receiveNoWait();
         assertNull(m);
         m = consumer3.receiveNoWait();
         assertNull(m);
      }
   }

   @Test
   public void testQueueConsumerReceiveTopicUsingFQQN() throws Exception {

      SimpleString queueName1 = new SimpleString("sub.queue1");
      SimpleString queueName2 = new SimpleString("sub.queue2");
      server.createQueue(new QueueConfiguration(queueName1).setAddress(multicastAddress).setDurable(false));
      server.createQueue(new QueueConfiguration(queueName2).setAddress(multicastAddress).setDurable(false));
      Connection connection = factory.createConnection();

      try {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue fqqn1 = session.createQueue(multicastAddress.toString() + "::" + queueName1);
         javax.jms.Queue fqqn2 = session.createQueue(multicastAddress.toString() + "::" + queueName2);

         MessageConsumer consumer1 = session.createConsumer(fqqn1);
         MessageConsumer consumer2 = session.createConsumer(fqqn2);

         Topic topic = session.createTopic(multicastAddress.toString());
         MessageProducer producer = session.createProducer(topic);

         producer.send(session.createMessage());

         Message m = consumer1.receive(2000);
         assertNotNull(m);

         m = consumer2.receive(2000);
         assertNotNull(m);

      } finally {
         connection.close();
      }
   }

   @Test
   //jms queues know no addresses, this test only shows
   //that it is possible for jms clients to receive from
   //core queues by its FQQN.
   public void testQueue() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setAutoCreateQueues(true).setAutoCreateAddresses(true));

      Connection connection = factory.createConnection();
      try {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue q1 = session.createQueue(CompositeAddress.toFullyQualified(anycastAddress, anycastQ1).toString());
         Queue q2 = session.createQueue(CompositeAddress.toFullyQualified(anycastAddress, anycastQ2).toString());
         Queue q3 = session.createQueue(CompositeAddress.toFullyQualified(anycastAddress, anycastQ3).toString());

         MessageProducer producer1 = session.createProducer(q1);
         producer1.send(session.createMessage());
         producer1.send(session.createMessage());
         assertTrue(Wait.waitFor(() -> server.locateQueue(anycastQ1).getMessageCount() == 2, 2000, 200));

         MessageProducer producer2 = session.createProducer(q2);
         producer2.send(session.createMessage());
         producer2.send(session.createMessage());
         producer2.send(session.createMessage());
         assertTrue(Wait.waitFor(() -> server.locateQueue(anycastQ2).getMessageCount() == 3, 2000, 200));

         MessageProducer producer3 = session.createProducer(q3);
         producer3.send(session.createMessage());
         producer3.send(session.createMessage());
         producer3.send(session.createMessage());
         producer3.send(session.createMessage());
         producer3.send(session.createMessage());
         assertTrue(Wait.waitFor(() -> server.locateQueue(anycastQ3).getMessageCount() == 5, 2000, 200));

         MessageConsumer consumer1 = session.createConsumer(q1);
         MessageConsumer consumer2 = session.createConsumer(q2);
         MessageConsumer consumer3 = session.createConsumer(q3);

         //each consumer receives one
         assertNotNull(consumer1.receive(2000));
         assertNotNull(consumer1.receive(2000));

         assertNotNull(consumer2.receive(2000));
         assertNotNull(consumer2.receive(2000));
         assertNotNull(consumer2.receive(2000));

         assertNotNull(consumer3.receive(2000));
         assertNotNull(consumer3.receive(2000));
         assertNotNull(consumer3.receive(2000));
         assertNotNull(consumer3.receive(2000));
         assertNotNull(consumer3.receive(2000));

         connection.close();
         //queues are empty now
         for (SimpleString q : new SimpleString[]{anycastQ1, anycastQ2, anycastQ3}) {
            //FQQN query
            QueueQueryResult query = server.queueQuery(CompositeAddress.toFullyQualified(anycastAddress, q));
            assertTrue(query.isExists() || query.isAutoCreateQueues());
            assertEquals(anycastAddress, query.getAddress());
            assertEquals(q, query.getName());
            assertEquals(0, query.getMessageCount());
            //try query again using qName
            query = server.queueQuery(q);
            assertEquals(q, query.getName());
         }
      } finally {
         connection.close();
         if (locator != null) {
            locator.close();
         }
      }
   }

   @Test
   public void testFQNConsumer() throws Exception {
      Connection exConn = null;

      SimpleString durableQueue = new SimpleString("myqueue");
      this.server.createQueue(new QueueConfiguration(durableQueue).setRoutingType(RoutingType.ANYCAST));

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();

         exConn = exFact.createConnection();

         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Destination destination = session.createQueue(durableQueue.toString());

         MessageProducer producer = session.createProducer(destination);

         TextMessage message = session.createTextMessage("This is a text message");

         producer.send(message);

         Destination destinationFQN = session.createQueue(CompositeAddress.toFullyQualified(durableQueue, durableQueue).toString());

         MessageConsumer messageConsumer = session.createConsumer(destinationFQN);

         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         assertEquals("This is a text message", messageReceived.getText());
      } finally {
         if (exConn != null) {
            exConn.close();
         }
      }
   }

   @Test
   public void testSpecialFQQNCase() throws Exception {
      Connection exConn = null;

      SimpleString durableQueue = new SimpleString("myqueue");
      this.server.createQueue(new QueueConfiguration(durableQueue).setRoutingType(RoutingType.ANYCAST));

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();
         exConn = exFact.createConnection();
         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination destination = session.createQueue(durableQueue.toString());

         MessageProducer producer = session.createProducer(destination);
         TextMessage message = session.createTextMessage("This is a text message");
         producer.send(message);

         //this should work as if only queue names is given
         Destination destinationFQN = session.createQueue(CompositeAddress.SEPARATOR + durableQueue);
         MessageConsumer messageConsumer = session.createConsumer(destinationFQN);
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
         assertEquals("This is a text message", messageReceived.getText());
         messageConsumer.close();

         destinationFQN = session.createQueue(durableQueue + CompositeAddress.SEPARATOR);
         try {
            session.createConsumer(destinationFQN);
            fail("should get exception");
         } catch (InvalidDestinationException e) {
            //expected.
         }
         destinationFQN = session.createQueue(CompositeAddress.SEPARATOR);
         try {
            session.createConsumer(destinationFQN);
            fail("should get exception");
         } catch (InvalidDestinationException e) {
            //expected.
         }

      } finally {
         if (exConn != null) {
            exConn.close();
         }
      }
   }

   @Test
   public void testVirtualTopicFQQN() throws Exception {
      Connection exConn = null;

      SimpleString topic = new SimpleString("VirtualTopic.Orders");
      SimpleString subscriptionQ = new SimpleString("Consumer.A");

      this.server.addAddressInfo(new AddressInfo(topic, RoutingType.MULTICAST));
      this.server.createQueue(new QueueConfiguration(subscriptionQ).setAddress(topic));

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();
         exFact.setWatchTopicAdvisories(false);
         exConn = exFact.createConnection();
         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination destination = session.createTopic(topic.toString());
         MessageProducer producer = session.createProducer(destination);

         Destination destinationFQN = session.createQueue(CompositeAddress.toFullyQualified(topic, subscriptionQ).toString());
         MessageConsumer messageConsumerA = session.createConsumer(destinationFQN);
         MessageConsumer messageConsumerB = session.createConsumer(destinationFQN);

         TextMessage message = session.createTextMessage("This is a text message");
         producer.send(message);

         // only one consumer should get the message
         TextMessage messageReceivedA = (TextMessage) messageConsumerA.receive(2000);
         TextMessage messageReceivedB = (TextMessage) messageConsumerB.receive(2000);

         assertTrue((messageReceivedA == null || messageReceivedB == null));
         String text = messageReceivedA != null ? messageReceivedA.getText() : messageReceivedB.getText();
         assertEquals("This is a text message", text);

         messageConsumerA.close();
         messageConsumerB.close();

      } finally {
         if (exConn != null) {
            exConn.close();
         }
      }
   }

   @Test
   public void testVirtualTopicFQQNAutoCreateQueue() throws Exception {
      Connection exConn = null;

      SimpleString topic = new SimpleString("VirtualTopic.Orders");
      SimpleString subscriptionQ = new SimpleString("Consumer.A");

      // defaults are false via test setUp
      this.server.addAddressInfo(new AddressInfo(topic, RoutingType.MULTICAST));
      this.server.getAddressSettingsRepository().getMatch("VirtualTopic.#").setAutoCreateQueues(true);

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();
         exFact.setWatchTopicAdvisories(false);
         exConn = exFact.createConnection();
         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination destination = session.createTopic(topic.toString());
         MessageProducer producer = session.createProducer(destination);

         Destination destinationFQN = session.createQueue(CompositeAddress.toFullyQualified(topic, subscriptionQ).toString());

         MessageConsumer messageConsumerA = session.createConsumer(destinationFQN);
         MessageConsumer messageConsumerB = session.createConsumer(destinationFQN);

         TextMessage message = session.createTextMessage("This is a text message");
         producer.send(message);

         // only one consumer should get the message
         TextMessage messageReceivedA = (TextMessage) messageConsumerA.receive(2000);
         TextMessage messageReceivedB = (TextMessage) messageConsumerB.receive(2000);

         assertTrue((messageReceivedA == null || messageReceivedB == null));
         String text = messageReceivedA != null ? messageReceivedA.getText() : messageReceivedB.getText();
         assertEquals("This is a text message", text);

         messageConsumerA.close();
         messageConsumerB.close();

      } finally {
         if (exConn != null) {
            exConn.close();
         }
      }
   }

   @Test
   public void testVirtualTopicFQQNAutoCreateQAndAddress() throws Exception {
      Connection exConn = null;

      SimpleString topic = new SimpleString("VirtualTopic.Orders");
      SimpleString subscriptionQ = new SimpleString("Consumer.A");

      // defaults are false via test setUp
      this.server.getAddressSettingsRepository().getMatch("VirtualTopic.#").setAutoCreateQueues(true);
      this.server.getAddressSettingsRepository().getMatch("VirtualTopic.#").setAutoCreateAddresses(true);

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();
         exFact.setWatchTopicAdvisories(false);
         exConn = exFact.createConnection();
         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination destination = session.createTopic(topic.toString());
         MessageProducer producer = session.createProducer(destination);

         Destination destinationFQN = session.createQueue(CompositeAddress.toFullyQualified(topic, subscriptionQ).toString());

         MessageConsumer messageConsumerA = session.createConsumer(destinationFQN);
         MessageConsumer messageConsumerB = session.createConsumer(destinationFQN);

         TextMessage message = session.createTextMessage("This is a text message");
         producer.send(message);

         // only one consumer should get the message
         TextMessage messageReceivedA = (TextMessage) messageConsumerA.receive(2000);
         TextMessage messageReceivedB = (TextMessage) messageConsumerB.receive(2000);

         assertTrue((messageReceivedA == null || messageReceivedB == null));
         String text = messageReceivedA != null ? messageReceivedA.getText() : messageReceivedB.getText();
         assertEquals("This is a text message", text);

         messageConsumerA.close();
         messageConsumerB.close();

      } finally {
         if (exConn != null) {
            exConn.close();
         }
      }
   }

   @Test
   public void testVirtualTopicFQQNConsumerAutoCreateQAndAddress() throws Exception {
      Connection exConn = null;

      SimpleString topic = new SimpleString("VirtualTopic.Orders");
      SimpleString subscriptionQ = new SimpleString("Consumer.A");

      // defaults are false via test setUp
      this.server.getAddressSettingsRepository().getMatch("VirtualTopic.#").setAutoCreateQueues(true);
      this.server.getAddressSettingsRepository().getMatch("VirtualTopic.#").setAutoCreateAddresses(true);

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();
         exFact.setWatchTopicAdvisories(false);
         exConn = exFact.createConnection();
         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination destination = session.createTopic(topic.toString());
         Destination destinationFQN = session.createQueue(CompositeAddress.toFullyQualified(topic, subscriptionQ).toString());

         MessageConsumer messageConsumerA = session.createConsumer(destinationFQN);
         MessageConsumer messageConsumerB = session.createConsumer(destinationFQN);

         MessageProducer producer = session.createProducer(destination);
         TextMessage message = session.createTextMessage("This is a text message");
         producer.send(message);

         // only one consumer should get the message
         TextMessage messageReceivedA = (TextMessage) messageConsumerA.receive(2000);
         TextMessage messageReceivedB = (TextMessage) messageConsumerB.receive(2000);

         assertTrue((messageReceivedA == null || messageReceivedB == null));
         String text = messageReceivedA != null ? messageReceivedA.getText() : messageReceivedB.getText();
         assertEquals("This is a text message", text);

         messageConsumerA.close();
         messageConsumerB.close();

      } finally {
         if (exConn != null) {
            exConn.close();
         }
      }
   }

   @Test
   public void testVirtualTopicFQQNAutoCreateQWithExistingAddressWithAnyCastDefault() throws Exception {
      Connection exConn = null;

      SimpleString topic = new SimpleString("VirtualTopic.Orders");
      SimpleString subscriptionQ = new SimpleString("Consumer.A");

      // defaults are false via test setUp
      this.server.addAddressInfo(new AddressInfo(topic, RoutingType.MULTICAST));
      this.server.getAddressSettingsRepository().getMatch("VirtualTopic.#").setAutoCreateQueues(true);
      this.server.getAddressSettingsRepository().getMatch("VirtualTopic.#").setAutoCreateAddresses(false);

      // set default to anycast which would fail if used in queue auto creation
      this.server.getAddressSettingsRepository().getMatch("VirtualTopic.#").setDefaultAddressRoutingType(RoutingType.ANYCAST);

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();
         exFact.setWatchTopicAdvisories(false);
         exConn = exFact.createConnection();
         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination destination = session.createTopic(topic.toString());
         MessageProducer producer = session.createProducer(destination);

         Destination destinationFQN = session.createQueue(CompositeAddress.toFullyQualified(topic, subscriptionQ).toString());

         MessageConsumer messageConsumerA = session.createConsumer(destinationFQN);
         MessageConsumer messageConsumerB = session.createConsumer(destinationFQN);

         TextMessage message = session.createTextMessage("This is a text message");
         producer.send(message);

         // only one consumer should get the message
         TextMessage messageReceivedA = (TextMessage) messageConsumerA.receive(2000);
         TextMessage messageReceivedB = (TextMessage) messageConsumerB.receive(2000);

         assertTrue((messageReceivedA == null || messageReceivedB == null));
         String text = messageReceivedA != null ? messageReceivedA.getText() : messageReceivedB.getText();
         assertEquals("This is a text message", text);

         messageConsumerA.close();
         messageConsumerB.close();

      } finally {
         if (exConn != null) {
            exConn.close();
         }
      }
   }
}
