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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

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
            System.out.println("checking binidng " + b.getUniqueName() + " " + ((LocalQueueBinding)b).getQueue().getDeliveringMessages());
            SimpleString qName = b.getUniqueName();
            //do FQQN query
            QueueQueryResult result = server.queueQuery(CompositeAddress.toFullQN(multicastAddress, qName));
            assertTrue(result.isExists());
            assertEquals(result.getName(), CompositeAddress.toFullQN(multicastAddress, qName));
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
   //jms queues know no addresses, this test only shows
   //that it is possible for jms clients to receive from
   //core queues by its FQQN.
   public void testQueue() throws Exception {
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ1, null, true, false, -1, false, true);
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ2, null, true, false, -1, false, true);
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ3, null, true, false, -1, false, true);

      Connection connection = factory.createConnection();
      try {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue q1 = session.createQueue(CompositeAddress.toFullQN(anycastAddress, anycastQ1).toString());
         Queue q2 = session.createQueue(CompositeAddress.toFullQN(anycastAddress, anycastQ2).toString());
         Queue q3 = session.createQueue(CompositeAddress.toFullQN(anycastAddress, anycastQ3).toString());

         //send 3 messages to anycastAddress
         locator = createNonHALocator(true);
         ClientSessionFactory cf = createSessionFactory(locator);
         ClientSession coreSession = cf.createSession();

         //send 3 messages
         ClientProducer coreProducer = coreSession.createProducer(anycastAddress);
         sendMessages(coreSession, coreProducer, 3);

         System.out.println("Queue is: " + q1);
         MessageConsumer consumer1 = session.createConsumer(q1);
         MessageConsumer consumer2 = session.createConsumer(q2);
         MessageConsumer consumer3 = session.createConsumer(q3);

         //each consumer receives one
         assertNotNull(consumer1.receive(2000));
         assertNotNull(consumer2.receive(2000));
         assertNotNull(consumer3.receive(2000));

         connection.close();
         //queues are empty now
         for (SimpleString q : new SimpleString[]{anycastQ1, anycastQ2, anycastQ3}) {
            //FQQN query
            QueueQueryResult query = server.queueQuery(CompositeAddress.toFullQN(anycastAddress, q));
            assertTrue(query.isExists());
            assertEquals(anycastAddress, query.getAddress());
            assertEquals(CompositeAddress.toFullQN(anycastAddress, q), query.getName());
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
      this.server.createQueue(durableQueue, RoutingType.ANYCAST, durableQueue, null, true, false, -1, false, true);

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();

         exConn = exFact.createConnection();

         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Destination destination = session.createQueue(durableQueue.toString());

         MessageProducer producer = session.createProducer(destination);

         TextMessage message = session.createTextMessage("This is a text message");

         producer.send(message);

         Destination destinationFQN = session.createQueue(CompositeAddress.toFullQN(durableQueue, durableQueue).toString());

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
      this.server.createQueue(durableQueue, RoutingType.ANYCAST, durableQueue, null, true, false, -1, false, true);

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
      this.server.createQueue(topic, RoutingType.MULTICAST, subscriptionQ, null, true, false, -1, false, true);

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();
         exFact.setWatchTopicAdvisories(false);
         exConn = exFact.createConnection();
         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination destination = session.createTopic(topic.toString());
         MessageProducer producer = session.createProducer(destination);

         Destination destinationFQN = session.createQueue(CompositeAddress.toFullQN(topic, subscriptionQ).toString());
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

         Destination destinationFQN = session.createQueue(CompositeAddress.toFullQN(topic, subscriptionQ).toString());

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

         Destination destinationFQN = session.createQueue(CompositeAddress.toFullQN(topic, subscriptionQ).toString());

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
         Destination destinationFQN = session.createQueue(CompositeAddress.toFullQN(topic, subscriptionQ).toString());

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

         Destination destinationFQN = session.createQueue(CompositeAddress.toFullQN(topic, subscriptionQ).toString());

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
