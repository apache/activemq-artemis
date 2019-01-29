/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.junit.Before;
import org.junit.Test;

public class AmqpFullyQualifiedNameTest extends JMSClientTestSupport {

   private SimpleString anycastAddress = new SimpleString("address.anycast");
   private SimpleString multicastAddress = new SimpleString("address.multicast");

   private SimpleString anycastQ1 = new SimpleString("q1");
   private SimpleString anycastQ2 = new SimpleString("q2");
   private SimpleString anycastQ3 = new SimpleString("q3");

   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      locator = createNettyNonHALocator();
   }

   @Override
   protected void addAdditionalAcceptors(ActiveMQServer server) throws Exception {
      server.getConfiguration().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, new HashMap<String, Object>(), "netty", new HashMap<String, Object>()));
   }

   @Test
   public void testFQQNTopicWhenQueueDoesNotExist() throws Exception {
      Exception e = null;
      String queueName = "testQueue";

      Connection connection = createConnection(false);
      try {
         connection.setClientID("FQQNconn");
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(multicastAddress.toString() + "::" + queueName);
         session.createConsumer(topic);
      } catch (InvalidDestinationException ide) {
         e = ide;
      } finally {
         connection.close();
      }
      assertNotNull(e);
      assertTrue(e.getMessage().contains("Queue: '" + queueName + "' does not exist"));
   }

   @Test
   public void testConsumeQueueToFQQNWrongQueueAttachedToAnotherAddress() throws Exception {

      // Create 2 Queues: address1::queue1, address2::queue2
      String address1 = "a1";
      String address2 = "a2";
      String queue1 = "q1";
      String queue2 = "q2";

      server.createQueue(SimpleString.toSimpleString(address1), RoutingType.ANYCAST, SimpleString.toSimpleString(queue1), null, true, false, -1, false, true);
      server.createQueue(SimpleString.toSimpleString(address2), RoutingType.ANYCAST, SimpleString.toSimpleString(queue2), null, true, false, -1, false, true);

      Exception e = null;

      // Wrong FQQN.  Attempt to subscribe to a queue belonging to a different address than given in the FQQN.
      String wrongFQQN = address1 + "::"  + queue2;
      Connection connection = createConnection(false);
      try {
         connection.setClientID("FQQNconn");
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(wrongFQQN);
         session.createConsumer(queue);
      } catch (InvalidDestinationException ide) {
         e = ide;
      } finally {
         connection.close();
      }
      assertNotNull(e);
      assertTrue(e.getMessage().contains("Queue: '" + queue2 + "' does not exist for address '" + address1 + "'"));
   }

   @Test
   public void testSubscribeTopicToFQQNWrongQueueAttachedToAnotherAddress() throws Exception {

      // Create 2 Queues: address1::queue1, address2::queue2
      String address1 = "a1";
      String address2 = "a2";
      String queue1 = "q1";
      String queue2 = "q2";

      server.createQueue(SimpleString.toSimpleString(address1), RoutingType.MULTICAST, SimpleString.toSimpleString(queue1), null, true, false, -1, false, true);
      server.createQueue(SimpleString.toSimpleString(address2), RoutingType.MULTICAST, SimpleString.toSimpleString(queue2), null, true, false, -1, false, true);

      Exception e = null;

      // Wrong FQQN.  Attempt to subscribe to a queue belonging to a different address than given in the FQQN.
      String wrongFQQN = address1 + "::"  + queue2;
      Connection connection = createConnection(false);
      try {
         connection.setClientID("FQQNconn");
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(wrongFQQN);
         session.createConsumer(topic);
      } catch (InvalidDestinationException ide) {
         e = ide;
      } finally {
         connection.close();
      }
      assertNotNull(e);
      assertTrue(e.getMessage().contains("Queue: '" + queue2 + "' does not exist for address '" + address1 + "'"));
   }

   @Test(timeout = 60000)
   //there isn't much use of FQQN for topics
   //however we can test query functionality
   public void testTopic() throws Exception {

      SimpleString queueName = new SimpleString("someAddress");
      server.createQueue(multicastAddress, RoutingType.MULTICAST, queueName, null, false, false);
      Connection connection = createConnection(false);

      try {
         connection.setClientID("FQQNconn");
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic fqqn = session.createTopic(multicastAddress.toString() + "::" + queueName);

         MessageConsumer consumer1 = session.createConsumer(fqqn);
         MessageConsumer consumer2 = session.createConsumer(fqqn);

         Topic topic = session.createTopic(multicastAddress.toString());
         MessageProducer producer = session.createProducer(topic);

         producer.send(session.createMessage());

         //each consumer receives one
         Message m = consumer1.receive(2000);
         assertNotNull(m);

         // Subscribing to FQQN is akin to shared subscription
         m = consumer2.receive(2000);
         assertNull(m);

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
   public void testQueue() throws Exception {
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ1, null, true, false, -1, false, true);
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ2, null, true, false, -1, false, true);
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ3, null, true, false, -1, false, true);

      Connection connection = createConnection();
      try {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         javax.jms.Queue q1 = session.createQueue(CompositeAddress.toFullQN(anycastAddress, anycastQ1).toString());
         javax.jms.Queue q2 = session.createQueue(CompositeAddress.toFullQN(anycastAddress, anycastQ2).toString());
         javax.jms.Queue q3 = session.createQueue(CompositeAddress.toFullQN(anycastAddress, anycastQ3).toString());

         //send 3 messages to anycastAddress
         ClientSessionFactory cf = createSessionFactory(locator);
         ClientSession coreSession = cf.createSession();

         //send 3 messages
         ClientProducer coreProducer = coreSession.createProducer(anycastAddress);
         sendMessages(coreSession, coreProducer, 3);

         MessageConsumer consumer1 = session.createConsumer(q1);
         MessageConsumer consumer2 = session.createConsumer(q2);
         MessageConsumer consumer3 = session.createConsumer(q3);

         //each consumer receives one
         assertNotNull(consumer1.receive(2000));
         assertNotNull(consumer2.receive(2000));
         assertNotNull(consumer3.receive(2000));

         Queue queue1 = getProxyToQueue(anycastQ1.toString());
         Wait.assertEquals(0, queue1::getMessageCount);
         Queue queue2 = getProxyToQueue(anycastQ2.toString());
         Wait.assertEquals(0, queue2::getMessageCount);
         Queue queue3 = getProxyToQueue(anycastQ3.toString());
         Wait.assertEquals(0, queue3::getMessageCount);

         connection.close();
         //queues are empty now
         for (SimpleString q : new SimpleString[]{anycastQ1, anycastQ2, anycastQ3}) {
            //FQQN query
            final QueueQueryResult query = server.queueQuery(CompositeAddress.toFullQN(anycastAddress, q));
            assertTrue(query.isExists());
            assertEquals(anycastAddress, query.getAddress());
            assertEquals(CompositeAddress.toFullQN(anycastAddress, q), query.getName());
            assertEquals("Message not consumed", 0, query.getMessageCount());
            //try query again using qName
            QueueQueryResult qNameQuery = server.queueQuery(q);
            assertEquals(q, qNameQuery.getName());
         }
      } finally {
         connection.close();
      }
   }

   /**
    * Broker should return exception if no address is passed in FQQN.
    * @throws Exception
    */
   @Test
   public void testQueueSpecial() throws Exception {
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ1, null, true, false, -1, false, true);

      Connection connection = createConnection();
      Exception expectedException = null;
      try {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //::queue ok!
         String specialName = CompositeAddress.toFullQN(new SimpleString(""), anycastQ1).toString();
         javax.jms.Queue q1 = session.createQueue(specialName);
         session.createConsumer(q1);
      } catch (InvalidDestinationException e) {
         expectedException = e;
      }
      assertNotNull(expectedException);
      assertTrue(expectedException.getMessage().contains("Queue: 'q1' does not exist for address ''"));
   }
}
