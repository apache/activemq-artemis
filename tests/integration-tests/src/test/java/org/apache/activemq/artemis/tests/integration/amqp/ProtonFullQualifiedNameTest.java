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

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.HashMap;
import java.util.Map;

public class ProtonFullQualifiedNameTest extends ProtonTestBase {

   private static final String amqpConnectionUri = "amqp://localhost:5672";

   private SimpleString anycastAddress = new SimpleString("address.anycast");
   private SimpleString multicastAddress = new SimpleString("address.multicast");

   private SimpleString anycastQ1 = new SimpleString("q1");
   private SimpleString anycastQ2 = new SimpleString("q2");
   private SimpleString anycastQ3 = new SimpleString("q3");

   JmsConnectionFactory factory = new JmsConnectionFactory(amqpConnectionUri);
   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      Configuration serverConfig = server.getConfiguration();

      Map<String, AddressSettings> settings = serverConfig.getAddressesSettings();
      assertNotNull(settings);
      AddressSettings addressSetting = settings.get("#");
      if (addressSetting == null) {
         addressSetting = new AddressSettings();
         settings.put("#", addressSetting);
      }
      addressSetting.setAutoCreateQueues(true);
      locator = createNettyNonHALocator();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      super.tearDown();
   }

   @Override
   protected void configureServer(Configuration serverConfig) {
      serverConfig.addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, new HashMap<String, Object>(), "netty", new HashMap<String, Object>()));
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
      }
   }

   @Test
   public void testQueueSpecial() throws Exception {
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ1, null, true, false, -1, false, true);

      Connection connection = factory.createConnection();
      try {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //::queue ok!
         String specialName = CompositeAddress.toFullQN(new SimpleString(""), anycastQ1).toString();
         Queue q1 = session.createQueue(specialName);

         ClientSessionFactory cf = createSessionFactory(locator);
         ClientSession coreSession = cf.createSession();

         ClientProducer coreProducer = coreSession.createProducer(anycastAddress);
         sendMessages(coreSession, coreProducer, 1);

         System.out.println("create consumer: " + q1);
         MessageConsumer consumer1 = session.createConsumer(q1);

         assertNotNull(consumer1.receive(2000));

         //queue::
         specialName = CompositeAddress.toFullQN(anycastQ1, new SimpleString("")).toString();
         q1 = session.createQueue(specialName);
         try {
            session.createConsumer(q1);
            fail("should get exception");
         } catch (InvalidDestinationException e) {
            //expected
         }

         //::
         specialName = CompositeAddress.toFullQN(new SimpleString(""), new SimpleString("")).toString();
         q1 = session.createQueue(specialName);
         try {
            session.createConsumer(q1);
            fail("should get exception");
         } catch (InvalidDestinationException e) {
            //expected
         }

      } finally {
         connection.close();
      }
   }

}
