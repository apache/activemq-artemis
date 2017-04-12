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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.junit.Before;
import org.junit.Test;

public class FullQualifiedQueueTest extends ActiveMQTestBase {

   private SimpleString anycastAddress = new SimpleString("address.anycast");
   private SimpleString multicastAddress = new SimpleString("address.multicast");
   private SimpleString mixedAddress = new SimpleString("address.mixed");

   private SimpleString anycastQ1 = new SimpleString("q1");
   private SimpleString anycastQ2 = new SimpleString("q2");
   private SimpleString anycastQ3 = new SimpleString("q3");

   private SimpleString multicastQ1 = new SimpleString("q4");
   private SimpleString multicastQ2 = new SimpleString("q5");
   private SimpleString multicastQ3 = new SimpleString("q6");

   private ActiveMQServer server;
   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false, true);

      server.start();
      locator = createNettyNonHALocator();
   }

   @Test
   public void testMixedQueues() throws Exception {
      server.createQueue(mixedAddress, RoutingType.MULTICAST, multicastQ1, null, true, false, -1, false, true);
      server.createQueue(mixedAddress, RoutingType.MULTICAST, multicastQ2, null, true, false, -1, false, true);
      server.createQueue(mixedAddress, RoutingType.MULTICAST, multicastQ3, null, true, false, -1, false, true);
      server.createQueue(mixedAddress, RoutingType.ANYCAST, anycastQ1, null, true, false, -1, false, true);
      server.createQueue(mixedAddress, RoutingType.ANYCAST, anycastQ2, null, true, false, -1, false, true);
      server.createQueue(mixedAddress, RoutingType.ANYCAST, anycastQ3, null, true, false, -1, false, true);

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession();
      session.start();

      //send 3 messages
      ClientProducer producer = session.createProducer(mixedAddress);
      final int num = 3;
      sendMessages(session, producer, num);

      ClientConsumer consumer1 = session.createConsumer(toFullQN(mixedAddress, anycastQ1));
      ClientConsumer consumer2 = session.createConsumer(toFullQN(mixedAddress, anycastQ2));
      ClientConsumer consumer3 = session.createConsumer(toFullQN(mixedAddress, anycastQ3));
      ClientConsumer consumer4 = session.createConsumer(toFullQN(mixedAddress, multicastQ1));
      ClientConsumer consumer5 = session.createConsumer(toFullQN(mixedAddress, multicastQ2));
      ClientConsumer consumer6 = session.createConsumer(toFullQN(mixedAddress, multicastQ3));

      session.start();

      //each anycast consumer receives one, each multicast receives three.
      ClientMessage m = consumer1.receive(2000);
      assertNotNull(m);
      System.out.println("consumer1 : " + m);
      m.acknowledge();

      m = consumer2.receive(2000);
      assertNotNull(m);
      System.out.println("consumer2 : " + m);
      m.acknowledge();

      m = consumer3.receive(2000);
      assertNotNull(m);
      System.out.println("consumer3 : " + m);
      m.acknowledge();

      for (int i = 0; i < num; i++) {
         m = consumer4.receive(2000);
         assertNotNull(m);
         System.out.println("consumer4 : " + m);
         m.acknowledge();
         m = consumer5.receive(2000);
         assertNotNull(m);
         System.out.println("consumer5 : " + m);
         m.acknowledge();
         m = consumer6.receive(2000);
         assertNotNull(m);
         System.out.println("consumer6 : " + m);
         m.acknowledge();
      }

      session.commit();

      //queues are empty now
      for (SimpleString q : new SimpleString[]{anycastQ1, anycastQ2, anycastQ3, multicastQ1, multicastQ2, multicastQ3}) {
         QueueQueryResult query = server.queueQuery(toFullQN(mixedAddress, q));
         assertTrue(query.isExists());
         assertEquals(mixedAddress, query.getAddress());
         assertEquals(toFullQN(mixedAddress, q), query.getName());
         assertEquals(0, query.getMessageCount());
      }
   }

   @Test
   public void testMulticastQueues() throws Exception {
      server.createQueue(multicastAddress, RoutingType.MULTICAST, multicastQ1, null, true, false, -1, false, true);
      server.createQueue(multicastAddress, RoutingType.MULTICAST, multicastQ2, null, true, false, -1, false, true);
      server.createQueue(multicastAddress, RoutingType.MULTICAST, multicastQ3, null, true, false, -1, false, true);

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession();
      session.start();

      //send 3 messages
      ClientProducer producer = session.createProducer(multicastAddress);
      sendMessages(session, producer, 1);

      ClientConsumer consumer1 = session.createConsumer(toFullQN(multicastAddress, multicastQ1));
      ClientConsumer consumer2 = session.createConsumer(toFullQN(multicastAddress, multicastQ2));
      ClientConsumer consumer3 = session.createConsumer(toFullQN(multicastAddress, multicastQ3));
      session.start();

      //each consumer receives one
      ClientMessage m = consumer1.receive(2000);
      assertNotNull(m);
      m.acknowledge();
      m = consumer2.receive(2000);
      assertNotNull(m);
      m.acknowledge();
      m = consumer3.receive(2000);
      assertNotNull(m);
      m.acknowledge();

      session.commit();
      //queues are empty now
      for (SimpleString q : new SimpleString[]{multicastQ1, multicastQ2, multicastQ3}) {
         QueueQueryResult query = server.queueQuery(toFullQN(multicastAddress, q));
         assertTrue(query.isExists());
         assertEquals(multicastAddress, query.getAddress());
         assertEquals(toFullQN(multicastAddress, q), query.getName());
         assertEquals(0, query.getMessageCount());
      }
   }

   @Test
   public void testAnycastQueues() throws Exception {
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ1, null, true, false, -1, false, true);
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ2, null, true, false, -1, false, true);
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ3, null, true, false, -1, false, true);

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession();
      session.start();

      //send 3 messages
      ClientProducer producer = session.createProducer(anycastAddress);
      sendMessages(session, producer, 3);

      ClientConsumer consumer1 = session.createConsumer(toFullQN(anycastAddress, anycastQ1));
      ClientConsumer consumer2 = session.createConsumer(toFullQN(anycastAddress, anycastQ2));
      ClientConsumer consumer3 = session.createConsumer(toFullQN(anycastAddress, anycastQ3));
      session.start();

      //each consumer receives one
      ClientMessage m = consumer1.receive(2000);
      assertNotNull(m);
      m.acknowledge();
      m = consumer2.receive(2000);
      assertNotNull(m);
      m.acknowledge();
      m = consumer3.receive(2000);
      assertNotNull(m);
      m.acknowledge();

      session.commit();
      //queues are empty now
      for (SimpleString q : new SimpleString[]{anycastQ1, anycastQ2, anycastQ3}) {
         QueueQueryResult query = server.queueQuery(toFullQN(anycastAddress, q));
         assertTrue(query.isExists());
         assertEquals(anycastAddress, query.getAddress());
         assertEquals(toFullQN(anycastAddress, q), query.getName());
         assertEquals(0, query.getMessageCount());
      }
   }

   @Test
   public void testSpecialCase() throws Exception {
      server.createQueue(anycastAddress, RoutingType.ANYCAST, anycastQ1, null, true, false, -1, false, true);

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession();
      session.start();

      ClientProducer producer = session.createProducer(anycastAddress);
      sendMessages(session, producer, 1);

      //::queue
      ClientConsumer consumer1 = session.createConsumer(toFullQN(new SimpleString(""), anycastQ1));
      session.start();

      ClientMessage m = consumer1.receive(2000);
      assertNotNull(m);
      m.acknowledge();

      session.commit();
      consumer1.close();

      try {
         //queue::
         session.createConsumer(toFullQN(anycastQ1, new SimpleString("")));
         fail("should get exception");
      } catch (ActiveMQNonExistentQueueException e) {
         //expected.
      }

      try {
         //::
         session.createConsumer(toFullQN(new SimpleString(""), new SimpleString("")));
         fail("should get exception");
      } catch (ActiveMQNonExistentQueueException e) {
         //expected.
      }
   }

   private SimpleString toFullQN(SimpleString address, SimpleString qName) {
      return address.concat(CompositeAddress.SEPARATOR).concat(qName);
   }
}
