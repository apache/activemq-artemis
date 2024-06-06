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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullQualifiedQueueTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private SimpleString anycastAddress = SimpleString.of("address.anycast");
   private SimpleString multicastAddress = SimpleString.of("address.multicast");
   private SimpleString mixedAddress = SimpleString.of("address.mixed");

   private SimpleString anycastQ1 = SimpleString.of("q1");
   private SimpleString anycastQ2 = SimpleString.of("q2");
   private SimpleString anycastQ3 = SimpleString.of("q3");

   private SimpleString multicastQ1 = SimpleString.of("q4");
   private SimpleString multicastQ2 = SimpleString.of("q5");
   private SimpleString multicastQ3 = SimpleString.of("q6");

   private ActiveMQServer server;
   private ServerLocator locator;

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false, true);

      server.start();
      locator = createNettyNonHALocator();
   }

   @Test
   public void testMixedQueues() throws Exception {
      server.createQueue(new QueueConfiguration(multicastQ1).setAddress(mixedAddress));
      server.createQueue(new QueueConfiguration(multicastQ2).setAddress(mixedAddress));
      server.createQueue(new QueueConfiguration(multicastQ3).setAddress(mixedAddress));
      server.createQueue(new QueueConfiguration(anycastQ1).setAddress(mixedAddress).setRoutingType(RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(anycastQ2).setAddress(mixedAddress).setRoutingType(RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(anycastQ3).setAddress(mixedAddress).setRoutingType(RoutingType.ANYCAST));

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession();
      session.start();

      //send 3 messages
      ClientProducer producer = session.createProducer(mixedAddress);
      final int num = 3;
      sendMessages(session, producer, num);

      ClientConsumer consumer1 = session.createConsumer(CompositeAddress.toFullyQualified(mixedAddress, anycastQ1));
      ClientConsumer consumer2 = session.createConsumer(CompositeAddress.toFullyQualified(mixedAddress, anycastQ2));
      ClientConsumer consumer3 = session.createConsumer(CompositeAddress.toFullyQualified(mixedAddress, anycastQ3));
      ClientConsumer consumer4 = session.createConsumer(CompositeAddress.toFullyQualified(mixedAddress, multicastQ1));
      ClientConsumer consumer5 = session.createConsumer(CompositeAddress.toFullyQualified(mixedAddress, multicastQ2));
      ClientConsumer consumer6 = session.createConsumer(CompositeAddress.toFullyQualified(mixedAddress, multicastQ3));

      session.start();

      //each anycast consumer receives one, each multicast receives three.
      ClientMessage m = consumer1.receive(2000);
      assertNotNull(m);
      logger.debug("consumer1 : {}", m);
      m.acknowledge();

      m = consumer2.receive(2000);
      assertNotNull(m);
      logger.debug("consumer2 : {}", m);
      m.acknowledge();

      m = consumer3.receive(2000);
      assertNotNull(m);
      logger.debug("consumer3 : {}", m);
      m.acknowledge();

      for (int i = 0; i < num; i++) {
         m = consumer4.receive(2000);
         assertNotNull(m);
         logger.debug("consumer4 : {}", m);
         m.acknowledge();
         m = consumer5.receive(2000);
         assertNotNull(m);
         logger.debug("consumer5 : {}", m);
         m.acknowledge();
         m = consumer6.receive(2000);
         assertNotNull(m);
         logger.debug("consumer6 : {}", m);
         m.acknowledge();
      }

      session.commit();

      //queues are empty now
      for (SimpleString q : new SimpleString[]{anycastQ1, anycastQ2, anycastQ3, multicastQ1, multicastQ2, multicastQ3}) {
         QueueQueryResult query = server.queueQuery(CompositeAddress.toFullyQualified(mixedAddress, q));
         assertTrue(query.isExists());
         assertEquals(mixedAddress, query.getAddress());
         assertEquals(q, query.getName());
         assertEquals(0, query.getMessageCount());
      }
   }

   @Test
   public void testMulticastQueues() throws Exception {
      server.createQueue(new QueueConfiguration(multicastQ1).setAddress(multicastAddress));
      server.createQueue(new QueueConfiguration(multicastQ2).setAddress(multicastAddress));
      server.createQueue(new QueueConfiguration(multicastQ3).setAddress(multicastAddress));

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession();
      session.start();

      //send 3 messages
      ClientProducer producer = session.createProducer(multicastAddress);
      sendMessages(session, producer, 1);

      ClientConsumer consumer1 = session.createConsumer(CompositeAddress.toFullyQualified(multicastAddress, multicastQ1));
      ClientConsumer consumer2 = session.createConsumer(CompositeAddress.toFullyQualified(multicastAddress, multicastQ2));
      ClientConsumer consumer3 = session.createConsumer(CompositeAddress.toFullyQualified(multicastAddress, multicastQ3));
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
         QueueQueryResult query = server.queueQuery(CompositeAddress.toFullyQualified(multicastAddress, q));
         assertTrue(query.isExists());
         assertEquals(multicastAddress, query.getAddress());
         assertEquals(q, query.getName());
         assertEquals(0, query.getMessageCount());
      }
   }

   @Test
   public void testAnycastQueues() throws Exception {
      server.createQueue(new QueueConfiguration(anycastQ1).setAddress(anycastAddress).setRoutingType(RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(anycastQ2).setAddress(anycastAddress).setRoutingType(RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(anycastQ3).setAddress(anycastAddress).setRoutingType(RoutingType.ANYCAST));

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession();
      session.start();

      ClientProducer producer1 = session.createProducer(CompositeAddress.toFullyQualified(anycastAddress, anycastQ1).toString());
      for (int i = 0; i < 2; i++) {
         producer1.send(session.createMessage(false));
      }
      assertTrue(org.apache.activemq.artemis.tests.util.Wait.waitFor(() -> server.locateQueue(anycastQ1).getMessageCount() == 2, 2000, 200));

      ClientProducer producer2 = session.createProducer(CompositeAddress.toFullyQualified(anycastAddress, anycastQ2).toString());
      for (int i = 0; i < 3; i++) {
         producer2.send(session.createMessage(false));
      }
      assertTrue(org.apache.activemq.artemis.tests.util.Wait.waitFor(() -> server.locateQueue(anycastQ2).getMessageCount() == 3, 2000, 200));

      ClientProducer producer3 = session.createProducer(CompositeAddress.toFullyQualified(anycastAddress, anycastQ3).toString());
      for (int i = 0; i < 5; i++) {
         producer3.send(session.createMessage(false));
      }
      assertTrue(org.apache.activemq.artemis.tests.util.Wait.waitFor(() -> server.locateQueue(anycastQ3).getMessageCount() == 5, 2000, 200));

      ClientConsumer consumer1 = session.createConsumer(CompositeAddress.toFullyQualified(anycastAddress, anycastQ1));
      ClientConsumer consumer2 = session.createConsumer(CompositeAddress.toFullyQualified(anycastAddress, anycastQ2));
      ClientConsumer consumer3 = session.createConsumer(CompositeAddress.toFullyQualified(anycastAddress, anycastQ3));

      ClientMessage m = null;

      for (int i = 0; i < 2; i++) {
         m = consumer1.receive(2000);
         assertNotNull(m);
         m.acknowledge();
      }
      for (int i = 0; i < 3; i++) {
         m = consumer2.receive(2000);
         assertNotNull(m);
         m.acknowledge();
      }
      for (int i = 0; i < 5; i++) {
         m = consumer3.receive(2000);
         assertNotNull(m);
         m.acknowledge();
      }
      session.commit();

      //queues are empty now
      for (SimpleString q : new SimpleString[]{anycastQ1, anycastQ2, anycastQ3}) {
         QueueQueryResult query = server.queueQuery(CompositeAddress.toFullyQualified(anycastAddress, q));
         assertTrue(query.isExists());
         assertEquals(anycastAddress, query.getAddress());
         assertEquals(q, query.getName());
         assertEquals(0, query.getMessageCount());
      }
   }

   @Test
   public void testSpecialCase() throws Exception {
      server.createQueue(new QueueConfiguration(anycastQ1).setAddress(anycastAddress).setRoutingType(RoutingType.ANYCAST));

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession();
      session.start();

      ClientProducer producer = session.createProducer(anycastAddress);
      sendMessages(session, producer, 1);

      //::queue
      ClientConsumer consumer1 = session.createConsumer(CompositeAddress.toFullyQualified(SimpleString.of(""), anycastQ1));
      session.start();

      ClientMessage m = consumer1.receive(2000);
      assertNotNull(m);
      m.acknowledge();

      session.commit();
      consumer1.close();

      try {
         //queue::
         session.createConsumer(CompositeAddress.toFullyQualified(anycastQ1, SimpleString.of("")));
         fail("should get exception");
      } catch (ActiveMQNonExistentQueueException e) {
         //expected.
      }

      try {
         //::
         session.createConsumer(CompositeAddress.toFullyQualified(SimpleString.of(""), SimpleString.of("")));
         fail("should get exception");
      } catch (ActiveMQNonExistentQueueException e) {
         //expected.
      }
   }
}
