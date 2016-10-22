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
package org.apache.activemq.artemis.tests.integration.addressing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQQueueMaxConsumerLimitReached;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class AddressingTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private ClientSessionFactory sessionFactory;

   @Before
   public void setup() throws Exception {
      server = createServer(true);
      server.start();

      server.waitForActivation(10, TimeUnit.SECONDS);

      ServerLocator sl = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      sessionFactory = sl.createSessionFactory();

      addSessionFactory(sessionFactory);
   }

   @Test
   public void testMulticastRouting() throws Exception {

      SimpleString sendAddress = new SimpleString("test.address");

      List<String> testAddresses = Arrays.asList("test.address", "test.#", "test.*");

      for (String consumeAddress : testAddresses) {

         // For each address, create 2 Queues with the same address, assert both queues receive message

         AddressInfo addressInfo = new AddressInfo(new SimpleString(consumeAddress));
         addressInfo.setRoutingType(AddressInfo.RoutingType.MULTICAST);

         server.createOrUpdateAddressInfo(addressInfo);
         Queue q1 = server.createQueue(new SimpleString(consumeAddress), new SimpleString(consumeAddress + ".1"), null, true, false);
         Queue q2 = server.createQueue(new SimpleString(consumeAddress), new SimpleString(consumeAddress + ".2"), null, true, false);

         ClientSession session = sessionFactory.createSession();
         session.start();

         ClientConsumer consumer1 = session.createConsumer(q1.getName());
         ClientConsumer consumer2 = session.createConsumer(q2.getName());

         ClientProducer producer = session.createProducer(sendAddress);
         ClientMessage m = session.createMessage(ClientMessage.TEXT_TYPE, true);
         m.getBodyBuffer().writeString("TestMessage");

         producer.send(m);

         assertNotNull(consumer1.receive(2000));
         assertNotNull(consumer2.receive(2000));

         q1.deleteQueue();
         q2.deleteQueue();
      }
   }

   @Test
   public void testAnycastRouting() throws Exception {

      SimpleString sendAddress = new SimpleString("test.address");

      List<String> testAddresses = Arrays.asList("test.address", "test.#", "test.*");

      for (String consumeAddress : testAddresses) {

         // For each address, create 2 Queues with the same address, assert one queue receive message

         AddressInfo addressInfo = new AddressInfo(new SimpleString(consumeAddress));
         addressInfo.setRoutingType(AddressInfo.RoutingType.ANYCAST);

         server.createOrUpdateAddressInfo(addressInfo);
         Queue q1 = server.createQueue(new SimpleString(consumeAddress), new SimpleString(consumeAddress + ".1"), null, true, false);
         Queue q2 = server.createQueue(new SimpleString(consumeAddress), new SimpleString(consumeAddress + ".2"), null, true, false);

         ClientSession session = sessionFactory.createSession();
         session.start();

         ClientConsumer consumer1 = session.createConsumer(q1.getName());
         ClientConsumer consumer2 = session.createConsumer(q2.getName());

         ClientProducer producer = session.createProducer(sendAddress);
         ClientMessage m = session.createMessage(ClientMessage.TEXT_TYPE, true);

         m.getBodyBuffer().writeString("TestMessage");

         producer.send(m);

         int count = 0;
         count = (consumer1.receive(1000) == null) ? count : count + 1;
         count = (consumer2.receive(1000) == null) ? count : count + 1;
         assertEquals(1, count);

         q1.deleteQueue();
         q2.deleteQueue();
      }
   }

   @Test
   public void testAnycastRoutingRoundRobin() throws Exception {

      SimpleString address = new SimpleString("test.address");
      AddressInfo addressInfo = new AddressInfo(address);
      addressInfo.setRoutingType(AddressInfo.RoutingType.ANYCAST);

      server.createOrUpdateAddressInfo(addressInfo);
      Queue q1 = server.createQueue(address, address.concat(".1"), null, true, false);
      Queue q2 = server.createQueue(address, address.concat(".2"), null, true, false);
      Queue q3 = server.createQueue(address, address.concat(".3"), null, true, false);

      ClientSession session = sessionFactory.createSession();
      session.start();

      ClientProducer producer = session.createProducer(address);

      ClientConsumer consumer1 = session.createConsumer(q1.getName());
      ClientConsumer consumer2 = session.createConsumer(q2.getName());
      ClientConsumer consumer3 = session.createConsumer(q3.getName());
      List<ClientConsumer> consumers = new ArrayList<>(Arrays.asList(new ClientConsumer[]{consumer1, consumer2, consumer3}));

      List<String> messages = new ArrayList<>();
      messages.add("Message1");
      messages.add("Message2");
      messages.add("Message3");

      ClientMessage clientMessage;
      for (String message : messages) {
         clientMessage = session.createMessage(true);
         clientMessage.getBodyBuffer().writeString(message);
         producer.send(clientMessage);
      }

      String m;
      for (ClientConsumer consumer : consumers) {
         clientMessage = consumer.receive(1000);
         m = clientMessage.getBodyBuffer().readString();
         messages.remove(m);
      }

      assertTrue(messages.isEmpty());

      // Check we don't receive more messages
      int count = 0;
      for (ClientConsumer consumer : consumers) {
         count = (consumer.receive(1000) == null) ? count : count + 1;
      }
      assertEquals(0, count);
   }

   @Test
   public void testMulticastRoutingBackwardsCompat() throws Exception {

      SimpleString sendAddress = new SimpleString("test.address");

      List<String> testAddresses = Arrays.asList("test.address", "test.#", "test.*");

      for (String consumeAddress : testAddresses) {

         // For each address, create 2 Queues with the same address, assert both queues receive message
         Queue q1 = server.createQueue(new SimpleString(consumeAddress), new SimpleString(consumeAddress + ".1"), null, true, false);
         Queue q2 = server.createQueue(new SimpleString(consumeAddress), new SimpleString(consumeAddress + ".2"), null, true, false);

         ClientSession session = sessionFactory.createSession();
         session.start();

         ClientConsumer consumer1 = session.createConsumer(q1.getName());
         ClientConsumer consumer2 = session.createConsumer(q2.getName());

         ClientProducer producer = session.createProducer(sendAddress);
         ClientMessage m = session.createMessage(ClientMessage.TEXT_TYPE, true);
         m.getBodyBuffer().writeString("TestMessage");

         producer.send(m);

         assertNotNull(consumer1.receive(2000));
         assertNotNull(consumer2.receive(2000));

         q1.deleteQueue();
         q2.deleteQueue();
      }
   }

   @Test
   public void testDeleteQueueOnNoConsumersTrue() throws Exception {

      SimpleString address = new SimpleString("test.address");
      SimpleString queueName = SimpleString.toSimpleString(UUID.randomUUID().toString());
      // For each address, create 2 Queues with the same address, assert both queues receive message
      boolean deleteOnNoConsumers = true;
      Queue q1 = server.createQueue(address, queueName, null, true, false, null, deleteOnNoConsumers);

      ClientSession session = sessionFactory.createSession();
      session.start();

      ClientConsumer consumer1 = session.createConsumer(q1.getName());
      consumer1.close();

      assertFalse(server.queueQuery(queueName).isExists());
   }

   @Test
   public void testDeleteQueueOnNoConsumersFalse() throws Exception {
      SimpleString address = new SimpleString("test.address");
      SimpleString queueName = SimpleString.toSimpleString(UUID.randomUUID().toString());
      // For each address, create 2 Queues with the same address, assert both queues receive message
      boolean deleteOnNoConsumers = false;
      Queue q1 = server.createQueue(address, queueName, null, true, false, null, deleteOnNoConsumers);

      ClientSession session = sessionFactory.createSession();
      session.start();

      ClientConsumer consumer1 = session.createConsumer(q1.getName());
      consumer1.close();

      assertTrue(server.queueQuery(queueName).isExists());
   }

   @Test
   public void testLimitOnMaxConsumers() throws Exception {
      SimpleString address = new SimpleString("test.address");
      SimpleString queueName = SimpleString.toSimpleString(UUID.randomUUID().toString());
      // For each address, create 2 Queues with the same address, assert both queues receive message
      boolean deleteOnNoConsumers = false;
      Queue q1 = server.createQueue(address, queueName, null, true, false, 0, deleteOnNoConsumers);

      Exception expectedException = null;
      String expectedMessage = "Maximum Consumer Limit Reached on Queue";
      try {
         ClientSession session = sessionFactory.createSession();
         session.start();

         session.createConsumer(q1.getName());
      } catch (ActiveMQQueueMaxConsumerLimitReached e) {
         expectedException = e;
      }

      assertNotNull(expectedException);
      assertTrue(expectedException.getMessage().contains(expectedMessage));
      assertTrue(expectedException.getMessage().contains(address));
      assertTrue(expectedException.getMessage().contains(queueName));
   }

   @Ignore
   @Test
   public void testUnlimitedMaxConsumers() throws Exception {
      int noConsumers = 50;
      SimpleString address = new SimpleString("test.address");
      SimpleString queueName = SimpleString.toSimpleString(UUID.randomUUID().toString());
      // For each address, create 2 Queues with the same address, assert both queues receive message
      boolean deleteOnNoConsumers = false;
      Queue q1 = server.createQueue(address, queueName, null, true, false, -1, deleteOnNoConsumers);

      ClientSession session = sessionFactory.createSession();
      session.start();

      for (int i = 0; i < noConsumers; i++) {
         session.createConsumer(q1.getName());
      }
   }

   @Ignore
   @Test
   public void testDefaultMaxConsumersFromAddress() throws Exception {
      int noConsumers = 50;
      SimpleString address = new SimpleString("test.address");
      SimpleString queueName = SimpleString.toSimpleString(UUID.randomUUID().toString());
      // For each address, create 2 Queues with the same address, assert both queues receive message
      boolean deleteOnNoConsumers = false;
      AddressInfo addressInfo = new AddressInfo(address);
      addressInfo.setDefaultMaxQueueConsumers(0);
      Queue q1 = server.createQueue(address, queueName, null, true, false, null, deleteOnNoConsumers);

      ClientSession session = sessionFactory.createSession();
      session.start();

      for (int i = 0; i < noConsumers; i++) {
         session.createConsumer(q1.getName());
      }
   }

   @Ignore
   @Test
   public void testDefaultDeleteOnNoConsumersFromAddress() {
      fail("Not Implemented");
   }
}
