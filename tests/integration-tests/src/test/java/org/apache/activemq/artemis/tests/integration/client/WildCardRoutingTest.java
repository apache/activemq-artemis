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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WildCardRoutingTest extends ActiveMQTestBase {

   private ActiveMQServer server;
   private ServerLocator locator;
   private ClientSession clientSession;
   private ClientSessionFactory sf;

   @Test
   public void testBasicWildcardRouting() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testBasicWildcardRoutingQueuesDontExist() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();
      clientSession.deleteQueue(queueName);

      Assert.assertEquals(0, server.getPostOffice().getBindingsForAddress(addressAB).getBindings().size());
      Assert.assertEquals(0, server.getPostOffice().getBindingsForAddress(addressAC).getBindings().size());
      Assert.assertEquals(0, server.getPostOffice().getBindingsForAddress(address).getBindings().size());
   }

   @Test
   public void testBasicWildcardRoutingQueuesDontExist2() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName = new SimpleString("Q");
      SimpleString queueName2 = new SimpleString("Q2");
      clientSession.createQueue(address, queueName, null, false);
      clientSession.createQueue(address, queueName2, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();
      clientSession.deleteQueue(queueName);

      Assert.assertEquals(1, server.getPostOffice().getBindingsForAddress(addressAB).getBindings().size());
      Assert.assertEquals(1, server.getPostOffice().getBindingsForAddress(addressAC).getBindings().size());
      Assert.assertEquals(1, server.getPostOffice().getBindingsForAddress(address).getBindings().size());

      clientSession.deleteQueue(queueName2);

      Assert.assertEquals(0, server.getPostOffice().getBindingsForAddress(addressAB).getBindings().size());
      Assert.assertEquals(0, server.getPostOffice().getBindingsForAddress(addressAC).getBindings().size());
      Assert.assertEquals(0, server.getPostOffice().getBindingsForAddress(address).getBindings().size());
   }

   @Test
   public void testBasicWildcardRoutingWithHash() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testWildcardRoutingQueuesAddedAfter() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testWildcardRoutingQueuesAddedThenDeleted() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      clientSession.deleteQueue(queueName1);
      // the wildcard binding should still exist
      Assert.assertEquals(server.getPostOffice().getBindingsForAddress(addressAB).getBindings().size(), 1);
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      clientConsumer.close();
      clientSession.deleteQueue(queueName);
      Assert.assertEquals(server.getPostOffice().getBindingsForAddress(addressAB).getBindings().size(), 0);
   }

   @Test
   public void testWildcardRoutingLotsOfQueuesAddedThenDeleted() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString addressAD = new SimpleString("a.d");
      SimpleString addressAE = new SimpleString("a.e");
      SimpleString addressAF = new SimpleString("a.f");
      SimpleString addressAG = new SimpleString("a.g");
      SimpleString addressAH = new SimpleString("a.h");
      SimpleString addressAJ = new SimpleString("a.j");
      SimpleString addressAK = new SimpleString("a.k");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName3 = new SimpleString("Q3");
      SimpleString queueName4 = new SimpleString("Q4");
      SimpleString queueName5 = new SimpleString("Q5");
      SimpleString queueName6 = new SimpleString("Q6");
      SimpleString queueName7 = new SimpleString("Q7");
      SimpleString queueName8 = new SimpleString("Q8");
      SimpleString queueName9 = new SimpleString("Q9");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(addressAD, queueName3, null, false);
      clientSession.createQueue(addressAE, queueName4, null, false);
      clientSession.createQueue(addressAF, queueName5, null, false);
      clientSession.createQueue(addressAG, queueName6, null, false);
      clientSession.createQueue(addressAH, queueName7, null, false);
      clientSession.createQueue(addressAJ, queueName8, null, false);
      clientSession.createQueue(addressAK, queueName9, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer();
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(addressAB, createTextMessage(clientSession, "m1"));
      producer.send(addressAC, createTextMessage(clientSession, "m2"));
      producer.send(addressAD, createTextMessage(clientSession, "m3"));
      producer.send(addressAE, createTextMessage(clientSession, "m4"));
      producer.send(addressAF, createTextMessage(clientSession, "m5"));
      producer.send(addressAG, createTextMessage(clientSession, "m6"));
      producer.send(addressAH, createTextMessage(clientSession, "m7"));
      producer.send(addressAJ, createTextMessage(clientSession, "m8"));
      producer.send(addressAK, createTextMessage(clientSession, "m9"));

      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m3", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m4", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m5", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m6", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m7", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m8", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m9", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      // now remove all the queues
      clientSession.deleteQueue(queueName1);
      clientSession.deleteQueue(queueName2);
      clientSession.deleteQueue(queueName3);
      clientSession.deleteQueue(queueName4);
      clientSession.deleteQueue(queueName5);
      clientSession.deleteQueue(queueName6);
      clientSession.deleteQueue(queueName7);
      clientSession.deleteQueue(queueName8);
      clientSession.deleteQueue(queueName9);
      clientConsumer.close();
      clientSession.deleteQueue(queueName);
   }

   @Test
   public void testWildcardRoutingLotsOfQueuesAddedThenDeletedHash() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString addressAD = new SimpleString("a.d");
      SimpleString addressAE = new SimpleString("a.e");
      SimpleString addressAF = new SimpleString("a.f");
      SimpleString addressAG = new SimpleString("a.g");
      SimpleString addressAH = new SimpleString("a.h");
      SimpleString addressAJ = new SimpleString("a.j");
      SimpleString addressAK = new SimpleString("a.k");
      SimpleString address = new SimpleString("#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName3 = new SimpleString("Q3");
      SimpleString queueName4 = new SimpleString("Q4");
      SimpleString queueName5 = new SimpleString("Q5");
      SimpleString queueName6 = new SimpleString("Q6");
      SimpleString queueName7 = new SimpleString("Q7");
      SimpleString queueName8 = new SimpleString("Q8");
      SimpleString queueName9 = new SimpleString("Q9");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(addressAD, queueName3, null, false);
      clientSession.createQueue(addressAE, queueName4, null, false);
      clientSession.createQueue(addressAF, queueName5, null, false);
      clientSession.createQueue(addressAG, queueName6, null, false);
      clientSession.createQueue(addressAH, queueName7, null, false);
      clientSession.createQueue(addressAJ, queueName8, null, false);
      clientSession.createQueue(addressAK, queueName9, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer();
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(addressAB, createTextMessage(clientSession, "m1"));
      producer.send(addressAC, createTextMessage(clientSession, "m2"));
      producer.send(addressAD, createTextMessage(clientSession, "m3"));
      producer.send(addressAE, createTextMessage(clientSession, "m4"));
      producer.send(addressAF, createTextMessage(clientSession, "m5"));
      producer.send(addressAG, createTextMessage(clientSession, "m6"));
      producer.send(addressAH, createTextMessage(clientSession, "m7"));
      producer.send(addressAJ, createTextMessage(clientSession, "m8"));
      producer.send(addressAK, createTextMessage(clientSession, "m9"));

      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m3", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m4", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m5", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m6", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m7", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m8", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m9", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      // now remove all the queues
      clientSession.deleteQueue(queueName1);
      clientSession.deleteQueue(queueName2);
      clientSession.deleteQueue(queueName3);
      clientSession.deleteQueue(queueName4);
      clientSession.deleteQueue(queueName5);
      clientSession.deleteQueue(queueName6);
      clientSession.deleteQueue(queueName7);
      clientSession.deleteQueue(queueName8);
      clientSession.deleteQueue(queueName9);
      clientConsumer.close();
      clientSession.deleteQueue(queueName);
   }

   @Test
   public void testWildcardRoutingWithSingleHash() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testWildcardRoutingWithHash() throws Exception {
      SimpleString addressAB = new SimpleString("a.b.f");
      SimpleString addressAC = new SimpleString("a.c.f");
      SimpleString address = new SimpleString("a.#.f");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testWildcardRoutingWithHashMultiLengthAddresses() throws Exception {
      SimpleString addressAB = new SimpleString("a.b.c.f");
      SimpleString addressAC = new SimpleString("a.c.f");
      SimpleString address = new SimpleString("a.#.f");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testWildcardRoutingWithDoubleStar() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("*.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testWildcardRoutingPartialMatchStar() throws Exception {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("*.b");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testWildcardRoutingVariableLengths() throws Exception {
      SimpleString addressAB = new SimpleString("a.b.c");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
   }

   @Test
   public void testWildcardRoutingVariableLengthsStar() throws Exception {
      SimpleString addressAB = new SimpleString("a.b.c");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testWildcardRoutingMultipleStars() throws Exception {
      SimpleString addressAB = new SimpleString("a.b.c");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("*.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testWildcardRoutingStarInMiddle() throws Exception {
      SimpleString addressAB = new SimpleString("a.b.c");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("*.b.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testWildcardRoutingStarAndHash() throws Exception {
      SimpleString addressAB = new SimpleString("a.b.c.d");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("*.b.#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testWildcardRoutingHashAndStar() throws Exception {
      SimpleString addressAB = new SimpleString("a.b.c");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("#.b.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

   @Test
   public void testLargeWildcardRouting() throws Exception {
      SimpleString addressAB = new SimpleString("a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      Assert.assertEquals(2, server.getPostOffice().getBindingsForAddress(addressAB).getBindings().size());
      Assert.assertEquals(2, server.getPostOffice().getBindingsForAddress(addressAC).getBindings().size());
      Assert.assertEquals(1, server.getPostOffice().getBindingsForAddress(address).getBindings().size());
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();
      clientSession.deleteQueue(queueName);
      Assert.assertEquals(1, server.getPostOffice().getBindingsForAddress(addressAB).getBindings().size());
      Assert.assertEquals(1, server.getPostOffice().getBindingsForAddress(addressAC).getBindings().size());
      Assert.assertEquals(0, server.getPostOffice().getBindingsForAddress(address).getBindings().size());
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      Configuration configuration = createDefaultInVMConfig().setWildcardRoutingEnabled(true).setTransactionTimeoutScanPeriod(500);
      server = addServer(ActiveMQServers.newActiveMQServer(configuration, false));
      server.start();
      server.getManagementService().enableNotifications(false);
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      clientSession = addClientSession(sf.createSession(false, true, true));
   }
}
