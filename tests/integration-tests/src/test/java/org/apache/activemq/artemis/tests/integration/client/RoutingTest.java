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

import java.util.EnumSet;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RoutingTest extends ActiveMQTestBase {

   public final SimpleString addressA = new SimpleString("addressA");
   public final SimpleString addressB = new SimpleString("addressB");
   public final SimpleString queueA = new SimpleString("queueA");
   public final SimpleString queueB = new SimpleString("queueB");
   public final SimpleString queueC = new SimpleString("queueC");
   public final SimpleString queueD = new SimpleString("queueD");

   private ServerLocator locator;
   private ActiveMQServer server;
   private ClientSessionFactory cf;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      locator = createInVMNonHALocator();
      server = createServer(false);

      server.start();
      cf = createSessionFactory(locator);
   }

   @Test
   public void testRouteToMultipleQueues() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, false);
      sendSession.createQueue(addressA, queueB, false);
      sendSession.createQueue(addressA, queueC, false);
      int numMessages = 300;
      ClientProducer p = sendSession.createProducer(addressA);
      for (int i = 0; i < numMessages; i++) {
         p.send(sendSession.createMessage(false));
      }
      ClientSession session = cf.createSession(false, true, true);
      ClientConsumer c1 = session.createConsumer(queueA);
      ClientConsumer c2 = session.createConsumer(queueB);
      ClientConsumer c3 = session.createConsumer(queueC);
      session.start();
      for (int i = 0; i < numMessages; i++) {
         ClientMessage m = c1.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         c2.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         c3.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
      }
      Assert.assertNull(c1.receiveImmediate());
      Assert.assertNull(c2.receiveImmediate());
      Assert.assertNull(c3.receiveImmediate());
      sendSession.close();
      session.close();
   }

   @Test
   public void testRouteToSingleNonDurableQueue() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, false);
      int numMessages = 300;
      ClientProducer p = sendSession.createProducer(addressA);
      for (int i = 0; i < numMessages; i++) {
         p.send(sendSession.createMessage(false));
      }
      ClientSession session = cf.createSession(false, true, true);
      ClientConsumer c1 = session.createConsumer(queueA);
      session.start();
      for (int i = 0; i < numMessages; i++) {
         ClientMessage m = c1.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
      }
      Assert.assertNull(c1.receiveImmediate());
      sendSession.close();
      session.close();
   }

   @Test
   public void testRouteToSingleDurableQueue() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, true);
      int numMessages = 300;
      ClientProducer p = sendSession.createProducer(addressA);
      for (int i = 0; i < numMessages; i++) {
         p.send(sendSession.createMessage(false));
      }
      ClientSession session = cf.createSession(false, true, true);
      ClientConsumer c1 = session.createConsumer(queueA);
      session.start();
      for (int i = 0; i < numMessages; i++) {
         ClientMessage m = c1.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
      }
      Assert.assertNull(c1.receiveImmediate());
      sendSession.close();
      session.close();
   }

   @Test
   public void testRouteToSingleQueueWithFilter() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, new SimpleString("foo = 'bar'"), false);
      int numMessages = 300;
      ClientProducer p = sendSession.createProducer(addressA);
      for (int i = 0; i < numMessages; i++) {
         ClientMessage clientMessage = sendSession.createMessage(false);
         clientMessage.putStringProperty(new SimpleString("foo"), new SimpleString("bar"));
         p.send(clientMessage);
      }
      ClientSession session = cf.createSession(false, true, true);
      ClientConsumer c1 = session.createConsumer(queueA);
      session.start();
      for (int i = 0; i < numMessages; i++) {
         ClientMessage m = c1.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
      }
      Assert.assertNull(c1.receiveImmediate());
      sendSession.close();
      session.close();
   }

   @Test
   public void testRouteToMultipleQueueWithFilters() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      sendSession.createQueue(addressA, queueA, new SimpleString("foo = 'bar'"), false);
      sendSession.createQueue(addressA, queueB, new SimpleString("x = 1"), false);
      sendSession.createQueue(addressA, queueC, new SimpleString("b = false"), false);
      int numMessages = 300;
      ClientProducer p = sendSession.createProducer(addressA);
      for (int i = 0; i < numMessages; i++) {
         ClientMessage clientMessage = sendSession.createMessage(false);
         if (i % 3 == 0) {
            clientMessage.putStringProperty(new SimpleString("foo"), new SimpleString("bar"));
         } else if (i % 3 == 1) {
            clientMessage.putIntProperty(new SimpleString("x"), 1);
         } else {
            clientMessage.putBooleanProperty(new SimpleString("b"), false);
         }
         p.send(clientMessage);
      }
      ClientSession session = cf.createSession(false, true, true);
      ClientConsumer c1 = session.createConsumer(queueA);
      ClientConsumer c2 = session.createConsumer(queueB);
      ClientConsumer c3 = session.createConsumer(queueC);
      session.start();
      for (int i = 0; i < numMessages / 3; i++) {
         ClientMessage m = c1.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = c2.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
         m = c3.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
      }
      Assert.assertNull(c1.receiveImmediate());
      Assert.assertNull(c2.receiveImmediate());
      Assert.assertNull(c3.receiveImmediate());
      sendSession.close();
      session.close();
   }

   @Test
   public void testRouteToSingleTemporaryQueue() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      sendSession.createTemporaryQueue(addressA, queueA);
      int numMessages = 300;
      ClientProducer p = sendSession.createProducer(addressA);
      for (int i = 0; i < numMessages; i++) {
         p.send(sendSession.createMessage(false));
      }
      ClientSession session = cf.createSession(false, true, true);
      ClientConsumer c1 = session.createConsumer(queueA);
      session.start();
      for (int i = 0; i < numMessages; i++) {
         ClientMessage m = c1.receive(5000);
         Assert.assertNotNull(m);
         m.acknowledge();
      }
      Assert.assertNull(c1.receiveImmediate());
      sendSession.close();
      session.close();
   }

   @Test
   public void testAnycastMessageRoutingExclusivity() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      EnumSet<RoutingType> routingTypes = EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST);
      sendSession.createAddress(addressA, routingTypes, false);
      sendSession.createQueue(addressA, RoutingType.ANYCAST, queueA);
      sendSession.createQueue(addressA, RoutingType.ANYCAST, queueB);
      sendSession.createQueue(addressA, RoutingType.MULTICAST, queueC);
      ClientProducer p = sendSession.createProducer(addressA);
      ClientMessage message = sendSession.createMessage(false);
      message.setRoutingType(RoutingType.ANYCAST);
      p.send(message);
      sendSession.close();
      assertTrue(Wait.waitFor(() -> server.locateQueue(queueA).getMessageCount() + server.locateQueue(queueB).getMessageCount() == 1));
      assertTrue(Wait.waitFor(() -> server.locateQueue(queueC).getMessageCount() == 0));
   }

   @Test
   public void testMulticastMessageRoutingExclusivity() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      EnumSet<RoutingType> routingTypes = EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST);
      sendSession.createAddress(addressA, routingTypes, false);
      sendSession.createQueue(addressA, RoutingType.ANYCAST, queueA);
      sendSession.createQueue(addressA, RoutingType.MULTICAST, queueB);
      sendSession.createQueue(addressA, RoutingType.MULTICAST, queueC);
      ClientProducer p = sendSession.createProducer(addressA);
      ClientMessage message = sendSession.createMessage(false);
      message.setRoutingType(RoutingType.MULTICAST);
      p.send(message);
      sendSession.close();
      assertTrue(Wait.waitFor(() -> server.locateQueue(queueA).getMessageCount() == 0));
      assertTrue(Wait.waitFor(() -> server.locateQueue(queueB).getMessageCount() + server.locateQueue(queueC).getMessageCount() == 2));
   }

   @Test
   public void testAmbiguousMessageRouting() throws Exception {
      ClientSession sendSession = cf.createSession(false, true, true);
      EnumSet<RoutingType> routingTypes = EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST);
      sendSession.createAddress(addressA, routingTypes, false);
      sendSession.createQueue(addressA, RoutingType.ANYCAST, queueA);
      sendSession.createQueue(addressA, RoutingType.ANYCAST, queueB);
      sendSession.createQueue(addressA, RoutingType.MULTICAST, queueC);
      sendSession.createQueue(addressA, RoutingType.MULTICAST, queueD);
      ClientProducer p = sendSession.createProducer(addressA);
      ClientMessage message = sendSession.createMessage(false);
      p.send(message);
      sendSession.close();

      assertTrue(Wait.waitFor(() -> server.locateQueue(queueA).getMessageCount() + server.locateQueue(queueB).getMessageCount() == 1));
      assertTrue(Wait.waitFor(() -> server.locateQueue(queueC).getMessageCount() + server.locateQueue(queueD).getMessageCount() == 2));
   }
}
