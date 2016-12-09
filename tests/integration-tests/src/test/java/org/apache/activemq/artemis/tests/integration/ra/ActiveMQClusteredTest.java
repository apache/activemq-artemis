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
package org.apache.activemq.artemis.tests.integration.ra;

import javax.jms.QueueConnection;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.ra.ActiveMQRAConnectionFactory;
import org.apache.activemq.artemis.ra.ActiveMQRAConnectionFactoryImpl;
import org.apache.activemq.artemis.ra.ActiveMQRAConnectionManager;
import org.apache.activemq.artemis.ra.ActiveMQRAManagedConnection;
import org.apache.activemq.artemis.ra.ActiveMQRAManagedConnectionFactory;
import org.apache.activemq.artemis.ra.ActiveMQRASession;
import org.apache.activemq.artemis.ra.ActiveMQResourceAdapter;
import org.apache.activemq.artemis.ra.inflow.ActiveMQActivation;
import org.apache.activemq.artemis.ra.inflow.ActiveMQActivationSpec;
import org.junit.Test;

public class ActiveMQClusteredTest extends ActiveMQRAClusteredTestBase {

   /*
   * the second server has no queue so this tests for partial initialisation
   * */
   @Test
   public void testShutdownOnPartialConnect() throws Exception {
      ActiveMQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.setHA(true);
      qResourceAdapter.start(ctx);
      ActiveMQActivationSpec spec = new ActiveMQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setSetupAttempts(0);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setHA(true);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY + "," + INVM_CONNECTOR_FACTORY);
      qResourceAdapter.setConnectionParameters("server-id=0, server-id=1");
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      //make sure thet activation didn't start, i.e. no MDB consumers
      assertEquals(((Queue) server.getPostOffice().getBinding(MDBQUEUEPREFIXEDSIMPLE).getBindable()).getConsumerCount(), 0);
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      qResourceAdapter.stop();
   }

   /**
    * https://bugzilla.redhat.com/show_bug.cgi?id=1029076
    * Look at the logs for this test, if you see exceptions it's an issue.
    *
    * @throws Exception
    */
   @Test
   public void testNonDurableInCluster() throws Exception {
      ActiveMQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      ActiveMQActivationSpec spec = new ActiveMQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Topic");
      spec.setDestination("mdbTopic");
      spec.setSetupAttempts(5);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      ClientSession session = addClientSession(locator.createSessionFactory().createSession());
      ClientProducer clientProducer = session.createProducer("mdbTopic");
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("test");
      clientProducer.send(message);

      ActiveMQActivation activation = lookupActivation(qResourceAdapter);

      SimpleString tempQueue = activation.getTopicTemporaryQueue();

      assertNotNull(server.locateQueue(tempQueue));
      assertNotNull(secondaryServer.locateQueue(tempQueue));

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "test");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();

      assertNull(server.locateQueue(tempQueue));
      assertNull(secondaryServer.locateQueue(tempQueue));

   }

   @Test
   public void testOutboundLoadBalancing() throws Exception {
      final int CONNECTION_COUNT = 100;
      ActiveMQResourceAdapter qResourceAdapter = newResourceAdapter();
      List<Session> sessions = new ArrayList<>();
      List<ActiveMQRAManagedConnection> managedConnections = new ArrayList<>();

      try {
         MyBootstrapContext ctx = new MyBootstrapContext();
         qResourceAdapter.start(ctx);
         ActiveMQRAConnectionManager qraConnectionManager = new ActiveMQRAConnectionManager();
         ActiveMQRAManagedConnectionFactory mcf = new ActiveMQRAManagedConnectionFactory();
         mcf.setResourceAdapter(qResourceAdapter);
         ActiveMQRAConnectionFactory qraConnectionFactory = new ActiveMQRAConnectionFactoryImpl(mcf, qraConnectionManager);

         QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
         Session s = queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         sessions.add(s);
         ActiveMQRAManagedConnection mc = (ActiveMQRAManagedConnection) ((ActiveMQRASession) s).getManagedConnection();
         managedConnections.add(mc);
         ActiveMQConnectionFactory cf1 = mc.getConnectionFactory();

         while (!((ServerLocatorImpl) cf1.getServerLocator()).isReceivedTopology()) {
            Thread.sleep(50);
         }

         for (int i = 0; i < CONNECTION_COUNT; i++) {
            queueConnection = qraConnectionFactory.createQueueConnection();
            s = queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            sessions.add(s);
            mc = (ActiveMQRAManagedConnection) ((ActiveMQRASession) s).getManagedConnection();
            managedConnections.add(mc);
         }

         assertTrue(server.getConnectionCount() >= (CONNECTION_COUNT / 2));
         assertTrue(secondaryServer.getConnectionCount() >= (CONNECTION_COUNT / 2));
      } finally {
         for (Session s : sessions) {
            s.close();
         }

         for (ActiveMQRAManagedConnection mc : managedConnections) {
            mc.destroy();
         }
      }
   }

   @Test
   public void testRebalanceWithInfiniteReconnect() throws Exception {
      testRebalanceInternal(-1);
   }

   @Test
   public void testRebalanceWithNoReconnect() throws Exception {
      testRebalanceInternal(0);
   }

   public void testRebalanceInternal(int reconnectAttempts) throws Exception {
      final int CONSUMER_COUNT = 10;
      secondaryJmsServer.createQueue(true, MDBQUEUE, null, true, "/jms/" + MDBQUEUE);

      ActiveMQResourceAdapter qResourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      ActiveMQActivationSpec spec = new ActiveMQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setRebalanceConnections(true);
      spec.setMaxSession(CONSUMER_COUNT);
      spec.setSetupAttempts(5);
      spec.setSetupInterval(200);
      spec.setReconnectAttempts(reconnectAttempts);
      spec.setHA(true); // if this isn't true then the toplogy listener won't get nodeDown notifications
      spec.setCallTimeout(500L); // if this isn't set then it may take a long time for tearDown to occur on the MDB connection
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);

      Queue primaryQueue = server.locateQueue(MDBQUEUEPREFIXEDSIMPLE);
      Queue secondaryQueue = secondaryServer.locateQueue(MDBQUEUEPREFIXEDSIMPLE);

      assertTrue(primaryQueue.getConsumerCount() < CONSUMER_COUNT);
      assertTrue(secondaryQueue.getConsumerCount() < CONSUMER_COUNT);
      assertTrue(primaryQueue.getConsumerCount() + secondaryQueue.getConsumerCount() == CONSUMER_COUNT);

      ClientSession session = addClientSession(locator.createSessionFactory().createSession());
      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("test");
      clientProducer.send(message);

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "test");

      for (int i = 0; i < 10; i++) {
         secondaryServer.stop();

         long mark = System.currentTimeMillis();
         long timeout = 5000;
         while (primaryQueue.getConsumerCount() < CONSUMER_COUNT && (System.currentTimeMillis() - mark) < timeout) {
            Thread.sleep(100);
         }

         assertTrue(primaryQueue.getConsumerCount() == CONSUMER_COUNT);

         secondaryServer.start();
         waitForServerToStart(secondaryServer);
         secondaryQueue = secondaryServer.locateQueue(MDBQUEUEPREFIXEDSIMPLE);

         mark = System.currentTimeMillis();
         while (((primaryQueue.getConsumerCount() + secondaryQueue.getConsumerCount()) < (CONSUMER_COUNT) || primaryQueue.getConsumerCount() == CONSUMER_COUNT) && (System.currentTimeMillis() - mark) <= timeout) {
            Thread.sleep(100);
         }

         assertTrue(primaryQueue.getConsumerCount() < CONSUMER_COUNT);
         assertTrue(secondaryQueue.getConsumerCount() < CONSUMER_COUNT);
         assertTrue(primaryQueue.getConsumerCount() + secondaryQueue.getConsumerCount() == CONSUMER_COUNT);
      }

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();
   }
}
