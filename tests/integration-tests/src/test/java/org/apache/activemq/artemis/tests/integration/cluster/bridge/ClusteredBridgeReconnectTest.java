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
package org.apache.activemq.artemis.tests.integration.cluster.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.client.impl.TopologyMemberImpl;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.server.cluster.MessageFlowRecord;
import org.apache.activemq.artemis.core.server.cluster.RemoteQueueBinding;
import org.apache.activemq.artemis.core.server.cluster.impl.BridgeTestAccessor;
import org.apache.activemq.artemis.core.server.cluster.impl.ClusterConnectionBridge;
import org.apache.activemq.artemis.core.server.cluster.impl.ClusterConnectionImpl;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * This will simulate a failure of a failure.
 * The bridge could eventually during a race or multiple failures not be able to reconnect because it failed again.
 * this should make the bridge to always reconnect itself.
 */

public class ClusteredBridgeReconnectTest extends ClusterTestBase {

   @Test
   public void testReconnectBridge() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0);

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, false);

      ClientSession session0 = sfs[0].createSession();
      ClientSession session1 = sfs[0].createSession();

      session0.start();
      session1.start();

      ClientProducer producer = session0.createProducer("queues.testaddress");

      int NUMBER_OF_MESSAGES = 100;
      int REPEATS = 5;

      assertEquals(1, servers[0].getClusterManager().getClusterConnections().size());

      ClusterConnectionImpl connection = servers[0].getClusterManager().getClusterConnections().toArray(new ClusterConnectionImpl[0])[0];
      assertEquals(1, connection.getRecords().size());

      MessageFlowRecord record = connection.getRecords().values().toArray(new MessageFlowRecord[1])[0];
      ClusterConnectionBridge bridge = (ClusterConnectionBridge) record.getBridge();

      Wait.assertEquals(2, () -> bridge.getSessionFactory().getServerLocator().getTopology().getMembers().size());
      ArrayList<TopologyMemberImpl> originalmembers = new ArrayList<>(bridge.getSessionFactory().getServerLocator().getTopology().getMembers());

      AtomicInteger errors = new AtomicInteger(0);

      // running the loop a couple of times
      for (int repeat = 0; repeat < REPEATS; repeat++) {
         CountDownLatch latchSent = new CountDownLatch(1);
         Thread t = new Thread(() -> {
            try {
               for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
                  ClientMessage msg = session0.createMessage(true);
                  producer.send(msg);
                  latchSent.countDown();
                  if (i % 10 == 0) {
                     session0.commit();
                  }
               }
               session0.commit();
            } catch (Exception e) {
               errors.incrementAndGet();
               // not really supposed to happen
               e.printStackTrace();
            }
         });
         t.start();

         Executor executorFail = servers[0].getExecutorFactory().getExecutor();

         assertTrue(latchSent.await(10, TimeUnit.SECONDS));

         Wait.waitFor(() -> BridgeTestAccessor.withinRefs(bridge, (refs) -> {
            synchronized (refs) {
               if (refs.size() > 0) {
                  executorFail.execute(() -> {
                     bridge.connectionFailed(new ActiveMQException("bye"), false);
                  });
                  return true;
               } else {
                  return false;
               }
            }
         }), 500, 1);

         Wait.assertEquals(0L, bridge.getQueue()::getMessageCount, 5000, 1);
         Wait.assertEquals(0, bridge.getQueue()::getDeliveringCount, 5000, 1);

         t.join(5000);
      }


      assertEquals(0, errors.get());
      Wait.assertEquals(2, () -> bridge.getSessionFactory().getServerLocator().getTopology().getMembers().size());

      ArrayList<TopologyMemberImpl> afterReconnectedMembers = new ArrayList<>(bridge.getSessionFactory().getServerLocator().getTopology().getMembers());

      boolean allFound = true;

      for (TopologyMemberImpl originalMember : originalmembers) {
         boolean found = false;
         for (TopologyMember reconnectedMember : afterReconnectedMembers) {
            if (originalMember.equals(reconnectedMember)) {
               found = true;
               break;
            }
         }

         if (!found) {
            allFound = false;
         }
      }

      assertTrue(allFound, "The topology is slightly different after a reconnect");

      int cons0Count = 0, cons1Count = 0;

      while (true) {
         ClientMessage msg = consumers[0].getConsumer().receiveImmediate();
         if (msg == null) {
            break;
         }
         cons0Count++;
         msg.acknowledge();
         session0.commit();
      }

      while (true) {
         ClientMessage msg = consumers[1].getConsumer().receiveImmediate();
         if (msg == null) {
            break;
         }
         cons1Count++;
         msg.acknowledge();
         session1.commit();
      }

      assertEquals(NUMBER_OF_MESSAGES * REPEATS, cons0Count + cons1Count, "cons0 = " + cons0Count + ", cons1 = " + cons1Count);

      session0.commit();
      session1.commit();

      connection = servers[0].getClusterManager().getClusterConnections().toArray(new ClusterConnectionImpl[0])[0];
      assertEquals(1, connection.getRecords().size());
      assertNotNull(bridge.getSessionFactory());

      stopServers(0, 1);

   }

   @Test
   public void testClusterBridgeAddRemoteBinding() throws Exception {

      final String ADDRESS = "queues.testaddress";
      final String QUEUE = UUID.randomUUID().toString();

      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0);

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, ADDRESS, QUEUE, null, false);

      addConsumer(0, 0, QUEUE, null);

      waitForBindings(0, ADDRESS, 1, 1, true);
      waitForBindings(1, ADDRESS, 0, 0, true);

      waitForBindings(0, ADDRESS, 0, 0, false);
      waitForBindings(1, ADDRESS, 1, 1, false);

      ClientSession session0 = sfs[0].createSession();
      ClientSession session1 = sfs[1].createSession();

      session0.start();
      session1.start();

      ClientProducer producer1 = session1.createProducer(ADDRESS);

      int NUMBER_OF_MESSAGES = 10;

      //send to node1 and receive from node0
      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         ClientMessage msg = session1.createMessage(true);
         producer1.send(msg);
         session1.commit();
      }

      int cons0Count = 0;

      while (true) {
         ClientMessage msg = consumers[0].getConsumer().receive(1000);
         if (msg == null) {
            break;
         }
         cons0Count++;
         msg.acknowledge();
         session0.commit();
      }
      assertEquals(NUMBER_OF_MESSAGES, cons0Count);

      //The following code similuates issue where a jms non-subscriber
      //fails over to backup. In the process the temp queue is recreated
      //on the backup with a new id while it's remote binding
      //is created on the other node.
      removeConsumer(0);
      servers[0].getManagementService().enableNotifications(false);
      servers[0].destroyQueue(SimpleString.of(QUEUE));
      servers[0].getManagementService().enableNotifications(true);

      createQueue(0, ADDRESS, QUEUE, null, false);

      addConsumer(0, 0, QUEUE, null);

      waitForBindings(0, ADDRESS, 1, 1, true);
      waitForBindings(1, ADDRESS, 0, 0, true);

      waitForBindings(0, ADDRESS, 0, 0, false);
      waitForBindings(1, ADDRESS, 1, 1, false);

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         ClientMessage msg = session1.createMessage(true);
         producer1.send(msg);
         session1.commit();
      }

      cons0Count = 0;

      while (true) {
         ClientMessage msg = consumers[0].getConsumer().receive(2000);
         if (msg == null) {
            break;
         }
         cons0Count++;
         msg.acknowledge();
         session0.commit();
      }
      assertEquals(NUMBER_OF_MESSAGES, cons0Count);

      stopServers(0, 1);
   }

   @Test
   public void testPauseAddressBlockingSnFQueue() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setRedistributionDelay(0);

      servers[0].getAddressSettingsRepository().addMatch("#", addressSettings);
      servers[1].getAddressSettingsRepository().addMatch("#", addressSettings);

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      ClientSession session0 = sfs[0].createSession();
      ClientSession session1 = sfs[1].createSession();

      session0.start();
      session1.start();

      createQueue(0, "queues.testaddress", "queue1", null, true);
      createQueue(1, "queues.testaddress", "queue1", null, true);
      ClientConsumer consumer1 = session1.createConsumer("queue1");

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 0, false);

      final int num = 10;
      //normal message flow should work
      ClientProducer goodProducer0 = session0.createProducer("queues.testaddress");
      for (int i = 0; i < num; i++) {
         Message msg = session0.createMessage(true);
         msg.putStringProperty("origin", "from producer 0");
         goodProducer0.send(msg);
      }

      //consumer1 can receive from node0
      for (int i = 0; i < num; i++) {
         ClientMessage m = consumer1.receive(5000);
         assertNotNull(m);
         String propValue = m.getStringProperty("origin");
         assertEquals("from producer 0", propValue);
         m.acknowledge();
      }
      assertNull(consumer1.receiveImmediate());

      //pause address from node0
      String addressControlResourceName = ResourceNames.ADDRESS + "queues.testaddress";
      Object resource = servers[0].getManagementService().getResource(addressControlResourceName);
      AddressControl addressControl0 = (AddressControl) resource;
      addressControl0.pause();

      Bindings bindings0 = servers[0].getPostOffice().getBindingsForAddress(SimpleString.of("queues.testaddress"));
      assertNotNull(bindings0);
      assertEquals(2, bindings0.getBindings().size());
      boolean localBindingPaused = false;
      boolean remoteBindingPaused = true;
      for (Binding bd : bindings0.getBindings()) {
         if (bd instanceof LocalQueueBinding) {
            localBindingPaused = ((LocalQueueBinding)bd).getQueue().isPaused();
         }
         if (bd instanceof RemoteQueueBinding) {
            remoteBindingPaused = ((RemoteQueueBinding)bd).getQueue().isPaused();
         }
      }
      assertTrue(localBindingPaused);
      assertFalse(remoteBindingPaused);

      //now message should flow to node 1 regardless of the pause
      for (int i = 0; i < num; i++) {
         Message msg = session0.createMessage(true);
         msg.putStringProperty("origin", "from producer 0");
         goodProducer0.send(msg);
      }

      //consumer1 can receive from node0
      for (int i = 0; i < num; i++) {
         ClientMessage m = consumer1.receive(5000);
         assertNotNull(m);
         String propValue = m.getStringProperty("origin");
         assertEquals("from producer 0", propValue);
         m.acknowledge();
      }
      assertNull(consumer1.receiveImmediate());

      stopServers(0, 1);
   }

   @Override
   @AfterEach
   public void tearDown() throws Exception {
      closeAllConsumers();
      closeAllSessionFactories();
      closeAllServerLocatorsFactories();
      super.tearDown();
   }

   public boolean isNetty() {
      return true;
   }
}
