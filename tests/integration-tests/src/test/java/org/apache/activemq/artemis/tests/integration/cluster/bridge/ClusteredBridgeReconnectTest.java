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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.server.cluster.MessageFlowRecord;
import org.apache.activemq.artemis.core.server.cluster.impl.ClusterConnectionBridge;
import org.apache.activemq.artemis.core.server.cluster.impl.ClusterConnectionImpl;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

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

      Assert.assertEquals(1, servers[0].getClusterManager().getClusterConnections().size());

      ClusterConnectionImpl connection = servers[0].getClusterManager().getClusterConnections().toArray(new ClusterConnectionImpl[0])[0];
      Assert.assertEquals(1, connection.getRecords().size());

      MessageFlowRecord record = connection.getRecords().values().toArray(new MessageFlowRecord[1])[0];
      ClusterConnectionBridge bridge = (ClusterConnectionBridge) record.getBridge();

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         ClientMessage msg = session0.createMessage(true);
         producer.send(msg);
         session0.commit();

         if (i == 17) {
            bridge.getSessionFactory().getConnection().fail(new ActiveMQException("failed once!"));
         }
      }

      int cons0Count = 0, cons1Count = 0;

      while (true) {
         ClientMessage msg = consumers[0].getConsumer().receive(1000);
         if (msg == null) {
            break;
         }
         cons0Count++;
         msg.acknowledge();
         session0.commit();
      }

      while (true) {
         ClientMessage msg = consumers[1].getConsumer().receive(1000);
         if (msg == null) {
            break;
         }
         cons1Count++;
         msg.acknowledge();
         session1.commit();
      }

      Assert.assertEquals("cons0 = " + cons0Count + ", cons1 = " + cons1Count, NUMBER_OF_MESSAGES, cons0Count + cons1Count);

      session0.commit();
      session1.commit();

      connection = servers[0].getClusterManager().getClusterConnections().toArray(new ClusterConnectionImpl[0])[0];
      Assert.assertEquals(1, connection.getRecords().size());
      Assert.assertNotNull(bridge.getSessionFactory());

      stopServers(0, 1);

   }



   @Override
   @After
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
