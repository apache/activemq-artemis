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
package org.apache.activemq.artemis.tests.integration.cluster.distribution;

import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.MessageFlowRecord;
import org.apache.activemq.artemis.core.server.cluster.impl.ClusterConnectionImpl;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.junit.Before;
import org.junit.Test;

public class OneWayChainClusterTest extends ClusterTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());
      setupServer(3, isFileStorage(), isNetty());
      setupServer(4, isFileStorage(), isNetty());
   }

   protected boolean isNetty() {
      return false;
   }

   @Test
   public void testBasicRoundRobin() throws Exception {
      setupClusterConnection("cluster0-1", 0, 1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster1-2", 1, 2, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster2-3", 2, 3, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster3-4", 3, 4, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster4-X", 4, -1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);

      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty(), true);

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      addConsumer(1, 4, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);
   }

   @Test
   public void testBasicNonLoadBalanced() throws Exception {
      setupClusterConnection("cluster0-1", 0, 1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster1-2", 1, 2, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster2-3", 2, 3, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster3-4", 3, 4, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster4-X", 4, -1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);

      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty(), true);

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(0, "queues.testaddress", "queue1", null, false);

      createQueue(4, "queues.testaddress", "queue2", null, false);
      createQueue(4, "queues.testaddress", "queue3", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 0, "queue1", null);

      addConsumer(2, 4, "queue2", null);
      addConsumer(3, 4, "queue3", null);

      waitForBindings(0, "queues.testaddress", 2, 2, true);
      waitForBindings(0, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveAll(10, 0, 1, 2, 3);
      verifyNotReceive(0, 1, 2, 3);
   }

   @Test
   public void testRoundRobinForwardWhenNoConsumersTrue() throws Exception {
      setupClusterConnection("cluster0-1", 0, 1, "queues", MessageLoadBalancingType.STRICT, 4, isNetty(), true);
      setupClusterConnection("cluster1-2", 1, 2, "queues", MessageLoadBalancingType.STRICT, 4, isNetty(), true);
      setupClusterConnection("cluster2-3", 2, 3, "queues", MessageLoadBalancingType.STRICT, 4, isNetty(), true);
      setupClusterConnection("cluster3-4", 3, 4, "queues", MessageLoadBalancingType.STRICT, 4, isNetty(), true);
      setupClusterConnection("cluster4-X", 4, -1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);

      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty(), true);

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(0, "queues.testaddress", 1, 0, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 4, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);
   }

   @Test
   public void testRoundRobinForwardWhenNoConsumersFalseNoLocalQueue() throws Exception {
      setupClusterConnection("cluster0-1", 0, 1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster1-2", 1, 2, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster2-3", 2, 3, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster3-4", 3, 4, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster4-X", 4, -1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);

      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty(), true);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      waitForBindings(0, "queues.testaddress", 1, 0, false);

      addConsumer(1, 4, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveAll(10, 1);
      verifyNotReceive(1);
   }

   @Test
   public void testRoundRobinForwardWhenNoConsumersFalse() throws Exception {
      setupClusterConnection("cluster0-1", 0, 1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster1-2", 1, 2, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster2-3", 2, 3, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster3-4", 3, 4, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster4-X", 4, -1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);

      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty(), true);

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(0, "queues.testaddress", 1, 0, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 4, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);

      // Should still be round robin'd since there's no local consumer

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);
   }

   @Test
   public void testRoundRobinForwardWhenNoConsumersFalseLocalConsumer() throws Exception {
      setupClusterConnection("cluster0-1", 0, 1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster1-2", 1, 2, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster2-3", 2, 3, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster3-4", 3, 4, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster4-X", 4, -1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);

      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty(), true);

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 0, false);

      send(0, "queues.testaddress", 10, false, null);

      addConsumer(1, 4, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);

      verifyReceiveAll(10, 0);
      verifyNotReceive(0, 1);
   }

   @Test
   public void testHopsTooLow() throws Exception {
      setupClusterConnection("cluster0-1", 0, 1, "queues", MessageLoadBalancingType.ON_DEMAND, 3, isNetty(), true);
      setupClusterConnection("cluster1-2", 1, 2, "queues", MessageLoadBalancingType.ON_DEMAND, 3, isNetty(), true);
      setupClusterConnection("cluster2-3", 2, 3, "queues", MessageLoadBalancingType.ON_DEMAND, 3, isNetty(), true);
      setupClusterConnection("cluster3-4", 3, 4, "queues", MessageLoadBalancingType.ON_DEMAND, 3, isNetty(), true);
      setupClusterConnection("cluster4-X", 4, -1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);

      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty(), true);

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      addConsumer(1, 4, "queue0", null);

      send(0, "queues.testaddress", 10, false, null);

      verifyReceiveAll(10, 0);

      verifyNotReceive(1);
   }

   @Test
   public void testStartStopMiddleOfChain() throws Exception {
      setupClusterConnection("cluster0-1", 0, 1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster1-2", 1, 2, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster2-3", 2, 3, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster3-4", 3, 4, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster4-X", 4, -1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);

      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty(), true);

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      addConsumer(1, 4, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);

      log.info("============================================ before restart");
      log.info(clusterDescription(servers[0]));
      log.info(clusterDescription(servers[1]));
      log.info(clusterDescription(servers[2]));
      log.info(clusterDescription(servers[3]));
      log.info(clusterDescription(servers[4]));

      stopServers(2);

      waitForTopology(servers[1], 4);

      Thread.sleep(1000);
      log.info("============================================ after stop");
      log.info(clusterDescription(servers[0]));
      log.info(clusterDescription(servers[1]));
      log.info(clusterDescription(servers[3]));
      log.info(clusterDescription(servers[4]));

      startServers(2);

      Thread.sleep(1000);

      waitForTopology(servers[1], 5);

      log.info("============================================ after start");
      log.info(clusterDescription(servers[0]));
      log.info(clusterDescription(servers[1]));
      log.info(clusterDescription(servers[2]));
      log.info(clusterDescription(servers[3]));
      log.info(clusterDescription(servers[4]));

      send(0, "queues.testaddress", 10, false, null);

      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);
   }

   @Test
   public void testChainClusterConnections() throws Exception {
      setupClusterConnection("cluster0-1", 0, 1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster1-2", 1, 2, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster2-3", 2, 3, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster3-4", 3, 4, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);
      setupClusterConnection("cluster4-X", 4, -1, "queues", MessageLoadBalancingType.ON_DEMAND, 4, isNetty(), true);

      startServers(0, 1, 2, 3, 4);
      Set<ClusterConnection> connectionSet = getServer(0).getClusterManager().getClusterConnections();
      assertNotNull(connectionSet);
      assertEquals(1, connectionSet.size());
      ClusterConnectionImpl ccon = (ClusterConnectionImpl) connectionSet.iterator().next();

      long timeout = System.currentTimeMillis() + 5000;
      Map<String, MessageFlowRecord> records = null;
      while (timeout > System.currentTimeMillis()) {
         records = ccon.getRecords();
         if (records != null && records.size() == 1) {
            break;
         }
      }
      assertNotNull(records);
      assertEquals(records.size(), 1);
      getServer(1).getClusterManager().getClusterConnections();
      assertNotNull(connectionSet);
      assertEquals(1, connectionSet.size());
      ccon = (ClusterConnectionImpl) connectionSet.iterator().next();

      records = ccon.getRecords();
      assertNotNull(records);
      assertEquals(records.size(), 1);
      getServer(2).getClusterManager().getClusterConnections();
      assertNotNull(connectionSet);
      assertEquals(1, connectionSet.size());
      ccon = (ClusterConnectionImpl) connectionSet.iterator().next();

      records = ccon.getRecords();
      assertNotNull(records);
      assertEquals(records.size(), 1);
      getServer(3).getClusterManager().getClusterConnections();
      assertNotNull(connectionSet);
      assertEquals(1, connectionSet.size());
      ccon = (ClusterConnectionImpl) connectionSet.iterator().next();

      records = ccon.getRecords();
      assertNotNull(records);
      assertEquals(records.size(), 1);

      getServer(4).getClusterManager().getClusterConnections();
      assertNotNull(connectionSet);
      assertEquals(1, connectionSet.size());
      ccon = (ClusterConnectionImpl) connectionSet.iterator().next();

      records = ccon.getRecords();
      assertNotNull(records);
      assertEquals(records.size(), 1);
   }
}
