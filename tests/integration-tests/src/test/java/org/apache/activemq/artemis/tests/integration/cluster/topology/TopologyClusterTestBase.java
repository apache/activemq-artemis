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
package org.apache.activemq.artemis.tests.integration.cluster.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.ActiveMQUnBlockedException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class TopologyClusterTestBase extends ClusterTestBase {

   private static final class LatchListener implements ClusterTopologyListener {

      private final CountDownLatch upLatch;
      private final List<String> nodes;
      private final CountDownLatch downLatch;

      // we need a separate list of the nodes we've seen go up that we don't remove IDs from
      // because stale UDP messages can mess up tests once nodes start going down
      private final List<String> seenUp = new ArrayList<>();

      /**
       * @param upLatch
       * @param nodes
       * @param downLatch
       */
      private LatchListener(CountDownLatch upLatch, List<String> nodes, CountDownLatch downLatch) {
         this.upLatch = upLatch;
         this.nodes = nodes;
         this.downLatch = downLatch;
      }

      @Override
      public synchronized void nodeUP(TopologyMember topologyMember, boolean last) {
         final String nodeID = topologyMember.getNodeId();

         if (!seenUp.contains(nodeID)) {
            nodes.add(nodeID);
            seenUp.add(nodeID);
            upLatch.countDown();
         }
      }

      @Override
      public synchronized void nodeDown(final long uniqueEventID, String nodeID) {
         if (nodes.contains(nodeID)) {
            nodes.remove(nodeID);
            downLatch.countDown();
         }
      }
   }

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected abstract ServerLocator createHAServerLocator();

   protected abstract void setupServers() throws Exception;

   protected abstract void setupCluster() throws Exception;

   protected abstract boolean isNetty() throws Exception;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      setupServers();

      setupCluster();
   }

   /**
    * Check that the actual list of received nodeIDs correspond to the expected order of nodes
    */
   protected void checkOrder(int[] expected, String[] nodeIDs, List<String> actual) {
      Assert.assertEquals(expected.length, actual.size());
      for (int i = 0; i < expected.length; i++) {
         Assert.assertEquals("did not receive expected nodeID at " + i, nodeIDs[expected[i]], actual.get(i));
      }
   }

   protected void checkContains(int[] expected, String[] nodeIDs, List<String> actual) {
      long start = System.currentTimeMillis();
      do {
         if (expected.length != actual.size()) {
            continue;
         }
         boolean ok = true;
         for (int element : expected) {
            ok = (ok && actual.contains(nodeIDs[element]));
         }
         if (ok) {
            return;
         }
      }
      while (System.currentTimeMillis() - start < 5000);
      Assert.fail("did not contain all expected node ID: " + actual);
   }

   protected String[] getNodeIDs(int... nodes) {
      String[] nodeIDs = new String[nodes.length];
      for (int i = 0; i < nodes.length; i++) {
         nodeIDs[i] = servers[i].getNodeID().toString();
      }
      return nodeIDs;
   }

   protected ClientSession checkSessionOrReconnect(ClientSession session, ServerLocator locator) throws Exception {
      try {
         String rand = RandomUtil.randomString();
         session.createQueue(rand, RoutingType.MULTICAST, rand);
         session.deleteQueue(rand);
         return session;
      } catch (ActiveMQObjectClosedException oce) {
         ClientSessionFactory sf = createSessionFactory(locator);
         return sf.createSession();
      } catch (ActiveMQUnBlockedException obe) {
         ClientSessionFactory sf = createSessionFactory(locator);
         return sf.createSession();
      }
   }

   protected void waitForClusterConnections(final int node, final int expected) throws Exception {
      ActiveMQServer server = servers[node];

      if (server == null) {
         throw new IllegalArgumentException("No server at " + node);
      }

      ClusterManager clusterManager = server.getClusterManager();

      long start = System.currentTimeMillis();

      int nodesCount;
      do {
         nodesCount = 0;

         for (ClusterConnection clusterConn : clusterManager.getClusterConnections()) {
            Map<String, String> nodes = clusterConn.getNodes();
            for (String id : nodes.keySet()) {
               if (clusterConn.isNodeActive(id)) {
                  nodesCount++;
               }
            }
         }

         if (nodesCount == expected) {
            return;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < ActiveMQTestBase.WAIT_TIMEOUT);

      log.error(clusterDescription(servers[node]));
      Assert.assertEquals("Timed out waiting for cluster connections for server " + node, expected, nodesCount);
   }

   @Test
   public void testReceiveNotificationsWhenOtherNodesAreStartedAndStopped() throws Throwable {
      startServers(0);

      ServerLocator locator = createHAServerLocator();

      locator.getTopology().setOwner("testReceive");

      final List<String> nodes = Collections.synchronizedList(new ArrayList<String>());
      final CountDownLatch upLatch = new CountDownLatch(5);
      final CountDownLatch downLatch = new CountDownLatch(4);

      locator.addClusterTopologyListener(new LatchListener(upLatch, nodes, downLatch));

      ClientSessionFactory sf = createSessionFactory(locator);

      startServers(1, 4, 3, 2);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      Assert.assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
      checkContains(new int[]{0, 1, 4, 3, 2}, nodeIDs, nodes);

      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);

      stopServers(2, 3, 1, 4);

      Assert.assertTrue("Was not notified that all servers are DOWN", downLatch.await(10, SECONDS));
      checkContains(new int[]{0}, nodeIDs, nodes);

      sf.close();

      locator.close();

      stopServers(0);

   }

   @Test
   public void testReceiveNotifications() throws Throwable {
      startServers(0, 1, 2, 3, 4);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      ServerLocator locator = createHAServerLocator();

      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);

      final List<String> nodes = Collections.synchronizedList(new ArrayList<String>());
      final CountDownLatch upLatch = new CountDownLatch(5);
      final CountDownLatch downLatch = new CountDownLatch(4);

      locator.addClusterTopologyListener(new LatchListener(upLatch, nodes, downLatch));

      ClientSessionFactory sf = createSessionFactory(locator);

      Assert.assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
      checkContains(new int[]{0, 1, 2, 3, 4}, nodeIDs, nodes);

      ClientSession session = sf.createSession();

      stopServers(0);
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1, 2, 3, 4}, nodeIDs, nodes);

      stopServers(2);
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1, 3, 4}, nodeIDs, nodes);

      stopServers(4);
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1, 3}, nodeIDs, nodes);

      stopServers(3);
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1}, nodeIDs, nodes);

      stopServers(1);

      Assert.assertTrue("Was not notified that all servers are DOWN", downLatch.await(10, SECONDS));
      checkContains(new int[]{}, nodeIDs, nodes);

      sf.close();
   }

   @Test
   public void testStopNodes() throws Throwable {
      startServers(0, 1, 2, 3, 4);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      ServerLocator locator = createHAServerLocator();

      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);

      final List<String> nodes = Collections.synchronizedList(new ArrayList<String>());
      final CountDownLatch upLatch = new CountDownLatch(5);

      locator.addClusterTopologyListener(new LatchListener(upLatch, nodes, new CountDownLatch(0)));
      ClientSessionFactory sf = createSessionFactory(locator);

      Assert.assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
      checkContains(new int[]{0, 1, 2, 3, 4}, nodeIDs, nodes);

      ClientSession session = sf.createSession();

      stopServers(0);
      Assert.assertFalse(servers[0].isStarted());
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1, 2, 3, 4}, nodeIDs, nodes);

      stopServers(2);
      Assert.assertFalse(servers[2].isStarted());
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1, 3, 4}, nodeIDs, nodes);

      stopServers(4);
      Assert.assertFalse(servers[4].isStarted());
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1, 3}, nodeIDs, nodes);

      stopServers(3);
      Assert.assertFalse(servers[3].isStarted());

      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1}, nodeIDs, nodes);

      stopServers(1);
      Assert.assertFalse(servers[1].isStarted());
      try {
         session = checkSessionOrReconnect(session, locator);
         Assert.fail();
      } catch (ActiveMQException expected) {
         Assert.assertEquals(ActiveMQExceptionType.NOT_CONNECTED, expected.getType());
      }
   }

   @Test
   public void testWrongPasswordTriggersClusterConnectionStop() throws Exception {
      Configuration config = servers[4].getConfiguration();
      for (ActiveMQServer s : servers) {
         if (s != null) {
            s.getConfiguration().setSecurityEnabled(true);
         }
      }
      Assert.assertEquals(ActiveMQTestBase.CLUSTER_PASSWORD, config.getClusterPassword());
      config.setClusterPassword(config.getClusterPassword() + "-1-2-3-");
      startServers(0, 4);
      Assert.assertTrue("one or the other cluster managers should stop", Wait.waitFor(() -> !servers[4].getClusterManager().isStarted() || !servers[0].getClusterManager().isStarted(), 5000));
      final String address = "foo1235";
      ServerLocator locator = createNonHALocator(isNetty());
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(config.getClusterUser(), ActiveMQTestBase.CLUSTER_PASSWORD, false, true, true, false, 1);
      session.createQueue(address, address, true);
      ClientProducer producer = session.createProducer(address);
      sendMessages(session, producer, 100);
      ClientConsumer consumer = session.createConsumer(address);
      session.start();
      receiveMessages(consumer, 0, 100, true);

   }

   @Test
   public void testMultipleClientSessionFactories() throws Throwable {
      startServers(0, 1, 2, 3, 4);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      ServerLocator locator = createHAServerLocator();

      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);

      final List<String> nodes = Collections.synchronizedList(new ArrayList<String>());
      final CountDownLatch upLatch = new CountDownLatch(5);
      final CountDownLatch downLatch = new CountDownLatch(4);

      locator.addClusterTopologyListener(new LatchListener(upLatch, nodes, downLatch));

      ClientSessionFactory[] sfs = new ClientSessionFactory[]{locator.createSessionFactory(), locator.createSessionFactory(), locator.createSessionFactory(), locator.createSessionFactory(), locator.createSessionFactory()};
      Assert.assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
      checkContains(new int[]{0, 1, 2, 3, 4}, nodeIDs, nodes);

      // we can't close all of the servers, we need to leave one up to notify us
      stopServers(4, 2, 3, 1);

      boolean ok = downLatch.await(10, SECONDS);
      if (!ok) {
         log.warn("TopologyClusterTestBase.testMultipleClientSessionFactories will fail");
      }
      Assert.assertTrue("Was not notified that all servers are Down", ok);
      checkContains(new int[]{0}, nodeIDs, nodes);

      for (ClientSessionFactory sf : sfs) {
         sf.close();
      }

      locator.close();

      stopServers(0);
   }
}
