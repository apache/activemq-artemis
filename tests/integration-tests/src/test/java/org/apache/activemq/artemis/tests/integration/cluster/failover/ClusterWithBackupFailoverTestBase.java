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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import java.util.HashSet;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.integration.cluster.util.SameProcessActiveMQServer;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.junit.Before;
import org.junit.Test;

public abstract class ClusterWithBackupFailoverTestBase extends ClusterTestBase {

   protected static final String QUEUE_NAME = "queue0";
   protected static final String QUEUES_TESTADDRESS = "queues.testaddress";
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected abstract void setupCluster(final MessageLoadBalancingType messageLoadBalancingType) throws Exception;

   protected abstract void setupServers() throws Exception;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      setupServers();
   }

   protected boolean isNetty() {
      return false;
   }

   @Test
   public void testFailLiveNodes() throws Throwable {
      setupCluster();

      startServers(3, 4, 5, 0, 1, 2);
      //startServers(0, 1, 2, 3, 4, 5);

      for (int i = 0; i < 3; i++) {
         waitForTopology(servers[i], 3, 3);
      }

      waitForFailoverTopology(3, 0, 1, 2);
      waitForFailoverTopology(4, 0, 1, 2);
      waitForFailoverTopology(5, 0, 1, 2);

      setupSessionFactory(0, 3, isNetty(), false);
      setupSessionFactory(1, 4, isNetty(), false);
      setupSessionFactory(2, 5, isNetty(), false);

      createQueue(0, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
      createQueue(1, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
      createQueue(2, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);

      addConsumer(0, 0, QUEUE_NAME, null);
      waitForBindings(0, QUEUES_TESTADDRESS, 1, 1, true);
      addConsumer(1, 1, QUEUE_NAME, null);
      waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, true);
      addConsumer(2, 2, QUEUE_NAME, null);
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);

      waitForBindings();

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);
      Thread.sleep(1000);
      log.info("######### Topology on client = " + locators[0].getTopology().describe() + " locator = " + locators[0]);
      log.info("######### Crashing it........., sfs[0] = " + sfs[0]);
      failNode(0);

      waitForFailoverTopology(4, 3, 1, 2);
      waitForFailoverTopology(5, 3, 1, 2);

      // live nodes
      waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);
      // activated backup nodes
      waitForBindings(3, QUEUES_TESTADDRESS, 1, 1, true);

      // live nodes
      waitForBindings(1, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(2, QUEUES_TESTADDRESS, 2, 2, false);
      // activated backup nodes
      waitForBindings(3, QUEUES_TESTADDRESS, 2, 2, false);

      ClusterWithBackupFailoverTestBase.log.info("** now sending");

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(1);

      waitForFailoverTopology(5, 3, 4, 2);

      Thread.sleep(1000);
      // live nodes
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);
      // activated backup nodes
      waitForBindings(3, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(4, QUEUES_TESTADDRESS, 1, 1, true);

      // live nodes
      waitForBindings(2, QUEUES_TESTADDRESS, 2, 2, false);
      // activated backup nodes
      waitForBindings(3, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(4, QUEUES_TESTADDRESS, 2, 2, false);

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(2);

      Thread.sleep(1000);
      // activated backup nodes
      waitForBindings(3, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(4, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(5, QUEUES_TESTADDRESS, 1, 1, true);

      // activated backup nodes
      waitForBindings(3, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(4, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(5, QUEUES_TESTADDRESS, 2, 2, false);

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      removeConsumer(0);
      removeConsumer(1);
      removeConsumer(2);
   }

   private void waitForBindings() throws Exception {
      waitForBindings(0, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);

      waitForBindings(0, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(1, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(2, QUEUES_TESTADDRESS, 2, 2, false);
   }

   @Test
   public void testFailBackupNodes() throws Exception {
      setupCluster();

      startServers(3, 4, 5, 0, 1, 2);

      for (int i = 0; i < 3; i++) {
         waitForTopology(servers[i], 3, 3);
      }

      setupSessionFactory(0, 3, isNetty(), false);
      setupSessionFactory(1, 4, isNetty(), false);
      setupSessionFactory(2, 5, isNetty(), false);

      createQueue(0, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
      createQueue(1, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
      createQueue(2, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);

      addConsumer(0, 0, QUEUE_NAME, null);
      addConsumer(1, 1, QUEUE_NAME, null);
      addConsumer(2, 2, QUEUE_NAME, null);

      waitForBindings();

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(3);

      waitForBindings();

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(4);

      waitForBindings();

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(5);

      waitForBindings();

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      removeConsumer(0);
      removeConsumer(1);
      removeConsumer(2);
   }

   protected void setupCluster() throws Exception {
      setupCluster(MessageLoadBalancingType.ON_DEMAND);
   }

   protected void failNode(final int node) throws Exception {
      failNode(node, node);
   }

   /**
    * @param node             The node which we should fail
    * @param originalLiveNode The number of the original node, to locate session to fail
    * @throws Exception
    */
   protected void failNode(final int node, final int originalLiveNode) throws Exception {
      ClusterWithBackupFailoverTestBase.log.info("*** failing node " + node);

      ActiveMQServer server = getServer(node);

      TestableServer tstServer = new SameProcessActiveMQServer(server);

      ClientSession[] sessionsArray = exploreSessions(originalLiveNode);

      tstServer.crash(sessionsArray);
   }

   private ClientSession[] exploreSessions(final int node) {
      HashSet<ClientSession> sessions = new HashSet<>();

      for (ConsumerHolder holder : consumers) {
         if (holder != null && holder.getNode() == node && holder.getSession() != null) {
            sessions.add(holder.getSession());
         }
      }

      ClientSession[] sessionsArray = sessions.toArray(new ClientSession[sessions.size()]);
      return sessionsArray;
   }

   @Test
   public void testFailAllNodes() throws Exception {
      setupCluster();

      startServers(0, 1, 2, 3, 4, 5);

      setupSessionFactory(0, 3, isNetty(), false);
      setupSessionFactory(1, 4, isNetty(), false);
      setupSessionFactory(2, 5, isNetty(), false);

      waitForFailoverTopology(3, 0, 1, 2);
      waitForFailoverTopology(4, 0, 1, 2);
      waitForFailoverTopology(5, 0, 1, 2);

      createQueue(0, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
      createQueue(1, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);
      createQueue(2, QUEUES_TESTADDRESS, QUEUE_NAME, null, true);

      addConsumer(0, 0, QUEUE_NAME, null);
      addConsumer(1, 1, QUEUE_NAME, null);
      addConsumer(2, 2, QUEUE_NAME, null);

      waitForBindings();

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      failNode(0);

      waitForFailoverTopology(4, 3, 1, 2);
      waitForFailoverTopology(5, 3, 1, 2);
      // live nodes
      waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);
      // activated backup nodes
      waitForBindings(3, QUEUES_TESTADDRESS, 1, 1, true);

      // live nodes
      waitForBindings(1, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(2, QUEUES_TESTADDRESS, 2, 2, false);
      // activated backup nodes
      waitForBindings(3, QUEUES_TESTADDRESS, 2, 2, false);

      ClusterWithBackupFailoverTestBase.log.info("** now sending");

      send(0, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 0, 1, 2);

      removeConsumer(0);
      failNode(3);

      // live nodes
      waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);

      // live nodes
      waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, false);
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, false);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      failNode(1);

      waitForFailoverTopology(5, 2, 4);
      // live nodes
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);
      // activated backup nodes
      waitForBindings(4, QUEUES_TESTADDRESS, 1, 1, true);

      // live nodes
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, false);
      // activated backup nodes
      waitForBindings(4, QUEUES_TESTADDRESS, 1, 1, false);

      send(1, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 1, 2);

      removeConsumer(1);

      // live nodes
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);
      // live nodes
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 0, false);

      failNode(4, 1);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 2);

      failNode(2);

      // live nodes
      waitForBindings(5, QUEUES_TESTADDRESS, 1, 1, true);
      // live nodes
      waitForBindings(5, QUEUES_TESTADDRESS, 0, 0, false);

      send(2, QUEUES_TESTADDRESS, 10, false, null);
      verifyReceiveRoundRobinInSomeOrder(true, 10, 2);

      removeConsumer(2);
      failNode(5);
   }
}
