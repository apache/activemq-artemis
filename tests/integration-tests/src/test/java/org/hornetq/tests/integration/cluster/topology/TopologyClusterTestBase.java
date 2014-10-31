/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.cluster.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQObjectClosedException;
import org.hornetq.api.core.HornetQUnBlockedException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.hornetq.tests.util.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A TopologyClusterTestBase
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public abstract class TopologyClusterTestBase extends ClusterTestBase
{

   private static final class LatchListener implements ClusterTopologyListener
   {
      private final CountDownLatch upLatch;
      private final List<String> nodes;
      private final CountDownLatch downLatch;

      /**
       * @param upLatch
       * @param nodes
       * @param downLatch
       */
      private LatchListener(CountDownLatch upLatch, List<String> nodes, CountDownLatch downLatch)
      {
         this.upLatch = upLatch;
         this.nodes = nodes;
         this.downLatch = downLatch;
      }

      @Override
      public synchronized void nodeUP(TopologyMember topologyMember, boolean last)
      {
         final String nodeID = topologyMember.getNodeId();

         if (!nodes.contains(nodeID))
         {
            nodes.add(nodeID);
            upLatch.countDown();
         }
      }

      @Override
      public synchronized void nodeDown(final long uniqueEventID, String nodeID)
      {
         if (nodes.contains(nodeID))
         {
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
   public void setUp() throws Exception
   {
      super.setUp();

      setupServers();

      setupCluster();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      stopServers(0, 1, 2, 3, 4);

      super.tearDown();
   }

   /**
    * Check that the actual list of received nodeIDs correspond to the expected order of nodes
    */
   protected void checkOrder(int[] expected, String[] nodeIDs, List<String> actual)
   {
      assertEquals(expected.length, actual.size());
      for (int i = 0; i < expected.length; i++)
      {
         assertEquals("did not receive expected nodeID at " + i, nodeIDs[expected[i]], actual.get(i));
      }
   }

   protected void checkContains(int[] expected, String[] nodeIDs, List<String> actual)
   {
      long start = System.currentTimeMillis();
      do
      {
         if (expected.length != actual.size())
         {
            continue;
         }
         boolean ok = true;
         for (int element : expected)
         {
            ok = (ok && actual.contains(nodeIDs[element]));
         }
         if (ok)
         {
            return;
         }
      } while (System.currentTimeMillis() - start < 5000);
      fail("did not contain all expected node ID: " + actual);
   }

   protected String[] getNodeIDs(int... nodes)
   {
      String[] nodeIDs = new String[nodes.length];
      for (int i = 0; i < nodes.length; i++)
      {
         nodeIDs[i] = servers[i].getNodeID().toString();
      }
      return nodeIDs;
   }

   protected ClientSession checkSessionOrReconnect(ClientSession session, ServerLocator locator) throws Exception
   {
      try
      {
         String rand = RandomUtil.randomString();
         session.createQueue(rand, rand);
         session.deleteQueue(rand);
         return session;
      }
      catch (HornetQObjectClosedException oce)
      {
         ClientSessionFactory sf = createSessionFactory(locator);
         return sf.createSession();
      }
      catch (HornetQUnBlockedException obe)
      {
         ClientSessionFactory sf = createSessionFactory(locator);
         return sf.createSession();
      }
   }

   protected void waitForClusterConnections(final int node, final int expected) throws Exception
   {
      HornetQServer server = servers[node];

      if (server == null)
      {
         throw new IllegalArgumentException("No server at " + node);
      }

      ClusterManager clusterManager = server.getClusterManager();

      long start = System.currentTimeMillis();

      int nodesCount;
      do
      {
         nodesCount = 0;

         for (ClusterConnection clusterConn : clusterManager.getClusterConnections())
         {
            Map<String, String> nodes = clusterConn.getNodes();
            for (String id : nodes.keySet())
            {
               if (clusterConn.isNodeActive(id))
               {
                  nodesCount++;
               }
            }
         }

         if (nodesCount == expected)
         {
            return;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < WAIT_TIMEOUT);

      log.error(clusterDescription(servers[node]));
      assertEquals("Timed out waiting for cluster connections for server " + node, expected, nodesCount);
   }


   @Test
   public void testReceiveNotificationsWhenOtherNodesAreStartedAndStopped() throws Throwable
   {
      startServers(0);

      ServerLocator locator = createHAServerLocator();

      ((ServerLocatorImpl) locator).getTopology().setOwner("testReceive");

      final List<String> nodes = new ArrayList<String>();
      final CountDownLatch upLatch = new CountDownLatch(5);
      final CountDownLatch downLatch = new CountDownLatch(4);

      locator.addClusterTopologyListener(new LatchListener(upLatch, nodes, downLatch));

      ClientSessionFactory sf = createSessionFactory(locator);

      startServers(1, 4, 3, 2);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
      checkContains(new int[]{0, 1, 4, 3, 2}, nodeIDs, nodes);

      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);

      stopServers(2, 3, 1, 4);

      assertTrue("Was not notified that all servers are DOWN", downLatch.await(10, SECONDS));
      checkContains(new int[]{0}, nodeIDs, nodes);

      sf.close();

      locator.close();

      stopServers(0);

   }

   @Test
   public void testReceiveNotifications() throws Throwable
   {
      startServers(0, 1, 2, 3, 4);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      ServerLocator locator = createHAServerLocator();

      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);

      final List<String> nodes = new ArrayList<String>();
      final CountDownLatch upLatch = new CountDownLatch(5);
      final CountDownLatch downLatch = new CountDownLatch(4);

      locator.addClusterTopologyListener(new LatchListener(upLatch, nodes, downLatch));

      ClientSessionFactory sf = createSessionFactory(locator);

      assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
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

      assertTrue("Was not notified that all servers are DOWN", downLatch.await(10, SECONDS));
      checkContains(new int[]{}, nodeIDs, nodes);

      sf.close();
   }


   @Test
   public void testStopNodes() throws Throwable
   {
      startServers(0, 1, 2, 3, 4);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      ServerLocator locator = createHAServerLocator();

      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);

      final List<String> nodes = new ArrayList<String>();
      final CountDownLatch upLatch = new CountDownLatch(5);

      locator.addClusterTopologyListener(new LatchListener(upLatch, nodes, new CountDownLatch(0)));
      ClientSessionFactory sf = createSessionFactory(locator);

      assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
      checkContains(new int[]{0, 1, 2, 3, 4}, nodeIDs, nodes);

      ClientSession session = sf.createSession();

      stopServers(0);
      assertFalse(servers[0].isStarted());
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1, 2, 3, 4}, nodeIDs, nodes);

      stopServers(2);
      assertFalse(servers[2].isStarted());
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1, 3, 4}, nodeIDs, nodes);

      stopServers(4);
      assertFalse(servers[4].isStarted());
      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1, 3}, nodeIDs, nodes);

      stopServers(3);
      assertFalse(servers[3].isStarted());

      session = checkSessionOrReconnect(session, locator);
      checkContains(new int[]{1}, nodeIDs, nodes);

      stopServers(1);
      assertFalse(servers[1].isStarted());
      try
      {
         session = checkSessionOrReconnect(session, locator);
         fail();
      }
      catch (HornetQException expected)
      {
         assertEquals(HornetQExceptionType.NOT_CONNECTED, expected.getType());
      }
   }

   @Test
   public void testWrongPasswordTriggersClusterConnectionStop() throws Exception
   {
      Configuration config = servers[4].getConfiguration();
      for (HornetQServer s : servers)
      {
         if (s != null)
         {
            s.getConfiguration().setSecurityEnabled(true);
         }
      }
      assertEquals(CLUSTER_PASSWORD, config.getClusterPassword());
      config.setClusterPassword(config.getClusterPassword() + "-1-2-3-");
      startServers(0, 1, 2, 4, 3);
      int n = 0;
      while (n++ < 10)
      {
         if (!servers[4].getClusterManager().isStarted())
         {
            break;
         }
         Thread.sleep(100);
      }
      Assert.assertFalse("cluster manager should stop", servers[4].getClusterManager().isStarted());
      final String address = "foo1235";
      ServerLocator locator = createHAServerLocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(config.getClusterUser(), CLUSTER_PASSWORD, false, true, true, false, 1);
      session.createQueue(address, address, true);
      ClientProducer producer = session.createProducer(address);
      sendMessages(session, producer, 100);
      ClientConsumer consumer = session.createConsumer(address);
      session.start();
      receiveMessages(consumer, 0, 100, true);

   }

   @Test
   public void testMultipleClientSessionFactories() throws Throwable
   {
      startServers(0, 1, 2, 3, 4);
      String[] nodeIDs = getNodeIDs(0, 1, 2, 3, 4);

      ServerLocator locator = createHAServerLocator();

      waitForClusterConnections(0, 4);
      waitForClusterConnections(1, 4);
      waitForClusterConnections(2, 4);
      waitForClusterConnections(3, 4);
      waitForClusterConnections(4, 4);

      final List<String> nodes = new ArrayList<String>();
      final CountDownLatch upLatch = new CountDownLatch(5);
      final CountDownLatch downLatch = new CountDownLatch(4);

      locator.addClusterTopologyListener(new LatchListener(upLatch, nodes, downLatch));

      ClientSessionFactory[] sfs = new ClientSessionFactory[]{
         locator.createSessionFactory(),
         locator.createSessionFactory(),
         locator.createSessionFactory(),
         locator.createSessionFactory(),
         locator.createSessionFactory()};
      assertTrue("Was not notified that all servers are UP", upLatch.await(10, SECONDS));
      checkContains(new int[]{0, 1, 2, 3, 4}, nodeIDs, nodes);

      // we can't close all of the servers, we need to leave one up to notify us
      stopServers(4, 2, 3, 1);

      boolean ok = downLatch.await(10, SECONDS);
      if (!ok)
      {
         log.warn("TopologyClusterTestBase.testMultipleClientSessionFactories will fail");
      }
      assertTrue("Was not notified that all servers are Down", ok);
      checkContains(new int[]{0}, nodeIDs, nodes);

      for (ClientSessionFactory sf : sfs)
      {
         sf.close();
      }

      locator.close();

      stopServers(0);
   }
}
