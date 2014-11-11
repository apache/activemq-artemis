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
package org.apache.activemq6.tests.integration.cluster.distribution;

import org.apache.activemq6.tests.integration.IntegrationTestLogger;
import org.junit.Before;
import org.junit.Test;

/**
 * A OnewayTwoNodeClusterTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *         <p/>
 *         Created 30 Jan 2009 18:03:28
 */
public class TwoWayTwoNodeClusterTest extends ClusterTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      setupServers();
      setupClusters();
   }

   protected void setupServers() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
   }

   protected void setupClusters()
   {
      setupClusterConnection("cluster0", 0, 1, "queues", false, 1, isNetty(), false);
      setupClusterConnection("cluster1", 1, 0, "queues", false, 1, isNetty(), false);
   }

   protected boolean isNetty()
   {
      return false;
   }

   @Test
   public void testStartStop() throws Exception
   {

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues", "queue0", null, false);
      createQueue(1, "queues", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues", 1, 1, true);
      waitForBindings(1, "queues", 1, 1, true);

      waitForBindings(0, "queues", 1, 1, false);
      waitForBindings(1, "queues", 1, 1, false);

      send(0, "queues", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);

      stopServers(0, 1);
   }

   @Test
   public void testStartPauseStartOther() throws Exception
   {

      startServers(0);

      setupSessionFactory(0, isNetty());
      createQueue(0, "queues", "queue0", null, false);
      addConsumer(0, 0, "queue0", null);

      startServers(1);
      setupSessionFactory(1, isNetty());
      createQueue(1, "queues", "queue0", null, false);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues", 1, 1, true);
      waitForBindings(1, "queues", 1, 1, true);

      waitForBindings(0, "queues", 1, 1, false);
      waitForBindings(1, "queues", 1, 1, false);

      send(0, "queues", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);

      stopServers(0, 1);
   }

   @Test
   public void testRestartServers() throws Throwable
   {
      String name = Thread.currentThread().getName();
      try
      {
         Thread.currentThread().setName("ThreadOnTestRestartTest");
         startServers(0, 1);
         waitForTopology(servers[0], 2);

         waitForTopology(servers[1], 2);

         for (int i = 0; i < 10; i++)
         {
            log.info("Sleep #test " + i);
            log.info("#stop #test #" + i);
            Thread.sleep(500);
            stopServers(1);

            waitForTopology(servers[0], 1, -1, 2000);
            log.info("#start #test #" + i);
            startServers(1);
            waitForTopology(servers[0], 2, -1, 2000);
            waitForTopology(servers[1], 2, -1, 2000);
         }
      }
      finally
      {
         Thread.currentThread().setName(name);
      }

   }

   @Test
   public void testStopStart() throws Exception
   {
      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues", "queue0", null, true);
      createQueue(1, "queues", "queue0", null, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues", 1, 1, true);
      waitForBindings(1, "queues", 1, 1, true);

      waitForBindings(0, "queues", 1, 1, false);
      waitForBindings(1, "queues", 1, 1, false);

      send(0, "queues", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);

      removeConsumer(1);

      closeSessionFactory(1);

      stopServers(1);

      //allow the topology to be propagated before restarting
      waitForTopology(servers[0], 1, -1, 2000);

      System.out.println(clusterDescription(servers[0]));

      startServers(1);

      System.out.println(clusterDescription(servers[0]));
      System.out.println(clusterDescription(servers[1]));

      setupSessionFactory(1, isNetty());

     // createQueue(1, "queues", "queue0", null, false);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues", 1, 1, true);
      waitForBindings(1, "queues", 1, 1, true);

      waitForBindings(1, "queues", 1, 1, false);
      waitForBindings(0, "queues", 1, 1, false);

      send(0, "queues", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);

      stopServers(0, 1);
   }
}
