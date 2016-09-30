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

import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.junit.Before;
import org.junit.Test;

public class ClusterWithBackupTest extends ClusterTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

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
   public void testBasicRoundRobin() throws Throwable {
      try {
         setupCluster();

         startServers(0, 1, 2, 3, 4, 5);

         setupSessionFactory(3, isNetty());
         setupSessionFactory(4, isNetty());
         setupSessionFactory(5, isNetty());

         createQueue(3, "queues.testaddress", "queue0", null, false);
         createQueue(4, "queues.testaddress", "queue0", null, false);
         createQueue(5, "queues.testaddress", "queue0", null, false);

         addConsumer(0, 3, "queue0", null);
         addConsumer(1, 4, "queue0", null);
         addConsumer(2, 5, "queue0", null);

         waitForBindings(3, "queues.testaddress", 1, 1, true);
         waitForBindings(4, "queues.testaddress", 1, 1, true);
         waitForBindings(5, "queues.testaddress", 1, 1, true);

         waitForBindings(3, "queues.testaddress", 2, 2, false);
         waitForBindings(4, "queues.testaddress", 2, 2, false);
         waitForBindings(5, "queues.testaddress", 2, 2, false);

         send(3, "queues.testaddress", 100, false, null);

         verifyReceiveRoundRobinInSomeOrder(100, 0, 1, 2);

         verifyNotReceive(0, 0, 1, 2);
      } catch (Throwable e) {
         e.printStackTrace();
         log.error(e.getMessage(), e);
         throw e;
      }
   }

   protected void setupCluster() throws Exception {
      setupCluster(MessageLoadBalancingType.ON_DEMAND);
   }

   protected void setupCluster(final MessageLoadBalancingType messageLoadBalancingType) throws Exception {
      setupClusterConnection("cluster0", "queues", messageLoadBalancingType, 1, isNetty(), 3, 4, 5);

      setupClusterConnection("cluster1", "queues", messageLoadBalancingType, 1, isNetty(), 4, 3, 5);

      setupClusterConnection("cluster2", "queues", messageLoadBalancingType, 1, isNetty(), 5, 3, 4);

      setupClusterConnection("cluster0", "queues", messageLoadBalancingType, 1, isNetty(), 0, 4, 5);

      setupClusterConnection("cluster1", "queues", messageLoadBalancingType, 1, isNetty(), 1, 3, 5);

      setupClusterConnection("cluster2", "queues", messageLoadBalancingType, 1, isNetty(), 2, 3, 4);
   }

   protected void setupServers() throws Exception {
      // The backups
      setupBackupServer(0, 3, isFileStorage(), true, isNetty());
      setupBackupServer(1, 4, isFileStorage(), true, isNetty());
      setupBackupServer(2, 5, isFileStorage(), true, isNetty());

      // The lives
      setupLiveServer(3, isFileStorage(), true, isNetty(), false);
      setupLiveServer(4, isFileStorage(), true, isNetty(), false);
      setupLiveServer(5, isFileStorage(), true, isNetty(), false);

   }
}
