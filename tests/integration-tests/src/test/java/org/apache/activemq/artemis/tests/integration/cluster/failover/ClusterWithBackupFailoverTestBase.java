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

public abstract class ClusterWithBackupFailoverTestBase extends ClusterTestBase {

   protected static final String QUEUE_NAME = "queue0";
   protected static final String QUEUES_TESTADDRESS = "queues.testaddress";
   protected static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected abstract void setupCluster(MessageLoadBalancingType messageLoadBalancingType) throws Exception;

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

   protected void waitForBindings() throws Exception {
      waitForBindings(0, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(1, QUEUES_TESTADDRESS, 1, 1, true);
      waitForBindings(2, QUEUES_TESTADDRESS, 1, 1, true);

      waitForBindings(0, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(1, QUEUES_TESTADDRESS, 2, 2, false);
      waitForBindings(2, QUEUES_TESTADDRESS, 2, 2, false);
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
}
