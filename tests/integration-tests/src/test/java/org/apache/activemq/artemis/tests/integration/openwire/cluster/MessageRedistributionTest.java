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
package org.apache.activemq.artemis.tests.integration.openwire.cluster;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.RemoteQueueBinding;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.ConsumerThread;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Session;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MessageRedistributionTest extends ClusterTestBase {

   @Test
   public void testRemoteConsumerClose() throws Exception {

      setupServer(0, true, true);
      setupServer(1, true, true);

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, true, 0, 1);
      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, true, 1, 0);

      startServers(0, 1);

      waitForTopology(servers[0], 2);
      waitForTopology(servers[1], 2);

      setupSessionFactory(0, true);
      setupSessionFactory(1, true);

      createAddressInfo(0, "queues.testAddress", RoutingType.ANYCAST, -1, false);
      createAddressInfo(1, "queues.testAddress", RoutingType.ANYCAST, -1, false);
      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);

      //alternately create consumers to the 2 nodes
      //close the connection then close consumer quickly
      //check server's consumer count
      for (int i = 0; i < 50; i++) {
         int target = i % 2;
         int remote = (i + 1) % 2;
         closeConsumerAndConnectionConcurrently(target, remote);
      }
   }

   @Override
   protected boolean isResolveProtocols() {
      return true;
   }

   private void closeConsumerAndConnectionConcurrently(int targetNode, int remoteNode) throws Exception {

      String targetUri = getServerUri(targetNode);
      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(targetUri);
      Connection conn = null;
      CountDownLatch active = new CountDownLatch(1);
      try {
         conn = factory.createConnection();
         conn.start();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination dest = ActiveMQDestination.createDestination("queue0", ActiveMQDestination.QUEUE_TYPE);
         ConsumerThread consumer = new ConsumerThread(session, dest);
         consumer.setMessageCount(0);
         consumer.setFinished(active);
         consumer.start();

         assertTrue("consumer takes too long to finish!", active.await(5, TimeUnit.SECONDS));
      } finally {
         conn.close();
      }

      Wait.waitFor(() -> getRemoteQueueBinding(servers[remoteNode]) != null);

      //check remote server's consumer count
      RemoteQueueBinding remoteBinding = getRemoteQueueBinding(servers[remoteNode]);

      assertNotNull(remoteBinding);

      Wait.waitFor(() -> remoteBinding.consumerCount() >= 0);
      int count = remoteBinding.consumerCount();
      assertTrue("consumer count should never be negative " + count, count >= 0);
   }

   private RemoteQueueBinding getRemoteQueueBinding(ActiveMQServer server) throws Exception {
      ActiveMQServer remoteServer = server;
      Bindings bindings = remoteServer.getPostOffice().getBindingsForAddress(new SimpleString("queues.testaddress"));
      Collection<Binding> bindingSet = bindings.getBindings();

      return getRemoteQueueBinding(bindingSet);
   }

   private RemoteQueueBinding getRemoteQueueBinding(Collection<Binding> bindingSet) {
      RemoteQueueBinding remoteBinding = null;
      for (Binding b : bindingSet) {
         if (b instanceof RemoteQueueBinding) {
            remoteBinding = (RemoteQueueBinding) b;
            break;
         }
      }
      return remoteBinding;
   }
}
