/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.routing.policies.ConsistentHashPolicy;
import org.apache.activemq.artemis.core.server.routing.policies.FirstElementPolicy;
import org.apache.activemq.artemis.core.server.routing.policies.LeastConnectionsPolicy;
import org.apache.activemq.artemis.core.server.routing.policies.RoundRobinPolicy;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.core.server.routing.KeyType;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class RedirectTest extends RoutingTestBase {

   @Parameters(name = "protocol: {0}, pool: {1}")
   public static Collection<Object[]> data() {
      final String[] protocols = new String[] {AMQP_PROTOCOL, CORE_PROTOCOL, OPENWIRE_PROTOCOL};
      final String[] pools = new String[] {CLUSTER_POOL, DISCOVERY_POOL, STATIC_POOL};
      Collection<Object[]> data = new ArrayList<>();

      for (String protocol : Arrays.asList(protocols)) {
         for (String pool : Arrays.asList(pools)) {
            data.add(new Object[] {protocol, pool});
         }
      }

      return data;
   }


   private final String protocol;

   private final String pool;


   public RedirectTest(String protocol, String pool) {
      this.protocol = protocol;

      this.pool = pool;
   }

   @TestTemplate
   public void testSimpleRedirect() throws Exception {
      final String queueName = "RedirectTestQueue";

      setupPrimaryServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupPrimaryServerWithDiscovery(1, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      if (CLUSTER_POOL.equals(pool)) {
         setupDiscoveryClusterConnection("cluster0", 0, "dg1", "queues", MessageLoadBalancingType.OFF, 1, true);
         setupDiscoveryClusterConnection("cluster1", 1, "dg1", "queues", MessageLoadBalancingType.OFF, 1, true);
         setupRouterServerWithCluster(0, KeyType.USER_NAME, FirstElementPolicy.NAME, null, false, "ACTIVEMQ.CLUSTER.ADMIN.USER", 1, "cluster0");
      } else if (DISCOVERY_POOL.equals(pool)) {
         setupRouterServerWithDiscovery(0, KeyType.USER_NAME, FirstElementPolicy.NAME, null, false, null, 1);
      } else {
         setupRouterServerWithStaticConnectors(0, KeyType.USER_NAME, FirstElementPolicy.NAME, null, false, null, 1, 1);
      }

      startServers(0, 1);

      getServer(0).createQueue(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));
      getServer(1).createQueue(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));

      QueueControl queueControl0 = (QueueControl)getServer(0).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl1 = (QueueControl)getServer(1).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);

      assertEquals(0, queueControl0.countMessages());
      assertEquals(0, queueControl1.countMessages());

      ConnectionFactory connectionFactory = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, null, "admin", "admin");


      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(queueName);
            try (MessageProducer producer = session.createProducer(queue)) {
               producer.send(session.createTextMessage("TEST"));
            }
         }
      }

      assertEquals(0, queueControl0.countMessages());
      assertEquals(1, queueControl1.countMessages());

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            try (MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {
               TextMessage message = (TextMessage) consumer.receive(1000);
               assertNotNull(message);
               assertEquals("TEST", message.getText());
            }
         }
      }

      assertEquals(0, queueControl0.countMessages());
      assertEquals(0, queueControl1.countMessages());

      stopServers(0, 1);
   }

   @TestTemplate
   public void testRoundRobinRedirect() throws Exception {
      testEvenlyRedirect(RoundRobinPolicy.NAME, null, false);
   }

   @TestTemplate
   public void testLeastConnectionsRedirect() throws Exception {
      testEvenlyRedirect(LeastConnectionsPolicy.NAME, Collections.singletonMap(
         LeastConnectionsPolicy.CONNECTION_COUNT_THRESHOLD, String.valueOf(30)), false);
   }

   @TestTemplate
   public void testRoundRobinRedirectWithFailure() throws Exception {
      testEvenlyRedirect(RoundRobinPolicy.NAME, null, true);
   }

   @TestTemplate
   public void testLeastConnectionsRedirectWithFailure() throws Exception {
      testEvenlyRedirect(LeastConnectionsPolicy.NAME, Collections.singletonMap(
         LeastConnectionsPolicy.CONNECTION_COUNT_THRESHOLD, String.valueOf(30)), true);
   }

   private void testEvenlyRedirect(final String policyName, final Map<String, String> properties, final boolean withFailure) throws Exception {
      final String queueName = "RedirectTestQueue";
      final int targets = MULTIPLE_TARGETS;
      int[] nodes = new int[targets + 1];
      int[] targetNodes = new int[targets];
      QueueControl[] queueControls = new QueueControl[targets + 1];

      nodes[0] = 0;
      setupPrimaryServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      for (int i = 0; i < targets; i++) {
         nodes[i + 1] = i + 1;
         targetNodes[i] = i + 1;
         setupPrimaryServerWithDiscovery(i + 1, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      }

      if (CLUSTER_POOL.equals(pool)) {
         for (int node : nodes) {
            setupDiscoveryClusterConnection("cluster" + node, node, "dg1", "queues", MessageLoadBalancingType.OFF, 1, true);
         }
         setupRouterServerWithCluster(0, KeyType.USER_NAME, policyName, properties, false, "ACTIVEMQ.CLUSTER.ADMIN.USER", targets, "cluster0");
      } else if (DISCOVERY_POOL.equals(pool)) {
         setupRouterServerWithDiscovery(0, KeyType.USER_NAME, policyName, properties, false, null, targets);
      } else {
         setupRouterServerWithStaticConnectors(0, KeyType.USER_NAME, policyName, properties, false, null, targets, 1, 2, 3);
      }

      if (withFailure) {
         setupRouterLocalCache(0, true, 0);
      }

      startServers(nodes);

      for (int node : nodes) {
         getServer(node).createQueue(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));

         queueControls[node] = (QueueControl)getServer(node).getManagementService()
            .getResource(ResourceNames.QUEUE + queueName);

         assertEquals(0, queueControls[node].countMessages(), "Unexpected messagecount for node " + node);
      }


      ConnectionFactory[] connectionFactories = new ConnectionFactory[targets];
      Connection[] connections = new Connection[targets];
      Session[] sessions = new Session[targets];

      for (int i = 0; i < targets; i++) {
         connectionFactories[i] = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
            TransportConstants.DEFAULT_PORT + 0, null, "user" + i, "user" + i);

         connections[i] = connectionFactories[i].createConnection();
         connections[i].start();

         sessions[i] = connections[i].createSession(false, Session.AUTO_ACKNOWLEDGE);
      }

      for (int i = 0; i < targets; i++) {
         try (MessageProducer producer = sessions[i].createProducer(sessions[i].createQueue(queueName))) {
            producer.send(sessions[i].createTextMessage("TEST" + i));
         }

         sessions[i].close();
         connections[i].close();
      }

      assertEquals(0, queueControls[0].countMessages());
      for (int targetNode : targetNodes) {
         assertEquals(1, queueControls[targetNode].countMessages(), "Messages of node " + targetNode);
      }

      if (withFailure) {
         crashAndWaitForFailure(getServer(0));

         startServers(0);
      }

      for (int i = 0; i < targets; i++) {
         try (Connection connection = connectionFactories[i].createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
               try (MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {
                  TextMessage message = (TextMessage) consumer.receive(1000);
                  assertNotNull(message);
                  assertEquals("TEST" + i, message.getText());
               }
            }
         }
      }

      for (int node : nodes) {
         assertEquals(0, queueControls[node].countMessages(), "Unexpected message count for node " + node);
      }

      stopServers(nodes);
   }

   @TestTemplate
   public void testSymmetricRedirect() throws Exception {
      final String queueName = "RedirectTestQueue";

      setupPrimaryServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupPrimaryServerWithDiscovery(1, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      if (CLUSTER_POOL.equals(pool)) {
         setupDiscoveryClusterConnection("cluster0", 0, "dg1", "queues", MessageLoadBalancingType.OFF, 1, true);
         setupDiscoveryClusterConnection("cluster1", 1, "dg1", "queues", MessageLoadBalancingType.OFF, 1, true);
         setupRouterServerWithCluster(0, KeyType.USER_NAME, ConsistentHashPolicy.NAME, null, true, "ACTIVEMQ.CLUSTER.ADMIN.USER", 2, "cluster0");
         setupRouterServerWithCluster(1, KeyType.USER_NAME, ConsistentHashPolicy.NAME, null, true, "ACTIVEMQ.CLUSTER.ADMIN.USER", 2, "cluster1");
      } else if (DISCOVERY_POOL.equals(pool)) {
         setupRouterServerWithDiscovery(0, KeyType.USER_NAME, ConsistentHashPolicy.NAME, null, true, null, 2);
         setupRouterServerWithDiscovery(1, KeyType.USER_NAME, ConsistentHashPolicy.NAME, null, true, null, 2);
      } else {
         setupRouterServerWithStaticConnectors(0, KeyType.USER_NAME, ConsistentHashPolicy.NAME, null, true, null, 2, 1);
         setupRouterServerWithStaticConnectors(1, KeyType.USER_NAME, ConsistentHashPolicy.NAME, null, true, null, 2, 0);
      }

      startServers(0, 1);

      assertTrue(getServer(0).getNodeID() != getServer(1).getNodeID());

      getServer(0).createQueue(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));
      getServer(1).createQueue(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));

      QueueControl queueControl0 = (QueueControl)getServer(0).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl1 = (QueueControl)getServer(1).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);

      assertEquals(0, queueControl0.countMessages(), "Unexpected message count for node 0");
      assertEquals(0, queueControl1.countMessages(), "Unexpected message count for node 1");

      ConnectionFactory connectionFactory0 = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, null, "admin", "admin");


      try (Connection connection = connectionFactory0.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(queueName);
            try (MessageProducer producer = session.createProducer(queue)) {
               producer.send(session.createTextMessage("TEST"));
            }
         }
      }

      assertTrue((queueControl0.countMessages() == 0 && queueControl1.countMessages() == 1) ||
         (queueControl0.countMessages() == 1 && queueControl1.countMessages() == 0));

      assertTrue(getServer(0).getNodeID() != getServer(1).getNodeID());

      ConnectionFactory connectionFactory1 = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 1, null, "admin", "admin");

      try (Connection connection = connectionFactory1.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            try (MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {
               TextMessage message = (TextMessage) consumer.receive(1000);
               assertNotNull(message);
               assertEquals("TEST", message.getText());
            }
         }
      }

      assertEquals(0, queueControl0.countMessages(), "Unexpected message count for node 0");
      assertEquals(0, queueControl1.countMessages(), "Unexpected message count for node 1");

      stopServers(0, 1);
   }

   @TestTemplate
   public void testRedirectAfterFailure() throws Exception {
      final String queueName = "RedirectTestQueue";

      setupPrimaryServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupPrimaryServerWithDiscovery(1, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupPrimaryServerWithDiscovery(2, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      if (CLUSTER_POOL.equals(pool)) {
         setupDiscoveryClusterConnection("cluster0", 0, "dg1", "queues", MessageLoadBalancingType.OFF, 1, true);
         setupDiscoveryClusterConnection("cluster1", 1, "dg1", "queues", MessageLoadBalancingType.OFF, 1, true);
         setupDiscoveryClusterConnection("cluster2", 2, "dg1", "queues", MessageLoadBalancingType.OFF, 1, true);
         setupRouterServerWithCluster(0, KeyType.USER_NAME, FirstElementPolicy.NAME, null, false, "ACTIVEMQ.CLUSTER.ADMIN.USER", 1, "cluster0");
      } else if (DISCOVERY_POOL.equals(pool)) {
         setupRouterServerWithDiscovery(0, KeyType.USER_NAME, FirstElementPolicy.NAME, null, false, null, 1);
      } else {
         setupRouterServerWithStaticConnectors(0, KeyType.USER_NAME, FirstElementPolicy.NAME, null, false, null, 1, 1, 2);
      }

      startServers(0, 1, 2);

      getServer(0).createQueue(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));
      getServer(1).createQueue(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));
      getServer(2).createQueue(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));

      QueueControl queueControl0 = (QueueControl)getServer(0).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl1 = (QueueControl)getServer(1).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);
      QueueControl queueControl2 = (QueueControl)getServer(2).getManagementService()
         .getResource(ResourceNames.QUEUE + queueName);

      assertEquals(0, queueControl0.countMessages(), "Unexpected message count for node 0");
      assertEquals(0, queueControl1.countMessages(), "Unexpected message count for node 1");
      assertEquals(0, queueControl2.countMessages(), "Unexpected message count for node 2");

      int failedNode;
      ConnectionFactory connectionFactory = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, null, "admin", "admin");


      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = session.createQueue(queueName);
            try (MessageProducer producer = session.createProducer(queue)) {
               producer.send(session.createTextMessage("TEST_BEFORE_FAILURE"));

               if (queueControl1.countMessages() > 0) {
                  failedNode = 1;
               } else {
                  failedNode = 2;
               }

               stopServers(failedNode);

               producer.send(session.createTextMessage("TEST_AFTER_FAILURE"));
            }
         }
      }

      startServers(failedNode);

      assertEquals(0, queueControl0.countMessages(), "Unexpected message count for node 0");
      assertEquals(1, queueControl1.countMessages(), "Unexpected message count for node 1");
      assertEquals(1, queueControl2.countMessages(), "Unexpected message count for node 2");

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            try (MessageConsumer consumer = session.createConsumer(session.createQueue(queueName))) {
               TextMessage message = (TextMessage) consumer.receive(1000);
               assertNotNull(message);
               assertEquals("TEST_AFTER_FAILURE", message.getText());
            }
         }
      }

      assertEquals(0, queueControl0.countMessages(), "Unexpected message count for node 0");
      if (failedNode == 1) {
         assertEquals(1, queueControl1.countMessages(), "Unexpected message count for node 1");
         assertEquals(0, queueControl2.countMessages(), "Unexpected message count for node 2");
      } else {
         assertEquals(0, queueControl1.countMessages(), "Unexpected message count for node 1");
         assertEquals(1, queueControl2.countMessages(), "Unexpected message count for node 2");
      }

      stopServers(0, 1, 2);
   }
}
