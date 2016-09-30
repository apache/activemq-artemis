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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class BridgeStartTest extends ActiveMQTestBase {

   @Parameterized.Parameters(name = "isNetty={0}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{true}, {false}});
   }

   public BridgeStartTest(boolean netty) {
      this.netty = netty;
   }

   private final boolean netty;

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected boolean isNetty() {
      return netty;
   }

   private String getConnector() {
      if (isNetty()) {
         return NETTY_CONNECTOR_FACTORY;
      }
      return INVM_CONNECTOR_FACTORY;
   }

   @Test
   public void testStartStop() throws Exception {
      Map<String, Object> server0Params = new HashMap<>();
      ActiveMQServer server0 = createClusteredServerWithParams(isNetty(), 0, true, server0Params);

      Map<String, Object> server1Params = new HashMap<>();
      if (isNetty()) {
         server1Params.put("port", org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);
      } else {
         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      }
      ActiveMQServer server1 = createClusteredServerWithParams(isNetty(), 1, true, server1Params);
      ServerLocator locator = null;
      try {
         final String testAddress = "testAddress";
         final String queueName0 = "queue0";
         final String forwardAddress = "forwardAddress";
         final String queueName1 = "queue1";

         Map<String, TransportConfiguration> connectors = new HashMap<>();
         TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params);
         TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params);
         connectors.put(server1tc.getName(), server1tc);

         server0.getConfiguration().setConnectorConfigurations(connectors);

         ArrayList<String> staticConnectors = new ArrayList<>();
         staticConnectors.add(server1tc.getName());

         final String bridgeName = "bridge1";

         BridgeConfiguration bridgeConfiguration = new BridgeConfiguration().setName(bridgeName).setQueueName(queueName0).setForwardingAddress(forwardAddress).setRetryInterval(1000).setReconnectAttempts(0).setReconnectAttemptsOnSameNode(0).setConfirmationWindowSize(1024).setStaticConnectors(staticConnectors);

         List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
         bridgeConfigs.add(bridgeConfiguration);
         server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

         CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName0);
         List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<>();
         queueConfigs0.add(queueConfig0);
         server0.getConfiguration().setQueueConfigurations(queueConfigs0);

         CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration().setAddress(forwardAddress).setName(queueName1);
         List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<>();
         queueConfigs1.add(queueConfig1);
         server1.getConfiguration().setQueueConfigurations(queueConfigs1);

         server1.start();
         waitForServerToStart(server1);

         server0.start();
         waitForServerToStart(server0);

         locator = ActiveMQClient.createServerLocatorWithoutHA(server0tc, server1tc);
         ClientSessionFactory sf0 = locator.createSessionFactory(server0tc);

         ClientSessionFactory sf1 = locator.createSessionFactory(server1tc);

         ClientSession session0 = sf0.createSession(false, true, true);

         ClientSession session1 = sf1.createSession(false, true, true);

         ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

         ClientConsumer consumer1 = session1.createConsumer(queueName1);

         session1.start();

         final int numMessages = 10;

         final SimpleString propKey = new SimpleString("testkey");

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = session0.createMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = consumer1.receive(200);

            Assert.assertNotNull(message);

            Assert.assertEquals(i, message.getObjectProperty(propKey));

            message.acknowledge();
         }

         Assert.assertNull(consumer1.receiveImmediate());

         Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);

         bridge.stop();

         bridge.flushExecutor();

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = session0.createMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         Assert.assertNull(consumer1.receiveImmediate());

         bridge.start();

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = consumer1.receive(1000);

            Assert.assertNotNull(message);

            Assert.assertEquals(i, message.getObjectProperty(propKey));

            message.acknowledge();
         }

         Assert.assertNull(consumer1.receiveImmediate());

         session0.close();

         session1.close();

         sf0.close();

         sf1.close();
      } finally {
         if (locator != null) {
            locator.close();
         }

         server0.stop();

         server1.stop();
      }

   }

   @Test
   public void testTargetServerUpAndDown() throws Exception {
      // This test needs to use real files, since it requires duplicate detection, since when the target server is
      // shutdown, messages will get resent when it is started, so the dup id cache needs
      // to be persisted

      Map<String, Object> server0Params = new HashMap<>();
      ActiveMQServer server0 = createClusteredServerWithParams(isNetty(), 0, true, server0Params);

      Map<String, Object> server1Params = new HashMap<>();
      if (isNetty()) {
         server1Params.put("port", org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);
      } else {
         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      }
      ActiveMQServer server1 = createClusteredServerWithParams(isNetty(), 1, true, server1Params);

      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";
      final String queueName1 = "queue1";

      Map<String, TransportConfiguration> connectors = new HashMap<>();
      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params);
      TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params);
      connectors.put(server1tc.getName(), server1tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);

      ArrayList<String> staticConnectors = new ArrayList<>();
      staticConnectors.add(server1tc.getName());

      final String bridgeName = "bridge1";

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration().setName(bridgeName).setQueueName(queueName0).setForwardingAddress(forwardAddress).setRetryInterval(500).setReconnectAttempts(-1).setReconnectAttemptsOnSameNode(0).setConfirmationWindowSize(1024).setStaticConnectors(staticConnectors);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName0);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration().setAddress(forwardAddress).setName(queueName1);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);
      ServerLocator locator = null;
      try {
         // Don't start server 1 yet

         server0.start();
         waitForServerToStart(server0);

         locator = ActiveMQClient.createServerLocatorWithoutHA(server0tc, server1tc);
         ClientSessionFactory sf0 = locator.createSessionFactory(server0tc);

         ClientSession session0 = sf0.createSession(false, true, true);

         ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

         final int numMessages = 10;

         final SimpleString propKey = new SimpleString("testkey");

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = session0.createMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         // Wait a bit
         Thread.sleep(1000);

         server1.start();
         waitForServerToStart(server1);

         ClientSessionFactory sf1 = locator.createSessionFactory(server1tc);

         ClientSession session1 = sf1.createSession(false, true, true);

         ClientConsumer consumer1 = session1.createConsumer(queueName1);

         session1.start();

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = consumer1.receive(1000);

            Assert.assertNotNull(message);

            Assert.assertEquals(i, message.getObjectProperty(propKey));

            message.acknowledge();
         }

         Assert.assertNull(consumer1.receiveImmediate());

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = session0.createMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = consumer1.receive(1000);

            Assert.assertNotNull(message);

            Assert.assertEquals(i, message.getObjectProperty(propKey));

            message.acknowledge();
         }

         Assert.assertNull(consumer1.receiveImmediate());

         session1.close();

         sf1.close();

         BridgeStartTest.log.info("stopping server 1");

         server1.stop();

         BridgeStartTest.log.info("stopped server 1");

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = session0.createMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         BridgeStartTest.log.info("sent some more messages");

         server1.start();
         waitForServerToStart(server1);

         BridgeStartTest.log.info("started server1");

         sf1 = locator.createSessionFactory(server1tc);

         session1 = sf1.createSession(false, true, true);

         consumer1 = session1.createConsumer(queueName1);

         session1.start();

         BridgeStartTest.log.info("started session");

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = consumer1.receive(1000);

            Assert.assertNotNull(message);

            Assert.assertEquals(i, message.getObjectProperty(propKey));

            message.acknowledge();
         }

         Assert.assertNull(consumer1.receiveImmediate());

         session1.close();

         sf1.close();

         session0.close();

         sf0.close();

         locator.close();
      } finally {
         if (locator != null) {
            locator.close();
         }

         server0.stop();

         server1.stop();
      }
   }

   @Test
   public void testTargetServerNotAvailableNoReconnectTries() throws Exception {
      Map<String, Object> server0Params = new HashMap<>();
      ActiveMQServer server0 = createClusteredServerWithParams(isNetty(), 0, false, server0Params);

      Map<String, Object> server1Params = new HashMap<>();
      if (isNetty()) {
         server1Params.put("port", org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);
      } else {
         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      }
      ActiveMQServer server1 = createClusteredServerWithParams(isNetty(), 1, false, server1Params);

      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";
      final String queueName1 = "queue1";
      ServerLocator locator = null;
      try {
         Map<String, TransportConfiguration> connectors = new HashMap<>();
         TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params);
         TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params);
         connectors.put(server1tc.getName(), server1tc);

         server0.getConfiguration().setConnectorConfigurations(connectors);

         ArrayList<String> staticConnectors = new ArrayList<>();
         staticConnectors.add(server1tc.getName());

         final String bridgeName = "bridge1";

         BridgeConfiguration bridgeConfiguration = new BridgeConfiguration().setName(bridgeName).setQueueName(queueName0).setForwardingAddress(forwardAddress).setRetryInterval(1000).setReconnectAttempts(0).setReconnectAttemptsOnSameNode(0).setUseDuplicateDetection(false).setConfirmationWindowSize(1024).setStaticConnectors(staticConnectors);

         List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
         bridgeConfigs.add(bridgeConfiguration);
         server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

         CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName0);
         List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<>();
         queueConfigs0.add(queueConfig0);
         server0.getConfiguration().setQueueConfigurations(queueConfigs0);

         CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration().setAddress(forwardAddress).setName(queueName1);
         List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<>();
         queueConfigs1.add(queueConfig1);
         server1.getConfiguration().setQueueConfigurations(queueConfigs1);

         // Don't start server 1 yet

         server0.start();
         waitForServerToStart(server0);

         locator = ActiveMQClient.createServerLocatorWithoutHA(server0tc, server1tc);
         ClientSessionFactory sf0 = locator.createSessionFactory(server0tc);

         ClientSession session0 = sf0.createSession(false, true, true);

         ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

         final int numMessages = 10;

         final SimpleString propKey = new SimpleString("testkey");

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = session0.createMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         // Wait a bit
         Thread.sleep(1000);

         // JMSBridge should be stopped since retries = 0

         server1.start();
         waitForServerToStart(server1);

         ClientSessionFactory sf1 = locator.createSessionFactory(server1tc);

         ClientSession session1 = sf1.createSession(false, true, true);

         ClientConsumer consumer1 = session1.createConsumer(queueName1);

         session1.start();

         // Won't be received since the bridge was deactivated
         Assert.assertNull(consumer1.receiveImmediate());

         // Now start the bridge manually

         Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);

         bridge.start();

         // Messages should now be received

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = consumer1.receive(3000);

            Assert.assertNotNull(message);

            Assert.assertEquals(i, message.getObjectProperty(propKey));

            message.acknowledge();
         }

         Assert.assertNull(consumer1.receiveImmediate());

         session1.close();

         sf1.close();

         session0.close();

         sf0.close();

      } finally {
         if (locator != null) {
            locator.close();
         }

         server0.stop();

         server1.stop();
      }

   }

   @Test
   public void testManualStopStart() throws Exception {
      Map<String, Object> server0Params = new HashMap<>();
      ActiveMQServer server0 = createClusteredServerWithParams(isNetty(), 0, false, server0Params);

      Map<String, Object> server1Params = new HashMap<>();
      if (isNetty()) {
         server1Params.put("port", org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);
      } else {
         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      }
      ActiveMQServer server1 = createClusteredServerWithParams(isNetty(), 1, false, server1Params);

      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";
      final String queueName1 = "queue1";
      ServerLocator locator = null;
      try {
         Map<String, TransportConfiguration> connectors = new HashMap<>();
         TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params);
         TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params);
         connectors.put(server1tc.getName(), server1tc);

         server0.getConfiguration().setConnectorConfigurations(connectors);

         ArrayList<String> staticConnectors = new ArrayList<>();
         staticConnectors.add(server1tc.getName());

         final String bridgeName = "bridge1";

         BridgeConfiguration bridgeConfiguration = new BridgeConfiguration().setName(bridgeName).setQueueName(queueName0).setForwardingAddress(forwardAddress).setRetryInterval(1000).setReconnectAttempts(1).setReconnectAttemptsOnSameNode(0).setConfirmationWindowSize(1024).setStaticConnectors(staticConnectors);

         List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
         bridgeConfigs.add(bridgeConfiguration);
         server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

         CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName0);
         List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<>();
         queueConfigs0.add(queueConfig0);
         server0.getConfiguration().setQueueConfigurations(queueConfigs0);

         CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration().setAddress(forwardAddress).setName(queueName1);
         List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<>();
         queueConfigs1.add(queueConfig1);
         server1.getConfiguration().setQueueConfigurations(queueConfigs1);

         server1.start();
         waitForServerToStart(server1);

         server0.start();
         waitForServerToStart(server0);

         locator = ActiveMQClient.createServerLocatorWithoutHA(server0tc, server1tc);
         ClientSessionFactory sf0 = locator.createSessionFactory(server0tc);

         ClientSession session0 = sf0.createSession(false, true, true);

         ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

         final int numMessages = 10;

         final SimpleString propKey = new SimpleString("testkey");

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = session0.createMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }
         ClientSessionFactory sf1 = locator.createSessionFactory(server1tc);

         ClientSession session1 = sf1.createSession(false, true, true);

         ClientConsumer consumer1 = session1.createConsumer(queueName1);

         session1.start();

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = consumer1.receive(1000);

            Assert.assertNotNull(message);

            Assert.assertEquals(i, message.getObjectProperty(propKey));

            message.acknowledge();
         }

         Assert.assertNull(consumer1.receiveImmediate());

         // Now stop the bridge manually

         Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);

         BridgeStartTest.log.info("stopping bridge manually");

         bridge.stop();

         bridge.flushExecutor();

         for (int i = numMessages; i < numMessages * 2; i++) {
            ClientMessage message = session0.createMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         Assert.assertNull(consumer1.receiveImmediate());

         bridge.start();

         BridgeStartTest.log.info("started bridge");

         // The previous messages will get resent, but with duplicate detection they will be rejected
         // at the target

         for (int i = numMessages; i < numMessages * 2; i++) {
            ClientMessage message = consumer1.receive(1000);

            Assert.assertNotNull(message);

            Assert.assertEquals(i, message.getObjectProperty(propKey));

            message.acknowledge();
         }

         Assert.assertNull(consumer1.receiveImmediate());

         bridge.stop();

         bridge.flushExecutor();

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = session0.createMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         Assert.assertNull(consumer1.receiveImmediate());

         bridge.start();

         for (int i = 0; i < numMessages; i++) {
            ClientMessage message = consumer1.receive(1000);

            Assert.assertNotNull(message);

            Assert.assertEquals(i, message.getObjectProperty(propKey));

            message.acknowledge();
         }

         Assert.assertNull(consumer1.receiveImmediate());

         session1.close();

         sf1.close();

         session0.close();

         sf0.close();
      } finally {
         if (locator != null) {
            locator.close();
         }
         server0.stop();

         server1.stop();
      }

   }
}
