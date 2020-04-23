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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnector;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.core.server.cluster.impl.BridgeImpl;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.Wait;
import org.jboss.logging.Logger;
import org.junit.Before;
import org.junit.Test;

public class BridgeReconnectTest extends BridgeTestBase {

   private static final Logger log = Logger.getLogger(BridgeReconnectTest.class);

   private static final int NUM_MESSAGES = 100;

   Map<String, Object> server0Params;
   Map<String, Object> server1Params;
   Map<String, Object> server2Params;

   ActiveMQServer server0;
   ActiveMQServer server1;
   ActiveMQServer server2;
   ServerLocator locator;

   ClientSession session0;
   ClientSession session1;
   ClientSession session2;

   private TransportConfiguration server1tc;
   private Map<String, TransportConfiguration> connectors;
   private ArrayList<String> staticConnectors;

   final String bridgeName = "bridge1";
   final String testAddress = "testAddress";
   final String queueName = "queue0";
   final String forwardAddress = "forwardAddress";

   final long retryInterval = 50;
   final double retryIntervalMultiplier = 1d;
   final int confirmationWindowSize = 1024;
   int reconnectAttempts = 3;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server0Params = new HashMap<>();
      server1Params = new HashMap<>();
      server2Params = new HashMap<>();
      connectors = new HashMap<>();

      server1 = createActiveMQServer(1, isNetty(), server1Params);
      server1tc = new TransportConfiguration(getConnector(), server1Params, "server1tc");
      connectors.put(server1tc.getName(), server1tc);
      staticConnectors = new ArrayList<>();
      staticConnectors.add(server1tc.getName());
   }

   protected boolean isNetty() {
      return false;
   }

   /**
    * @return
    */
   private String getConnector() {
      if (isNetty()) {
         return NETTY_CONNECTOR_FACTORY;
      }
      return INVM_CONNECTOR_FACTORY;
   }

   /**
    * Backups must successfully deploy its bridges on fail-over.
    *
    * @see https://bugzilla.redhat.com/show_bug.cgi?id=900764
    */
   @Test
   public void testFailoverDeploysBridge() throws Exception {
      NodeManager nodeManager = new InVMNodeManager(false);
      server0 = createActiveMQServer(0, server0Params, isNetty(), nodeManager);
      server2 = createBackupActiveMQServer(2, server2Params, isNetty(), 0, nodeManager);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");
      TransportConfiguration server2tc = new TransportConfiguration(getConnector(), server2Params, "server2tc");

      connectors.put(server2tc.getName(), server2tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);
      server2.getConfiguration().setConnectorConfigurations(connectors);
      reconnectAttempts = -1;

      BridgeConfiguration bridgeConfiguration = createBridgeConfig();
      bridgeConfiguration.setQueueName(queueName);
      List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);
      server2.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      QueueConfiguration queueConfig0 = new QueueConfiguration(queueName).setAddress(testAddress);
      List<QueueConfiguration> queueConfigs0 = new ArrayList<>();
      queueConfigs0.add(queueConfig0);
      server1.getConfiguration().setQueueConfigs(queueConfigs0);

      QueueConfiguration queueConfig1 = new QueueConfiguration(queueName).setAddress(forwardAddress);
      List<QueueConfiguration> queueConfigs1 = new ArrayList<>();
      queueConfigs1.add(queueConfig1);
      server0.getConfiguration().setQueueConfigs(queueConfigs1);
      server2.getConfiguration().setQueueConfigs(queueConfigs1);

      startServers();

      waitForServerStart(server0);
      server0.fail(true);

      waitForServerStart(server2);

      locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(server0tc, server2tc));

      ClientSessionFactory csf0 = addSessionFactory(locator.createSessionFactory(server2tc));

      session0 = csf0.createSession(false, true, true);
      Map<String, Bridge> bridges = server2.getClusterManager().getBridges();
      assertTrue("backup must deploy bridge on failover", !bridges.isEmpty());
   }

   // Fail bridge and reconnecting immediately
   @Test
   public void testFailoverAndReconnectImmediately() throws Exception {
      NodeManager nodeManager = new InVMNodeManager(false);
      server0 = createActiveMQServer(0, server0Params, isNetty(), nodeManager);
      server2 = createBackupActiveMQServer(2, server2Params, isNetty(), 0, nodeManager);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");
      TransportConfiguration server2tc = new TransportConfiguration(getConnector(), server2Params, "server2tc");

      connectors.put(server2tc.getName(), server2tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);

      reconnectAttempts = 1;

      BridgeConfiguration bridgeConfiguration = createBridgeConfig();

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      QueueConfiguration queueConfig0 = new QueueConfiguration(queueName).setAddress(testAddress);
      List<QueueConfiguration> queueConfigs0 = new ArrayList<>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigs(queueConfigs0);

      QueueConfiguration queueConfig1 = new QueueConfiguration(queueName).setAddress(forwardAddress);
      List<QueueConfiguration> queueConfigs1 = new ArrayList<>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigs(queueConfigs1);
      server2.getConfiguration().setQueueConfigs(queueConfigs1);

      startServers();

      BridgeReconnectTest.log.debug("** failing connection");
      // Now we will simulate a failure of the bridge connection between server0 and server1
      server0.fail(true);

      waitForServerStart(server2);

      locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(server0tc, server2tc));

      ClientSessionFactory csf0 = addSessionFactory(locator.createSessionFactory(server2tc));

      session0 = csf0.createSession(false, true, true);

      ClientProducer prod0 = session0.createProducer(testAddress);

      ClientSessionFactory csf2 = addSessionFactory(locator.createSessionFactory(server2tc));

      session2 = csf2.createSession(false, true, true);

      ClientConsumer cons2 = session2.createConsumer(queueName);

      session2.start();

      final int numMessages = NUM_MESSAGES;

      SimpleString propKey = new SimpleString("propkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session0.createMessage(true);
         message.putIntProperty(propKey, i);

         prod0.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage r1 = cons2.receive(1500);
         assertNotNull(r1);
         assertEquals(i, r1.getObjectProperty(propKey));
      }
      closeServers();

      assertNoMoreConnections();
   }

   private BridgeConfiguration createBridgeConfig() {
      return new BridgeConfiguration().setName(bridgeName).setQueueName(queueName).setForwardingAddress(forwardAddress).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setReconnectAttempts(reconnectAttempts).setReconnectAttemptsOnSameNode(0).setConfirmationWindowSize(confirmationWindowSize).setStaticConnectors(staticConnectors).setPassword(CLUSTER_PASSWORD);
   }

   // Fail bridge and attempt failover a few times before succeeding
   @Test
   public void testFailoverAndReconnectAfterAFewTries() throws Exception {
      NodeManager nodeManager = new InVMNodeManager(false);

      server0 = createActiveMQServer(0, server0Params, isNetty(), nodeManager);
      server2 = createBackupActiveMQServer(2, server2Params, isNetty(), 0, nodeManager);

      TransportConfiguration server2tc = new TransportConfiguration(getConnector(), server2Params, "server2tc");

      connectors.put(server2tc.getName(), server2tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);

      BridgeConfiguration bridgeConfiguration = createBridgeConfig();

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      QueueConfiguration queueConfig0 = new QueueConfiguration(queueName).setAddress(testAddress);
      List<QueueConfiguration> queueConfigs0 = new ArrayList<>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigs(queueConfigs0);

      QueueConfiguration queueConfig1 = new QueueConfiguration(queueName).setAddress(forwardAddress);
      List<QueueConfiguration> queueConfigs1 = new ArrayList<>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigs(queueConfigs1);
      server2.getConfiguration().setQueueConfigs(queueConfigs1);

      startServers();
      // Now we will simulate a failure of the bridge connection between server0 and server1
      server0.fail(true);

      locator = addServerLocator(ActiveMQClient.createServerLocatorWithHA(server2tc)).setReconnectAttempts(100);
      ClientSessionFactory csf0 = addSessionFactory(locator.createSessionFactory(server2tc));
      session0 = csf0.createSession(false, true, true);

      ClientSessionFactory csf2 = addSessionFactory(locator.createSessionFactory(server2tc));
      session2 = csf2.createSession(false, true, true);

      ClientProducer prod0 = session0.createProducer(testAddress);

      ClientConsumer cons2 = session2.createConsumer(queueName);

      session2.start();

      final int numMessages = NUM_MESSAGES;

      SimpleString propKey = new SimpleString("propkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session0.createMessage(false);
         message.putIntProperty(propKey, i);

         prod0.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage r1 = cons2.receive(1500);
         assertNotNull(r1);
         assertEquals(i, r1.getObjectProperty(propKey));
      }
      closeServers();

      assertNoMoreConnections();
   }

   // Fail bridge and reconnect same node, no backup specified
   @Test
   public void testReconnectSameNode() throws Exception {
      server0 = createActiveMQServer(0, isNetty(), server0Params);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);

      BridgeConfiguration bridgeConfiguration = createBridgeConfig();

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      QueueConfiguration queueConfig0 = new QueueConfiguration(queueName).setAddress(testAddress);
      List<QueueConfiguration> queueConfigs0 = new ArrayList<>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigs(queueConfigs0);

      QueueConfiguration queueConfig1 = new QueueConfiguration(queueName).setAddress(forwardAddress);
      List<QueueConfiguration> queueConfigs1 = new ArrayList<>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigs(queueConfigs1);

      startServers();

      locator = addServerLocator(ActiveMQClient.createServerLocatorWithHA(server0tc, server1tc));
      ClientSessionFactory csf0 = locator.createSessionFactory(server0tc);
      session0 = csf0.createSession(false, true, true);

      ClientSessionFactory csf1 = locator.createSessionFactory(server1tc);
      session1 = csf1.createSession(false, true, true);

      ClientProducer prod0 = session0.createProducer(testAddress);

      ClientConsumer cons1 = session1.createConsumer(queueName);

      session1.start();

      // Now we will simulate a failure of the bridge connection between server0 and server1
      Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);
      assertNotNull(bridge);
      RemotingConnection forwardingConnection = getForwardingConnection(bridge);
      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = reconnectAttempts - 1;
      forwardingConnection.fail(new ActiveMQNotConnectedException());

      forwardingConnection = getForwardingConnection(bridge);
      forwardingConnection.fail(new ActiveMQNotConnectedException());

      final ManagementService managementService = server0.getManagementService();
      QueueControl coreQueueControl = (QueueControl) managementService.getResource(ResourceNames.QUEUE + queueName);
      assertEquals(0, coreQueueControl.getDeliveringCount());

      final int numMessages = NUM_MESSAGES;

      SimpleString propKey = new SimpleString("propkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session0.createMessage(false);
         message.putIntProperty(propKey, i);

         prod0.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage r1 = cons1.receive(1500);
         assertNotNull(r1);
         assertEquals(i, r1.getObjectProperty(propKey));
      }
      closeServers();

      assertNoMoreConnections();
   }

   // We test that we can pause more than client failure check period (to prompt the pinger to failing)
   // before reconnecting
   @Test
   public void testShutdownServerCleanlyAndReconnectSameNodeWithSleep() throws Exception {
      testShutdownServerCleanlyAndReconnectSameNode(true);
   }

   @Test
   public void testShutdownServerCleanlyAndReconnectSameNode() throws Exception {
      testShutdownServerCleanlyAndReconnectSameNode(false);
   }

   private void testShutdownServerCleanlyAndReconnectSameNode(final boolean sleep) throws Exception {
      server0 = createActiveMQServer(0, isNetty(), server0Params);
      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);
      reconnectAttempts = -1;
      final long clientFailureCheckPeriod = 1000;

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration().setName(bridgeName).setQueueName(queueName).setForwardingAddress(forwardAddress).setClientFailureCheckPeriod(clientFailureCheckPeriod).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setReconnectAttempts(reconnectAttempts).setReconnectAttemptsOnSameNode(0).setConfirmationWindowSize(confirmationWindowSize).setStaticConnectors(staticConnectors).setPassword(CLUSTER_PASSWORD);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      QueueConfiguration queueConfig0 = new QueueConfiguration(queueName).setAddress(testAddress);
      List<QueueConfiguration> queueConfigs0 = new ArrayList<>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigs(queueConfigs0);

      QueueConfiguration queueConfig1 = new QueueConfiguration(queueName).setAddress(forwardAddress);
      List<QueueConfiguration> queueConfigs1 = new ArrayList<>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigs(queueConfigs1);
      startServers();

      waitForServerStart(server0);
      waitForServerStart(server1);

      locator = addServerLocator(ActiveMQClient.createServerLocatorWithHA(server0tc, server1tc));
      ClientSessionFactory csf0 = locator.createSessionFactory(server0tc);
      session0 = csf0.createSession(false, true, true);

      ClientProducer prod0 = session0.createProducer(testAddress);

      BridgeReconnectTest.log.debug("stopping server1");
      server1.stop();

      if (sleep) {
         Thread.sleep(2 * clientFailureCheckPeriod);
      }

      BridgeReconnectTest.log.debug("restarting server1");
      server1.start();
      BridgeReconnectTest.log.debug("server 1 restarted");

      ClientSessionFactory csf1 = locator.createSessionFactory(server1tc);
      session1 = csf1.createSession(false, true, true);

      ClientConsumer cons1 = session1.createConsumer(queueName);

      session1.start();

      final int numMessages = NUM_MESSAGES;

      SimpleString propKey = new SimpleString("propkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session0.createMessage(false);
         message.putIntProperty(propKey, i);

         prod0.send(message);
      }

      BridgeReconnectTest.log.debug("sent messages");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage r1 = cons1.receive(30000);
         assertNotNull("received expected msg", r1);
         assertEquals("property value matches", i, r1.getObjectProperty(propKey));
      }

      BridgeReconnectTest.log.debug("got messages");
      closeServers();
      assertNoMoreConnections();
   }

   /**
    * @throws Exception
    */
   private void closeServers() throws Exception {
      if (session0 != null)
         session0.close();
      if (session1 != null)
         session1.close();
      if (session2 != null)
         session2.close();

      if (locator != null) {
         locator.close();
      }

      server0.stop();
      server1.stop();
      if (server2 != null)
         server2.stop();
   }

   private void assertNoMoreConnections() {
      assertEquals(0, server0.getRemotingService().getConnections().size());
      assertEquals(0, server1.getRemotingService().getConnections().size());
      if (server2 != null)
         assertEquals(0, server2.getRemotingService().getConnections().size());
   }

   @Test
   public void testFailoverThenFailAgainAndReconnect() throws Exception {
      server0 = createActiveMQServer(0, isNetty(), server0Params);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");

      server0.getConfiguration().setConnectorConfigurations(connectors);

      BridgeConfiguration bridgeConfiguration = createBridgeConfig();

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      QueueConfiguration queueConfig0 = new QueueConfiguration(queueName).setAddress(testAddress);
      List<QueueConfiguration> queueConfigs0 = new ArrayList<>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigs(queueConfigs0);

      QueueConfiguration queueConfig1 = new QueueConfiguration(queueName).setAddress(forwardAddress);
      List<QueueConfiguration> queueConfigs1 = new ArrayList<>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigs(queueConfigs1);

      startServers();

      locator = addServerLocator(ActiveMQClient.createServerLocatorWithHA(server0tc, server1tc));
      ClientSessionFactory csf0 = locator.createSessionFactory(server0tc);
      session0 = csf0.createSession(false, true, true);

      ClientSessionFactory csf1 = locator.createSessionFactory(server1tc);
      session1 = csf1.createSession(false, true, true);

      ClientProducer prod0 = session0.createProducer(testAddress);

      ClientConsumer cons1 = session1.createConsumer(queueName);

      session1.start();

      Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);
      RemotingConnection forwardingConnection = getForwardingConnection(bridge);
      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = reconnectAttempts - 1;
      forwardingConnection.fail(new ActiveMQNotConnectedException());

      final int numMessages = NUM_MESSAGES;

      SimpleString propKey = new SimpleString("propkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session0.createMessage(false);
         message.putIntProperty(propKey, i);

         prod0.send(message);
      }
      int outOfOrder = -1;
      int supposed = -1;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage r1 = cons1.receive(1500);
         assertNotNull(r1);
         if (outOfOrder == -1 && i != r1.getIntProperty(propKey).intValue()) {
            outOfOrder = r1.getIntProperty(propKey).intValue();
            supposed = i;
         }
      }
      if (outOfOrder != -1) {
         fail("Message " + outOfOrder + " was received out of order, it was supposed to be " + supposed);
      }

      log.debug("=========== second failure, sending message");

      // Fail again - should reconnect
      forwardingConnection = ((BridgeImpl) bridge).getForwardingConnection();
      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = reconnectAttempts - 1;
      forwardingConnection.fail(new ActiveMQException(ActiveMQExceptionType.UNBLOCKED));

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session0.createMessage(false);
         message.putIntProperty(propKey, i);

         prod0.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage r1 = cons1.receive(1500);
         assertNotNull("Didn't receive message", r1);
         if (outOfOrder == -1 && i != r1.getIntProperty(propKey).intValue()) {
            outOfOrder = r1.getIntProperty(propKey).intValue();
            supposed = i;
         }
      }

      if (outOfOrder != -1) {
         fail("Message " + outOfOrder + " was received out of order, it was supposed to be " + supposed);
      }
      closeServers();

      assertNoMoreConnections();
   }

   @Test
   public void testDeliveringCountOnBridgeConnectionFailure() throws Exception {
      server0 = createActiveMQServer(0, isNetty(), server0Params);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");

      server0.getConfiguration().setConnectorConfigurations(connectors);

      BridgeConfiguration bridgeConfiguration = createBridgeConfig();

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      QueueConfiguration queueConfig0 = new QueueConfiguration(queueName).setAddress(testAddress);
      List<QueueConfiguration> queueConfigs0 = new ArrayList<>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigs(queueConfigs0);

      QueueConfiguration queueConfig1 = new QueueConfiguration(queueName).setAddress(forwardAddress);
      List<QueueConfiguration> queueConfigs1 = new ArrayList<>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigs(queueConfigs1);

      startServers();

      locator = addServerLocator(ActiveMQClient.createServerLocatorWithHA(server0tc, server1tc));
      ClientSessionFactory csf0 = locator.createSessionFactory(server0tc);
      session0 = csf0.createSession(false, true, true);

      ClientSessionFactory csf1 = locator.createSessionFactory(server1tc);
      session1 = csf1.createSession(false, true, true);

      ClientProducer prod0 = session0.createProducer(testAddress);

      session1.start();

      Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);
      RemotingConnection forwardingConnection = getForwardingConnection(bridge);
      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = reconnectAttempts - 1;
      //forwardingConnection.fail(new ActiveMQNotConnectedException());

      final int numMessages = NUM_MESSAGES;

      SimpleString propKey = new SimpleString("propkey");

      final Queue queue = (Queue) server0.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session0.createMessage(false);
         message.putIntProperty(propKey, i);

         prod0.send(message);

         if (i == 50) {
            forwardingConnection.fail(new ActiveMQException(ActiveMQExceptionType.UNBLOCKED));
         }
      }

      Wait.assertEquals(0, queue::getDeliveringCount);

      closeServers();

      assertNoMoreConnections();
   }

   private void startServers() throws Exception {
      if (server2 != null)
         server2.start();
      server1.start();
      server0.start();
   }

   private RemotingConnection getForwardingConnection(final Bridge bridge) throws Exception {
      long start = System.currentTimeMillis();

      do {
         RemotingConnection forwardingConnection = ((BridgeImpl) bridge).getForwardingConnection();

         if (forwardingConnection != null) {
            return forwardingConnection;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < 50000);

      throw new IllegalStateException("Failed to get forwarding connection");
   }

}
