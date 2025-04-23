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
package org.apache.activemq.artemis.tests.integration.amqp.connect;

import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.PULL_RECEIVER_BATCH_SIZE;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.RECEIVER_CREDITS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBridgeAddressPolicyElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBridgeBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBridgeQueuePolicyElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ComponentConfigurationRoutingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test AMQP Bridge between two Artemis servers.
 */
class AMQPBridgeServerToServerTest extends AmqpClientTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final int SERVER_PORT = AMQP_PORT;
   private static final int SERVER_PORT_REMOTE = AMQP_PORT + 1;

   private static final int MIN_LARGE_MESSAGE_SIZE = 10 * 1024;

   protected ActiveMQServer remoteServer;
   protected ActiveMQServer remoteServer2; // Used in two hop tests

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,CORE";
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      remoteServer = createServer(SERVER_PORT_REMOTE, false);

      return createServer(SERVER_PORT, false);
   }

   @Override
   protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
      params.put("amqpMinLargeMessageSize", MIN_LARGE_MESSAGE_SIZE);
   }

   @AfterEach
   @Override
   public void tearDown() throws Exception {
      super.tearDown();

      try {
         if (remoteServer != null) {
            remoteServer.stop();
            remoteServer = null;
         }
      } catch (Exception e) {
      }

      try {
         if (remoteServer2 != null) {
            remoteServer2.stop();
            remoteServer2 = null;
         }
      } catch (Exception e) {
      }
   }

   @Test
   @Timeout(20)
   public void testAddresDemandOnLocalBrokerBridgesMessagesFromRemoteAMQP() throws Exception {
      testAddresDemandOnLocalBrokerBridgesMessagesFromRemote("AMQP");
   }

   @Test
   @Timeout(20)
   public void testAddresDemandOnLocalBrokerBridgesMessagesFromRemoteCORE() throws Exception {
      testAddresDemandOnLocalBrokerBridgesMessagesFromRemote("CORE");
   }

   private void testAddresDemandOnLocalBrokerBridgesMessagesFromRemote(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPBridgeAddressPolicyElement bridgeAddressPolicy = new AMQPBridgeAddressPolicyElement();
      bridgeAddressPolicy.setName("test-policy");
      bridgeAddressPolicy.addToIncludes(getTestName());

      final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
      element.setName(getTestName());
      element.addBridgeFromAddressPolicy(bridgeAddressPolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration(getTestName(), "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      server.getConfiguration().addAMQPConnection(amqpConnection);
      remoteServer.start();
      server.start();

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final Topic topic = sessionL.createTopic(getTestName());

         final MessageConsumer consumerL = sessionL.createConsumer(topic);

         connectionL.start();
         connectionR.start();

         // Demand on local address should trigger receiver on remote.
         Wait.assertTrue(() -> server.addressQuery(SimpleString.of(getTestName())).isExists());
         Wait.assertTrue(() -> remoteServer.addressQuery(SimpleString.of(getTestName())).isExists());

         // Captures state of JMS consumers and bridge consumers attached on each node
         Wait.assertTrue(() -> server.bindingQuery(SimpleString.of(getTestName()), false).getQueueNames().size() >= 1);
         Wait.assertTrue(() -> remoteServer.bindingQuery(SimpleString.of(getTestName()), false).getQueueNames().size() >= 1);

         final MessageProducer producerR = sessionR.createProducer(topic);
         final TextMessage message = sessionR.createTextMessage("Hello World");

         message.setStringProperty("testProperty", "testValue");

         producerR.send(message);

         final Message received = consumerL.receive(5_000);
         assertNotNull(received);
         assertTrue(received instanceof TextMessage);
         assertEquals("Hello World", ((TextMessage) received).getText());
         assertTrue(message.propertyExists("testProperty"));
         assertEquals("testValue", received.getStringProperty("testProperty"));
      }
   }

   @Test
   @Timeout(20)
   public void testDivertAddressDemandOnLocalBrokerBridgesMessagesFromRemoteAMQP() throws Exception {
      testDivertAddresDemandOnLocalBrokerBridgesMessagesFromRemote("AMQP");
   }

   @Test
   @Timeout(20)
   public void testDivertAddresDemandOnLocalBrokerBridgesMessagesFromRemoteCORE() throws Exception {
      testDivertAddresDemandOnLocalBrokerBridgesMessagesFromRemote("CORE");
   }

   private void testDivertAddresDemandOnLocalBrokerBridgesMessagesFromRemote(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPBridgeAddressPolicyElement bridgeAddressPolicy = new AMQPBridgeAddressPolicyElement();
      bridgeAddressPolicy.setName("test-policy");
      bridgeAddressPolicy.addToIncludes(getTestName());
      bridgeAddressPolicy.setIncludeDivertBindings(true);

      final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
      element.setName(getTestName());
      element.addBridgeFromAddressPolicy(bridgeAddressPolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration(getTestName(), "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      final DivertConfiguration divert = new DivertConfiguration();
      divert.setName("test-divert");
      divert.setAddress(getTestName());
      divert.setForwardingAddress("target");
      divert.setRoutingType(ComponentConfigurationRoutingType.MULTICAST);

      server.getConfiguration().addAMQPConnection(amqpConnection);
      remoteServer.start();
      server.start();
      server.deployDivert(divert);
      // Currently the address must exist on the local before we will federate from the remote
      server.addAddressInfo(new AddressInfo(SimpleString.of(getTestName()), RoutingType.MULTICAST));

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final Topic target = sessionL.createTopic("target");
         final Topic source = sessionL.createTopic(getTestName());

         final MessageConsumer consumerL = sessionL.createConsumer(target);

         connectionL.start();
         connectionR.start();

         // Demand on local address should trigger receiver on remote.
         Wait.assertTrue(() -> remoteServer.addressQuery(SimpleString.of(getTestName())).isExists());

         // Captures state of JMS consumers and bridge consumers attached on each node
         Wait.assertTrue(() -> server.bindingQuery(SimpleString.of("target"), false).getQueueNames().size() >= 1);
         Wait.assertTrue(() -> remoteServer.bindingQuery(SimpleString.of(getTestName()), false).getQueueNames().size() >= 1);

         final MessageProducer producerR = sessionR.createProducer(source);
         final TextMessage message = sessionR.createTextMessage("Hello World");

         message.setStringProperty("testProperty", "testValue");

         producerR.send(message);

         final Message received = consumerL.receive(5_000);
         assertNotNull(received);
         assertTrue(received instanceof TextMessage);
         assertEquals("Hello World", ((TextMessage) received).getText());
         assertTrue(message.propertyExists("testProperty"));
         assertEquals("testValue", received.getStringProperty("testProperty"));
      }
   }

   @Test
   @Timeout(20)
   public void testQueueDemandOnLocalBrokerBridgesMessagesFromRemoteAMQP() throws Exception {
      testQueueDemandOnLocalBrokerBridgesMessagesFromRemote("AMQP");
   }

   @Test
   @Timeout(20)
   public void testQueueDemandOnLocalBrokerBridgesMessagesFromRemoteCORE() throws Exception {
      testQueueDemandOnLocalBrokerBridgesMessagesFromRemote("CORE");
   }

   private void testQueueDemandOnLocalBrokerBridgesMessagesFromRemote(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPBridgeQueuePolicyElement bridgeQueuePolicy = new AMQPBridgeQueuePolicyElement();
      bridgeQueuePolicy.setName("test-policy");
      bridgeQueuePolicy.addToIncludes("#", getTestName());

      final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
      element.setName(getTestName());
      element.addBridgeFromQueuePolicy(bridgeQueuePolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration(getTestName(), "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      server.getConfiguration().addAMQPConnection(amqpConnection);
      remoteServer.start();
      remoteServer.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                   .setAddress(getTestName())
                                                                   .setAutoCreated(false));
      server.start();

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final Queue queue = sessionL.createQueue(getTestName());

         final MessageConsumer consumerL = sessionL.createConsumer(queue);

         connectionL.start();
         connectionR.start();

         // Demand on local queue should trigger receiver on remote.
         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).isExists());

         final MessageProducer producerR = sessionR.createProducer(queue);
         final TextMessage message = sessionR.createTextMessage("Hello World");

         message.setStringProperty("testProperty", "testValue");

         producerR.send(message);

         final Message received = consumerL.receive(5_000);
         assertNotNull(received);
         assertTrue(received instanceof TextMessage);
         assertEquals("Hello World", ((TextMessage) received).getText());
         assertTrue(message.propertyExists("testProperty"));
         assertEquals("testValue", received.getStringProperty("testProperty"));
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeToAddressOnLocalBrokerBridgesMessagesFromLocalAMQP() throws Exception {
      testBridgeToAddressOnLocalBrokerBridgesMessagesFromLocal("AMQP");
   }

   @Test
   @Timeout(20)
   public void testBridgeToAddressOnLocalBrokerBridgesMessagesFromLocalCore() throws Exception {
      testBridgeToAddressOnLocalBrokerBridgesMessagesFromLocal("CORE");
   }

   private void testBridgeToAddressOnLocalBrokerBridgesMessagesFromLocal(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPBridgeAddressPolicyElement bridgeAddressPolicy = new AMQPBridgeAddressPolicyElement();
      bridgeAddressPolicy.setName("test-policy");
      bridgeAddressPolicy.addToIncludes(getTestName());

      final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
      element.setName(getTestName());
      element.addBridgeToAddressPolicy(bridgeAddressPolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration(getTestName(), "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      server.getConfiguration().addAMQPConnection(amqpConnection);
      remoteServer.start();
      server.start();

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final Topic topic = sessionL.createTopic(getTestName());

         final MessageConsumer consumerR = sessionR.createConsumer(topic);

         connectionL.start();
         connectionR.start();

         // Remote consumer is attached and ready for the bridged message
         Wait.assertTrue(() -> remoteServer.addressQuery(SimpleString.of(getTestName())).isExists());
         Wait.assertTrue(() -> remoteServer.bindingQuery(SimpleString.of(getTestName()), false).getQueueNames().size() >= 1);

         // We need to add the address before the bridge will start routing message to the remote.
         server.addAddressInfo(new AddressInfo(SimpleString.of(getTestName()), RoutingType.MULTICAST));
         // The bridge has been notified and has created a local consumer to bridge to the remote.
         Wait.assertTrue(() -> server.bindingQuery(SimpleString.of(getTestName()), false).getQueueNames().size() >= 1);

         final MessageProducer producerL = sessionL.createProducer(topic);
         final TextMessage message = sessionL.createTextMessage("Hello World");

         message.setStringProperty("testProperty", "testValue");

         producerL.send(message);

         final Message received = consumerR.receive(5_000);
         assertNotNull(received);
         assertTrue(received instanceof TextMessage);
         assertEquals("Hello World", ((TextMessage) received).getText());
         assertTrue(message.propertyExists("testProperty"));
         assertEquals("testValue", received.getStringProperty("testProperty"));
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueToMessageFromLocalToRemoteAMQP() throws Exception {
      testBridgeQueueToMessageFromLocalToRemote("AMQP");
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueToMessageFromLocalToRemoteCore() throws Exception {
      testBridgeQueueToMessageFromLocalToRemote("CORE");
   }

   public void testBridgeQueueToMessageFromLocalToRemote(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPBridgeQueuePolicyElement bridgeQueuePolicy = new AMQPBridgeQueuePolicyElement();
      bridgeQueuePolicy.setName("test-policy");
      bridgeQueuePolicy.addToIncludes("#", getTestName());

      final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
      element.setName(getTestName());
      element.addBridgeToQueuePolicy(bridgeQueuePolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration(getTestName(), "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      server.getConfiguration().addAMQPConnection(amqpConnection);
      remoteServer.start();
      server.start();

      // Need to define the right type on the remote in order to get expected results
      remoteServer.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                   .setAddress(getTestName())
                                                                   .setAutoCreated(false));

      server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                             .setAddress(getTestName())
                                                             .setAutoCreated(false));

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final Queue queue = sessionL.createQueue(getTestName());

         final MessageConsumer consumerR = sessionR.createConsumer(queue);

         connectionL.start();
         connectionR.start();

         // Demand on remote queue should trigger receiver on remote.
         Wait.assertTrue(() -> remoteServer.queueQuery(SimpleString.of(getTestName())).isExists());

         final MessageProducer producerL = sessionL.createProducer(queue);
         final TextMessage message = sessionL.createTextMessage("Hello World");

         message.setStringProperty("testProperty", "testValue");

         producerL.send(message);

         final Message received = consumerR.receive(5_000);
         assertNotNull(received);
         assertTrue(received instanceof TextMessage);
         assertEquals("Hello World", ((TextMessage) received).getText());
         assertTrue(message.propertyExists("testProperty"));
         assertEquals("testValue", received.getStringProperty("testProperty"));
      }
   }

   @Test
   @Timeout(20)
   public void testQueueDemandOnLocalBrokerBridgesMatchingFilteredMessagesFromRemoteAMQP() throws Exception {
      testQueueDemandOnLocalBrokerBridgesMatchingFilteredMessagesFromRemote("AMQP");
   }

   @Test
   @Timeout(20)
   public void testQueueDemandOnLocalBrokerBridgesMatchingFilteredMessagesFromRemoteCORE() throws Exception {
      testQueueDemandOnLocalBrokerBridgesMatchingFilteredMessagesFromRemote("CORE");
   }

   private void testQueueDemandOnLocalBrokerBridgesMatchingFilteredMessagesFromRemote(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPBridgeQueuePolicyElement bridgeQueuePolicy = new AMQPBridgeQueuePolicyElement();
      bridgeQueuePolicy.setName("test-policy");
      bridgeQueuePolicy.addToIncludes("#", getTestName());

      final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
      element.setName(getTestName());
      element.addBridgeFromQueuePolicy(bridgeQueuePolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration(getTestName(), "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      server.getConfiguration().addAMQPConnection(amqpConnection);
      remoteServer.start();
      remoteServer.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                   .setAddress(getTestName())
                                                                   .setFilterString("color='red' OR color='green' OR color='blue'")
                                                                   .setAutoCreated(false));
      server.start();

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final Queue queue = sessionL.createQueue(getTestName());

         final MessageConsumer consumerL1 = sessionL.createConsumer(queue, "color='red'");
         final MessageConsumer consumerL2 = sessionL.createConsumer(queue, "color='blue'");

         connectionL.start();
         connectionR.start();

         // Demand on local queue should trigger receiver on remote.
         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).isExists());

         final MessageProducer producerR = sessionR.createProducer(queue);

         final TextMessage message1 = sessionR.createTextMessage("Hello World 1");
         message1.setStringProperty("color", "green");
         final TextMessage message2 = sessionR.createTextMessage("Hello World 2");
         message2.setStringProperty("color", "red");
         final TextMessage message3 = sessionR.createTextMessage("Hello World 3");
         message3.setStringProperty("color", "blue");

         producerR.send(message1);
         producerR.send(message2);
         producerR.send(message3);

         final Message receivedL1 = consumerL1.receive(5_000);
         assertNotNull(receivedL1);
         assertTrue(receivedL1 instanceof TextMessage);
         assertEquals("Hello World 2", ((TextMessage) receivedL1).getText());
         assertTrue(receivedL1.propertyExists("color"));
         assertEquals("red", receivedL1.getStringProperty("color"));

         final Message receivedL2 = consumerL2.receive(5_000);
         assertNotNull(receivedL2);
         assertTrue(receivedL2 instanceof TextMessage);
         assertEquals("Hello World 3", ((TextMessage) receivedL2).getText());
         assertTrue(receivedL2.propertyExists("color"));
         assertEquals("blue", receivedL2.getStringProperty("color"));

         // See if the green message is still on the remote where it should be as the
         // filter should prevent it from moving across the federation link(s)
         final MessageConsumer consumerR = sessionR.createConsumer(queue, "color='green'");

         final Message receivedR = consumerR.receive(5_000);
         assertNotNull(receivedR);
         assertTrue(receivedR instanceof TextMessage);
         assertEquals("Hello World 1", ((TextMessage) receivedR).getText());
         assertTrue(receivedR.propertyExists("color"));
         assertEquals("green", receivedR.getStringProperty("color"));
      }
   }

   @RepeatedTest(1)
   @Timeout(20)
   public void testTwoPullConsumerOnPullingBridgeConfigurationEachCanTakeOneMessageProduceOnLocal() throws Exception {
      doTestTwoPullConsumerOnPullingBridgeConfigurationEachCanTakeOneMessage(true);
   }

   @RepeatedTest(1)
   @Timeout(20)
   public void testTwoPullConsumerOnPullingBridgeConfigurationEachCanTakeOneMessageProduceOnRemote() throws Exception {
      doTestTwoPullConsumerOnPullingBridgeConfigurationEachCanTakeOneMessage(false);
   }

   public void doTestTwoPullConsumerOnPullingBridgeConfigurationEachCanTakeOneMessage(boolean produceLocal) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPBridgeQueuePolicyElement bridgeQueuePolicy = new AMQPBridgeQueuePolicyElement();
      bridgeQueuePolicy.setName("bridge-queue-policy");
      bridgeQueuePolicy.addToIncludes(getTestName(), getTestName());
      bridgeQueuePolicy.addProperty(RECEIVER_CREDITS, 0);         // Enable Pull mode
      bridgeQueuePolicy.addProperty(PULL_RECEIVER_BATCH_SIZE, 1); // Pull mode batch is one

      final AMQPBridgeBrokerConnectionElement element1 = new AMQPBridgeBrokerConnectionElement();
      element1.setName("Bridge-Messages-From-Remote");
      element1.addBridgeFromQueuePolicy(bridgeQueuePolicy);

      final AMQPBridgeBrokerConnectionElement element2 = new AMQPBridgeBrokerConnectionElement();
      element2.setName("Bridge-Messages-From-Local");
      element2.addBridgeFromQueuePolicy(bridgeQueuePolicy);

      final AMQPBrokerConnectConfiguration amqpConnection1 =
         new AMQPBrokerConnectConfiguration(getTestName(), "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection1.setReconnectAttempts(10);// Limit reconnects
      amqpConnection1.addElement(element1);

      final AMQPBrokerConnectConfiguration amqpConnection2 =
         new AMQPBrokerConnectConfiguration(getTestName(), "tcp://localhost:" + SERVER_PORT);
      amqpConnection2.setReconnectAttempts(10);// Limit reconnects
      amqpConnection2.addElement(element2);

      server.getConfiguration().addAMQPConnection(amqpConnection1);
      remoteServer.getConfiguration().addAMQPConnection(amqpConnection2);

      remoteServer.start();
      remoteServer.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                   .setAddress(getTestName())
                                                                   .setAutoCreated(false));
      server.start();
      server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                             .setAddress(getTestName())
                                                             .setAutoCreated(false));

      final int MESSAGE_COUNT = 2;
      final JmsConnectionFactory factory;
      if (produceLocal) {
         factory = new JmsConnectionFactory("amqp://localhost:" + SERVER_PORT);
      } else {
         factory = new JmsConnectionFactory("amqp://localhost:" + SERVER_PORT_REMOTE);
      }

      try (Connection connection = factory.createConnection();
           Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {

         final Queue queue = session.createQueue(getTestName());
         final MessageProducer producer = session.createProducer(queue);

         for (int i = 0; i < MESSAGE_COUNT; ++i) {
            TextMessage message = session.createTextMessage("test-message:" + i);

            message.setIntProperty("messageNo", i);

            producer.send(message);
         }
      }

      final JmsConnectionFactory factoryLocal = new JmsConnectionFactory(
         "amqp://localhost:" + SERVER_PORT + "?jms.prefetchPolicy.all=0");
      final JmsConnectionFactory factoryRemote = new JmsConnectionFactory(
         "amqp://localhost:" + SERVER_PORT_REMOTE + "?jms.prefetchPolicy.all=0");

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection();
           Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
           Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE)) {

         connectionL.start();
         connectionR.start();

         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).isExists(), 10_000);
         Wait.assertTrue(() -> remoteServer.queueQuery(SimpleString.of(getTestName())).isExists(), 10_000);

         final Queue queue = sessionL.createQueue(getTestName());
         final MessageConsumer consumerL = sessionL.createConsumer(queue);
         final MessageConsumer consumerR = sessionR.createConsumer(queue);

         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).getConsumerCount() >= 2, 10_000);
         Wait.assertTrue(() -> remoteServer.queueQuery(SimpleString.of(getTestName())).getConsumerCount() >= 2, 10_000);

         final TextMessage messageL = (TextMessage) consumerL.receive(2_000); // Read from local
         final TextMessage messageR = (TextMessage) consumerR.receive(2_000); // Read from remote after federated

         assertNotNull(messageL);
         assertNotNull(messageR);
      }
   }

   @Test
   @Timeout(20)
   public void testCoreConsumerDemandOnLocalBrokerBridgesMessageFromAMQPClient() throws Exception {
      testCoreConsumerDemandOnLocalBrokerBridgesMessageFromAMQPClient("CORE", "AMQP", false); // Tunneling doesn't matter here
   }

   @Test
   @Timeout(20)
   public void testCoreConsumerDemandOnLocalBrokerBridgesMessageFromCoreClientTunneled() throws Exception {
      testCoreConsumerDemandOnLocalBrokerBridgesMessageFromAMQPClient("CORE", "CORE", true);
   }

   @Test
   @Timeout(20)
   public void testCoreConsumerDemandOnLocalBrokerBridgesMessageFromCoreClientUnTunneled() throws Exception {
      testCoreConsumerDemandOnLocalBrokerBridgesMessageFromAMQPClient("CORE", "CORE", false);
   }

   @Test
   @Timeout(20)
   public void testAMQPConsumerDemandOnLocalBrokerBridgesMessageFromCoreClientTunneled() throws Exception {
      testCoreConsumerDemandOnLocalBrokerBridgesMessageFromAMQPClient("AMQP", "CORE", true);
   }

   @Test
   @Timeout(20)
   public void testAMQPConsumerDemandOnLocalBrokerBridgesMessageFromCoreClientNotTunneled() throws Exception {
      testCoreConsumerDemandOnLocalBrokerBridgesMessageFromAMQPClient("AMQP", "CORE", false);
   }

   private void testCoreConsumerDemandOnLocalBrokerBridgesMessageFromAMQPClient(String localProtocol,
                                                                                String remoteProtocol,
                                                                                boolean enableCoreTunneling) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPBridgeQueuePolicyElement localQueuePolicy = new AMQPBridgeQueuePolicyElement();
      localQueuePolicy.setName("test-policy");
      localQueuePolicy.addToIncludes("#", getTestName());
      localQueuePolicy.addProperty(AmqpSupport.TUNNEL_CORE_MESSAGES, Boolean.toString(enableCoreTunneling));

      final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
      element.setName(getTestName());
      element.addBridgeFromQueuePolicy(localQueuePolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration(getTestName(), "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      server.getConfiguration().addAMQPConnection(amqpConnection);
      remoteServer.start();
      remoteServer.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                   .setAddress(getTestName())
                                                                   .setAutoCreated(false));
      server.start();

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(localProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(remoteProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final MessageConsumer consumerL = sessionL.createConsumer(sessionL.createQueue(getTestName()));

         connectionL.start();
         connectionR.start();

         // Demand on local address should trigger receiver on remote.
         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).isExists());
         Wait.assertTrue(() -> remoteServer.queueQuery(SimpleString.of(getTestName())).isExists());

         final MessageProducer producerR = sessionR.createProducer(sessionR.createQueue(getTestName()));
         final BytesMessage message = sessionR.createBytesMessage();
         final byte[] bodyBytes = new byte[(int)(MIN_LARGE_MESSAGE_SIZE * 1.5)];

         Arrays.fill(bodyBytes, (byte)1);

         message.writeBytes(bodyBytes);
         message.setStringProperty("testProperty", "testValue");
         message.setIntProperty("testIntProperty", 42);
         message.setJMSCorrelationID("myCorrelationId");
         message.setJMSReplyTo(sessionR.createTopic("reply-topic"));

         producerR.setDeliveryMode(DeliveryMode.PERSISTENT);
         producerR.send(message);

         final Message received = consumerL.receive(5_000);
         assertNotNull(received);
         assertInstanceOf(BytesMessage.class, received);

         final byte[] receivedBytes = new byte[bodyBytes.length];
         final BytesMessage receivedBytesMsg = (BytesMessage) received;
         receivedBytesMsg.readBytes(receivedBytes);

         assertArrayEquals(bodyBytes, receivedBytes);
         assertTrue(message.propertyExists("testProperty"));
         assertEquals("testValue", received.getStringProperty("testProperty"));
         assertTrue(message.propertyExists("testIntProperty"));
         assertEquals(42, received.getIntProperty("testIntProperty"));
         assertEquals("myCorrelationId", received.getJMSCorrelationID());
         assertEquals("reply-topic", ((Topic) received.getJMSReplyTo()).getTopicName());
         assertEquals(DeliveryMode.PERSISTENT, received.getJMSDeliveryMode());
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeToCoreConsumerOnRemoteBrokerMessageFromAMQPClient() throws Exception {
      testBridgeToConsumerOnRemoteBrokerMessageFromLocalProducer("AMQP", "CORE", false); // Tunneling doesn't matter here
   }

   @Test
   @Timeout(20)
   public void testBridgeToCoreConsumerOnRemoteBrokerBridgesMessageFromCoreClientTunneled() throws Exception {
      testBridgeToConsumerOnRemoteBrokerMessageFromLocalProducer("CORE", "CORE", true);
   }

   @Test
   @Timeout(20)
   public void testBridgeToCoreConsumerOnRemoteBrokerBridgesMessageFromCoreClientUnTunneled() throws Exception {
      testBridgeToConsumerOnRemoteBrokerMessageFromLocalProducer("CORE", "CORE", false);
   }

   @Test
   @Timeout(20)
   public void testBridgeToAMQPConsumerOnRemoteBrokerBridgesMessageFromCoreClientTunneled() throws Exception {
      testBridgeToConsumerOnRemoteBrokerMessageFromLocalProducer("CORE", "AMQP", true);
   }

   @Test
   @Timeout(20)
   public void testBridgeToAMQPConsumerOnRemoteBrokerBridgesMessageFromCoreClientNotTunneled() throws Exception {
      testBridgeToConsumerOnRemoteBrokerMessageFromLocalProducer("CORE", "AMQP", false);
   }

   private void testBridgeToConsumerOnRemoteBrokerMessageFromLocalProducer(String localProtocol,
                                                                           String remoteProtocol,
                                                                           boolean enableCoreTunneling) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPBridgeQueuePolicyElement localQueuePolicy = new AMQPBridgeQueuePolicyElement();
      localQueuePolicy.setName("test-policy");
      localQueuePolicy.addToIncludes("#", getTestName());
      localQueuePolicy.addProperty(AmqpSupport.TUNNEL_CORE_MESSAGES, Boolean.toString(enableCoreTunneling));

      final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
      element.setName(getTestName());
      element.addBridgeToQueuePolicy(localQueuePolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration(getTestName(), "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      server.getConfiguration().addAMQPConnection(amqpConnection);
      remoteServer.start();
      remoteServer.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                   .setAddress(getTestName())
                                                                   .setAutoCreated(false));
      server.start();
      server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                             .setAddress(getTestName())
                                                             .setAutoCreated(false));

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(localProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(remoteProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final MessageConsumer consumerR = sessionR.createConsumer(sessionR.createQueue(getTestName()));

         connectionL.start();
         connectionR.start();

         // Demand on local address should trigger receiver on remote.
         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).isExists());
         Wait.assertTrue(() -> remoteServer.queueQuery(SimpleString.of(getTestName())).isExists());

         final MessageProducer producerL = sessionL.createProducer(sessionL.createQueue(getTestName()));
         final BytesMessage message = sessionL.createBytesMessage();
         final byte[] bodyBytes = new byte[(int)(MIN_LARGE_MESSAGE_SIZE * 1.5)];

         Arrays.fill(bodyBytes, (byte)1);

         message.writeBytes(bodyBytes);
         message.setStringProperty("testProperty", "testValue");
         message.setIntProperty("testIntProperty", 42);
         message.setJMSCorrelationID("myCorrelationId");
         message.setJMSReplyTo(sessionL.createTopic("reply-topic"));

         producerL.setDeliveryMode(DeliveryMode.PERSISTENT);
         producerL.send(message);

         final Message received = consumerR.receive(5_000);
         assertNotNull(received);
         assertInstanceOf(BytesMessage.class, received);

         final byte[] receivedBytes = new byte[bodyBytes.length];
         final BytesMessage receivedBytesMsg = (BytesMessage) received;
         receivedBytesMsg.readBytes(receivedBytes);

         assertArrayEquals(bodyBytes, receivedBytes);
         assertTrue(message.propertyExists("testProperty"));
         assertEquals("testValue", received.getStringProperty("testProperty"));
         assertTrue(message.propertyExists("testIntProperty"));
         assertEquals(42, received.getIntProperty("testIntProperty"));
         assertEquals("myCorrelationId", received.getJMSCorrelationID());
         assertEquals("reply-topic", ((Topic) received.getJMSReplyTo()).getTopicName());
         assertEquals(DeliveryMode.PERSISTENT, received.getJMSDeliveryMode());
      }
   }
}
