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

import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADDRESS_AUTO_DELETE;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADDRESS_AUTO_DELETE_DELAY;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADDRESS_AUTO_DELETE_MSG_COUNT;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADDRESS_ENABLE_DIVERT_BINDINGS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADDRESS_INCLUDES;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADDRESS_MAX_HOPS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADD_ADDRESS_POLICY;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADD_QUEUE_POLICY;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_ADDRESS_RECEIVER;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONTROL_LINK;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_EVENT_LINK;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_POLICY_NAME;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_QUEUE_RECEIVER;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.OPERATION_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.POLICY_NAME;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.QUEUE_INCLUDES;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.QUEUE_INCLUDE_FEDERATED;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.QUEUE_PRIORITY_ADJUSTMENT;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.BrokerConnectionControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederatedBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationAddressPolicyElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationQueuePolicyElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConsumerControl;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationControl;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationLocalPolicyControl;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationManagementSupport;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationProducerControl;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationRemotePolicyControlType;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationLocalPolicyControlType;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.MessageAnnotationsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.TransferPayloadCompositeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that the broker create management objects for federation configurations.
 */
class AMQPFederationManagementTest extends AmqpClientTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,CORE";
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      // Creates the broker used to make the outgoing connection. The port passed is for
      // that brokers acceptor. The test server connected to by the broker binds to a random port.
      return createServer(AMQP_PORT, false);
   }

   @Test
   @Timeout(20)
   public void testFederationCreatesManagementResourcesForAddressPolicyConfigurations() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofReceiver()
                            .withSenderSettleModeSettled()
                            .withSource().withDynamic(true)
                            .and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind()
                            .withTarget().withAddress("test-dynamic-events");
         peer.expectFlow().withLinkCredit(10);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes(getTestName());

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString(getTestName()),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.MULTICAST)
                                                                .setAddress(getTestName())
                                                                .setAutoCreated(false));

         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).isExists());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         final String brokerConnectionName = ResourceNames.BROKER_CONNECTION + getTestName();

         final BrokerConnectionControl brokerConnection = (BrokerConnectionControl)
            server.getManagementService().getResource(brokerConnectionName);

         assertNotNull(brokerConnection);
         assertTrue(brokerConnection.isConnected());

         final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "address-policy");
         final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName(getTestName(), "address-policy", getTestName());

         final AMQPFederationControl federationControl =
            (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);
         final AMQPFederationLocalPolicyControl addressPolicyControl =
            (AMQPFederationLocalPolicyControl) server.getManagementService().getResource(policyResourceName);

         assertNotNull(federationControl);
         assertEquals(getTestName(), federationControl.getName());
         assertEquals(0, federationControl.getMessagesReceived());
         assertEquals(0, federationControl.getMessagesSent());

         assertNotNull(addressPolicyControl);
         assertEquals("address-policy", addressPolicyControl.getName());
         assertEquals(0, addressPolicyControl.getMessagesReceived());

         final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
            server.getManagementService().getResource(consumerResourceName);

         assertNotNull(consumerControl);
         assertEquals(getTestName(), consumerControl.getAddress());
         assertEquals(0, consumerControl.getMessagesReceived());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         brokerConnection.stop();

         // Stopping the connection should remove the federation consumer management objects.
         Wait.assertTrue(() -> server.getManagementService().getResource(consumerResourceName) == null, 5_000, 100);

         assertNotNull(server.getManagementService().getResource(policyResourceName));

         server.getBrokerConnections().forEach((connection) -> {
            try {
               connection.shutdown();
            } catch (Exception e) {
               fail("Broker connection shutdown should not have thrown an exception");
            }
         });

         Wait.assertTrue(() -> server.getManagementService().getResource(policyResourceName) == null, 5_000, 100);
         Wait.assertTrue(() -> server.getManagementService().getResource(federationResourceName) == null, 5_000, 100);
         Wait.assertTrue(() -> server.getManagementService().getResource(brokerConnectionName) == null, 5_000, 100);

         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testFederationRemovesAddressConsumerManagementWhenConnectionDrops() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofReceiver()
                            .withSenderSettleModeSettled()
                            .withSource().withDynamic(true)
                            .and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind()
                            .withTarget().withAddress("test-dynamic-events");
         peer.expectFlow().withLinkCredit(10);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes(getTestName());

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString(getTestName()),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.MULTICAST)
                                                                .setAddress(getTestName())
                                                                .setAutoCreated(false));

         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).isExists());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         final BrokerConnectionControl brokerConnection = (BrokerConnectionControl)
            server.getManagementService().getResource(ResourceNames.BROKER_CONNECTION + getTestName());

         assertNotNull(brokerConnection);
         assertTrue(brokerConnection.isConnected());

         final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "address-policy");
         final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName(getTestName(), "address-policy", getTestName());

         final AMQPFederationControl federationControl =
            (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);
         final AMQPFederationLocalPolicyControl addressPolicyControl =
            (AMQPFederationLocalPolicyControl) server.getManagementService().getResource(policyResourceName);
         final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
            server.getManagementService().getResource(consumerResourceName);

         assertNotNull(federationControl);
         assertNotNull(addressPolicyControl);
         assertNotNull(consumerControl);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         // Closing the connection without a detach or close frame should still cleanup the management
         // resources for the consumer but the policy views should still be in place
         Wait.assertTrue(() -> server.getManagementService().getResource(consumerResourceName) == null, 5_000, 100);
         Wait.assertTrue(() -> server.getManagementService().getResource(policyResourceName) != null, 5_000, 100);
      }
   }

   @Test
   @Timeout(20)
   public void testFederationRemovesAddressConsumerManagementWhenBrokerConnectionStopped() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofReceiver()
                            .withSenderSettleModeSettled()
                            .withSource().withDynamic(true)
                            .and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind()
                            .withTarget().withAddress("test-dynamic-events");
         peer.expectFlow().withLinkCredit(10);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes(getTestName());

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString(getTestName()),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.MULTICAST)
                                                                .setAddress(getTestName())
                                                                .setAutoCreated(false));

         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).isExists());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         final BrokerConnectionControl brokerConnection = (BrokerConnectionControl)
            server.getManagementService().getResource(ResourceNames.BROKER_CONNECTION + getTestName());

         assertNotNull(brokerConnection);
         assertTrue(brokerConnection.isConnected());

         final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "address-policy");
         final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName(getTestName(), "address-policy", getTestName());

         final AMQPFederationControl federationControl =
            (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);
         final AMQPFederationLocalPolicyControl addressPolicyControl =
            (AMQPFederationLocalPolicyControl) server.getManagementService().getResource(policyResourceName);
         final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
            server.getManagementService().getResource(consumerResourceName);

         assertNotNull(federationControl);
         assertNotNull(addressPolicyControl);
         assertNotNull(consumerControl);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectConnectionToDrop();

         brokerConnection.stop();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         // Closing the connection without a detach or close frame should still cleanup the management
         // resources for the consumer but the policy views should still be in place
         Wait.assertTrue(() -> server.getManagementService().getResource(consumerResourceName) == null, 5_000, 100);

         assertNotNull(server.getManagementService().getResource(policyResourceName));

         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testFederationCreatesManagementResourcesForQueuePolicyConfigurations() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofReceiver()
                            .withSenderSettleModeSettled()
                            .withSource().withDynamic(true)
                            .and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind()
                            .withTarget().withAddress("test-dynamic-events");
         peer.expectFlow().withLinkCredit(10);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("#", getTestName());
         receiveFromQueue.setPriorityAdjustment(1);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString(getTestName()),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress(getTestName())
                                                                .setAutoCreated(false));

         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).isExists());

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue(getTestName()));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final String brokerConnectionName = ResourceNames.BROKER_CONNECTION + getTestName();
            final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
            final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "queue-policy");
            final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName(getTestName(), "queue-policy", getTestName() + "::" + getTestName());

            final BrokerConnectionControl brokerConnection = (BrokerConnectionControl)
               server.getManagementService().getResource(brokerConnectionName);
            final AMQPFederationControl federationControl =
               (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);

            assertNotNull(brokerConnection);
            assertTrue(brokerConnection.isConnected());
            assertNotNull(federationControl);
            assertEquals(getTestName(), federationControl.getName());
            assertEquals(0, federationControl.getMessagesReceived());
            assertEquals(0, federationControl.getMessagesSent());

            final AMQPFederationLocalPolicyControlType queuePolicyControl =
               (AMQPFederationLocalPolicyControlType) server.getManagementService().getResource(policyResourceName);

            assertNotNull(queuePolicyControl);
            assertEquals("queue-policy", queuePolicyControl.getName());
            assertEquals(0, queuePolicyControl.getMessagesReceived());

            final AMQPFederationConsumerControl consumerControl =
               (AMQPFederationConsumerControl) server.getManagementService().getResource(consumerResourceName);

            assertNotNull(consumerControl);
            assertEquals(getTestName(), consumerControl.getAddress());
            assertEquals(getTestName(), consumerControl.getQueueName());
            assertEquals(getTestName() + "::" + getTestName(), consumerControl.getFqqn());
            assertEquals(0, consumerControl.getMessagesReceived());
            assertEquals(1, consumerControl.getPriority());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            brokerConnection.stop();

            // Stopping the connection should remove the federation management objects.
            Wait.assertTrue(() -> server.getManagementService().getResource(consumerResourceName) == null, 5_000, 100);

            assertNotNull(server.getManagementService().getResource(policyResourceName));

            server.getBrokerConnections().forEach((brConnection) -> {
               try {
                  brConnection.shutdown();
               } catch (Exception e) {
                  fail("Broker connection shutdown should not have thrown an exception");
               }
            });

            Wait.assertTrue(() -> server.getManagementService().getResource(policyResourceName) == null, 5_000, 100);
            Wait.assertTrue(() -> server.getManagementService().getResource(federationResourceName) == null, 5_000, 100);
            Wait.assertTrue(() -> server.getManagementService().getResource(brokerConnectionName) == null, 5_000, 100);

            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testFederationRemovesQueueConsumerManagementWhenConnectionDrops() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofReceiver()
                            .withSenderSettleModeSettled()
                            .withSource().withDynamic(true)
                            .and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind()
                            .withTarget().withAddress("test-dynamic-events");
         peer.expectFlow().withLinkCredit(10);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("#", getTestName());
         receiveFromQueue.setPriorityAdjustment(1);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString(getTestName()),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress(getTestName())
                                                                .setAutoCreated(false));

         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).isExists());

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue(getTestName()));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
            final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "queue-policy");
            final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName(getTestName(), "queue-policy", getTestName() + "::" + getTestName());

            final BrokerConnectionControl brokerConnection = (BrokerConnectionControl)
               server.getManagementService().getResource(ResourceNames.BROKER_CONNECTION + getTestName());

            assertNotNull(brokerConnection);
            assertTrue(brokerConnection.isConnected());

            final AMQPFederationControl federationControl =
               (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);
            final AMQPFederationLocalPolicyControlType queuePolicyControl =
               (AMQPFederationLocalPolicyControlType) server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl =
               (AMQPFederationConsumerControl) server.getManagementService().getResource(consumerResourceName);

            assertNotNull(federationControl);
            assertNotNull(queuePolicyControl);
            assertNotNull(consumerControl);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();

            // Closing the connection without a detach or close frame should still cleanup the management
            // resources for the consumer but the policy views should still be in place
            Wait.assertTrue(() -> server.getManagementService().getResource(consumerResourceName) == null, 5_000, 100);
            Wait.assertTrue(() -> server.getManagementService().getResource(policyResourceName) != null, 5_000, 100);
         }
      }
   }

   @Test
   @Timeout(20)
   public void testFederationRemovesQueueConsumerManagementWhenBrokerConnectionStopped() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofReceiver()
                            .withSenderSettleModeSettled()
                            .withSource().withDynamic(true)
                            .and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind()
                            .withTarget().withAddress("test-dynamic-events");
         peer.expectFlow().withLinkCredit(10);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("#", getTestName());
         receiveFromQueue.setPriorityAdjustment(1);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString(getTestName()),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress(getTestName())
                                                                .setAutoCreated(false));

         Wait.assertTrue(() -> server.queueQuery(SimpleString.of(getTestName())).isExists());

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue(getTestName()));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
            final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "queue-policy");
            final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName(getTestName(), "queue-policy", getTestName() + "::" + getTestName());

            final BrokerConnectionControl brokerConnection = (BrokerConnectionControl)
               server.getManagementService().getResource(ResourceNames.BROKER_CONNECTION + getTestName());

            assertNotNull(brokerConnection);
            assertTrue(brokerConnection.isConnected());

            final AMQPFederationControl federationControl =
               (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);
            final AMQPFederationLocalPolicyControlType queuePolicyControl =
               (AMQPFederationLocalPolicyControlType) server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl =
               (AMQPFederationConsumerControl) server.getManagementService().getResource(consumerResourceName);

            assertNotNull(federationControl);
            assertNotNull(queuePolicyControl);
            assertNotNull(consumerControl);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectConnectionToDrop();

            brokerConnection.stop();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Closing the connection without a detach or close frame should still cleanup the management
            // resources for the consumer but the policy views should still be in place
            Wait.assertTrue(() -> server.getManagementService().getResource(consumerResourceName) == null, 5_000, 100);

            assertNotNull(server.getManagementService().getResource(policyResourceName));

            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testAddressManagementTracksMessagesAtPolicyAndConsumerLevels() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respondInKind();
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind();
         peer.expectFlow().withLinkCredit(10);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes(getTestName());

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.of(getTestName()), RoutingType.MULTICAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString(getTestName()),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(0)
                              .queue();
         peer.expectDisposition().withSettled(true).withState().accepted();

         final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "address-policy");
         final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName(getTestName(), "address-policy", getTestName());

         final AMQPFederationControl federationControl =
            (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic(getTestName()));

            connection.start();

            final Message message = consumer.receive(5_000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final AMQPFederationLocalPolicyControl addressPolicyControl = (AMQPFederationLocalPolicyControl)
               server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
               server.getManagementService().getResource(consumerResourceName);

            assertEquals(1, federationControl.getMessagesReceived());
            assertEquals(1, addressPolicyControl.getMessagesReceived());
            assertEquals(1, consumerControl.getMessagesReceived());

            peer.expectFlow().withLinkCredit(999).withDrain(true)
                             .respond()
                             .withLinkCredit(0).withDeliveryCount(1000).withDrain(true);
            peer.expectDetach().respond(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         // Policy bean should still be active but consumer bean should be unregistered
         {
            final AMQPFederationLocalPolicyControl addressPolicyControl = (AMQPFederationLocalPolicyControl)
               server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
               server.getManagementService().getResource(consumerResourceName);

            assertNotNull(addressPolicyControl);
            assertNull(consumerControl);
         }

         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString(getTestName()),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(1)
                              .queue();
         peer.expectDisposition().withSettled(true).withState().accepted();

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic(getTestName()));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final Message message = consumer.receive(5_000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final AMQPFederationLocalPolicyControl addressPolicyControl = (AMQPFederationLocalPolicyControl)
               server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
               server.getManagementService().getResource(consumerResourceName);

            assertEquals(2, federationControl.getMessagesReceived());
            assertEquals(2, addressPolicyControl.getMessagesReceived());
            assertEquals(1, consumerControl.getMessagesReceived());

            peer.expectFlow().withLinkCredit(999).withDrain(true)
                             .respond()
                             .withLinkCredit(0).withDeliveryCount(1000).withDrain(true);
            peer.expectDetach().respond(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testQueueManagementTracksMessagesAtPolicyAndConsumerLevels() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respondInKind();
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind();
         peer.expectFlow().withLinkCredit(10);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes(getTestName(), getTestName());

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString(getTestName()),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(0)
                              .queue();
         peer.expectDisposition().withSettled(true).withState().accepted();

         server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress(getTestName())
                                                                .setAutoCreated(false));

         final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "queue-policy");
         final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName(getTestName(), "queue-policy", getTestName() + "::" + getTestName());

         final AMQPFederationControl federationControl =
            (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createQueue(getTestName()));

            connection.start();

            final Message message = consumer.receive(5_000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final AMQPFederationLocalPolicyControlType queuePolicyControl = (AMQPFederationLocalPolicyControlType)
               server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
               server.getManagementService().getResource(consumerResourceName);

            assertEquals(1, federationControl.getMessagesReceived());
            assertEquals(1, queuePolicyControl.getMessagesReceived());
            assertEquals(1, consumerControl.getMessagesReceived());

            peer.expectFlow().withLinkCredit(999).withDrain(true)
                             .respond()
                             .withLinkCredit(0).withDeliveryCount(1000).withDrain(true);
            peer.expectDetach().respond(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         // Policy bean should still be active but consumer bean should be unregistered
         {
            final AMQPFederationLocalPolicyControlType queuePolicyControl = (AMQPFederationLocalPolicyControlType)
               server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
               server.getManagementService().getResource(consumerResourceName);

            assertNotNull(queuePolicyControl);
            assertNull(consumerControl);
         }

         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString(getTestName()),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(1)
                              .queue();
         peer.expectDisposition().withSettled(true).withState().accepted();

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createQueue(getTestName()));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final Message message = consumer.receive(5_000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final AMQPFederationLocalPolicyControlType queuePolicyControl = (AMQPFederationLocalPolicyControlType)
               server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
               server.getManagementService().getResource(consumerResourceName);

            assertEquals(2, federationControl.getMessagesReceived());
            assertEquals(2, queuePolicyControl.getMessagesReceived());
            assertEquals(1, consumerControl.getMessagesReceived());

            peer.expectFlow().withLinkCredit(999).withDrain(true)
                             .respond()
                             .withLinkCredit(0).withDeliveryCount(1000).withDrain(true);
            peer.expectDetach().respond(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testRemoteAddressFederationTracksMessagesAtPolicyAndProducerLevels() throws Exception {
      final MessageAnnotationsMatcher maMatcher = new MessageAnnotationsMatcher(true);
      maMatcher.withEntry(OPERATION_TYPE.toString(), Matchers.is(ADD_ADDRESS_POLICY));
      final Map<String, Object> policyMap = new LinkedHashMap<>();

      final List<String> includes = new ArrayList<>();
      includes.add(getTestName());

      policyMap.put(POLICY_NAME, "remote-address-policy");
      policyMap.put(ADDRESS_AUTO_DELETE, false);
      policyMap.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
      policyMap.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);
      policyMap.put(ADDRESS_MAX_HOPS, 5);
      policyMap.put(ADDRESS_ENABLE_DIVERT_BINDINGS, false);
      policyMap.put(ADDRESS_INCLUDES, includes);

      final EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher(policyMap);
      final TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
      payloadMatcher.setMessageAnnotationsMatcher(maMatcher);
      payloadMatcher.addMessageContentMatcher(bodyMatcher);

      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withHandle(0)
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true)
                            .and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind()
                            .withTarget().withAddress("test-dynamic-events-sender");
         peer.remoteFlow().withLinkCredit(10).queue();
         peer.remoteFlow().withLinkCredit(10).withHandle(0).queue(); // Give control link credit now to ensure ordering
         peer.expectTransfer().withPayload(payloadMatcher); // Remote address policy
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement remoteReceiveFromAddress = new AMQPFederationAddressPolicyElement();
         remoteReceiveFromAddress.setName("remote-address-policy");
         remoteReceiveFromAddress.addToIncludes(getTestName());
         remoteReceiveFromAddress.setAutoDelete(false);
         remoteReceiveFromAddress.setAutoDeleteDelay(-1L);
         remoteReceiveFromAddress.setAutoDeleteMessageCount(-1L);
         remoteReceiveFromAddress.setMaxHops(5);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addRemoteAddressPolicy(remoteReceiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(getTestName()).addRoutingType(RoutingType.MULTICAST));

         final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "remote-address-policy");
         final String producerResourceName = AMQPFederationManagementSupport.getFederationAddressProducerResourceName(getTestName(), "remote-address-policy", getTestName());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-address-receiver")
                                       .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                                       .withTarget().also()
                                       .withSource().withAddress(getTestName());

         // Connect to server from remote as if queue had demand and matched our remote federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName("federation-address-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withProperty(FEDERATION_POLICY_NAME.toString(), "remote-address-policy")
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress(getTestName())
                                         .withCapabilities("topic")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectTransfer().accept(); // Federated message

         // Federate a message to check link is attached properly
         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createTopic(getTestName()));

            producer.send(session.createMessage());
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         final AMQPFederationControl federationControl =
            (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);
         final AMQPFederationRemotePolicyControlType remotePolicyControl = (AMQPFederationRemotePolicyControlType)
            server.getManagementService().getResource(policyResourceName);
         AMQPFederationProducerControl producerControl = (AMQPFederationProducerControl)
            server.getManagementService().getResource(producerResourceName);

         assertNotNull(remotePolicyControl);
         assertNotNull(producerControl);

         assertEquals("remote-address-policy", remotePolicyControl.getName());
         assertEquals("address-federation", remotePolicyControl.getType());
         assertEquals(getTestName(), producerControl.getAddress());

         assertEquals(1, federationControl.getMessagesSent());
         assertEquals(1, remotePolicyControl.getMessagesSent());
         assertEquals(1, producerControl.getMessagesSent());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectTransfer().accept(); // Federated message

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createTopic(getTestName()));

            producer.send(session.createMessage());
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         assertEquals(2, federationControl.getMessagesSent());
         assertEquals(2, remotePolicyControl.getMessagesSent());
         assertEquals(2, producerControl.getMessagesSent());

         // Disconnect the remote federation consumer.
         peer.expectDetach().respond();
         peer.remoteDetach().now();

         peer.waitForScriptToComplete(500000, TimeUnit.SECONDS);

         // Should have cleaned up the producer management resource
         assertNull(server.getManagementService().getResource(producerResourceName));

         peer.expectAttach().ofSender().withName("federation-address-receiver")
                                       .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                                       .withTarget().also()
                                       .withSource().withAddress(getTestName());

         // Connect to server from remote as if queue had demand and matched our remote federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName("federation-address-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withProperty(FEDERATION_POLICY_NAME.toString(), "remote-address-policy")
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress(getTestName())
                                         .withCapabilities("topic")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectTransfer().accept(); // Federated message

         producerControl = (AMQPFederationProducerControl)
            server.getManagementService().getResource(producerResourceName);

         assertNotNull(producerControl);
         assertEquals(getTestName(), producerControl.getAddress());
         assertEquals(0, producerControl.getMessagesSent());

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createTopic(getTestName()));

            producer.send(session.createMessage());
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         assertEquals(3, federationControl.getMessagesSent());
         assertEquals(3, remotePolicyControl.getMessagesSent());
         assertEquals(1, producerControl.getMessagesSent());

         peer.close();

         // Connection drop should clean up producer instance from management.
         Wait.assertTrue(() -> server.getManagementService().getResource(producerResourceName) == null, 5000, 100);
      }
   }

   @Test
   @Timeout(20)
   public void testRemoteAddressFederationTrackingCleanedUpOnBrokerConnectionStopped() throws Exception {
      final MessageAnnotationsMatcher maMatcher = new MessageAnnotationsMatcher(true);
      maMatcher.withEntry(OPERATION_TYPE.toString(), Matchers.is(ADD_ADDRESS_POLICY));
      final Map<String, Object> policyMap = new LinkedHashMap<>();

      final List<String> includes = new ArrayList<>();
      includes.add(getTestName());

      policyMap.put(POLICY_NAME, "remote-address-policy");
      policyMap.put(ADDRESS_AUTO_DELETE, false);
      policyMap.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
      policyMap.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);
      policyMap.put(ADDRESS_MAX_HOPS, 5);
      policyMap.put(ADDRESS_ENABLE_DIVERT_BINDINGS, false);
      policyMap.put(ADDRESS_INCLUDES, includes);

      final EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher(policyMap);
      final TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
      payloadMatcher.setMessageAnnotationsMatcher(maMatcher);
      payloadMatcher.addMessageContentMatcher(bodyMatcher);

      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withHandle(0)
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true)
                            .and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind()
                            .withTarget().withAddress("test-dynamic-events-sender");
         peer.remoteFlow().withLinkCredit(10).queue();
         peer.remoteFlow().withLinkCredit(10).withHandle(0).queue(); // Give control link credit now to ensure ordering
         peer.expectTransfer().withPayload(payloadMatcher); // Remote address policy
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement remoteReceiveFromAddress = new AMQPFederationAddressPolicyElement();
         remoteReceiveFromAddress.setName("remote-address-policy");
         remoteReceiveFromAddress.addToIncludes(getTestName());
         remoteReceiveFromAddress.setAutoDelete(false);
         remoteReceiveFromAddress.setAutoDeleteDelay(-1L);
         remoteReceiveFromAddress.setAutoDeleteMessageCount(-1L);
         remoteReceiveFromAddress.setMaxHops(5);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addRemoteAddressPolicy(remoteReceiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(getTestName()).addRoutingType(RoutingType.MULTICAST));

         final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "remote-address-policy");
         final String producerResourceName = AMQPFederationManagementSupport.getFederationAddressProducerResourceName(getTestName(), "remote-address-policy", getTestName());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-address-receiver")
                                       .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                                       .withTarget().also()
                                       .withSource().withAddress(getTestName());

         // Connect to server from remote as if queue had demand and matched our remote federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName("federation-address-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withProperty(FEDERATION_POLICY_NAME.toString(), "remote-address-policy")
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress(getTestName())
                                         .withCapabilities("topic")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectConnectionToDrop();

         final BrokerConnectionControl brokerConnection = (BrokerConnectionControl)
            server.getManagementService().getResource(ResourceNames.BROKER_CONNECTION + getTestName());
         final AMQPFederationControl federationControl =
            (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);
         final AMQPFederationRemotePolicyControlType remotePolicyControl = (AMQPFederationRemotePolicyControlType)
            server.getManagementService().getResource(policyResourceName);
         AMQPFederationProducerControl producerControl = (AMQPFederationProducerControl)
            server.getManagementService().getResource(producerResourceName);

         assertNotNull(brokerConnection);
         assertTrue(brokerConnection.isConnected());
         assertNotNull(remotePolicyControl);
         assertNotNull(producerControl);
         assertEquals(0, federationControl.getMessagesSent());
         assertEquals(0, remotePolicyControl.getMessagesSent());
         assertEquals(0, producerControl.getMessagesSent());

         brokerConnection.stop();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         // Broker connection stop should clean up producer instance from management.
         Wait.assertTrue(() -> server.getManagementService().getResource(producerResourceName) == null, 10_000, 100);

         assertNotNull(server.getManagementService().getResource(policyResourceName));

         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testRemoteQueueFederationTracksMessagesAtPolicyAndProducerLevels() throws Exception {
      final MessageAnnotationsMatcher maMatcher = new MessageAnnotationsMatcher(true);
      maMatcher.withEntry(OPERATION_TYPE.toString(), Matchers.is(ADD_QUEUE_POLICY));
      final Map<String, Object> policyMap = new LinkedHashMap<>();

      final List<String> includes = new ArrayList<>();
      includes.add("*");
      includes.add(getTestName());

      policyMap.put(POLICY_NAME, "remote-queue-policy");
      policyMap.put(QUEUE_INCLUDE_FEDERATED, false);
      policyMap.put(QUEUE_PRIORITY_ADJUSTMENT, 64);
      policyMap.put(QUEUE_INCLUDES, includes);

      final EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher(policyMap);
      final TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
      payloadMatcher.setMessageAnnotationsMatcher(maMatcher);
      payloadMatcher.addMessageContentMatcher(bodyMatcher);

      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withHandle(0)
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true)
                            .and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind()
                            .withTarget().withAddress("test-dynamic-events-sender");
         peer.remoteFlow().withLinkCredit(10).queue();
         peer.remoteFlow().withLinkCredit(10).withHandle(0).queue(); // Give control link credit now to ensure ordering
         peer.expectTransfer().withPayload(payloadMatcher); // Remote address policy
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement remoteReceiveFromQueue = new AMQPFederationQueuePolicyElement();
         remoteReceiveFromQueue.setName("remote-queue-policy");
         remoteReceiveFromQueue.setIncludeFederated(false);
         remoteReceiveFromQueue.setPriorityAdjustment(64);
         remoteReceiveFromQueue.addToIncludes("*", getTestName());

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addRemoteQueuePolicy(remoteReceiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress(getTestName())
                                                                .setAutoCreated(false));

         final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "remote-queue-policy");
         final String producerResourceName = AMQPFederationManagementSupport.getFederationAddressProducerResourceName(getTestName(), "remote-queue-policy", getTestName() + "::" + getTestName());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-queue-receiver")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withTarget().also()
                                       .withSource().withAddress(getTestName() + "::" + getTestName());

         // Connect to server from remote as if queue had demand and matched our remote federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("federation-queue-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withProperty(FEDERATION_POLICY_NAME.toString(), "remote-queue-policy")
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress(getTestName() + "::" + getTestName())
                                         .withCapabilities("queue")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectTransfer().accept(); // Federated message

         // Federate a message to check link is attached properly
         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue(getTestName()));

            producer.send(session.createMessage());
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         final AMQPFederationControl federationControl =
            (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);
         final AMQPFederationRemotePolicyControlType remotePolicyControl = (AMQPFederationRemotePolicyControlType)
            server.getManagementService().getResource(policyResourceName);
         AMQPFederationProducerControl producerControl = (AMQPFederationProducerControl)
            server.getManagementService().getResource(producerResourceName);

         assertNotNull(remotePolicyControl);
         assertNotNull(producerControl);

         assertEquals("remote-queue-policy", remotePolicyControl.getName());
         assertEquals("queue-federation", remotePolicyControl.getType());
         assertEquals(getTestName(), producerControl.getQueueName());

         assertEquals(1, federationControl.getMessagesSent());
         assertEquals(1, remotePolicyControl.getMessagesSent());
         assertEquals(1, producerControl.getMessagesSent());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectTransfer().accept(); // Federated message

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue(getTestName()));

            producer.send(session.createMessage());
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         assertEquals(2, federationControl.getMessagesSent());
         assertEquals(2, remotePolicyControl.getMessagesSent());
         assertEquals(2, producerControl.getMessagesSent());

         // Disconnect the remote federation consumer.
         peer.expectDetach().respond();
         peer.remoteDetach().now();

         peer.waitForScriptToComplete(500000, TimeUnit.SECONDS);

         // Should have cleaned up the producer management resource
         assertNull(server.getManagementService().getResource(producerResourceName));

         peer.expectAttach().ofSender().withName("federation-queue-receiver")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withTarget().also()
                                       .withSource().withAddress(getTestName() + "::" + getTestName());

         // Connect to server from remote as if queue had demand and matched our remote federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("federation-queue-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withProperty(FEDERATION_POLICY_NAME.toString(), "remote-queue-policy")
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress(getTestName() + "::" + getTestName())
                                         .withCapabilities("queue")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectTransfer().accept(); // Federated message

         producerControl = (AMQPFederationProducerControl)
            server.getManagementService().getResource(producerResourceName);

         assertNotNull(producerControl);
         assertEquals(getTestName(), producerControl.getQueueName());
         assertEquals(0, producerControl.getMessagesSent());

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue(getTestName()));

            producer.send(session.createMessage());
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         assertEquals(3, federationControl.getMessagesSent());
         assertEquals(3, remotePolicyControl.getMessagesSent());
         assertEquals(1, producerControl.getMessagesSent());

         peer.close();

         Wait.assertTrue(() -> server.getManagementService().getResource(producerResourceName) == null, 5000, 100);
      }
   }

   @Test
   @Timeout(20)
   public void testRemoteQueueFederationTrackingCleanedUpOnBrokerConnectionStopped() throws Exception {
      final MessageAnnotationsMatcher maMatcher = new MessageAnnotationsMatcher(true);
      maMatcher.withEntry(OPERATION_TYPE.toString(), Matchers.is(ADD_QUEUE_POLICY));
      final Map<String, Object> policyMap = new LinkedHashMap<>();

      final List<String> includes = new ArrayList<>();
      includes.add("*");
      includes.add(getTestName());

      policyMap.put(POLICY_NAME, "remote-queue-policy");
      policyMap.put(QUEUE_INCLUDE_FEDERATED, false);
      policyMap.put(QUEUE_PRIORITY_ADJUSTMENT, 64);
      policyMap.put(QUEUE_INCLUDES, includes);

      final EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher(policyMap);
      final TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
      payloadMatcher.setMessageAnnotationsMatcher(maMatcher);
      payloadMatcher.addMessageContentMatcher(bodyMatcher);

      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withHandle(0)
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true)
                            .and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind()
                            .withTarget().withAddress("test-dynamic-events-sender");
         peer.remoteFlow().withLinkCredit(10).queue();
         peer.remoteFlow().withLinkCredit(10).withHandle(0).queue(); // Give control link credit now to ensure ordering
         peer.expectTransfer().withPayload(payloadMatcher); // Remote address policy
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement remoteReceiveFromQueue = new AMQPFederationQueuePolicyElement();
         remoteReceiveFromQueue.setName("remote-queue-policy");
         remoteReceiveFromQueue.setIncludeFederated(false);
         remoteReceiveFromQueue.setPriorityAdjustment(64);
         remoteReceiveFromQueue.addToIncludes("*", getTestName());

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName(getTestName());
         element.addRemoteQueuePolicy(remoteReceiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress(getTestName())
                                                                .setAutoCreated(false));

         final String federationResourceName = AMQPFederationManagementSupport.getFederationSourceResourceName(getTestName());
         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName(getTestName(), "remote-queue-policy");
         final String producerResourceName = AMQPFederationManagementSupport.getFederationAddressProducerResourceName(getTestName(), "remote-queue-policy", getTestName() + "::" + getTestName());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-queue-receiver")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withTarget().also()
                                       .withSource().withAddress(getTestName() + "::" + getTestName());

         // Connect to server from remote as if queue had demand and matched our remote federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("federation-queue-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withProperty(FEDERATION_POLICY_NAME.toString(), "remote-queue-policy")
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress(getTestName() + "::" + getTestName())
                                         .withCapabilities("queue")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         final BrokerConnectionControl brokerConnection = (BrokerConnectionControl)
            server.getManagementService().getResource(ResourceNames.BROKER_CONNECTION + getTestName());
         final AMQPFederationControl federationControl =
            (AMQPFederationControl) server.getManagementService().getResource(federationResourceName);
         final AMQPFederationRemotePolicyControlType remotePolicyControl = (AMQPFederationRemotePolicyControlType)
            server.getManagementService().getResource(policyResourceName);
         AMQPFederationProducerControl producerControl = (AMQPFederationProducerControl)
            server.getManagementService().getResource(producerResourceName);

         assertNotNull(brokerConnection);
         assertTrue(brokerConnection.isConnected());
         assertNotNull(remotePolicyControl);
         assertNotNull(producerControl);
         assertEquals(0, federationControl.getMessagesSent());
         assertEquals(0, remotePolicyControl.getMessagesSent());
         assertEquals(0, producerControl.getMessagesSent());

         brokerConnection.stop();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         // Broker connection stop should clean up producer instance from management.
         Wait.assertTrue(() -> server.getManagementService().getResource(producerResourceName) == null, 5_000, 100);

         assertNotNull(server.getManagementService().getResource(policyResourceName));

         peer.close();
      }
   }
}
