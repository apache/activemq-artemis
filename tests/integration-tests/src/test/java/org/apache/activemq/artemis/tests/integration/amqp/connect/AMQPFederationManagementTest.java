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

import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_ADDRESS_RECEIVER;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONTROL_LINK;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_EVENT_LINK;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_QUEUE_RECEIVER;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
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
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationAddressPolicyControl;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConsumerControl;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationManagementSupport;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationQueuePolicyControl;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
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

         final BrokerConnectionControl brokerConnection = (BrokerConnectionControl)
            server.getManagementService().getResource(ResourceNames.BROKER_CONNECTION + getTestName());

         assertNotNull(brokerConnection);
         assertTrue(brokerConnection.isConnected());

         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName("address-policy");
         final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName("address-policy", getTestName());

         final AMQPFederationAddressPolicyControl addressPolicyControl =
            (AMQPFederationAddressPolicyControl) server.getManagementService().getResource(policyResourceName);

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

         // Stopping the connection should remove the federation management objects.
         Wait.assertTrue(() -> server.getManagementService().getResource(policyResourceName) == null, 5_000, 100);
         Wait.assertTrue(() -> server.getManagementService().getResource(consumerResourceName) == null, 5_000, 100);

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

            final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName("queue-policy");
            final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName("queue-policy", getTestName() + "::" + getTestName());

            final BrokerConnectionControl brokerConnection = (BrokerConnectionControl)
               server.getManagementService().getResource(ResourceNames.BROKER_CONNECTION + getTestName());

            assertNotNull(brokerConnection);
            assertTrue(brokerConnection.isConnected());

            final AMQPFederationQueuePolicyControl queuePolicyControl =
               (AMQPFederationQueuePolicyControl) server.getManagementService().getResource(policyResourceName);

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
            Wait.assertTrue(() -> server.getManagementService().getResource(policyResourceName) == null, 5_000, 100);
            Wait.assertTrue(() -> server.getManagementService().getResource(consumerResourceName) == null, 5_000, 100);

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

         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName("address-policy");
         final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName("address-policy", getTestName());

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

            final AMQPFederationAddressPolicyControl addressPolicyControl = (AMQPFederationAddressPolicyControl)
               server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
               server.getManagementService().getResource(consumerResourceName);

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
            final AMQPFederationAddressPolicyControl addressPolicyControl = (AMQPFederationAddressPolicyControl)
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

            final AMQPFederationAddressPolicyControl addressPolicyControl = (AMQPFederationAddressPolicyControl)
               server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
               server.getManagementService().getResource(consumerResourceName);

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

         final String policyResourceName = AMQPFederationManagementSupport.getFederationPolicyResourceName("queue-policy");
         final String consumerResourceName = AMQPFederationManagementSupport.getFederationAddressConsumerResourceName("queue-policy", getTestName() + "::" + getTestName());

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

            final AMQPFederationQueuePolicyControl queuePolicyControl = (AMQPFederationQueuePolicyControl)
               server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
               server.getManagementService().getResource(consumerResourceName);

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
            final AMQPFederationQueuePolicyControl queuePolicyControl = (AMQPFederationQueuePolicyControl)
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

            final AMQPFederationQueuePolicyControl queuePolicyControl = (AMQPFederationQueuePolicyControl)
               server.getManagementService().getResource(policyResourceName);
            final AMQPFederationConsumerControl consumerControl = (AMQPFederationConsumerControl)
               server.getManagementService().getResource(consumerResourceName);

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
}
