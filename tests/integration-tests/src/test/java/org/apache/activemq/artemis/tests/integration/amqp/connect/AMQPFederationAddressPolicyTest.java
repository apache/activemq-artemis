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
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADDRESS_EXCLUDES;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADDRESS_INCLUDES;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADDRESS_MAX_HOPS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADD_ADDRESS_POLICY;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_ADDRESS_RECEIVER;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONFIGURATION;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONTROL_LINK;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.LARGE_MESSAGE_THRESHOLD;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.OPERATION_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.POLICY_NAME;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS_LOW;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.TRANSFORMER_CLASS_NAME;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.TRANSFORMER_PROPERTIES_MAP;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.POLICY_PROPERTIES_MAP;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationPolicySupport.FEDERATED_ADDRESS_SOURCE_PROPERTIES;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationPolicySupport.MESSAGE_HOPS_ANNOTATION;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationPolicySupport.generateAddressFilter;
import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPTunneledMessageConstants.AMQP_TUNNELED_CORE_LARGE_MESSAGE_FORMAT;
import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPTunneledMessageConstants.AMQP_TUNNELED_CORE_MESSAGE_FORMAT;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederatedBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationAddressPolicyElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ComponentConfigurationRoutingType;
import org.apache.activemq.artemis.core.server.Divert;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.transformer.Transformer;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.ActiveMQServerAMQPFederationPlugin;
import org.apache.activemq.artemis.protocol.amqp.federation.Federation;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumer;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromAddressPolicy;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.protonj2.test.driver.ProtonTestClient;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.HeaderMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.MessageAnnotationsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.PropertiesMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.TransferPayloadCompositeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.hamcrest.Matchers;
import org.jgroups.util.UUID;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for AMQP Broker federation handling of the receive from and send to address policy
 * configuration handling.
 */
public class AMQPFederationAddressPolicyTest extends AmqpClientTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final WildcardConfiguration DEFAULT_WILDCARD_CONFIGURATION = new WildcardConfiguration();

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

   @Test(timeout = 20000)
   public void testFederationCreatesAddressReceiverWhenLocalQueueIsStaticlyDefined() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(false);
         receiveFromAddress.setAutoDeleteDelay(-1L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, false);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.MULTICAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         Wait.assertTrue(() -> server.queueQuery(SimpleString.toSimpleString("test")).isExists());

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         // Should be no frames generated as we already federated the address and the statically added
         // queue should retain demand when this consumer leaves.
         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);

            session.createConsumer(session.createTopic("test"));
            session.createConsumer(session.createTopic("test"));

            connection.start();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDetach().respond();

         // This should trigger the federation consumer to be shutdown as the statically defined queue
         // should be the only remaining demand on the address.
         logger.info("Removing Queues from federated address to eliminate demand");
         server.destroyQueue(SimpleString.toSimpleString("test"));
         Wait.assertFalse(() -> server.queueQuery(SimpleString.toSimpleString("test")).isExists());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testFederationCreatesAddressReceiverLinkForAddressMatch() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(true);
         receiveFromAddress.setAutoDeleteDelay(10_000L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, true);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, 10_000L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createTopic("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationCreatesAddressReceiverLinkForAddressMatchUsingPolicyCredit() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(true);
         receiveFromAddress.setAutoDeleteDelay(10_000L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);
         receiveFromAddress.addProperty(RECEIVER_CREDITS, "25");
         receiveFromAddress.addProperty(RECEIVER_CREDITS_LOW, "5");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, true);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, 10_000L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(25);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createTopic("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationCreatesAddressReceiverLinkForAddressMatchWithMaxHopsFilter() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.setMaxHops(1);
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(true);
         receiveFromAddress.setAutoDeleteDelay(10_000L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         final String expectedJMSFilter = generateAddressFilter(1);

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, true);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, 10_000L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .withSource().withJMSSelector(expectedJMSFilter).and()
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         // Induce demand on the local broker which should then create a receiver to our remote peer.
         try (ProtonTestClient receivingPeer = new ProtonTestClient()) {
            receivingPeer.queueClientSaslAnonymousConnect();
            receivingPeer.connect("localhost", AMQP_PORT);
            receivingPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            receivingPeer.expectOpen();
            receivingPeer.expectBegin();
            receivingPeer.expectAttach();
            receivingPeer.remoteOpen().withContainerId("test-sender").now();
            receivingPeer.remoteBegin().now();
            receivingPeer.remoteAttach().ofReceiver()
                                        .withInitialDeliveryCount(0)
                                        .withName("sending-peer")
                                        .withSource().withAddress("test")
                                                     .withCapabilities("topic").also()
                                        .withTarget().also()
                                        .now();
            receivingPeer.remoteFlow().withLinkCredit(10).now();
            receivingPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Check that annotation for hops is present in the forwarded message.
            final HeaderMatcher headerMatcher = new HeaderMatcher(true);
            final MessageAnnotationsMatcher annotationsMatcher = new MessageAnnotationsMatcher(true);
            annotationsMatcher.withEntry("x-opt-test", Matchers.equalTo("test"));
            annotationsMatcher.withEntry(MESSAGE_HOPS_ANNOTATION.toString(), Matchers.equalTo(1));
            final EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher("Hello World");
            final TransferPayloadCompositeMatcher matcher = new TransferPayloadCompositeMatcher();
            matcher.setHeadersMatcher(headerMatcher);
            matcher.setMessageAnnotationsMatcher(annotationsMatcher);
            matcher.addMessageContentMatcher(bodyMatcher);

            // Broker should route the federated message to the client and it should
            // carry the hops annotation indicating that one hop has occurred.
            receivingPeer.expectTransfer().withPayload(matcher).accept();

            peer.expectDisposition().withState().accepted();
            peer.remoteTransfer().withHeader().withDurability(true)
                                 .also()
                                 .withMessageAnnotations().withAnnotation("x-opt-test", "test").also()
                                 .withBody().withString("Hello World")
                                 .also()
                                 .withDeliveryId(1)
                                 .now();
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            receivingPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationClosesAddressReceiverLinkWhenDemandRemoved() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(false);
         receiveFromAddress.setAutoDeleteDelay(-1L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, false);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            // Demand is removed so receiver should be detached.
            consumer.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationRetainsAddressReceiverLinkWhenDurableSubscriberIsOffline() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(false);
         receiveFromAddress.setAutoDeleteDelay(-1L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, false);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            connection.setClientID("test-clientId");

            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Topic topic = session.createTopic("test");
            final MessageConsumer consumer = session.createSharedDurableConsumer(topic, "shared-subscription");

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Consumer goes offline but demand is retained for address
            consumer.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            session.unsubscribe("shared-subscription");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationClosesAddressReceiverLinkWaitsForAllDemandToRemoved() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(false);
         receiveFromAddress.setAutoDeleteDelay(-1L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, false);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer1 = session.createConsumer(session.createTopic("test"));
            final MessageConsumer consumer2 = session.createConsumer(session.createTopic("test"));

            connection.start();
            consumer1.close(); // One is gone but another remains

            // Will fail if any frames arrive
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            consumer2.close(); // Now demand is gone

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationHandlesAddressDeletedAndConsumerRecreates() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respondInKind();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000).optional();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createTopic("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            server.removeAddressInfo(SimpleString.toSimpleString("test"), null, true);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         // Consumer recreates Address and adds demand back and federation should restart
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000).optional();

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createTopic("test"));

            connection.start();

            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testFederationConsumerCreatedWhenDemandAddedToDivertAddress() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(false);
         receiveFromAddress.setAutoDeleteDelay(-1L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);
         receiveFromAddress.setEnableDivertBindings(true);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         final DivertConfiguration divertConfig = new DivertConfiguration().setAddress("test")
                                                                           .setForwardingAddress("forward")
                                                                           .setName("test-divert");

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.deployDivert(divertConfig);
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, false);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         // Demand on the forwarding address should create a remote consumer for the forwarded address.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic("forward"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            consumer.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationConsumerCreatedWhenDemandAddedToCompositeDivertAddress() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(false);
         receiveFromAddress.setAutoDeleteDelay(-1L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);
         receiveFromAddress.setEnableDivertBindings(true);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         final DivertConfiguration divertConfig = new DivertConfiguration().setAddress("test")
                                                                           .setForwardingAddress("forward1,forward2")
                                                                           .setName("test-divert");

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.deployDivert(divertConfig);
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, false);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         // Demand on the forwarding address should create a remote consumer for the forwarded address.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);

            // Creating a consumer on each should result in only one attach for the source address
            final MessageConsumer consumer1 = session.createConsumer(session.createTopic("forward1"));
            final MessageConsumer consumer2 = session.createConsumer(session.createTopic("forward2"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Closing one should not remove all demand on the source address
            consumer1.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            consumer2.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationConsumerRemovesDemandFromDivertConsumersOnlyWhenAllDemandIsRemoved() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(false);
         receiveFromAddress.setAutoDeleteDelay(-1L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);
         receiveFromAddress.setEnableDivertBindings(true);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         final DivertConfiguration divertConfig = new DivertConfiguration().setAddress("test")
                                                                           .setForwardingAddress("forward")
                                                                           .setName("test-divert");

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.deployDivert(divertConfig);
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, false);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         // Demand on the forwarding address should create a remote consumer for the forwarded address.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer1 = session.createConsumer(session.createTopic("forward"));
            final MessageConsumer consumer2 = session.createConsumer(session.createTopic("forward"));

            connection.start();
            consumer1.close(); // One is gone but another remains

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            consumer2.close(); // Now demand is gone

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationConsumerRetainsDemandForDivertBindingWithoutActiveAnycastSubscriptions() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("source"); // Divert matching works on the source address of the divert
         receiveFromAddress.setAutoDelete(false);
         receiveFromAddress.setAutoDeleteDelay(-1L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);
         receiveFromAddress.setEnableDivertBindings(true);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("myFederations");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         // Any demand on the forwarding address even if the forward is a Queue (ANYCAST) should create
         // demand on the remote for the source address (If the source is MULTICAST)
         final DivertConfiguration divertConfig = new DivertConfiguration().setAddress("source")
                                                                           .setForwardingAddress("forward")
                                                                           .setRoutingType(ComponentConfigurationRoutingType.ANYCAST)
                                                                           .setName("test-divert");

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.deployDivert(divertConfig);
         // Current implementation requires the source address exist on the local broker before it
         // will attempt to federate it from the remote.
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("source"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, false);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         // Demand on the forwarding address should create a remote consumer for the forwarded address.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("myFederations"),
                                            containsString("source"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue("forward");
            final MessageConsumer consumer1 = session.createConsumer(queue);
            final MessageConsumer consumer2 = session.createConsumer(queue);

            connection.start();
            consumer1.close(); // One is gone but another remains

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            consumer2.close(); // Demand remains as the Queue continues to exist

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationConsumerRemovesDemandForDivertBindingWithoutActiveMulticastSubscriptions() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("source"); // Divert matching works on the source address of the divert
         receiveFromAddress.setAutoDelete(false);
         receiveFromAddress.setAutoDeleteDelay(-1L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);
         receiveFromAddress.setEnableDivertBindings(true);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("myFederations");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         // Any demand on the forwarding address even if the forward is a Queue (ANYCAST) should create
         // demand on the remote for the source address (If the source is MULTICAST)
         final DivertConfiguration divertConfig = new DivertConfiguration().setAddress("source")
                                                                           .setForwardingAddress("forward")
                                                                           .setRoutingType(ComponentConfigurationRoutingType.MULTICAST)
                                                                           .setName("test-divert");

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.deployDivert(divertConfig);
         // Current implementation requires the source address exist on the local broker before it
         // will attempt to federate it from the remote.
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("source"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, false);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         // Demand on the forwarding address should create a remote consumer for the forwarded address.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("myFederations"),
                                            containsString("source"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Topic topic = session.createTopic("forward");
            final MessageConsumer consumer1 = session.createConsumer(topic);
            final MessageConsumer consumer2 = session.createConsumer(topic);

            connection.start();
            consumer1.close(); // One is gone but another remains

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            consumer2.close(); // Now demand is gone from the divert

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationRemovesRemoteDemandIfDivertIsRemoved() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("source");
         receiveFromAddress.setAutoDelete(false);
         receiveFromAddress.setAutoDeleteDelay(-1L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);
         receiveFromAddress.setEnableDivertBindings(true);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         final DivertConfiguration divertConfig = new DivertConfiguration().setAddress("source")
                                                                           .setForwardingAddress("forward")
                                                                           .setName("test-divert");

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.deployDivert(divertConfig);
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("source"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, false);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         // Demand on the forwarding address should create a remote consumer for the forwarding address.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("source"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createTopic("forward"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            server.destroyDivert(SimpleString.toSimpleString("test-divert"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testDivertBindingsDoNotCreateAdditionalDemandIfDemandOnForwardingAddressAlreadyExists() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(false);
         receiveFromAddress.setAutoDeleteDelay(-1L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);
         receiveFromAddress.setEnableDivertBindings(true);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         final DivertConfiguration divertConfig = new DivertConfiguration().setAddress("test")
                                                                           .setForwardingAddress("forward")
                                                                           .setName("test-divert");

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.deployDivert(divertConfig);
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         final Map<String, Object> expectedSourceProperties = new HashMap<>();
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE, false);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, -1L);
         expectedSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, -1L);

         // Demand on the main address creates demand on the same address remotely and then the diverts
         // should just be tracked under that original demand.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), expectedSourceProperties)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final MessageConsumer consumer1 = session.createConsumer(session.createTopic("forward"));
            final MessageConsumer consumer2 = session.createConsumer(session.createTopic("forward"));

            consumer1.close();
            consumer2.close();

            server.destroyDivert(SimpleString.toSimpleString("test-divert"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            consumer.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testInboundMessageRoutedToReceiverOnLocalAddress() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respondInKind();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(0)
                              .queue();
         peer.expectDisposition().withSettled(true).withState().accepted();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic("test"));

            connection.start();

            final Message message = consumer.receive(5_000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteBrokerAcceptsAddressPolicyFromControlLink() throws Exception {
      server.start();

      final ArrayList<String> includes = new ArrayList<>();
      includes.add("address1");
      includes.add("address2");
      final ArrayList<String> excludes = new ArrayList<>();
      includes.add("address3");

      final FederationReceiveFromAddressPolicy policy =
         new FederationReceiveFromAddressPolicy("test-address-policy",
                                                true, 30_000L, 1000L, 1, true,
                                                includes, excludes, null, null,
                                                DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendAddresPolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteBrokerAcceptsAddressPolicyFromControlLinkWithTransformerConfiguration() throws Exception {
      server.start();

      final ArrayList<String> includes = new ArrayList<>();
      includes.add("address1");
      includes.add("address2");
      final ArrayList<String> excludes = new ArrayList<>();
      includes.add("address3");

      final Map<String, String> transformerProperties = new HashMap<>();
      transformerProperties.put("key1", "value1");
      transformerProperties.put("key2", "value2");

      final TransformerConfiguration transformerConfiguration = new TransformerConfiguration();
      transformerConfiguration.setClassName(ApplicationPropertiesTransformer.class.getName());
      transformerConfiguration.setProperties(transformerProperties);

      final FederationReceiveFromAddressPolicy policy =
         new FederationReceiveFromAddressPolicy("test-address-policy",
                                                true, 30_000L, 1000L, 1, true,
                                                includes, excludes, null, transformerConfiguration,
                                                DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendAddresPolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteFederatesAddressWhenDemandIsApplied() throws Exception {
      server.start();

      final ArrayList<String> includes = new ArrayList<>();
      includes.add("address1");

      final FederationReceiveFromAddressPolicy policy =
         new FederationReceiveFromAddressPolicy("test-address-policy",
                                                true, 30_000L, 1000L, 1, true,
                                                includes, null, null, null,
                                                DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendAddresPolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withSource().withAddress("address1")
                            .and()
                            .respondInKind(); // Server detected demand
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(1)
                              .queue();
         peer.expectDisposition().withSettled(true).withState().accepted();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic("address1"));

            connection.start();

            final Message message = consumer.receive(5_000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteFederatesAddressWhenDemandIsAppliedUsingControllerDefinedLinkCredit() throws Exception {
      server.start();

      final ArrayList<String> includes = new ArrayList<>();
      includes.add("address1");

      final FederationReceiveFromAddressPolicy policy =
         new FederationReceiveFromAddressPolicy("test-address-policy",
                                                true, 30_000L, 1000L, 1, true,
                                                includes, null, null, null,
                                                DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test", 10, 9);
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendAddresPolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withSource().withAddress("address1")
                            .and()
                            .respondInKind(); // Server detected demand
         peer.expectFlow().withLinkCredit(10);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(1)
                              .queue();
         peer.expectFlow().withLinkCredit(10); // Should top up the credit as we set low to nine
         peer.expectDisposition().withSettled(true).withState().accepted();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic("address1"));

            connection.start();

            final Message message = consumer.receive(5_000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteFederatesAddressWhenDemandIsAppliedUsingPolicyDefinedLinkCredit() throws Exception {
      server.start();

      final ArrayList<String> includes = new ArrayList<>();
      includes.add("address1");

      final Map<String, Object> properties = new HashMap<>();
      properties.put(RECEIVER_CREDITS, 40);
      properties.put(RECEIVER_CREDITS_LOW, "39");
      properties.put(LARGE_MESSAGE_THRESHOLD, 1024);

      final FederationReceiveFromAddressPolicy policy =
         new FederationReceiveFromAddressPolicy("test-address-policy",
                                                true, 30_000L, 1000L, 1, true,
                                                includes, null, properties, null,
                                                DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test", 10, 9);
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendAddresPolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withSource().withAddress("address1")
                            .and()
                            .respondInKind(); // Server detected demand
         peer.expectFlow().withLinkCredit(40);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(1)
                              .queue();
         peer.expectFlow().withLinkCredit(40); // Should top up the credit as we set low to 39
         peer.expectDisposition().withSettled(true).withState().accepted();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic("address1"));

            connection.start();

            final Message message = consumer.receive(5_000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteFederatesAddressAndAppliesTransformerWhenDemandIsApplied() throws Exception {
      server.start();

      final ArrayList<String> includes = new ArrayList<>();
      includes.add("address1");

      final Map<String, String> transformerProperties = new HashMap<>();
      transformerProperties.put("key1", "value1");
      transformerProperties.put("key2", "value2");

      final TransformerConfiguration transformerConfiguration = new TransformerConfiguration();
      transformerConfiguration.setClassName(ApplicationPropertiesTransformer.class.getName());
      transformerConfiguration.setProperties(transformerProperties);

      final FederationReceiveFromAddressPolicy policy =
         new FederationReceiveFromAddressPolicy("test-address-policy",
                                                true, 30_000L, 1000L, 1, true,
                                                includes, null, null, transformerConfiguration,
                                                DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendAddresPolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withSource().withAddress("address1")
                            .and()
                            .respondInKind(); // Server detected demand
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(1)
                              .queue();
         peer.expectDisposition().withSettled(true).withState().accepted();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic("address1"));

            connection.start();

            final Message message = consumer.receive(5_000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());
            assertEquals("value1", message.getStringProperty("key1"));
            assertEquals("value2", message.getStringProperty("key2"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteBrokerAnswersAttachOfFederationReceiverProperly() throws Exception {
      server.start();

      final Map<String, Object> remoteSourceProperties = new HashMap<>();
      remoteSourceProperties.put(ADDRESS_AUTO_DELETE, true);
      remoteSourceProperties.put(ADDRESS_AUTO_DELETE_DELAY, 10_000L);
      remoteSourceProperties.put(ADDRESS_AUTO_DELETE_MSG_COUNT, 1L);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-address-receiver")
                                       .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                                       .withTarget().also()
                                       .withSource().withAddress("test");

         // Connect to remote as if an queue had demand and matched our federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName("federation-address-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withProperty(FEDERATED_ADDRESS_SOURCE_PROPERTIES.toString(), remoteSourceProperties)
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test")
                                         .withCapabilities("topic")
                                         .and()
                            .withTarget().and()
                            .now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testReceiverWithMaxHopsFilterAppliesFilterCorrectly() throws Exception {
      server.start();

      final String maxHopsJMSFilter = "\"m." + MESSAGE_HOPS_ANNOTATION +
                                      "\" IS NULL OR \"m." + MESSAGE_HOPS_ANNOTATION +
                                      "\"<2";

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-address-receiver")
                                       .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                                       .withSource().withAddress("test")
                                                    .withJMSSelector(maxHopsJMSFilter);

         // Connect to remote as if an queue had demand and matched our federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName("federation-address-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test")
                                         .withCapabilities("topic")
                                         .withJMSSelector(maxHopsJMSFilter)
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         // Match typical generic Qpid JMS Text Message structure
         final HeaderMatcher headerMatcher = new HeaderMatcher(true);
         final MessageAnnotationsMatcher annotationsMatcher = new MessageAnnotationsMatcher(true);
         final PropertiesMatcher properties = new PropertiesMatcher(true);
         final EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher("Hello World");
         final TransferPayloadCompositeMatcher matcher = new TransferPayloadCompositeMatcher();
         matcher.setHeadersMatcher(headerMatcher);
         matcher.setMessageAnnotationsMatcher(annotationsMatcher);
         matcher.setPropertiesMatcher(properties);
         matcher.addMessageContentMatcher(bodyMatcher);

         peer.expectTransfer().withPayload(matcher).accept();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createTopic("test"));

            producer.send(session.createTextMessage("Hello World"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         peer.expectTransfer().withPayload(matcher).accept();

         try (ProtonTestClient sendingPeer = new ProtonTestClient()) {
            sendingPeer.queueClientSaslAnonymousConnect();
            sendingPeer.connect("localhost", AMQP_PORT);
            sendingPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            sendingPeer.expectOpen();
            sendingPeer.expectBegin();
            sendingPeer.expectAttach();
            sendingPeer.expectFlow();
            sendingPeer.remoteOpen().withContainerId("test-sender").now();
            sendingPeer.remoteBegin().now();
            sendingPeer.remoteAttach().ofSender()
                                      .withInitialDeliveryCount(0)
                                      .withName("sending-peer")
                                      .withTarget().withAddress("test")
                                                   .withCapabilities("topic").also()
                                      .withSource().also()
                                      .now();
            sendingPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            sendingPeer.expectDisposition().withSettled(true)
                                           .withState().accepted();
            sendingPeer.expectDisposition().withSettled(true)
                                           .withState().accepted();

            sendingPeer.remoteTransfer().withDeliveryId(0)
                                        .withHeader().withDurability(false).also()
                                        .withProperties().withMessageId("ID:1").also()
                                        .withMessageAnnotations().withAnnotation(MESSAGE_HOPS_ANNOTATION.toString(), 1).also()
                                        .withBody().withString("Hello World")
                                        .also()
                                        .now();

            // Should be accepted but not routed to the main test peer client acting as a federated receiver
            sendingPeer.remoteTransfer().withDeliveryId(1)
                                        .withHeader().withDurability(false).also()
                                        .withProperties().withMessageId("ID:2").also()
                                        .withMessageAnnotations().withAnnotation(MESSAGE_HOPS_ANNOTATION.toString(), 2).also()
                                        .withBody().withString("Hello World")
                                        .also()
                                        .now();
            sendingPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteConnectionCannotAttachAddressFederationLinkWithoutControlLink() throws Exception {
      server.start();

      try (ProtonTestClient peer = new ProtonTestClient()) {
         peer.queueClientSaslAnonymousConnect();
         peer.remoteOpen().queue();
         peer.expectOpen();
         peer.remoteBegin().queue();
         peer.expectBegin();
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         // Broker should reject the attach since there's no control link
         peer.expectAttach().ofSender().withName("federation-address-receiver")
                                       .withSource(nullValue())
                                       .withTarget();
         peer.expectDetach().respond();

         // Attempt to create a federation address receiver link without existing control link
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName("federation-address-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test")
                                         .withCapabilities("topic")
                                         .and()
                            .withTarget().and()
                            .now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testTransformInboundFederatedMessageBeforeDispatch() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respondInKind();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final Map<String, String> newApplicationProperties = new HashMap<>();
         newApplicationProperties.put("appProperty1", "one");
         newApplicationProperties.put("appProperty2", "two");

         final TransformerConfiguration transformerConfiguration = new TransformerConfiguration();
         transformerConfiguration.setClassName(ApplicationPropertiesTransformer.class.getName());
         transformerConfiguration.setProperties(newApplicationProperties);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.setTransformerConfiguration(transformerConfiguration);
         receiveFromAddress.addToIncludes("test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(0)
                              .queue();
         peer.expectDisposition().withSettled(true).withState().accepted();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic("test"));

            connection.start();

            final Message message = consumer.receive(5_000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());
            assertEquals("one", message.getStringProperty("appProperty1"));
            assertEquals("two", message.getStringProperty("appProperty2"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testFederationDoesNotCreateAddressReceiverLinkForAddressMatchWhenLinkCreditIsSetToZero() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setAutoDelete(true);
         receiveFromAddress.setAutoDeleteDelay(10_000L);
         receiveFromAddress.setAutoDeleteMessageCount(-1L);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(
               "test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort() + "?amqpCredits=0");
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createTopic("test"));

            connection.start();

            assertNull(consumer.receiveNoWait());
            consumer.close();

            // Should be no interactions with the peer as credit is zero and address policy
            // will not apply to any match when credit cannot be offered to avoid stranding
            // a receiver on a remote address with no credit.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testCoreMessageConvertedToAMQPWhenTunnelingDisabled() throws Exception {
      doTestCoreMessageHandlingBasedOnTunnelingState(false);
   }

   @Test(timeout = 20000)
   public void testCoreMessageNotConvertedToAMQPWhenTunnelingEnabled() throws Exception {
      doTestCoreMessageHandlingBasedOnTunnelingState(true);
   }

   private void doTestCoreMessageHandlingBasedOnTunnelingState(boolean tunneling) throws Exception {
      server.start();

      final String[] receiverOfferedCapabilities;
      final int messageFormat;

      if (tunneling) {
         receiverOfferedCapabilities = new String[] {AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString()};
         messageFormat = AMQP_TUNNELED_CORE_MESSAGE_FORMAT;
      } else {
         receiverOfferedCapabilities = null;
         messageFormat = 0;
      }

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-address-receiver")
                                       .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                                       .withDesiredCapabilities(AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString())
                                       .withSource().withAddress("test");

         // Connect to remote as if an address had demand and matched our federation policy
         // If core message tunneling is enabled we include the desired capability
         peer.remoteAttach().ofReceiver()
                            .withOfferedCapabilities(receiverOfferedCapabilities)
                            .withDesiredCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName("federation-address-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test")
                                         .withCapabilities("topic")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectTransfer().withNonNullPayload()
                              .withMessageFormat(messageFormat).accept();

         final ConnectionFactory factory = CFUtil.createConnectionFactory(
            "CORE", "tcp://localhost:" + AMQP_PORT + "?minLargeMessageSize=512");

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createTopic("test"));

            producer.send(session.createTextMessage("Hello World"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testCoreLargeMessageConvertedToAMQPWhenTunnelingDisabled() throws Exception {
      doTestCoreLargeMessageHandlingBasedOnTunnelingState(false);
   }

   @Test(timeout = 20000)
   public void testCoreLargeMessageNotConvertedToAMQPWhenTunnelingEnabled() throws Exception {
      doTestCoreLargeMessageHandlingBasedOnTunnelingState(true);
   }

   private void doTestCoreLargeMessageHandlingBasedOnTunnelingState(boolean tunneling) throws Exception {
      server.start();

      final String[] receiverOfferedCapabilities;
      final int messageFormat;

      if (tunneling) {
         receiverOfferedCapabilities = new String[] {AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString()};
         messageFormat = AMQP_TUNNELED_CORE_LARGE_MESSAGE_FORMAT;
      } else {
         receiverOfferedCapabilities = null;
         messageFormat = 0;
      }

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-address-receiver")
                                       .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                                       .withDesiredCapabilities(AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString())
                                       .withSource().withAddress("test");

         // Connect to remote as if an address had demand and matched our federation policy
         // If core message tunneling is enabled we include the offered capability
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withOfferedCapabilities(receiverOfferedCapabilities)
                            .withName("federation-address-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test")
                                         .withCapabilities("topic")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectTransfer().withNonNullPayload()
                              .withMessageFormat(messageFormat).accept();

         final ConnectionFactory factory = CFUtil.createConnectionFactory(
            "CORE", "tcp://localhost:" + AMQP_PORT + "?minLargeMessageSize=512");

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createTopic("test"));

            final byte[] payload = new byte[1024];
            Arrays.fill(payload, (byte) 65);

            final BytesMessage message = session.createBytesMessage();

            message.writeBytes(payload);

            producer.send(message);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testFederationStartedTriggersRemoteDemandWithExistingAddressBindings() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.setAutostart(false);
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.MULTICAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         Wait.assertTrue(() -> server.queueQuery(SimpleString.toSimpleString("test")).isExists());

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         // Create demand on the addresses so that on start federation should happen
         final Connection connection = factory.createConnection();
         final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);

         session.createConsumer(session.createTopic("test"));
         session.createConsumer(session.createTopic("test"));

         // Add other non-federation address bindings for the policy to check on start.
         session.createConsumer(session.createTopic("a1"));
         session.createConsumer(session.createTopic("a2"));

         connection.start();

         // Should be no interactions at this point, check to make sure.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         // Starting the broker connection should trigger federation of address with demand.
         server.getBrokerConnections().forEach(c -> {
            try {
               c.start();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         });

         // Add more demand while federation is starting
         session.createConsumer(session.createTopic("test"));
         session.createConsumer(session.createTopic("test"));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         // This removes the connection demand, but leaves behind the static queue
         connection.close();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDetach().respond();

         // This should trigger the federation consumer to be shutdown as the statically defined queue
         // should be the only remaining demand on the address.
         logger.info("Removing Queues from federated address to eliminate demand");
         server.destroyQueue(SimpleString.toSimpleString("test"));
         Wait.assertFalse(() -> server.queueQuery(SimpleString.toSimpleString("test")).isExists());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testFederationStartedTriggersRemoteDemandWithExistingAddressAndDivertBindings() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setEnableDivertBindings(true);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.setAutostart(false);
         amqpConnection.addElement(element);

         final DivertConfiguration divert = new DivertConfiguration();
         divert.setName("test-divert");
         divert.setAddress("test");
         divert.setExclusive(false);
         divert.setForwardingAddress("target");
         divert.setRoutingType(ComponentConfigurationRoutingType.MULTICAST);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         // Configure addresses and divert for the test
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("target"), RoutingType.MULTICAST));
         server.deployDivert(divert);

         // Create demand on the addresses so that on start federation should happen
         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);
         final Connection connection = factory.createConnection();
         final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
         final Topic test = session.createTopic("test");
         final Topic target = session.createTopic("target");

         session.createConsumer(test);
         session.createConsumer(test);

         session.createConsumer(target);
         session.createConsumer(target);

         // Add other non-federation address bindings for the policy to check on start.
         session.createConsumer(session.createTopic("a1"));
         session.createConsumer(session.createTopic("a2"));

         connection.start();

         // Should be no interactions at this point, check to make sure.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         // Starting the broker connection should trigger federation of address with demand.
         server.getBrokerConnections().forEach(c -> {
            try {
               c.start();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         });

         // Add more demand while federation is starting
         session.createConsumer(test);
         session.createConsumer(target);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDetach().respond();

         // This removes the connection demand, but leaves behind the static queue
         connection.close();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testFederationStartTriggersFederationWithMultipleDivertsAndRemainsActiveAfterOneRemoved() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setEnableDivertBindings(true);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.setAutostart(false);
         amqpConnection.addElement(element);

         final DivertConfiguration divert1 = new DivertConfiguration();
         divert1.setName("test-divert-1");
         divert1.setAddress("test");
         divert1.setExclusive(false);
         divert1.setForwardingAddress("target1,target2");
         divert1.setRoutingType(ComponentConfigurationRoutingType.MULTICAST);

         final DivertConfiguration divert2 = new DivertConfiguration();
         divert2.setName("test-divert-2");
         divert2.setAddress("test");
         divert2.setExclusive(false);
         divert2.setForwardingAddress("target1,target3");
         divert2.setRoutingType(ComponentConfigurationRoutingType.MULTICAST);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         // Configure addresses and divert for the test
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("target1"), RoutingType.MULTICAST));
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("target2"), RoutingType.MULTICAST));
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("target3"), RoutingType.MULTICAST));
         server.deployDivert(divert1);
         server.deployDivert(divert2);

         // Create demand on the addresses so that on start federation should happen
         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);
         final Connection connection = factory.createConnection();
         final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
         final Topic target1 = session.createTopic("target1");
         final Topic target2 = session.createTopic("target2");
         final Topic target3 = session.createTopic("target2");

         session.createConsumer(target1);
         session.createConsumer(target2);
         session.createConsumer(target3);

         connection.start();

         // Should be no interactions at this point, check to make sure.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("address-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         // Starting the broker connection should trigger federation of address with demand.
         server.getBrokerConnections().forEach(c -> {
            try {
               c.start();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         });

         // Add more demand while federation is starting
         session.createConsumer(target1);
         session.createConsumer(target2);
         session.createConsumer(target3);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         server.destroyDivert(SimpleString.toSimpleString(divert1.getName()));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDetach().respond();

         server.destroyDivert(SimpleString.toSimpleString(divert2.getName()));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         connection.close();

         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testFederationPluginCanLimitDemandToOnlyTheConfiguredDivert() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respond()
                            .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement receiveFromAddress = new AMQPFederationAddressPolicyElement();
         receiveFromAddress.setName("address-policy");
         receiveFromAddress.addToIncludes("test");
         receiveFromAddress.setEnableDivertBindings(true);

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalAddressPolicy(receiveFromAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(5);
         amqpConnection.addElement(element);

         final DivertConfiguration divert = new DivertConfiguration();
         divert.setName("test-divert-1");
         divert.setAddress("test");
         divert.setExclusive(false);
         divert.setForwardingAddress("target");
         divert.setRoutingType(ComponentConfigurationRoutingType.MULTICAST);

         final AMQPTestFederationBrokerPlugin federationPlugin = new AMQPTestFederationBrokerPlugin();
         federationPlugin.shouldCreateConsumerForDivert = (d, q) -> true;
         federationPlugin.shouldCreateConsumerForQueue = (q) -> {
            // Disallow any binding on the source address from creating federation demand
            // any other binding that matches the policy will create demand for federation.
            if (q.getAddress().toString().equals("test")) {
               return false;
            }

            return true;
         };

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         // Configure addresses and divert for the test
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("target"), RoutingType.MULTICAST));
         server.deployDivert(divert);
         server.registerBrokerPlugin(federationPlugin);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);

            // Should be ignored as plugin rejects this binding as demand.
            final MessageConsumer consumer = session.createConsumer(session.createTopic("test"));

            connection.start();

            // Get a round trip to the broker to allow time for federation to hopefully
            // reject this first consumer on the source address.
            consumer.receiveNoWait();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withDesiredCapability(FEDERATION_ADDRESS_RECEIVER.toString())
                               .withName(allOf(containsString("sample-federation"),
                                               containsString("test"),
                                               containsString("address-receiver"),
                                               containsString(server.getNodeID().toString())))
                               .respond()
                               .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString());
            peer.expectFlow().withLinkCredit(1000);

            session.createConsumer(session.createTopic("target"));

            // Now a federation receiver should get created for the divert demand.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   public static class ApplicationPropertiesTransformer implements Transformer {

      private final Map<String, String> properties = new HashMap<>();

      @Override
      public void init(Map<String, String> externalProperties) {
         properties.putAll(externalProperties);
      }

      @Override
      public org.apache.activemq.artemis.api.core.Message transform(org.apache.activemq.artemis.api.core.Message message) {
         if (!(message instanceof AMQPMessage)) {
            return message;
         }

         properties.forEach((k, v) -> {
            message.putStringProperty(k, v);
         });

         // An AMQP message must be encoded again to carry along the modifications.
         message.reencode();

         return message;
      }
   }

   private void sendAddresPolicyToRemote(ProtonTestClient peer, FederationReceiveFromAddressPolicy policy) {
      final Map<String, Object> policyMap = new LinkedHashMap<>();

      policyMap.put(POLICY_NAME, policy.getPolicyName());
      policyMap.put(ADDRESS_AUTO_DELETE, policy.isAutoDelete());
      policyMap.put(ADDRESS_AUTO_DELETE_DELAY, policy.getAutoDeleteDelay());
      policyMap.put(ADDRESS_AUTO_DELETE_MSG_COUNT, policy.getAutoDeleteMessageCount());
      policyMap.put(ADDRESS_MAX_HOPS, policy.getMaxHops());
      policyMap.put(ADDRESS_ENABLE_DIVERT_BINDINGS, policy.isEnableDivertBindings());

      if (!policy.getIncludes().isEmpty()) {
         policyMap.put(ADDRESS_INCLUDES, new ArrayList<>(policy.getIncludes()));
      }
      if (!policy.getExcludes().isEmpty()) {
         policyMap.put(ADDRESS_EXCLUDES, new ArrayList<>(policy.getExcludes()));
      }

      final TransformerConfiguration transformerConfig = policy.getTransformerConfiguration();

      if (transformerConfig != null) {
         policyMap.put(TRANSFORMER_CLASS_NAME, transformerConfig.getClassName());
         if (transformerConfig.getProperties() != null && !transformerConfig.getProperties().isEmpty()) {
            policyMap.put(TRANSFORMER_PROPERTIES_MAP, transformerConfig.getProperties());
         }
      }

      if (!policy.getProperties().isEmpty()) {
         policyMap.put(POLICY_PROPERTIES_MAP, policy.getProperties());
      }

      peer.remoteTransfer().withDeliveryId(0)
                           .withMessageAnnotations().withAnnotation(OPERATION_TYPE.toString(), ADD_ADDRESS_POLICY)
                           .also()
                           .withBody().withValue(policyMap)
                           .also()
                           .now();
   }

   // Use this method to script the initial handshake that a broker that is establishing
   // a federation connection with a remote broker instance would perform.
   private void scriptFederationConnectToRemote(ProtonTestClient peer, String federationName) {
      scriptFederationConnectToRemote(peer, federationName, AmqpSupport.AMQP_CREDITS_DEFAULT, AmqpSupport.AMQP_LOW_CREDITS_DEFAULT);
   }

   private void scriptFederationConnectToRemote(ProtonTestClient peer, String federationName, int amqpCredits, int amqpLowCredits) {
      final String federationControlLinkName = "Federation:control:" + UUID.randomUUID().toString();

      final Map<String, Object> federationConfiguration = new HashMap<>();
      federationConfiguration.put(RECEIVER_CREDITS, amqpCredits);
      federationConfiguration.put(RECEIVER_CREDITS_LOW, amqpLowCredits);

      final Map<String, Object> senderProperties = new HashMap<>();
      senderProperties.put(FEDERATION_CONFIGURATION.toString(), federationConfiguration);

      peer.queueClientSaslAnonymousConnect();
      peer.remoteOpen().queue();
      peer.expectOpen();
      peer.remoteBegin().queue();
      peer.expectBegin();
      peer.remoteAttach().ofSender()
                         .withInitialDeliveryCount(0)
                         .withName(federationControlLinkName)
                         .withPropertiesMap(senderProperties)
                         .withDesiredCapabilities(FEDERATION_CONTROL_LINK.toString())
                         .withSenderSettleModeUnsettled()
                         .withReceivervSettlesFirst()
                         .withSource().also()
                         .withTarget().withDynamic(true)
                                      .withDurabilityOfNone()
                                      .withExpiryPolicyOnLinkDetach()
                                      .withLifetimePolicyOfDeleteOnClose()
                                      .withCapabilities("temporary-topic")
                                      .also()
                         .queue();
      peer.expectAttach().ofReceiver()
                         .withTarget()
                            .withAddress(notNullValue())
                         .also()
                         .withOfferedCapability(FEDERATION_CONTROL_LINK.toString());
      peer.expectFlow();
   }

   private class AMQPTestFederationBrokerPlugin implements ActiveMQServerAMQPFederationPlugin {

      public final AtomicBoolean started = new AtomicBoolean();
      public final AtomicBoolean stopped = new AtomicBoolean();

      public final AtomicReference<FederationConsumerInfo> beforeCreateConsumerCapture = new AtomicReference<>();
      public final AtomicReference<FederationConsumer> afterCreateConsumerCapture = new AtomicReference<>();
      public final AtomicReference<FederationConsumer> beforeCloseConsumerCapture = new AtomicReference<>();
      public final AtomicReference<FederationConsumer> afterCloseConsumerCapture = new AtomicReference<>();

      public Consumer<FederationConsumerInfo> beforeCreateConsumer = (c) -> beforeCreateConsumerCapture.set(c);;
      public Consumer<FederationConsumer> afterCreateConsumer = (c) -> afterCreateConsumerCapture.set(c);
      public Consumer<FederationConsumer> beforeCloseConsumer = (c) -> beforeCloseConsumerCapture.set(c);
      public Consumer<FederationConsumer> afterCloseConsumer = (c) -> afterCloseConsumerCapture.set(c);

      public BiConsumer<FederationConsumer, org.apache.activemq.artemis.api.core.Message> beforeMessageHandled = (c, m) -> { };
      public BiConsumer<FederationConsumer, org.apache.activemq.artemis.api.core.Message> afterMessageHandled = (c, m) -> { };

      public Function<AddressInfo, Boolean> shouldCreateConsumerForAddress = (a) -> true;
      public Function<org.apache.activemq.artemis.core.server.Queue, Boolean> shouldCreateConsumerForQueue = (q) -> true;
      public BiFunction<Divert, org.apache.activemq.artemis.core.server.Queue, Boolean> shouldCreateConsumerForDivert = (d, q) -> true;

      @Override
      public void federationStarted(final Federation federation) throws ActiveMQException {
         started.set(true);
      }

      @Override
      public void federationStopped(final Federation federation) throws ActiveMQException {
         stopped.set(true);
      }

      @Override
      public void beforeCreateFederationConsumer(final FederationConsumerInfo consumerInfo) throws ActiveMQException {
         beforeCreateConsumer.accept(consumerInfo);
      }

      @Override
      public void afterCreateFederationConsumer(final FederationConsumer consumer) throws ActiveMQException {
         afterCreateConsumer.accept(consumer);
      }

      @Override
      public void beforeCloseFederationConsumer(final FederationConsumer consumer) throws ActiveMQException {
         beforeCloseConsumer.accept(consumer);
      }

      @Override
      public void afterCloseFederationConsumer(final FederationConsumer consumer) throws ActiveMQException {
         afterCloseConsumer.accept(consumer);
      }

      @Override
      public void beforeFederationConsumerMessageHandled(final FederationConsumer consumer, org.apache.activemq.artemis.api.core.Message message) throws ActiveMQException {
         beforeMessageHandled.accept(consumer, message);
      }

      @Override
      public void afterFederationConsumerMessageHandled(final FederationConsumer consumer, org.apache.activemq.artemis.api.core.Message message) throws ActiveMQException {
         afterMessageHandled.accept(consumer, message);
      }

      @Override
      public boolean shouldCreateFederationConsumerForAddress(final AddressInfo address) throws ActiveMQException {
         return shouldCreateConsumerForAddress.apply(address);
      }

      @Override
      public boolean shouldCreateFederationConsumerForQueue(final org.apache.activemq.artemis.core.server.Queue queue) throws ActiveMQException {
         return shouldCreateConsumerForQueue.apply(queue);
      }

      @Override
      public boolean shouldCreateFederationConsumerForDivert(Divert divert, org.apache.activemq.artemis.core.server.Queue queue) throws ActiveMQException {
         return shouldCreateConsumerForDivert.apply(divert, queue);
      }
   }
}
