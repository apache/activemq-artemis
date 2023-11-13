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

import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADD_QUEUE_POLICY;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONFIGURATION;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONTROL_LINK;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_QUEUE_RECEIVER;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_RECEIVER_PRIORITY;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.LARGE_MESSAGE_THRESHOLD;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.OPERATION_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.POLICY_NAME;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.QUEUE_EXCLUDES;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.QUEUE_INCLUDES;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.QUEUE_INCLUDE_FEDERATED;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.QUEUE_PRIORITY_ADJUSTMENT;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS_LOW;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.TRANSFORMER_CLASS_NAME;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.TRANSFORMER_PROPERTIES_MAP;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.POLICY_PROPERTIES_MAP;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationPolicySupport.DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationQueueConsumer.DEFAULT_PULL_CREDIT_BATCH_SIZE;
import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPTunneledMessageConstants.AMQP_TUNNELED_CORE_MESSAGE_FORMAT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Message;
import javax.jms.Queue;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederatedBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationQueuePolicyElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.transformer.Transformer;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromQueuePolicy;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.qpid.protonj2.test.driver.ProtonTestClient;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.ApplicationPropertiesMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.HeaderMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.MessageAnnotationsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.PropertiesMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.TransferPayloadCompositeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.jgroups.util.UUID;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for AMQP Broker federation handling of the receive from and send to queue policy
 * configuration handling.
 */
public class AMQPFederationQueuePolicyTest extends AmqpClientTestSupport {

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
   public void testFederationCreatesQueueReceiverLinkForQueueMatchAnycast() throws Exception {
      doTestFederationCreatesQueueReceiverLinkForQueueMatch(RoutingType.ANYCAST);
   }

   @Test(timeout = 20000)
   public void testFederationCreatesQueueReceiverLinkForQueueMatchMulticast() throws Exception {
      doTestFederationCreatesQueueReceiverLinkForQueueMatch(RoutingType.MULTICAST);
   }

   private void doTestFederationCreatesQueueReceiverLinkForQueueMatch(RoutingType routingType) throws Exception {
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
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(routingType)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test::test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            if (RoutingType.MULTICAST.equals(routingType)) {
               // Requires FQQN to meet the test expectations
               session.createConsumer(session.createTopic("test::test"));
            } else {
               session.createConsumer(session.createQueue("test"));
            }

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationQueueReceiverCarriesConfiguredQueueFilter() throws Exception {
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
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setFilterString("color='red'")
                                                          .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                            .withSource().withJMSSelector("color='red'").and()
                            .respond()
                            .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationCreatesQueueReceiverLinkForQueueMatchUsingPolicyCredit() throws Exception {
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
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.addProperty(RECEIVER_CREDITS, "30");
         receiveFromQueue.addProperty(RECEIVER_CREDITS_LOW, "3");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                            .withSource().and()
                            .withTarget().and()
                            .respond()
                            .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(30);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testFederationClosesQueueReceiverWhenDemandIsRemovedFromQueue() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test::test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000).optional();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testFederationHandlesQueueDeletedAndConsumerRecreates() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test::test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000).optional();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            server.destroyQueue(SimpleString.toSimpleString("test"), null, false, true);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         // Consumer recreates Queue and adds demand back and federation should restart
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test::test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000).optional();

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testSecondQueueConsumerDoesNotGenerateAdditionalFederationReceiver() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000).optional();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testLinkCreatedForEachDistinctQueueMatchInSameConfiguredPolicy() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("addr1", "test.1");
         receiveFromQueue.addToIncludes("addr2", "test.2");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test.1").setRoutingType(RoutingType.ANYCAST)
                                                            .setAddress("addr1")
                                                            .setAutoCreated(false));
         server.createQueue(new QueueConfiguration("test.2").setRoutingType(RoutingType.ANYCAST)
                                                            .setAddress("addr2")
                                                            .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("addr1::test.1"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000).optional();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.1"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                               .withName(allOf(containsString("sample-federation"),
                                               containsString("addr2::test.2"),
                                               containsString("queue-receiver"),
                                               containsString(server.getNodeID().toString())))
                               .respondInKind();
            peer.expectFlow().withLinkCredit(1000).optional();

            session.createConsumer(session.createQueue("test.2"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testFederationReceiverCreatedWhenWildcardPolicyMatchesConsumerQueue() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress("test")
                                                                .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteCloseOfQueueReceiverRespondsToDetach() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress("test")
                                                                .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach();
            peer.remoteDetach().withErrorCondition("amqp:resource-deleted", "the resource was deleted").afterDelay(10).now();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testRejectedQueueReceiverAttachWhenLocalMatchingQueueNotFoundIsHandled() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress("test")
                                                                .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind()
                            .withNullSource();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteDetach().withErrorCondition("amqp:not-found", "the requested queue was not found").queue().afterDelay(10);
         peer.expectDetach();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            // Broker normally treats any remote link closure on the broker connection as terminal
            // but shouldn't in this case as it indicates the requested federated queue wasn't present
            // on the remote. New queue interest should result in a new attempt to federate the queue
            // and this time we will let it succeed.

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                               .withName(allOf(containsString("sample-federation"),
                                               containsString("test::test.queue"),
                                               containsString("queue-receiver"),
                                               containsString(server.getNodeID().toString())))
                               .respondInKind();
            peer.expectFlow().withLinkCredit(1000);

            session.createConsumer(session.createQueue("test.queue"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteCloseQueueReceiverWhenRemoteResourceIsDeletedIsHandled() throws Exception {
      doTestRemoteCloseQueueReceiverForExpectedConditionsIsHandled("amqp:resource-deleted");
   }

   @Test(timeout = 20000)
   public void testRemoteCloseQueueReceiverWhenRemoteReceiverIsForcedToDetachIsHandled() throws Exception {
      doTestRemoteCloseQueueReceiverForExpectedConditionsIsHandled("amqp:link:detach-forced");
   }

   private void doTestRemoteCloseQueueReceiverForExpectedConditionsIsHandled(String condition) throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress("test")
                                                                .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test::test.queue"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteDetach().withErrorCondition(condition, "error message from remote....").queue().afterDelay(20);
         peer.expectDetach();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            // Broker normally treats any remote link closure on the broker connection as terminal
            // but shouldn't in this case as it indicates the requested federated queue receiver was
            // forced closed. New queue interest should result in a new attempt to federate the queue

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                               .withName(allOf(containsString("sample-federation"),
                                               containsString("test::test.queue"),
                                               containsString("queue-receiver"),
                                               containsString(server.getNodeID().toString())))
                               .respondInKind();
            peer.expectFlow().withLinkCredit(1000);

            session.createConsumer(session.createQueue("test.queue"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testUnhandledRemoteReceiverCloseConditionCausesConnectionRebuild() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(1); // One reconnect to meet test expectations and use a
         amqpConnection.setRetryInterval(100);   // Short reconnect interval.
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress("test")
                                                                .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test::test.queue"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            // Broker treats some remote link closures on the broker connection as terminal
            // in this case we signal some internal error which should cause rebuild of the
            // broker connection.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            peer.expectDetach().optional(); // Broker is not consistent on sending the detach
            peer.expectClose().optional();
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender()
                               .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                               .respondInKind();
            peer.expectAttach().ofReceiver()
                               .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                               .withName(allOf(containsString("sample-federation"),
                                               containsString("test::test.queue"),
                                               containsString("queue-receiver"),
                                               containsString(server.getNodeID().toString())))
                               .respondInKind();
            peer.expectFlow().withLinkCredit(1000);

            // Trigger the error that should cause the broker to drop and reconnect
            peer.remoteDetach().withErrorCondition("amqp:internal-error", "the resource suffered an internal error").afterDelay(10).now();

            peer.waitForScriptToComplete(50, TimeUnit.SECONDS);
            peer.expectDetach(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testInboundMessageRoutedToReceiverOnLocalQueue() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                                .setAddress("test")
                                                                .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test::test.queue"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withSource().withAddress("test::test.queue")
                            .and()
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
            final MessageConsumer consumer = session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            final Message message = consumer.receive(20_0000);
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
   public void testFederationCreatesQueueReceiverLinkWithDefaultPrioirty() throws Exception {
      doTestFederationCreatesQueueReceiverLinkWithAdjustedPriority(0);
   }

   @Test(timeout = 20000)
   public void testFederationCreatesQueueReceiverLinkWithIncreasedPriority() throws Exception {
      doTestFederationCreatesQueueReceiverLinkWithAdjustedPriority(5);
   }

   @Test(timeout = 20000)
   public void testFederationCreatesQueueReceiverLinkWithDecreasedPriority() throws Exception {
      doTestFederationCreatesQueueReceiverLinkWithAdjustedPriority(-5);
   }

   private void doTestFederationCreatesQueueReceiverLinkWithAdjustedPriority(int adjustment) throws Exception {
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
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.setPriorityAdjustment(adjustment);
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test::test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(),
                                          ActiveMQDefaultConfiguration.getDefaultConsumerPriority() + adjustment)
                            .respond()
                            .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString());
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testLinkCreatedForEachDistinctQueueMatchInSameConfiguredPolicyWithSameAddressMatch() throws Exception {
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

         // Ensures the code doesn't lose track when the address value is the same across includes.
         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("addr", "test.1");
         receiveFromQueue.addToIncludes("addr", "test.2");
         receiveFromQueue.addToIncludes("addr", "test.3");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test.1").setRoutingType(RoutingType.ANYCAST)
                                                            .setAddress("addr")
                                                            .setAutoCreated(false));
         server.createQueue(new QueueConfiguration("test.2").setRoutingType(RoutingType.ANYCAST)
                                                            .setAddress("addr")
                                                            .setAutoCreated(false));
         server.createQueue(new QueueConfiguration("test.3").setRoutingType(RoutingType.ANYCAST)
                                                            .setAddress("addr")
                                                            .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("addr::test.1"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(1000).optional();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.1"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                               .withName(allOf(containsString("sample-federation"),
                                               containsString("addr::test.2"),
                                               containsString("queue-receiver"),
                                               containsString(server.getNodeID().toString())))
                               .respondInKind();
            peer.expectFlow().withLinkCredit(1000).optional();

            session.createConsumer(session.createQueue("test.2"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                               .withName(allOf(containsString("sample-federation"),
                                               containsString("addr::test.3"),
                                               containsString("queue-receiver"),
                                               containsString(server.getNodeID().toString())))
                               .respondInKind();
            peer.expectFlow().withLinkCredit(1000).optional();

            session.createConsumer(session.createQueue("test.3"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectDetach().respond();
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteBrokerAcceptsQueuePolicyFromControlLink() throws Exception {
      server.start();

      final Collection<Map.Entry<String, String>> includes = new ArrayList<>();
      includes.add(new AbstractMap.SimpleEntry<>("#", "testQueue"));
      final Collection<Map.Entry<String, String>> excludes = new ArrayList<>();
      excludes.add(new AbstractMap.SimpleEntry<>("address1", "test.#"));

      final FederationReceiveFromQueuePolicy policy =
         new FederationReceiveFromQueuePolicy("test-queue-policy",
                                              true, -2, includes, excludes, null, null,
                                              DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendQueuePolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteBrokerAcceptsQueuePolicyFromControlLinkWithTransformerConfiguration() throws Exception {
      server.start();

      final Collection<Map.Entry<String, String>> includes = new ArrayList<>();
      includes.add(new AbstractMap.SimpleEntry<>("#", "testQueue"));
      final Collection<Map.Entry<String, String>> excludes = new ArrayList<>();
      excludes.add(new AbstractMap.SimpleEntry<>("address1", "test.#"));

      final Map<String, String> transformerProperties = new HashMap<>();
      transformerProperties.put("key1", "value1");
      transformerProperties.put("key2", "value2");

      final TransformerConfiguration transformerConfiguration = new TransformerConfiguration();
      transformerConfiguration.setClassName(ApplicationPropertiesTransformer.class.getName());
      transformerConfiguration.setProperties(transformerProperties);

      final FederationReceiveFromQueuePolicy policy =
         new FederationReceiveFromQueuePolicy("test-queue-policy",
                                              true, -2, includes, excludes,
                                              null, transformerConfiguration,
                                              DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendQueuePolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testRemoteFederatesQueueWhenDemandIsApplied() throws Exception {
      server.start();

      final Collection<Map.Entry<String, String>> includes = new ArrayList<>();
      includes.add(new AbstractMap.SimpleEntry<>("#", "testQueue"));

      final FederationReceiveFromQueuePolicy policy =
         new FederationReceiveFromQueuePolicy("test-queue-policy",
                                              true, -2, includes, null, null, null,
                                              DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendQueuePolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withSource().withAddress("testQueue::testQueue")
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
            final MessageConsumer consumer = session.createConsumer(session.createQueue("testQueue"));

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
   public void testRemoteFederatesQueueWhenDemandIsAppliedUsingControllerDefinedLinkCredit() throws Exception {
      server.start();

      final Collection<Map.Entry<String, String>> includes = new ArrayList<>();
      includes.add(new AbstractMap.SimpleEntry<>("#", "testQueue"));

      final FederationReceiveFromQueuePolicy policy =
         new FederationReceiveFromQueuePolicy("test-queue-policy",
                                              true, -2, includes, null, null, null,
                                              DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test", 10, 9);
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendQueuePolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withSource().withAddress("testQueue::testQueue")
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
            final MessageConsumer consumer = session.createConsumer(session.createQueue("testQueue"));

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
   public void testRemoteFederatesQueueWhenDemandIsAppliedUsingPolicyDefinedLinkCredit() throws Exception {
      server.start();

      final Collection<Map.Entry<String, String>> includes = new ArrayList<>();
      includes.add(new AbstractMap.SimpleEntry<>("#", "testQueue"));

      final Map<String, Object> properties = new HashMap<>();
      properties.put(RECEIVER_CREDITS, "40");
      properties.put(RECEIVER_CREDITS_LOW, 39);
      properties.put(LARGE_MESSAGE_THRESHOLD, 2048);

      final FederationReceiveFromQueuePolicy policy =
         new FederationReceiveFromQueuePolicy("test-queue-policy",
                                              true, -2, includes, null, properties, null,
                                              DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test", 10, 9);
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendQueuePolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withSource().withAddress("testQueue::testQueue")
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
            final MessageConsumer consumer = session.createConsumer(session.createQueue("testQueue"));

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
   public void testRemoteFederatesQueueAndAppliesTransformerWhenDemandIsApplied() throws Exception {
      server.start();

      final Collection<Map.Entry<String, String>> includes = new ArrayList<>();
      includes.add(new AbstractMap.SimpleEntry<>("#", "testQueue"));

      final Map<String, String> transformerProperties = new HashMap<>();
      transformerProperties.put("key1", "value1");
      transformerProperties.put("key2", "value2");

      final TransformerConfiguration transformerConfiguration = new TransformerConfiguration();
      transformerConfiguration.setClassName(ApplicationPropertiesTransformer.class.getName());
      transformerConfiguration.setProperties(transformerProperties);

      final FederationReceiveFromQueuePolicy policy =
         new FederationReceiveFromQueuePolicy("test-queue-policy",
                                              true, -2, includes, null,
                                              null, transformerConfiguration,
                                              DEFAULT_WILDCARD_CONFIGURATION);

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDisposition().withSettled(true).withState().accepted();

         sendQueuePolicyToRemote(peer, policy);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withSource().withAddress("testQueue::testQueue")
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
            final MessageConsumer consumer = session.createConsumer(session.createQueue("testQueue"));

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
      server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                       .setAddress("test")
                                                       .setAutoCreated(false));

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("test::test")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withSource().withAddress("test::test");

         // Connect to remote as if an queue had demand and matched our federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("test::test")
                            .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test::test")
                                         .withCapabilities("queue")
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
   public void testRemoteBrokerRoutesInboundMessageToFederatedReceiver() throws Exception {
      server.start();
      server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                       .setAddress("test")
                                                       .setAutoCreated(false));

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("test::test")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withSource().withAddress("test::test");

         // Connect to remote as if an queue had demand and matched our federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("test::test")
                            .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test::test")
                                         .withCapabilities("queue")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(1).now();
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
            final MessageProducer producer = session.createProducer(session.createQueue("test"));

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
   public void testRemoteBrokerCanFailLinkAttachIfQueueDoesNotExistWithoutClosingTheConnection() throws Exception {
      server.start();

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("test::test").withSource(nullValue());
         peer.expectDetach().withError("amqp:not-found", "Queue: 'test' does not exist").respond();

         // Connect to remote as if an queue had demand and matched our federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("test::test")
                            .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test::test")
                                         .withCapabilities("queue")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("test::test")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withSource().withAddress("test::test");

         // Now create it and try again
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         // Connect to remote as if an queue had demand and matched our federation policy
         // this is our second attempt and the remote queue exists now so it should succeed.
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("test::test")
                            .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test::test")
                                         .withCapabilities("queue")
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
   public void testRemoteBrokerCanFailLinkAttachIfQueueDoesNotMatchFullExpectationWithoutClosingTheConnection() throws Exception {
      server.start();

      // Now create it and try again
      server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                       .setAddress("test")
                                                       .setAutoCreated(false));

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("test::test").withSource(nullValue());
         peer.expectDetach().withError("amqp:not-found", "Queue: 'test' is not mapped to specified address: address").respond();

         // Connect to remote as if an queue had demand and matched our federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("test::test")
                            .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("address::test")
                                         .withCapabilities("queue")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("test::test")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withSource().withAddress("test::test");

         // Connect to remote as if an queue had demand and matched our federation policy
         // this is our second attempt and the remote queue exists now so it should succeed.
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("test::test")
                            .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test::test")
                                         .withCapabilities("queue")
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
   public void testRemoteConnectionCannotAttachQueueFederationLinkWithoutControlLink() throws Exception {
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
         peer.expectAttach().ofSender().withName("federation-queue-receiver")
                                       .withSource(nullValue())
                                       .withTarget();
         peer.expectDetach().respond();

         // Attempt to create a federation queue receiver link without existing control link
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("federation-queue-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test::test")
                                         .withCapabilities("queue")
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
   public void testRemoteBrokerRoutesInboundMessageToFederatedReceiverWithFilterApplied() throws Exception {
      server.start();
      server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                       .setAddress("test")
                                                       .setAutoCreated(false));

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("test::test")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withSource().withAddress("test::test");

         // Connect to remote as if an queue had demand and matched our federation policy
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("test::test")
                            .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withJMSSelector("color = 'red'")
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test::test")
                                         .withCapabilities("queue")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         // Match typical generic Qpid JMS Text Message structure
         final HeaderMatcher headerMatcher = new HeaderMatcher(true);
         final MessageAnnotationsMatcher annotationsMatcher = new MessageAnnotationsMatcher(true);
         final ApplicationPropertiesMatcher appPropertiesMatcher = new ApplicationPropertiesMatcher(true);
         appPropertiesMatcher.withEntry("color", equalTo("red"));
         final PropertiesMatcher properties = new PropertiesMatcher(true);
         final EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher("red");
         final TransferPayloadCompositeMatcher matcher = new TransferPayloadCompositeMatcher();
         matcher.setHeadersMatcher(headerMatcher);
         matcher.setMessageAnnotationsMatcher(annotationsMatcher);
         matcher.setPropertiesMatcher(properties);
         matcher.setApplicationPropertiesMatcher(appPropertiesMatcher);
         matcher.addMessageContentMatcher(bodyMatcher);

         peer.expectTransfer().withPayload(matcher).accept();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue("test"));

            // Federation receiver should have applied filter that causes this message not to federate
            final Message blueMessage = session.createTextMessage("blue");
            blueMessage.setStringProperty("color", "blue");

            final Message redMessage = session.createTextMessage("red");
            redMessage.setStringProperty("color", "red");

            producer.send(blueMessage);
            producer.send(redMessage);

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
   public void testBrokerDoesNotFederateQueueIfOnlyDemandIsFromAnotherBrokerFederationSubscription() throws Exception {
      try (ProtonTestServer target = new ProtonTestServer()) {
         target.expectSASLAnonymousConnect();
         target.expectOpen().respond();
         target.expectBegin().respond();
         target.expectAttach().ofSender()
                              .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                              .respond()
                              .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         target.start();

         final URI remoteURI = target.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.setIncludeFederated(false); // No federation for federation subscriptions

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         target.waitForScriptToComplete(5, TimeUnit.SECONDS);

         // Simulate another broker connecting as a federation instance that will create demand on
         // queue "test::test" but should not generate demand to the server the broker connected
         // to as a federation control because we configured it to ignore federation subscriptions
         // when considering demand on the matching queues.
         try (ProtonTestClient client = new ProtonTestClient()) {
            scriptFederationConnectToRemote(client, "incoming-federation");
            client.connect("localhost", AMQP_PORT);

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectAttach().ofSender().withName("fake-federation-incoming-receiver-link")
                                            .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                            .withSource().withAddress("test::test");

            // Connect to remote as if a queue had demand and matched our federation policy
            client.remoteAttach().ofReceiver()
                                 .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                 .withName("fake-federation-incoming-receiver-link")
                                 .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                                 .withSenderSettleModeUnsettled()
                                 .withReceivervSettlesFirst()
                                 .withSource().withDurabilityOfNone()
                                              .withExpiryPolicyOnLinkDetach()
                                              .withAddress("test::test")
                                              .withCapabilities("queue")
                                              .and()
                                 .withTarget().and()
                                 .now();
            client.remoteFlow().withLinkCredit(10).now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectDetach();
            client.expectClose();

            client.remoteDetach().withErrorCondition("amqp:resource-deleted", "The resource was deleted").now();
            client.remoteClose().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         // Would fail if any frames arrived that are not scripted to.
         target.waitForScriptToComplete(5, TimeUnit.SECONDS);
         target.expectAttach().ofReceiver().withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                                           .withName(allOf(containsString("sample-federation"),
                                                           containsString("test::test"),
                                                           containsString("queue-receiver"),
                                                           containsString(server.getNodeID().toString())))
                                           .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT).respond()
                                           .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString());
         target.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         // Now create demand that isn't a federation consumer and the remote should see an incoming
         // receiver attach for the federated queue.
         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            target.waitForScriptToComplete(5, TimeUnit.SECONDS);
            target.expectClose();
            target.remoteClose().now();

            target.waitForScriptToComplete(5, TimeUnit.SECONDS);
            target.close();
         }
      }
   }

   @Test(timeout = 20000)
   public void testBrokerCanFederateQueueIfOnlyDemandIsFromAnotherBrokerFederationSubscription() throws Exception {
      try (ProtonTestServer target = new ProtonTestServer()) {
         target.expectSASLAnonymousConnect();
         target.expectOpen().respond();
         target.expectBegin().respond();
         target.expectAttach().ofSender()
                              .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                              .respond()
                              .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString());
         target.start();

         final URI remoteURI = target.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.setIncludeFederated(true); // do federate for federation subscriptions

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         target.waitForScriptToComplete(5, TimeUnit.SECONDS);
         // broker should create a new receiver that extends the federation receiver to this "broker"
         // but because this is a federation of a federation the priority should drop by an additional
         // increment as we apply the adjustment on each step
         target.expectAttach().ofReceiver().withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                                           .withName(allOf(containsString("sample-federation"),
                                                           containsString("test::test"),
                                                           containsString("queue-receiver"),
                                                           containsString(server.getNodeID().toString())))
                                           .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT - 1).respond()
                                           .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString());
         // Should get a flow but if the link goes away quick enough the broker won't get to this before detaching.
         target.expectFlow().withLinkCredit(1000).optional();
         target.expectDetach().respond();

         // Simulate another broker connecting as a federation instance that will create demand on
         // queue "test::test" and should generate demand to the server the broker connected to
         // as a federation control because we configured to not ignore federation subscriptions
         // when considering demand on the matching queues.
         try (ProtonTestClient client = new ProtonTestClient()) {
            scriptFederationConnectToRemote(client, "incoming-federation");
            client.connect("localhost", AMQP_PORT);

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectAttach().ofSender().withName("fake-federation-incoming-receiver-link")
                                            .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                            .withSource().withAddress("test::test");

            // Connect to remote as if a queue had demand and matched our federation policy
            client.remoteAttach().ofReceiver()
                                 .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                 .withName("fake-federation-incoming-receiver-link")
                                 .withProperty(FEDERATION_RECEIVER_PRIORITY.toString(), DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT)
                                 .withSenderSettleModeUnsettled()
                                 .withReceivervSettlesFirst()
                                 .withSource().withDurabilityOfNone()
                                              .withExpiryPolicyOnLinkDetach()
                                              .withAddress("test::test")
                                              .withCapabilities("queue")
                                              .and()
                                 .withTarget().and()
                                 .now();
            client.remoteFlow().withLinkCredit(10).now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);

            client.expectDetach();
            client.remoteDetach().withErrorCondition("amqp:resource-deleted", "Resource deleted").later(30);
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         target.waitForScriptToComplete(5, TimeUnit.SECONDS);
         target.expectClose();
         target.remoteClose().now();

         target.waitForScriptToComplete(5, TimeUnit.SECONDS);
         target.close();
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.setTransformerConfiguration(transformerConfiguration);
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("queue-receiver"),
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
            final MessageConsumer consumer = session.createConsumer(session.createQueue("test"));

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
   public void testPullQueueConsumerGrantsCreditOnEmptyQueue() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(
               "testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort() + "?amqpCredits=0");
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();
         peer.expectFlow().withLinkCredit(DEFAULT_PULL_CREDIT_BATCH_SIZE);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            peer.close();
         }
      }
   }

   @Test(timeout = 30000)
   public void testPullQueueConsumerGrantsCreditOnlyWhenPendingMessageIsConsumed() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(
               "testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort() + "?amqpCredits=0");
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();

         // Must be pull consumer to ensure we can check that demand on remote doesn't
         // offer credit until the pending message count on the Queue is zeroed
         final ConnectionFactory factory = CFUtil.createConnectionFactory(
            "AMQP", "tcp://localhost:" + AMQP_PORT + "?jms.prefetchPolicy.all=0");

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue("test");
            final MessageProducer producer = session.createProducer(queue);

            // Add to backlog
            producer.send(session.createMessage());

            // Now create demand on the queue
            final MessageConsumer consumer = session.createConsumer(queue);

            connection.start();
            producer.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withLinkCredit(DEFAULT_PULL_CREDIT_BATCH_SIZE);

            // Remove the backlog and credit should be offered to the remote
            assertNotNull(consumer.receiveNoWait());

            peer.waitForScriptToComplete(20, TimeUnit.SECONDS);

            peer.close();
         }
      }
   }

   @Test(timeout = 30000)
   public void testPullQueueConsumerBatchCreditTopUpAfterEachBacklogDrain() throws Exception {
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

         final AMQPFederationQueuePolicyElement receiveFromQueue = new AMQPFederationQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("sample-federation");
         element.addLocalQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(
               "testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort() + "?amqpCredits=0");
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withDesiredCapability(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName(allOf(containsString("sample-federation"),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString(server.getNodeID().toString())))
                            .respondInKind();

         // Must be pull consumer to ensure we can check that demand on remote doesn't
         // offer credit until the pending message count on the Queue is zeroed
         final ConnectionFactory factory = CFUtil.createConnectionFactory(
            "AMQP", "tcp://localhost:" + AMQP_PORT + "?jms.prefetchPolicy.all=0");

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue("test");
            final MessageProducer producer = session.createProducer(queue);

            // Add to backlog
            producer.send(session.createMessage());

            // Now create demand on the queue
            final MessageConsumer consumer = session.createConsumer(queue);

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withLinkCredit(DEFAULT_PULL_CREDIT_BATCH_SIZE);

            // Remove the backlog and credit should be offered to the remote
            assertNotNull(consumer.receiveNoWait());

            peer.waitForScriptToComplete(20, TimeUnit.SECONDS);

            // Consume all the credit that was presented in the batch
            for (int i = 0; i < DEFAULT_PULL_CREDIT_BATCH_SIZE; ++i) {
               peer.expectDisposition().withState().accepted();
               peer.remoteTransfer().withBody().withString("test-message")
                                    .also()
                                    .withDeliveryId(i)
                                    .now();
            }

            // Consume all the newly received message from the remote except one
            // which should leave the queue with a pending message so no credit
            // should be offered.
            for (int i = 0; i < DEFAULT_PULL_CREDIT_BATCH_SIZE - 1; ++i) {
               assertNotNull(consumer.receiveNoWait());
            }

            // We should not get a new batch yet as there is still one pending
            // message on the local queue we have not consumed.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withLinkCredit(DEFAULT_PULL_CREDIT_BATCH_SIZE);

            // Remove the backlog and credit should be offered to the remote again
            assertNotNull(consumer.receiveNoWait());

            peer.waitForScriptToComplete(20, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            consumer.close(); // Remove local demand and federation consumer is torn down.

            peer.waitForScriptToComplete(20, TimeUnit.SECONDS);
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
      server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                       .setAddress("test")
                                                       .setAutoCreated(false));

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
         peer.expectAttach().ofSender().withName("federation-queue-receiver")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withDesiredCapabilities(AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString())
                                       .withSource().withAddress("test::test");

         // Connect to remote as if an queue had demand and matched our federation policy
         // If core message tunneling is enabled we include the desired capability
         peer.remoteAttach().ofReceiver()
                            .withOfferedCapabilities(receiverOfferedCapabilities)
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("federation-queue-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test::test")
                                         .withCapabilities("queue")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectTransfer().withNonNullPayload()
                              .withMessageFormat(messageFormat).accept();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue("test"));

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
      server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                       .setAddress("test")
                                                       .setAutoCreated(false));

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
         peer.expectAttach().ofSender().withName("federation-queue-receiver")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withDesiredCapabilities(AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString())
                                       .withSource().withAddress("test::test");

         // Connect to remote as if an queue had demand and matched our federation policy
         // If core message tunneling is enabled we include the desired capability
         peer.remoteAttach().ofReceiver()
                            .withOfferedCapabilities(receiverOfferedCapabilities)
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("federation-queue-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress("test::test")
                                         .withCapabilities("queue")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectTransfer().withNonNullPayload()
                              .withMessageFormat(messageFormat).accept();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue("test"));

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

   private void sendQueuePolicyToRemote(ProtonTestClient peer, FederationReceiveFromQueuePolicy policy) {
      final Map<String, Object> policyMap = new LinkedHashMap<>();

      policyMap.put(POLICY_NAME, policy.getPolicyName());
      policyMap.put(QUEUE_INCLUDE_FEDERATED, policy.isIncludeFederated());
      policyMap.put(QUEUE_PRIORITY_ADJUSTMENT, policy.getPriorityAjustment());

      if (!policy.getIncludes().isEmpty()) {
         final List<String> flattenedIncludes = new ArrayList<>(policy.getIncludes().size() * 2);
         policy.getIncludes().forEach((entry) -> {
            flattenedIncludes.add(entry.getKey());
            flattenedIncludes.add(entry.getValue());
         });

         policyMap.put(QUEUE_INCLUDES, flattenedIncludes);
      }

      if (!policy.getExcludes().isEmpty()) {
         final List<String> flatteneExcludes = new ArrayList<>(policy.getExcludes().size() * 2);
         policy.getExcludes().forEach((entry) -> {
            flatteneExcludes.add(entry.getKey());
            flatteneExcludes.add(entry.getValue());
         });

         policyMap.put(QUEUE_EXCLUDES, flatteneExcludes);
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
                           .withMessageAnnotations().withAnnotation(OPERATION_TYPE.toString(), ADD_QUEUE_POLICY)
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
}
