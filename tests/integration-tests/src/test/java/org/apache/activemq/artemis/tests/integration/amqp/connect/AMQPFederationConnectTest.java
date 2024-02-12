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
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.ADD_QUEUE_POLICY;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONFIGURATION;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONTROL_LINK;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONTROL_LINK_VALIDATION_ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_EVENT_LINK;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.LARGE_MESSAGE_THRESHOLD;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.LINK_ATTACH_TIMEOUT;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.OPERATION_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.POLICY_NAME;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.QUEUE_EXCLUDES;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.QUEUE_INCLUDES;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.QUEUE_INCLUDE_FEDERATED;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.QUEUE_PRIORITY_ADJUSTMENT;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS_LOW;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.allOf;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederatedBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationAddressPolicyElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationQueuePolicyElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.protonj2.test.driver.ProtonTestClient;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.MessageAnnotationsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.TransferPayloadCompositeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.hamcrest.Matchers;
import org.jgroups.util.UUID;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests basic connect handling of the AMQP federation feature.
 */
public class AMQPFederationConnectTest extends AmqpClientTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Override
   protected ActiveMQServer createServer() throws Exception {
      // Creates the broker used to make the outgoing connection. The port passed is for
      // that brokers acceptor. The test server connected to by the broker binds to a random port.
      return createServer(AMQP_PORT, false);
   }

   @Test(timeout = 20000)
   public void testBrokerConnectsWithAnonymous() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect("PLAIN", "ANONYMOUS");
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         // No user or pass given, it will have to select ANONYMOUS even though PLAIN also offered
         AMQPBrokerConnectConfiguration amqpConnection =
               new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
      }
   }

   @Test(timeout = 20000)
   public void testFederatedBrokerConnectsWithPlain() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLPlainConnect("user", "pass", "PLAIN", "ANONYMOUS");
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         AMQPBrokerConnectConfiguration amqpConnection =
               new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         server.getConfiguration().addAMQPConnection(amqpConnection);
         amqpConnection.setUser("user");
         amqpConnection.setPassword("pass");
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
      }
   }

   @Test(timeout = 20000)
   public void testFederationConfiguredCreatesControlLink() throws Exception {
      final int AMQP_MIN_LARGE_MESSAGE_SIZE = 10_000;
      final int AMQP_CREDITS = 100;
      final int AMQP_CREDITS_LOW = 50;
      final int AMQP_LINK_ATTACH_TIMEOUT = 60;
      final boolean AMQP_TUNNEL_CORE_MESSAGES = false;

      final Map<String, Object> federationConfiguration = new HashMap<>();
      federationConfiguration.put(RECEIVER_CREDITS, AMQP_CREDITS);
      federationConfiguration.put(RECEIVER_CREDITS_LOW, AMQP_CREDITS_LOW);
      federationConfiguration.put(LARGE_MESSAGE_THRESHOLD, AMQP_MIN_LARGE_MESSAGE_SIZE);
      federationConfiguration.put(LINK_ATTACH_TIMEOUT, AMQP_LINK_ATTACH_TIMEOUT);
      federationConfiguration.put(AmqpSupport.TUNNEL_CORE_MESSAGES, AMQP_TUNNEL_CORE_MESSAGES);

      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect("PLAIN", "ANONYMOUS");
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withDesiredCapability(AMQPFederationConstants.FEDERATION_CONTROL_LINK.toString())
                            .withName(allOf(containsString("Federation"), containsString("myFederation")))
                            .withProperty(FEDERATION_CONFIGURATION.toString(), federationConfiguration)
                            .withTarget().withDynamic(true)
                                         .withCapabilities("temporary-topic")
                            .and()
                            .respond()
                            .withTarget().withAddress("test-control-address")
                            .and()
                            .withOfferedCapabilities(AMQPFederationConstants.FEDERATION_CONTROL_LINK.toString());
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(
            "testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort() +
            "?amqpCredits=" + AMQP_CREDITS + "&amqpLowCredits=" + AMQP_CREDITS_LOW +
            "&amqpMinLargeMessageSize=" + AMQP_MIN_LARGE_MESSAGE_SIZE);
         amqpConnection.setReconnectAttempts(0);// No reconnects
         final AMQPFederatedBrokerConnectionElement federation = new AMQPFederatedBrokerConnectionElement("myFederation");
         federation.addProperty(LINK_ATTACH_TIMEOUT, AMQP_LINK_ATTACH_TIMEOUT);
         federation.addProperty(AmqpSupport.TUNNEL_CORE_MESSAGES, Boolean.toString(AMQP_TUNNEL_CORE_MESSAGES));
         amqpConnection.addElement(federation);
         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         Wait.assertTrue(() -> server.locateQueue("test-control-address") != null);
      }
   }

   @Test(timeout = 20000)
   public void testFederationCreatesControlLinkAndClosesConnectionIfCapabilityIsAbsent() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect("PLAIN", "ANONYMOUS");
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender().withDesiredCapability(AMQPFederationConstants.FEDERATION_CONTROL_LINK.toString()).respond();
         peer.expectConnectionToDrop();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         AMQPBrokerConnectConfiguration amqpConnection =
               new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(new AMQPFederatedBrokerConnectionElement("test"));
         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
      }
   }

   @Test(timeout = 20000)
   public void testFederationCreatesControlLinkAndClosesConnectionDetachIndicatesNotAuthorized() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect("PLAIN", "ANONYMOUS");
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender().withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                                       .respond()
                                       .withOfferedCapabilities(FEDERATION_CONTROL_LINK.toString())
                                       .withSource()
                                       .also()
                                       .withNullTarget();
         peer.remoteDetach().withErrorCondition("amqp:unauthorized-access", "Not authroized").queue();
         peer.expectDetach().optional();
         peer.expectClose().optional();
         peer.expectConnectionToDrop();
         // Broker reconnect and allow it to attach this time.
         peer.expectSASLAnonymousConnect("PLAIN", "ANONYMOUS");
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true).and()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respondInKind()
                            .withTarget().withAddress("dynamic-name");
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         AMQPBrokerConnectConfiguration amqpConnection =
               new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(1);// One reconnects
         amqpConnection.setRetryInterval(200);
         amqpConnection.addElement(new AMQPFederatedBrokerConnectionElement("test"));
         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(10, TimeUnit.SECONDS);

         server.stop();
      }
   }

   @Test(timeout = 20000)
   public void testFederationSendsReceiveFromQueuePolicyToRemoteWhenSendToIsConfigured() throws Exception {
      final MessageAnnotationsMatcher maMatcher = new MessageAnnotationsMatcher(true);
      maMatcher.withEntry(OPERATION_TYPE.toString(), Matchers.is(ADD_QUEUE_POLICY));
      final Map<String, Object> policyMap = new LinkedHashMap<>();

      final List<String> includes = new ArrayList<>();
      includes.add("a");
      includes.add("b");
      includes.add("c");
      includes.add("d");
      final List<String> excludes = new ArrayList<>();
      excludes.add("e");
      excludes.add("f");
      excludes.add("g");
      excludes.add("h");

      policyMap.put(POLICY_NAME, "test-policy");
      policyMap.put(QUEUE_INCLUDE_FEDERATED, true);
      policyMap.put(QUEUE_PRIORITY_ADJUSTMENT, 42);
      policyMap.put(QUEUE_INCLUDES, includes);
      policyMap.put(QUEUE_EXCLUDES, excludes);

      final EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher(policyMap);
      final TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
      payloadMatcher.setMessageAnnotationsMatcher(maMatcher);
      payloadMatcher.addMessageContentMatcher(bodyMatcher);

      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true).and()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respondInKind().withTarget().withAddress("test-dynamic");
         peer.remoteFlow().withLinkCredit(10).queue();
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true).and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind().withTarget().withAddress("test-dynamic-events");
         peer.remoteFlow().withLinkCredit(10).queue();
         peer.expectTransfer().withPayload(payloadMatcher);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement sendToQueue = new AMQPFederationQueuePolicyElement();
         sendToQueue.setName("test-policy");
         sendToQueue.setIncludeFederated(true);
         sendToQueue.setPriorityAdjustment(42);
         sendToQueue.addToIncludes("a", "b");
         sendToQueue.addToIncludes("c", "d");
         sendToQueue.addToExcludes("e", "f");
         sendToQueue.addToExcludes("g", "h");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("test");
         element.addRemoteQueuePolicy(sendToQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testFederationSendsReceiveFromQueuePolicyToRemoteWhenSendToIsConfiguredAndEventSenderRejected() throws Exception {
      final MessageAnnotationsMatcher maMatcher = new MessageAnnotationsMatcher(true);
      maMatcher.withEntry(OPERATION_TYPE.toString(), Matchers.is(ADD_QUEUE_POLICY));
      final Map<String, Object> policyMap = new LinkedHashMap<>();

      final List<String> includes = new ArrayList<>();
      includes.add("a");
      includes.add("b");
      includes.add("c");
      includes.add("d");
      final List<String> excludes = new ArrayList<>();
      excludes.add("e");
      excludes.add("f");
      excludes.add("g");
      excludes.add("h");

      policyMap.put(POLICY_NAME, "test-policy");
      policyMap.put(QUEUE_INCLUDE_FEDERATED, true);
      policyMap.put(QUEUE_PRIORITY_ADJUSTMENT, 42);
      policyMap.put(QUEUE_INCLUDES, includes);
      policyMap.put(QUEUE_EXCLUDES, excludes);

      final EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher(policyMap);
      final TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
      payloadMatcher.setMessageAnnotationsMatcher(maMatcher);
      payloadMatcher.addMessageContentMatcher(bodyMatcher);

      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true).and()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respondInKind().withTarget().withAddress("test-dynamic");
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true).and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respond()
                            .withNullSource();
         peer.expectDetach().respond();
         peer.remoteFlow().withHandle(0).withLinkCredit(10).queue(); // Ensure order of events
         peer.expectTransfer().withPayload(payloadMatcher);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPFederationQueuePolicyElement sendToQueue = new AMQPFederationQueuePolicyElement();
         sendToQueue.setName("test-policy");
         sendToQueue.setIncludeFederated(true);
         sendToQueue.setPriorityAdjustment(42);
         sendToQueue.addToIncludes("a", "b");
         sendToQueue.addToIncludes("c", "d");
         sendToQueue.addToExcludes("e", "f");
         sendToQueue.addToExcludes("g", "h");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("test");
         element.addRemoteQueuePolicy(sendToQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("testSimpleConnect", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testFederationSendsReceiveFromAddressPolicyToRemoteWhenSendToIsConfigured() throws Exception {
      final MessageAnnotationsMatcher maMatcher = new MessageAnnotationsMatcher(true);
      maMatcher.withEntry(OPERATION_TYPE.toString(), Matchers.is(ADD_ADDRESS_POLICY));
      final Map<String, Object> policyMap = new LinkedHashMap<>();

      final List<String> includes = new ArrayList<>();
      includes.add("include");
      final List<String> excludes = new ArrayList<>();
      excludes.add("exclude");

      policyMap.put(POLICY_NAME, "test-policy");
      policyMap.put(ADDRESS_AUTO_DELETE, true);
      policyMap.put(ADDRESS_AUTO_DELETE_DELAY, 42L);
      policyMap.put(ADDRESS_AUTO_DELETE_MSG_COUNT, 314L);
      policyMap.put(ADDRESS_MAX_HOPS, 5);
      policyMap.put(ADDRESS_ENABLE_DIVERT_BINDINGS, false);
      policyMap.put(ADDRESS_INCLUDES, includes);
      policyMap.put(ADDRESS_EXCLUDES, excludes);

      final EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher(policyMap);
      final TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
      payloadMatcher.setMessageAnnotationsMatcher(maMatcher);
      payloadMatcher.addMessageContentMatcher(bodyMatcher);

      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true).and()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respondInKind().withTarget().withAddress("test-dynamic");
         peer.remoteFlow().withLinkCredit(10).queue();
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true).and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respondInKind().withTarget().withAddress("test-dynamic-events");
         peer.remoteFlow().withLinkCredit(10).queue();
         peer.expectTransfer().withPayload(payloadMatcher);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement sendToAddress = new AMQPFederationAddressPolicyElement();
         sendToAddress.setName("test-policy");
         sendToAddress.setAutoDelete(true);
         sendToAddress.setAutoDeleteDelay(42L);
         sendToAddress.setAutoDeleteMessageCount(314L);
         sendToAddress.setMaxHops(5);
         sendToAddress.setEnableDivertBindings(false);
         sendToAddress.addToIncludes("include");
         sendToAddress.addToExcludes("exclude");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("test");
         element.addRemoteAddressPolicy(sendToAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-send-policy", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testFederationSendsReceiveFromAddressPolicyToRemoteWhenSendToIsConfiguredAndEventSenderRejected() throws Exception {
      final MessageAnnotationsMatcher maMatcher = new MessageAnnotationsMatcher(true);
      maMatcher.withEntry(OPERATION_TYPE.toString(), Matchers.is(ADD_ADDRESS_POLICY));
      final Map<String, Object> policyMap = new LinkedHashMap<>();

      final List<String> includes = new ArrayList<>();
      includes.add("include");
      final List<String> excludes = new ArrayList<>();
      excludes.add("exclude");

      policyMap.put(POLICY_NAME, "test-policy");
      policyMap.put(ADDRESS_AUTO_DELETE, true);
      policyMap.put(ADDRESS_AUTO_DELETE_DELAY, 42L);
      policyMap.put(ADDRESS_AUTO_DELETE_MSG_COUNT, 314L);
      policyMap.put(ADDRESS_MAX_HOPS, 5);
      policyMap.put(ADDRESS_ENABLE_DIVERT_BINDINGS, false);
      policyMap.put(ADDRESS_INCLUDES, includes);
      policyMap.put(ADDRESS_EXCLUDES, excludes);

      final EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher(policyMap);
      final TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
      payloadMatcher.setMessageAnnotationsMatcher(maMatcher);
      payloadMatcher.addMessageContentMatcher(bodyMatcher);

      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true).and()
                            .withDesiredCapability(FEDERATION_CONTROL_LINK.toString())
                            .respondInKind().withTarget().withAddress("test-dynamic");
         peer.expectAttach().ofSender()
                            .withTarget().withDynamic(true).and()
                            .withDesiredCapability(FEDERATION_EVENT_LINK.toString())
                            .respond()
                            .withNullTarget();
         peer.expectDetach().respond();
         peer.remoteFlow().withHandle(0).withLinkCredit(10).queue(); // Ensure order of events
         peer.expectTransfer().withPayload(payloadMatcher);
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPFederationAddressPolicyElement sendToAddress = new AMQPFederationAddressPolicyElement();
         sendToAddress.setName("test-policy");
         sendToAddress.setAutoDelete(true);
         sendToAddress.setAutoDeleteDelay(42L);
         sendToAddress.setAutoDeleteMessageCount(314L);
         sendToAddress.setMaxHops(5);
         sendToAddress.setEnableDivertBindings(false);
         sendToAddress.addToIncludes("include");
         sendToAddress.addToExcludes("exclude");

         final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
         element.setName("test");
         element.addRemoteAddressPolicy(sendToAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration("test-send-policy", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testConnectToBrokerFromRemoteAsFederatedSourceAndCreateControlLink() throws Exception {
      server.start();

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();

         logger.info("Test stopped");
      }
   }

   @Test(timeout = 20000)
   public void testConnectToBrokerFromRemoteAsFederatedSourceAndCreateEventsSenderLink() throws Exception {
      server.start();

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test", false, null, null, true, false);
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();

         logger.info("Test stopped");
      }
   }

   @Test(timeout = 20000)
   public void testConnectToBrokerFromRemoteAsFederatedSourceAndCreateEventsReceiverLink() throws Exception {
      server.start();

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test", false, null, null, false, true);
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();

         logger.info("Test stopped");
      }
   }

   @Test(timeout = 20000)
   public void testConnectToBrokerFromRemoteAsFederatedSourceAndCreateEventsLinks() throws Exception {
      server.start();

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test", false, null, null, true, true);
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();

         logger.info("Test stopped");
      }
   }

   @Test(timeout = 20000)
   public void testControlLinkPassesConnectAttemptWhenUserHasPrivledges() throws Exception {
      enableSecurity(server, FEDERATION_CONTROL_LINK_VALIDATION_ADDRESS);
      server.start();

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test", fullUser, fullPass);
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();

         logger.info("Test stopped");
      }
   }

   @Test(timeout = 20000)
   public void testControlLinkRefusesConnectAttemptWhenUseDoesNotHavePrivledgesForControlAddress() throws Exception {
      enableSecurity(server, FEDERATION_CONTROL_LINK_VALIDATION_ADDRESS);
      server.start();

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemoteNotAuthorizedForControlAddress(peer, "test", guestUser, guestPass);
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();

         logger.info("Test stopped");
      }
   }

   @Test(timeout = 20000)
   public void testRemoteConnectionCannotAttachEventReceiverLinkWithoutControlLink() throws Exception {
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
         peer.expectAttach().ofSender().withName("federation-event-receiver")
                                       .withNullSource()
                                       .withTarget();
         peer.expectDetach().withError(AmqpError.ILLEGAL_STATE.toString()).respond();

         // Attempt to create a federation event receiver link without existing control link
         peer.remoteAttach().ofReceiver()
                            .withDesiredCapabilities(FEDERATION_EVENT_LINK.toString())
                            .withName("federation-event-receiver")
                            .withSenderSettleModeSettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withCapabilities("temporary-topic")
                                         .withDynamic(true)
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
   public void testRemoteConnectionCannotAttachEventSenderLinkWithoutControlLink() throws Exception {
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
         peer.expectAttach().ofReceiver().withName("federation-event-sender")
                                         .withSource().also()
                                         .withNullTarget();
         peer.expectDetach().withError(AmqpError.ILLEGAL_STATE.toString()).respond();

         // Attempt to create a federation event receiver link without existing control link
         peer.remoteAttach().ofSender()
                            .withDesiredCapabilities(FEDERATION_EVENT_LINK.toString())
                            .withName("federation-event-sender")
                            .withSenderSettleModeSettled()
                            .withReceivervSettlesFirst()
                            .withTarget().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withCapabilities("temporary-topic")
                                         .withDynamic(true)
                                         .and()
                            .withSource().and()
                            .now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   // Use these methods to script the initial handshake that a broker that is establishing
   // a federation connection with a remote broker instance would perform.

   private void scriptFederationConnectToRemote(ProtonTestClient peer, String federationName) {
      scriptFederationConnectToRemote(peer, federationName, false, null, null);
   }

   private void scriptFederationConnectToRemote(ProtonTestClient peer, String federationName, String username, String password) {
      scriptFederationConnectToRemote(peer, federationName, true, username, password);
   }

   private void scriptFederationConnectToRemote(ProtonTestClient peer, String federationName, boolean auth, String username, String password) {
      scriptFederationConnectToRemote(peer, federationName, auth, username, password, false, false);
   }

   private void scriptFederationConnectToRemote(ProtonTestClient peer, String federationName, boolean auth, String username, String password, boolean eventsSender, boolean eventsReceiver) {
      final String federationControlLinkName = "Federation:test:" + UUID.randomUUID().toString();

      if (auth) {
         peer.queueClientSaslPlainConnect(username, password);
      } else {
         peer.queueClientSaslAnonymousConnect();
      }

      peer.remoteOpen().queue();
      peer.expectOpen();
      peer.remoteBegin().queue();
      peer.expectBegin();
      peer.remoteAttach().ofSender()
                         .withName(federationControlLinkName)
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
                         .withName(federationControlLinkName)
                         .withTarget()
                            .withAddress(notNullValue())
                         .also()
                         .withOfferedCapability(FEDERATION_CONTROL_LINK.toString());
      peer.expectFlow();

      if (eventsSender) {
         final String federationEventsSenderLinkName = "Federation:events-sender:test:" + UUID.randomUUID().toString();

         peer.remoteAttach().ofSender()
                            .withName(federationEventsSenderLinkName)
                            .withDesiredCapabilities(FEDERATION_EVENT_LINK.toString())
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
                            .withName(federationEventsSenderLinkName)
                            .withTarget()
                               .withAddress(notNullValue())
                            .also()
                            .withOfferedCapability(FEDERATION_EVENT_LINK.toString());
         peer.expectFlow();
      }

      if (eventsReceiver) {
         final String federationEventsSenderLinkName = "Federation:events-receiver:test:" + UUID.randomUUID().toString();

         peer.remoteAttach().ofReceiver()
                            .withName(federationEventsSenderLinkName)
                            .withDesiredCapabilities(FEDERATION_EVENT_LINK.toString())
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withTarget().also()
                            .withSource().withDynamic(true)
                                         .withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withLifetimePolicyOfDeleteOnClose()
                                         .withCapabilities("temporary-topic")
                                         .also()
                                         .queue();
         peer.remoteFlow().withLinkCredit(10).queue();
         peer.expectAttach().ofSender()
                            .withName(federationEventsSenderLinkName)
                            .withSource()
                               .withAddress(notNullValue())
                            .also()
                            .withOfferedCapability(FEDERATION_EVENT_LINK.toString());
      }
   }

   private void scriptFederationConnectToRemoteNotAuthorizedForControlAddress(ProtonTestClient peer, String federationName, String username, String password) {
      final String federationControlLinkName = "Federation:test:" + UUID.randomUUID().toString();

      peer.queueClientSaslPlainConnect(username, password);
      peer.remoteOpen().queue();
      peer.expectOpen();
      peer.remoteBegin().queue();
      peer.expectBegin();
      peer.remoteAttach().ofSender()
                         .withName(federationControlLinkName)
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
                         .withTarget(nullValue());
      peer.expectDetach().withError("amqp:unauthorized-access",
                                    "User does not have permission to attach to the federation control address").respond();
      peer.remoteClose().queue();
      peer.expectClose();
   }
}
