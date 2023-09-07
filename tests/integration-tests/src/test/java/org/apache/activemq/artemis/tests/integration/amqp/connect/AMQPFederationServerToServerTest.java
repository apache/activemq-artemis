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

import java.lang.invoke.MethodHandles;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
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
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederatedBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationAddressPolicyElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationQueuePolicyElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ComponentConfigurationRoutingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test AMQP federation between two servers.
 */
public class AMQPFederationServerToServerTest extends AmqpClientTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final int SERVER_PORT = AMQP_PORT;
   private static final int SERVER_PORT_REMOTE = AMQP_PORT + 1;

   protected ActiveMQServer remoteServer;

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,CORE";
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      remoteServer = createServer(SERVER_PORT_REMOTE, false);

      return createServer(SERVER_PORT, false);
   }

   @After
   @Override
   public void tearDown() throws Exception {
      super.tearDown();

      try {
         if (remoteServer != null) {
            remoteServer.stop();
         }
      } catch (Exception e) {
      }
   }

   @Test(timeout = 20000)
   public void testAddresDemandOnLocalBrokerFederatesMessagesFromRemoteAMQP() throws Exception {
      testAddresDemandOnLocalBrokerFederatesMessagesFromRemote("AMQP");
   }

   @Test(timeout = 20000)
   public void testAddresDemandOnLocalBrokerFederatesMessagesFromRemoteCORE() throws Exception {
      testAddresDemandOnLocalBrokerFederatesMessagesFromRemote("CORE");
   }

   private void testAddresDemandOnLocalBrokerFederatesMessagesFromRemote(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPFederationAddressPolicyElement localAddressPolicy = new AMQPFederationAddressPolicyElement();
      localAddressPolicy.setName("test-policy");
      localAddressPolicy.addToIncludes("test");
      localAddressPolicy.setAutoDelete(false);
      localAddressPolicy.setAutoDeleteDelay(-1L);
      localAddressPolicy.setAutoDeleteMessageCount(-1L);

      final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
      element.setName("test");
      element.addLocalAddressPolicy(localAddressPolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://localhost:" + SERVER_PORT_REMOTE);
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

         final Topic topic = sessionL.createTopic("test");

         final MessageConsumer consumerL = sessionL.createConsumer(topic);

         connectionL.start();
         connectionR.start();

         // Demand on local address should trigger receiver on remote.
         Wait.assertTrue(() -> server.addressQuery(SimpleString.toSimpleString("test")).isExists());
         Wait.assertTrue(() -> remoteServer.addressQuery(SimpleString.toSimpleString("test")).isExists());

         final MessageProducer producerR = sessionR.createProducer(topic);
         final TextMessage message = sessionR.createTextMessage("Hello World");

         producerR.send(message);

         final Message received = consumerL.receive(5_000);
         assertNotNull(received);
      }
   }

   @Test(timeout = 20000)
   public void testDivertAddressDemandOnLocalBrokerFederatesMessagesFromRemoteAMQP() throws Exception {
      testDivertAddresDemandOnLocalBrokerFederatesMessagesFromRemote("AMQP");
   }

   @Test(timeout = 20000)
   public void testDivertAddresDemandOnLocalBrokerFederatesMessagesFromRemoteCORE() throws Exception {
      testDivertAddresDemandOnLocalBrokerFederatesMessagesFromRemote("CORE");
   }

   private void testDivertAddresDemandOnLocalBrokerFederatesMessagesFromRemote(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPFederationAddressPolicyElement localAddressPolicy = new AMQPFederationAddressPolicyElement();
      localAddressPolicy.setName("test-policy");
      localAddressPolicy.addToIncludes("source");
      localAddressPolicy.setAutoDelete(false);
      localAddressPolicy.setAutoDeleteDelay(-1L);
      localAddressPolicy.setAutoDeleteMessageCount(-1L);
      localAddressPolicy.setEnableDivertBindings(true);

      final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
      element.setName("test");
      element.addLocalAddressPolicy(localAddressPolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      final DivertConfiguration divert = new DivertConfiguration();
      divert.setName("test-divert");
      divert.setAddress("source");
      divert.setForwardingAddress("target");
      divert.setRoutingType(ComponentConfigurationRoutingType.MULTICAST);

      server.getConfiguration().addAMQPConnection(amqpConnection);
      remoteServer.start();
      server.start();
      server.deployDivert(divert);
      // Currently the address must exist on the local before we will federate from the remote
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("source"), RoutingType.MULTICAST));

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final Topic target = sessionL.createTopic("target");
         final Topic source = sessionL.createTopic("source");

         final MessageConsumer consumerL = sessionL.createConsumer(target);

         connectionL.start();
         connectionR.start();

         // Demand on local address should trigger receiver on remote.
         Wait.assertTrue(() -> remoteServer.addressQuery(SimpleString.toSimpleString("source")).isExists());

         final MessageProducer producerR = sessionR.createProducer(source);
         final TextMessage message = sessionR.createTextMessage("Hello World");

         producerR.send(message);

         final Message received = consumerL.receive(5_000);
         assertNotNull(received);
      }
   }

   @Test(timeout = 20000)
   public void testQueueDemandOnLocalBrokerFederatesMessagesFromRemoteAMQP() throws Exception {
      testQueueDemandOnLocalBrokerFederatesMessagesFromRemote("AMQP");
   }

   @Test(timeout = 20000)
   public void testQueueDemandOnLocalBrokerFederatesMessagesFromRemoteCORE() throws Exception {
      testQueueDemandOnLocalBrokerFederatesMessagesFromRemote("CORE");
   }

   private void testQueueDemandOnLocalBrokerFederatesMessagesFromRemote(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPFederationQueuePolicyElement localQueuePolicy = new AMQPFederationQueuePolicyElement();
      localQueuePolicy.setName("test-policy");
      localQueuePolicy.addToIncludes("#", "test");

      final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
      element.setName("test");
      element.addLocalQueuePolicy(localQueuePolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration("test-queue-federation", "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      server.getConfiguration().addAMQPConnection(amqpConnection);
      remoteServer.start();
      remoteServer.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                             .setAddress("test")
                                                             .setAutoCreated(false));
      server.start();

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final Queue queue = sessionL.createQueue("test");

         final MessageConsumer consumerL = sessionL.createConsumer(queue);

         connectionL.start();
         connectionR.start();

         // Demand on local queue should trigger receiver on remote.
         Wait.assertTrue(() -> server.queueQuery(SimpleString.toSimpleString("test")).isExists());

         final MessageProducer producerR = sessionR.createProducer(queue);
         final TextMessage message = sessionR.createTextMessage("Hello World");

         producerR.send(message);

         final Message received = consumerL.receive(5_000);
         assertNotNull(received);
      }
   }

   @Test(timeout = 20000)
   public void testAddresDemandOnRemoteBrokerFederatesMessagesFromLocalAMQP() throws Exception {
      testAddresDemandOnRemoteBrokerFederatesMessagesFromLocal("AMQP");
   }

   @Test(timeout = 20000)
   public void testAddresDemandOnRemoteBrokerFederatesMessagesFromLocalCORE() throws Exception {
      testAddresDemandOnRemoteBrokerFederatesMessagesFromLocal("CORE");
   }

   private void testAddresDemandOnRemoteBrokerFederatesMessagesFromLocal(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPFederationAddressPolicyElement remoteAddressPolicy = new AMQPFederationAddressPolicyElement();
      remoteAddressPolicy.setName("test-policy");
      remoteAddressPolicy.addToIncludes("test");
      remoteAddressPolicy.setAutoDelete(false);
      remoteAddressPolicy.setAutoDeleteDelay(-1L);
      remoteAddressPolicy.setAutoDeleteMessageCount(-1L);

      final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
      element.setName("test");
      element.addRemoteAddressPolicy(remoteAddressPolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://localhost:" + SERVER_PORT_REMOTE);
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

         final Topic topic = sessionL.createTopic("test");

         final MessageConsumer consumerR = sessionR.createConsumer(topic);

         connectionL.start();
         connectionR.start();

         // Demand on local address should trigger receiver on remote.
         Wait.assertTrue(() -> server.addressQuery(SimpleString.toSimpleString("test")).isExists());
         Wait.assertTrue(() -> remoteServer.addressQuery(SimpleString.toSimpleString("test")).isExists());

         final MessageProducer producerL = sessionL.createProducer(topic);
         final TextMessage message = sessionL.createTextMessage("Hello World");

         producerL.send(message);

         final Message received = consumerR.receive(5_000);
         assertNotNull(received);
      }
   }

   @Test(timeout = 20000)
   public void testQueueDemandOnRemoteWithRemoteConfigrationLeadsToMessageBeingFederatedAMQP() throws Exception {
      testQueueDemandOnRemoteWithRemoteConfigrationLeadsToMessageBeingFederated("AMQP");
   }

   @Test(timeout = 20000)
   public void testQueueDemandOnRemoteWithRemoteConfigrationLeadsToMessageBeingFederatedCORE() throws Exception {
      testQueueDemandOnRemoteWithRemoteConfigrationLeadsToMessageBeingFederated("CORE");
   }

   public void testQueueDemandOnRemoteWithRemoteConfigrationLeadsToMessageBeingFederated(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPFederationQueuePolicyElement remoteQueuePolicy = new AMQPFederationQueuePolicyElement();
      remoteQueuePolicy.setName("test-policy");
      remoteQueuePolicy.addToIncludes("#", "test");

      final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
      element.setName("test");
      element.addRemoteQueuePolicy(remoteQueuePolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration("test-queue-federation", "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      server.getConfiguration().addAMQPConnection(amqpConnection);
      remoteServer.start();
      server.start();
      server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                       .setAddress("test")
                                                       .setAutoCreated(false));

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final Queue queue = sessionL.createQueue("test");

         final MessageConsumer consumerR = sessionR.createConsumer(queue);

         connectionL.start();
         connectionR.start();

         // Demand on remote queue should trigger receiver on remote.
         Wait.assertTrue(() -> remoteServer.queueQuery(SimpleString.toSimpleString("test")).isExists());

         final MessageProducer producerL = sessionL.createProducer(queue);
         final TextMessage message = sessionL.createTextMessage("Hello World");

         producerL.send(message);

         final Message received = consumerR.receive(5_000);
         assertNotNull(received);
      }
   }

   @Test(timeout = 20000)
   public void testDivertAddresDemandOnRemoteBrokerFederatesMessagesFromLocalAMQP() throws Exception {
      testDivertAddresDemandOnRemoteBrokerFederatesMessagesFromLocal("AMQP");
   }

   @Test(timeout = 20000)
   public void testDivertAddresDemandOnRemoteBrokerFederatesMessagesFromLocalCORE() throws Exception {
      testDivertAddresDemandOnRemoteBrokerFederatesMessagesFromLocal("CORE");
   }

   private void testDivertAddresDemandOnRemoteBrokerFederatesMessagesFromLocal(String clientProtocol) throws Exception {
      logger.info("Test started: {}", getTestName());

      final AMQPFederationAddressPolicyElement remoteAddressPolicy = new AMQPFederationAddressPolicyElement();
      remoteAddressPolicy.setName("test-policy");
      remoteAddressPolicy.addToIncludes("source");
      remoteAddressPolicy.setAutoDelete(false);
      remoteAddressPolicy.setAutoDeleteDelay(-1L);
      remoteAddressPolicy.setAutoDeleteMessageCount(-1L);
      remoteAddressPolicy.setEnableDivertBindings(true);

      final AMQPFederatedBrokerConnectionElement element = new AMQPFederatedBrokerConnectionElement();
      element.setName("test");
      element.addRemoteAddressPolicy(remoteAddressPolicy);

      final AMQPBrokerConnectConfiguration amqpConnection =
         new AMQPBrokerConnectConfiguration("test-address-federation", "tcp://localhost:" + SERVER_PORT_REMOTE);
      amqpConnection.setReconnectAttempts(10);// Limit reconnects
      amqpConnection.addElement(element);

      final DivertConfiguration divert = new DivertConfiguration();
      divert.setName("test-divert");
      divert.setAddress("source");
      divert.setForwardingAddress("target");
      divert.setRoutingType(ComponentConfigurationRoutingType.MULTICAST);

      remoteServer.start();
      remoteServer.deployDivert(divert);
      // Currently the address must exist on the local before we will federate from the remote
      // and in this case since we are instructing the remote to federate from us the address must
      // exist on the remote for that to happen.
      remoteServer.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("source"), RoutingType.MULTICAST));
      server.getConfiguration().addAMQPConnection(amqpConnection);
      server.start();

      final ConnectionFactory factoryLocal = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT);
      final ConnectionFactory factoryRemote = CFUtil.createConnectionFactory(clientProtocol, "tcp://localhost:" + SERVER_PORT_REMOTE);

      try (Connection connectionL = factoryLocal.createConnection();
           Connection connectionR = factoryRemote.createConnection()) {

         final Session sessionL = connectionL.createSession(Session.AUTO_ACKNOWLEDGE);
         final Session sessionR = connectionR.createSession(Session.AUTO_ACKNOWLEDGE);

         final Topic target = sessionL.createTopic("target");
         final Topic source = sessionL.createTopic("source");

         final MessageConsumer consumerR = sessionR.createConsumer(target);

         connectionL.start();
         connectionR.start();

         // Demand on local address should trigger receiver on remote.
         Wait.assertTrue(() -> server.addressQuery(SimpleString.toSimpleString("source")).isExists());

         final MessageProducer producerL = sessionL.createProducer(source);
         final TextMessage message = sessionL.createTextMessage("Hello World");

         producerL.send(message);

         final Message received = consumerR.receive(5_000);
         assertNotNull(received);
      }
   }
}
