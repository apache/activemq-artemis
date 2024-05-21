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
import java.net.URI;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPSenderBrokerConnectionElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the Sender functionality on AMQP broker connections
 */
public class AMQPBrokerConnectionSenderTest extends AmqpClientTestSupport {

   private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
   public void testBrokerConnectionCreatesSenderOnRemoteUsingBaseConnectionType() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender().respondInKind();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         LOG.info("Test started, peer listening on: {}", remoteURI);

         // This can still be done in code or via broker properties
         final AMQPBrokerConnectionElement element = new AMQPBrokerConnectionElement();
         element.setType(AMQPBrokerConnectionAddressType.SENDER);
         element.setName(getTestName());
         element.setMatchAddress("test");

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);
         amqpConnection.addElement(element);
         amqpConnection.setAutostart(true);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         peer.waitForScriptToComplete();
         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete();
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testBrokerConnectionCreatesSenderOnRemoteWhenQueueIsConfigured() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender().withTarget().withAddress("test").and().respondInKind();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         LOG.info("Test started, peer listening on: {}", remoteURI);

         final AMQPSenderBrokerConnectionElement element = new AMQPSenderBrokerConnectionElement();
         element.setName(getTestName());
         element.setQueueName("test");

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);
         amqpConnection.addElement(element);
         amqpConnection.setAutostart(true);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(new QueueConfiguration("test").setRoutingType(RoutingType.MULTICAST)
                                                          .setAddress("test")
                                                          .setAutoCreated(false));

         peer.waitForScriptToComplete();
         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete();
         peer.close();
      }
   }

   @Test(timeout = 20000)
   public void testCreatesSenderOnRemoteAfterClientConnects() throws Exception {
      doTestCreatesSenderOnRemote("test", null, "test", null);
   }

   @Test(timeout = 20000)
   public void testCreatesSenderWithConfiguredTargetAddressOnRemote() throws Exception {
      doTestCreatesSenderOnRemote("test", "topic://test", "topic://test", null);
   }

   @Test(timeout = 20000)
   public void testCreatesSenderWithConfiguredTargetElementsOnRemote() throws Exception {
      doTestCreatesSenderOnRemote("test", "topic://test", "topic://test", "topic");
   }

   @Test(timeout = 20000)
   public void testCreatesSenderMultipleTargetCapabilitiesOnRemote() throws Exception {
      doTestCreatesSenderOnRemote("test", "topic://test", "topic://test", "topic,temp,test");
   }

   private void doTestCreatesSenderOnRemote(String address, String targetAddress, String expectedTargetAddress, String targetCapabilities) throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         LOG.info("Test started, peer listening on: {}", remoteURI);

         final AMQPSenderBrokerConnectionElement element = new AMQPSenderBrokerConnectionElement();
         element.setName(getTestName());
         element.setMatchAddress(address);
         if (targetAddress != null) {
            element.setTargetAddress("topic://test");
         }
         if (targetCapabilities != null) {
            element.setTargetCapabilities(targetCapabilities);
         }

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);
         amqpConnection.addElement(element);
         amqpConnection.setAutostart(true);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         final String[] capabilities = targetCapabilities == null ? null : targetCapabilities.split(",");

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withTarget()
                                       .withAddress(expectedTargetAddress)
                                       .withCapabilities(capabilities)
                                       .and()
                                       .respondInKind();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Topic topic = session.createTopic("test");
            final MessageConsumer consumer = session.createConsumer(topic);

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            consumer.close();
         }

         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete();

         peer.close();
      }
   }
}
