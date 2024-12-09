/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp.connect;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPTunneledMessageConstants.AMQP_TUNNELED_CORE_MESSAGE_FORMAT;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.CONNECTION_FORCED;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.TUNNEL_CORE_MESSAGES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AMQPNoForwardMirroringTest extends AmqpClientTestSupport {

   protected static final int AMQP_PORT_2 = 5673;
   protected static final int AMQP_PORT_3 = 5674;
   protected static final int AMQP_PORT_4 = 5675;

   ActiveMQServer server_2;
   ActiveMQServer server_3;
   ActiveMQServer server_4;

   @Override
   protected ActiveMQServer createServer() throws Exception {
      return createServer(AMQP_PORT, false);
   }

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,CORE,OPENWIRE";
   }

   @Test
   public void testNoForward() throws Exception {
      server_2 = createServer(AMQP_PORT_2, false);
      server_3 = createServer(AMQP_PORT_3, false);

      // name the servers, for convenience during debugging
      server.getConfiguration().setName("1");
      server_2.getConfiguration().setName("2");
      server_3.getConfiguration().setName("3");

      /**
       * Topology of the test:
       *
       *    1 -(noForward)-> 2 -> 3
       *    ^                |
       *    |________________|
       */

      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "1to2", "tcp://localhost:" + AMQP_PORT_2).setRetryInterval(100).setReconnectAttempts(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setAddressFilter(getQueueName()).setNoForward(true));
         server.getConfiguration().addAMQPConnection(amqpConnection);
      }

      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "2to1", "tcp://localhost:" + AMQP_PORT).setRetryInterval(100).setReconnectAttempts(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setAddressFilter(getQueueName()));
         server_2.getConfiguration().addAMQPConnection(amqpConnection);

         amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "2to3", "tcp://localhost:" + AMQP_PORT_3).setRetryInterval(100).setReconnectAttempts(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setAddressFilter(getQueueName()));
         server_2.getConfiguration().addAMQPConnection(amqpConnection);
      }

      server.start();
      server_2.start();
      server_3.start();

      createAddressAndQueues(server);
      Wait.assertTrue(() -> server.locateQueue(getQueueName()) != null);
      Wait.assertTrue(() -> server_2.locateQueue(getQueueName()) != null);
      // queue creation doesn't reach 3 b/c of the noForward link between 1 and 2.
      Wait.assertTrue(() -> server_3.locateQueue(getQueueName()) == null);

      Queue q1 = server.locateQueue(getQueueName());
      assertNotNull(q1);

      Queue q2 = server_2.locateQueue(getQueueName());
      assertNotNull(q2);

      ConnectionFactory factory = CFUtil.createConnectionFactory(randomProtocol(), "tcp://localhost:" + AMQP_PORT);
      ConnectionFactory factory2 = CFUtil.createConnectionFactory(randomProtocol(), "tcp://localhost:" + AMQP_PORT_2);
      ConnectionFactory factory3 = CFUtil.createConnectionFactory(randomProtocol(), "tcp://localhost:" + AMQP_PORT_3);

      // send from 1, 2 receives, 3 don't.
      try (Connection conn = factory.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }
      Wait.assertEquals(10L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q2::getMessageCount, 1000, 100);

      // consume from 2, 1 and 2 counters go back to 0
      try (Connection conn = factory2.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
      }

      Wait.assertEquals(0L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q2::getMessageCount, 1000, 100);

      Thread.sleep(100); // some time to allow eventual loops

      // queue creation was originated on server, with noForward in place,
      // the messages never reached server_3, for the rest of the test suite,
      // we need server_3 to have access to the queue
      createAddressAndQueues(server_3);
      Wait.assertTrue(() -> server_3.locateQueue(getQueueName()) != null);
      Queue q3 = server_3.locateQueue(getQueueName());
      assertNotNull(q3);

      // produce on 2. 1, 2 and 3 receive messages.
      try (Connection conn = factory2.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }

      Wait.assertEquals(10L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q3::getMessageCount, 1000, 100);

      // consume on 1. 1, 2, and 3 counters are back to 0
      try (Connection conn = factory.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }
      Wait.assertEquals(0L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q3::getMessageCount, 1000, 100);

      // produce on 2. 1, 2 and 3 receive messages.
      try (Connection conn = factory2.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }

      Wait.assertEquals(10L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q3::getMessageCount, 1000, 100);

      // consume on 3. 1, 2 counters are still at 10 and 3 is at 0.
      try (Connection conn = factory3.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }
      Wait.assertEquals(10L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q3::getMessageCount, 1000, 100);

      // consume on 2. 1, 2 and 3 counters are back to 0
      try (Connection conn = factory2.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }
      Wait.assertEquals(0L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q3::getMessageCount, 1000, 100);

      // produce on 3. only 3 has messages.
      try (Connection conn = factory3.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }

      Wait.assertEquals(0L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q3::getMessageCount, 1000, 100);

      // consume on 3. 1, 2, and 3 counters are back to 0
      try (Connection conn = factory3.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }
      Wait.assertEquals(0L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q3::getMessageCount, 1000, 100);

      try (Connection conn = factory.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         assertNull(consumer.receiveNoWait());
         consumer.close();
      }

      try (Connection conn = factory2.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         assertNull(consumer.receiveNoWait());
         consumer.close();
      }

      try (Connection conn = factory3.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         assertNull(consumer.receiveNoWait());
         consumer.close();
      }

   }

   @Test
   public void testSquare() throws Exception {
      server_2 = createServer(AMQP_PORT_2, false);
      server_3 = createServer(AMQP_PORT_3, false);
      server_4 = createServer(AMQP_PORT_4, false);

      // name the servers, for convenience during debugging
      server.getConfiguration().setName("1");
      server_2.getConfiguration().setName("2");
      server_3.getConfiguration().setName("3");
      server_4.getConfiguration().setName("4");

      /**
       *
       * Setup the mirroring topology to be a square:
       *
       * 1 <- - -> 2
       * ^         ^       The link between 1 and 2 and the
       * |         |       link between 3 and 4 are noForward
       * v         v       links in both directions.
       * 4 <- - -> 3
       */

      {
         AMQPBrokerConnectConfiguration amqpConnection1to2 = new AMQPBrokerConnectConfiguration(getTestMethodName() + "1to2", "tcp://localhost:" + AMQP_PORT_2).setRetryInterval(100).setReconnectAttempts(100);
         amqpConnection1to2.addElement(new AMQPMirrorBrokerConnectionElement().setNoForward(true));
         server.getConfiguration().addAMQPConnection(amqpConnection1to2);

         AMQPBrokerConnectConfiguration amqpConnection1to4 = new AMQPBrokerConnectConfiguration(getTestMethodName() + "1to4", "tcp://localhost:" + AMQP_PORT_4).setRetryInterval(100).setReconnectAttempts(100);
         amqpConnection1to4.addElement(new AMQPMirrorBrokerConnectionElement());
         server.getConfiguration().addAMQPConnection(amqpConnection1to4);
      }

      {
         AMQPBrokerConnectConfiguration amqpConnection2to1 = new AMQPBrokerConnectConfiguration(getTestMethodName() + "2to1", "tcp://localhost:" + AMQP_PORT).setRetryInterval(100).setReconnectAttempts(100);
         amqpConnection2to1.addElement(new AMQPMirrorBrokerConnectionElement().setNoForward(true));
         server_2.getConfiguration().addAMQPConnection(amqpConnection2to1);

         AMQPBrokerConnectConfiguration amqpConnection2to3 = new AMQPBrokerConnectConfiguration(getTestMethodName() + "2to3", "tcp://localhost:" + AMQP_PORT_3).setRetryInterval(100).setReconnectAttempts(100);
         amqpConnection2to3.addElement(new AMQPMirrorBrokerConnectionElement());
         server_2.getConfiguration().addAMQPConnection(amqpConnection2to3);
      }

      {
         AMQPBrokerConnectConfiguration amqpConnection3to2 = new AMQPBrokerConnectConfiguration(getTestMethodName() + "3to2", "tcp://localhost:" + AMQP_PORT_2).setRetryInterval(100).setReconnectAttempts(100);
         amqpConnection3to2.addElement(new AMQPMirrorBrokerConnectionElement());
         server_3.getConfiguration().addAMQPConnection(amqpConnection3to2);

         AMQPBrokerConnectConfiguration amqpConnection3to4 = new AMQPBrokerConnectConfiguration(getTestMethodName() + "3to4", "tcp://localhost:" + AMQP_PORT_4).setRetryInterval(100).setReconnectAttempts(100);
         amqpConnection3to4.addElement(new AMQPMirrorBrokerConnectionElement().setNoForward(true));
         server_3.getConfiguration().addAMQPConnection(amqpConnection3to4);
      }

      {
         AMQPBrokerConnectConfiguration amqpConnection4to1 = new AMQPBrokerConnectConfiguration(getTestMethodName() + "4to1", "tcp://localhost:" + AMQP_PORT).setRetryInterval(100).setReconnectAttempts(100);
         amqpConnection4to1.addElement(new AMQPMirrorBrokerConnectionElement());
         server_4.getConfiguration().addAMQPConnection(amqpConnection4to1);

         AMQPBrokerConnectConfiguration amqpConnection4to3 = new AMQPBrokerConnectConfiguration(getTestMethodName() + "4to3", "tcp://localhost:" + AMQP_PORT_3).setRetryInterval(100).setReconnectAttempts(100);
         amqpConnection4to3.addElement(new AMQPMirrorBrokerConnectionElement().setNoForward(true));
         server_4.getConfiguration().addAMQPConnection(amqpConnection4to3);
      }

      server.start();
      server_2.start();
      server_3.start();
      server_4.start();

      createAddressAndQueues(server);
      Wait.assertTrue(() -> server.locateQueue(getQueueName()) != null);
      Wait.assertTrue(() -> server_2.locateQueue(getQueueName()) != null);
      Wait.assertTrue(() -> server_3.locateQueue(getQueueName()) != null);
      Wait.assertTrue(() -> server_4.locateQueue(getQueueName()) != null);

      Queue q1 = server.locateQueue(getQueueName());
      assertNotNull(q1);

      Queue q2 = server_2.locateQueue(getQueueName());
      assertNotNull(q2);

      Queue q3 = server_3.locateQueue(getQueueName());
      assertNotNull(q3);

      Queue q4 = server_4.locateQueue(getQueueName());
      assertNotNull(q4);

      ConnectionFactory factory = CFUtil.createConnectionFactory(randomProtocol(), "tcp://localhost:" + AMQP_PORT);
      ConnectionFactory factory2 = CFUtil.createConnectionFactory(randomProtocol(), "tcp://localhost:" + AMQP_PORT_2);
      ConnectionFactory factory3 = CFUtil.createConnectionFactory(randomProtocol(), "tcp://localhost:" + AMQP_PORT_3);
      ConnectionFactory factory4 = CFUtil.createConnectionFactory(randomProtocol(), "tcp://localhost:" + AMQP_PORT_4);

      try (Connection conn = factory4.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }
      try (Connection conn = factory3.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 10; i < 20; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }
      try (Connection conn = factory2.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 20; i < 30; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }
      try (Connection conn = factory.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 30; i < 40; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }

      Thread.sleep(100); // some time to allow eventual loops

      Wait.assertEquals(40L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(40L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(40L, q3::getMessageCount, 1000, 100);
      Wait.assertEquals(40L, q4::getMessageCount, 1000, 100);

      try (Connection conn = factory.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }

      Wait.assertEquals(30L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(30L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(30L, q3::getMessageCount, 1000, 100);
      Wait.assertEquals(30L, q4::getMessageCount, 1000, 100);

      try (Connection conn = factory2.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 10; i < 20; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }

      Wait.assertEquals(20L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(20L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(20L, q3::getMessageCount, 1000, 100);
      Wait.assertEquals(20L, q4::getMessageCount, 1000, 100);

      try (Connection conn = factory3.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 20; i < 30; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }

      Wait.assertEquals(10L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q3::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q4::getMessageCount, 1000, 100);

      try (Connection conn = factory4.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 30; i < 40; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }

      Wait.assertEquals(0L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q3::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q4::getMessageCount, 1000, 100);

      try (Connection conn = factory.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         assertNull(consumer.receiveNoWait());
         consumer.close();
      }

      try (Connection conn = factory2.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         assertNull(consumer.receiveNoWait());
         consumer.close();
      }

      try (Connection conn = factory3.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         assertNull(consumer.receiveNoWait());
         consumer.close();
      }

      try (Connection conn = factory4.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         assertNull(consumer.receiveNoWait());
         consumer.close();
      }

   }

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   @Timeout(20)
   public void testBrokerHandlesSenderLinkOmitsNoForwardCapability() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect("PLAIN", "ANONYMOUS");
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofSender()
            .withName(Matchers.startsWith("$ACTIVEMQ_ARTEMIS_MIRROR"))
            .withDesiredCapabilities(String.valueOf(AMQPMirrorControllerSource.MIRROR_CAPABILITY), AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString(), AMQPMirrorControllerSource.NO_FORWARD.toString())
            .respond()
            .withOfferedCapabilities(String.valueOf(AMQPMirrorControllerSource.MIRROR_CAPABILITY), AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString());
         peer.expectClose().withError(CONNECTION_FORCED.toString()).optional(); // Can hit the wire in rare instances.
         peer.expectConnectionToDrop();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         try (AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler()) {
            // No user or pass given, it will have to select ANONYMOUS even though PLAIN also offered
            AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
            amqpConnection.setReconnectAttempts(0);// No reconnects
            amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setNoForward(true));
            server.getConfiguration().addAMQPConnection(amqpConnection);
            server.start();

            org.apache.activemq.artemis.tests.util.Wait.assertTrue(() -> loggerHandler.findText("AMQ111001"));
            assertEquals(1, loggerHandler.countText("AMQ119018"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            server.stop();
         }
      }
   }

   private int minLargeMessageSize = 100 * 1024;
   @Override
   protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
      params.put("amqpMinLargeMessageSize", minLargeMessageSize);
   }

   private String buildCoreLargeMessage() {
      String message = "A message!";
      StringBuilder builder = new StringBuilder();
      builder.append(message);
      for (int i = 0; i < ((minLargeMessageSize * 2) / message.length()) + 1; i++) {
         builder.append(message);
      }
      return builder.toString();
   }

   @Test
   @Timeout(20)
   public void testNoForwardBlocksMessagesAndControlsPropagation() throws Exception {
      doTestNoForwardBlocksMessagesAndControlsPropagation(false, false);
   }

   @Test
   @Timeout(20)
   public void testNoForwardBlocksMessagesAndControlsPropagationWithTunneling() throws Exception {
      doTestNoForwardBlocksMessagesAndControlsPropagation(true, false);
   }

   @Test
   @Timeout(20)
   public void testNoForwardBlocksCoreLargeMessagesAndControlsPropagation() throws Exception {
      doTestNoForwardBlocksMessagesAndControlsPropagation(false, true);
   }

   @Test
   @Timeout(20)
   public void testNoForwardBlocksCoreLargeMessagesAndControlsPropagationWithTunneling() throws Exception {
      doTestNoForwardBlocksMessagesAndControlsPropagation(true, true);
   }

   private void doTestNoForwardBlocksMessagesAndControlsPropagation(boolean tunneling, boolean largeMessage) throws Exception {
      final Map<String, Object> brokerProperties = new HashMap<>();
      brokerProperties.put(AMQPMirrorControllerSource.BROKER_ID.toString(), "Test-Broker");

      final String[] capabilities;
      ArrayList<String> capabilitiesList = new ArrayList<>();
      int messageFormat = 0;

      capabilitiesList.add(String.valueOf(AMQPMirrorControllerSource.MIRROR_CAPABILITY));
      if (tunneling) {
         capabilitiesList.add(AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString());
         messageFormat = AMQP_TUNNELED_CORE_MESSAGE_FORMAT;
      }
      capabilities = capabilitiesList.toArray(new String[]{});

      // Topology of the test: server -(noForward)-> server_2 -> peer_3
      //                          ^                      |
      //                          |______________________|

      // the objective of the test is to make sure that the proton peer doesn't receive any message coming from "server"
      // since the link be "server" and "server_2" is set to have the noForward property.
      // the only way there is to assess that peer_3 didn't receive any message is to have server_2 sending it a message at the end
      // if the message gets successfully decoded, in turn it means that nothing got received in the meantime.
      try (ProtonTestServer peer_3 = new ProtonTestServer()) {
         peer_3.expectSASLPlainConnect("user", "pass", "PLAIN", "ANONYMOUS");
         peer_3.expectOpen().respond();
         peer_3.expectBegin().respond();
         peer_3.expectAttach().ofSender()
            .withName(Matchers.startsWith("$ACTIVEMQ_ARTEMIS_MIRROR"))
            .withDesiredCapabilities(capabilities)
            .respond()
            .withOfferedCapabilities(capabilities)
            .withPropertiesMap(brokerProperties);
         peer_3.remoteFlow().withLinkCredit(10).queue();
         peer_3.start();

         final URI remoteURI = peer_3.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final String secondaryQueueName = "secondaryQueue";
         final int AMQP_PORT_2 = AMQP_PORT + 1;
         final ActiveMQServer server_2 = createServer(AMQP_PORT_2, false);
         {
            AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "toPeer3", "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
            amqpConnection.setReconnectAttempts(0);// No reconnects
            amqpConnection.setUser("user");
            amqpConnection.setPassword("pass");
            amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().addProperty(TUNNEL_CORE_MESSAGES, Boolean.toString(tunneling)).setQueueCreation(true).setAddressFilter(getQueueName() + "," + secondaryQueueName));
            server_2.getConfiguration().addAMQPConnection(amqpConnection);

            amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "toServer", "tcp://localhost:" + AMQP_PORT);
            amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setAddressFilter(getQueueName()));
            server_2.getConfiguration().addAMQPConnection(amqpConnection);
         }

         {
            AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "toServer2", "tcp://localhost:" + AMQP_PORT_2);
            amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setAddressFilter(getQueueName()).setNoForward(true));
            server.getConfiguration().addAMQPConnection(amqpConnection);
         }

         // connect the topology
         server_2.start();
         server.start();
         peer_3.waitForScriptToComplete(5, TimeUnit.SECONDS);

         // Create queues & send messages on server, nothing will reach peer_3
         createAddressAndQueues(server);
         org.apache.activemq.artemis.tests.util.Wait.assertTrue(() -> server_2.locateQueue(getQueueName()) != null);
         org.apache.activemq.artemis.tests.util.Wait.assertTrue(() -> server.locateQueue(getQueueName()) != null);

         final org.apache.activemq.artemis.core.server.Queue q1 = server.locateQueue(getQueueName());
         assertNotNull(q1);

         final org.apache.activemq.artemis.core.server.Queue q2 = server_2.locateQueue(getQueueName());
         assertNotNull(q2);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:" + AMQP_PORT + "?minLargeMessageSize=" + minLargeMessageSize);
         final ConnectionFactory factory_2 = CFUtil.createConnectionFactory("CORE", "tcp://localhost:" + AMQP_PORT_2 + "?minLargeMessageSize=" + minLargeMessageSize);

         String message = largeMessage ? buildCoreLargeMessage() : "A message!";

         try (Connection conn = factory.createConnection()) {
            final Session session = conn.createSession();
            conn.start();

            final MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
            producer.send(session.createTextMessage(message));
            producer.close();
         }

         org.apache.activemq.artemis.tests.util.Wait.assertEquals(1L, q1::getMessageCount, 100, 100);
         org.apache.activemq.artemis.tests.util.Wait.assertEquals(1L, q2::getMessageCount, 100, 100);

         try (Connection conn = factory_2.createConnection()) {
            final Session session = conn.createSession();
            conn.start();

            final MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
            TextMessage rcvMsg = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals(message, rcvMsg.getText());
            consumer.close();
         }

         org.apache.activemq.artemis.tests.util.Wait.assertEquals(0L, q2::getMessageCount, 100, 100);
         org.apache.activemq.artemis.tests.util.Wait.assertEquals(0L, q1::getMessageCount, 6000, 100);

         // give some time to peer_3 to receive messages (if any)
         Thread.sleep(100);
         peer_3.waitForScriptToComplete(5, TimeUnit.SECONDS); // if messages are received here, this should error out

         // Then send messages on the broker directly connected to the peer, the messages should make it to the peer.
         // Receiving these 3 messages in that order confirms that no previous data reched the Peer, therefore validating
         // the test.
         final String peer3Message = "test";
         peer_3.expectTransfer().accept(); // Address create
         peer_3.expectTransfer().accept(); // Queue create
         if (tunneling) {
            peer_3.expectTransfer().withMessageFormat(messageFormat).accept();
         } else {
            peer_3.expectTransfer().withMessageFormat(messageFormat).withMessage().withHeader().also().withDeliveryAnnotations().also().withMessageAnnotations().also().withProperties().also().withApplicationProperties().also().withValue(peer3Message).and().accept();
         }

         server_2.addAddressInfo(new AddressInfo(SimpleString.of(secondaryQueueName), RoutingType.ANYCAST));
         server_2.createQueue(QueueConfiguration.of(secondaryQueueName).setRoutingType(RoutingType.ANYCAST));

         try (Connection connection = factory_2.createConnection()) {
            final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue(secondaryQueueName));
            final TextMessage msg = session.createTextMessage(peer3Message);

            connection.start();
            producer.send(msg);
         }

         peer_3.waitForScriptToComplete(5, TimeUnit.SECONDS);

         server.stop();
         server_2.stop();
      }
   }

}
