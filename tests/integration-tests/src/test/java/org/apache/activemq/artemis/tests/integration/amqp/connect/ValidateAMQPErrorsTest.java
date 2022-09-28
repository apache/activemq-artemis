/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonServerOptions;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.connect.AMQPBrokerConnection;
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.Target;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.impl.ConnectionImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.EnumSet.of;
import static org.apache.qpid.proton.engine.EndpointState.ACTIVE;

/**
 * This test will make sure the Broker connection will react accordingly to a few misconfigs and possible errors on the network of brokers and eventually qipd-dispatch.
 */
public class ValidateAMQPErrorsTest extends AmqpClientTestSupport {

   protected static final int AMQP_PORT_2 = 5673;
   private static final Logger logger = LoggerFactory.getLogger(ValidateAMQPErrorsTest.class);
   protected Vertx vertx;

   protected MockServer mockServer;

   public void startVerx() {
      vertx = Vertx.vertx();
   }

   @After
   public void stop() throws Exception {
      try {
         if (mockServer != null) {
            mockServer.close();
            mockServer = null;
         }
         if (vertx != null) {
            try {
               CountDownLatch latch = new CountDownLatch(1);
               vertx.close((x) -> latch.countDown());
               Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
            } finally {
               vertx = null;
            }
         }
      } finally {
         AssertionLoggerHandler.stopCapture(); // Just in case startCapture was called in any of the tests here
      }
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      return createServer(AMQP_PORT, false);
   }

   /**
    * Connecting to itself should issue an error.
    * and the max retry should still be counted, not just keep connecting forever.
    */
   @Test
   public void testConnectItself() throws Exception {
      try {
         AssertionLoggerHandler.startCapture();

         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT).setReconnectAttempts(10).setRetryInterval(1);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
         server.getConfiguration().addAMQPConnection(amqpConnection);

         server.start();

         Assert.assertEquals(1, server.getBrokerConnections().size());
         server.getBrokerConnections().forEach((t) -> Wait.assertFalse(t::isStarted));
         Wait.assertTrue(() -> AssertionLoggerHandler.findText("AMQ111001")); // max retry
         AssertionLoggerHandler.clear();
         Thread.sleep(100);
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ111002")); // there shouldn't be a retry after the last failure
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ111003")); // there shouldn't be a retry after the last failure
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   @Test
   public void testCloseLinkOnMirror() throws Exception {
      try {
         AssertionLoggerHandler.startCapture();

         ActiveMQServer server2 = createServer(AMQP_PORT_2, false);

         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT_2).setReconnectAttempts(-1).setRetryInterval(10);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
         server.getConfiguration().addAMQPConnection(amqpConnection);

         server.start();
         Assert.assertEquals(1, server.getBrokerConnections().size());
         Wait.assertTrue(() -> AssertionLoggerHandler.findText("AMQ111002"));
         server.getBrokerConnections().forEach((t) -> Wait.assertTrue(() -> ((AMQPBrokerConnection) t).isConnecting()));

         server2.start();

         server.getBrokerConnections().forEach((t) -> Wait.assertFalse(() -> ((AMQPBrokerConnection) t).isConnecting()));

         createAddressAndQueues(server);

         Wait.assertTrue(() -> server2.locateQueue(getQueueName()) != null);

         Wait.assertEquals(1, server2.getRemotingService()::getConnectionCount);
         server2.getRemotingService().getConnections().forEach((t) -> {
            try {
               ActiveMQProtonRemotingConnection connection = (ActiveMQProtonRemotingConnection) t;
               ConnectionImpl protonConnection = (ConnectionImpl) connection.getAmqpConnection().getHandler().getConnection();
               Wait.waitFor(() -> protonConnection.linkHead(of(ACTIVE), of(ACTIVE)) != null);
               connection.getAmqpConnection().runNow(() -> {
                  Receiver receiver = (Receiver) protonConnection.linkHead(of(ACTIVE), of(ACTIVE));
                  receiver.close();
                  connection.flush();
               });
            } catch (Exception e) {
               e.printStackTrace();
            }
         });

         ConnectionFactory cf1 = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);
         ConnectionFactory cf2 = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT_2);

         try (Connection connection = cf1.createConnection()) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
            for (int i = 0; i < 10; i++) {
               producer.send(session.createTextMessage("message " + i));
            }
         }

         // messages should still flow after a disconnect on the link
         // the server should reconnect as if it was a failure
         try (Connection connection = cf2.createConnection()) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
            connection.start();
            for (int i = 0; i < 10; i++) {
               Assert.assertEquals("message " + i, ((TextMessage) consumer.receive(5000)).getText());
            }
         }

      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   @Test
   public void testCloseLinkOnSender() throws Exception {
      testCloseLink(true);
   }

   @Test
   public void testCloseLinkOnReceiver() throws Exception {
      testCloseLink(false);
   }

   public void testCloseLink(boolean isSender) throws Exception {

      AtomicInteger errors = new AtomicInteger(0);
      AssertionLoggerHandler.startCapture(true);

      ActiveMQServer server2 = createServer(AMQP_PORT_2, false);

      if (isSender) {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT_2).setReconnectAttempts(-1).setRetryInterval(10);
         amqpConnection.addElement(new AMQPBrokerConnectionElement().setMatchAddress(getQueueName()).setType(AMQPBrokerConnectionAddressType.SENDER));
         server.getConfiguration().addAMQPConnection(amqpConnection);
      } else {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT).setReconnectAttempts(-1).setRetryInterval(10);
         amqpConnection.addElement(new AMQPBrokerConnectionElement().setMatchAddress(getQueueName()).setType(AMQPBrokerConnectionAddressType.RECEIVER));
         server2.getConfiguration().addAMQPConnection(amqpConnection);
      }

      if (isSender) {
         server.start();
         Assert.assertEquals(1, server.getBrokerConnections().size());
      } else {
         server2.start();
         Assert.assertEquals(1, server2.getBrokerConnections().size());
      }
      Wait.assertTrue(() -> AssertionLoggerHandler.findText("AMQ111002"));
      server.getBrokerConnections().forEach((t) -> Wait.assertTrue(() -> ((AMQPBrokerConnection) t).isConnecting()));

      if (isSender) {
         server2.start();
      } else {
         server.start();
      }

      server.getBrokerConnections().forEach((t) -> Wait.assertFalse(() -> ((AMQPBrokerConnection) t).isConnecting()));

      createAddressAndQueues(server);
      createAddressAndQueues(server2);

      Wait.assertTrue(() -> server.locateQueue(getQueueName()) != null);
      Wait.assertTrue(() -> server2.locateQueue(getQueueName()) != null);

      ActiveMQServer serverReceivingConnections = isSender ? server2 : server;
      Wait.assertEquals(1, serverReceivingConnections.getRemotingService()::getConnectionCount);
      serverReceivingConnections.getRemotingService().getConnections().forEach((t) -> {
         try {
            ActiveMQProtonRemotingConnection connection = (ActiveMQProtonRemotingConnection) t;
            ConnectionImpl protonConnection = (ConnectionImpl) connection.getAmqpConnection().getHandler().getConnection();
            Wait.waitFor(() -> protonConnection.linkHead(of(ACTIVE), of(ACTIVE)) != null);
            connection.getAmqpConnection().runNow(() -> {
               Link theLink = protonConnection.linkHead(of(ACTIVE), of(ACTIVE));
               theLink.close();
               connection.flush();
            });
         } catch (Exception e) {
            errors.incrementAndGet();
            e.printStackTrace();
         }
      });

      Wait.assertEquals(1, () -> AssertionLoggerHandler.countText("AMQ119021"));

      ConnectionFactory cf1 = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);
      ConnectionFactory cf2 = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT_2);

      try (Connection connection = cf1.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }

      // messages should still flow after a disconnect on the link
      // the server should reconnect as if it was a failure
      try (Connection connection = cf2.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         connection.start();
         for (int i = 0; i < 10; i++) {
            Assert.assertEquals("message " + i, ((TextMessage) consumer.receive(5000)).getText());
         }
      }

      Assert.assertEquals(0, errors.get());

   }

   @Test
   public void testTimeoutOnSenderOpen() throws Exception {

      startVerx();

      ProtonServerOptions serverOptions = new ProtonServerOptions();

      mockServer = new MockServer(vertx, serverOptions, null, serverConnection -> {
         serverConnection.openHandler(serverSender -> {
            serverConnection.closeHandler(x -> serverConnection.close());
            serverConnection.open();
         });
         serverConnection.sessionOpenHandler((s) -> {
            s.open();
         });
         serverConnection.senderOpenHandler((x) -> {
            x.open();
         });
         serverConnection.receiverOpenHandler((x) -> {
            //x.open(); // I'm missing the open, so it won't ever connect
         });
      });

      try {
         AssertionLoggerHandler.startCapture(true);

         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + mockServer.actualPort() + "?connect-timeout-millis=20").setReconnectAttempts(5).setRetryInterval(10);
         amqpConnection.addElement(new AMQPBrokerConnectionElement().setMatchAddress(getQueueName()).setType(AMQPBrokerConnectionAddressType.SENDER));
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         Wait.assertTrue(() -> AssertionLoggerHandler.findText("AMQ111001"));
         Wait.assertEquals(6, () -> AssertionLoggerHandler.countText("AMQ119020")); // 0..5 == 6

      } finally {
         mockServer.close();
      }
   }

   @Test
   public void testReconnectAfterSenderOpenTimeout() throws Exception {

      startVerx();

      AssertionLoggerHandler.startCapture(true);

      ProtonServerOptions serverOptions = new ProtonServerOptions();

      AtomicInteger countOpen = new AtomicInteger(0);
      CyclicBarrier startFlag = new CyclicBarrier(2);
      CountDownLatch blockBeforeOpen = new CountDownLatch(1);
      AtomicInteger disconnects = new AtomicInteger(0);
      AtomicInteger messagesReceived = new AtomicInteger(0);
      AtomicInteger errors = new AtomicInteger(0);

      ConcurrentHashSet<ProtonConnection> connections = new ConcurrentHashSet<>();

      mockServer = new MockServer(vertx, serverOptions, null, serverConnection -> {
         serverConnection.disconnectHandler(c -> {
            disconnects.incrementAndGet(); // number of retries
            connections.remove(c);
         });
         serverConnection.openHandler(serverSender -> {
            serverConnection.closeHandler(x -> {
               serverConnection.close();
               connections.remove(serverConnection);
            });
            serverConnection.open();
            connections.add(serverConnection);
         });
         serverConnection.sessionOpenHandler((s) -> {
            s.open();
         });
         serverConnection.senderOpenHandler((x) -> {
            x.open();
         });
         serverConnection.receiverOpenHandler((x) -> {
            if (countOpen.incrementAndGet() > 2) {
               if (countOpen.get() == 3) {
                  try {
                     startFlag.await(10, TimeUnit.SECONDS);
                     blockBeforeOpen.await(10, TimeUnit.SECONDS);
                     return;
                  } catch (Throwable ignored) {
                  }
               }
               HashMap<Symbol, Object> brokerIDProperties = new HashMap<>();
               brokerIDProperties.put(AMQPMirrorControllerSource.BROKER_ID, "fake-id");
               x.setProperties(brokerIDProperties);
               x.setOfferedCapabilities(new Symbol[]{AMQPMirrorControllerSource.MIRROR_CAPABILITY});
               x.setTarget(x.getRemoteTarget());
               x.open();
               x.handler((del, msg) -> {
                  if (msg.getApplicationProperties() != null) {
                     Map map = msg.getApplicationProperties().getValue();
                     Object value = map.get("sender");
                     if (value != null) {
                        if (messagesReceived.get() != ((Integer) value).intValue()) {
                           logger.warn("Message out of order. Expected " + messagesReceived.get() + " but received " + value);
                           errors.incrementAndGet();
                        }
                        messagesReceived.incrementAndGet();
                     }
                  }
               });
            }
         });
      });

      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + mockServer.actualPort() + "?connect-timeout-millis=1000").setReconnectAttempts(10).setRetryInterval(10);
      amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
      server.getConfiguration().addAMQPConnection(amqpConnection);
      server.start();

      startFlag.await(10, TimeUnit.SECONDS);
      blockBeforeOpen.countDown();

      Wait.assertEquals(2, disconnects::intValue);
      Wait.assertEquals(1, connections::size);

      Wait.assertEquals(3, () -> AssertionLoggerHandler.countText("AMQ119020"));

      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 0; i < 100; i++) {
            TextMessage message = session.createTextMessage("hello");
            message.setIntProperty("sender", i);
            producer.send(message);
         }
      }

      Wait.assertEquals(100, messagesReceived::intValue, 5000);
      Assert.assertEquals(0, errors.get(), 5000);
   }

   @Test
   public void testNoServerOfferedMirrorCapability() throws Exception {
      startVerx();

      mockServer = new MockServer(vertx, serverConnection -> {
         serverConnection.openHandler(serverSender -> {
            serverConnection.open();
         });
         serverConnection.sessionOpenHandler((s) -> {
            s.open();
         });
         serverConnection.senderOpenHandler((x) -> {
            x.open();
         });
         serverConnection.receiverOpenHandler((x) -> {
            x.setTarget(x.getRemoteTarget());
            x.open();
         });
      });

      AssertionLoggerHandler.startCapture(true);

      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + mockServer.actualPort() + "?connect-timeout-millis=100").setReconnectAttempts(5).setRetryInterval(10);
      amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
      server.getConfiguration().addAMQPConnection(amqpConnection);
      server.start();

      Wait.assertTrue(() -> AssertionLoggerHandler.findText("AMQ111001"));
      Assert.assertEquals(6, AssertionLoggerHandler.countText("AMQ119018")); // 0..5 = 6
   }

   /**
    * Refuse the first mirror link, verify broker handles it and reconnects
    *
    * @throws Exception
    */
   @Test
   public void testReconnectAfterMirrorLinkRefusal() throws Exception {
      startVerx();

      AtomicInteger errors = new AtomicInteger(0);

      AtomicInteger messagesReceived = new AtomicInteger(0);

      List<ProtonConnection> connections = Collections.synchronizedList(new ArrayList<ProtonConnection>());
      List<ProtonConnection> disconnected = Collections.synchronizedList(new ArrayList<ProtonConnection>());
      AtomicInteger refusedLinkMessageCount = new AtomicInteger();
      AtomicInteger linkOpens = new AtomicInteger(0);

      mockServer = new MockServer(vertx, serverConnection -> {
         serverConnection.disconnectHandler(c -> {
            disconnected.add(serverConnection);
         });

         serverConnection.openHandler(c -> {
            connections.add(serverConnection);
            serverConnection.open();
         });

         serverConnection.closeHandler(c -> {
            serverConnection.close();
            connections.remove(serverConnection);
         });

         serverConnection.sessionOpenHandler(session -> {
            session.open();
         });

         serverConnection.receiverOpenHandler(serverReceiver -> {
            Target remoteTarget = serverReceiver.getRemoteTarget();
            String remoteAddress = remoteTarget == null ? null : remoteTarget.getAddress();
            if (remoteAddress == null || !remoteAddress.startsWith(ProtonProtocolManager.MIRROR_ADDRESS)) {
               errors.incrementAndGet();
               logger.warn("Receiving address as " + remoteAddress);
               return;
            }
            if (linkOpens.incrementAndGet() != 2) {
               logger.debug("Link Opens::{}", linkOpens);
               logger.debug("ServerReceiver = {}", serverReceiver.getTarget());
               serverReceiver.setTarget(null);

               serverReceiver.handler((del, msg) -> {
                  refusedLinkMessageCount.incrementAndGet();
                  logger.debug("Should not have got message on refused link: {}", msg);
               });

               serverReceiver.open();

               vertx.setTimer(20, x -> {
                  serverReceiver.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, "Testing refusal of mirror link for $reasons"));
                  serverReceiver.close();
               });
            } else {
               serverReceiver.setTarget(serverReceiver.getRemoteTarget());
               HashMap<Symbol, Object> linkProperties = new HashMap<>();
               linkProperties.put(AMQPMirrorControllerSource.BROKER_ID, "fake-id");

               serverReceiver.setProperties(linkProperties);
               serverReceiver.setOfferedCapabilities(new Symbol[]{AMQPMirrorControllerSource.MIRROR_CAPABILITY});

               serverReceiver.handler((del, msg) -> {
                  logger.debug("prefetch = {}, Got message: {}", serverReceiver.getPrefetch(), msg);
                  if (msg.getApplicationProperties() != null) {
                     Map map = msg.getApplicationProperties().getValue();
                     Object value = map.get("sender");
                     if (value != null) {
                        if (messagesReceived.get() != ((Integer) value).intValue()) {
                           logger.warn("Message out of order. Expected " + messagesReceived.get() + " but received " + value);
                           errors.incrementAndGet();
                        }
                        messagesReceived.incrementAndGet();
                     }
                  }
                  del.disposition(Accepted.getInstance(), true);
                  if (serverReceiver.getPrefetch() == 0) {
                     serverReceiver.flow(1);
                  }
               });

               serverReceiver.open();
            }
         });
      });

      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + mockServer.actualPort()).setReconnectAttempts(3).setRetryInterval(10);
      amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
      server.getConfiguration().addAMQPConnection(amqpConnection);
      server.start();

      Wait.assertEquals(1, disconnected::size, 6000);
      Wait.assertEquals(2, connections::size, 6000);

      assertSame(connections.get(0), disconnected.get(0));
      assertFalse(connections.get(1).isDisconnected());

      assertEquals("Should not have got any message on refused link", 0, refusedLinkMessageCount.get());
      assertEquals(0, errors.get());

      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 0; i < 100; i++) {
            TextMessage message = session.createTextMessage("hello");
            message.setIntProperty("sender", i);
            producer.send(message);
         }
      }

      Wait.assertEquals(100, messagesReceived::intValue);
      assertEquals(0, errors.get()); // Meant to check again. the errors check before was because of connection issues. This one is about duplicates on receiving
   }

   @Test
   public void testNoClientDesiredMirrorCapability() throws Exception {
      AssertionLoggerHandler.startCapture();
      server.start();

      AmqpClient client = new AmqpClient(new URI("tcp://localhost:" + AMQP_PORT), null, null);
      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectOpenedResource(Sender sender) {
            ErrorCondition condition = sender.getRemoteCondition();

            if (condition != null && condition.getCondition() != null) {
               if (!condition.getCondition().equals(AmqpError.ILLEGAL_STATE)) {
                  markAsInvalid("Should have been closed with an illegal state error, but error was: " + condition);
               }

               if (!condition.getDescription().contains("AMQ119024")) {
                  markAsInvalid("should have indicated the error code about missing a desired capability");
               }

               if (!condition.getDescription().contains(AMQPMirrorControllerSource.MIRROR_CAPABILITY)) {
                  markAsInvalid("should have indicated the error code about missing a desired capability");
               }
            } else {
               markAsInvalid("Sender should have been detached with an error");
            }
         }
      });

      String address = ProtonProtocolManager.getMirrorAddress(getTestName());

      AmqpConnection connection = client.connect();
      try {
         AmqpSession session = connection.createSession();

         try {
            session.createSender(address);
            fail("Link should have been refused.");
         } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("AMQ119024"));
            instanceLog.debug("Caught expected exception");
         }

         connection.getStateInspector().assertValid();
      } finally {
         connection.close();
      }

      Wait.assertTrue(() -> AssertionLoggerHandler.findText("AMQ119024"));
   }
}
