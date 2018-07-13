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
package org.apache.activemq.artemis.tests.integration.stomp;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTProtocolManagerFactory;
import org.apache.activemq.artemis.core.protocol.stomp.Stomp;
import org.apache.activemq.artemis.core.protocol.stomp.StompProtocolManagerFactory;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.TopicConfigurationImpl;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class StompTestBase extends ActiveMQTestBase {

   @Parameterized.Parameter
   public String scheme;

   protected URI uri;

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][]{{"ws+v10.stomp"}, {"tcp+v10.stomp"}});
   }

   protected String hostname = "127.0.0.1";

   protected final int port = 61613;

   private ConnectionFactory connectionFactory;

   protected Connection connection;

   protected Session session;

   protected Queue queue;

   protected Topic topic;

   protected JMSServerManager server;

   protected String defUser = "brianm";

   protected String defPass = "wombats";

   // Implementation methods
   // -------------------------------------------------------------------------
   public boolean isCompressLargeMessages() {
      return false;
   }

   public boolean isSecurityEnabled() {
      return false;
   }

   public boolean isPersistenceEnabled() {
      return false;
   }

   public boolean isEnableStompMessageId() {
      return false;
   }

   public Integer getStompMinLargeMessageSize() {
      return null;
   }

   public List<String> getIncomingInterceptors() {
      return null;
   }

   public List<String> getOutgoingInterceptors() {
      return null;
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      uri = new URI(scheme + "://" + hostname + ":" + port);

      server = createServer();
      server.start();

      waitForServerToStart(server.getActiveMQServer());

      connectionFactory = createConnectionFactory();

      ((ActiveMQConnectionFactory)connectionFactory).setCompressLargeMessage(isCompressLargeMessages());

      if (isSecurityEnabled()) {
         connection = connectionFactory.createConnection("brianm", "wombats");
      } else {
         connection = connectionFactory.createConnection();
      }
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queue = session.createQueue(getQueueName());
      topic = session.createTopic(getTopicName());
      connection.start();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (connection != null) {
         connection.close();
      }
      super.tearDown();
   }


   /**
    * @return
    * @throws Exception
    */
   protected JMSServerManager createServer() throws Exception {
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, StompProtocolManagerFactory.STOMP_PROTOCOL_NAME + ","  + MQTTProtocolManagerFactory.MQTT_PROTOCOL_NAME);
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      params.put(TransportConstants.STOMP_CONSUMERS_CREDIT, "-1");
      if (isEnableStompMessageId()) {
         params.put(TransportConstants.STOMP_ENABLE_MESSAGE_ID, true);
      }
      if (getStompMinLargeMessageSize() != null) {
         params.put(TransportConstants.STOMP_MIN_LARGE_MESSAGE_SIZE, 2048);
      }
      TransportConfiguration stompTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);

      Configuration config = createBasicConfig().setSecurityEnabled(isSecurityEnabled())
                                                .setPersistenceEnabled(isPersistenceEnabled())
                                                .addAcceptorConfiguration(stompTransport)
                                                .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()))
                                                .setConnectionTtlCheckInterval(500);

      if (getIncomingInterceptors() != null) {
         config.setIncomingInterceptorClassNames(getIncomingInterceptors());
      }

      if (getOutgoingInterceptors() != null) {
         config.setOutgoingInterceptorClassNames(getOutgoingInterceptors());
      }

      config.setPersistenceEnabled(true);

      ActiveMQServer activeMQServer = addServer(ActiveMQServers.newActiveMQServer(config, defUser, defPass));

      if (isSecurityEnabled()) {
         ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) activeMQServer.getSecurityManager();

         final String role = "testRole";
         securityManager.getConfiguration().addRole(defUser, role);
         config.getSecurityRoles().put("#", new HashSet<Role>() {
            {
               add(new Role(role, true, true, true, true, true, true, true, true, true, true));
            }
         });
      }

      JMSConfiguration jmsConfig = new JMSConfigurationImpl();
      jmsConfig.getQueueConfigurations().add(new JMSQueueConfigurationImpl().setName(getQueueName()).setBindings(getQueueName()));
      jmsConfig.getTopicConfigurations().add(new TopicConfigurationImpl().setName(getTopicName()).setBindings(getTopicName()));
      server = new JMSServerManagerImpl(activeMQServer, jmsConfig);
      server.setRegistry(new JndiBindingRegistry(new InVMNamingContext()));
      return server;
   }

   protected ConnectionFactory createConnectionFactory() {
      return new ActiveMQJMSConnectionFactory(false, new TransportConfiguration(InVMConnectorFactory.class.getName()));
   }

   protected static String getQueueName() {
      return "testQueue";
   }

   protected static String getQueuePrefix() {
      return "";
   }

   protected static String getTopicName() {
      return "testtopic";
   }

   protected static String getTopicPrefix() {
      return "";
   }

   public void sendJmsMessage(String msg) throws Exception {
      sendJmsMessage(msg, queue);
   }

   public void sendJmsMessage(String msg, Destination destination) throws Exception {
      MessageProducer producer = session.createProducer(destination);
      TextMessage message = session.createTextMessage(msg);
      producer.send(message);
   }

   public void sendJmsMessage(byte[] data, Destination destination) throws Exception {
      sendJmsMessage(data, "foo", "xyz", destination);
   }

   public void sendJmsMessage(String msg, String propertyName, String propertyValue) throws Exception {
      sendJmsMessage(msg.getBytes(StandardCharsets.UTF_8), propertyName, propertyValue, queue);
   }

   public void sendJmsMessage(byte[] data,
                              String propertyName,
                              String propertyValue,
                              Destination destination) throws Exception {
      MessageProducer producer = session.createProducer(destination);
      BytesMessage message = session.createBytesMessage();
      message.setStringProperty(propertyName, propertyValue);
      message.writeBytes(data);
      producer.send(message);
   }

   public static void abortTransaction(StompClientConnection conn, String txID) throws IOException, InterruptedException {
      ClientStompFrame abortFrame = conn.createFrame(Stomp.Commands.ABORT)
                                        .addHeader(Stomp.Headers.TRANSACTION, txID);

      conn.sendFrame(abortFrame);
   }

   public static void beginTransaction(StompClientConnection conn, String txID) throws IOException, InterruptedException {
      ClientStompFrame beginFrame = conn.createFrame(Stomp.Commands.BEGIN)
                                        .addHeader(Stomp.Headers.TRANSACTION, txID);

      conn.sendFrame(beginFrame);
   }

   public static void commitTransaction(StompClientConnection conn, String txID) throws IOException, InterruptedException {
      commitTransaction(conn, txID, false);
   }

   public static void commitTransaction(StompClientConnection conn,
                                 String txID,
                                 boolean receipt) throws IOException, InterruptedException {
      ClientStompFrame beginFrame = conn.createFrame(Stomp.Commands.COMMIT)
                                        .addHeader(Stomp.Headers.TRANSACTION, txID);
      String uuid = UUID.randomUUID().toString();
      if (receipt) {
         beginFrame.addHeader(Stomp.Headers.RECEIPT_REQUESTED, uuid);
      }
      ClientStompFrame resp = conn.sendFrame(beginFrame);
      if (receipt) {
         assertEquals(uuid, resp.getHeader(Stomp.Headers.Response.RECEIPT_ID));
      }
   }

   public static void ack(StompClientConnection conn,
                   String subscriptionId,
                   ClientStompFrame messageIdFrame) throws IOException, InterruptedException {
      String messageID = messageIdFrame.getHeader(Stomp.Headers.Message.MESSAGE_ID);

      ClientStompFrame frame = conn.createFrame(Stomp.Commands.ACK)
                                      .addHeader(Stomp.Headers.Message.MESSAGE_ID, messageID);

      if (subscriptionId != null) {
         frame.addHeader(Stomp.Headers.Ack.SUBSCRIPTION, subscriptionId);
      }

      ClientStompFrame response = conn.sendFrame(frame);
      if (response != null) {
         throw new IOException("failed to ack " + response);
      }
   }

   public static void ack(StompClientConnection conn,
                   String subscriptionId,
                   String mid,
                   String txID) throws IOException, InterruptedException {
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.ACK)
                                      .addHeader(Stomp.Headers.Ack.SUBSCRIPTION, subscriptionId)
                                      .addHeader(Stomp.Headers.Message.MESSAGE_ID, mid);
      if (txID != null) {
         frame.addHeader(Stomp.Headers.TRANSACTION, txID);
      }

      conn.sendFrame(frame);
   }

   public static void nack(StompClientConnection conn, String subscriptionId, String messageId) throws IOException, InterruptedException {
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.NACK)
                                      .addHeader(Stomp.Headers.Ack.SUBSCRIPTION, subscriptionId)
                                      .addHeader(Stomp.Headers.Message.MESSAGE_ID, messageId);

      conn.sendFrame(frame);
   }

   public static ClientStompFrame subscribe(StompClientConnection conn,
                                     String subscriptionId) throws IOException, InterruptedException {
      return subscribe(conn, subscriptionId, Stomp.Headers.Subscribe.AckModeValues.AUTO);
   }

   public static ClientStompFrame subscribe(StompClientConnection conn,
                                     String subscriptionId,
                                     String ack) throws IOException, InterruptedException {
      return subscribe(conn, subscriptionId, ack, null);
   }

   public static ClientStompFrame subscribe(StompClientConnection conn,
                                     String subscriptionId,
                                     String ack,
                                     String durableId) throws IOException, InterruptedException {
      return subscribe(conn, subscriptionId, ack, durableId, true);
   }

   public static ClientStompFrame subscribe(StompClientConnection conn,
                                     String subscriptionId,
                                     String ack,
                                     String durableId,
                                     boolean receipt) throws IOException, InterruptedException {
      return subscribe(conn, subscriptionId, ack, durableId, null, receipt);
   }

   public static ClientStompFrame subscribe(StompClientConnection conn,
                                     String subscriptionId,
                                     String ack,
                                     String durableId,
                                     String selector) throws IOException, InterruptedException {
      return subscribe(conn, subscriptionId, ack, durableId, selector, true);
   }

   public static ClientStompFrame subscribe(StompClientConnection conn,
                                     String subscriptionId,
                                     String ack,
                                     String durableId,
                                     String selector,
                                     boolean receipt) throws IOException, InterruptedException {
      return subscribe(conn, subscriptionId, ack, durableId, selector, getQueuePrefix() + getQueueName(), receipt);
   }

   public static ClientStompFrame subscribeQueue(StompClientConnection conn, String subId, String destination) throws IOException, InterruptedException {
      return subscribe(conn, subId, Stomp.Headers.Subscribe.AckModeValues.AUTO, null, null, destination, true);
   }

   public static ClientStompFrame subscribe(StompClientConnection conn,
                                     String subscriptionId,
                                     String ack,
                                     String durableId,
                                     String selector,
                                     String destination,
                                     boolean receipt) throws IOException, InterruptedException {
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SUBSCRIBE)
                                   .addHeader(Stomp.Headers.Subscribe.SUBSCRIPTION_TYPE, RoutingType.ANYCAST.toString())
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, destination);
      if (subscriptionId != null) {
         frame.addHeader(Stomp.Headers.Subscribe.ID, subscriptionId);
      }
      if (ack != null) {
         frame.addHeader(Stomp.Headers.Subscribe.ACK_MODE, ack);
      }
      if (durableId != null) {
         frame.addHeader(Stomp.Headers.Subscribe.DURABLE_SUBSCRIPTION_NAME, durableId);
      }
      if (selector != null) {
         frame.addHeader(Stomp.Headers.Subscribe.SELECTOR, selector);
      }

      String uuid = UUID.randomUUID().toString();
      if (receipt) {
         frame.addHeader(Stomp.Headers.RECEIPT_REQUESTED, uuid);
      }

      frame = conn.sendFrame(frame);

      // Return Error Frame back to the client
      if (frame != null && frame.getCommand().equals("ERROR")) {
         return frame;
      }

      if (receipt) {
         assertEquals(uuid, frame.getHeader(Stomp.Headers.Response.RECEIPT_ID));
      }

      return frame;
   }

   public static ClientStompFrame subscribeTopic(StompClientConnection conn,
                                          String subscriptionId,
                                          String ack,
                                          String durableId) throws IOException, InterruptedException {
      return subscribeTopic(conn, subscriptionId, ack, durableId, true);
   }

   public static ClientStompFrame subscribeTopic(StompClientConnection conn,
                                          String subscriptionId,
                                          String ack,
                                          String durableId,
                                          boolean receipt) throws IOException, InterruptedException {
      return subscribeTopic(conn, subscriptionId, ack, durableId, receipt, false);
   }

   public static ClientStompFrame subscribeTopic(StompClientConnection conn,
         String subscriptionId,
         String ack,
         String durableId,
         boolean receipt,
         boolean noLocal) throws IOException, InterruptedException {
      return subscribeTopic(conn, subscriptionId, ack, durableId, Stomp.Headers.Subscribe.DURABLE_SUBSCRIPTION_NAME, receipt, noLocal);
   }

   public static ClientStompFrame subscribeTopicLegacyActiveMQ(StompClientConnection conn,
         String subscriptionId,
         String ack,
         String durableId,
         boolean receipt,
         boolean noLocal) throws IOException, InterruptedException {
      return subscribeTopic(conn, subscriptionId, ack, durableId, Stomp.Headers.Subscribe.ACTIVEMQ_DURABLE_SUBSCRIPTION_NAME, receipt, noLocal);
   }

   public static ClientStompFrame subscribeTopic(StompClientConnection conn,
                                          String subscriptionId,
                                          String ack,
                                          String durableId,
                                          String durableIdHeader,
                                          boolean receipt,
                                          boolean noLocal) throws IOException, InterruptedException {
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SUBSCRIBE)
                                   .addHeader(Stomp.Headers.Subscribe.SUBSCRIPTION_TYPE, RoutingType.MULTICAST.toString())
                                   .addHeader(Stomp.Headers.Subscribe.DESTINATION, getTopicPrefix() + getTopicName());
      if (subscriptionId != null) {
         frame.addHeader(Stomp.Headers.Subscribe.ID, subscriptionId);
      }
      if (ack != null) {
         frame.addHeader(Stomp.Headers.Subscribe.ACK_MODE, ack);
      }
      if (durableId != null) {
         frame.addHeader(durableIdHeader, durableId);
      }
      String uuid = UUID.randomUUID().toString();
      if (receipt) {
         frame.addHeader(Stomp.Headers.RECEIPT_REQUESTED, uuid);
      }
      if (noLocal) {
         frame.addHeader(Stomp.Headers.Subscribe.NO_LOCAL, "true");
      }

      frame = conn.sendFrame(frame);

      if (frame.getCommand().equals("ERROR")) {
         return frame;
      }

      if (receipt) {
         assertNotNull("Requested receipt, but response is null", frame);
         assertTrue(frame.getHeader(Stomp.Headers.Response.RECEIPT_ID).equals(uuid));
      }

      return frame;
   }

   public static ClientStompFrame unsubscribe(StompClientConnection conn, String subscriptionId) throws IOException, InterruptedException {
      return unsubscribe(conn, subscriptionId, null, false, false);
   }

   public static ClientStompFrame unsubscribe(StompClientConnection conn,
                                       String subscriptionId,
                                       boolean receipt) throws IOException, InterruptedException {
      return unsubscribe(conn, subscriptionId, null, receipt, false);
   }

   public static ClientStompFrame unsubscribe(StompClientConnection conn,
         String subscriptionId,
         String destination,
         boolean receipt,
         boolean durable) throws IOException, InterruptedException {
      return unsubscribe(conn, subscriptionId, Stomp.Headers.Unsubscribe.DURABLE_SUBSCRIPTION_NAME, destination, receipt, durable);
   }

   public static ClientStompFrame unsubscribeLegacyActiveMQ(StompClientConnection conn,
         String subscriptionId,
         String destination,
         boolean receipt,
         boolean durable) throws IOException, InterruptedException {
      return unsubscribe(conn, subscriptionId, Stomp.Headers.Unsubscribe.ACTIVEMQ_DURABLE_SUBSCRIPTION_NAME, destination, receipt, durable);
   }

   public static ClientStompFrame unsubscribe(StompClientConnection conn,
                                       String subscriptionId,
                                       String subscriptionIdHeader,
                                       String destination,
                                       boolean receipt,
                                       boolean durable) throws IOException, InterruptedException {
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.UNSUBSCRIBE);
      if (durable && subscriptionId != null) {
         frame.addHeader(subscriptionIdHeader, subscriptionId);
      } else if (!durable && subscriptionId != null) {
         frame.addHeader(Stomp.Headers.Unsubscribe.ID, subscriptionId);
      }

      if (destination != null) {
         frame.addHeader(Stomp.Headers.Unsubscribe.DESTINATION, destination);
      }

      String uuid = UUID.randomUUID().toString();

      if (receipt) {
         frame.addHeader(Stomp.Headers.RECEIPT_REQUESTED, uuid);
      }

      frame = conn.sendFrame(frame);

      if (receipt) {
         assertEquals(Stomp.Responses.RECEIPT, frame.getCommand());
         assertEquals(uuid, frame.getHeader(Stomp.Headers.Response.RECEIPT_ID));
      }

      return frame;
   }

   public static ClientStompFrame send(StompClientConnection conn, String destination, String contentType, String body) throws IOException, InterruptedException {
      return send(conn, destination, contentType, body, false);
   }

   public static ClientStompFrame send(StompClientConnection conn, String destination, String contentType, String body, boolean receipt) throws IOException, InterruptedException {
      return send(conn, destination, contentType, body, receipt, null);
   }

   public static ClientStompFrame send(StompClientConnection conn, String destination, String contentType, String body, boolean receipt, RoutingType destinationType) throws IOException, InterruptedException {
      return send(conn, destination, contentType, body, receipt, destinationType, null);
   }

   public static ClientStompFrame send(StompClientConnection conn, String destination, String contentType, String body, boolean receipt, RoutingType destinationType, String txId) throws IOException, InterruptedException {
      ClientStompFrame frame = conn.createFrame(Stomp.Commands.SEND)
                                   .addHeader(Stomp.Headers.Send.DESTINATION, destination)
                                   .setBody(body);

      if (contentType != null) {
         frame.addHeader(Stomp.Headers.CONTENT_TYPE, contentType);
      }

      if (destinationType != null) {
         frame.addHeader(Stomp.Headers.Send.DESTINATION_TYPE, destinationType.toString());
      }

      if (txId != null) {
         frame.addHeader(Stomp.Headers.TRANSACTION, txId);
      }

      String uuid = UUID.randomUUID().toString();

      if (receipt) {
         frame.addHeader(Stomp.Headers.RECEIPT_REQUESTED, uuid);
      }
      frame = conn.sendFrame(frame);

      if (frame != null && frame.getCommand().equals("ERROR")) {
         return frame;
      }

      if (receipt) {
         assertEquals(Stomp.Responses.RECEIPT, frame.getCommand());
         assertEquals(uuid, frame.getHeader(Stomp.Headers.Response.RECEIPT_ID));
      }

      IntegrationTestLogger.LOGGER.info("Received: " + frame);

      return frame;
   }

   public static URI createStompClientUri(String scheme, String hostname, int port) throws URISyntaxException {
      return new URI(scheme + "://" + hostname + ":" + port);
   }
}
