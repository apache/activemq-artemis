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
package org.apache.activemq.artemis.tests.integration.jms.multiprotocol;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Before;

public abstract class MultiProtocolJMSClientTestSupport extends ActiveMQTestBase {

   private static final Logger logger = Logger.getLogger(MultiProtocolJMSClientTestSupport.class);

   protected LinkedList<Connection> jmsConnections = new LinkedList<>();

   protected static final String BROKER_NAME = "localhost";
   protected static final String NETTY_ACCEPTOR = "netty-acceptor";

   protected String noprivUser = "noprivs";
   protected String noprivPass = "noprivs";

   protected String browseUser = "browser";
   protected String browsePass = "browser";

   protected String guestUser = "guest";
   protected String guestPass = "guest";

   protected String fullUser = "user";
   protected String fullPass = "pass";

   protected ActiveMQServer server;

   protected MBeanServer mBeanServer = MBeanServerFactory.createMBeanServer();

   ConnectionSupplier amqpConnectionSupplier = () -> createConnection();
   ConnectionSupplier coreConnectionSupplier = () -> createCoreConnection();
   ConnectionSupplier openWireConnectionSupplier = () -> createOpenWireConnection();

   protected static final int AMQP_PORT = 5672;

   protected boolean useSSL;
   protected boolean useWebSockets;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      server = createServer();

      // Bug in Qpid JMS not shutting down a connection thread on certain errors
      // TODO - Reevaluate after Qpid JMS 0.23.0 is released.
      disableCheckThread();
   }

   @After
   @Override
   public void tearDown() throws Exception {
      try {
         for (Connection connection : jmsConnections) {
            try {
               connection.close();
            } catch (Throwable ignored) {
               ignored.printStackTrace();
            }
         }
      } catch (Exception e) {
         logger.warn(e);
      }
      jmsConnections.clear();

      try {
         if (server != null) {
            server.stop();
         }
      } finally {
         super.tearDown();
      }
   }

   protected Connection trackJMSConnection(Connection connection) {
      jmsConnections.add(connection);
      return connection;
   }

   protected String getJmsConnectionURIOptions() {
      return "";
   }

   protected String getBrokerQpidJMSConnectionString() {
      try {
         int port = AMQP_PORT;

         String uri = null;

         if (isUseSSL()) {
            if (isUseWebSockets()) {
               uri = "amqpwss://127.0.0.1:" + port;
            } else {
               uri = "amqps://127.0.0.1:" + port;
            }
         } else {
            if (isUseWebSockets()) {
               uri = "amqpws://127.0.0.1:" + port;
            } else {
               uri = "amqp://127.0.0.1:" + port;
            }
         }

         if (!getJmsConnectionURIOptions().isEmpty()) {
            uri = uri + "?" + getJmsConnectionURIOptions();
         }

         return uri;
      } catch (Exception e) {
         throw new RuntimeException();
      }
   }

   protected URI getBrokerQpidJMSConnectionURI() {
      try {
         return new URI(getBrokerQpidJMSConnectionString());
      } catch (Exception e) {
         throw new RuntimeException();
      }
   }

   protected URI getBrokerQpidJMSFailoverConnectionURI() {
      try {
         return new URI("failover:(" + getBrokerQpidJMSConnectionString() + ")");
      } catch (Exception e) {
         throw new RuntimeException();
      }
   }

   protected Connection createConnection() throws JMSException {
      return createConnection(getBrokerQpidJMSConnectionURI(), null, null, null, true);
   }

   protected Connection createFailoverConnection() throws JMSException {
      return createConnection(getBrokerQpidJMSFailoverConnectionURI(), null, null, null, true);
   }

   protected Connection createConnection(boolean start) throws JMSException {
      return createConnection(getBrokerQpidJMSConnectionURI(), null, null, null, start);
   }

   protected Connection createConnection(String clientId) throws JMSException {
      return createConnection(getBrokerQpidJMSConnectionURI(), null, null, clientId, true);
   }

   protected Connection createConnection(String clientId, boolean start) throws JMSException {
      return createConnection(getBrokerQpidJMSConnectionURI(), null, null, clientId, start);
   }

   protected Connection createConnection(String username, String password) throws JMSException {
      return createConnection(getBrokerQpidJMSConnectionURI(), username, password, null, true);
   }

   protected Connection createConnection(String username, String password, String clientId) throws JMSException {
      return createConnection(getBrokerQpidJMSConnectionURI(), username, password, clientId, true);
   }

   protected Connection createConnection(String username, String password, String clientId, boolean start) throws JMSException {
      return createConnection(getBrokerQpidJMSConnectionURI(), username, password, clientId, start);
   }

   protected Connection createConnection(URI remoteURI, String username, String password, String clientId, boolean start) throws JMSException {
      JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);

      Connection connection = trackJMSConnection(factory.createConnection(username, password));

      connection.setExceptionListener(exception -> exception.printStackTrace());

      if (clientId != null && !clientId.isEmpty()) {
         connection.setClientID(clientId);
      }

      if (start) {
         connection.start();
      }

      return connection;
   }

   protected String getBrokerCoreJMSConnectionString() {

      try {
         int port = AMQP_PORT;

         String uri = null;

         if (isUseSSL()) {
            uri = "tcp://127.0.0.1:" + port;
         } else {
            uri = "tcp://127.0.0.1:" + port;
         }

         if (!getJmsConnectionURIOptions().isEmpty()) {
            uri = uri + "?" + getJmsConnectionURIOptions();
         }

         return uri;
      } catch (Exception e) {
         throw new RuntimeException();
      }
   }

   protected Connection createCoreConnection() throws JMSException {
      return createCoreConnection(getBrokerCoreJMSConnectionString(), null, null, null, true);
   }

   protected Connection createCoreConnection(boolean start) throws JMSException {
      return createCoreConnection(getBrokerCoreJMSConnectionString(), null, null, null, start);
   }

   protected Connection createCoreConnection(String username, String password) throws JMSException {
      return createCoreConnection(getBrokerCoreJMSConnectionString(), username, password, null, true);
   }

   private Connection createCoreConnection(String connectionString, String username, String password, String clientId, boolean start) throws JMSException {
      ActiveMQJMSConnectionFactory factory = new ActiveMQJMSConnectionFactory(connectionString);

      Connection connection = trackJMSConnection(factory.createConnection(username, password));

      connection.setExceptionListener(exception -> exception.printStackTrace());

      if (clientId != null && !clientId.isEmpty()) {
         connection.setClientID(clientId);
      }

      if (start) {
         connection.start();
      }

      return connection;
   }

   protected String getBrokerOpenWireJMSConnectionString() {

      try {
         int port = AMQP_PORT;

         String uri = null;

         if (isUseSSL()) {
            uri = "tcp://127.0.0.1:" + port;
         } else {
            uri = "tcp://127.0.0.1:" + port;
         }

         if (!getJmsConnectionURIOptions().isEmpty()) {
            uri = uri + "?" + getJmsConnectionURIOptions();
         } else {
            uri = uri + "?wireFormat.cacheEnabled=true";
         }

         return uri;
      } catch (Exception e) {
         throw new RuntimeException();
      }
   }

   protected Connection createOpenWireConnection() throws JMSException {
      return createOpenWireConnection(getBrokerOpenWireJMSConnectionString(), null, null, null, true);
   }

   protected Connection createOpenWireConnection(boolean start) throws JMSException {
      return createOpenWireConnection(getBrokerOpenWireJMSConnectionString(), null, null, null, start);
   }

   protected Connection createOpenWireConnection(String username, String password) throws JMSException {
      return createOpenWireConnection(getBrokerOpenWireJMSConnectionString(), username, password, null, true);
   }

   private Connection createOpenWireConnection(String connectionString, String username, String password, String clientId, boolean start) throws JMSException {
      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionString);

      Connection connection = trackJMSConnection(factory.createConnection(username, password));

      connection.setExceptionListener(exception -> exception.printStackTrace());

      if (clientId != null && !clientId.isEmpty()) {
         connection.setClientID(clientId);
      }

      if (start) {
         connection.start();
      }

      return connection;
   }

   interface ConnectionSupplier {
      Connection createConnection() throws JMSException;
   }

   interface SecureConnectionSupplier {
      Connection createConnection(String username, String Password) throws JMSException;
   }

   protected boolean isAutoCreateQueues() {
      return true;
   }

   protected boolean isAutoCreateAddresses() {
      return true;
   }

   protected boolean isSecurityEnabled() {
      return false;
   }

   protected String getDeadLetterAddress() {
      return "ActiveMQ.DLQ";
   }

   protected int getPrecreatedQueueSize() {
      return 10;
   }

   protected ActiveMQServer createServer() throws Exception {
      return createServer(AMQP_PORT);
   }

   protected ActiveMQServer createServer(int port) throws Exception {
      return createServer(port, true);
   }

   protected ActiveMQServer createServer(int port, boolean start) throws Exception {

      final ActiveMQServer server = this.createServer(true, true);

      server.getConfiguration().getAcceptorConfigurations().clear();
      server.getConfiguration().getAcceptorConfigurations().add(addAcceptorConfiguration(server, port));
      server.getConfiguration().setName(BROKER_NAME);
      server.getConfiguration().setJournalDirectory(server.getConfiguration().getJournalDirectory() + port);
      server.getConfiguration().setBindingsDirectory(server.getConfiguration().getBindingsDirectory() + port);
      server.getConfiguration().setPagingDirectory(server.getConfiguration().getPagingDirectory() + port);
      if (port == AMQP_PORT) {
         // we use the default large directory if the default port
         // as some tests will assert number of files
         server.getConfiguration().setLargeMessagesDirectory(server.getConfiguration().getLargeMessagesDirectory());
      } else {
         server.getConfiguration().setLargeMessagesDirectory(server.getConfiguration().getLargeMessagesDirectory() + port);
      }
      server.getConfiguration().setJMXManagementEnabled(true);
      server.getConfiguration().setMessageExpiryScanPeriod(100);
      server.setMBeanServer(mBeanServer);

      // Add any additional Acceptors needed for tests
      addAdditionalAcceptors(server);

      // Address configuration
      configureAddressPolicy(server);

      // Add optional security for tests that need it
      configureBrokerSecurity(server);

      // Add extra configuration
      addConfiguration(server);

      if (start) {
         server.start();

         // Prepare all addresses and queues for client tests.
         createAddressAndQueues(server);
      }

      return server;
   }

   protected void addConfiguration(ActiveMQServer server) {

   }

   protected TransportConfiguration addAcceptorConfiguration(ActiveMQServer server, int port) {
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PORT_PROP_NAME, String.valueOf(port));
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, getConfiguredProtocols());
      HashMap<String, Object> amqpParams = new HashMap<>();
      configureAMQPAcceptorParameters(amqpParams);
      TransportConfiguration tc = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, NETTY_ACCEPTOR, amqpParams);
      configureAMQPAcceptorParameters(tc);
      return tc;
   }

   protected String getConfiguredProtocols() {
      return "AMQP,CORE,OPENWIRE";
   }

   protected void configureAddressPolicy(ActiveMQServer server) {
      // Address configuration
      AddressSettings addressSettings = new AddressSettings();

      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      addressSettings.setAutoCreateQueues(isAutoCreateQueues());
      addressSettings.setAutoCreateAddresses(isAutoCreateAddresses());
      addressSettings.setDeadLetterAddress(SimpleString.toSimpleString(getDeadLetterAddress()));
      addressSettings.setExpiryAddress(SimpleString.toSimpleString(getDeadLetterAddress()));

      server.getConfiguration().getAddressSettings().put("#", addressSettings);
      Set<TransportConfiguration> acceptors = server.getConfiguration().getAcceptorConfigurations();
      for (TransportConfiguration tc : acceptors) {
         if (tc.getName().equals(NETTY_ACCEPTOR)) {
            tc.getExtraParams().put("anycastPrefix", "anycast://");
            tc.getExtraParams().put("multicastPrefix", "multicast://");
         }
      }
   }

   protected void createAddressAndQueues(ActiveMQServer server) throws Exception {
      // Default Queue
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(getQueueName()), RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(getQueueName()).setRoutingType(RoutingType.ANYCAST));

      // Default DLQ
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(getDeadLetterAddress()), RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(getDeadLetterAddress()).setRoutingType(RoutingType.ANYCAST));

      // Default Topic
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(getTopicName()), RoutingType.MULTICAST));
      server.createQueue(new QueueConfiguration(getTopicName()));

      // Additional Test Queues
      for (int i = 0; i < getPrecreatedQueueSize(); ++i) {
         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(getQueueName(i)), RoutingType.ANYCAST));
         server.createQueue(new QueueConfiguration(getQueueName(i)).setRoutingType(RoutingType.ANYCAST));
      }
   }

   protected void addAdditionalAcceptors(ActiveMQServer server) throws Exception {
      // None by default
   }

   protected void configureBrokerSecurity(ActiveMQServer server) {
      if (isSecurityEnabled()) {
         enableSecurity(server);
      } else {
         server.getConfiguration().setSecurityEnabled(false);
      }
   }

   protected void enableSecurity(ActiveMQServer server, String... securityMatches) {
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();

      // User additions
      securityManager.getConfiguration().addUser(noprivUser, noprivPass);
      securityManager.getConfiguration().addRole(noprivUser, "nothing");
      securityManager.getConfiguration().addUser(browseUser, browsePass);
      securityManager.getConfiguration().addRole(browseUser, "browser");
      securityManager.getConfiguration().addUser(guestUser, guestPass);
      securityManager.getConfiguration().addRole(guestUser, "guest");
      securityManager.getConfiguration().addUser(fullUser, fullPass);
      securityManager.getConfiguration().addRole(fullUser, "full");

      // Configure roles
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      HashSet<Role> value = new HashSet<>();
      value.add(new Role("nothing", false, false, false, false, false, false, false, false, false, false));
      value.add(new Role("browser", false, false, false, false, false, false, false, true, false, false));
      value.add(new Role("guest", false, true, false, false, false, false, false, true, false, false));
      value.add(new Role("full", true, true, true, true, true, true, true, true, true, true));
      securityRepository.addMatch(getQueueName(), value);

      for (String match : securityMatches) {
         securityRepository.addMatch(match, value);
      }

      server.getConfiguration().setSecurityEnabled(true);
   }

   protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
      // None by default
   }

   protected void configureAMQPAcceptorParameters(TransportConfiguration tc) {
      // None by default
   }

   public Queue getProxyToQueue(String queueName) {
      return server.locateQueue(SimpleString.toSimpleString(queueName));
   }

   public AddressInfo getProxyToAddress(String addressName) {
      return server.getAddressInfo(SimpleString.toSimpleString(addressName));
   }

   public String getTestName() {
      return getName();
   }

   public String getTopicName() {
      return getName() + "-Topic";
   }

   public String getQueueName() {
      return getName();
   }

   public String getQueueName(int index) {
      return getName() + "-" + index;
   }

   protected void sendMessagesOpenWire(String destinationName, int count, boolean durable) throws Exception {
      sendMessagesOpenWire(destinationName, count, durable, null);
   }

   protected void sendMessagesOpenWire(String destinationName,
                                       int count,
                                       boolean durable,
                                       byte[] payload) throws Exception {
      ConnectionFactory cf = new ActiveMQConnectionFactory("tcp://127.0.0.1:5672");
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try {
         MessageProducer producer = session.createProducer(session.createQueue(destinationName));
         if (durable) {
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
         } else {
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         }

         for (int i = 0; i < count; ++i) {
            BytesMessage message = session.createBytesMessage();
            if (payload != null) {
               message.writeBytes(payload);
            }
            producer.send(message);
         }
      } finally {
         connection.close();
      }
   }

   public boolean isUseSSL() {
      return useSSL;
   }

   public boolean isUseWebSockets() {
      return useWebSockets;
   }

}
