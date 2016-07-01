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
package org.apache.activemq.artemis.tests.integration.jms.server.management;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSConnectionInfo;
import org.apache.activemq.artemis.api.jms.management.JMSConsumerInfo;
import org.apache.activemq.artemis.api.jms.management.JMSServerControl;
import org.apache.activemq.artemis.api.jms.management.JMSSessionInfo;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQMessage;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.ra.ActiveMQResourceAdapter;
import org.apache.activemq.artemis.ra.inflow.ActiveMQActivation;
import org.apache.activemq.artemis.ra.inflow.ActiveMQActivationSpec;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.integration.management.ManagementTestBase;
import org.apache.activemq.artemis.tests.unit.ra.BootstrapContext;
import org.apache.activemq.artemis.tests.unit.ra.MessageEndpointFactory;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class JMSServerControl2Test extends ManagementTestBase {

   private static final long CONNECTION_TTL = 1000;

   private static final long PING_PERIOD = JMSServerControl2Test.CONNECTION_TTL / 2;

   private ActiveMQServer server;

   private JMSServerManagerImpl serverManager;

   private InVMNamingContext context;

   // Static --------------------------------------------------------

   private void startActiveMQServer(final String acceptorFactory) throws Exception {
      Configuration config = createBasicConfig().addAcceptorConfiguration(new TransportConfiguration(acceptorFactory));
      server = addServer(ActiveMQServers.newActiveMQServer(config, mbeanServer, true));
      server.start();

      context = new InVMNamingContext();
      serverManager = new JMSServerManagerImpl(server);
      addActiveMQComponent(serverManager);
      serverManager.setRegistry(new JndiBindingRegistry(context));
      serverManager.start();
      serverManager.activated();
   }

   @Test
   public void testListClientConnectionsForInVM() throws Exception {
      doListClientConnections(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   @Test
   public void testListClientConnectionsForNetty() throws Exception {
      doListClientConnections(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   @Test
   public void testCloseConnectionsForAddressForInVM() throws Exception {
      doCloseConnectionsForAddress(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   @Test
   public void testCloseConnectionsForAddressForNetty() throws Exception {
      doCloseConnectionsForAddress(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   @Test
   public void testCloseConnectionsForUnknownAddressForInVM() throws Exception {
      doCloseConnectionsForUnknownAddress(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   @Test
   public void testCloseConnectionsForUnknownAddressForNetty() throws Exception {
      doCloseConnectionsForUnknownAddress(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   @Test
   public void testCloseConsumerConnectionsForAddressForInVM() throws Exception {
      doCloseConsumerConnectionsForAddress(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   @Test
   public void testCloseConsumerConnectionsForAddressForNetty() throws Exception {
      doCloseConsumerConnectionsForAddress(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   @Test
   public void testCloseConsumerConnectionsForWildcardAddressForInVM() throws Exception {
      doCloseConsumerConnectionsForWildcardAddress(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   @Test
   public void testCloseConsumerConnectionsForWildcardAddressForNetty() throws Exception {
      doCloseConsumerConnectionsForWildcardAddress(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   @Test
   public void testCloseConnectionsForUserForInVM() throws Exception {
      doCloseConnectionsForUser(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   @Test
   public void testCloseConnectionsForUserForNetty() throws Exception {
      doCloseConnectionsForUser(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   @Test
   public void testListSessionsForInVM() throws Exception {
      doListSessions(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   @Test
   public void testListSessionsForNetty() throws Exception {
      doListSessions(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   @Test
   public void testListConnectionIDsForInVM() throws Exception {
      doListConnectionIDs(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   @Test
   public void testListConnectionIDsForNetty() throws Exception {
      doListConnectionIDs(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   @Test
   public void testListConnectionsAsJSONForNetty() throws Exception {
      doListConnectionsAsJSON(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   @Test
   public void testListConnectionsAsJSONForInVM() throws Exception {
      doListConnectionsAsJSON(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   @Test
   public void testListConsumersAsJSON() throws Exception {
      String queueName = RandomUtil.randomString();

      try {
         startActiveMQServer(NETTY_ACCEPTOR_FACTORY);
         serverManager.createQueue(false, queueName, null, true, queueName);
         Queue queue = ActiveMQJMSClient.createQueue(queueName);

         JMSServerControl control = createManagementControl();

         long startTime = System.currentTimeMillis();

         String jsonStr = control.listConnectionsAsJSON();
         assertNotNull(jsonStr);
         JMSConnectionInfo[] infos = JMSConnectionInfo.from(jsonStr);
         assertEquals(0, infos.length);

         ConnectionFactory cf1 = JMSUtil.createFactory(NettyConnectorFactory.class.getName(), JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection = cf1.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         TemporaryTopic temporaryTopic = session.createTemporaryTopic();

         // create a regular message consumer
         MessageConsumer consumer = session.createConsumer(queue);

         jsonStr = control.listConnectionsAsJSON();
         assertNotNull(jsonStr);
         infos = JMSConnectionInfo.from(jsonStr);
         assertEquals(1, infos.length);
         String connectionID = infos[0].getConnectionID();

         String consJsonStr = control.listConsumersAsJSON(connectionID);
         assertNotNull(consJsonStr);
         JMSConsumerInfo[] consumerInfos = JMSConsumerInfo.from(consJsonStr);
         assertEquals(1, consumerInfos.length);
         JMSConsumerInfo consumerInfo = consumerInfos[0];
         assertNotNull(consumerInfo.getConsumerID());
         assertEquals(connectionID, consumerInfo.getConnectionID());
         assertEquals(queue.getQueueName(), consumerInfo.getDestinationName());
         assertEquals("queue", consumerInfo.getDestinationType());
         assertNull(consumerInfo.getFilter());
         assertEquals(false, consumerInfo.isBrowseOnly());
         assertEquals(false, consumerInfo.isDurable());
         assertTrue(startTime <= consumerInfo.getCreationTime() && consumerInfo.getCreationTime() <= System.currentTimeMillis());

         consumer.close();

         consJsonStr = control.listConsumersAsJSON(connectionID);
         assertNotNull(consJsonStr);
         consumerInfos = JMSConsumerInfo.from(consJsonStr);
         assertEquals(0, consumerInfos.length);

         // create a queue browser
         QueueBrowser browser = session.createBrowser(queue);
         // the server resources are created when the browser starts enumerating
         browser.getEnumeration();

         consJsonStr = control.listConsumersAsJSON(connectionID);
         assertNotNull(consJsonStr);
         System.out.println(consJsonStr);
         consumerInfos = JMSConsumerInfo.from(consJsonStr);
         assertEquals(1, consumerInfos.length);
         consumerInfo = consumerInfos[0];
         assertNotNull(consumerInfo.getConsumerID());
         assertEquals(connectionID, consumerInfo.getConnectionID());
         assertEquals(queue.getQueueName(), consumerInfo.getDestinationName());
         assertEquals("queue", consumerInfo.getDestinationType());
         assertNull(consumerInfo.getFilter());
         assertEquals(true, consumerInfo.isBrowseOnly());
         assertEquals(false, consumerInfo.isDurable());
         assertTrue(startTime <= consumerInfo.getCreationTime() && consumerInfo.getCreationTime() <= System.currentTimeMillis());

         browser.close();

         // create a regular consumer w/ filter on a temp topic
         String filter = "color = 'red'";
         consumer = session.createConsumer(temporaryTopic, filter);

         consJsonStr = control.listConsumersAsJSON(connectionID);
         assertNotNull(consJsonStr);
         System.out.println(consJsonStr);
         consumerInfos = JMSConsumerInfo.from(consJsonStr);
         assertEquals(1, consumerInfos.length);
         consumerInfo = consumerInfos[0];
         assertNotNull(consumerInfo.getConsumerID());
         assertEquals(connectionID, consumerInfo.getConnectionID());
         assertEquals(temporaryTopic.getTopicName(), consumerInfo.getDestinationName());
         assertEquals("temptopic", consumerInfo.getDestinationType());
         assertEquals(filter, consumerInfo.getFilter());
         assertEquals(false, consumerInfo.isBrowseOnly());
         assertEquals(false, consumerInfo.isDurable());
         assertTrue(startTime <= consumerInfo.getCreationTime() && consumerInfo.getCreationTime() <= System.currentTimeMillis());

         consumer.close();

         connection.close();
      }
      finally {
         if (serverManager != null) {
            serverManager.destroyQueue(queueName);
            serverManager.stop();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   /**
    * test for durable subscriber
    */
   @Test
   public void testListConsumersAsJSON2() throws Exception {
      String topicName = RandomUtil.randomString();
      String clientID = RandomUtil.randomString();
      String subName = RandomUtil.randomString();

      try {
         startActiveMQServer(NettyAcceptorFactory.class.getName());
         serverManager.createTopic(false, topicName, topicName);
         Topic topic = ActiveMQJMSClient.createTopic(topicName);

         JMSServerControl control = createManagementControl();

         long startTime = System.currentTimeMillis();

         String jsonStr = control.listConnectionsAsJSON();
         assertNotNull(jsonStr);
         JMSConnectionInfo[] infos = JMSConnectionInfo.from(jsonStr);
         assertEquals(0, infos.length);

         ConnectionFactory cf1 = JMSUtil.createFactory(NettyConnectorFactory.class.getName(), JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection = cf1.createConnection();
         connection.setClientID(clientID);
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // create a durable subscriber
         MessageConsumer consumer = session.createDurableSubscriber(topic, subName);

         jsonStr = control.listConnectionsAsJSON();
         assertNotNull(jsonStr);
         infos = JMSConnectionInfo.from(jsonStr);
         assertEquals(1, infos.length);
         String connectionID = infos[0].getConnectionID();

         String consJsonStr = control.listConsumersAsJSON(connectionID);
         assertNotNull(consJsonStr);
         JMSConsumerInfo[] consumerInfos = JMSConsumerInfo.from(consJsonStr);
         assertEquals(1, consumerInfos.length);
         JMSConsumerInfo consumerInfo = consumerInfos[0];
         assertNotNull(consumerInfo.getConsumerID());
         assertEquals(connectionID, consumerInfo.getConnectionID());
         assertEquals(topic.getTopicName(), consumerInfo.getDestinationName());
         assertEquals("topic", consumerInfo.getDestinationType());
         assertNull(consumerInfo.getFilter());
         assertEquals(false, consumerInfo.isBrowseOnly());
         assertEquals(true, consumerInfo.isDurable());
         assertTrue(startTime <= consumerInfo.getCreationTime() && consumerInfo.getCreationTime() <= System.currentTimeMillis());

         consumer.close();

         connection.close();
      }
      finally {
         if (serverManager != null) {
            serverManager.destroyTopic(topicName);
            serverManager.stop();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   //https://jira.jboss.org/browse/HORNETQ-416
   @Test
   public void testProducerInfo() throws Exception {
      String queueName = RandomUtil.randomString();

      System.out.println("queueName is: " + queueName);

      Connection connection = null;

      try {
         startActiveMQServer(NettyAcceptorFactory.class.getName());
         serverManager.createQueue(false, queueName, null, true, queueName);
         Queue queue = ActiveMQJMSClient.createQueue(queueName);

         JMSServerControl control = createManagementControl();

         long startTime = System.currentTimeMillis();

         ConnectionFactory cf1 = JMSUtil.createFactory(NettyConnectorFactory.class.getName(), JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         connection = cf1.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(queue);

         TextMessage msgSent = null;
         for (int i = 0; i < 10; i++) {
            msgSent = session.createTextMessage("mymessage-" + i);
            producer.send(msgSent);
            System.out.println("sending msgID " + msgSent.getJMSMessageID());
         }

         connection.start();

         // create a regular message consumer
         MessageConsumer consumer = session.createConsumer(queue);

         TextMessage receivedMsg = null;
         for (int i = 0; i < 10; i++) {
            receivedMsg = (TextMessage) consumer.receive(3000);
            assertNotNull(receivedMsg);
         }

         assertEquals(msgSent.getJMSMessageID(), receivedMsg.getJMSMessageID());

         ActiveMQMessage jmsMessage = (ActiveMQMessage) receivedMsg;
         String lastMsgID = jmsMessage.getCoreMessage().getUserID().toString();

         String jsonStr = control.listConnectionsAsJSON();
         JMSConnectionInfo[] infos = JMSConnectionInfo.from(jsonStr);

         JMSConnectionInfo connInfo = infos[0];

         String sessionsStr = control.listSessionsAsJSON(connInfo.getConnectionID());
         JMSSessionInfo[] sessInfos = JMSSessionInfo.from(sessionsStr);

         assertTrue(sessInfos.length > 0);
         boolean lastMsgFound = false;
         for (JMSSessionInfo sInfo : sessInfos) {
            System.out.println("Session name: " + sInfo.getSessionID());
            assertNotNull(sInfo.getSessionID());
            long createTime = sInfo.getCreationTime();
            assertTrue(startTime <= createTime && createTime <= System.currentTimeMillis());
            String lastID = control.getLastSentMessageID(sInfo.getSessionID(), "jms.queue." + queueName);
            if (lastID != null) {
               assertEquals(lastMsgID, lastID);
               lastMsgFound = true;
            }
         }
         assertTrue(lastMsgFound);

         consumer.close();

         connection.close();
      }
      catch (Exception e) {
         e.printStackTrace();
         throw e;
      }
      finally {
         try {
            if (connection != null) {
               connection.close();
            }

            if (serverManager != null) {
               serverManager.destroyQueue(queueName);
               serverManager.stop();
            }
         }
         catch (Throwable ignored) {
            ignored.printStackTrace();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   @Test
   public void testStartActivationListConnections() throws Exception {
      ActiveMQActivation activation = null;
      ActiveMQResourceAdapter ra = null;

      try {
         startActiveMQServer(InVMAcceptorFactory.class.getName());
         ActiveMQJMSClient.createQueue("test");
         serverManager.createQueue(false, "test", null, true, "test");

         JMSServerControl control = createManagementControl();

         ra = new ActiveMQResourceAdapter();

         ra.setConnectorClassName(INVM_CONNECTOR_FACTORY);
         ra.setUserName("userGlobal");
         ra.setPassword("passwordGlobal");
         ra.start(new BootstrapContext());
         ra.setClientID("my-client-id");
         ra.setUserName("user");
         Connection conn = ra.getDefaultActiveMQConnectionFactory().createConnection();

         conn.close();

         ActiveMQActivationSpec spec = new ActiveMQActivationSpec();

         spec.setResourceAdapter(ra);

         spec.setUseJNDI(false);

         spec.setPassword("password");

         spec.setDestinationType("javax.jms.Queue");
         spec.setDestination("test");

         spec.setMinSession(1);
         spec.setMaxSession(1);

         activation = new ActiveMQActivation(ra, new MessageEndpointFactory(), spec);

         activation.start();

         String cons = control.listConnectionsAsJSON();

         JMSConnectionInfo[] jmsConnectionInfos = JMSConnectionInfo.from(cons);

         assertEquals(1, jmsConnectionInfos.length);

         assertEquals("user", jmsConnectionInfos[0].getUsername());

         assertEquals("my-client-id", jmsConnectionInfos[0].getClientID());
      }
      finally {
         if (activation != null)
            activation.stop();

         if (ra != null)
            ra.stop();

         try {
            /*if (connection != null)
            {
               connection.close();
            }*/

            if (serverManager != null) {
               //serverManager.destroyQueue(queueName);
               serverManager.stop();
            }
         }
         catch (Throwable ignored) {
            ignored.printStackTrace();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   @Test
   public void testStartActivationOverrideListConnections() throws Exception {
      ActiveMQActivation activation = null;
      ActiveMQResourceAdapter ra = null;

      try {
         startActiveMQServer(InVMAcceptorFactory.class.getName());
         ActiveMQJMSClient.createQueue("test");
         serverManager.createQueue(false, "test", null, true, "test");

         JMSServerControl control = createManagementControl();

         ra = new ActiveMQResourceAdapter();

         ra.setConnectorClassName("org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory");
         ra.setUserName("userGlobal");
         ra.setPassword("passwordGlobal");
         ra.start(new BootstrapContext());

         Connection conn = ra.getDefaultActiveMQConnectionFactory().createConnection();

         conn.close();

         ActiveMQActivationSpec spec = new ActiveMQActivationSpec();

         spec.setResourceAdapter(ra);

         spec.setUseJNDI(false);

         spec.setClientID("my-client-id");

         spec.setUser("user");
         spec.setPassword("password");

         spec.setDestinationType("javax.jms.Queue");
         spec.setDestination("test");

         spec.setMinSession(1);
         spec.setMaxSession(1);

         activation = new ActiveMQActivation(ra, new MessageEndpointFactory(), spec);

         activation.start();

         String cons = control.listConnectionsAsJSON();

         JMSConnectionInfo[] jmsConnectionInfos = JMSConnectionInfo.from(cons);

         assertEquals(1, jmsConnectionInfos.length);

         assertEquals("user", jmsConnectionInfos[0].getUsername());

         assertEquals("my-client-id", jmsConnectionInfos[0].getClientID());
      }
      finally {
         if (activation != null)
            activation.stop();

         if (ra != null)
            ra.stop();

         try {
            /*if (connection != null)
            {
               connection.close();
            }*/

            if (serverManager != null) {
               //serverManager.destroyQueue(queueName);
               serverManager.stop();
            }
         }
         catch (Throwable ignored) {
            ignored.printStackTrace();
         }

         if (server != null) {
            server.stop();
         }
      }
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected JMSServerControl createManagementControl() throws Exception {
      return ManagementControlHelper.createJMSServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   private void doListConnectionIDs(final String acceptorFactory, final String connectorFactory) throws Exception {
      try {
         startActiveMQServer(acceptorFactory);

         JMSServerControl control = createManagementControl();

         Assert.assertEquals(0, control.listConnectionIDs().length);

         ConnectionFactory cf1 = JMSUtil.createFactory(connectorFactory, JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection = cf1.createConnection();

         String[] connectionIDs = control.listConnectionIDs();
         Assert.assertEquals(1, connectionIDs.length);

         ConnectionFactory cf2 = JMSUtil.createFactory(connectorFactory, JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection2 = cf2.createConnection();
         Assert.assertEquals(2, control.listConnectionIDs().length);

         connection.close();

         waitForConnectionIDs(1, control);

         connection2.close();

         waitForConnectionIDs(0, control);
      }
      finally {
         if (serverManager != null) {
            serverManager.stop();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   private void doListConnectionsAsJSON(final String acceptorFactory, final String connectorFactory) throws Exception {
      try {
         startActiveMQServer(acceptorFactory);

         JMSServerControl control = createManagementControl();

         long startTime = System.currentTimeMillis();

         String jsonStr = control.listConnectionsAsJSON();
         assertNotNull(jsonStr);
         JMSConnectionInfo[] infos = JMSConnectionInfo.from(jsonStr);
         assertEquals(0, infos.length);

         ConnectionFactory cf1 = JMSUtil.createFactory(connectorFactory, JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection = cf1.createConnection();

         jsonStr = control.listConnectionsAsJSON();
         assertNotNull(jsonStr);
         infos = JMSConnectionInfo.from(jsonStr);
         assertEquals(1, infos.length);
         for (JMSConnectionInfo info : infos) {
            assertNotNull(info.getConnectionID());
            assertNotNull(info.getClientAddress());
            assertTrue(startTime <= info.getCreationTime() && info.getCreationTime() <= System.currentTimeMillis());
         }

         ConnectionFactory cf2 = JMSUtil.createFactory(connectorFactory, JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection2 = cf2.createConnection();

         jsonStr = control.listConnectionsAsJSON();
         assertNotNull(jsonStr);
         infos = JMSConnectionInfo.from(jsonStr);
         assertEquals(2, infos.length);
         for (JMSConnectionInfo info : infos) {
            assertNotNull(info.getConnectionID());
            assertNotNull(info.getClientAddress());
            assertTrue(startTime <= info.getCreationTime() && info.getCreationTime() <= System.currentTimeMillis());
            assertNull(info.getClientID());
            assertNull(info.getUsername());
         }

         connection.close();

         waitForConnectionIDs(1, control);

         connection2.close();

         waitForConnectionIDs(0, control);

         Connection connection3 = cf2.createConnection("guest", "guest");
         connection3.setClientID("MyClient");

         jsonStr = control.listConnectionsAsJSON();
         assertNotNull(jsonStr);

         infos = JMSConnectionInfo.from(jsonStr);
         JMSConnectionInfo info = infos[0];
         assertEquals("MyClient", info.getClientID());
         assertEquals("guest", info.getUsername());

         connection3.close();

         jsonStr = control.listConnectionsAsJSON();
         assertNotNull(jsonStr);
         infos = JMSConnectionInfo.from(jsonStr);
         assertEquals(0, infos.length);
      }
      finally {
         if (serverManager != null) {
            serverManager.stop();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   private void waitForConnectionIDs(final int num, final JMSServerControl control) throws Exception {
      final long timeout = 10000;
      long start = System.currentTimeMillis();
      while (true) {
         if (control.listConnectionIDs().length == num) {
            return;
         }

         if (System.currentTimeMillis() - start > timeout) {
            throw new IllegalStateException("Timed out waiting for number of connections");
         }

         Thread.sleep(10);
      }
   }

   private void doListSessions(final String acceptorFactory, final String connectorFactory) throws Exception {
      try {
         startActiveMQServer(acceptorFactory);

         JMSServerControl control = createManagementControl();

         Assert.assertEquals(0, control.listConnectionIDs().length);

         ConnectionFactory cf = JMSUtil.createFactory(connectorFactory, JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection = cf.createConnection();

         String[] connectionIDs = control.listConnectionIDs();
         Assert.assertEquals(1, connectionIDs.length);
         String connectionID = connectionIDs[0];

         String[] sessions = control.listSessions(connectionID);
         Assert.assertEquals(1, sessions.length);
         connection.close();
         sessions = control.listSessions(connectionID);
         Assert.assertEquals("got " + Arrays.asList(sessions), 0, sessions.length);

         connection.close();

         waitForConnectionIDs(0, control);

         Assert.assertEquals(0, control.listConnectionIDs().length);
      }
      finally {
         if (serverManager != null) {
            serverManager.stop();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   private void doListClientConnections(final String acceptorFactory, final String connectorFactory) throws Exception {
      try {
         startActiveMQServer(acceptorFactory);

         JMSServerControl control = createManagementControl();

         Assert.assertEquals(0, control.listRemoteAddresses().length);

         ConnectionFactory cf = JMSUtil.createFactory(connectorFactory, JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection = cf.createConnection();

         String[] remoteAddresses = control.listRemoteAddresses();
         Assert.assertEquals(1, remoteAddresses.length);

         for (String remoteAddress : remoteAddresses) {
            System.out.println(remoteAddress);
         }

         connection.close();

         waitForConnectionIDs(0, control);

         remoteAddresses = control.listRemoteAddresses();
         Assert.assertEquals("got " + Arrays.asList(remoteAddresses), 0, remoteAddresses.length);
      }
      finally {
         if (serverManager != null) {
            serverManager.stop();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   private void doCloseConnectionsForAddress(final String acceptorFactory,
                                             final String connectorFactory) throws Exception {
      try {
         startActiveMQServer(acceptorFactory);

         JMSServerControl control = createManagementControl();

         Assert.assertEquals(0, server.getConnectionCount());
         Assert.assertEquals(0, control.listRemoteAddresses().length);

         ConnectionFactory cf = JMSUtil.createFactory(connectorFactory, JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection = cf.createConnection();

         Assert.assertEquals(1, server.getConnectionCount());

         String[] remoteAddresses = control.listRemoteAddresses();
         Assert.assertEquals(1, remoteAddresses.length);
         String remoteAddress = remoteAddresses[0];

         final CountDownLatch exceptionLatch = new CountDownLatch(1);
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(final JMSException e) {
               exceptionLatch.countDown();
            }
         });

         Assert.assertTrue(control.closeConnectionsForAddress(remoteAddress));

         boolean gotException = exceptionLatch.await(2 * JMSServerControl2Test.CONNECTION_TTL, TimeUnit.MILLISECONDS);
         Assert.assertTrue("did not received the expected JMSException", gotException);

         remoteAddresses = control.listRemoteAddresses();
         Assert.assertEquals("got " + Arrays.asList(remoteAddresses), 0, remoteAddresses.length);
         Assert.assertEquals(0, server.getConnectionCount());

         connection.close();
      }
      finally {
         if (serverManager != null) {
            serverManager.stop();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   private void doCloseConnectionsForUnknownAddress(final String acceptorFactory,
                                                    final String connectorFactory) throws Exception {
      String unknownAddress = RandomUtil.randomString();

      try {
         startActiveMQServer(acceptorFactory);

         JMSServerControl control = createManagementControl();

         Assert.assertEquals(0, server.getConnectionCount());
         Assert.assertEquals(0, control.listRemoteAddresses().length);

         ConnectionFactory cf = JMSUtil.createFactory(connectorFactory, JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection = cf.createConnection();

         Assert.assertEquals(1, server.getConnectionCount());
         String[] remoteAddresses = control.listRemoteAddresses();
         Assert.assertEquals(1, remoteAddresses.length);

         final CountDownLatch exceptionLatch = new CountDownLatch(1);
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(final JMSException e) {
               exceptionLatch.countDown();
            }
         });

         Assert.assertFalse(control.closeConnectionsForAddress(unknownAddress));

         boolean gotException = exceptionLatch.await(2 * JMSServerControl2Test.CONNECTION_TTL, TimeUnit.MILLISECONDS);
         Assert.assertFalse(gotException);

         Assert.assertEquals(1, control.listRemoteAddresses().length);
         Assert.assertEquals(1, server.getConnectionCount());

         connection.close();

      }
      finally {
         if (serverManager != null) {
            serverManager.stop();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   private void doCloseConsumerConnectionsForAddress(final String acceptorFactory,
                                                     final String connectorFactory) throws Exception {
      String queueName = RandomUtil.randomString();
      String queueName2 = RandomUtil.randomString();

      try {
         startActiveMQServer(acceptorFactory);
         serverManager.createQueue(false, queueName, null, true, queueName);
         Queue queue = ActiveMQJMSClient.createQueue(queueName);
         serverManager.createQueue(false, queueName2, null, true, queueName2);
         Queue queue2 = ActiveMQJMSClient.createQueue(queueName2);

         JMSServerControl control = createManagementControl();
         QueueControl queueControl = createManagementControl("jms.queue." + queueName, "jms.queue." + queueName);
         QueueControl queueControl2 = createManagementControl("jms.queue." + queueName2, "jms.queue." + queueName2);

         Assert.assertEquals(0, server.getConnectionCount());
         Assert.assertEquals(0, control.listRemoteAddresses().length);
         Assert.assertEquals(0, queueControl.getConsumerCount());
         Assert.assertEquals(0, queueControl2.getConsumerCount());

         ConnectionFactory cf = JMSUtil.createFactory(connectorFactory, JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection = cf.createConnection();
         Session session = connection.createSession();
         MessageConsumer messageConsumer = session.createConsumer(queue);

         Connection connection2 = cf.createConnection();
         Session session2 = connection2.createSession();
         MessageConsumer messageConsumer2 = session2.createConsumer(queue2);

         Assert.assertEquals(2, server.getConnectionCount());

         String[] remoteAddresses = control.listRemoteAddresses();
         Assert.assertEquals(2, remoteAddresses.length);

         Assert.assertEquals(1, queueControl.getConsumerCount());
         Assert.assertEquals(1, queueControl2.getConsumerCount());

         final CountDownLatch exceptionLatch = new CountDownLatch(1);
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(final JMSException e) {
               exceptionLatch.countDown();
            }
         });

         Assert.assertTrue(control.closeConsumerConnectionsForAddress("jms.queue." + queueName));

         boolean gotException = exceptionLatch.await(2 * JMSServerControl2Test.CONNECTION_TTL, TimeUnit.MILLISECONDS);
         Assert.assertTrue("did not received the expected JMSException", gotException);

         remoteAddresses = control.listRemoteAddresses();
         Assert.assertEquals("got " + Arrays.asList(remoteAddresses), 1, remoteAddresses.length);
         Assert.assertEquals(1, server.getConnectionCount());

         Assert.assertEquals(0, queueControl.getConsumerCount());
         Assert.assertEquals(1, queueControl2.getConsumerCount());

         connection.close();
         connection2.close();
      }
      finally {
         if (serverManager != null) {
            serverManager.stop();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   private void doCloseConsumerConnectionsForWildcardAddress(final String acceptorFactory,
                                                             final String connectorFactory) throws Exception {
      String queueName1 = "x." + RandomUtil.randomString();
      String queueName2 = "x." + RandomUtil.randomString();
      String queueName3 = "y." + RandomUtil.randomString();

      try {
         startActiveMQServer(acceptorFactory);
         serverManager.createQueue(false, queueName1, null, true, queueName1);
         Queue queue = ActiveMQJMSClient.createQueue(queueName1);
         serverManager.createQueue(false, queueName2, null, true, queueName2);
         Queue queue2 = ActiveMQJMSClient.createQueue(queueName2);
         serverManager.createQueue(false, queueName3, null, true, queueName3);
         Queue queue3 = ActiveMQJMSClient.createQueue(queueName3);

         JMSServerControl control = createManagementControl();
         QueueControl queueControl = createManagementControl("jms.queue." + queueName1, "jms.queue." + queueName1);
         QueueControl queueControl2 = createManagementControl("jms.queue." + queueName2, "jms.queue." + queueName2);
         QueueControl queueControl3 = createManagementControl("jms.queue." + queueName3, "jms.queue." + queueName3);

         Assert.assertEquals(0, server.getConnectionCount());
         Assert.assertEquals(0, control.listRemoteAddresses().length);
         Assert.assertEquals(0, queueControl.getConsumerCount());
         Assert.assertEquals(0, queueControl2.getConsumerCount());
         Assert.assertEquals(0, queueControl3.getConsumerCount());

         ConnectionFactory cf = JMSUtil.createFactory(connectorFactory, JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection = cf.createConnection();
         Session session = connection.createSession();
         MessageConsumer messageConsumer = session.createConsumer(queue);

         Connection connection2 = cf.createConnection();
         Session session2 = connection2.createSession();
         MessageConsumer messageConsumer2 = session2.createConsumer(queue2);

         Connection connection3 = cf.createConnection();
         Session session3 = connection3.createSession();
         MessageConsumer messageConsumer3 = session3.createConsumer(queue3);

         Assert.assertEquals(3, server.getConnectionCount());

         String[] remoteAddresses = control.listRemoteAddresses();
         Assert.assertEquals(3, remoteAddresses.length);

         Assert.assertEquals(1, queueControl.getConsumerCount());
         Assert.assertEquals(1, queueControl2.getConsumerCount());
         Assert.assertEquals(1, queueControl3.getConsumerCount());

         final CountDownLatch exceptionLatch = new CountDownLatch(2);
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(final JMSException e) {
               exceptionLatch.countDown();
            }
         });

         connection2.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(final JMSException e) {
               exceptionLatch.countDown();
            }
         });

         Assert.assertTrue(control.closeConsumerConnectionsForAddress("jms.queue.x.#"));

         boolean gotException = exceptionLatch.await(2 * JMSServerControl2Test.CONNECTION_TTL, TimeUnit.MILLISECONDS);
         Assert.assertTrue("did not received the expected JMSException", gotException);

         remoteAddresses = control.listRemoteAddresses();
         Assert.assertEquals("got " + Arrays.asList(remoteAddresses), 1, remoteAddresses.length);
         Assert.assertEquals(1, server.getConnectionCount());

         Assert.assertEquals(0, queueControl.getConsumerCount());
         Assert.assertEquals(0, queueControl2.getConsumerCount());
         Assert.assertEquals(1, queueControl3.getConsumerCount());

         connection.close();
         connection2.close();
         connection3.close();
      }
      finally {
         if (serverManager != null) {
            serverManager.stop();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   private void doCloseConnectionsForUser(final String acceptorFactory,
                                          final String connectorFactory) throws Exception {
      String queueName = RandomUtil.randomString();
      String queueName2 = RandomUtil.randomString();

      try {
         startActiveMQServer(acceptorFactory);
         serverManager.createQueue(false, queueName, null, true, queueName);
         Queue queue = ActiveMQJMSClient.createQueue(queueName);
         serverManager.createQueue(false, queueName2, null, true, queueName2);
         Queue queue2 = ActiveMQJMSClient.createQueue(queueName2);

         JMSServerControl control = createManagementControl();
         QueueControl queueControl = createManagementControl("jms.queue." + queueName, "jms.queue." + queueName);
         QueueControl queueControl2 = createManagementControl("jms.queue." + queueName2, "jms.queue." + queueName2);

         Assert.assertEquals(0, server.getConnectionCount());
         Assert.assertEquals(0, control.listRemoteAddresses().length);
         Assert.assertEquals(0, queueControl.getConsumerCount());
         Assert.assertEquals(0, queueControl2.getConsumerCount());

         ConnectionFactory cf = JMSUtil.createFactory(connectorFactory, JMSServerControl2Test.CONNECTION_TTL, JMSServerControl2Test.PING_PERIOD);
         Connection connection = cf.createConnection("fakeUser", "fakePassword");
         Session session = connection.createSession();
         MessageConsumer messageConsumer = session.createConsumer(queue);

         Connection connection2 = cf.createConnection();
         Session session2 = connection2.createSession();
         MessageConsumer messageConsumer2 = session2.createConsumer(queue2);

         Assert.assertEquals(2, server.getConnectionCount());

         String[] remoteAddresses = control.listRemoteAddresses();
         Assert.assertEquals(2, remoteAddresses.length);

         Assert.assertEquals(1, queueControl.getConsumerCount());
         Assert.assertEquals(1, queueControl2.getConsumerCount());

         final CountDownLatch exceptionLatch = new CountDownLatch(1);
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(final JMSException e) {
               exceptionLatch.countDown();
            }
         });

         Assert.assertTrue(control.closeConnectionsForUser("fakeUser"));

         boolean gotException = exceptionLatch.await(2 * JMSServerControl2Test.CONNECTION_TTL, TimeUnit.MILLISECONDS);
         Assert.assertTrue("did not received the expected JMSException", gotException);

         remoteAddresses = control.listRemoteAddresses();
         Assert.assertEquals("got " + Arrays.asList(remoteAddresses), 1, remoteAddresses.length);
         Assert.assertEquals(1, server.getConnectionCount());

         Assert.assertEquals(0, queueControl.getConsumerCount());
         Assert.assertEquals(1, queueControl2.getConsumerCount());

         connection.close();
         connection2.close();
      }
      finally {
         if (serverManager != null) {
            serverManager.stop();
         }

         if (server != null) {
            server.stop();
         }
      }
   }

   // Inner classes -------------------------------------------------

}