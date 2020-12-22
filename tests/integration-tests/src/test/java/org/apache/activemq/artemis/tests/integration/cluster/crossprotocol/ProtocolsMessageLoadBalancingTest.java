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
package org.apache.activemq.artemis.tests.integration.cluster.crossprotocol;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireProtocolManagerFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.MessageFlowRecord;
import org.apache.activemq.artemis.core.server.cluster.impl.ClusterConnectionImpl;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.utils.RetryRule;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class ProtocolsMessageLoadBalancingTest extends ClusterTestBase {

   @Rule
   public RetryRule retryRule = new RetryRule(2);

   private static final int NUMBER_OF_SERVERS = 2;
   private static final SimpleString queueName = SimpleString.toSimpleString("queues.0");

   // I'm taking any number that /2 = Odd
   // to avoid perfect roundings and making sure messages are evenly distributed
   private static final int NUMBER_OF_MESSAGES = 77 * 2;

   @Parameterized.Parameters(name = "protocol={0}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{"AMQP"}, {"CORE"}, {"OPENWIRE"}});
   }

   @Parameterized.Parameter(0)
   public String protocol;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

   }

   private void startServers(MessageLoadBalancingType loadBalancingType) throws Exception {
      setupServers();

      setRedistributionDelay(0);

      setupCluster(loadBalancingType);

      AddressSettings as = new AddressSettings().setRedistributionDelay(0).setExpiryAddress(SimpleString.toSimpleString("queues.expiry"));

      getServer(0).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(1).getAddressSettingsRepository().addMatch("queues.*", as);

      startServers(0);
      startServers(1);

      createQueue(SimpleString.toSimpleString("queues.expiry"));
      createQueue(queueName);
   }

   private void createQueue(SimpleString queueName) throws Exception {
      QueueConfiguration queueConfiguration = new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST);
      servers[0].createQueue(queueConfiguration);
      servers[1].createQueue(queueConfiguration);
   }

   protected boolean isNetty() {
      return true;
   }

   private ConnectionFactory getJmsConnectionFactory(int node) {
      if (protocol.equals("AMQP")) {
         return new JmsConnectionFactory("amqp://localhost:" + (61616 + node));
      } else if (protocol.equals("OPENWIRE")) {
         return new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:" + (61616 + node));
      } else if (protocol.equals("CORE")) {
         return new ActiveMQConnectionFactory("tcp://localhost:" + (61616 + node));
      } else {
         Assert.fail("Protocol " + protocol + " unkown");
         return null;
      }
   }

   private void pauseClusteringBridges(ActiveMQServer server) throws Exception {
      for (ClusterConnection clusterConnection : server.getClusterManager().getClusterConnections()) {
         for (MessageFlowRecord record : ((ClusterConnectionImpl) clusterConnection).getRecords().values()) {
            record.getBridge().pause();
         }
      }
   }

   @Test
   public void testLoadBalancing() throws Exception {

      startServers(MessageLoadBalancingType.STRICT);

      ConnectionFactory[] factory = new ConnectionFactory[NUMBER_OF_SERVERS];
      Connection[] connection = new Connection[NUMBER_OF_SERVERS];
      Session[] session = new Session[NUMBER_OF_SERVERS];
      MessageConsumer[] consumer = new MessageConsumer[NUMBER_OF_SERVERS];

      // this will pre create consumers to make sure messages are distributed evenly without redistribution
      for (int node = 0; node < NUMBER_OF_SERVERS; node++) {
         factory[node] = getJmsConnectionFactory(node);
         connection[node] = factory[node].createConnection();
         session[node] = connection[node].createSession(false, Session.AUTO_ACKNOWLEDGE);
         consumer[node] = session[node].createConsumer(session[node].createQueue(queueName.toString()));
      }

      waitForBindings(0, "queues.0", 1, 1, true);
      waitForBindings(1, "queues.0", 1, 1, true);

      waitForBindings(0, "queues.0", 1, 1, false);
      waitForBindings(1, "queues.0", 1, 1, false);

      pauseClusteringBridges(servers[0]);

      // sending Messages.. they should be load balanced
      {
         ConnectionFactory cf = getJmsConnectionFactory(0);
         Connection cn = cf.createConnection();
         Session sn = cn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer pd = sn.createProducer(sn.createQueue(queueName.toString()));

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            pd.send(sn.createTextMessage("hello " + i));
         }

         cn.close();
      }

      receiveMessages(connection[0], consumer[0], NUMBER_OF_MESSAGES / 2, true);
      connection[1].start();
      Assert.assertNull(consumer[1].receiveNoWait());
      connection[1].stop();

      servers[0].stop();
      clearServer(0);

      setupServer(0, isFileStorage(), isNetty());
      servers[0].addProtocolManagerFactory(new ProtonProtocolManagerFactory());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.STRICT, 1, isNetty(), 0, 1);

      servers[0].start();

      receiveMessages(connection[1], consumer[1], NUMBER_OF_MESSAGES / 2, true);
      for (int node = 0; node < NUMBER_OF_SERVERS; node++) {
         connection[node].close();
      }

   }

   @Test
   public void testRedistributionStoppedWithNoRemoteConsumers() throws Exception {
      startServers(MessageLoadBalancingType.ON_DEMAND);

      ConnectionFactory factory = getJmsConnectionFactory(1);

      // Wait for cluster nodes to sync
      waitForBindings(0, "queues.0", 1, 0, true);
      waitForBindings(1, "queues.0", 1, 0, true);

      waitForBindings(0, "queues.0", 1, 0, false);
      waitForBindings(1, "queues.0", 1, 0, false);

      // Create CFs
      ConnectionFactory cf0 = getJmsConnectionFactory(0);
      ConnectionFactory cf1 = getJmsConnectionFactory(1);

      // Create Consumers
      Connection cn0 = cf0.createConnection();
      Session sn0 = cn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer c0 = sn0.createConsumer(sn0.createQueue(queueName.toString()));
      cn0.start();

      Connection cn1 = cf1.createConnection();
      Session sn1 = cn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer c1 = sn1.createConsumer(sn0.createQueue(queueName.toString()));
      cn1.start();

      MessageProducer pd = sn0.createProducer(sn0.createQueue(queueName.toString()));

      // Wait for cluster nodes to sync consumer count
      waitForBindings(0, "queues.0", 1, 1, false);
      waitForBindings(1, "queues.0", 1, 1, false);

      // Start queue redistributor
      c0.close();

      // Send Messages to node1.
      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         pd.send(sn0.createTextMessage("hello " + i));
      }

      // Ensure the messages are redistributed from node0 to node1.
      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         assertNotNull(c1.receive(1000));
      }

      // Close client on node1.  This should make the node0 stop redistributing.
      c1.close();
      sn1.close();
      cn1.close();

      // Send more messages (these should stay in node0)
      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         pd.send(sn0.createTextMessage("hello " + i));
      }

      cn0.close();

      // Messages should stay in node 1 and note get redistributed.
      assertEquals(NUMBER_OF_MESSAGES, servers[0].locateQueue(queueName).getMessageCount());
      assertEquals(0, servers[1].locateQueue(queueName).getMessageCount());
   }

   @Test
   public void testExpireRedistributed() throws Exception {
      startServers(MessageLoadBalancingType.ON_DEMAND);

      ConnectionFactory factory = getJmsConnectionFactory(1);

      waitForBindings(0, "queues.0", 1, 0, true);
      waitForBindings(1, "queues.0", 1, 0, true);

      waitForBindings(0, "queues.0", 1, 0, false);
      waitForBindings(1, "queues.0", 1, 0, false);

      // sending Messages..
      {
         ConnectionFactory cf = getJmsConnectionFactory(0);
         Connection cn = cf.createConnection();
         Session sn = cn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer pd = sn.createProducer(sn.createQueue(queueName.toString()));
         pd.setTimeToLive(200);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            pd.send(sn.createTextMessage("hello " + i));
         }

         cn.close();
      }

      // time to let stuff expire
      Thread.sleep(200);

      Connection connection = factory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(session.createQueue("queues.expiry"));

      receiveMessages(connection, consumer, NUMBER_OF_MESSAGES, true);
      connection.close();
   }

   @Test
   public void testRestartConnection() throws Exception {

      startServers(MessageLoadBalancingType.STRICT);

      instanceLog.debug("connections " + servers[1].getRemotingService().getConnections().size());

      Wait.assertEquals(3, () -> servers[1].getRemotingService().getConnections().size());
      Wait.assertEquals(3, () -> servers[0].getRemotingService().getConnections().size());

      RemotingConnection[] connectionsServer1 = servers[1].getRemotingService().getConnections().toArray(new RemotingConnection[3]);
      RemotingConnection[] connectionsServer0 = servers[0].getRemotingService().getConnections().toArray(new RemotingConnection[3]);

      ConnectionFactory[] factory = new ConnectionFactory[NUMBER_OF_SERVERS];
      Connection[] connection = new Connection[NUMBER_OF_SERVERS];
      Session[] session = new Session[NUMBER_OF_SERVERS];
      MessageConsumer[] consumer = new MessageConsumer[NUMBER_OF_SERVERS];

      // this will pre create consumers to make sure messages are distributed evenly without redistribution
      for (int node = 0; node < NUMBER_OF_SERVERS; node++) {
         factory[node] = getJmsConnectionFactory(node);
         connection[node] = factory[node].createConnection();
         session[node] = connection[node].createSession(false, Session.AUTO_ACKNOWLEDGE);
         consumer[node] = session[node].createConsumer(session[node].createQueue(queueName.toString()));
      }

      waitForBindings(0, "queues.0", 1, 1, true);
      waitForBindings(1, "queues.0", 1, 1, true);

      waitForBindings(0, "queues.0", 1, 1, false);
      waitForBindings(1, "queues.0", 1, 1, false);

      for (RemotingConnection remotingConnection : servers[1].getRemotingService().getConnections()) {
         remotingConnection.fail(new ActiveMQException("forcing failure"));
      }
      for (RemotingConnection remotingConnection : servers[1].getRemotingService().getConnections()) {
         remotingConnection.fail(new ActiveMQException("forcing failure"));
      }

      // this is to allow reconnects
      Thread.sleep(500);

      // this will pre create consumers to make sure messages are distributed evenly without redistribution
      for (int node = 0; node < NUMBER_OF_SERVERS; node++) {
         try {
            connection[node].close();
         } catch (Throwable e) {
            e.printStackTrace();
         }
         factory[node] = getJmsConnectionFactory(node);
         connection[node] = factory[node].createConnection();
         session[node] = connection[node].createSession(false, Session.AUTO_ACKNOWLEDGE);
         consumer[node] = session[node].createConsumer(session[node].createQueue(queueName.toString()));
      }

      waitForBindings(0, "queues.0", 1, 1, true);
      waitForBindings(1, "queues.0", 1, 1, true);

      waitForBindings(0, "queues.0", 1, 1, false);
      waitForBindings(1, "queues.0", 1, 1, false);

      instanceLog.debug("connections " + servers[1].getRemotingService().getConnections().size());

      // sending Messages.. they should be load balanced
      {
         ConnectionFactory cf = getJmsConnectionFactory(0);
         Connection cn = cf.createConnection();
         Session sn = cn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer pd = sn.createProducer(sn.createQueue(queueName.toString()));

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            pd.send(sn.createTextMessage("hello " + i));
         }

         cn.close();
      }

      receiveMessages(connection[0], consumer[0], NUMBER_OF_MESSAGES / 2, true);
      receiveMessages(connection[1], consumer[1], NUMBER_OF_MESSAGES / 2, true);

      for (int node = 0; node < NUMBER_OF_SERVERS; node++) {
         connection[node].close();
      }

   }

   @Test
   public void testRedistributeAfterLoadBalanced() throws Exception {

      startServers(MessageLoadBalancingType.ON_DEMAND);

      ConnectionFactory[] factory = new ConnectionFactory[NUMBER_OF_SERVERS];
      Connection[] connection = new Connection[NUMBER_OF_SERVERS];
      Session[]  session = new Session[NUMBER_OF_SERVERS];
      MessageConsumer[]  consumer = new MessageConsumer[NUMBER_OF_SERVERS];

      // this will pre create consumers to make sure messages are distributed evenly without redistribution
      for (int node = 0; node < NUMBER_OF_SERVERS; node++) {
         factory[node] = getJmsConnectionFactory(node);
         connection[node] = factory[node].createConnection();
         session[node] = connection[node].createSession(false, Session.AUTO_ACKNOWLEDGE);
         consumer[node] = session[node].createConsumer(session[node].createQueue(queueName.toString()));
      }

      waitForBindings(0, "queues.0", 1, 1, true);
      waitForBindings(1, "queues.0", 1, 1, true);

      waitForBindings(0, "queues.0", 1, 1, false);
      waitForBindings(1, "queues.0", 1, 1, false);



      if (protocol.equals("AMQP")) {


         ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616");
         locator.setMinLargeMessageSize(1024);
         ClientSessionFactory coreFactory = locator.createSessionFactory();
         ClientSession clientSession = coreFactory.createSession();
         ClientProducer producer = clientSession.createProducer(queueName);
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            ClientMessage message = clientSession.createMessage((byte)0, true);
            StringBuffer stringbuffer = new StringBuffer();
            stringbuffer.append("hello");
            if (i % 3 == 0) {
               // making 1/3 of the messages to be large message
               for (int j = 0; j < 10 * 1024; j++) {
                  stringbuffer.append(" ");
               }
            }
            message.getBodyBuffer().writeUTF(stringbuffer.toString());
            producer.send(message);
         }
         coreFactory.close();

      } else {

         // sending Messages.. they should be load balanced
         ConnectionFactory cf = getJmsConnectionFactory(0);
         Connection cn = cf.createConnection();
         Session sn = cn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer pd = sn.createProducer(sn.createQueue(queueName.toString()));

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            StringBuffer stringbuffer = new StringBuffer();
            stringbuffer.append("hello");
            if (i % 3 == 0) {
               // making 1/3 of the messages to be large message
               for (int j = 0; j < 300 * 1024; j++) {
                  stringbuffer.append(" ");
               }
            }
            pd.send(sn.createTextMessage(stringbuffer.toString()));
         }

         cn.close();
      }

      Wait.assertEquals(NUMBER_OF_MESSAGES / 2, servers[1].locateQueue(queueName)::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES / 2, servers[0].locateQueue(queueName)::getMessageCount);
      receiveMessages(connection[0], consumer[0], NUMBER_OF_MESSAGES / 2, true);
      connection[1].close();
      // this wil be after redistribution
      receiveMessages(connection[0], consumer[0], NUMBER_OF_MESSAGES / 2, true);
      connection[0].close();
   }


   private void receiveMessages(Connection connection,
                                MessageConsumer messageConsumer,
                                int messageCount,
                                boolean exactCount) throws JMSException {
      connection.start();

      for (int i = 0; i < messageCount; i++) {
         Message msg = messageConsumer.receive(5000);
         Assert.assertNotNull(msg);
      }

      // this means no more messages received
      if (exactCount) {
         Assert.assertNull(messageConsumer.receiveNoWait());
      }
   }

   protected void setupCluster(final MessageLoadBalancingType messageLoadBalancingType) throws Exception {
      setupClusterConnection("cluster0", "queues", messageLoadBalancingType, 1, isNetty(), 0, 1);

      setupClusterConnection("cluster1", "queues", messageLoadBalancingType, 1, isNetty(), 1, 0);
   }

   protected void setRedistributionDelay(final long delay) {
   }

   protected void setupServers() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());

      servers[0].addProtocolManagerFactory(new ProtonProtocolManagerFactory());
      servers[1].addProtocolManagerFactory(new ProtonProtocolManagerFactory());
      servers[0].addProtocolManagerFactory(new OpenWireProtocolManagerFactory());
      servers[1].addProtocolManagerFactory(new OpenWireProtocolManagerFactory());
   }

   protected void stopServers() throws Exception {
      closeAllConsumers();

      closeAllSessionFactories();

      closeAllServerLocatorsFactories();

      stopServers(0, 1);

      clearServer(0, 1);
   }

   /**
    * @param serverID
    * @return
    * @throws Exception
    */
   @Override
   protected ConfigurationImpl createBasicConfig(final int serverID) {
      ConfigurationImpl configuration = super.createBasicConfig(serverID);
      configuration.setMessageExpiryScanPeriod(100);

      return configuration;
   }

}
