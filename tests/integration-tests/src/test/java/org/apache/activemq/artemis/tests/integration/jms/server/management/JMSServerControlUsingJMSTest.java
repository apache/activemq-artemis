/**
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
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.api.jms.management.JMSServerControl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.activemq.artemis.jms.client.ActiveMQQueueConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;

public class JMSServerControlUsingJMSTest extends JMSServerControlTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private QueueConnection connection;

   private QueueSession session;

   // Static --------------------------------------------------------

   private static String[] toStringArray(final Object[] res)
   {
      String[] names = new String[res.length];
      for (int i = 0; i < res.length; i++)
      {
         names[i] = res[i].toString();
      }
      return names;
   }

   // The JMS test won't support the server being restarted, hence we have to do a slight different test on that case
   @Override
   @Test
   public void testCreateConnectionFactory_CompleteList() throws Exception
   {
      JMSServerControl control = createManagementControl();
      control.createConnectionFactory("test", //name
                                      true, // ha
                                      false, // useDiscovery
                                      1, // cfType
                                      "invm", // connectorNames
                                      "tst", // jndiBindins
                                      "tst", // clientID
                                      1, // clientFailureCheckPeriod
                                      1,  // connectionTTL
                                      1, // callTimeout
                                      1, // callFailoverTimeout
                                      1, // minLargeMessageSize
                                      true, // compressLargeMessages
                                      1, // consumerWindowSize
                                      1, // consumerMaxRate
                                      1, // confirmationWindowSize
                                      1, // ProducerWindowSize
                                      1, // producerMaxRate
                                      true, // blockOnACK
                                      true, // blockOnDurableSend
                                      true, // blockOnNonDurableSend
                                      true, // autoGroup
                                      true, // preACK
                                      ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, // loadBalancingClassName
                                      1, // transactionBatchSize
                                      1, // dupsOKBatchSize
                                      true, // useGlobalPools
                                      1, // scheduleThreadPoolSize
                                      1, // threadPoolMaxSize
                                      1, // retryInterval
                                      1, // retryIntervalMultiplier
                                      1, // maxRetryInterval
                                      1, // reconnectAttempts
                                      true, // failoverOnInitialConnection
                                      "tst"); // groupID


      ActiveMQQueueConnectionFactory cf = (ActiveMQQueueConnectionFactory)context.lookup("tst");

      Assert.assertEquals(true, cf.isHA());
      Assert.assertEquals("tst", cf.getClientID());
      Assert.assertEquals(1, cf.getClientFailureCheckPeriod());
      Assert.assertEquals(1, cf.getConnectionTTL());
      Assert.assertEquals(1, cf.getCallTimeout());
      Assert.assertEquals(1, cf.getCallFailoverTimeout());
      Assert.assertEquals(1, cf.getMinLargeMessageSize());
      Assert.assertEquals(true, cf.isCompressLargeMessage());
      Assert.assertEquals(1, cf.getConsumerWindowSize());
      Assert.assertEquals(1, cf.getConfirmationWindowSize());
      Assert.assertEquals(1, cf.getProducerWindowSize());
      Assert.assertEquals(1, cf.getProducerMaxRate());
      Assert.assertEquals(true, cf.isBlockOnAcknowledge());
      Assert.assertEquals(true, cf.isBlockOnDurableSend());
      Assert.assertEquals(true, cf.isBlockOnNonDurableSend());
      Assert.assertEquals(true, cf.isAutoGroup());
      Assert.assertEquals(true, cf.isPreAcknowledge());
      Assert.assertEquals(ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, cf.getConnectionLoadBalancingPolicyClassName());
      Assert.assertEquals(1, cf.getTransactionBatchSize());
      Assert.assertEquals(1, cf.getDupsOKBatchSize());
      Assert.assertEquals(true, cf.isUseGlobalPools());
      Assert.assertEquals(1, cf.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(1, cf.getThreadPoolMaxSize());
      Assert.assertEquals(1, cf.getRetryInterval());
      Assert.assertEquals(1.0, cf.getRetryIntervalMultiplier(), 0.000001);
      Assert.assertEquals(1, cf.getMaxRetryInterval());
      Assert.assertEquals(1, cf.getReconnectAttempts());
      Assert.assertEquals(true, cf.isFailoverOnInitialConnection());
      Assert.assertEquals("tst", cf.getGroupID());

   }
   // Constructors --------------------------------------------------

   // JMSServerControlTest overrides --------------------------------
   @Override
   protected int getNumberOfConsumers()
   {
      return 1;
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                        new TransportConfiguration(InVMConnectorFactory.class.getName()));
      connection = cf.createQueueConnection();
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();
   }

   @Override
   protected JMSServerControl createManagementControl() throws Exception
   {
      ActiveMQQueue managementQueue = (ActiveMQQueue) ActiveMQJMSClient.createQueue("activemq.management");
      final JMSMessagingProxy proxy = new JMSMessagingProxy(session, managementQueue, ResourceNames.JMS_SERVER);

      return new JMSServerControl()
      {

         public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
         {
            return (Boolean)proxy.invokeOperation("closeConnectionsForAddress", ipAddress);
         }

         public boolean closeConsumerConnectionsForAddress(final String address) throws Exception
         {
            return (Boolean)proxy.invokeOperation("closeConsumerConnectionsForAddress", address);
         }

         public boolean closeConnectionsForUser(final String userName) throws Exception
         {
            return (Boolean)proxy.invokeOperation("closeConnectionsForUser", userName);
         }

         public boolean createQueue(final String name) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createQueue", name);
         }

         public boolean createQueue(String name, String jndiBindings, String selector) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createQueue", name, jndiBindings, selector);
         }

         public boolean createQueue(String name, String jndiBindings, String selector, boolean durable) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createQueue", name, jndiBindings, selector, durable);
         }

         public boolean createTopic(final String name) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createTopic", name);
         }

         public void destroyConnectionFactory(final String name) throws Exception
         {
            proxy.invokeOperation("destroyConnectionFactory", name);
         }

         public boolean destroyQueue(final String name) throws Exception
         {
            return (Boolean)proxy.invokeOperation("destroyQueue", name);
         }

         @Override
         public boolean destroyQueue(String name, boolean removeConsumers) throws Exception
         {
            return (Boolean)proxy.invokeOperation("destroyQueue", name, removeConsumers);
         }

         public boolean destroyTopic(final String name) throws Exception
         {
            return (Boolean)proxy.invokeOperation("destroyTopic", name);
         }

         public boolean destroyTopic(final String name, boolean removeConsumers) throws Exception
         {
            return (Boolean)proxy.invokeOperation("destroyTopic", name, removeConsumers);
         }

         public String getVersion()
         {
            return (String)proxy.retrieveAttributeValue("version");
         }

         public boolean isStarted()
         {
            return (Boolean)proxy.retrieveAttributeValue("started");
         }

         public String[] getQueueNames()
         {
            return JMSServerControlUsingJMSTest.toStringArray((Object[])proxy.retrieveAttributeValue("queueNames"));
         }

         public String[] getTopicNames()
         {
            return JMSServerControlUsingJMSTest.toStringArray((Object[])proxy.retrieveAttributeValue("topicNames"));
         }

         public String[] getConnectionFactoryNames()
         {
            return JMSServerControlUsingJMSTest.toStringArray((Object[])proxy.retrieveAttributeValue("connectionFactoryNames"));
         }

         public String[] listConnectionIDs() throws Exception
         {
            return (String[])proxy.invokeOperation("listConnectionIDs");
         }

         public String listConnectionsAsJSON() throws Exception
         {
            return (String)proxy.invokeOperation("listConnectionsAsJSON");
         }

         public String listConsumersAsJSON(String connectionID) throws Exception
         {
            return (String)proxy.invokeOperation("listConsumersAsJSON", connectionID);
         }

         public String[] listRemoteAddresses() throws Exception
         {
            return (String[])proxy.invokeOperation("listRemoteAddresses");
         }

         public String[] listRemoteAddresses(final String ipAddress) throws Exception
         {
            return (String[])proxy.invokeOperation("listRemoteAddresses", ipAddress);
         }

         public String[] listSessions(final String connectionID) throws Exception
         {
            return (String[])proxy.invokeOperation("listSessions", connectionID);
         }

         public boolean createQueue(String name, String jndiBinding) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createQueue", name, jndiBinding);
         }

         public boolean createTopic(String name, String jndiBinding) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createTopic", name, jndiBinding);
         }

         public String[] listTargetDestinations(String sessionID) throws Exception
         {
            return null;
         }

         public String getLastSentMessageID(String sessionID, String address) throws Exception
         {
            return null;
         }

         public String getSessionCreationTime(String sessionID) throws Exception
         {
            return (String)proxy.invokeOperation("getSessionCreationTime", sessionID);
         }

         public String listSessionsAsJSON(String connectionID) throws Exception
         {
            return (String)proxy.invokeOperation("listSessionsAsJSON", connectionID);
         }

         public String listPreparedTransactionDetailsAsJSON() throws Exception
         {
            return (String)proxy.invokeOperation("listPreparedTransactionDetailsAsJSON");
         }

         public String listPreparedTransactionDetailsAsHTML() throws Exception
         {
            return (String)proxy.invokeOperation("listPreparedTransactionDetailsAsHTML");
         }

         public void createConnectionFactory(String name,
                                             boolean ha,
                                             boolean useDiscovery,
                                             int cfType,
                                             String[] connectorNames,
                                             Object[] bindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory", name, ha, useDiscovery, cfType, connectorNames, bindings);

         }

         public void createConnectionFactory(String name,
                                             boolean ha,
                                             boolean useDiscovery,
                                             int cfType,
                                             String connectors,
                                             String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory", name, ha, useDiscovery, cfType, connectors, jndiBindings);
         }

         public String listAllConsumersAsJSON() throws Exception
         {
            return (String)proxy.invokeOperation("listAllConsumersAsJSON");
         }

         public void createConnectionFactory(String name,
                                             boolean ha,
                                             boolean useDiscovery,
                                             int cfType,
                                             String[] connectors,
                                             String[] jndiBindings,
                                             String clientID,
                                             long clientFailureCheckPeriod,
                                             long connectionTTL,
                                             long callTimeout,
                                             long callFailoverTimeout,
                                             int minLargeMessageSize,
                                             boolean compressLargeMessages,
                                             int consumerWindowSize,
                                             int consumerMaxRate,
                                             int confirmationWindowSize,
                                             int producerWindowSize,
                                             int producerMaxRate,
                                             boolean blockOnAcknowledge,
                                             boolean blockOnDurableSend,
                                             boolean blockOnNonDurableSend,
                                             boolean autoGroup,
                                             boolean preAcknowledge,
                                             String loadBalancingPolicyClassName,
                                             int transactionBatchSize,
                                             int dupsOKBatchSize,
                                             boolean useGlobalPools,
                                             int scheduledThreadPoolMaxSize,
                                             int threadPoolMaxSize,
                                             long retryInterval,
                                             double retryIntervalMultiplier,
                                             long maxRetryInterval,
                                             int reconnectAttempts,
                                             boolean failoverOnInitialConnection,
                                             String groupId) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  ha,
                                  useDiscovery,
                                  cfType,
                                  connectors,
                                  jndiBindings,
                                  clientID,
                                  clientFailureCheckPeriod,
                                  connectionTTL,
                                  callTimeout,
                                  callFailoverTimeout,
                                  minLargeMessageSize,
                                  compressLargeMessages,
                                  consumerWindowSize,
                                  consumerMaxRate,
                                  confirmationWindowSize,
                                  producerWindowSize,
                                  producerMaxRate,
                                  blockOnAcknowledge,
                                  blockOnDurableSend,
                                  blockOnNonDurableSend,
                                  autoGroup,
                                  preAcknowledge,
                                  loadBalancingPolicyClassName,
                                  transactionBatchSize,
                                  dupsOKBatchSize,
                                  useGlobalPools,
                                  scheduledThreadPoolMaxSize,
                                  threadPoolMaxSize,
                                  retryInterval,
                                  retryIntervalMultiplier,
                                  maxRetryInterval,
                                  reconnectAttempts,
                                  failoverOnInitialConnection,
                                  groupId);
         }

         public void createConnectionFactory(String name,
                                             boolean ha,
                                             boolean useDiscovery,
                                             int cfType,
                                             String connectors,
                                             String jndiBindings,
                                             String clientID,
                                             long clientFailureCheckPeriod,
                                             long connectionTTL,
                                             long callTimeout,
                                             long callFailoverTimeout,
                                             int minLargeMessageSize,
                                             boolean compressLargeMessages,
                                             int consumerWindowSize,
                                             int consumerMaxRate,
                                             int confirmationWindowSize,
                                             int producerWindowSize,
                                             int producerMaxRate,
                                             boolean blockOnAcknowledge,
                                             boolean blockOnDurableSend,
                                             boolean blockOnNonDurableSend,
                                             boolean autoGroup,
                                             boolean preAcknowledge,
                                             String loadBalancingPolicyClassName,
                                             int transactionBatchSize,
                                             int dupsOKBatchSize,
                                             boolean useGlobalPools,
                                             int scheduledThreadPoolMaxSize,
                                             int threadPoolMaxSize,
                                             long retryInterval,
                                             double retryIntervalMultiplier,
                                             long maxRetryInterval,
                                             int reconnectAttempts,
                                             boolean failoverOnInitialConnection,
                                             String groupId) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory",
                                  name,
                                  ha,
                                  useDiscovery,
                                  cfType,
                                  connectors,
                                  jndiBindings,
                                  clientID,
                                  clientFailureCheckPeriod,
                                  connectionTTL,
                                  callTimeout,
                                  callFailoverTimeout,
                                  minLargeMessageSize,
                                  compressLargeMessages,
                                  consumerWindowSize,
                                  consumerMaxRate,
                                  confirmationWindowSize,
                                  producerWindowSize,
                                  producerMaxRate,
                                  blockOnAcknowledge,
                                  blockOnDurableSend,
                                  blockOnNonDurableSend,
                                  autoGroup,
                                  preAcknowledge,
                                  loadBalancingPolicyClassName,
                                  transactionBatchSize,
                                  dupsOKBatchSize,
                                  useGlobalPools,
                                  scheduledThreadPoolMaxSize,
                                  threadPoolMaxSize,
                                  retryInterval,
                                  retryIntervalMultiplier,
                                  maxRetryInterval,
                                  reconnectAttempts,
                                  failoverOnInitialConnection,
                                  groupId);
         }

         public String closeConnectionWithClientID(String clientID) throws Exception
         {
            return (String)proxy.invokeOperation("closeConnectionWithClientID", clientID);
         }

      };
   }
}
