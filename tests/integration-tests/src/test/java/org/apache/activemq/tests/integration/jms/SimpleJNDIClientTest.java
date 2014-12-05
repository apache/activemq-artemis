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
package org.apache.activemq.tests.integration.jms;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.UDPBroadcastGroupConfiguration;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.jndi.ActiveMQInitialContextFactory;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * A ActiveMQConnectionFactoryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class SimpleJNDIClientTest extends UnitTestCase
{
   private final String groupAddress = getUDPDiscoveryAddress();

   private final int groupPort = getUDPDiscoveryPort();

   private ActiveMQServer liveService;

   private TransportConfiguration liveTC;

   @Test
   public void testDefaultConnectionFactories() throws NamingException, JMSException
   {
      Hashtable<String, Object> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("ConnectionFactory");
      Assert.assertEquals(JMSFactoryType.CF.intValue(), ((ActiveMQConnectionFactory)connectionFactory).getFactoryType());
      connectionFactory.createConnection().close();

      connectionFactory = (ConnectionFactory) ctx.lookup("XAConnectionFactory");
      Assert.assertEquals(JMSFactoryType.XA_CF.intValue(), ((ActiveMQConnectionFactory)connectionFactory).getFactoryType());
      connectionFactory.createConnection().close();

      connectionFactory = (ConnectionFactory) ctx.lookup("TopicConnectionFactory");
      Assert.assertEquals(JMSFactoryType.TOPIC_CF.intValue(), ((ActiveMQConnectionFactory)connectionFactory).getFactoryType());
      connectionFactory.createConnection().close();

      connectionFactory = (ConnectionFactory) ctx.lookup("QueueConnectionFactory");
      Assert.assertEquals(JMSFactoryType.QUEUE_CF.intValue(), ((ActiveMQConnectionFactory)connectionFactory).getFactoryType());
      connectionFactory.createConnection().close();
   }

   @Test
   public void testCustomCF() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      props.put("connection.myConnectionFactory.type", "CF");
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(JMSFactoryType.CF.intValue(), ((ActiveMQConnectionFactory)connectionFactory).getFactoryType());

      connectionFactory.createConnection().close();
   }

   @Test
   public void testVMCF0() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "vm://0");
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("ConnectionFactory");

      connectionFactory.createConnection().close();
   }

   @Test
   public void testVMCF1() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "vm://1");
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("ConnectionFactory");

      connectionFactory.createConnection().close();
   }

   @Test
   public void testXACF() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      props.put("connection.myConnectionFactory.type", "XA_CF");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(JMSFactoryType.XA_CF.intValue(), connectionFactory.getFactoryType());
   }

   @Test
   public void testQueueCF() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      props.put("connection.myConnectionFactory.type", "QUEUE_CF");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(JMSFactoryType.QUEUE_CF.intValue(), connectionFactory.getFactoryType());
   }

   @Test
   public void testQueueXACF() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      props.put("connection.myConnectionFactory.type", "QUEUE_XA_CF");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(JMSFactoryType.QUEUE_XA_CF.intValue(), connectionFactory.getFactoryType());
   }

   @Test
   public void testTopicCF() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      props.put("connection.myConnectionFactory.type", "TOPIC_CF");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(JMSFactoryType.TOPIC_CF.intValue(), connectionFactory.getFactoryType());
   }

   @Test
   public void testTopicXACF() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      props.put("connection.myConnectionFactory.type", "TOPIC_XA_CF");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(JMSFactoryType.TOPIC_XA_CF.intValue(), connectionFactory.getFactoryType());
   }

   @Test
   public void testCFWithProperties() throws NamingException, JMSException
   {
      // we don't test the 'ha' property here because it's not supported on a local connection factory (i.e. one
      // constructed from an InitialContext where the environment doesn't contain the property "java.naming.provider.url")

      long callFailoverTimeout = RandomUtil.randomPositiveLong();
      long callTimeout = RandomUtil.randomPositiveLong();
      long clientFailureCheckPeriod = RandomUtil.randomPositiveLong();
      String clientID = RandomUtil.randomString();
      int confirmationWindowSize = RandomUtil.randomPositiveInt();
      String connectionLoadBalancingPolicyClassName = RandomUtil.randomString();
      long connectionTTL = RandomUtil.randomPositiveLong();
      int consumerMaxRate = RandomUtil.randomPositiveInt();
      int consumerWindowSize = RandomUtil.randomPositiveInt();
      int minLargeMessageSize = RandomUtil.randomPositiveInt();
      int dupsOKBatchSize = RandomUtil.randomPositiveInt();
      String groupID = RandomUtil.randomString();
      int initialConnectAttempts = RandomUtil.randomPositiveInt();
      int initialMessagePacketSize = RandomUtil.randomPositiveInt();
      long maxRetryInterval = RandomUtil.randomPositiveLong();
      int producerMaxRate = RandomUtil.randomPositiveInt();
      int producerWindowSize = RandomUtil.randomPositiveInt();
      int reconnectAttempts = RandomUtil.randomPositiveInt();
      long retryInterval = RandomUtil.randomPositiveLong();
      double retryIntervalMultiplier = RandomUtil.randomDouble();
      int scheduledThreadPoolMaxSize = RandomUtil.randomPositiveInt();
      int threadPoolMaxSize = RandomUtil.randomPositiveInt();
      int transactionBatchSize = RandomUtil.randomPositiveInt();
      boolean autoGroup = RandomUtil.randomBoolean();
      boolean blockOnAcknowledge = RandomUtil.randomBoolean();
      boolean blockOnDurableSend = RandomUtil.randomBoolean();
      boolean blockOnNonDurableSend = RandomUtil.randomBoolean();
      boolean cacheLargeMessagesClient = RandomUtil.randomBoolean();
      boolean compressLargeMessage = RandomUtil.randomBoolean();
      boolean failoverOnInitialConnection = RandomUtil.randomBoolean();
      boolean preAcknowledge = RandomUtil.randomBoolean();
      boolean useGlobalPools = RandomUtil.randomBoolean();

      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      props.put("connection.myConnectionFactory.callFailoverTimeout", callFailoverTimeout);
      props.put("connection.myConnectionFactory.callTimeout", callTimeout);
      props.put("connection.myConnectionFactory.clientFailureCheckPeriod", clientFailureCheckPeriod);
      props.put("connection.myConnectionFactory.clientID", clientID);
      props.put("connection.myConnectionFactory.confirmationWindowSize", confirmationWindowSize);
      props.put("connection.myConnectionFactory.connectionLoadBalancingPolicyClassName", connectionLoadBalancingPolicyClassName);
      props.put("connection.myConnectionFactory.connectionTTL", connectionTTL);
      props.put("connection.myConnectionFactory.consumerMaxRate", consumerMaxRate);
      props.put("connection.myConnectionFactory.consumerWindowSize", consumerWindowSize);
      props.put("connection.myConnectionFactory.minLargeMessageSize", minLargeMessageSize);
      props.put("connection.myConnectionFactory.dupsOKBatchSize", dupsOKBatchSize);
      props.put("connection.myConnectionFactory.groupID", groupID);
      props.put("connection.myConnectionFactory.initialConnectAttempts", initialConnectAttempts);
      props.put("connection.myConnectionFactory.initialMessagePacketSize", initialMessagePacketSize);
      props.put("connection.myConnectionFactory.maxRetryInterval", maxRetryInterval);
      props.put("connection.myConnectionFactory.producerMaxRate", producerMaxRate);
      props.put("connection.myConnectionFactory.producerWindowSize", producerWindowSize);
      props.put("connection.myConnectionFactory.reconnectAttempts", reconnectAttempts);
      props.put("connection.myConnectionFactory.retryInterval", retryInterval);
      props.put("connection.myConnectionFactory.retryIntervalMultiplier", retryIntervalMultiplier);
      props.put("connection.myConnectionFactory.scheduledThreadPoolMaxSize", scheduledThreadPoolMaxSize);
      props.put("connection.myConnectionFactory.threadPoolMaxSize", threadPoolMaxSize);
      props.put("connection.myConnectionFactory.transactionBatchSize", transactionBatchSize);
      props.put("connection.myConnectionFactory.blockOnAcknowledge", blockOnAcknowledge);
      props.put("connection.myConnectionFactory.blockOnDurableSend", blockOnDurableSend);
      props.put("connection.myConnectionFactory.blockOnNonDurableSend", blockOnNonDurableSend);
      props.put("connection.myConnectionFactory.cacheLargeMessagesClient", cacheLargeMessagesClient);
      props.put("connection.myConnectionFactory.compressLargeMessage", compressLargeMessage);
      props.put("connection.myConnectionFactory.failoverOnInitialConnection", failoverOnInitialConnection);
      props.put("connection.myConnectionFactory.autoGroup", autoGroup);
      props.put("connection.myConnectionFactory.preAcknowledge", preAcknowledge);
      props.put("connection.myConnectionFactory.useGlobalPools", useGlobalPools);
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(callFailoverTimeout, cf.getCallFailoverTimeout());
      Assert.assertEquals(callTimeout, cf.getCallTimeout());
      Assert.assertEquals(clientFailureCheckPeriod, cf.getClientFailureCheckPeriod());
      Assert.assertEquals(clientID, cf.getClientID());
      Assert.assertEquals(confirmationWindowSize, cf.getConfirmationWindowSize());
      Assert.assertEquals(connectionLoadBalancingPolicyClassName, cf.getConnectionLoadBalancingPolicyClassName());
      Assert.assertEquals(connectionTTL, cf.getConnectionTTL());
      Assert.assertEquals(consumerMaxRate, cf.getConsumerMaxRate());
      Assert.assertEquals(consumerWindowSize, cf.getConsumerWindowSize());
      Assert.assertEquals(minLargeMessageSize, cf.getMinLargeMessageSize());
      Assert.assertEquals(dupsOKBatchSize, cf.getDupsOKBatchSize());
      Assert.assertEquals(groupID, cf.getGroupID());
      Assert.assertEquals(initialConnectAttempts, cf.getInitialConnectAttempts());
      Assert.assertEquals(initialMessagePacketSize, cf.getInitialMessagePacketSize());
      Assert.assertEquals(maxRetryInterval, cf.getMaxRetryInterval());
      Assert.assertEquals(producerMaxRate, cf.getProducerMaxRate());
      Assert.assertEquals(producerWindowSize, cf.getProducerWindowSize());
      Assert.assertEquals(reconnectAttempts, cf.getReconnectAttempts());
      Assert.assertEquals(retryInterval, cf.getRetryInterval());
      Assert.assertEquals(retryIntervalMultiplier, cf.getRetryIntervalMultiplier(), 0.0001);
      Assert.assertEquals(scheduledThreadPoolMaxSize, cf.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(threadPoolMaxSize, cf.getThreadPoolMaxSize());
      Assert.assertEquals(transactionBatchSize, cf.getTransactionBatchSize());
      Assert.assertEquals(autoGroup, cf.isAutoGroup());
      Assert.assertEquals(blockOnAcknowledge, cf.isBlockOnAcknowledge());
      Assert.assertEquals(blockOnDurableSend, cf.isBlockOnDurableSend());
      Assert.assertEquals(blockOnNonDurableSend, cf.isBlockOnNonDurableSend());
      Assert.assertEquals(cacheLargeMessagesClient, cf.isCacheLargeMessagesClient());
      Assert.assertEquals(compressLargeMessage, cf.isCompressLargeMessage());
      Assert.assertEquals(failoverOnInitialConnection, cf.isFailoverOnInitialConnection());
      Assert.assertEquals(preAcknowledge, cf.isPreAcknowledge());
      Assert.assertEquals(useGlobalPools, cf.isUseGlobalPools());
   }

   @Test
   public void testCFWithStringProperties() throws NamingException, JMSException
   {
      // we don't test the 'ha' property here because it's not supported on a local connection factory (i.e. one
      // constructed from an InitialContext where the environment doesn't contain the property "java.naming.provider.url")

      String callFailoverTimeout = Long.toString(RandomUtil.randomPositiveLong());
      String callTimeout = Long.toString(RandomUtil.randomPositiveLong());
      String clientFailureCheckPeriod = Long.toString(RandomUtil.randomPositiveLong());
      String clientID = RandomUtil.randomString();
      String confirmationWindowSize = Integer.toString(RandomUtil.randomPositiveInt());
      String connectionLoadBalancingPolicyClassName = RandomUtil.randomString();
      String connectionTTL = Long.toString(RandomUtil.randomPositiveLong());
      String consumerMaxRate = Integer.toString(RandomUtil.randomPositiveInt());
      String consumerWindowSize = Integer.toString(RandomUtil.randomPositiveInt());
      String minLargeMessageSize = Integer.toString(RandomUtil.randomPositiveInt());
      String dupsOKBatchSize = Integer.toString(RandomUtil.randomPositiveInt());
      String groupID = RandomUtil.randomString();
      String initialConnectAttempts = Integer.toString(RandomUtil.randomPositiveInt());
      String initialMessagePacketSize = Integer.toString(RandomUtil.randomPositiveInt());
      String maxRetryInterval = Long.toString(RandomUtil.randomPositiveLong());
      String producerMaxRate = Integer.toString(RandomUtil.randomPositiveInt());
      String producerWindowSize = Integer.toString(RandomUtil.randomPositiveInt());
      String reconnectAttempts = Integer.toString(RandomUtil.randomPositiveInt());
      String retryInterval = Long.toString(RandomUtil.randomPositiveLong());
      String retryIntervalMultiplier = Double.toString(RandomUtil.randomDouble());
      String scheduledThreadPoolMaxSize = Integer.toString(RandomUtil.randomPositiveInt());
      String threadPoolMaxSize = Integer.toString(RandomUtil.randomPositiveInt());
      String transactionBatchSize = Integer.toString(RandomUtil.randomPositiveInt());
      String autoGroup = Boolean.toString(RandomUtil.randomBoolean());
      String blockOnAcknowledge = Boolean.toString(RandomUtil.randomBoolean());
      String blockOnDurableSend = Boolean.toString(RandomUtil.randomBoolean());
      String blockOnNonDurableSend = Boolean.toString(RandomUtil.randomBoolean());
      String cacheLargeMessagesClient = Boolean.toString(RandomUtil.randomBoolean());
      String compressLargeMessage = Boolean.toString(RandomUtil.randomBoolean());
      String failoverOnInitialConnection = Boolean.toString(RandomUtil.randomBoolean());
      String preAcknowledge = Boolean.toString(RandomUtil.randomBoolean());
      String useGlobalPools = Boolean.toString(RandomUtil.randomBoolean());

      Hashtable props = new Hashtable<String, String>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      props.put("connection.myConnectionFactory.callFailoverTimeout", callFailoverTimeout);
      props.put("connection.myConnectionFactory.callTimeout", callTimeout);
      props.put("connection.myConnectionFactory.clientFailureCheckPeriod", clientFailureCheckPeriod);
      props.put("connection.myConnectionFactory.clientID", clientID);
      props.put("connection.myConnectionFactory.confirmationWindowSize", confirmationWindowSize);
      props.put("connection.myConnectionFactory.connectionLoadBalancingPolicyClassName", connectionLoadBalancingPolicyClassName);
      props.put("connection.myConnectionFactory.connectionTTL", connectionTTL);
      props.put("connection.myConnectionFactory.consumerMaxRate", consumerMaxRate);
      props.put("connection.myConnectionFactory.consumerWindowSize", consumerWindowSize);
      props.put("connection.myConnectionFactory.minLargeMessageSize", minLargeMessageSize);
      props.put("connection.myConnectionFactory.dupsOKBatchSize", dupsOKBatchSize);
      props.put("connection.myConnectionFactory.groupID", groupID);
      props.put("connection.myConnectionFactory.initialConnectAttempts", initialConnectAttempts);
      props.put("connection.myConnectionFactory.initialMessagePacketSize", initialMessagePacketSize);
      props.put("connection.myConnectionFactory.maxRetryInterval", maxRetryInterval);
      props.put("connection.myConnectionFactory.producerMaxRate", producerMaxRate);
      props.put("connection.myConnectionFactory.producerWindowSize", producerWindowSize);
      props.put("connection.myConnectionFactory.reconnectAttempts", reconnectAttempts);
      props.put("connection.myConnectionFactory.retryInterval", retryInterval);
      props.put("connection.myConnectionFactory.retryIntervalMultiplier", retryIntervalMultiplier);
      props.put("connection.myConnectionFactory.scheduledThreadPoolMaxSize", scheduledThreadPoolMaxSize);
      props.put("connection.myConnectionFactory.threadPoolMaxSize", threadPoolMaxSize);
      props.put("connection.myConnectionFactory.transactionBatchSize", transactionBatchSize);
      props.put("connection.myConnectionFactory.blockOnAcknowledge", blockOnAcknowledge);
      props.put("connection.myConnectionFactory.blockOnDurableSend", blockOnDurableSend);
      props.put("connection.myConnectionFactory.blockOnNonDurableSend", blockOnNonDurableSend);
      props.put("connection.myConnectionFactory.cacheLargeMessagesClient", cacheLargeMessagesClient);
      props.put("connection.myConnectionFactory.compressLargeMessage", compressLargeMessage);
      props.put("connection.myConnectionFactory.failoverOnInitialConnection", failoverOnInitialConnection);
      props.put("connection.myConnectionFactory.autoGroup", autoGroup);
      props.put("connection.myConnectionFactory.preAcknowledge", preAcknowledge);
      props.put("connection.myConnectionFactory.useGlobalPools", useGlobalPools);
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(Long.parseLong(callFailoverTimeout), cf.getCallFailoverTimeout());
      Assert.assertEquals(Long.parseLong(callTimeout), cf.getCallTimeout());
      Assert.assertEquals(Long.parseLong(clientFailureCheckPeriod), cf.getClientFailureCheckPeriod());
      Assert.assertEquals(clientID, cf.getClientID());
      Assert.assertEquals(Integer.parseInt(confirmationWindowSize), cf.getConfirmationWindowSize());
      Assert.assertEquals(connectionLoadBalancingPolicyClassName, cf.getConnectionLoadBalancingPolicyClassName());
      Assert.assertEquals(Long.parseLong(connectionTTL), cf.getConnectionTTL());
      Assert.assertEquals(Integer.parseInt(consumerMaxRate), cf.getConsumerMaxRate());
      Assert.assertEquals(Integer.parseInt(consumerWindowSize), cf.getConsumerWindowSize());
      Assert.assertEquals(Integer.parseInt(minLargeMessageSize), cf.getMinLargeMessageSize());
      Assert.assertEquals(Integer.parseInt(dupsOKBatchSize), cf.getDupsOKBatchSize());
      Assert.assertEquals(groupID, cf.getGroupID());
      Assert.assertEquals(Integer.parseInt(initialConnectAttempts), cf.getInitialConnectAttempts());
      Assert.assertEquals(Integer.parseInt(initialMessagePacketSize), cf.getInitialMessagePacketSize());
      Assert.assertEquals(Long.parseLong(maxRetryInterval), cf.getMaxRetryInterval());
      Assert.assertEquals(Integer.parseInt(producerMaxRate), cf.getProducerMaxRate());
      Assert.assertEquals(Integer.parseInt(producerWindowSize), cf.getProducerWindowSize());
      Assert.assertEquals(Integer.parseInt(reconnectAttempts), cf.getReconnectAttempts());
      Assert.assertEquals(Long.parseLong(retryInterval), cf.getRetryInterval());
      Assert.assertEquals(Double.parseDouble(retryIntervalMultiplier), cf.getRetryIntervalMultiplier(), 0.0001);
      Assert.assertEquals(Integer.parseInt(scheduledThreadPoolMaxSize), cf.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(Integer.parseInt(threadPoolMaxSize), cf.getThreadPoolMaxSize());
      Assert.assertEquals(Integer.parseInt(transactionBatchSize), cf.getTransactionBatchSize());
      Assert.assertEquals(Boolean.parseBoolean(autoGroup), cf.isAutoGroup());
      Assert.assertEquals(Boolean.parseBoolean(blockOnAcknowledge), cf.isBlockOnAcknowledge());
      Assert.assertEquals(Boolean.parseBoolean(blockOnDurableSend), cf.isBlockOnDurableSend());
      Assert.assertEquals(Boolean.parseBoolean(blockOnNonDurableSend), cf.isBlockOnNonDurableSend());
      Assert.assertEquals(Boolean.parseBoolean(cacheLargeMessagesClient), cf.isCacheLargeMessagesClient());
      Assert.assertEquals(Boolean.parseBoolean(compressLargeMessage), cf.isCompressLargeMessage());
      Assert.assertEquals(Boolean.parseBoolean(failoverOnInitialConnection), cf.isFailoverOnInitialConnection());
      Assert.assertEquals(Boolean.parseBoolean(preAcknowledge), cf.isPreAcknowledge());
      Assert.assertEquals(Boolean.parseBoolean(useGlobalPools), cf.isUseGlobalPools());
   }

   @Test
   public void testRemoteCFWithTCP() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "tcp://127.0.0.1:5445");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("myConnectionFactory");

      connectionFactory.createConnection().close();
   }

   @Test
   public void testRemoteCFWithTCPandHA() throws NamingException, JMSException
   {
      boolean ha = true;

      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "tcp://127.0.0.1:5445");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      props.put("connection.myConnectionFactory.ha", ha);
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(ha, cf.isHA());
   }

   @Test
   public void testRemoteCFWithJGroups() throws Exception
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "jgroups://test-jgroups-file_ping.xml");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("ConnectionFactory");
      connectionFactory.getDiscoveryGroupConfiguration().getBroadcastEndpointFactoryConfiguration().createBroadcastEndpointFactory().createBroadcastEndpoint().close(false);
   }

   @Test
   public void testRemoteCFWithJgroupsWithTransportConfig() throws Exception
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, ActiveMQInitialContextFactory.class.getCanonicalName());
      props.put(Context.PROVIDER_URL, "jgroups://test-jgroups-file_ping.xml?" +
         ActiveMQInitialContextFactory.REFRESH_TIMEOUT + "=5000&" +
         ActiveMQInitialContextFactory.DISCOVERY_INITIAL_WAIT_TIMEOUT + "=6000");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("ConnectionFactory");

      DiscoveryGroupConfiguration discoveryGroupConfiguration = cf.getDiscoveryGroupConfiguration();
      Assert.assertEquals(5000, discoveryGroupConfiguration.getRefreshTimeout());
      Assert.assertEquals(6000, discoveryGroupConfiguration.getDiscoveryInitialWaitTimeout());

      cf.getDiscoveryGroupConfiguration().getBroadcastEndpointFactoryConfiguration().createBroadcastEndpointFactory().createBroadcastEndpoint().close(false);
   }

   @Test
   public void testRemoteCFWithUDP() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "udp://" + getUDPDiscoveryAddress() + ":" + getUDPDiscoveryPort());
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("myConnectionFactory");

      connectionFactory.createConnection().close();
   }

   @Test
   public void testRemoteCFWithUDPWithTransportConfig() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, ActiveMQInitialContextFactory.class.getCanonicalName());
      props.put(Context.PROVIDER_URL, "udp://" + getUDPDiscoveryAddress() + ":" + getUDPDiscoveryPort() + "?" +
         TransportConstants.LOCAL_ADDRESS_PROP_NAME + "=127.0.0.1&" +
         TransportConstants.LOCAL_PORT_PROP_NAME + "=1198&" +
         ActiveMQInitialContextFactory.REFRESH_TIMEOUT + "=5000&" +
         ActiveMQInitialContextFactory.DISCOVERY_INITIAL_WAIT_TIMEOUT + "=6000");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      DiscoveryGroupConfiguration discoveryGroupConfiguration = cf.getDiscoveryGroupConfiguration();
      Assert.assertEquals(5000, discoveryGroupConfiguration.getRefreshTimeout());
      Assert.assertEquals(6000, discoveryGroupConfiguration.getDiscoveryInitialWaitTimeout());

      UDPBroadcastGroupConfiguration udpBroadcastGroupConfiguration = (UDPBroadcastGroupConfiguration) discoveryGroupConfiguration.getBroadcastEndpointFactoryConfiguration();
      Assert.assertEquals("127.0.0.1", udpBroadcastGroupConfiguration.getLocalBindAddress());
      Assert.assertEquals(1198, udpBroadcastGroupConfiguration.getLocalBindPort());
      Assert.assertEquals(getUDPDiscoveryAddress(), udpBroadcastGroupConfiguration.getGroupAddress());
      Assert.assertEquals(getUDPDiscoveryPort(), udpBroadcastGroupConfiguration.getGroupPort());
   }

   @Test
   public void testRemoteCFWithMultipleHosts() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "tcp://127.0.0.1:5445,127.0.0.2:5446");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("myConnectionFactory");

      connectionFactory.createConnection().close();
   }

   @Test
   public void testRemoteCFWithTransportConfig() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "tcp://127.0.0.1:5445?" +
         TransportConstants.SSL_ENABLED_PROP_NAME + "=mySSLEnabledPropValue&" +
         TransportConstants.HTTP_ENABLED_PROP_NAME + "=myHTTPEnabledPropValue&" +
         TransportConstants.HTTP_CLIENT_IDLE_PROP_NAME + "=myHTTPClientIdlePropValue&" +
         TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD + "=myHTTPClientIdleScanPeriodValue&" +
         TransportConstants.HTTP_REQUIRES_SESSION_ID + "=myHTTPRequiresSessionIDValue&" +
         TransportConstants.HTTP_UPGRADE_ENABLED_PROP_NAME + "=myHTTPUpgradeEnabledPropValue&" +
         TransportConstants.HTTP_UPGRADE_ENDPOINT_PROP_NAME + "=myHTTPUpgradeEndpointPropValue&" +
         TransportConstants.USE_SERVLET_PROP_NAME + "=myUseServletPropValue&" +
         TransportConstants.SERVLET_PATH + "=myServletPathValue&" +
         TransportConstants.USE_NIO_PROP_NAME + "=myUseNIOPropValue&" +
         TransportConstants.USE_NIO_GLOBAL_WORKER_POOL_PROP_NAME + "=myUseNIOGlobalWorkerPoolPropValue&" +
         TransportConstants.LOCAL_ADDRESS_PROP_NAME + "=myLocalAddressPropValue&" +
         TransportConstants.LOCAL_PORT_PROP_NAME + "=myLocalPortPropValue&" +
         TransportConstants.KEYSTORE_PROVIDER_PROP_NAME + "=myKeystoreProviderPropValue&" +
         TransportConstants.KEYSTORE_PATH_PROP_NAME + "=myKeystorePathPropValue&" +
         TransportConstants.KEYSTORE_PASSWORD_PROP_NAME + "=myKeystorePasswordPropValue&" +
         TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME + "=myTruststoreProviderPropValue&" +
         TransportConstants.TRUSTSTORE_PATH_PROP_NAME + "=myTruststorePathPropValue&" +
         TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME + "=myTruststorePasswordPropValue&" +
         TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME + "=myEnabledCipherSuitesPropValue&" +
         TransportConstants.ENABLED_PROTOCOLS_PROP_NAME + "=myEnabledProtocolsPropValue&" +
         TransportConstants.TCP_NODELAY_PROPNAME + "=myTCPNoDelayPropValue&" +
         TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME + "=myTCPSendbufferSizePropValue&" +
         TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME + "=myTCPReceivebufferSizePropValue&" +
         TransportConstants.NIO_REMOTING_THREADS_PROPNAME + "=myNIORemotingThreadsPropValue&" +
         TransportConstants.BATCH_DELAY + "=myBatchDelay&" +
         ActiveMQDefaultConfiguration.getPropMaskPassword() + "=myPropMaskPassword&" +
         ActiveMQDefaultConfiguration.getPropPasswordCodec() + "=myPropPasswordCodec&" +
         TransportConstants.NETTY_CONNECT_TIMEOUT + "=myNettyConnectTimeout&");
      props.put(ActiveMQInitialContextFactory.CONNECTION_FACTORY_NAMES, "myConnectionFactory");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Map parametersFromJNDI = cf.getServerLocator().getStaticTransportConfigurations()[0].getParams();

      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.SSL_ENABLED_PROP_NAME), "mySSLEnabledPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_ENABLED_PROP_NAME), "myHTTPEnabledPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_CLIENT_IDLE_PROP_NAME), "myHTTPClientIdlePropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD), "myHTTPClientIdleScanPeriodValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_REQUIRES_SESSION_ID), "myHTTPRequiresSessionIDValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_UPGRADE_ENABLED_PROP_NAME), "myHTTPUpgradeEnabledPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_UPGRADE_ENDPOINT_PROP_NAME), "myHTTPUpgradeEndpointPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.USE_SERVLET_PROP_NAME), "myUseServletPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.SERVLET_PATH), "myServletPathValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.USE_NIO_PROP_NAME), "myUseNIOPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.USE_NIO_GLOBAL_WORKER_POOL_PROP_NAME), "myUseNIOGlobalWorkerPoolPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.LOCAL_ADDRESS_PROP_NAME), "myLocalAddressPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.LOCAL_PORT_PROP_NAME), "myLocalPortPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME), "myKeystoreProviderPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.KEYSTORE_PATH_PROP_NAME), "myKeystorePathPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME), "myKeystorePasswordPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME), "myTruststoreProviderPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TRUSTSTORE_PATH_PROP_NAME), "myTruststorePathPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME), "myTruststorePasswordPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME), "myEnabledCipherSuitesPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME), "myEnabledProtocolsPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TCP_NODELAY_PROPNAME), "myTCPNoDelayPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME), "myTCPSendbufferSizePropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME), "myTCPReceivebufferSizePropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.NIO_REMOTING_THREADS_PROPNAME), "myNIORemotingThreadsPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.BATCH_DELAY), "myBatchDelay");
      Assert.assertEquals(parametersFromJNDI.get(ActiveMQDefaultConfiguration.getPropMaskPassword()), "myPropMaskPassword");
      Assert.assertEquals(parametersFromJNDI.get(ActiveMQDefaultConfiguration.getPropPasswordCodec()), "myPropPasswordCodec");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.NETTY_CONNECT_TIMEOUT), "myNettyConnectTimeout");
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      startServer();
   }

   private void startServer() throws Exception
   {
      liveTC = new TransportConfiguration(INVM_CONNECTOR_FACTORY);
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      connectors.put(liveTC.getName(), liveTC);
      List<String> connectorNames = new ArrayList<String>();
      connectorNames.add(liveTC.getName());

      Map params = new HashMap();
      params.put("server-id", 1);

      Configuration liveConf = createBasicConfig()
         .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY))
         .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params))
         .addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY))
         .setConnectorConfigurations(connectors)
         .setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration());

      final long broadcastPeriod = 250;

      final String bcGroupName = "bc1";

      final int localBindPort = 5432;

      BroadcastGroupConfiguration bcConfig1 = new BroadcastGroupConfiguration()
         .setName(bcGroupName)
         .setBroadcastPeriod(broadcastPeriod)
         .setConnectorInfos(connectorNames)
         .setEndpointFactoryConfiguration(new UDPBroadcastGroupConfiguration()
                                             .setGroupAddress(groupAddress)
                                             .setGroupPort(groupPort)
                                             .setLocalBindPort(localBindPort));

      List<BroadcastGroupConfiguration> bcConfigs1 = new ArrayList<BroadcastGroupConfiguration>();
      bcConfigs1.add(bcConfig1);
      liveConf.setBroadcastGroupConfigurations(bcConfigs1);

      liveService = addServer(ActiveMQServers.newActiveMQServer(liveConf, false));
      liveService.start();
   }

   @Test
   public void testQueue() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put("queue.myQueue", "myQueue");
      props.put("queue.queues/myQueue", "myQueue");
      Context ctx = new InitialContext(props);

      Destination destination = (Destination) ctx.lookup("myQueue");
      Assert.assertTrue(destination instanceof Queue);

      destination = (Destination) ctx.lookup("queues/myQueue");
      Assert.assertTrue(destination instanceof Queue);
   }

   @Test
   public void testDynamicQueue() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      Context ctx = new InitialContext(props);

      Destination destination = (Destination) ctx.lookup("dynamicQueues/myQueue");
      Assert.assertTrue(destination instanceof Queue);
   }

   @Test
   public void testTopic() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      props.put("topic.myTopic", "myTopic");
      props.put("topic.topics/myTopic", "myTopic");
      Context ctx = new InitialContext(props);

      Destination destination = (Destination) ctx.lookup("myTopic");
      Assert.assertTrue(destination instanceof Topic);

      destination = (Destination) ctx.lookup("topics/myTopic");
      Assert.assertTrue(destination instanceof Topic);
   }

   @Test
   public void testDynamicTopic() throws NamingException, JMSException
   {
      Hashtable props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      Context ctx = new InitialContext(props);

      Destination destination = (Destination) ctx.lookup("dynamicTopics/myTopic");
      Assert.assertTrue(destination instanceof Topic);
   }
}
