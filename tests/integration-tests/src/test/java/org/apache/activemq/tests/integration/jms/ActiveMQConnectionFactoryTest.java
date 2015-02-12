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
import org.apache.activemq.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.junit.Before;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.junit.Assert;

import org.apache.activemq.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.jms.ActiveMQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.UnitTestCase;

/**
 *
 * A ActiveMQConnectionFactoryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ActiveMQConnectionFactoryTest extends UnitTestCase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private final String groupAddress = getUDPDiscoveryAddress();

   private final int groupPort = getUDPDiscoveryPort();

   private ActiveMQServer liveService;

   private TransportConfiguration liveTC;

   @Test
   public void testDefaultConstructor() throws Exception
   {
      ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF);
      assertFactoryParams(cf,
                          null,
                          null,
                          null,
                          ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                          ActiveMQClient.DEFAULT_CONNECTION_TTL,
                          ActiveMQClient.DEFAULT_CALL_TIMEOUT,
                          ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                          ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                          ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                          ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE,
                          ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                          ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE,
                          ActiveMQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                          ActiveMQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
                          ActiveMQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                          ActiveMQClient.DEFAULT_AUTO_GROUP,
                          ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE,
                          ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                          ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                          ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                          ActiveMQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS,
                          ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          ActiveMQClient.DEFAULT_RETRY_INTERVAL,
                          ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS);
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Assert.fail("Should throw exception");
      }
      catch (JMSException e)
      {
         // Ok
      }
      if (conn != null)
      {
         conn.close();
      }

      ActiveMQConnectionFactoryTest.log.info("Got here");

      testSettersThrowException(cf);
   }

   @Test
   public void testDefaultConstructorAndSetConnectorPairs() throws Exception
   {
      ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, liveTC);

      assertFactoryParams(cf,
                          new TransportConfiguration[]{liveTC},
                          null,
                          null,
                          ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                          ActiveMQClient.DEFAULT_CONNECTION_TTL,
                          ActiveMQClient.DEFAULT_CALL_TIMEOUT,
                          ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                          ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                          ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                          ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE,
                          ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                          ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE,
                          ActiveMQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                          ActiveMQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
                          ActiveMQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                          ActiveMQClient.DEFAULT_AUTO_GROUP,
                          ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE,
                          ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                          ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                          ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                          ActiveMQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS,
                          ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          ActiveMQClient.DEFAULT_RETRY_INTERVAL,
                          ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS);

      Connection conn = cf.createConnection();

      conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      testSettersThrowException(cf);

      conn.close();
   }

   @Test
   public void testDiscoveryConstructor() throws Exception
   {
      DiscoveryGroupConfiguration groupConfiguration = new DiscoveryGroupConfiguration()
         .setBroadcastEndpointFactory(new UDPBroadcastEndpointFactory()
                                            .setGroupAddress(groupAddress)
                                            .setGroupPort(groupPort));
      ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(groupConfiguration, JMSFactoryType.CF);
      assertFactoryParams(cf,
                          null,
                          groupConfiguration,
                          null,
                          ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                          ActiveMQClient.DEFAULT_CONNECTION_TTL,
                          ActiveMQClient.DEFAULT_CALL_TIMEOUT,
                          ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                          ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                          ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                          ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE,
                          ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                          ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE,
                          ActiveMQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                          ActiveMQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
                          ActiveMQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                          ActiveMQClient.DEFAULT_AUTO_GROUP,
                          ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE,
                          ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                          ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                          ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                          ActiveMQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS,
                          ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          ActiveMQClient.DEFAULT_RETRY_INTERVAL,
                          ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS);
      Connection conn = cf.createConnection();

      conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      testSettersThrowException(cf);

      conn.close();
   }

   @Test
   public void testStaticConnectorListConstructor() throws Exception
   {
      ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, liveTC);
      assertFactoryParams(cf,
                          new TransportConfiguration[]{liveTC},
                          null,
                          null,
                          ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                          ActiveMQClient.DEFAULT_CONNECTION_TTL,
                          ActiveMQClient.DEFAULT_CALL_TIMEOUT,
                          ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                          ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                          ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                          ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE,
                          ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                          ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE,
                          ActiveMQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                          ActiveMQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
                          ActiveMQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                          ActiveMQClient.DEFAULT_AUTO_GROUP,
                          ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE,
                          ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                          ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                          ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                          ActiveMQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS,
                          ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          ActiveMQClient.DEFAULT_RETRY_INTERVAL,
                          ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS);
      Connection conn = cf.createConnection();

      conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      testSettersThrowException(cf);

      conn.close();

   }

   @Test
   public void testStaticConnectorLiveConstructor() throws Exception
   {
      ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, liveTC);
      assertFactoryParams(cf,
                          new TransportConfiguration[]{liveTC},
                          null,
                          null,
                          ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                          ActiveMQClient.DEFAULT_CONNECTION_TTL,
                          ActiveMQClient.DEFAULT_CALL_TIMEOUT,
                          ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                          ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                          ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                          ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE,
                          ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                          ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE,
                          ActiveMQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                          ActiveMQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
                          ActiveMQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                          ActiveMQClient.DEFAULT_AUTO_GROUP,
                          ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE,
                          ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                          ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                          ActiveMQClient.DEFAULT_ACK_BATCH_SIZE,
                          ActiveMQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                          ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS,
                          ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                          ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                          ActiveMQClient.DEFAULT_RETRY_INTERVAL,
                          ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS);
      Connection conn = cf.createConnection();

      conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      testSettersThrowException(cf);

      cf.close();

      conn.close();
   }


   @Test
   public void testGettersAndSetters()
   {
      ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, liveTC);

      long clientFailureCheckPeriod = RandomUtil.randomPositiveLong();
      long connectionTTL = RandomUtil.randomPositiveLong();
      long callTimeout = RandomUtil.randomPositiveLong();
      int minLargeMessageSize = RandomUtil.randomPositiveInt();
      int consumerWindowSize = RandomUtil.randomPositiveInt();
      int consumerMaxRate = RandomUtil.randomPositiveInt();
      int confirmationWindowSize = RandomUtil.randomPositiveInt();
      int producerMaxRate = RandomUtil.randomPositiveInt();
      boolean blockOnAcknowledge = RandomUtil.randomBoolean();
      boolean blockOnDurableSend = RandomUtil.randomBoolean();
      boolean blockOnNonDurableSend = RandomUtil.randomBoolean();
      boolean autoGroup = RandomUtil.randomBoolean();
      boolean preAcknowledge = RandomUtil.randomBoolean();
      String loadBalancingPolicyClassName = RandomUtil.randomString();
      boolean useGlobalPools = RandomUtil.randomBoolean();
      int scheduledThreadPoolMaxSize = RandomUtil.randomPositiveInt();
      int threadPoolMaxSize = RandomUtil.randomPositiveInt();
      long retryInterval = RandomUtil.randomPositiveLong();
      double retryIntervalMultiplier = RandomUtil.randomDouble();
      int reconnectAttempts = RandomUtil.randomPositiveInt();
      cf.setClientFailureCheckPeriod(clientFailureCheckPeriod);
      cf.setConnectionTTL(connectionTTL);
      cf.setCallTimeout(callTimeout);
      cf.setMinLargeMessageSize(minLargeMessageSize);
      cf.setConsumerWindowSize(consumerWindowSize);
      cf.setConsumerMaxRate(consumerMaxRate);
      cf.setConfirmationWindowSize(confirmationWindowSize);
      cf.setProducerMaxRate(producerMaxRate);
      cf.setBlockOnAcknowledge(blockOnAcknowledge);
      cf.setBlockOnDurableSend(blockOnDurableSend);
      cf.setBlockOnNonDurableSend(blockOnNonDurableSend);
      cf.setAutoGroup(autoGroup);
      cf.setPreAcknowledge(preAcknowledge);
      cf.setConnectionLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
      cf.setUseGlobalPools(useGlobalPools);
      cf.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
      cf.setThreadPoolMaxSize(threadPoolMaxSize);
      cf.setRetryInterval(retryInterval);
      cf.setRetryIntervalMultiplier(retryIntervalMultiplier);
      cf.setReconnectAttempts(reconnectAttempts);
      Assert.assertEquals(clientFailureCheckPeriod, cf.getClientFailureCheckPeriod());
      Assert.assertEquals(connectionTTL, cf.getConnectionTTL());
      Assert.assertEquals(callTimeout, cf.getCallTimeout());
      Assert.assertEquals(minLargeMessageSize, cf.getMinLargeMessageSize());
      Assert.assertEquals(consumerWindowSize, cf.getConsumerWindowSize());
      Assert.assertEquals(consumerMaxRate, cf.getConsumerMaxRate());
      Assert.assertEquals(confirmationWindowSize, cf.getConfirmationWindowSize());
      Assert.assertEquals(producerMaxRate, cf.getProducerMaxRate());
      Assert.assertEquals(blockOnAcknowledge, cf.isBlockOnAcknowledge());
      Assert.assertEquals(blockOnDurableSend, cf.isBlockOnDurableSend());
      Assert.assertEquals(blockOnNonDurableSend, cf.isBlockOnNonDurableSend());
      Assert.assertEquals(autoGroup, cf.isAutoGroup());
      Assert.assertEquals(preAcknowledge, cf.isPreAcknowledge());
      Assert.assertEquals(loadBalancingPolicyClassName, cf.getConnectionLoadBalancingPolicyClassName());
      Assert.assertEquals(useGlobalPools, cf.isUseGlobalPools());
      Assert.assertEquals(scheduledThreadPoolMaxSize, cf.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(threadPoolMaxSize, cf.getThreadPoolMaxSize());
      Assert.assertEquals(retryInterval, cf.getRetryInterval());
      Assert.assertEquals(retryIntervalMultiplier, cf.getRetryIntervalMultiplier(), 0.0001);
      Assert.assertEquals(reconnectAttempts, cf.getReconnectAttempts());

      cf.close();
   }

   private void testSettersThrowException(final ActiveMQConnectionFactory cf)
   {

      String discoveryAddress = RandomUtil.randomString();
      int discoveryPort = RandomUtil.randomPositiveInt();
      long discoveryRefreshTimeout = RandomUtil.randomPositiveLong();
      String clientID = RandomUtil.randomString();
      long clientFailureCheckPeriod = RandomUtil.randomPositiveLong();
      long connectionTTL = RandomUtil.randomPositiveLong();
      long callTimeout = RandomUtil.randomPositiveLong();
      int minLargeMessageSize = RandomUtil.randomPositiveInt();
      int consumerWindowSize = RandomUtil.randomPositiveInt();
      int consumerMaxRate = RandomUtil.randomPositiveInt();
      int confirmationWindowSize = RandomUtil.randomPositiveInt();
      int producerMaxRate = RandomUtil.randomPositiveInt();
      boolean blockOnAcknowledge = RandomUtil.randomBoolean();
      boolean blockOnDurableSend = RandomUtil.randomBoolean();
      boolean blockOnNonDurableSend = RandomUtil.randomBoolean();
      boolean autoGroup = RandomUtil.randomBoolean();
      boolean preAcknowledge = RandomUtil.randomBoolean();
      String loadBalancingPolicyClassName = RandomUtil.randomString();
      int dupsOKBatchSize = RandomUtil.randomPositiveInt();
      int transactionBatchSize = RandomUtil.randomPositiveInt();
      long initialWaitTimeout = RandomUtil.randomPositiveLong();
      boolean useGlobalPools = RandomUtil.randomBoolean();
      int scheduledThreadPoolMaxSize = RandomUtil.randomPositiveInt();
      int threadPoolMaxSize = RandomUtil.randomPositiveInt();
      long retryInterval = RandomUtil.randomPositiveLong();
      double retryIntervalMultiplier = RandomUtil.randomDouble();
      int reconnectAttempts = RandomUtil.randomPositiveInt();
      boolean failoverOnServerShutdown = RandomUtil.randomBoolean();

      try
      {
         cf.setClientID(clientID);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setClientFailureCheckPeriod(clientFailureCheckPeriod);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setConnectionTTL(connectionTTL);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setCallTimeout(callTimeout);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setMinLargeMessageSize(minLargeMessageSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setConsumerWindowSize(consumerWindowSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setConsumerMaxRate(consumerMaxRate);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setConfirmationWindowSize(confirmationWindowSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setProducerMaxRate(producerMaxRate);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setBlockOnAcknowledge(blockOnAcknowledge);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setBlockOnDurableSend(blockOnDurableSend);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setBlockOnNonDurableSend(blockOnNonDurableSend);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setAutoGroup(autoGroup);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setPreAcknowledge(preAcknowledge);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setConnectionLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setDupsOKBatchSize(dupsOKBatchSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setTransactionBatchSize(transactionBatchSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setUseGlobalPools(useGlobalPools);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setThreadPoolMaxSize(threadPoolMaxSize);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setRetryInterval(retryInterval);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setRetryIntervalMultiplier(retryIntervalMultiplier);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }
      try
      {
         cf.setReconnectAttempts(reconnectAttempts);
         Assert.fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         // OK
      }

      cf.getStaticConnectors();
      cf.getClientID();
      cf.getClientFailureCheckPeriod();
      cf.getConnectionTTL();
      cf.getCallTimeout();
      cf.getMinLargeMessageSize();
      cf.getConsumerWindowSize();
      cf.getConsumerMaxRate();
      cf.getConfirmationWindowSize();
      cf.getProducerMaxRate();
      cf.isBlockOnAcknowledge();
      cf.isBlockOnDurableSend();
      cf.isBlockOnNonDurableSend();
      cf.isAutoGroup();
      cf.isPreAcknowledge();
      cf.getConnectionLoadBalancingPolicyClassName();
      cf.getDupsOKBatchSize();
      cf.getTransactionBatchSize();
      cf.isUseGlobalPools();
      cf.getScheduledThreadPoolMaxSize();
      cf.getThreadPoolMaxSize();
      cf.getRetryInterval();
      cf.getRetryIntervalMultiplier();
      cf.getReconnectAttempts();

      cf.close();
   }

   private void assertFactoryParams(final ActiveMQConnectionFactory cf,
                                    final TransportConfiguration[] staticConnectors,
                                    final DiscoveryGroupConfiguration discoveryGroupConfiguration,
                                    final String clientID,
                                    final long clientFailureCheckPeriod,
                                    final long connectionTTL,
                                    final long callTimeout,
                                    final long callFailoverTimeout,
                                    final int minLargeMessageSize,
                                    final int consumerWindowSize,
                                    final int consumerMaxRate,
                                    final int confirmationWindowSize,
                                    final int producerMaxRate,
                                    final boolean blockOnAcknowledge,
                                    final boolean blockOnDurableSend,
                                    final boolean blockOnNonDurableSend,
                                    final boolean autoGroup,
                                    final boolean preAcknowledge,
                                    final String loadBalancingPolicyClassName,
                                    final int dupsOKBatchSize,
                                    final int transactionBatchSize,
                                    final long initialWaitTimeout,
                                    final boolean useGlobalPools,
                                    final int scheduledThreadPoolMaxSize,
                                    final int threadPoolMaxSize,
                                    final long retryInterval,
                                    final double retryIntervalMultiplier,
                                    final int reconnectAttempts)
   {
      TransportConfiguration[] cfStaticConnectors = cf.getStaticConnectors();
      if (staticConnectors == null)
      {
         Assert.assertNull(staticConnectors);
      }
      else
      {
         Assert.assertEquals(staticConnectors.length, cfStaticConnectors.length);

         for (int i = 0; i < staticConnectors.length; i++)
         {
            Assert.assertEquals(staticConnectors[i], cfStaticConnectors[i]);
         }
      }
      Assert.assertEquals(cf.getClientID(), clientID);
      Assert.assertEquals(cf.getClientFailureCheckPeriod(), clientFailureCheckPeriod);
      Assert.assertEquals(cf.getConnectionTTL(), connectionTTL);
      Assert.assertEquals(cf.getCallTimeout(), callTimeout);
      Assert.assertEquals(cf.getCallFailoverTimeout(), callFailoverTimeout);
      Assert.assertEquals(cf.getMinLargeMessageSize(), minLargeMessageSize);
      Assert.assertEquals(cf.getConsumerWindowSize(), consumerWindowSize);
      Assert.assertEquals(cf.getConsumerMaxRate(), consumerMaxRate);
      Assert.assertEquals(cf.getConfirmationWindowSize(), confirmationWindowSize);
      Assert.assertEquals(cf.getProducerMaxRate(), producerMaxRate);
      Assert.assertEquals(cf.isBlockOnAcknowledge(), blockOnAcknowledge);
      Assert.assertEquals(cf.isBlockOnDurableSend(), blockOnDurableSend);
      Assert.assertEquals(cf.isBlockOnNonDurableSend(), blockOnNonDurableSend);
      Assert.assertEquals(cf.isAutoGroup(), autoGroup);
      Assert.assertEquals(cf.isPreAcknowledge(), preAcknowledge);
      Assert.assertEquals(cf.getConnectionLoadBalancingPolicyClassName(), loadBalancingPolicyClassName);
      Assert.assertEquals(cf.getDupsOKBatchSize(), dupsOKBatchSize);
      Assert.assertEquals(cf.getTransactionBatchSize(), transactionBatchSize);
      Assert.assertEquals(cf.isUseGlobalPools(), useGlobalPools);
      Assert.assertEquals(cf.getScheduledThreadPoolMaxSize(), scheduledThreadPoolMaxSize);
      Assert.assertEquals(cf.getThreadPoolMaxSize(), threadPoolMaxSize);
      Assert.assertEquals(cf.getRetryInterval(), retryInterval);
      Assert.assertEquals(cf.getRetryIntervalMultiplier(), retryIntervalMultiplier, 0.00001);
      Assert.assertEquals(cf.getReconnectAttempts(), reconnectAttempts);
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

      Configuration liveConf = createBasicConfig()
         .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY))
         .setConnectorConfigurations(connectors)
         .setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration());

      final long broadcastPeriod = 250;

      final String bcGroupName = "bc1";

      final int localBindPort = 5432;

      BroadcastGroupConfiguration bcConfig1 = new BroadcastGroupConfiguration()
         .setName(bcGroupName)
         .setBroadcastPeriod(broadcastPeriod)
         .setConnectorInfos(connectorNames)
         .setEndpointFactory(new UDPBroadcastEndpointFactory()
                                   .setGroupAddress(groupAddress)
                                   .setGroupPort(groupPort)
                                   .setLocalBindPort(localBindPort));

      List<BroadcastGroupConfiguration> bcConfigs1 = new ArrayList<BroadcastGroupConfiguration>();
      bcConfigs1.add(bcConfig1);
      liveConf.setBroadcastGroupConfigurations(bcConfigs1);

      liveService = addServer(ActiveMQServers.newActiveMQServer(liveConf, false));
      liveService.start();
   }

}
