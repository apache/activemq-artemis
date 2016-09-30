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
package org.apache.activemq.artemis.tests.integration.client;

import java.util.Arrays;

import org.apache.activemq.artemis.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SessionFactoryTest extends ActiveMQTestBase {

   private final DiscoveryGroupConfiguration groupConfiguration = new DiscoveryGroupConfiguration().setBroadcastEndpointFactory(new UDPBroadcastEndpointFactory().setGroupAddress(getUDPDiscoveryAddress()).setGroupPort(getUDPDiscoveryPort()));

   private ActiveMQServer liveService;

   private TransportConfiguration liveTC;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      startServer();
   }

   @Test
   public void testCloseUnusedClientSessionFactoryWithoutGlobalPools() throws Exception {
      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(liveTC);

      ClientSessionFactory csf = createSessionFactory(locator);
      csf.close();
   }

   @Test
   public void testDiscoveryConstructor() throws Exception {
      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(groupConfiguration);

      assertFactoryParams(locator, null, groupConfiguration, ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, ActiveMQClient.DEFAULT_CONNECTION_TTL, ActiveMQClient.DEFAULT_CALL_TIMEOUT, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE, ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE, ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE, ActiveMQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE, ActiveMQClient.DEFAULT_BLOCK_ON_DURABLE_SEND, ActiveMQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND, ActiveMQClient.DEFAULT_AUTO_GROUP, ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE, ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS, ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_RETRY_INTERVAL, ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER, ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS);

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession(false, true, true);
      Assert.assertNotNull(session);
      session.close();
      testSettersThrowException(cf);

      cf.close();

      locator.close();
   }

   @Test
   public void testStaticConnectorListConstructor() throws Exception {
      TransportConfiguration[] tc = new TransportConfiguration[]{liveTC};
      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(tc);

      assertFactoryParams(locator, tc, null, ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, ActiveMQClient.DEFAULT_CONNECTION_TTL, ActiveMQClient.DEFAULT_CALL_TIMEOUT, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE, ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE, ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE, ActiveMQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE, ActiveMQClient.DEFAULT_BLOCK_ON_DURABLE_SEND, ActiveMQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND, ActiveMQClient.DEFAULT_AUTO_GROUP, ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE, ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS, ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_RETRY_INTERVAL, ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER, ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS);

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession(false, true, true);
      Assert.assertNotNull(session);
      session.close();
      testSettersThrowException(cf);

      cf.close();
   }

   @Test
   public void testGettersAndSetters() throws Exception {
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
      int ackBatchSize = RandomUtil.randomPositiveInt();
      boolean useGlobalPools = RandomUtil.randomBoolean();
      int scheduledThreadPoolMaxSize = RandomUtil.randomPositiveInt();
      int threadPoolMaxSize = RandomUtil.randomPositiveInt();
      long retryInterval = RandomUtil.randomPositiveLong();
      double retryIntervalMultiplier = RandomUtil.randomDouble();
      int reconnectAttempts = RandomUtil.randomPositiveInt();
      TransportConfiguration[] tc = new TransportConfiguration[]{liveTC};

      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(tc).setClientFailureCheckPeriod(clientFailureCheckPeriod).setConnectionTTL(connectionTTL).setCallTimeout(callTimeout).setMinLargeMessageSize(minLargeMessageSize).setConsumerWindowSize(consumerWindowSize).setConsumerMaxRate(consumerMaxRate).setConfirmationWindowSize(confirmationWindowSize).setProducerMaxRate(producerMaxRate).setBlockOnAcknowledge(blockOnAcknowledge).setBlockOnDurableSend(blockOnDurableSend).setBlockOnNonDurableSend(blockOnNonDurableSend).setAutoGroup(autoGroup).setPreAcknowledge(preAcknowledge).setConnectionLoadBalancingPolicyClassName(loadBalancingPolicyClassName).setAckBatchSize(ackBatchSize).setUseGlobalPools(useGlobalPools).setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize).setThreadPoolMaxSize(threadPoolMaxSize).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setReconnectAttempts(reconnectAttempts);

      assertEqualsTransportConfigurations(tc, locator.getStaticTransportConfigurations());
      Assert.assertEquals(clientFailureCheckPeriod, locator.getClientFailureCheckPeriod());
      Assert.assertEquals(connectionTTL, locator.getConnectionTTL());
      Assert.assertEquals(callTimeout, locator.getCallTimeout());
      Assert.assertEquals(minLargeMessageSize, locator.getMinLargeMessageSize());
      Assert.assertEquals(consumerWindowSize, locator.getConsumerWindowSize());
      Assert.assertEquals(consumerMaxRate, locator.getConsumerMaxRate());
      Assert.assertEquals(confirmationWindowSize, locator.getConfirmationWindowSize());
      Assert.assertEquals(producerMaxRate, locator.getProducerMaxRate());
      Assert.assertEquals(blockOnAcknowledge, locator.isBlockOnAcknowledge());
      Assert.assertEquals(blockOnDurableSend, locator.isBlockOnDurableSend());
      Assert.assertEquals(blockOnNonDurableSend, locator.isBlockOnNonDurableSend());
      Assert.assertEquals(autoGroup, locator.isAutoGroup());
      Assert.assertEquals(preAcknowledge, locator.isPreAcknowledge());
      Assert.assertEquals(loadBalancingPolicyClassName, locator.getConnectionLoadBalancingPolicyClassName());
      Assert.assertEquals(ackBatchSize, locator.getAckBatchSize());
      Assert.assertEquals(useGlobalPools, locator.isUseGlobalPools());
      Assert.assertEquals(scheduledThreadPoolMaxSize, locator.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(threadPoolMaxSize, locator.getThreadPoolMaxSize());
      Assert.assertEquals(retryInterval, locator.getRetryInterval());
      Assert.assertEquals(retryIntervalMultiplier, locator.getRetryIntervalMultiplier(), 0.000001);
      Assert.assertEquals(reconnectAttempts, locator.getReconnectAttempts());
   }

   private void testSettersThrowException(final ClientSessionFactory cf) {
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
      int ackBatchSize = RandomUtil.randomPositiveInt();
      boolean useGlobalPools = RandomUtil.randomBoolean();
      int scheduledThreadPoolMaxSize = RandomUtil.randomPositiveInt();
      int threadPoolMaxSize = RandomUtil.randomPositiveInt();
      long retryInterval = RandomUtil.randomPositiveLong();
      double retryIntervalMultiplier = RandomUtil.randomDouble();
      int reconnectAttempts = RandomUtil.randomPositiveInt();

      try {
         cf.getServerLocator().setClientFailureCheckPeriod(clientFailureCheckPeriod);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setConnectionTTL(connectionTTL);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setCallTimeout(callTimeout);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setMinLargeMessageSize(minLargeMessageSize);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setConsumerWindowSize(consumerWindowSize);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setConsumerMaxRate(consumerMaxRate);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setConfirmationWindowSize(confirmationWindowSize);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setProducerMaxRate(producerMaxRate);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setBlockOnAcknowledge(blockOnAcknowledge);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setBlockOnDurableSend(blockOnDurableSend);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setBlockOnNonDurableSend(blockOnNonDurableSend);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setAutoGroup(autoGroup);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setPreAcknowledge(preAcknowledge);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setConnectionLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setAckBatchSize(ackBatchSize);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setUseGlobalPools(useGlobalPools);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setThreadPoolMaxSize(threadPoolMaxSize);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setRetryInterval(retryInterval);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setRetryIntervalMultiplier(retryIntervalMultiplier);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }
      try {
         cf.getServerLocator().setReconnectAttempts(reconnectAttempts);
         Assert.fail("Should throw exception");
      } catch (IllegalStateException e) {
         // OK
      }

      cf.getServerLocator().getStaticTransportConfigurations();
      cf.getServerLocator().getClientFailureCheckPeriod();
      cf.getServerLocator().getConnectionTTL();
      cf.getServerLocator().getCallTimeout();
      cf.getServerLocator().getMinLargeMessageSize();
      cf.getServerLocator().getConsumerWindowSize();
      cf.getServerLocator().getConsumerMaxRate();
      cf.getServerLocator().getConfirmationWindowSize();
      cf.getServerLocator().getProducerMaxRate();
      cf.getServerLocator().isBlockOnAcknowledge();
      cf.getServerLocator().isBlockOnDurableSend();
      cf.getServerLocator().isBlockOnNonDurableSend();
      cf.getServerLocator().isAutoGroup();
      cf.getServerLocator().isPreAcknowledge();
      cf.getServerLocator().getConnectionLoadBalancingPolicyClassName();
      cf.getServerLocator().getAckBatchSize();
      cf.getServerLocator().isUseGlobalPools();
      cf.getServerLocator().getScheduledThreadPoolMaxSize();
      cf.getServerLocator().getThreadPoolMaxSize();
      cf.getServerLocator().getRetryInterval();
      cf.getServerLocator().getRetryIntervalMultiplier();
      cf.getServerLocator().getReconnectAttempts();

   }

   private void assertFactoryParams(final ServerLocator locator,
                                    final TransportConfiguration[] staticConnectors,
                                    final DiscoveryGroupConfiguration discoveryGroupConfiguration,
                                    final long clientFailureCheckPeriod,
                                    final long connectionTTL,
                                    final long callTimeout,
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
                                    final int ackBatchSize,
                                    final boolean useGlobalPools,
                                    final int scheduledThreadPoolMaxSize,
                                    final int threadPoolMaxSize,
                                    final long retryInterval,
                                    final double retryIntervalMultiplier,
                                    final int reconnectAttempts) {
      if (staticConnectors == null) {
         Assert.assertTrue("no static connectors", Arrays.equals(new String[]{}, locator.getStaticTransportConfigurations()));
      } else {
         assertEqualsTransportConfigurations(staticConnectors, locator.getStaticTransportConfigurations());
      }
      Assert.assertEquals(locator.getDiscoveryGroupConfiguration(), discoveryGroupConfiguration);
      Assert.assertEquals(locator.getClientFailureCheckPeriod(), clientFailureCheckPeriod);
      Assert.assertEquals(locator.getConnectionTTL(), connectionTTL);
      Assert.assertEquals(locator.getCallTimeout(), callTimeout);
      Assert.assertEquals(locator.getMinLargeMessageSize(), minLargeMessageSize);
      Assert.assertEquals(locator.getConsumerWindowSize(), consumerWindowSize);
      Assert.assertEquals(locator.getConsumerMaxRate(), consumerMaxRate);
      Assert.assertEquals(locator.getConfirmationWindowSize(), confirmationWindowSize);
      Assert.assertEquals(locator.getProducerMaxRate(), producerMaxRate);
      Assert.assertEquals(locator.isBlockOnAcknowledge(), blockOnAcknowledge);
      Assert.assertEquals(locator.isBlockOnDurableSend(), blockOnDurableSend);
      Assert.assertEquals(locator.isBlockOnNonDurableSend(), blockOnNonDurableSend);
      Assert.assertEquals(locator.isAutoGroup(), autoGroup);
      Assert.assertEquals(locator.isPreAcknowledge(), preAcknowledge);
      Assert.assertEquals(locator.getConnectionLoadBalancingPolicyClassName(), loadBalancingPolicyClassName);
      Assert.assertEquals(locator.getAckBatchSize(), ackBatchSize);
      Assert.assertEquals(locator.isUseGlobalPools(), useGlobalPools);
      Assert.assertEquals(locator.getScheduledThreadPoolMaxSize(), scheduledThreadPoolMaxSize);
      Assert.assertEquals(locator.getThreadPoolMaxSize(), threadPoolMaxSize);
      Assert.assertEquals(locator.getRetryInterval(), retryInterval);
      Assert.assertEquals(locator.getRetryIntervalMultiplier(), retryIntervalMultiplier, 0.000001);
      Assert.assertEquals(locator.getReconnectAttempts(), reconnectAttempts);
   }

   private void startServer() throws Exception {
      liveTC = new TransportConfiguration(INVM_CONNECTOR_FACTORY);

      final long broadcastPeriod = 250;

      final String bcGroupName = "bc1";

      final int localBindPort = 5432;

      BroadcastGroupConfiguration broadcastGroupConfiguration = new BroadcastGroupConfiguration().setName(bcGroupName).setBroadcastPeriod(broadcastPeriod).setConnectorInfos(Arrays.asList(liveTC.getName())).setEndpointFactory(new UDPBroadcastEndpointFactory().setGroupAddress(getUDPDiscoveryAddress()).setGroupPort(getUDPDiscoveryPort()).setLocalBindPort(localBindPort));

      Configuration liveConf = createDefaultInVMConfig().addConnectorConfiguration(liveTC.getName(), liveTC).setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration()).addBroadcastGroupConfiguration(broadcastGroupConfiguration);

      liveService = createServer(false, liveConf);
      liveService.start();
   }
}
