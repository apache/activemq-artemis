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
package org.apache.activemq.artemis.api.config;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;

public class ServerLocatorConfig {
   public long clientFailureCheckPeriod = ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;
   public long connectionTTL = ActiveMQClient.DEFAULT_CONNECTION_TTL;
   public long callTimeout = ActiveMQClient.DEFAULT_CALL_TIMEOUT;
   public long callFailoverTimeout = ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT;
   public int minLargeMessageSize = ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
   public int consumerWindowSize = ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE;
   public int consumerMaxRate = ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE;
   public int confirmationWindowSize = ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE;
   public int producerWindowSize = ActiveMQClient.DEFAULT_PRODUCER_WINDOW_SIZE;
   public int producerMaxRate = ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE;
   public boolean blockOnAcknowledge = ActiveMQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
   public boolean blockOnDurableSend = ActiveMQClient.DEFAULT_BLOCK_ON_DURABLE_SEND;
   public boolean blockOnNonDurableSend = ActiveMQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND;
   public boolean autoGroup = ActiveMQClient.DEFAULT_AUTO_GROUP;
   public boolean preAcknowledge = ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE;
   public int ackBatchSize = ActiveMQClient.DEFAULT_ACK_BATCH_SIZE;
   public String connectionLoadBalancingPolicyClassName = ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
   public boolean useGlobalPools = ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS;
   public int threadPoolMaxSize = ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE;
   public int scheduledThreadPoolMaxSize = ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;
   public long retryInterval = ActiveMQClient.DEFAULT_RETRY_INTERVAL;
   public double retryIntervalMultiplier = ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
   public long maxRetryInterval = ActiveMQClient.DEFAULT_MAX_RETRY_INTERVAL;
   public int reconnectAttempts = ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS;
   public int initialConnectAttempts = ActiveMQClient.INITIAL_CONNECT_ATTEMPTS;
   public int initialMessagePacketSize = ActiveMQClient.DEFAULT_INITIAL_MESSAGE_PACKET_SIZE;
   public boolean cacheLargeMessagesClient = ActiveMQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;
   public boolean compressLargeMessage = ActiveMQClient.DEFAULT_COMPRESS_LARGE_MESSAGES;
   public boolean useTopologyForLoadBalancing = ActiveMQClient.DEFAULT_USE_TOPOLOGY_FOR_LOADBALANCING;

   public ServerLocatorConfig() {
   }

   public ServerLocatorConfig(final ServerLocatorConfig locator) {
      compressLargeMessage = locator.compressLargeMessage;
      cacheLargeMessagesClient = locator.cacheLargeMessagesClient;
      clientFailureCheckPeriod = locator.clientFailureCheckPeriod;
      connectionTTL = locator.connectionTTL;
      callTimeout = locator.callTimeout;
      callFailoverTimeout = locator.callFailoverTimeout;
      minLargeMessageSize = locator.minLargeMessageSize;
      consumerWindowSize = locator.consumerWindowSize;
      consumerMaxRate = locator.consumerMaxRate;
      confirmationWindowSize = locator.confirmationWindowSize;
      producerWindowSize = locator.producerWindowSize;
      producerMaxRate = locator.producerMaxRate;
      blockOnAcknowledge = locator.blockOnAcknowledge;
      blockOnDurableSend = locator.blockOnDurableSend;
      blockOnNonDurableSend = locator.blockOnNonDurableSend;
      autoGroup = locator.autoGroup;
      preAcknowledge = locator.preAcknowledge;
      connectionLoadBalancingPolicyClassName = locator.connectionLoadBalancingPolicyClassName;
      ackBatchSize = locator.ackBatchSize;
      useGlobalPools = locator.useGlobalPools;
      scheduledThreadPoolMaxSize = locator.scheduledThreadPoolMaxSize;
      threadPoolMaxSize = locator.threadPoolMaxSize;
      retryInterval = locator.retryInterval;
      retryIntervalMultiplier = locator.retryIntervalMultiplier;
      maxRetryInterval = locator.maxRetryInterval;
      reconnectAttempts = locator.reconnectAttempts;
      initialConnectAttempts = locator.initialConnectAttempts;
      initialMessagePacketSize = locator.initialMessagePacketSize;
      useTopologyForLoadBalancing = locator.useTopologyForLoadBalancing;
   }
}
