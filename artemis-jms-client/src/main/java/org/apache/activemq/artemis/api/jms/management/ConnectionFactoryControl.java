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
package org.apache.activemq.artemis.api.jms.management;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.Operation;
import org.apache.activemq.artemis.api.core.management.Parameter;

/**
 * A ConnectionFactoryControl is used to manage a JMS ConnectionFactory. <br>
 * ActiveMQ Artemis JMS ConnectionFactory uses an underlying ClientSessionFactory to connect to ActiveMQ
 * servers. Please refer to the ClientSessionFactory for a detailed description.
 *
 * @see org.apache.activemq.artemis.api.core.client.ServerLocator
 * @see org.apache.activemq.artemis.api.core.client.ClientSessionFactory
 */
public interface ConnectionFactoryControl {

   /**
    * Returns the configuration name of this connection factory.
    */
   String getName();

   /**
    * Returns the Registry bindings associated  to this connection factory.
    */
   String[] getRegistryBindings();

   /**
    * does ths cf support HA
    *
    * @return true if it supports HA
    */
   boolean isHA();

   /**
    * return the type of factory
    *
    * @return 0 = jms cf, 1 = queue cf, 2 = topic cf, 3 = xa cf, 4 = xa queue cf, 5 = xa topic cf
    */
   int getFactoryType();

   /**
    * Returns the Client ID of this connection factory (or {@code null} if it is not set.
    */
   String getClientID();

   /**
    * Sets the Client ID for this connection factory.
    */
   void setClientID(String clientID);

   /**
    * @return whether large messages are compressed
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#isCompressLargeMessage()
    */
   boolean isCompressLargeMessages();

   void setCompressLargeMessages(boolean compress);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getClientFailureCheckPeriod()
    */
   long getClientFailureCheckPeriod();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setClientFailureCheckPeriod
    */
   void setClientFailureCheckPeriod(long clientFailureCheckPeriod);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getCallTimeout()
    */
   long getCallTimeout();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setCallTimeout(long)
    */
   void setCallTimeout(long callTimeout);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getCallFailoverTimeout()
    */
   long getCallFailoverTimeout();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setCallFailoverTimeout(long)
    */

   void setCallFailoverTimeout(long callTimeout);

   /**
    * Returns the batch size (in bytes) between acknowledgements when using DUPS_OK_ACKNOWLEDGE
    * mode.
    *
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getAckBatchSize()
    * @see javax.jms.Session#DUPS_OK_ACKNOWLEDGE
    */
   int getDupsOKBatchSize();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setAckBatchSize(int)
    */
   void setDupsOKBatchSize(int dupsOKBatchSize);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getConsumerMaxRate()
    */
   int getConsumerMaxRate();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setConsumerMaxRate(int)
    */
   void setConsumerMaxRate(int consumerMaxRate);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getConsumerWindowSize()
    */
   int getConsumerWindowSize();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setConfirmationWindowSize(int)
    */
   void setConsumerWindowSize(int consumerWindowSize);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getProducerMaxRate()
    */
   int getProducerMaxRate();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setProducerMaxRate(int)
    */
   void setProducerMaxRate(int producerMaxRate);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getConfirmationWindowSize()
    */
   int getConfirmationWindowSize();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setConfirmationWindowSize(int)
    */
   void setConfirmationWindowSize(int confirmationWindowSize);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#isBlockOnAcknowledge()
    */
   boolean isBlockOnAcknowledge();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setBlockOnAcknowledge(boolean)
    */
   void setBlockOnAcknowledge(boolean blockOnAcknowledge);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#isBlockOnDurableSend()
    */
   boolean isBlockOnDurableSend();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setBlockOnDurableSend(boolean)
    */
   void setBlockOnDurableSend(boolean blockOnDurableSend);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#isBlockOnNonDurableSend()
    */
   boolean isBlockOnNonDurableSend();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setBlockOnNonDurableSend(boolean)
    */
   void setBlockOnNonDurableSend(boolean blockOnNonDurableSend);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#isPreAcknowledge()
    */
   boolean isPreAcknowledge();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setPreAcknowledge(boolean)
    */
   void setPreAcknowledge(boolean preAcknowledge);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getConnectionTTL()
    */
   long getConnectionTTL();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setConnectionTTL(long)
    */
   void setConnectionTTL(long connectionTTL);

   /**
    * Returns the batch size (in bytes) between acknowledgements when using a transacted session.
    *
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getAckBatchSize()
    */
   int getTransactionBatchSize();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setAckBatchSize(int)
    */
   void setTransactionBatchSize(int transactionBatchSize);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getMinLargeMessageSize()
    */
   int getMinLargeMessageSize();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setMinLargeMessageSize(int)
    */
   void setMinLargeMessageSize(int minLargeMessageSize);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#isAutoGroup()
    */
   boolean isAutoGroup();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setAutoGroup(boolean)
    */
   void setAutoGroup(boolean autoGroup);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getRetryInterval()
    */
   long getRetryInterval();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setRetryInterval(long)
    */
   void setRetryInterval(long retryInterval);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getRetryIntervalMultiplier()
    */
   double getRetryIntervalMultiplier();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setRetryIntervalMultiplier(double)
    */
   void setRetryIntervalMultiplier(double retryIntervalMultiplier);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getReconnectAttempts()
    */
   int getReconnectAttempts();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setReconnectAttempts(int)
    */
   void setReconnectAttempts(int reconnectAttempts);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#isFailoverOnInitialConnection()
    */
   boolean isFailoverOnInitialConnection();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setFailoverOnInitialConnection(boolean)
    */
   void setFailoverOnInitialConnection(boolean failoverOnInitialConnection);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getProducerWindowSize()
    */
   int getProducerWindowSize();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setProducerWindowSize(int)
    */
   void setProducerWindowSize(int producerWindowSize);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#isCacheLargeMessagesClient()
    */
   boolean isCacheLargeMessagesClient();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setCacheLargeMessagesClient(boolean)
    */
   void setCacheLargeMessagesClient(boolean cacheLargeMessagesClient);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getMaxRetryInterval()
    */
   long getMaxRetryInterval();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setMaxRetryInterval(long)
    */
   void setMaxRetryInterval(long retryInterval);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getScheduledThreadPoolMaxSize()
    */
   int getScheduledThreadPoolMaxSize();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setScheduledThreadPoolMaxSize(int)
    */
   void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getThreadPoolMaxSize()
    */
   int getThreadPoolMaxSize();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setThreadPoolMaxSize(int)
    */
   void setThreadPoolMaxSize(int threadPoolMaxSize);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getGroupID()
    */
   String getGroupID();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setGroupID(String)
    */
   void setGroupID(String groupID);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getInitialMessagePacketSize()
    */
   int getInitialMessagePacketSize();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#isUseGlobalPools()
    */
   boolean isUseGlobalPools();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setUseGlobalPools(boolean)
    */
   void setUseGlobalPools(boolean useGlobalPools);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getConnectionLoadBalancingPolicyClassName()
    */
   String getConnectionLoadBalancingPolicyClassName();

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#setConnectionLoadBalancingPolicyClassName(String)
    */
   void setConnectionLoadBalancingPolicyClassName(String connectionLoadBalancingPolicyClassName);

   /**
    * @see org.apache.activemq.artemis.api.core.client.ServerLocator#getStaticTransportConfigurations()
    */
   TransportConfiguration[] getStaticConnectors();

   /**
    * get the discovery group configuration
    */
   DiscoveryGroupConfiguration getDiscoveryGroupConfiguration();

   /**
    * Add the Registry binding to this destination
    */
   @Operation(desc = "Adds the factory to another Registry binding")
   void addBinding(@Parameter(name = "binding", desc = "the name of the binding for the Registry") String binding) throws Exception;

   /**
    * Remove a Registry binding
    */
   @Operation(desc = "Remove an existing Registry binding")
   void removeBinding(@Parameter(name = "binding", desc = "the name of the binding for Registry") String binding) throws Exception;
}
