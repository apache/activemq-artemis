/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.api.jms.management;

import org.apache.activemq6.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.management.Operation;
import org.apache.activemq6.api.core.management.Parameter;

/**
 * A ConnectionFactoryControl is used to manage a JMS ConnectionFactory. <br>
 * HornetQ JMS ConnectionFactory uses an underlying ClientSessionFactory to connect to HornetQ
 * servers. Please refer to the ClientSessionFactory for a detailed description.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:fox@redhat.com">Tim Fox</a>
 * @see ServerLocator
 * @see ClientSessionFactory
 */
public interface ConnectionFactoryControl
{
   /**
    * Returns the configuration name of this connection factory.
    */
   String getName();

   /**
    * Returns the JNDI bindings associated  to this connection factory.
    */
   String[] getJNDIBindings();

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
    * @see ServerLocator#isCompressLargeMessage()
    */
   boolean isCompressLargeMessages();

   void setCompressLargeMessages(boolean compress);

   /**
    * @see ServerLocator#getClientFailureCheckPeriod()
    */
   long getClientFailureCheckPeriod();

   /**
    * @see ServerLocator#setClientFailureCheckPeriod
    */
   void setClientFailureCheckPeriod(long clientFailureCheckPeriod);

   /**
    * @see ServerLocator#getCallTimeout()
    */
   long getCallTimeout();

   /**
    * @see ServerLocator#setCallTimeout(long)
    */
   void setCallTimeout(long callTimeout);

   /**
    * @see ServerLocator#getCallFailoverTimeout()
    */
   long getCallFailoverTimeout();

   /**
    * @see ServerLocator#setCallFailoverTimeout(long)
    */

   void setCallFailoverTimeout(long callTimeout);

   /**
    * Returns the batch size (in bytes) between acknowledgements when using DUPS_OK_ACKNOWLEDGE
    * mode.
    *
    * @see ServerLocator#getAckBatchSize()
    * @see javax.jms.Session#DUPS_OK_ACKNOWLEDGE
    */
   int getDupsOKBatchSize();

   /**
    * @see ServerLocator#setAckBatchSize(int)
    */
   void setDupsOKBatchSize(int dupsOKBatchSize);

   /**
    * @see ServerLocator#getConsumerMaxRate()
    */
   int getConsumerMaxRate();

   /**
    * @see ServerLocator#setConsumerMaxRate(int)
    */
   void setConsumerMaxRate(int consumerMaxRate);

   /**
    * @see ServerLocator#getConsumerWindowSize()
    */
   int getConsumerWindowSize();

   /**
    * @see ServerLocator#setConfirmationWindowSize(int)
    */
   void setConsumerWindowSize(int consumerWindowSize);

   /**
    * @see ServerLocator#getProducerMaxRate()
    */
   int getProducerMaxRate();

   /**
    * @see ServerLocator#setProducerMaxRate(int)
    */
   void setProducerMaxRate(int producerMaxRate);

   /**
    * @see ServerLocator#getConfirmationWindowSize()
    */
   int getConfirmationWindowSize();

   /**
    * @see ServerLocator#setConfirmationWindowSize(int)
    */
   void setConfirmationWindowSize(int confirmationWindowSize);

   /**
    * @see ServerLocator#isBlockOnAcknowledge()
    */
   boolean isBlockOnAcknowledge();

   /**
    * @see ServerLocator#setBlockOnAcknowledge(boolean)
    */
   void setBlockOnAcknowledge(boolean blockOnAcknowledge);

   /**
    * @see ServerLocator#isBlockOnDurableSend()
    */
   boolean isBlockOnDurableSend();

   /**
    * @see ServerLocator#setBlockOnDurableSend(boolean)
    */
   void setBlockOnDurableSend(boolean blockOnDurableSend);

   /**
    * @see ServerLocator#isBlockOnNonDurableSend()
    */
   boolean isBlockOnNonDurableSend();

   /**
    * @see ServerLocator#setBlockOnNonDurableSend(boolean)
    */
   void setBlockOnNonDurableSend(boolean blockOnNonDurableSend);

   /**
    * @see ServerLocator#isPreAcknowledge()
    */
   boolean isPreAcknowledge();

   /**
    * @see ServerLocator#setPreAcknowledge(boolean)
    */
   void setPreAcknowledge(boolean preAcknowledge);


   /**
    * @see ServerLocator#getConnectionTTL()
    */
   long getConnectionTTL();

   /**
    * @see ServerLocator#setConnectionTTL(long)
    */
   void setConnectionTTL(long connectionTTL);

   /**
    * Returns the batch size (in bytes) between acknowledgements when using a transacted session.
    *
    * @see ServerLocator#getAckBatchSize()
    */
   int getTransactionBatchSize();

   /**
    * @see ServerLocator#setAckBatchSize(int)
    */
   void setTransactionBatchSize(int transactionBatchSize);

   /**
    * @see ServerLocator#getMinLargeMessageSize()
    */
   int getMinLargeMessageSize();

   /**
    * @see ServerLocator#setMinLargeMessageSize(int)
    */
   void setMinLargeMessageSize(int minLargeMessageSize);

   /**
    * @see ServerLocator#isAutoGroup()
    */
   boolean isAutoGroup();

   /**
    * @see ServerLocator#setAutoGroup(boolean)
    */
   void setAutoGroup(boolean autoGroup);

   /**
    * @see ServerLocator#getRetryInterval()
    */
   long getRetryInterval();

   /**
    * @see ServerLocator#setRetryInterval(long)
    */
   void setRetryInterval(long retryInterval);

   /**
    * @see ServerLocator#getRetryIntervalMultiplier()
    */
   double getRetryIntervalMultiplier();

   /**
    * @see ServerLocator#setRetryIntervalMultiplier(double)
    */
   void setRetryIntervalMultiplier(double retryIntervalMultiplier);

   /**
    * @see ServerLocator#getReconnectAttempts()
    */
   int getReconnectAttempts();

   /**
    * @see ServerLocator#setReconnectAttempts(int)
    */
   void setReconnectAttempts(int reconnectAttempts);

   /**
    * @see ServerLocator#isFailoverOnInitialConnection()
    */
   boolean isFailoverOnInitialConnection();

   /**
    * @see ServerLocator#setFailoverOnInitialConnection(boolean)
    */
   void setFailoverOnInitialConnection(boolean failoverOnInitialConnection);


   /**
    * @see org.apache.activemq6.api.core.client.ServerLocator#getProducerWindowSize()
    */
   int getProducerWindowSize();

   /**
    * @see ServerLocator#setProducerWindowSize(int)
    */
   void setProducerWindowSize(int producerWindowSize);

   /**
    * @see ServerLocator#isCacheLargeMessagesClient()
    */
   boolean isCacheLargeMessagesClient();

   /**
    * @see ServerLocator#setCacheLargeMessagesClient(boolean)
    */
   void setCacheLargeMessagesClient(boolean cacheLargeMessagesClient);

   /**
    * @see ServerLocator#getMaxRetryInterval()
    */
   long getMaxRetryInterval();

   /**
    * @see ServerLocator#setMaxRetryInterval(long)
    */
   void setMaxRetryInterval(long retryInterval);

   /**
    * @see ServerLocator#getScheduledThreadPoolMaxSize()
    */
   int getScheduledThreadPoolMaxSize();

   /**
    * @see ServerLocator#setScheduledThreadPoolMaxSize(int)
    */
   void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize);

   /**
    * @see ServerLocator#getThreadPoolMaxSize()
    */
   int getThreadPoolMaxSize();

   /**
    * @see ServerLocator#setThreadPoolMaxSize(int)
    */
   void setThreadPoolMaxSize(int threadPoolMaxSize);

   /**
    * @see ServerLocator#getGroupID()
    */
   String getGroupID();

   /**
    * @see ServerLocator#setGroupID(String)
    */
   void setGroupID(String groupID);

   /**
    * @see ServerLocator#getInitialMessagePacketSize()
    */
   int getInitialMessagePacketSize();

   /**
    * @see ServerLocator#isUseGlobalPools()
    */
   boolean isUseGlobalPools();

   /**
    * @see ServerLocator#setUseGlobalPools(boolean)
    */
   void setUseGlobalPools(boolean useGlobalPools);

   /**
    * @see ServerLocator#getConnectionLoadBalancingPolicyClassName()
    */
   String getConnectionLoadBalancingPolicyClassName();

   /**
    * @see ServerLocator#setConnectionLoadBalancingPolicyClassName(String)
    */
   void setConnectionLoadBalancingPolicyClassName(String connectionLoadBalancingPolicyClassName);

   /**
    * @see ClientSessionFactory#getStaticConnectors()
    */
   TransportConfiguration[] getStaticConnectors();

   /**
    * get the discovery group configuration
    */
   DiscoveryGroupConfiguration getDiscoveryGroupConfiguration();

   /**
    * Add the JNDI binding to this destination
    */
   @Operation(desc = "Adds the factory to another JNDI binding")
   void addJNDI(@Parameter(name = "jndiBinding", desc = "the name of the binding for JNDI") String jndi) throws Exception;

   /**
    * Remove a JNDI binding
    */
   @Operation(desc = "Remove an existing JNDI binding")
   void removeJNDI(@Parameter(name = "jndiBinding", desc = "the name of the binding for JNDI") String jndi) throws Exception;
}
