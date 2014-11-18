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
package org.apache.activemq.jms.server.config.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.HornetQClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.utils.BufferHelper;
import org.apache.activemq.utils.DataConstants;

/**
 * This class contains the configuration properties of a connection factory.
 * <p>
 * It is also persisted on the journal at the time of management is used to created a connection factory and set to store.
 * <p>
 * Every property on this class has to be also set through encoders through EncodingSupport implementation at this class.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ConnectionFactoryConfigurationImpl implements ConnectionFactoryConfiguration
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String name = null;

   private boolean persisted = false;

   private String[] bindings = null;

   private List<String> connectorNames = null;

   private String discoveryGroupName = null;

   private String clientID = null;

   private boolean ha = HornetQClient.DEFAULT_HA;

   private long clientFailureCheckPeriod = HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;

   private long connectionTTL = HornetQClient.DEFAULT_CONNECTION_TTL;

   private long callTimeout = HornetQClient.DEFAULT_CALL_TIMEOUT;

   private long callFailoverTimeout = HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT;

   private boolean cacheLargeMessagesClient = HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;

   private int minLargeMessageSize = HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

   private boolean compressLargeMessage = HornetQClient.DEFAULT_COMPRESS_LARGE_MESSAGES;

   private int consumerWindowSize = HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE;

   private int consumerMaxRate = HornetQClient.DEFAULT_CONSUMER_MAX_RATE;

   private int confirmationWindowSize = HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE;

   private int producerWindowSize = HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE;

   private int producerMaxRate = HornetQClient.DEFAULT_PRODUCER_MAX_RATE;

   private boolean blockOnAcknowledge = HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE;

   private boolean blockOnDurableSend = HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND;

   private boolean blockOnNonDurableSend = HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND;

   private boolean autoGroup = HornetQClient.DEFAULT_AUTO_GROUP;

   private boolean preAcknowledge = HornetQClient.DEFAULT_PRE_ACKNOWLEDGE;

   private String loadBalancingPolicyClassName = HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;

   private int transactionBatchSize = HornetQClient.DEFAULT_ACK_BATCH_SIZE;

   private int dupsOKBatchSize = HornetQClient.DEFAULT_ACK_BATCH_SIZE;

   private long initialWaitTimeout = HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT;

   private boolean useGlobalPools = HornetQClient.DEFAULT_USE_GLOBAL_POOLS;

   private int scheduledThreadPoolMaxSize = HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;

   private int threadPoolMaxSize = HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE;

   private long retryInterval = HornetQClient.DEFAULT_RETRY_INTERVAL;

   private double retryIntervalMultiplier = HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER;

   private long maxRetryInterval = HornetQClient.DEFAULT_MAX_RETRY_INTERVAL;

   private int reconnectAttempts = HornetQClient.DEFAULT_RECONNECT_ATTEMPTS;

   private boolean failoverOnInitialConnection = HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION;

   private String groupID = null;

   private JMSFactoryType factoryType = JMSFactoryType.CF;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionFactoryConfigurationImpl()
   {
   }

   // ConnectionFactoryConfiguration implementation -----------------

   public String[] getBindings()
   {
      return bindings;
   }

   public ConnectionFactoryConfiguration setBindings(final String... bindings)
   {
      this.bindings = bindings;
      return this;
   }

   public String getName()
   {
      return name;
   }

   public ConnectionFactoryConfiguration setName(String name)
   {
      this.name = name;
      return this;
   }

   public boolean isPersisted()
   {
      return persisted;
   }

   /**
    * @return the discoveryGroupName
    */
   public String getDiscoveryGroupName()
   {
      return discoveryGroupName;
   }

   /**
    * @param discoveryGroupName the discoveryGroupName to set
    */
   public ConnectionFactoryConfiguration setDiscoveryGroupName(String discoveryGroupName)
   {
      this.discoveryGroupName = discoveryGroupName;
      return this;
   }

   public List<String> getConnectorNames()
   {
      return connectorNames;
   }

   public ConnectionFactoryConfiguration setConnectorNames(final List<String> connectorNames)
   {
      this.connectorNames = connectorNames;
      return this;
   }

   public boolean isHA()
   {
      return ha;
   }

   public ConnectionFactoryConfiguration setHA(final boolean ha)
   {
      this.ha = ha;
      return this;
   }

   public String getClientID()
   {
      return clientID;
   }

   public ConnectionFactoryConfiguration setClientID(final String clientID)
   {
      this.clientID = clientID;
      return this;
   }

   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   public ConnectionFactoryConfiguration setClientFailureCheckPeriod(final long clientFailureCheckPeriod)
   {
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      return this;
   }

   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   public ConnectionFactoryConfiguration setConnectionTTL(final long connectionTTL)
   {
      this.connectionTTL = connectionTTL;
      return this;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public ConnectionFactoryConfiguration setCallTimeout(final long callTimeout)
   {
      this.callTimeout = callTimeout;
      return this;
   }

   public long getCallFailoverTimeout()
   {
      return callFailoverTimeout;
   }

   public ConnectionFactoryConfiguration setCallFailoverTimeout(long callFailoverTimeout)
   {
      this.callFailoverTimeout = callFailoverTimeout;
      return this;
   }

   public boolean isCacheLargeMessagesClient()
   {
      return cacheLargeMessagesClient;
   }

   public ConnectionFactoryConfiguration setCacheLargeMessagesClient(final boolean cacheLargeMessagesClient)
   {
      this.cacheLargeMessagesClient = cacheLargeMessagesClient;
      return this;
   }

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public ConnectionFactoryConfiguration setMinLargeMessageSize(final int minLargeMessageSize)
   {
      this.minLargeMessageSize = minLargeMessageSize;
      return this;
   }

   public int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public ConnectionFactoryConfiguration setConsumerWindowSize(final int consumerWindowSize)
   {
      this.consumerWindowSize = consumerWindowSize;
      return this;
   }

   public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public ConnectionFactoryConfiguration setConsumerMaxRate(final int consumerMaxRate)
   {
      this.consumerMaxRate = consumerMaxRate;
      return this;
   }

   public int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   public ConnectionFactoryConfiguration setConfirmationWindowSize(final int confirmationWindowSize)
   {
      this.confirmationWindowSize = confirmationWindowSize;
      return this;
   }

   public int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public ConnectionFactoryConfiguration setProducerMaxRate(final int producerMaxRate)
   {
      this.producerMaxRate = producerMaxRate;
      return this;
   }

   public int getProducerWindowSize()
   {
      return producerWindowSize;
   }

   public ConnectionFactoryConfiguration setProducerWindowSize(final int producerWindowSize)
   {
      this.producerWindowSize = producerWindowSize;
      return this;
   }

   public boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public ConnectionFactoryConfiguration setBlockOnAcknowledge(final boolean blockOnAcknowledge)
   {
      this.blockOnAcknowledge = blockOnAcknowledge;
      return this;
   }

   public boolean isBlockOnDurableSend()
   {
      return blockOnDurableSend;
   }

   public ConnectionFactoryConfiguration setBlockOnDurableSend(final boolean blockOnDurableSend)
   {
      this.blockOnDurableSend = blockOnDurableSend;
      return this;
   }

   public boolean isBlockOnNonDurableSend()
   {
      return blockOnNonDurableSend;
   }

   public ConnectionFactoryConfiguration setBlockOnNonDurableSend(final boolean blockOnNonDurableSend)
   {
      this.blockOnNonDurableSend = blockOnNonDurableSend;
      return this;
   }

   public boolean isAutoGroup()
   {
      return autoGroup;
   }

   public ConnectionFactoryConfiguration setAutoGroup(final boolean autoGroup)
   {
      this.autoGroup = autoGroup;
      return this;
   }

   public boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public ConnectionFactoryConfiguration setPreAcknowledge(final boolean preAcknowledge)
   {
      this.preAcknowledge = preAcknowledge;
      return this;
   }

   public String getLoadBalancingPolicyClassName()
   {
      return loadBalancingPolicyClassName;
   }

   public ConnectionFactoryConfiguration setLoadBalancingPolicyClassName(final String loadBalancingPolicyClassName)
   {
      this.loadBalancingPolicyClassName = loadBalancingPolicyClassName;
      return this;
   }

   public int getTransactionBatchSize()
   {
      return transactionBatchSize;
   }

   public ConnectionFactoryConfiguration setTransactionBatchSize(final int transactionBatchSize)
   {
      this.transactionBatchSize = transactionBatchSize;
      return this;
   }

   public int getDupsOKBatchSize()
   {
      return dupsOKBatchSize;
   }

   public ConnectionFactoryConfiguration setDupsOKBatchSize(final int dupsOKBatchSize)
   {
      this.dupsOKBatchSize = dupsOKBatchSize;
      return this;
   }

   public long getInitialWaitTimeout()
   {
      return initialWaitTimeout;
   }

   public ConnectionFactoryConfiguration setInitialWaitTimeout(final long initialWaitTimeout)
   {
      this.initialWaitTimeout = initialWaitTimeout;
      return this;
   }

   public boolean isUseGlobalPools()
   {
      return useGlobalPools;
   }

   public ConnectionFactoryConfiguration setUseGlobalPools(final boolean useGlobalPools)
   {
      this.useGlobalPools = useGlobalPools;
      return this;
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public ConnectionFactoryConfiguration setScheduledThreadPoolMaxSize(final int scheduledThreadPoolMaxSize)
   {
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
      return this;
   }

   public int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }

   public ConnectionFactoryConfiguration setThreadPoolMaxSize(final int threadPoolMaxSize)
   {
      this.threadPoolMaxSize = threadPoolMaxSize;
      return this;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   public ConnectionFactoryConfiguration setRetryInterval(final long retryInterval)
   {
      this.retryInterval = retryInterval;
      return this;
   }

   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   public ConnectionFactoryConfiguration setRetryIntervalMultiplier(final double retryIntervalMultiplier)
   {
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      return this;
   }

   public long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   public ConnectionFactoryConfiguration setMaxRetryInterval(final long maxRetryInterval)
   {
      this.maxRetryInterval = maxRetryInterval;
      return this;
   }

   public int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public ConnectionFactoryConfiguration setReconnectAttempts(final int reconnectAttempts)
   {
      this.reconnectAttempts = reconnectAttempts;
      return this;
   }

   public boolean isFailoverOnInitialConnection()
   {
      return failoverOnInitialConnection;
   }

   public ConnectionFactoryConfiguration setFailoverOnInitialConnection(final boolean failover)
   {
      failoverOnInitialConnection = failover;
      return this;
   }

   public String getGroupID()
   {
      return groupID;
   }

   public ConnectionFactoryConfiguration setGroupID(final String groupID)
   {
      this.groupID = groupID;
      return this;
   }

   // Encoding Support Implementation --------------------------------------------------------------

   @Override
   public void decode(final ActiveMQBuffer buffer)
   {
      persisted = true;

      name = buffer.readSimpleString().toString();

      discoveryGroupName = BufferHelper.readNullableSimpleStringAsString(buffer);

      int nConnectors = buffer.readInt();

      if (nConnectors > 0)
      {
         connectorNames = new ArrayList<String>(nConnectors);

         for (int i = 0; i < nConnectors; i++)
         {
            SimpleString str = buffer.readSimpleString();

            connectorNames.add(str.toString());
         }
      }

      ha = buffer.readBoolean();

      clientID = BufferHelper.readNullableSimpleStringAsString(buffer);

      clientFailureCheckPeriod = buffer.readLong();

      connectionTTL = buffer.readLong();

      callTimeout = buffer.readLong();

      cacheLargeMessagesClient = buffer.readBoolean();

      minLargeMessageSize = buffer.readInt();

      consumerWindowSize = buffer.readInt();

      consumerMaxRate = buffer.readInt();

      confirmationWindowSize = buffer.readInt();

      producerWindowSize = buffer.readInt();

      producerMaxRate = buffer.readInt();

      blockOnAcknowledge = buffer.readBoolean();

      blockOnDurableSend = buffer.readBoolean();

      blockOnNonDurableSend = buffer.readBoolean();

      autoGroup = buffer.readBoolean();

      preAcknowledge = buffer.readBoolean();

      loadBalancingPolicyClassName = buffer.readSimpleString().toString();

      transactionBatchSize = buffer.readInt();

      dupsOKBatchSize = buffer.readInt();

      initialWaitTimeout = buffer.readLong();

      useGlobalPools = buffer.readBoolean();

      scheduledThreadPoolMaxSize = buffer.readInt();

      threadPoolMaxSize = buffer.readInt();

      retryInterval = buffer.readLong();

      retryIntervalMultiplier = buffer.readDouble();

      maxRetryInterval = buffer.readLong();

      reconnectAttempts = buffer.readInt();

      failoverOnInitialConnection = buffer.readBoolean();

      compressLargeMessage = buffer.readBoolean();

      groupID = BufferHelper.readNullableSimpleStringAsString(buffer);

      factoryType = JMSFactoryType.valueOf(buffer.readInt());
   }

   @Override
   public void encode(final ActiveMQBuffer buffer)
   {
      persisted = true;

      BufferHelper.writeAsSimpleString(buffer, name);

      BufferHelper.writeAsNullableSimpleString(buffer, discoveryGroupName);

      if (this.connectorNames == null)
      {
         buffer.writeInt(0);
      }
      else
      {
         buffer.writeInt(connectorNames.size());

         for (String tc : connectorNames)
         {
            BufferHelper.writeAsSimpleString(buffer, tc);
         }
      }

      buffer.writeBoolean(ha);

      BufferHelper.writeAsNullableSimpleString(buffer, clientID);

      buffer.writeLong(clientFailureCheckPeriod);

      buffer.writeLong(connectionTTL);

      buffer.writeLong(callTimeout);

      buffer.writeBoolean(cacheLargeMessagesClient);

      buffer.writeInt(minLargeMessageSize);

      buffer.writeInt(consumerWindowSize);

      buffer.writeInt(consumerMaxRate);

      buffer.writeInt(confirmationWindowSize);

      buffer.writeInt(producerWindowSize);

      buffer.writeInt(producerMaxRate);

      buffer.writeBoolean(blockOnAcknowledge);

      buffer.writeBoolean(blockOnDurableSend);

      buffer.writeBoolean(blockOnNonDurableSend);

      buffer.writeBoolean(autoGroup);

      buffer.writeBoolean(preAcknowledge);

      BufferHelper.writeAsSimpleString(buffer, loadBalancingPolicyClassName);

      buffer.writeInt(transactionBatchSize);

      buffer.writeInt(dupsOKBatchSize);

      buffer.writeLong(initialWaitTimeout);

      buffer.writeBoolean(useGlobalPools);

      buffer.writeInt(scheduledThreadPoolMaxSize);

      buffer.writeInt(threadPoolMaxSize);

      buffer.writeLong(retryInterval);

      buffer.writeDouble(retryIntervalMultiplier);

      buffer.writeLong(maxRetryInterval);

      buffer.writeInt(reconnectAttempts);

      buffer.writeBoolean(failoverOnInitialConnection);

      buffer.writeBoolean(compressLargeMessage);

      BufferHelper.writeAsNullableSimpleString(buffer, groupID);

      buffer.writeInt(factoryType.intValue());
   }

   @Override
   public int getEncodeSize()
   {
      int size = BufferHelper.sizeOfSimpleString(name) +

         BufferHelper.sizeOfNullableSimpleString(discoveryGroupName);

      size += DataConstants.SIZE_INT;

      if (this.connectorNames != null)
      {
         for (String tc : connectorNames)
         {
            size += BufferHelper.sizeOfSimpleString(tc);
         }
      }

      size += BufferHelper.sizeOfNullableSimpleString(clientID) +

         DataConstants.SIZE_BOOLEAN +
         // ha

         DataConstants.SIZE_LONG +
         // clientFailureCheckPeriod

         DataConstants.SIZE_LONG +
         // connectionTTL

         DataConstants.SIZE_LONG +
         // callTimeout

         DataConstants.SIZE_BOOLEAN +
         // cacheLargeMessagesClient

         DataConstants.SIZE_INT +
         // minLargeMessageSize

         DataConstants.SIZE_INT +
         // consumerWindowSize

         DataConstants.SIZE_INT +
         // consumerMaxRate

         DataConstants.SIZE_INT +
         // confirmationWindowSize

         DataConstants.SIZE_INT +
         // producerWindowSize

         DataConstants.SIZE_INT +
         // producerMaxRate

         DataConstants.SIZE_BOOLEAN +
         // blockOnAcknowledge

         DataConstants.SIZE_BOOLEAN +
         // blockOnDurableSend

         DataConstants.SIZE_BOOLEAN +
         // blockOnNonDurableSend

         DataConstants.SIZE_BOOLEAN +
         // autoGroup

         DataConstants.SIZE_BOOLEAN +
         // preAcknowledge

         BufferHelper.sizeOfSimpleString(loadBalancingPolicyClassName) +

         DataConstants.SIZE_INT +
         // transactionBatchSize

         DataConstants.SIZE_INT +
         // dupsOKBatchSize

         DataConstants.SIZE_LONG +
         // initialWaitTimeout

         DataConstants.SIZE_BOOLEAN +
         // useGlobalPools

         DataConstants.SIZE_INT +
         // scheduledThreadPoolMaxSize

         DataConstants.SIZE_INT +
         // threadPoolMaxSize

         DataConstants.SIZE_LONG +
         // retryInterval

         DataConstants.SIZE_DOUBLE +
         // retryIntervalMultiplier

         DataConstants.SIZE_LONG +
         // maxRetryInterval

         DataConstants.SIZE_INT +
         // reconnectAttempts

         DataConstants.SIZE_BOOLEAN +
         // failoverOnInitialConnection

         DataConstants.SIZE_BOOLEAN +
         // compress-large-message

         BufferHelper.sizeOfNullableSimpleString(groupID) +

         DataConstants.SIZE_INT; // factoryType

      return size;
   }

   public ConnectionFactoryConfiguration setFactoryType(final JMSFactoryType factoryType)
   {
      this.factoryType = factoryType;
      return this;
   }

   public JMSFactoryType getFactoryType()
   {
      return factoryType;
   }

   @Override
   public ConnectionFactoryConfiguration setCompressLargeMessages(boolean compressLargeMessage)
   {
      this.compressLargeMessage = compressLargeMessage;
      return this;
   }

   @Override
   public boolean isCompressLargeMessages()
   {
      return this.compressLargeMessage;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
