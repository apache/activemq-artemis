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
package org.hornetq.jms.server.config.impl;

import java.util.ArrayList;
import java.util.List;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.utils.BufferHelper;
import org.hornetq.utils.DataConstants;

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

   private String name;

   private boolean persisted;

   private String[] bindings;

   private List<String> connectorNames;

   private String discoveryGroupName;

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

   /**
    * To be used on persistence only
    */
   public ConnectionFactoryConfigurationImpl()
   {
   }

   public ConnectionFactoryConfigurationImpl(final String name,
                                             final boolean ha,
                                             final List<String> connectorNames,
                                             final String... bindings)
   {
      this(name, ha, bindings);
      this.connectorNames = connectorNames;
   }

   public ConnectionFactoryConfigurationImpl(final String name, final boolean ha, final String... bindings)
   {
      this.name = name;
      this.ha = ha;
      this.bindings = new String[bindings.length];
      System.arraycopy(bindings, 0, this.bindings, 0, bindings.length);
   }

   // ConnectionFactoryConfiguration implementation -----------------

   public String[] getBindings()
   {
      return bindings;
   }

   public void setBindings(final String[] bindings)
   {
      this.bindings = bindings;
   }

   public String getName()
   {
      return name;
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
   public void setDiscoveryGroupName(String discoveryGroupName)
   {
      this.discoveryGroupName = discoveryGroupName;
   }

   public List<String> getConnectorNames()
   {
      return connectorNames;
   }

   public void setConnectorNames(final List<String> connectorNames)
   {
      this.connectorNames = connectorNames;
   }

   public boolean isHA()
   {
      return ha;
   }

   public void setHA(final boolean ha)
   {
      this.ha = ha;
   }

   public String getClientID()
   {
      return clientID;
   }

   public void setClientID(final String clientID)
   {
      this.clientID = clientID;
   }

   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   public void setClientFailureCheckPeriod(final long clientFailureCheckPeriod)
   {
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   public void setConnectionTTL(final long connectionTTL)
   {
      this.connectionTTL = connectionTTL;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public void setCallTimeout(final long callTimeout)
   {
      this.callTimeout = callTimeout;
   }

   public long getCallFailoverTimeout()
   {
      return callFailoverTimeout;
   }

   public void setCallFailoverTimeout(long callFailoverTimeout)
   {
      this.callFailoverTimeout = callFailoverTimeout;
   }

   public boolean isCacheLargeMessagesClient()
   {
      return cacheLargeMessagesClient;
   }

   public void setCacheLargeMessagesClient(final boolean cacheLargeMessagesClient)
   {
      this.cacheLargeMessagesClient = cacheLargeMessagesClient;
   }

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public void setMinLargeMessageSize(final int minLargeMessageSize)
   {
      this.minLargeMessageSize = minLargeMessageSize;
   }

   public int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public void setConsumerWindowSize(final int consumerWindowSize)
   {
      this.consumerWindowSize = consumerWindowSize;
   }

   public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public void setConsumerMaxRate(final int consumerMaxRate)
   {
      this.consumerMaxRate = consumerMaxRate;
   }

   public int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   public void setConfirmationWindowSize(final int confirmationWindowSize)
   {
      this.confirmationWindowSize = confirmationWindowSize;
   }

   public int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public void setProducerMaxRate(final int producerMaxRate)
   {
      this.producerMaxRate = producerMaxRate;
   }

   public int getProducerWindowSize()
   {
      return producerWindowSize;
   }

   public void setProducerWindowSize(final int producerWindowSize)
   {
      this.producerWindowSize = producerWindowSize;
   }

   public boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public void setBlockOnAcknowledge(final boolean blockOnAcknowledge)
   {
      this.blockOnAcknowledge = blockOnAcknowledge;
   }

   public boolean isBlockOnDurableSend()
   {
      return blockOnDurableSend;
   }

   public void setBlockOnDurableSend(final boolean blockOnDurableSend)
   {
      this.blockOnDurableSend = blockOnDurableSend;
   }

   public boolean isBlockOnNonDurableSend()
   {
      return blockOnNonDurableSend;
   }

   public void setBlockOnNonDurableSend(final boolean blockOnNonDurableSend)
   {
      this.blockOnNonDurableSend = blockOnNonDurableSend;
   }

   public boolean isAutoGroup()
   {
      return autoGroup;
   }

   public void setAutoGroup(final boolean autoGroup)
   {
      this.autoGroup = autoGroup;
   }

   public boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public void setPreAcknowledge(final boolean preAcknowledge)
   {
      this.preAcknowledge = preAcknowledge;
   }

   public String getLoadBalancingPolicyClassName()
   {
      return loadBalancingPolicyClassName;
   }

   public void setLoadBalancingPolicyClassName(final String loadBalancingPolicyClassName)
   {
      this.loadBalancingPolicyClassName = loadBalancingPolicyClassName;
   }

   public int getTransactionBatchSize()
   {
      return transactionBatchSize;
   }

   public void setTransactionBatchSize(final int transactionBatchSize)
   {
      this.transactionBatchSize = transactionBatchSize;
   }

   public int getDupsOKBatchSize()
   {
      return dupsOKBatchSize;
   }

   public void setDupsOKBatchSize(final int dupsOKBatchSize)
   {
      this.dupsOKBatchSize = dupsOKBatchSize;
   }

   public long getInitialWaitTimeout()
   {
      return initialWaitTimeout;
   }

   public void setInitialWaitTimeout(final long initialWaitTimeout)
   {
      this.initialWaitTimeout = initialWaitTimeout;
   }

   public boolean isUseGlobalPools()
   {
      return useGlobalPools;
   }

   public void setUseGlobalPools(final boolean useGlobalPools)
   {
      this.useGlobalPools = useGlobalPools;
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(final int scheduledThreadPoolMaxSize)
   {
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
   }

   public int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }

   public void setThreadPoolMaxSize(final int threadPoolMaxSize)
   {
      this.threadPoolMaxSize = threadPoolMaxSize;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   public void setRetryInterval(final long retryInterval)
   {
      this.retryInterval = retryInterval;
   }

   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   public void setRetryIntervalMultiplier(final double retryIntervalMultiplier)
   {
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   public long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   public void setMaxRetryInterval(final long maxRetryInterval)
   {
      this.maxRetryInterval = maxRetryInterval;
   }

   public int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public void setReconnectAttempts(final int reconnectAttempts)
   {
      this.reconnectAttempts = reconnectAttempts;
   }

   public boolean isFailoverOnInitialConnection()
   {
      return failoverOnInitialConnection;
   }

   public void setFailoverOnInitialConnection(final boolean failover)
   {
      failoverOnInitialConnection = failover;
   }

   public String getGroupID()
   {
      return groupID;
   }

   public void setGroupID(final String groupID)
   {
      this.groupID = groupID;
   }

   // Encoding Support Implementation --------------------------------------------------------------

   @Override
   public void decode(final HornetQBuffer buffer)
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
   public void encode(final HornetQBuffer buffer)
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

   public void setFactoryType(final JMSFactoryType factoryType)
   {
      this.factoryType = factoryType;
   }

   public JMSFactoryType getFactoryType()
   {
      return factoryType;
   }

   @Override
   public void setCompressLargeMessages(boolean compressLargeMessage)
   {
      this.compressLargeMessage = compressLargeMessage;
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
