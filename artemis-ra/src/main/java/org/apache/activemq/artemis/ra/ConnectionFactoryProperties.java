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
package org.apache.activemq.artemis.ra;

import java.util.List;
import java.util.Map;

public class ConnectionFactoryProperties
{
   /**
    * Trace enabled
    */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

   private boolean hasBeenUpdated = false;

   /**
    * The transport type, changing the default configured from the RA
    */
   private List<String> connectorClassName;

   /**
    * The transport config, changing the default configured from the RA
    */
   private List<Map<String, Object>> connectionParameters;

   private Boolean ha;

   private String connectionLoadBalancingPolicyClassName;

   private String jgroupsFile;

   private String jgroupsChannelName;

   private String discoveryAddress;

   private Integer discoveryPort;

   private String discoveryLocalBindAddress;

   private Long discoveryRefreshTimeout;

   private Long discoveryInitialWaitTimeout;

   private String clientID;

   private Integer dupsOKBatchSize;

   private Integer transactionBatchSize;

   private Long clientFailureCheckPeriod;

   private Long connectionTTL;

   private Boolean cacheLargeMessagesClient;

   private Long callTimeout;

   private Long callFailoverTimeout;

   private Boolean compressLargeMessage;

   private Integer consumerWindowSize;

   private Integer producerWindowSize;

   private Integer consumerMaxRate;

   private Integer confirmationWindowSize;

   private Boolean failoverOnInitialConnection;

   private Integer producerMaxRate;

   private Integer minLargeMessageSize;

   private Boolean blockOnAcknowledge;

   private Boolean blockOnNonDurableSend;

   private Boolean blockOnDurableSend;

   private Boolean autoGroup;

   private Boolean preAcknowledge;

   private Integer initialConnectAttempts;

   private Long retryInterval;

   private Double retryIntervalMultiplier;

   private Long maxRetryInterval;

   private Integer reconnectAttempts;

   private Boolean useGlobalPools;

   private Integer initialMessagePacketSize;

   private Integer scheduledThreadPoolMaxSize;

   private Integer threadPoolMaxSize;

   private String groupID;

   /**
    * @return the transportType
    */
   public List<String> getParsedConnectorClassNames()
   {
      return connectorClassName;
   }

   public List<Map<String, Object>> getParsedConnectionParameters()
   {
      return connectionParameters;
   }

   public void setParsedConnectionParameters(final List<Map<String, Object>> connectionParameters)
   {
      this.connectionParameters = connectionParameters;
      hasBeenUpdated = true;
   }

   public void setParsedConnectorClassNames(final List<String> value)
   {
      connectorClassName = value;
      hasBeenUpdated = true;
   }

   public Boolean isHA()
   {
      return ha;
   }

   public void setHA(final Boolean ha)
   {
      hasBeenUpdated = true;
      this.ha = ha;
   }

   public Boolean isCacheLargeMessagesClient()
   {
      return cacheLargeMessagesClient;
   }

   public void setCacheLargeMessagesClient(Boolean cacheLargeMessagesClient)
   {
      hasBeenUpdated = true;
      this.cacheLargeMessagesClient = cacheLargeMessagesClient;
   }

   public Boolean isCompressLargeMessage()
   {
      return compressLargeMessage;
   }

   public void setCompressLargeMessage(Boolean compressLargeMessage)
   {
      hasBeenUpdated = true;
      this.compressLargeMessage = compressLargeMessage;
   }

   public String getConnectionLoadBalancingPolicyClassName()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getConnectionLoadBalancingPolicyClassName()");
      }
      return connectionLoadBalancingPolicyClassName;
   }

   public void setConnectionLoadBalancingPolicyClassName(final String connectionLoadBalancingPolicyClassName)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setSessionDefaultType(" + connectionLoadBalancingPolicyClassName + ")");
      }
      hasBeenUpdated = true;
      this.connectionLoadBalancingPolicyClassName = connectionLoadBalancingPolicyClassName;
   }

   public String getDiscoveryAddress()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getDiscoveryAddress()");
      }
      return discoveryAddress;
   }

   public void setDiscoveryAddress(final String discoveryAddress)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setDiscoveryAddress(" + discoveryAddress + ")");
      }
      hasBeenUpdated = true;
      this.discoveryAddress = discoveryAddress;
   }

   public Integer getDiscoveryPort()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getDiscoveryPort()");
      }
      return discoveryPort;
   }

   public void setDiscoveryLocalBindAddress(final String discoveryLocalBindAddress)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setDiscoveryLocalBindAddress(" + discoveryLocalBindAddress + ")");
      }
      hasBeenUpdated = true;
      this.discoveryLocalBindAddress = discoveryLocalBindAddress;
   }

   public String getDiscoveryLocalBindAddress()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getDiscoveryLocalBindAddress()");
      }
      return discoveryLocalBindAddress;
   }

   public void setDiscoveryPort(final Integer discoveryPort)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setDiscoveryPort(" + discoveryPort + ")");
      }
      hasBeenUpdated = true;
      this.discoveryPort = discoveryPort;
   }

   public Long getDiscoveryRefreshTimeout()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getDiscoveryRefreshTimeout()");
      }
      return discoveryRefreshTimeout;
   }

   public void setDiscoveryRefreshTimeout(final Long discoveryRefreshTimeout)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setDiscoveryRefreshTimeout(" + discoveryRefreshTimeout + ")");
      }
      hasBeenUpdated = true;
      this.discoveryRefreshTimeout = discoveryRefreshTimeout;
   }

   public Long getDiscoveryInitialWaitTimeout()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getDiscoveryInitialWaitTimeout()");
      }
      return discoveryInitialWaitTimeout;
   }

   public void setDiscoveryInitialWaitTimeout(final Long discoveryInitialWaitTimeout)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setDiscoveryInitialWaitTimeout(" + discoveryInitialWaitTimeout + ")");
      }
      hasBeenUpdated = true;
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
   }

   public String getClientID()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getClientID()");
      }
      return clientID;
   }

   public void setClientID(final String clientID)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setClientID(" + clientID + ")");
      }
      hasBeenUpdated = true;
      this.clientID = clientID;
   }

   public Integer getDupsOKBatchSize()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getDupsOKBatchSize()");
      }
      return dupsOKBatchSize;
   }

   public void setDupsOKBatchSize(final Integer dupsOKBatchSize)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setDupsOKBatchSize(" + dupsOKBatchSize + ")");
      }
      hasBeenUpdated = true;
      this.dupsOKBatchSize = dupsOKBatchSize;
   }

   public Integer getTransactionBatchSize()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getTransactionBatchSize()");
      }
      return transactionBatchSize;
   }

   public void setTransactionBatchSize(final Integer transactionBatchSize)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setTransactionBatchSize(" + transactionBatchSize + ")");
      }
      hasBeenUpdated = true;
      this.transactionBatchSize = transactionBatchSize;
   }

   public Long getClientFailureCheckPeriod()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getClientFailureCheckPeriod()");
      }
      return clientFailureCheckPeriod;
   }

   public void setClientFailureCheckPeriod(final Long clientFailureCheckPeriod)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setClientFailureCheckPeriod(" + clientFailureCheckPeriod + ")");
      }
      hasBeenUpdated = true;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   public Long getConnectionTTL()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getConnectionTTL()");
      }
      return connectionTTL;
   }

   public void setConnectionTTL(final Long connectionTTL)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setConnectionTTL(" + connectionTTL + ")");
      }
      hasBeenUpdated = true;
      this.connectionTTL = connectionTTL;
   }

   public Long getCallTimeout()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getCallTimeout()");
      }
      return callTimeout;
   }

   public void setCallTimeout(final Long callTimeout)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setCallTimeout(" + callTimeout + ")");
      }
      hasBeenUpdated = true;
      this.callTimeout = callTimeout;
   }

   public Long getCallFailoverTimeout()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getCallFailoverTimeout()");
      }
      return callFailoverTimeout;
   }

   public void setCallFailoverTimeout(final Long callFailoverTimeout)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setCallFailoverTimeout(" + callFailoverTimeout + ")");
      }
      hasBeenUpdated = true;
      this.callFailoverTimeout = callFailoverTimeout;
   }

   public Integer getConsumerWindowSize()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getConsumerWindowSize()");
      }
      return consumerWindowSize;
   }

   public void setConsumerWindowSize(final Integer consumerWindowSize)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setConsumerWindowSize(" + consumerWindowSize + ")");
      }
      hasBeenUpdated = true;
      this.consumerWindowSize = consumerWindowSize;
   }

   public Integer getConsumerMaxRate()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getConsumerMaxRate()");
      }
      return consumerMaxRate;
   }

   public void setConsumerMaxRate(final Integer consumerMaxRate)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setConsumerMaxRate(" + consumerMaxRate + ")");
      }
      hasBeenUpdated = true;
      this.consumerMaxRate = consumerMaxRate;
   }

   public Integer getConfirmationWindowSize()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getConfirmationWindowSize()");
      }
      return confirmationWindowSize;
   }

   public void setConfirmationWindowSize(final Integer confirmationWindowSize)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setConfirmationWindowSize(" + confirmationWindowSize + ")");
      }
      hasBeenUpdated = true;
      this.confirmationWindowSize = confirmationWindowSize;
   }

   public Boolean isFailoverOnInitialConnection()
   {
      return failoverOnInitialConnection;
   }

   public void setFailoverOnInitialConnection(Boolean failoverOnInitialConnection)
   {
      hasBeenUpdated = true;
      this.failoverOnInitialConnection = failoverOnInitialConnection;
   }

   public Integer getProducerMaxRate()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getProducerMaxRate()");
      }
      return producerMaxRate;
   }

   public void setProducerMaxRate(final Integer producerMaxRate)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setProducerMaxRate(" + producerMaxRate + ")");
      }
      hasBeenUpdated = true;
      this.producerMaxRate = producerMaxRate;
   }

   public Integer getProducerWindowSize()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getProducerWindowSize()");
      }
      return producerWindowSize;
   }

   public void setProducerWindowSize(final Integer producerWindowSize)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setProducerWindowSize(" + producerWindowSize + ")");
      }
      hasBeenUpdated = true;
      this.producerWindowSize = producerWindowSize;
   }

   public Integer getMinLargeMessageSize()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getMinLargeMessageSize()");
      }
      return minLargeMessageSize;
   }

   public void setMinLargeMessageSize(final Integer minLargeMessageSize)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setMinLargeMessageSize(" + minLargeMessageSize + ")");
      }
      hasBeenUpdated = true;
      this.minLargeMessageSize = minLargeMessageSize;
   }

   public Boolean isBlockOnAcknowledge()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("isBlockOnAcknowledge()");
      }
      return blockOnAcknowledge;
   }

   public void setBlockOnAcknowledge(final Boolean blockOnAcknowledge)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setBlockOnAcknowledge(" + blockOnAcknowledge + ")");
      }
      hasBeenUpdated = true;
      this.blockOnAcknowledge = blockOnAcknowledge;
   }

   public Boolean isBlockOnNonDurableSend()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("isBlockOnNonDurableSend()");
      }
      return blockOnNonDurableSend;
   }

   public void setBlockOnNonDurableSend(final Boolean blockOnNonDurableSend)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setBlockOnNonDurableSend(" + blockOnNonDurableSend + ")");
      }
      hasBeenUpdated = true;
      this.blockOnNonDurableSend = blockOnNonDurableSend;
   }

   public Boolean isBlockOnDurableSend()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("isBlockOnDurableSend()");
      }
      return blockOnDurableSend;
   }

   public void setBlockOnDurableSend(final Boolean blockOnDurableSend)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setBlockOnDurableSend(" + blockOnDurableSend + ")");
      }
      hasBeenUpdated = true;
      this.blockOnDurableSend = blockOnDurableSend;
   }

   public Boolean isAutoGroup()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("isAutoGroup()");
      }
      return autoGroup;
   }

   public void setAutoGroup(final Boolean autoGroup)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setAutoGroup(" + autoGroup + ")");
      }
      hasBeenUpdated = true;
      this.autoGroup = autoGroup;
   }

   public Boolean isPreAcknowledge()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("isPreAcknowledge()");
      }
      return preAcknowledge;
   }

   public void setPreAcknowledge(final Boolean preAcknowledge)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setPreAcknowledge(" + preAcknowledge + ")");
      }
      hasBeenUpdated = true;
      this.preAcknowledge = preAcknowledge;
   }

   public Long getRetryInterval()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getRetryInterval()");
      }
      return retryInterval;
   }

   public void setRetryInterval(final Long retryInterval)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setRetryInterval(" + retryInterval + ")");
      }
      hasBeenUpdated = true;
      this.retryInterval = retryInterval;
   }

   public Double getRetryIntervalMultiplier()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getRetryIntervalMultiplier()");
      }
      return retryIntervalMultiplier;
   }

   public void setRetryIntervalMultiplier(final Double retryIntervalMultiplier)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setRetryIntervalMultiplier(" + retryIntervalMultiplier + ")");
      }
      hasBeenUpdated = true;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   public Long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   public void setMaxRetryInterval(Long maxRetryInterval)
   {
      hasBeenUpdated = true;
      this.maxRetryInterval = maxRetryInterval;
   }

   public Integer getReconnectAttempts()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getReconnectAttempts()");
      }
      return reconnectAttempts;
   }

   public void setReconnectAttempts(final Integer reconnectAttempts)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setReconnectAttempts(" + reconnectAttempts + ")");
      }
      hasBeenUpdated = true;
      this.reconnectAttempts = reconnectAttempts;
   }

   public Boolean isUseGlobalPools()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("isUseGlobalPools()");
      }
      return useGlobalPools;
   }

   public void setUseGlobalPools(final Boolean useGlobalPools)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setUseGlobalPools(" + useGlobalPools + ")");
      }
      hasBeenUpdated = true;
      this.useGlobalPools = useGlobalPools;
   }

   public Integer getScheduledThreadPoolMaxSize()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getScheduledThreadPoolMaxSize()");
      }
      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(final Integer scheduledThreadPoolMaxSize)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setScheduledThreadPoolMaxSize(" + scheduledThreadPoolMaxSize + ")");
      }
      hasBeenUpdated = true;
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
   }

   public Integer getThreadPoolMaxSize()
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getThreadPoolMaxSize()");
      }
      return threadPoolMaxSize;
   }

   public void setThreadPoolMaxSize(final Integer threadPoolMaxSize)
   {
      if (ConnectionFactoryProperties.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setThreadPoolMaxSize(" + threadPoolMaxSize + ")");
      }
      hasBeenUpdated = true;
      this.threadPoolMaxSize = threadPoolMaxSize;
   }

   public String getGroupID()
   {
      return groupID;
   }

   public void setGroupID(String groupID)
   {
      hasBeenUpdated = true;
      this.groupID = groupID;
   }

   public Integer getInitialConnectAttempts()
   {
      return initialConnectAttempts;
   }

   public void setInitialConnectAttempts(Integer initialConnectAttempts)
   {
      hasBeenUpdated = true;
      this.initialConnectAttempts = initialConnectAttempts;
   }

   public Integer getInitialMessagePacketSize()
   {
      return initialMessagePacketSize;
   }

   public void setInitialMessagePacketSize(Integer initialMessagePacketSize)
   {
      hasBeenUpdated = true;
      this.initialMessagePacketSize = initialMessagePacketSize;
   }

   public String getJgroupsFile()
   {
      return jgroupsFile;
   }

   public void setJgroupsFile(String jgroupsFile)
   {
      this.jgroupsFile = jgroupsFile;
      hasBeenUpdated = true;
   }

   public String getJgroupsChannelName()
   {
      return jgroupsChannelName;
   }

   public void setJgroupsChannelName(String jgroupsChannelName)
   {
      this.jgroupsChannelName = jgroupsChannelName;
      hasBeenUpdated = true;
   }

   public boolean isHasBeenUpdated()
   {
      return hasBeenUpdated;
   }
}
