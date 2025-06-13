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
package org.apache.activemq.artemis.ra;

import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;
import java.util.Objects;

public class ConnectionFactoryProperties implements ConnectionFactoryOptions {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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

   private Integer compressionLevel;

   private Integer consumerWindowSize;

   private Integer producerWindowSize;

   private Integer consumerMaxRate;

   private Integer confirmationWindowSize;

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

   private Boolean cacheDestinations;

   private Boolean enable1xPrefixes;

   private Integer initialMessagePacketSize;

   private Integer scheduledThreadPoolMaxSize;

   private Integer threadPoolMaxSize;

   private String groupID;

   private String protocolManagerFactoryStr;

   private String deserializationDenyList;

   private String deserializationAllowList;

   private Boolean enableSharedClientID;

   public List<String> getParsedConnectorClassNames() {
      return connectorClassName;
   }

   public List<Map<String, Object>> getParsedConnectionParameters() {
      return connectionParameters;
   }

   public void setParsedConnectionParameters(final List<Map<String, Object>> connectionParameters) {
      this.connectionParameters = connectionParameters;
      hasBeenUpdated = true;
   }

   public void setParsedConnectorClassNames(final List<String> value) {
      connectorClassName = value;
      hasBeenUpdated = true;
   }

   public Boolean isHA() {
      return ha;
   }

   public void setHA(final Boolean ha) {
      hasBeenUpdated = true;
      this.ha = ha;
   }

   public Boolean isCacheLargeMessagesClient() {
      return cacheLargeMessagesClient;
   }

   public void setCacheLargeMessagesClient(Boolean cacheLargeMessagesClient) {
      hasBeenUpdated = true;
      this.cacheLargeMessagesClient = cacheLargeMessagesClient;
   }

   public Boolean isCompressLargeMessage() {
      return compressLargeMessage;
   }

   public void setCompressLargeMessage(Boolean compressLargeMessage) {
      hasBeenUpdated = true;
      this.compressLargeMessage = compressLargeMessage;
   }

   public Integer getCompressionLevel() {
      return compressionLevel;
   }

   public void setCompressionLevel(Integer compressionLevel) {
      hasBeenUpdated = true;
      this.compressionLevel = compressionLevel;
   }

   public String getConnectionLoadBalancingPolicyClassName() {
      logger.trace("getConnectionLoadBalancingPolicyClassName()");

      return connectionLoadBalancingPolicyClassName;
   }

   public void setConnectionLoadBalancingPolicyClassName(final String connectionLoadBalancingPolicyClassName) {
      logger.trace("setSessionDefaultType({})", connectionLoadBalancingPolicyClassName);

      hasBeenUpdated = true;
      this.connectionLoadBalancingPolicyClassName = connectionLoadBalancingPolicyClassName;
   }

   public String getDiscoveryAddress() {
      logger.trace("getDiscoveryAddress()");

      return discoveryAddress;
   }

   public void setDiscoveryAddress(final String discoveryAddress) {
      logger.trace("setDiscoveryAddress({})", discoveryAddress);

      hasBeenUpdated = true;
      this.discoveryAddress = discoveryAddress;
   }

   public Integer getDiscoveryPort() {
      logger.trace("getDiscoveryPort()");

      return discoveryPort;
   }

   public void setDiscoveryLocalBindAddress(final String discoveryLocalBindAddress) {
      logger.trace("setDiscoveryLocalBindAddress({})", discoveryLocalBindAddress);

      hasBeenUpdated = true;
      this.discoveryLocalBindAddress = discoveryLocalBindAddress;
   }

   public String getDiscoveryLocalBindAddress() {
      logger.trace("getDiscoveryLocalBindAddress()");

      return discoveryLocalBindAddress;
   }

   public void setDiscoveryPort(final Integer discoveryPort) {
      logger.trace("setDiscoveryPort({})", discoveryPort);

      hasBeenUpdated = true;
      this.discoveryPort = discoveryPort;
   }

   public Long getDiscoveryRefreshTimeout() {
      logger.trace("getDiscoveryRefreshTimeout()");

      return discoveryRefreshTimeout;
   }

   public void setDiscoveryRefreshTimeout(final Long discoveryRefreshTimeout) {
      logger.trace("setDiscoveryRefreshTimeout({})", discoveryRefreshTimeout);

      hasBeenUpdated = true;
      this.discoveryRefreshTimeout = discoveryRefreshTimeout;
   }

   public Long getDiscoveryInitialWaitTimeout() {
      logger.trace("getDiscoveryInitialWaitTimeout()");

      return discoveryInitialWaitTimeout;
   }

   public void setDiscoveryInitialWaitTimeout(final Long discoveryInitialWaitTimeout) {
      logger.trace("setDiscoveryInitialWaitTimeout({})", discoveryInitialWaitTimeout);

      hasBeenUpdated = true;
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
   }

   public String getClientID() {
      logger.trace("getClientID()");

      return clientID;
   }

   public void setClientID(final String clientID) {
      logger.trace("setClientID({})", clientID);

      hasBeenUpdated = true;
      this.clientID = clientID;
   }

   public Integer getDupsOKBatchSize() {
      logger.trace("getDupsOKBatchSize()");

      return dupsOKBatchSize;
   }

   public void setDupsOKBatchSize(final Integer dupsOKBatchSize) {
      logger.trace("setDupsOKBatchSize({})", dupsOKBatchSize);

      hasBeenUpdated = true;
      this.dupsOKBatchSize = dupsOKBatchSize;
   }

   public Integer getTransactionBatchSize() {
      logger.trace("getTransactionBatchSize()");

      return transactionBatchSize;
   }

   public void setTransactionBatchSize(final Integer transactionBatchSize) {
      logger.trace("setTransactionBatchSize({})", transactionBatchSize);

      hasBeenUpdated = true;
      this.transactionBatchSize = transactionBatchSize;
   }

   public Long getClientFailureCheckPeriod() {
      logger.trace("getClientFailureCheckPeriod()");

      return clientFailureCheckPeriod;
   }

   public void setClientFailureCheckPeriod(final Long clientFailureCheckPeriod) {
      logger.trace("setClientFailureCheckPeriod({})", clientFailureCheckPeriod);

      hasBeenUpdated = true;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   public Long getConnectionTTL() {
      logger.trace("getConnectionTTL()");

      return connectionTTL;
   }

   public void setConnectionTTL(final Long connectionTTL) {
      logger.trace("setConnectionTTL({})", connectionTTL);

      hasBeenUpdated = true;
      this.connectionTTL = connectionTTL;
   }

   public Long getCallTimeout() {
      logger.trace("getCallTimeout()");

      return callTimeout;
   }

   public void setCallTimeout(final Long callTimeout) {
      logger.trace("setCallTimeout({})", callTimeout);

      hasBeenUpdated = true;
      this.callTimeout = callTimeout;
   }

   public Long getCallFailoverTimeout() {
      logger.trace("getCallFailoverTimeout()");

      return callFailoverTimeout;
   }

   public void setCallFailoverTimeout(final Long callFailoverTimeout) {
      logger.trace("setCallFailoverTimeout({})", callFailoverTimeout);

      hasBeenUpdated = true;
      this.callFailoverTimeout = callFailoverTimeout;
   }

   public Integer getConsumerWindowSize() {
      logger.trace("getConsumerWindowSize()");

      return consumerWindowSize;
   }

   public void setConsumerWindowSize(final Integer consumerWindowSize) {
      logger.trace("setConsumerWindowSize({})", consumerWindowSize);

      hasBeenUpdated = true;
      this.consumerWindowSize = consumerWindowSize;
   }

   public Integer getConsumerMaxRate() {
      logger.trace("getConsumerMaxRate()");

      return consumerMaxRate;
   }

   public void setConsumerMaxRate(final Integer consumerMaxRate) {
      logger.trace("setConsumerMaxRate({})", consumerMaxRate);

      hasBeenUpdated = true;
      this.consumerMaxRate = consumerMaxRate;
   }

   public Integer getConfirmationWindowSize() {
      logger.trace("getConfirmationWindowSize()");

      return confirmationWindowSize;
   }

   public void setConfirmationWindowSize(final Integer confirmationWindowSize) {
      logger.trace("setConfirmationWindowSize({})", confirmationWindowSize);

      hasBeenUpdated = true;
      this.confirmationWindowSize = confirmationWindowSize;
   }

   @Deprecated
   public Boolean isFailoverOnInitialConnection() {
      return false;
   }

   @Deprecated
   public void setFailoverOnInitialConnection(Boolean failoverOnInitialConnection) {
   }

   public Integer getProducerMaxRate() {
      logger.trace("getProducerMaxRate()");

      return producerMaxRate;
   }

   public void setProducerMaxRate(final Integer producerMaxRate) {
      logger.trace("setProducerMaxRate({})", producerMaxRate);

      hasBeenUpdated = true;
      this.producerMaxRate = producerMaxRate;
   }

   public Integer getProducerWindowSize() {
      logger.trace("getProducerWindowSize()");

      return producerWindowSize;
   }

   public void setProducerWindowSize(final Integer producerWindowSize) {
      logger.trace("setProducerWindowSize({})", producerWindowSize);

      hasBeenUpdated = true;
      this.producerWindowSize = producerWindowSize;
   }

   public Integer getMinLargeMessageSize() {
      logger.trace("getMinLargeMessageSize()");

      return minLargeMessageSize;
   }

   public void setMinLargeMessageSize(final Integer minLargeMessageSize) {
      logger.trace("setMinLargeMessageSize({})", minLargeMessageSize);

      hasBeenUpdated = true;
      this.minLargeMessageSize = minLargeMessageSize;
   }

   public Boolean isBlockOnAcknowledge() {
      logger.trace("isBlockOnAcknowledge()");

      return blockOnAcknowledge;
   }

   public void setBlockOnAcknowledge(final Boolean blockOnAcknowledge) {
      logger.trace("setBlockOnAcknowledge({})", blockOnAcknowledge);

      hasBeenUpdated = true;
      this.blockOnAcknowledge = blockOnAcknowledge;
   }

   public Boolean isBlockOnNonDurableSend() {
      logger.trace("isBlockOnNonDurableSend()");

      return blockOnNonDurableSend;
   }

   public void setBlockOnNonDurableSend(final Boolean blockOnNonDurableSend) {
      logger.trace("setBlockOnNonDurableSend({})", blockOnNonDurableSend);

      hasBeenUpdated = true;
      this.blockOnNonDurableSend = blockOnNonDurableSend;
   }

   public Boolean isBlockOnDurableSend() {
      logger.trace("isBlockOnDurableSend()");

      return blockOnDurableSend;
   }

   public void setBlockOnDurableSend(final Boolean blockOnDurableSend) {
      logger.trace("setBlockOnDurableSend({})", blockOnDurableSend);

      hasBeenUpdated = true;
      this.blockOnDurableSend = blockOnDurableSend;
   }

   public Boolean isAutoGroup() {
      logger.trace("isAutoGroup()");

      return autoGroup;
   }

   public void setAutoGroup(final Boolean autoGroup) {
      logger.trace("setAutoGroup({})", autoGroup);

      hasBeenUpdated = true;
      this.autoGroup = autoGroup;
   }

   public Boolean isPreAcknowledge() {
      logger.trace("isPreAcknowledge()");

      return preAcknowledge;
   }

   public void setPreAcknowledge(final Boolean preAcknowledge) {
      logger.trace("setPreAcknowledge({})", preAcknowledge);

      hasBeenUpdated = true;
      this.preAcknowledge = preAcknowledge;
   }

   public Long getRetryInterval() {
      logger.trace("getRetryInterval()");

      return retryInterval;
   }

   public void setRetryInterval(final Long retryInterval) {
      logger.trace("setRetryInterval({})", retryInterval);

      hasBeenUpdated = true;
      this.retryInterval = retryInterval;
   }

   public Double getRetryIntervalMultiplier() {
      logger.trace("getRetryIntervalMultiplier()");

      return retryIntervalMultiplier;
   }

   public void setRetryIntervalMultiplier(final Double retryIntervalMultiplier) {
      logger.trace("setRetryIntervalMultiplier({})", retryIntervalMultiplier);

      hasBeenUpdated = true;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   public Long getMaxRetryInterval() {
      return maxRetryInterval;
   }

   public void setMaxRetryInterval(Long maxRetryInterval) {
      hasBeenUpdated = true;
      this.maxRetryInterval = maxRetryInterval;
   }

   public Integer getReconnectAttempts() {
      logger.trace("getReconnectAttempts()");

      return reconnectAttempts;
   }

   public void setReconnectAttempts(final Integer reconnectAttempts) {
      logger.trace("setReconnectAttempts({})", reconnectAttempts);

      hasBeenUpdated = true;
      this.reconnectAttempts = reconnectAttempts;
   }

   public Boolean isUseGlobalPools() {
      logger.trace("isUseGlobalPools()");

      return useGlobalPools;
   }

   public void setUseGlobalPools(final Boolean useGlobalPools) {
      logger.trace("setUseGlobalPools({})", useGlobalPools);

      hasBeenUpdated = true;
      this.useGlobalPools = useGlobalPools;
   }

   public Boolean isCacheDestinations() {
      logger.trace("isCacheDestinations()");

      return cacheDestinations;
   }

   public void setCacheDestinations(final Boolean cacheDestinations) {
      logger.trace("setCacheDestinations({})", cacheDestinations);

      hasBeenUpdated = true;
      this.cacheDestinations = cacheDestinations;
   }

   public Boolean isEnable1xPrefixes() {
      logger.trace("isEnable1xPrefixes()");

      return enable1xPrefixes;
   }

   public void setEnable1xPrefixes(final Boolean enable1xPrefixes) {
      logger.trace("setEnable1xPrefixes({})", enable1xPrefixes);

      hasBeenUpdated = true;
      this.enable1xPrefixes = enable1xPrefixes;
   }

   public Integer getScheduledThreadPoolMaxSize() {
      logger.trace("getScheduledThreadPoolMaxSize()");

      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(final Integer scheduledThreadPoolMaxSize) {
      logger.trace("setScheduledThreadPoolMaxSize({})", scheduledThreadPoolMaxSize);

      hasBeenUpdated = true;
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
   }

   public Integer getThreadPoolMaxSize() {
      logger.trace("getThreadPoolMaxSize()");

      return threadPoolMaxSize;
   }

   public void setThreadPoolMaxSize(final Integer threadPoolMaxSize) {
      logger.trace("setThreadPoolMaxSize({})", threadPoolMaxSize);

      hasBeenUpdated = true;
      this.threadPoolMaxSize = threadPoolMaxSize;
   }

   public String getGroupID() {
      return groupID;
   }

   public void setGroupID(String groupID) {
      hasBeenUpdated = true;
      this.groupID = groupID;
   }

   public Integer getInitialConnectAttempts() {
      return initialConnectAttempts;
   }

   public void setInitialConnectAttempts(Integer initialConnectAttempts) {
      hasBeenUpdated = true;
      this.initialConnectAttempts = initialConnectAttempts;
   }

   public Integer getInitialMessagePacketSize() {
      return initialMessagePacketSize;
   }

   public void setInitialMessagePacketSize(Integer initialMessagePacketSize) {
      hasBeenUpdated = true;
      this.initialMessagePacketSize = initialMessagePacketSize;
   }

   public String getJgroupsFile() {
      return jgroupsFile;
   }

   public void setJgroupsFile(String jgroupsFile) {
      this.jgroupsFile = jgroupsFile;
      hasBeenUpdated = true;
   }

   public String getJgroupsChannelName() {
      return jgroupsChannelName;
   }

   public void setJgroupsChannelName(String jgroupsChannelName) {
      this.jgroupsChannelName = jgroupsChannelName;
      hasBeenUpdated = true;
   }

   public String getProtocolManagerFactoryStr() {
      return protocolManagerFactoryStr;
   }

   public void setProtocolManagerFactoryStr(String protocolManagerFactoryStr) {
      this.protocolManagerFactoryStr = protocolManagerFactoryStr;
   }

   @Override
   @Deprecated(forRemoval = true)
   public String getDeserializationBlackList() {
      return deserializationDenyList;
   }

   @Override
   @Deprecated(forRemoval = true)
   public void setDeserializationBlackList(String deserializationDenyList) {
      this.deserializationDenyList = deserializationDenyList;
      hasBeenUpdated = true;
   }

   @Override
   @Deprecated(forRemoval = true)
   public String getDeserializationWhiteList() {
      return this.deserializationAllowList;
   }

   @Override
   @Deprecated(forRemoval = true)
   public void setDeserializationWhiteList(String deserializationAllowList) {
      this.deserializationAllowList = deserializationAllowList;
      hasBeenUpdated = true;
   }

   @Override
   public String getDeserializationDenyList() {
      return deserializationDenyList;
   }

   @Override
   public void setDeserializationDenyList(String deserializationDenyList) {
      this.deserializationDenyList = deserializationDenyList;
      hasBeenUpdated = true;
   }

   @Override
   public String getDeserializationAllowList() {
      return this.deserializationAllowList;
   }

   @Override
   public void setDeserializationAllowList(String deserializationAllowList) {
      this.deserializationAllowList = deserializationAllowList;
      hasBeenUpdated = true;
   }

   public boolean isHasBeenUpdated() {
      return hasBeenUpdated;
   }

   // This is here just for backward compatibility and not used
   public void setEnableSharedClientID(boolean enable) {
      this.enableSharedClientID = enable;
   }

   public boolean isEnableSharedClientID() {
      return Objects.requireNonNullElse(enableSharedClientID, false);
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      ConnectionFactoryProperties other = (ConnectionFactoryProperties) obj;
      if (this.autoGroup == null) {
         if (other.autoGroup != null)
            return false;
      } else if (!this.autoGroup.equals(other.autoGroup))
         return false;
      if (this.blockOnAcknowledge == null) {
         if (other.blockOnAcknowledge != null)
            return false;
      } else if (!this.blockOnAcknowledge.equals(other.blockOnAcknowledge))
         return false;
      if (this.blockOnDurableSend == null) {
         if (other.blockOnDurableSend != null)
            return false;
      } else if (!this.blockOnDurableSend.equals(other.blockOnDurableSend))
         return false;
      if (this.blockOnNonDurableSend == null) {
         if (other.blockOnNonDurableSend != null)
            return false;
      } else if (!this.blockOnNonDurableSend.equals(other.blockOnNonDurableSend))
         return false;
      if (this.cacheLargeMessagesClient == null) {
         if (other.cacheLargeMessagesClient != null)
            return false;
      } else if (!this.cacheLargeMessagesClient.equals(other.cacheLargeMessagesClient))
         return false;
      if (this.compressLargeMessage == null) {
         if (other.compressLargeMessage != null)
            return false;
      } else if (!this.compressLargeMessage.equals(other.compressLargeMessage))
         return false;
      if (this.ha == null) {
         if (other.ha != null)
            return false;
      } else if (!this.ha.equals(other.ha))
         return false;
      if (this.preAcknowledge == null) {
         if (other.preAcknowledge != null)
            return false;
      } else if (!this.preAcknowledge.equals(other.preAcknowledge))
         return false;
      if (this.callFailoverTimeout == null) {
         if (other.callFailoverTimeout != null)
            return false;
      } else if (!this.callFailoverTimeout.equals(other.callFailoverTimeout))
         return false;
      if (this.callTimeout == null) {
         if (other.callTimeout != null)
            return false;
      } else if (!this.callTimeout.equals(other.callTimeout))
         return false;
      if (this.clientFailureCheckPeriod == null) {
         if (other.clientFailureCheckPeriod != null)
            return false;
      } else if (!this.clientFailureCheckPeriod.equals(other.clientFailureCheckPeriod))
         return false;
      if (this.clientID == null) {
         if (other.clientID != null)
            return false;
      } else if (!this.clientID.equals(other.clientID))
         return false;
      if (this.confirmationWindowSize == null) {
         if (other.confirmationWindowSize != null)
            return false;
      } else if (!this.confirmationWindowSize.equals(other.confirmationWindowSize))
         return false;
      if (this.connectionLoadBalancingPolicyClassName == null) {
         if (other.connectionLoadBalancingPolicyClassName != null)
            return false;
      } else if (!this.connectionLoadBalancingPolicyClassName.equals(other.connectionLoadBalancingPolicyClassName))
         return false;
      if (this.connectionTTL == null) {
         if (other.connectionTTL != null)
            return false;
      } else if (!this.connectionTTL.equals(other.connectionTTL))
         return false;
      if (this.consumerMaxRate == null) {
         if (other.consumerMaxRate != null)
            return false;
      } else if (!this.consumerMaxRate.equals(other.consumerMaxRate))
         return false;
      if (this.consumerWindowSize == null) {
         if (other.consumerWindowSize != null)
            return false;
      } else if (!this.consumerWindowSize.equals(other.consumerWindowSize))
         return false;
      if (this.discoveryAddress == null) {
         if (other.discoveryAddress != null)
            return false;
      } else if (!this.discoveryAddress.equals(other.discoveryAddress))
         return false;
      if (this.discoveryInitialWaitTimeout == null) {
         if (other.discoveryInitialWaitTimeout != null)
            return false;
      } else if (!this.discoveryInitialWaitTimeout.equals(other.discoveryInitialWaitTimeout))
         return false;
      if (this.discoveryLocalBindAddress == null) {
         if (other.discoveryLocalBindAddress != null)
            return false;
      } else if (!this.discoveryLocalBindAddress.equals(other.discoveryLocalBindAddress))
         return false;
      if (this.discoveryPort == null) {
         if (other.discoveryPort != null)
            return false;
      } else if (!this.discoveryPort.equals(other.discoveryPort))
         return false;
      if (this.discoveryRefreshTimeout == null) {
         if (other.discoveryRefreshTimeout != null)
            return false;
      } else if (!this.discoveryRefreshTimeout.equals(other.discoveryRefreshTimeout))
         return false;
      if (this.dupsOKBatchSize == null) {
         if (other.dupsOKBatchSize != null)
            return false;
      } else if (!this.dupsOKBatchSize.equals(other.dupsOKBatchSize))
         return false;
      if (this.groupID == null) {
         if (other.groupID != null)
            return false;
      } else if (!this.groupID.equals(other.groupID))
         return false;
      if (this.initialConnectAttempts == null) {
         if (other.initialConnectAttempts != null)
            return false;
      } else if (!this.initialConnectAttempts.equals(other.initialConnectAttempts))
         return false;
      if (this.initialMessagePacketSize == null) {
         if (other.initialMessagePacketSize != null)
            return false;
      } else if (!this.initialMessagePacketSize.equals(other.initialMessagePacketSize))
         return false;
      if (this.jgroupsChannelName == null) {
         if (other.jgroupsChannelName != null)
            return false;
      } else if (!this.jgroupsChannelName.equals(other.jgroupsChannelName))
         return false;
      if (this.jgroupsFile == null) {
         if (other.jgroupsFile != null)
            return false;
      } else if (!this.jgroupsFile.equals(other.jgroupsFile))
         return false;
      if (this.maxRetryInterval == null) {
         if (other.maxRetryInterval != null)
            return false;
      } else if (!this.maxRetryInterval.equals(other.maxRetryInterval))
         return false;
      if (this.minLargeMessageSize == null) {
         if (other.minLargeMessageSize != null)
            return false;
      } else if (!this.minLargeMessageSize.equals(other.minLargeMessageSize))
         return false;
      if (this.producerMaxRate == null) {
         if (other.producerMaxRate != null)
            return false;
      } else if (!this.producerMaxRate.equals(other.producerMaxRate))
         return false;
      if (this.producerWindowSize == null) {
         if (other.producerWindowSize != null)
            return false;
      } else if (!this.producerWindowSize.equals(other.producerWindowSize))
         return false;
      if (this.protocolManagerFactoryStr == null) {
         if (other.protocolManagerFactoryStr != null)
            return false;
      } else if (!protocolManagerFactoryStr.equals(other.protocolManagerFactoryStr))
         return false;
      if (this.reconnectAttempts == null) {
         if (other.reconnectAttempts != null)
            return false;
      } else if (!this.reconnectAttempts.equals(other.reconnectAttempts))
         return false;
      if (this.retryInterval == null) {
         if (other.retryInterval != null)
            return false;
      } else if (!this.retryInterval.equals(other.retryInterval))
         return false;
      if (this.retryIntervalMultiplier == null) {
         if (other.retryIntervalMultiplier != null)
            return false;
      } else if (!this.retryIntervalMultiplier.equals(other.retryIntervalMultiplier))
         return false;
      if (this.scheduledThreadPoolMaxSize == null) {
         if (other.scheduledThreadPoolMaxSize != null)
            return false;
      } else if (!this.scheduledThreadPoolMaxSize.equals(other.scheduledThreadPoolMaxSize))
         return false;
      if (this.threadPoolMaxSize == null) {
         if (other.threadPoolMaxSize != null)
            return false;
      } else if (!this.threadPoolMaxSize.equals(other.threadPoolMaxSize))
         return false;
      if (this.transactionBatchSize == null) {
         if (other.transactionBatchSize != null)
            return false;
      } else if (!this.transactionBatchSize.equals(other.transactionBatchSize))
         return false;
      if (this.useGlobalPools == null) {
         if (other.useGlobalPools != null)
            return false;
      } else if (!this.useGlobalPools.equals(other.useGlobalPools))
         return false;
      if (connectorClassName == null) {
         if (other.connectorClassName != null)
            return false;
      } else if (!connectorClassName.equals(other.connectorClassName))
         return false;
      if (this.connectionParameters == null) {
         if (other.connectionParameters != null)
            return false;
      } else if (!connectionParameters.equals(other.connectionParameters))
         return false;

      if (deserializationDenyList == null) {
         if (other.deserializationDenyList != null)
            return false;
      } else if (!deserializationDenyList.equals(other.deserializationDenyList))
         return false;

      if (deserializationAllowList == null) {
         if (other.deserializationAllowList != null)
            return false;
      } else if (!deserializationAllowList.equals(other.deserializationAllowList))
         return false;

      if (this.enable1xPrefixes == null) {
         if (other.enable1xPrefixes != null)
            return false;
      } else if (!this.enable1xPrefixes.equals(other.enable1xPrefixes))
         return false;

      if (enableSharedClientID == null) {
         if (other.enableSharedClientID != null)
            return false;
      } else if (!this.enableSharedClientID.equals(other.enableSharedClientID))
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((ha == null) ? 0 : ha.hashCode());
      result = prime * result + ((connectionLoadBalancingPolicyClassName == null) ? 0 : connectionLoadBalancingPolicyClassName.hashCode());
      result = prime * result + ((jgroupsFile == null) ? 0 : jgroupsFile.hashCode());
      result = prime * result + ((jgroupsChannelName == null) ? 0 : jgroupsChannelName.hashCode());
      result = prime * result + ((discoveryAddress == null) ? 0 : discoveryAddress.hashCode());
      result = prime * result + ((discoveryPort == null) ? 0 : discoveryPort.hashCode());
      result = prime * result + ((discoveryLocalBindAddress == null) ? 0 : discoveryLocalBindAddress.hashCode());
      result = prime * result + ((discoveryRefreshTimeout == null) ? 0 : discoveryRefreshTimeout.hashCode());
      result = prime * result + ((discoveryInitialWaitTimeout == null) ? 0 : discoveryInitialWaitTimeout.hashCode());
      result = prime * result + ((clientID == null) ? 0 : clientID.hashCode());
      result = prime * result + ((dupsOKBatchSize == null) ? 0 : dupsOKBatchSize.hashCode());
      result = prime * result + ((transactionBatchSize == null) ? 0 : transactionBatchSize.hashCode());
      result = prime * result + ((clientFailureCheckPeriod == null) ? 0 : clientFailureCheckPeriod.hashCode());
      result = prime * result + ((connectionTTL == null) ? 0 : connectionTTL.hashCode());
      result = prime * result + ((cacheLargeMessagesClient == null) ? 0 : cacheLargeMessagesClient.hashCode());
      result = prime * result + ((callTimeout == null) ? 0 : callTimeout.hashCode());
      result = prime * result + ((callFailoverTimeout == null) ? 0 : callFailoverTimeout.hashCode());
      result = prime * result + ((compressLargeMessage == null) ? 0 : compressLargeMessage.hashCode());
      result = prime * result + ((consumerWindowSize == null) ? 0 : consumerWindowSize.hashCode());
      result = prime * result + ((producerWindowSize == null) ? 0 : producerWindowSize.hashCode());
      result = prime * result + ((protocolManagerFactoryStr == null) ? 0 : protocolManagerFactoryStr.hashCode());
      result = prime * result + ((consumerMaxRate == null) ? 0 : consumerMaxRate.hashCode());
      result = prime * result + ((confirmationWindowSize == null) ? 0 : confirmationWindowSize.hashCode());
      result = prime * result + ((producerMaxRate == null) ? 0 : producerMaxRate.hashCode());
      result = prime * result + ((minLargeMessageSize == null) ? 0 : minLargeMessageSize.hashCode());
      result = prime * result + ((blockOnAcknowledge == null) ? 0 : blockOnAcknowledge.hashCode());
      result = prime * result + ((blockOnNonDurableSend == null) ? 0 : blockOnNonDurableSend.hashCode());
      result = prime * result + ((blockOnDurableSend == null) ? 0 : blockOnDurableSend.hashCode());
      result = prime * result + ((autoGroup == null) ? 0 : autoGroup.hashCode());
      result = prime * result + ((preAcknowledge == null) ? 0 : preAcknowledge.hashCode());
      result = prime * result + ((initialConnectAttempts == null) ? 0 : initialConnectAttempts.hashCode());
      result = prime * result + ((retryInterval == null) ? 0 : retryInterval.hashCode());
      result = prime * result + ((retryIntervalMultiplier == null) ? 0 : retryIntervalMultiplier.hashCode());
      result = prime * result + ((maxRetryInterval == null) ? 0 : maxRetryInterval.hashCode());
      result = prime * result + ((reconnectAttempts == null) ? 0 : reconnectAttempts.hashCode());
      result = prime * result + ((useGlobalPools == null) ? 0 : useGlobalPools.hashCode());
      result = prime * result + ((initialMessagePacketSize == null) ? 0 : initialMessagePacketSize.hashCode());
      result = prime * result + ((scheduledThreadPoolMaxSize == null) ? 0 : scheduledThreadPoolMaxSize.hashCode());
      result = prime * result + ((threadPoolMaxSize == null) ? 0 : threadPoolMaxSize.hashCode());
      result = prime * result + ((groupID == null) ? 0 : groupID.hashCode());
      result = prime * result + ((connectorClassName == null) ? 0 : connectorClassName.hashCode());
      result = prime * result + ((connectionParameters == null) ? 0 : connectionParameters.hashCode());
      result = prime * result + ((deserializationDenyList == null) ? 0 : deserializationDenyList.hashCode());
      result = prime * result + ((deserializationAllowList == null) ? 0 : deserializationAllowList.hashCode());
      result = prime * result + ((enable1xPrefixes == null) ? 0 : enable1xPrefixes.hashCode());
      result = prime * result + ((enableSharedClientID == null) ? 0 : enableSharedClientID.hashCode());
      return result;
   }
}
