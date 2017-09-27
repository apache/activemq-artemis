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
package org.apache.activemq.artemis.core.config;

import java.io.Serializable;
import java.util.List;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;

public final class BridgeConfiguration implements Serializable {

   private static final long serialVersionUID = -1057244274380572226L;

   private String name = null;

   private String queueName = null;

   private String forwardingAddress = null;

   private String filterString = null;

   private List<String> staticConnectors = null;

   private String discoveryGroupName = null;

   private boolean ha = false;

   private TransformerConfiguration transformerConfiguration = null;

   private long retryInterval = ActiveMQClient.DEFAULT_RETRY_INTERVAL;

   private double retryIntervalMultiplier = ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER;

   private int initialConnectAttempts = ActiveMQDefaultConfiguration.getDefaultBridgeInitialConnectAttempts();

   private int reconnectAttempts = ActiveMQDefaultConfiguration.getDefaultBridgeReconnectAttempts();

   private int reconnectAttemptsOnSameNode = ActiveMQDefaultConfiguration.getDefaultBridgeConnectSameNode();

   private boolean useDuplicateDetection = ActiveMQDefaultConfiguration.isDefaultBridgeDuplicateDetection();

   private int confirmationWindowSize = ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE;

   // disable flow control
   private int producerWindowSize = ActiveMQDefaultConfiguration.getDefaultBridgeProducerWindowSize();

   private long clientFailureCheckPeriod = ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;

   private String user = ActiveMQDefaultConfiguration.getDefaultClusterUser();

   private String password = ActiveMQDefaultConfiguration.getDefaultClusterPassword();

   private long connectionTTL = ActiveMQClient.DEFAULT_CONNECTION_TTL;

   private long maxRetryInterval = ActiveMQClient.DEFAULT_MAX_RETRY_INTERVAL;

   private int minLargeMessageSize = ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

   // At this point this is only changed on testcases
   // The bridge shouldn't be sending blocking anyways
   private long callTimeout = ActiveMQClient.DEFAULT_CALL_TIMEOUT;

   public BridgeConfiguration() {
   }

   public String getName() {
      return name;
   }

   /**
    * @param name the name to set
    */
   public BridgeConfiguration setName(final String name) {
      this.name = name;
      return this;
   }

   public String getQueueName() {
      return queueName;
   }

   /**
    * @param queueName the queueName to set
    */
   public BridgeConfiguration setQueueName(final String queueName) {
      this.queueName = queueName;
      return this;
   }

   /**
    * @return the connectionTTL
    */
   public long getConnectionTTL() {
      return connectionTTL;
   }

   public BridgeConfiguration setConnectionTTL(long connectionTTL) {
      this.connectionTTL = connectionTTL;
      return this;
   }

   /**
    * @return the maxRetryInterval
    */
   public long getMaxRetryInterval() {
      return maxRetryInterval;
   }

   public BridgeConfiguration setMaxRetryInterval(long maxRetryInterval) {
      this.maxRetryInterval = maxRetryInterval;
      return this;
   }

   public String getForwardingAddress() {
      return forwardingAddress;
   }

   /**
    * @param forwardingAddress the forwardingAddress to set
    */
   public BridgeConfiguration setForwardingAddress(final String forwardingAddress) {
      this.forwardingAddress = forwardingAddress;
      return this;
   }

   public String getFilterString() {
      return filterString;
   }

   /**
    * @param filterString the filterString to set
    */
   public BridgeConfiguration setFilterString(final String filterString) {
      this.filterString = filterString;
      return this;
   }

   public TransformerConfiguration getTransformerConfiguration() {
      return transformerConfiguration;
   }

   /**
    * @param transformerConfiguration the transformerConfiguration to set
    */
   public BridgeConfiguration setTransformerConfiguration(final TransformerConfiguration transformerConfiguration) {
      this.transformerConfiguration = transformerConfiguration;
      return this;
   }

   public List<String> getStaticConnectors() {
      return staticConnectors;
   }

   /**
    * @param staticConnectors the staticConnectors to set
    */
   public BridgeConfiguration setStaticConnectors(final List<String> staticConnectors) {
      this.staticConnectors = staticConnectors;
      return this;
   }

   public String getDiscoveryGroupName() {
      return discoveryGroupName;
   }

   /**
    * @param discoveryGroupName the discoveryGroupName to set
    */
   public BridgeConfiguration setDiscoveryGroupName(final String discoveryGroupName) {
      this.discoveryGroupName = discoveryGroupName;
      return this;
   }

   public boolean isHA() {
      return ha;
   }

   /**
    * @param ha is the bridge supporting HA?
    */
   public BridgeConfiguration setHA(final boolean ha) {
      this.ha = ha;
      return this;
   }

   public long getRetryInterval() {
      return retryInterval;
   }

   /**
    * @param retryInterval the retryInterval to set
    */
   public BridgeConfiguration setRetryInterval(final long retryInterval) {
      this.retryInterval = retryInterval;
      return this;
   }

   public double getRetryIntervalMultiplier() {
      return retryIntervalMultiplier;
   }

   /**
    * @param retryIntervalMultiplier the retryIntervalMultiplier to set
    */
   public BridgeConfiguration setRetryIntervalMultiplier(final double retryIntervalMultiplier) {
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      return this;
   }

   public int getInitialConnectAttempts() {
      return initialConnectAttempts;
   }

   /**
    * @param initialConnectAttempts the initialConnectAttempts to set
    */
   public BridgeConfiguration setInitialConnectAttempts(final int initialConnectAttempts) {
      this.initialConnectAttempts = initialConnectAttempts;
      return this;
   }

   public int getReconnectAttempts() {
      return reconnectAttempts;
   }

   /**
    * @param reconnectAttempts the reconnectAttempts to set
    */
   public BridgeConfiguration setReconnectAttempts(final int reconnectAttempts) {
      this.reconnectAttempts = reconnectAttempts;
      return this;
   }

   public boolean isUseDuplicateDetection() {
      return useDuplicateDetection;
   }

   /**
    * @param useDuplicateDetection the useDuplicateDetection to set
    */
   public BridgeConfiguration setUseDuplicateDetection(final boolean useDuplicateDetection) {
      this.useDuplicateDetection = useDuplicateDetection;
      return this;
   }

   public int getConfirmationWindowSize() {
      return confirmationWindowSize;
   }

   /**
    * @param confirmationWindowSize the confirmationWindowSize to set
    */
   public BridgeConfiguration setConfirmationWindowSize(final int confirmationWindowSize) {
      this.confirmationWindowSize = confirmationWindowSize;
      return this;
   }

   public int getProducerWindowSize() {
      return producerWindowSize;
   }

   /**
    * @param producerWindowSize the producerWindowSize to set
    */
   public BridgeConfiguration setProducerWindowSize(final int producerWindowSize) {
      this.producerWindowSize = producerWindowSize;
      return this;
   }

   public long getClientFailureCheckPeriod() {
      return clientFailureCheckPeriod;
   }

   public BridgeConfiguration setClientFailureCheckPeriod(long clientFailureCheckPeriod) {
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      return this;
   }

   /**
    * @return the minLargeMessageSize
    */
   public int getMinLargeMessageSize() {
      return minLargeMessageSize;
   }

   public BridgeConfiguration setMinLargeMessageSize(int minLargeMessageSize) {
      this.minLargeMessageSize = minLargeMessageSize;
      return this;
   }

   public String getUser() {
      return user;
   }

   public BridgeConfiguration setUser(String user) {
      this.user = user;
      return this;
   }

   public String getPassword() {
      return password;
   }

   public BridgeConfiguration setPassword(String password) {
      this.password = password;
      return this;
   }

   /**
    * @return the callTimeout
    */
   public long getCallTimeout() {
      return callTimeout;
   }

   public int getReconnectAttemptsOnSameNode() {
      return reconnectAttemptsOnSameNode;
   }

   public BridgeConfiguration setReconnectAttemptsOnSameNode(int reconnectAttemptsOnSameNode) {
      this.reconnectAttemptsOnSameNode = reconnectAttemptsOnSameNode;
      return this;
   }

   /**
    * At this point this is only changed on testcases
    * The bridge shouldn't be sending blocking anyways
    *
    * @param callTimeout the callTimeout to set
    */
   public BridgeConfiguration setCallTimeout(long callTimeout) {
      this.callTimeout = callTimeout;
      return this;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (callTimeout ^ (callTimeout >>> 32));
      result = prime * result + (int) (clientFailureCheckPeriod ^ (clientFailureCheckPeriod >>> 32));
      result = prime * result + confirmationWindowSize;
      result = prime * result + producerWindowSize;
      result = prime * result + (int) (connectionTTL ^ (connectionTTL >>> 32));
      result = prime * result + ((discoveryGroupName == null) ? 0 : discoveryGroupName.hashCode());
      result = prime * result + ((filterString == null) ? 0 : filterString.hashCode());
      result = prime * result + ((forwardingAddress == null) ? 0 : forwardingAddress.hashCode());
      result = prime * result + (ha ? 1231 : 1237);
      result = prime * result + (int) (maxRetryInterval ^ (maxRetryInterval >>> 32));
      result = prime * result + minLargeMessageSize;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((password == null) ? 0 : password.hashCode());
      result = prime * result + ((queueName == null) ? 0 : queueName.hashCode());
      result = prime * result + initialConnectAttempts;
      result = prime * result + reconnectAttempts;
      result = prime * result + (int) (retryInterval ^ (retryInterval >>> 32));
      long temp;
      temp = Double.doubleToLongBits(retryIntervalMultiplier);
      result = prime * result + (int) (temp ^ (temp >>> 32));
      result = prime * result + ((staticConnectors == null) ? 0 : staticConnectors.hashCode());
      result = prime * result + ((transformerConfiguration == null) ? 0 : transformerConfiguration.hashCode());
      result = prime * result + (useDuplicateDetection ? 1231 : 1237);
      result = prime * result + ((user == null) ? 0 : user.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      BridgeConfiguration other = (BridgeConfiguration) obj;
      if (callTimeout != other.callTimeout)
         return false;
      if (clientFailureCheckPeriod != other.clientFailureCheckPeriod)
         return false;
      if (confirmationWindowSize != other.confirmationWindowSize)
         return false;
      if (producerWindowSize != other.producerWindowSize)
         return false;
      if (connectionTTL != other.connectionTTL)
         return false;
      if (discoveryGroupName == null) {
         if (other.discoveryGroupName != null)
            return false;
      } else if (!discoveryGroupName.equals(other.discoveryGroupName))
         return false;
      if (filterString == null) {
         if (other.filterString != null)
            return false;
      } else if (!filterString.equals(other.filterString))
         return false;
      if (forwardingAddress == null) {
         if (other.forwardingAddress != null)
            return false;
      } else if (!forwardingAddress.equals(other.forwardingAddress))
         return false;
      if (ha != other.ha)
         return false;
      if (maxRetryInterval != other.maxRetryInterval)
         return false;
      if (minLargeMessageSize != other.minLargeMessageSize)
         return false;
      if (name == null) {
         if (other.name != null)
            return false;
      } else if (!name.equals(other.name))
         return false;
      if (password == null) {
         if (other.password != null)
            return false;
      } else if (!password.equals(other.password))
         return false;
      if (queueName == null) {
         if (other.queueName != null)
            return false;
      } else if (!queueName.equals(other.queueName))
         return false;
      if (initialConnectAttempts != other.initialConnectAttempts)
         return false;
      if (reconnectAttempts != other.reconnectAttempts)
         return false;
      if (retryInterval != other.retryInterval)
         return false;
      if (Double.doubleToLongBits(retryIntervalMultiplier) != Double.doubleToLongBits(other.retryIntervalMultiplier))
         return false;
      if (staticConnectors == null) {
         if (other.staticConnectors != null)
            return false;
      } else if (!staticConnectors.equals(other.staticConnectors))
         return false;
      if (transformerConfiguration == null) {
         if (other.transformerConfiguration != null)
            return false;
      } else if (!transformerConfiguration.equals(other.transformerConfiguration))
         return false;
      if (useDuplicateDetection != other.useDuplicateDetection)
         return false;
      if (user == null) {
         if (other.user != null)
            return false;
      } else if (!user.equals(other.user))
         return false;
      return true;
   }

}
