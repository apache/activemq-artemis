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

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public final class ClusterConnectionConfiguration implements Serializable {

   private static final long serialVersionUID = 8948303813427795935L;

   private String name;

   private String address;

   private String connectorName;

   private long clientFailureCheckPeriod = ActiveMQDefaultConfiguration.getDefaultClusterFailureCheckPeriod();

   private long connectionTTL = ActiveMQDefaultConfiguration.getDefaultClusterConnectionTtl();

   private long retryInterval = ActiveMQDefaultConfiguration.getDefaultClusterRetryInterval();

   private double retryIntervalMultiplier = ActiveMQDefaultConfiguration.getDefaultClusterRetryIntervalMultiplier();

   private long maxRetryInterval = ActiveMQDefaultConfiguration.getDefaultClusterMaxRetryInterval();

   private int initialConnectAttempts = ActiveMQDefaultConfiguration.getDefaultClusterInitialConnectAttempts();

   private int reconnectAttempts = ActiveMQDefaultConfiguration.getDefaultClusterReconnectAttempts();

   private long callTimeout = ActiveMQDefaultConfiguration.getDefaultClusterCallTimeout();

   private long callFailoverTimeout = ActiveMQDefaultConfiguration.getDefaultClusterCallFailoverTimeout();

   private boolean duplicateDetection = ActiveMQDefaultConfiguration.isDefaultClusterDuplicateDetection();

   private MessageLoadBalancingType messageLoadBalancingType = Enum.valueOf(MessageLoadBalancingType.class, ActiveMQDefaultConfiguration.getDefaultClusterMessageLoadBalancingType());

   private List<String> staticConnectors = Collections.emptyList();

   private String discoveryGroupName = null;

   private int maxHops = ActiveMQDefaultConfiguration.getDefaultClusterMaxHops();

   private int confirmationWindowSize = ActiveMQDefaultConfiguration.getDefaultClusterConfirmationWindowSize();

   private boolean allowDirectConnectionsOnly = false;

   private int minLargeMessageSize = ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

   private long clusterNotificationInterval = ActiveMQDefaultConfiguration.getDefaultClusterNotificationInterval();

   private int clusterNotificationAttempts = ActiveMQDefaultConfiguration.getDefaultClusterNotificationAttempts();

   public ClusterConnectionConfiguration() {
   }

   public String getName() {
      return name;
   }

   public ClusterConnectionConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public String getAddress() {
      return address;
   }

   public ClusterConnectionConfiguration setAddress(String address) {
      this.address = address;
      return this;
   }

   /**
    * @return the clientFailureCheckPeriod
    */
   public long getClientFailureCheckPeriod() {
      return clientFailureCheckPeriod;
   }

   /**
    * @return the connectionTTL
    */
   public long getConnectionTTL() {
      return connectionTTL;
   }

   /**
    * @return the retryIntervalMultiplier
    */
   public double getRetryIntervalMultiplier() {
      return retryIntervalMultiplier;
   }

   /**
    * @return the maxRetryInterval
    */
   public long getMaxRetryInterval() {
      return maxRetryInterval;
   }

   /**
    * @return the initialConnectAttempts
    */
   public int getInitialConnectAttempts() {
      return initialConnectAttempts;
   }

   /**
    * @return the reconnectAttempts
    */
   public int getReconnectAttempts() {
      return reconnectAttempts;
   }

   public long getCallTimeout() {
      return callTimeout;
   }

   public long getCallFailoverTimeout() {
      return callFailoverTimeout;
   }

   public String getConnectorName() {
      return connectorName;
   }

   public ClusterConnectionConfiguration setConnectorName(String connectorName) {
      this.connectorName = connectorName;
      return this;
   }

   public boolean isDuplicateDetection() {
      return duplicateDetection;
   }

   public MessageLoadBalancingType getMessageLoadBalancingType() {
      return messageLoadBalancingType;
   }

   public int getMaxHops() {
      return maxHops;
   }

   public ClusterConnectionConfiguration setMaxHops(int maxHops) {
      this.maxHops = maxHops;
      return this;
   }

   public int getConfirmationWindowSize() {
      return confirmationWindowSize;
   }

   public ClusterConnectionConfiguration setConfirmationWindowSize(int confirmationWindowSize) {
      this.confirmationWindowSize = confirmationWindowSize;
      return this;
   }

   public List<String> getStaticConnectors() {
      return staticConnectors;
   }

   public ClusterConnectionConfiguration setStaticConnectors(List<String> staticConnectors) {
      this.staticConnectors = staticConnectors;
      return this;
   }

   public String getDiscoveryGroupName() {
      return discoveryGroupName;
   }

   public ClusterConnectionConfiguration setDiscoveryGroupName(String discoveryGroupName) {
      this.discoveryGroupName = discoveryGroupName;
      return this;
   }

   public long getRetryInterval() {
      return retryInterval;
   }

   public boolean isAllowDirectConnectionsOnly() {
      return allowDirectConnectionsOnly;
   }

   public ClusterConnectionConfiguration setAllowDirectConnectionsOnly(boolean allowDirectConnectionsOnly) {
      this.allowDirectConnectionsOnly = allowDirectConnectionsOnly;
      return this;
   }

   /**
    * @return the minLargeMessageSize
    */
   public int getMinLargeMessageSize() {
      return minLargeMessageSize;
   }

   /**
    * @param minLargeMessageSize the minLargeMessageSize to set
    */
   public ClusterConnectionConfiguration setMinLargeMessageSize(final int minLargeMessageSize) {
      this.minLargeMessageSize = minLargeMessageSize;
      return this;
   }

   /**
    * @param clientFailureCheckPeriod the clientFailureCheckPeriod to set
    */
   public ClusterConnectionConfiguration setClientFailureCheckPeriod(long clientFailureCheckPeriod) {
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      return this;
   }

   /**
    * @param connectionTTL the connectionTTL to set
    */
   public ClusterConnectionConfiguration setConnectionTTL(long connectionTTL) {
      this.connectionTTL = connectionTTL;
      return this;
   }

   /**
    * @param retryInterval the retryInterval to set
    */
   public ClusterConnectionConfiguration setRetryInterval(long retryInterval) {
      this.retryInterval = retryInterval;
      return this;
   }

   /**
    * @param retryIntervalMultiplier the retryIntervalMultiplier to set
    */
   public ClusterConnectionConfiguration setRetryIntervalMultiplier(double retryIntervalMultiplier) {
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      return this;
   }

   /**
    * @param maxRetryInterval the maxRetryInterval to set
    */
   public ClusterConnectionConfiguration setMaxRetryInterval(long maxRetryInterval) {
      this.maxRetryInterval = maxRetryInterval;
      return this;
   }

   /**
    * @param initialConnectAttempts the reconnectAttempts to set
    */
   public ClusterConnectionConfiguration setInitialConnectAttempts(int initialConnectAttempts) {
      this.initialConnectAttempts = initialConnectAttempts;
      return this;
   }

   /**
    * @param reconnectAttempts the reconnectAttempts to set
    */
   public ClusterConnectionConfiguration setReconnectAttempts(int reconnectAttempts) {
      this.reconnectAttempts = reconnectAttempts;
      return this;
   }

   /**
    * @param callTimeout the callTimeout to set
    */
   public ClusterConnectionConfiguration setCallTimeout(long callTimeout) {
      this.callTimeout = callTimeout;
      return this;
   }

   /**
    * @param callFailoverTimeout the callTimeout to set
    */
   public ClusterConnectionConfiguration setCallFailoverTimeout(long callFailoverTimeout) {
      this.callFailoverTimeout = callFailoverTimeout;
      return this;
   }

   /**
    * @param duplicateDetection the duplicateDetection to set
    */
   public ClusterConnectionConfiguration setDuplicateDetection(boolean duplicateDetection) {
      this.duplicateDetection = duplicateDetection;
      return this;
   }

   /**
    * @param messageLoadBalancingType
    * @return
    */
   public ClusterConnectionConfiguration setMessageLoadBalancingType(MessageLoadBalancingType messageLoadBalancingType) {
      this.messageLoadBalancingType = messageLoadBalancingType;
      return this;
   }

   /*
   * returns the cluster update interval
   * */
   public long getClusterNotificationInterval() {
      return clusterNotificationInterval;
   }

   public ClusterConnectionConfiguration setClusterNotificationInterval(long clusterNotificationInterval) {
      this.clusterNotificationInterval = clusterNotificationInterval;
      return this;
   }

   public int getClusterNotificationAttempts() {
      return clusterNotificationAttempts;
   }

   public ClusterConnectionConfiguration setClusterNotificationAttempts(int clusterNotificationAttempts) {
      this.clusterNotificationAttempts = clusterNotificationAttempts;
      return this;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((address == null) ? 0 : address.hashCode());
      result = prime * result + (allowDirectConnectionsOnly ? 1231 : 1237);
      result = prime * result + (int) (callFailoverTimeout ^ (callFailoverTimeout >>> 32));
      result = prime * result + (int) (callTimeout ^ (callTimeout >>> 32));
      result = prime * result + (int) (clientFailureCheckPeriod ^ (clientFailureCheckPeriod >>> 32));
      result = prime * result + clusterNotificationAttempts;
      result = prime * result + (int) (clusterNotificationInterval ^ (clusterNotificationInterval >>> 32));
      result = prime * result + confirmationWindowSize;
      result = prime * result + (int) (connectionTTL ^ (connectionTTL >>> 32));
      result = prime * result + ((connectorName == null) ? 0 : connectorName.hashCode());
      result = prime * result + ((discoveryGroupName == null) ? 0 : discoveryGroupName.hashCode());
      result = prime * result + (duplicateDetection ? 1231 : 1237);
      result = prime * result + (messageLoadBalancingType == null ? 0 : messageLoadBalancingType.hashCode());
      result = prime * result + maxHops;
      result = prime * result + (int) (maxRetryInterval ^ (maxRetryInterval >>> 32));
      result = prime * result + minLargeMessageSize;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + initialConnectAttempts;
      result = prime * result + reconnectAttempts;
      result = prime * result + (int) (retryInterval ^ (retryInterval >>> 32));
      long temp;
      temp = Double.doubleToLongBits(retryIntervalMultiplier);
      result = prime * result + (int) (temp ^ (temp >>> 32));
      result = prime * result + ((staticConnectors == null) ? 0 : staticConnectors.hashCode());
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
      ClusterConnectionConfiguration other = (ClusterConnectionConfiguration) obj;
      if (address == null) {
         if (other.address != null)
            return false;
      }
      else if (!address.equals(other.address))
         return false;
      if (allowDirectConnectionsOnly != other.allowDirectConnectionsOnly)
         return false;
      if (callFailoverTimeout != other.callFailoverTimeout)
         return false;
      if (callTimeout != other.callTimeout)
         return false;
      if (clientFailureCheckPeriod != other.clientFailureCheckPeriod)
         return false;
      if (clusterNotificationAttempts != other.clusterNotificationAttempts)
         return false;
      if (clusterNotificationInterval != other.clusterNotificationInterval)
         return false;
      if (confirmationWindowSize != other.confirmationWindowSize)
         return false;
      if (connectionTTL != other.connectionTTL)
         return false;
      if (connectorName == null) {
         if (other.connectorName != null)
            return false;
      }
      else if (!connectorName.equals(other.connectorName))
         return false;
      if (discoveryGroupName == null) {
         if (other.discoveryGroupName != null)
            return false;
      }
      else if (!discoveryGroupName.equals(other.discoveryGroupName))
         return false;
      if (duplicateDetection != other.duplicateDetection)
         return false;
      if (messageLoadBalancingType != other.messageLoadBalancingType)
         return false;
      if (maxHops != other.maxHops)
         return false;
      if (maxRetryInterval != other.maxRetryInterval)
         return false;
      if (minLargeMessageSize != other.minLargeMessageSize)
         return false;
      if (name == null) {
         if (other.name != null)
            return false;
      }
      else if (!name.equals(other.name))
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
      }
      else if (!staticConnectors.equals(other.staticConnectors))
         return false;
      return true;
   }
}
