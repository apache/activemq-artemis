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
package org.apache.activemq.artemis.service.extensions.xa.recovery;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManagerFactory;

/**
 * This represents the configuration of a single connection factory.
 *
 * A wrapper around info needed for the xa recovery resource
 */
public class XARecoveryConfig {

   public static final String JNDI_NAME_PROPERTY_KEY = "JNDI_NAME";

   private final boolean ha;
   private final TransportConfiguration[] transportConfiguration;
   private final DiscoveryGroupConfiguration discoveryConfiguration;
   private final String username;
   private final String password;
   private final Map<String, String> properties;
   private final ClientProtocolManagerFactory clientProtocolManager;

   // ServerLocator properties
   private Long callFailoverTimeout;
   private Long callTimeout;
   private Long clientFailureCheckPeriod;
   private Integer confirmationWindowSize;
   private String connectionLoadBalancingPolicyClassName;
   private Long connectionTTL;
   private Integer consumerMaxRate;
   private Integer consumerWindowSize;
   private Integer initialConnectAttempts;
   private Integer producerMaxRate;
   private Integer producerWindowSize;
   private Integer minLargeMessageSize;
   private Long retryInterval;
   private Double retryIntervalMultiplier;
   private Long maxRetryInterval;
   private Integer reconnectAttempts;
   private Integer initialMessagePacketSize;
   private Integer scheduledThreadPoolMaxSize;
   private Integer threadPoolMaxSize;
   private boolean autoGroup;
   private boolean blockOnAcknowledge;
   private boolean blockOnNonDurableSend;
   private boolean blockOnDurableSend;
   private boolean preAcknowledge;
   private boolean useGlobalPools;
   private boolean cacheLargeMessagesClient;
   private boolean compressLargeMessage;
   private boolean failoverOnInitialConnection;

   public static XARecoveryConfig newConfig(ActiveMQConnectionFactory factory,
                                            String userName,
                                            String password,
                                            Map<String, String> properties) {
      return new XARecoveryConfig(factory.getServerLocator(), userName, password, properties);
   }

   public XARecoveryConfig(final boolean ha,
                           final TransportConfiguration[] transportConfiguration,
                           final String username,
                           final String password,
                           final Map<String, String> properties,
                           final ClientProtocolManagerFactory clientProtocolManager) {
      TransportConfiguration[] newTransportConfiguration = new TransportConfiguration[transportConfiguration.length];
      for (int i = 0; i < transportConfiguration.length; i++) {
         if (clientProtocolManager != null) {
            newTransportConfiguration[i] = clientProtocolManager.adaptTransportConfiguration(transportConfiguration[i].newTransportConfig(""));
         } else {
            newTransportConfiguration[i] = transportConfiguration[i].newTransportConfig("");
         }
      }

      this.transportConfiguration = newTransportConfiguration;
      this.discoveryConfiguration = null;
      this.username = username;
      this.password = password;
      this.ha = ha;
      this.properties = properties == null ? Collections.unmodifiableMap(new HashMap<String, String>()) : Collections.unmodifiableMap(properties);
      this.clientProtocolManager = clientProtocolManager;
   }

   public XARecoveryConfig(final boolean ha,
                           final TransportConfiguration[] transportConfiguration,
                           final String username,
                           final String password,
                           final Map<String, String> properties) {
      this(ha, transportConfiguration, username, password, properties, null);
   }

   public XARecoveryConfig(final boolean ha,
                           final DiscoveryGroupConfiguration discoveryConfiguration,
                           final String username,
                           final String password,
                           final Map<String, String> properties,
                           final ClientProtocolManagerFactory clientProtocolManager) {
      this.discoveryConfiguration = discoveryConfiguration;
      this.transportConfiguration = null;
      this.username = username;
      this.password = password;
      this.ha = ha;
      this.clientProtocolManager = clientProtocolManager;
      this.properties = properties == null ? Collections.unmodifiableMap(new HashMap<String, String>()) : Collections.unmodifiableMap(properties);
   }

   public XARecoveryConfig(final boolean ha,
                           final DiscoveryGroupConfiguration discoveryConfiguration,
                           final String username,
                           final String password,
                           final Map<String, String> properties) {
      this(ha, discoveryConfiguration, username, password, properties, null);
   }

   private XARecoveryConfig(ServerLocator serverLocator,
                            String username,
                            String password,
                            Map<String, String> properties) {
      ClientProtocolManagerFactory clientProtocolManager = serverLocator.getProtocolManagerFactory();
      if (serverLocator.getDiscoveryGroupConfiguration() != null) {
         this.discoveryConfiguration = serverLocator.getDiscoveryGroupConfiguration();
         this.transportConfiguration = null;
      } else {
         TransportConfiguration[] transportConfiguration = serverLocator.getStaticTransportConfigurations();
         TransportConfiguration[] newTransportConfiguration = new TransportConfiguration[transportConfiguration.length];
         for (int i = 0; i < transportConfiguration.length; i++) {
            if (clientProtocolManager != null) {
               newTransportConfiguration[i] = clientProtocolManager.adaptTransportConfiguration(transportConfiguration[i].newTransportConfig(""));
            } else {
               newTransportConfiguration[i] = transportConfiguration[i].newTransportConfig("");
            }
         }

         this.transportConfiguration = newTransportConfiguration;
         this.discoveryConfiguration = null;
      }
      this.username = username;
      this.password = password;
      this.ha = serverLocator.isHA();
      this.properties = properties == null ? Collections.unmodifiableMap(new HashMap<String, String>()) : Collections.unmodifiableMap(properties);
      this.clientProtocolManager = clientProtocolManager;

      readLocatorProperties(serverLocator);
   }

   public boolean isHA() {
      return ha;
   }

   public DiscoveryGroupConfiguration getDiscoveryConfiguration() {
      return discoveryConfiguration;
   }

   public TransportConfiguration[] getTransportConfig() {
      return transportConfiguration;
   }

   public String getUsername() {
      return username;
   }

   public String getPassword() {
      return password;
   }

   public Map<String, String> getProperties() {
      return properties;
   }

   public ClientProtocolManagerFactory getClientProtocolManager() {
      return clientProtocolManager;
   }

   /**
    * Create a serverLocator using the configuration
    *
    * @return locator
    */
   public ServerLocator createServerLocator() {
      ServerLocator serverLocator;
      if (getDiscoveryConfiguration() != null) {
         serverLocator = ActiveMQClient.createServerLocator(isHA(), getDiscoveryConfiguration()).setProtocolManagerFactory(clientProtocolManager);
      } else {
         serverLocator = ActiveMQClient.createServerLocator(isHA(), getTransportConfig()).setProtocolManagerFactory(clientProtocolManager);
      }

      writeLocatorProperties(serverLocator);

      return serverLocator;
   }

   private void writeLocatorProperties(ServerLocator serverLocator) {
      serverLocator.setAutoGroup(this.autoGroup);
      serverLocator.setBlockOnAcknowledge(this.blockOnAcknowledge);
      serverLocator.setBlockOnNonDurableSend(this.blockOnNonDurableSend);
      serverLocator.setBlockOnDurableSend(this.blockOnDurableSend);
      serverLocator.setPreAcknowledge(this.preAcknowledge);
      serverLocator.setUseGlobalPools(this.useGlobalPools);
      serverLocator.setCacheLargeMessagesClient(this.cacheLargeMessagesClient);
      serverLocator.setCompressLargeMessage(this.compressLargeMessage);
      serverLocator.setFailoverOnInitialConnection(this.failoverOnInitialConnection);

      serverLocator.setConsumerMaxRate(this.consumerMaxRate);
      serverLocator.setConsumerWindowSize(this.consumerWindowSize);
      serverLocator.setMinLargeMessageSize(this.minLargeMessageSize);
      serverLocator.setProducerMaxRate(this.producerMaxRate);
      serverLocator.setProducerWindowSize(this.producerWindowSize);
      serverLocator.setConfirmationWindowSize(this.confirmationWindowSize);
      serverLocator.setReconnectAttempts(this.reconnectAttempts);
      serverLocator.setThreadPoolMaxSize(this.threadPoolMaxSize);
      serverLocator.setScheduledThreadPoolMaxSize(this.scheduledThreadPoolMaxSize);
      serverLocator.setInitialConnectAttempts(this.initialConnectAttempts);
      serverLocator.setInitialMessagePacketSize(this.initialMessagePacketSize);

      serverLocator.setClientFailureCheckPeriod(this.clientFailureCheckPeriod);
      serverLocator.setCallTimeout(this.callTimeout);
      serverLocator.setCallFailoverTimeout(this.callFailoverTimeout);
      serverLocator.setConnectionTTL(this.connectionTTL);
      serverLocator.setRetryInterval(this.retryInterval);
      serverLocator.setMaxRetryInterval(this.maxRetryInterval);

      serverLocator.setRetryIntervalMultiplier(this.retryIntervalMultiplier);

      serverLocator.setConnectionLoadBalancingPolicyClassName(this.connectionLoadBalancingPolicyClassName);
}

   private void readLocatorProperties(ServerLocator locator) {

      this.autoGroup = locator.isAutoGroup();
      this.blockOnAcknowledge = locator.isBlockOnAcknowledge();
      this.blockOnNonDurableSend = locator.isBlockOnNonDurableSend();
      this.blockOnDurableSend = locator.isBlockOnDurableSend();
      this.preAcknowledge = locator.isPreAcknowledge();
      this.useGlobalPools = locator.isUseGlobalPools();
      this.cacheLargeMessagesClient = locator.isCacheLargeMessagesClient();
      this.compressLargeMessage = locator.isCompressLargeMessage();
      this.failoverOnInitialConnection = locator.isFailoverOnInitialConnection();

      this.consumerMaxRate = locator.getConsumerMaxRate();
      this.consumerWindowSize = locator.getConsumerWindowSize();
      this.minLargeMessageSize = locator.getMinLargeMessageSize();
      this.producerMaxRate = locator.getProducerMaxRate();
      this.producerWindowSize = locator.getProducerWindowSize();
      this.confirmationWindowSize = locator.getConfirmationWindowSize();
      this.reconnectAttempts = locator.getReconnectAttempts();
      this.threadPoolMaxSize = locator.getThreadPoolMaxSize();
      this.scheduledThreadPoolMaxSize = locator.getScheduledThreadPoolMaxSize();
      this.initialConnectAttempts = locator.getInitialConnectAttempts();
      this.initialMessagePacketSize = locator.getInitialMessagePacketSize();

      this.clientFailureCheckPeriod = locator.getClientFailureCheckPeriod();
      this.callTimeout = locator.getCallTimeout();
      this.callFailoverTimeout = locator.getCallFailoverTimeout();
      this.connectionTTL = locator.getConnectionTTL();
      this.retryInterval = locator.getRetryInterval();
      this.maxRetryInterval = locator.getMaxRetryInterval();

      this.retryIntervalMultiplier = locator.getRetryIntervalMultiplier();

      this.connectionLoadBalancingPolicyClassName = locator.getConnectionLoadBalancingPolicyClassName();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((discoveryConfiguration == null) ? 0 : discoveryConfiguration.hashCode());
      result = prime * result + Arrays.hashCode(transportConfiguration);
      return result;
   }

   /*
    * We don't use username and password on purpose.
    * Just having the connector is enough, as we don't want to duplicate resources just because of usernames
    */
   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      XARecoveryConfig other = (XARecoveryConfig) obj;
      if (discoveryConfiguration == null) {
         if (other.discoveryConfiguration != null)
            return false;
      } else if (!discoveryConfiguration.equals(other.discoveryConfiguration))
         return false;
      if (!Arrays.equals(transportConfiguration, other.transportConfiguration))
         return false;
      return true;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      StringBuilder builder = new StringBuilder();

      builder.append("XARecoveryConfig [transportConfiguration=" + Arrays.toString(transportConfiguration));
      builder.append(", discoveryConfiguration=" + discoveryConfiguration);
      builder.append(", username=" + username);
      builder.append(", password=****");

      for (Map.Entry<String, String> entry : properties.entrySet()) {
         builder.append(", " + entry.getKey() + "=" + entry.getValue());
      }
      builder.append("]");

      return builder.toString();
   }
}
