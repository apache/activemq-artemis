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

   public static XARecoveryConfig newConfig(ActiveMQConnectionFactory factory,
                                            String userName,
                                            String password,
                                            Map<String, String> properties) {
      if (factory.getServerLocator().getDiscoveryGroupConfiguration() != null) {
         return new XARecoveryConfig(factory.getServerLocator().isHA(), factory.getServerLocator().getDiscoveryGroupConfiguration(), userName, password, properties, factory.getServerLocator().getProtocolManagerFactory());
      } else {
         return new XARecoveryConfig(factory.getServerLocator().isHA(), factory.getServerLocator().getStaticTransportConfigurations(), userName, password, properties, factory.getServerLocator().getProtocolManagerFactory());
      }

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
      if (getDiscoveryConfiguration() != null) {
         return ActiveMQClient.createServerLocator(isHA(), getDiscoveryConfiguration()).setProtocolManagerFactory(clientProtocolManager);
      } else {
         return ActiveMQClient.createServerLocator(isHA(), getTransportConfig()).setProtocolManagerFactory(clientProtocolManager);
      }

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
