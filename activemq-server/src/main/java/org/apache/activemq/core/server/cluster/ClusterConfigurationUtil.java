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
package org.apache.activemq.core.server.cluster;


import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.ActiveMQServerLogger;

import java.lang.reflect.Array;
import java.util.List;

public class ClusterConfigurationUtil
{
   public static TransportConfiguration getTransportConfiguration(ClusterConnectionConfiguration config, Configuration configuration)
   {
      if (config.getName() == null)
      {
         ActiveMQServerLogger.LOGGER.clusterConnectionNotUnique();

         return null;
      }

      if (config.getAddress() == null)
      {
         ActiveMQServerLogger.LOGGER.clusterConnectionNoForwardAddress();

         return null;
      }

      TransportConfiguration connector = configuration.getConnectorConfigurations().get(config.getConnectorName());

      if (connector == null)
      {
         ActiveMQServerLogger.LOGGER.clusterConnectionNoConnector(config.getConnectorName());
         return null;
      }
      return connector;
   }

   public static DiscoveryGroupConfiguration getDiscoveryGroupConfiguration(ClusterConnectionConfiguration config, Configuration configuration)
   {
      DiscoveryGroupConfiguration dg = configuration.getDiscoveryGroupConfigurations()
            .get(config.getDiscoveryGroupName());

      if (dg == null)
      {
         ActiveMQServerLogger.LOGGER.clusterConnectionNoDiscoveryGroup(config.getDiscoveryGroupName());
         return null;
      }
      return dg;
   }

   public static TransportConfiguration[] getTransportConfigurations(ClusterConnectionConfiguration config, Configuration configuration)
   {
      return config.getStaticConnectors() != null ? connectorNameListToArray(config.getStaticConnectors(), configuration)
            : null;
   }

   public static TransportConfiguration[] connectorNameListToArray(final List<String> connectorNames, Configuration configuration)
   {
      TransportConfiguration[] tcConfigs = (TransportConfiguration[]) Array.newInstance(TransportConfiguration.class,
            connectorNames.size());
      int count = 0;
      for (String connectorName : connectorNames)
      {
         TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorName);

         if (connector == null)
         {
            ActiveMQServerLogger.LOGGER.bridgeNoConnector(connectorName);

            return null;
         }

         tcConfigs[count++] = connector;
      }

      return tcConfigs;
   }
}
