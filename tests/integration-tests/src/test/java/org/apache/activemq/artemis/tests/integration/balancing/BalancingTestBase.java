/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.balancing;

import javax.jms.ConnectionFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.balancing.BrokerBalancerConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PolicyConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PoolConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;

public class BalancingTestBase extends ClusterTestBase {
   protected static final String AMQP_PROTOCOL = "AMQP";
   protected static final String CORE_PROTOCOL = "CORE";
   protected static final String OPENWIRE_PROTOCOL = "OPENWIRE";

   protected static final String CLUSTER_POOL = "CLUSTER";
   protected static final String DISCOVERY_POOL = "DISCOVERY";
   protected static final String STATIC_POOL = "STATIC";

   protected static final String BROKER_BALANCER_NAME = "bb1";

   protected static final String DEFAULT_CONNECTOR_NAME = "DEFAULT";

   protected static final String GROUP_ADDRESS = ActiveMQTestBase.getUDPDiscoveryAddress();

   protected static final int GROUP_PORT = ActiveMQTestBase.getUDPDiscoveryPort();

   protected static final int MULTIPLE_TARGETS = 3;


   protected TransportConfiguration getDefaultServerAcceptor(final int node) {
      return getServer(node).getConfiguration().getAcceptorConfigurations().stream().findFirst().get();
   }

   protected TransportConfiguration getDefaultServerConnector(final int node) {
      Map<String, TransportConfiguration> connectorConfigurations = getServer(node).getConfiguration().getConnectorConfigurations();
      TransportConfiguration connector = connectorConfigurations.get(DEFAULT_CONNECTOR_NAME);
      return connector != null ? connector : connectorConfigurations.values().stream().findFirst().get();
   }

   protected TransportConfiguration setupDefaultServerConnector(final int node) {
      TransportConfiguration defaultServerConnector = getDefaultServerConnector(node);

      if (!defaultServerConnector.getName().equals(DEFAULT_CONNECTOR_NAME)) {
         defaultServerConnector = new TransportConfiguration(defaultServerConnector.getFactoryClassName(),
            defaultServerConnector.getParams(), DEFAULT_CONNECTOR_NAME, defaultServerConnector.getExtraParams());

         getServer(node).getConfiguration().getConnectorConfigurations().put(DEFAULT_CONNECTOR_NAME, defaultServerConnector);
      }

      return defaultServerConnector;
   }

   protected void setupBalancerServerWithCluster(final int node, final TargetKey targetKey, final String policyName, final Map<String, String> properties, final boolean localTargetEnabled, final String localTargetFilter, final int quorumSize, String clusterConnection) {
      Configuration configuration = getServer(node).getConfiguration();
      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration().setName(BROKER_BALANCER_NAME);

      setupDefaultServerConnector(node);

      brokerBalancerConfiguration.setTargetKey(targetKey).setLocalTargetFilter(localTargetFilter)
         .setPoolConfiguration(new PoolConfiguration().setCheckPeriod(1000).setQuorumSize(quorumSize)
            .setLocalTargetEnabled(localTargetEnabled).setClusterConnection(clusterConnection))
         .setPolicyConfiguration(new PolicyConfiguration().setName(policyName).setProperties(properties));

      configuration.setBalancerConfigurations(Collections.singletonList(brokerBalancerConfiguration));

      TransportConfiguration acceptor = getDefaultServerAcceptor(node);
      acceptor.getParams().put("redirect-to", BROKER_BALANCER_NAME);
   }

   protected void setupBalancerServerWithDiscovery(final int node, final TargetKey targetKey, final String policyName, final Map<String, String> properties, final boolean localTargetEnabled, final String localTargetFilter, final int quorumSize) {
      Configuration configuration = getServer(node).getConfiguration();
      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration().setName(BROKER_BALANCER_NAME);

      setupDefaultServerConnector(node);

      brokerBalancerConfiguration.setTargetKey(targetKey).setLocalTargetFilter(localTargetFilter)
         .setPoolConfiguration(new PoolConfiguration().setCheckPeriod(1000).setQuorumSize(quorumSize)
            .setLocalTargetEnabled(localTargetEnabled).setDiscoveryGroupName("dg1"))
         .setPolicyConfiguration(new PolicyConfiguration().setName(policyName).setProperties(properties));

      configuration.setBalancerConfigurations(Collections.singletonList(brokerBalancerConfiguration));

      TransportConfiguration acceptor = getDefaultServerAcceptor(node);
      acceptor.getParams().put("redirect-to", BROKER_BALANCER_NAME);
   }

   protected void setupBalancerServerWithStaticConnectors(final int node, final TargetKey targetKey, final String policyName, final Map<String, String> properties, final boolean localTargetEnabled, final String localTargetFilter, final int quorumSize, final int... targetNodes) {
      Configuration configuration = getServer(node).getConfiguration();
      BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration().setName(BROKER_BALANCER_NAME);

      setupDefaultServerConnector(node);

      List<String> staticConnectors = new ArrayList<>();
      for (int targetNode : targetNodes) {
         TransportConfiguration connector = getDefaultServerConnector(targetNode);
         configuration.getConnectorConfigurations().put(connector.getName(), connector);
         staticConnectors.add(connector.getName());
      }

      brokerBalancerConfiguration.setTargetKey(targetKey).setLocalTargetFilter(localTargetFilter)
         .setPoolConfiguration(new PoolConfiguration().setCheckPeriod(1000).setQuorumSize(quorumSize)
            .setLocalTargetEnabled(localTargetEnabled).setStaticConnectors(staticConnectors))
         .setPolicyConfiguration(new PolicyConfiguration().setName(policyName).setProperties(properties));

      configuration.setBalancerConfigurations(Collections.singletonList(brokerBalancerConfiguration));

      TransportConfiguration acceptor = getDefaultServerAcceptor(node);
      acceptor.getParams().put("redirect-to", BROKER_BALANCER_NAME);
   }

   protected ConnectionFactory createFactory(String protocol, boolean sslEnabled, String host, int port, String clientID, String user, String password) throws Exception {
      switch (protocol) {
         case CORE_PROTOCOL: {
            StringBuilder urlBuilder = new StringBuilder();

            urlBuilder.append("tcp://");
            urlBuilder.append(host);
            urlBuilder.append(":");
            urlBuilder.append(port);
            urlBuilder.append("?ha=true&reconnectAttempts=30");

            urlBuilder.append("&sniHost=");
            urlBuilder.append(host);

            if (clientID != null) {
               urlBuilder.append("&clientID=");
               urlBuilder.append(clientID);
            }

            if (sslEnabled) {
               urlBuilder.append("&");
               urlBuilder.append(TransportConstants.SSL_ENABLED_PROP_NAME);
               urlBuilder.append("=");
               urlBuilder.append(true);

               urlBuilder.append("&");
               urlBuilder.append(TransportConstants.TRUSTSTORE_PATH_PROP_NAME);
               urlBuilder.append("=");
               urlBuilder.append("server-ca-truststore.jks");

               urlBuilder.append("&");
               urlBuilder.append(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME);
               urlBuilder.append("=");
               urlBuilder.append("securepass");
            }

            return new ActiveMQConnectionFactory(urlBuilder.toString(), user, password);
         }
         case AMQP_PROTOCOL: {
            StringBuilder urlBuilder = new StringBuilder();

            urlBuilder.append("failover:(");

            if (sslEnabled) {
               urlBuilder.append("amqps://");
               urlBuilder.append(host);
               urlBuilder.append(":");
               urlBuilder.append(port);

               urlBuilder.append("?transport.trustStoreLocation=");
               urlBuilder.append(getClass().getClassLoader().getResource("server-ca-truststore.jks").getFile());
               urlBuilder.append("&transport.trustStorePassword=securepass)");
            } else {
               urlBuilder.append("amqp://");
               urlBuilder.append(host);
               urlBuilder.append(":");
               urlBuilder.append(port);
               urlBuilder.append(")");
            }

            if (clientID != null) {
               urlBuilder.append("?jms.clientID=");
               urlBuilder.append(clientID);
            }

            return new JmsConnectionFactory(user, password, urlBuilder.toString());
         }
         case OPENWIRE_PROTOCOL: {
            StringBuilder urlBuilder = new StringBuilder();

            urlBuilder.append("failover:(");

            if (sslEnabled) {
               urlBuilder.append("ssl://");
               urlBuilder.append(host);
               urlBuilder.append(":");
               urlBuilder.append(port);
               urlBuilder.append(")");
            } else {
               urlBuilder.append("tcp://");
               urlBuilder.append(host);
               urlBuilder.append(":");
               urlBuilder.append(port);
               urlBuilder.append(")");
            }

            if (clientID != null) {
               urlBuilder.append("?jms.clientID=");
               urlBuilder.append(clientID);
            }

            if (sslEnabled) {
               org.apache.activemq.ActiveMQSslConnectionFactory sslConnectionFactory = new org.apache.activemq.ActiveMQSslConnectionFactory(urlBuilder.toString());
               sslConnectionFactory.setUserName(user);
               sslConnectionFactory.setPassword(password);
               sslConnectionFactory.setTrustStore("server-ca-truststore.jks");
               sslConnectionFactory.setTrustStorePassword("securepass");
               return sslConnectionFactory;
            } else {
               return new org.apache.activemq.ActiveMQConnectionFactory(user, password, urlBuilder.toString());
            }
         }
         default:
            throw new IllegalStateException("Unexpected value: " + protocol);
      }
   }
}
