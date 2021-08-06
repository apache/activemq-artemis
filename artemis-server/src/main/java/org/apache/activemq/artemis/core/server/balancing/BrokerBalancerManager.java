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

package org.apache.activemq.artemis.core.server.balancing;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.config.balancing.BrokerBalancerConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PolicyConfiguration;
import org.apache.activemq.artemis.core.config.balancing.PoolConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancing.policies.Policy;
import org.apache.activemq.artemis.core.server.balancing.policies.PolicyFactory;
import org.apache.activemq.artemis.core.server.balancing.policies.PolicyFactoryResolver;
import org.apache.activemq.artemis.core.server.balancing.pools.ClusterPool;
import org.apache.activemq.artemis.core.server.balancing.pools.DiscoveryGroupService;
import org.apache.activemq.artemis.core.server.balancing.pools.DiscoveryPool;
import org.apache.activemq.artemis.core.server.balancing.pools.DiscoveryService;
import org.apache.activemq.artemis.core.server.balancing.pools.Pool;
import org.apache.activemq.artemis.core.server.balancing.pools.StaticPool;
import org.apache.activemq.artemis.core.server.balancing.targets.ActiveMQTargetFactory;
import org.apache.activemq.artemis.core.server.balancing.targets.LocalTarget;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetFactory;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public final class BrokerBalancerManager implements ActiveMQComponent {
   private static final Logger logger = Logger.getLogger(BrokerBalancerManager.class);


   private final Configuration config;

   private final ActiveMQServer server;

   private final ScheduledExecutorService scheduledExecutor;

   private volatile boolean started = false;

   private Map<String, BrokerBalancer> balancerControllers = new HashMap<>();


   @Override
   public boolean isStarted() {
      return started;
   }


   public BrokerBalancerManager(final Configuration config, final ActiveMQServer server, ScheduledExecutorService scheduledExecutor) {
      this.config = config;
      this.server = server;
      this.scheduledExecutor = scheduledExecutor;
   }

   public void deploy() throws Exception {
      for (BrokerBalancerConfiguration balancerConfig : config.getBalancerConfigurations()) {
         deployBrokerBalancer(balancerConfig);
      }
   }

   public void deployBrokerBalancer(BrokerBalancerConfiguration config) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debugf("Deploying BrokerBalancer " + config.getName());
      }

      Target localTarget = new LocalTarget(null, server);

      Pool pool = deployPool(config.getPoolConfiguration(), localTarget);

      Policy policy = deployPolicy(config.getPolicyConfiguration(), pool);

      BrokerBalancer balancer = new BrokerBalancer(config.getName(), config.getTargetKey(), config.getTargetKeyFilter(),
         localTarget, config.getLocalTargetFilter(), pool, policy, config.getCacheTimeout());

      balancerControllers.put(balancer.getName(), balancer);

      server.getManagementService().registerBrokerBalancer(balancer);
   }

   private Pool deployPool(PoolConfiguration config, Target localTarget) throws Exception {
      Pool pool;
      TargetFactory targetFactory = new ActiveMQTargetFactory();

      targetFactory.setUsername(config.getUsername());
      targetFactory.setPassword(config.getPassword());

      if (config.getClusterConnection() != null) {
         ClusterConnection clusterConnection = server.getClusterManager()
            .getClusterConnection(config.getClusterConnection());

         pool = new ClusterPool(targetFactory, scheduledExecutor, config.getCheckPeriod(), clusterConnection);
      } else if (config.getDiscoveryGroupName() != null) {
         DiscoveryGroupConfiguration discoveryGroupConfiguration = server.getConfiguration().
            getDiscoveryGroupConfigurations().get(config.getDiscoveryGroupName());

         DiscoveryService discoveryService = new DiscoveryGroupService(new DiscoveryGroup(server.getNodeID().toString(), config.getDiscoveryGroupName(),
            discoveryGroupConfiguration.getRefreshTimeout(), discoveryGroupConfiguration.getBroadcastEndpointFactory(), null));

         pool = new DiscoveryPool(targetFactory, scheduledExecutor, config.getCheckPeriod(), discoveryService);
      } else if (config.getStaticConnectors() != null) {
         Map<String, TransportConfiguration> connectorConfigurations =
            server.getConfiguration().getConnectorConfigurations();

         List<TransportConfiguration> staticConnectors = new ArrayList<>();
         for (String staticConnector : config.getStaticConnectors()) {
            TransportConfiguration connector = connectorConfigurations.get(staticConnector);

            if (connector != null) {
               staticConnectors.add(connector);
            } else {
               logger.warn("Static connector not found: " + config.isLocalTargetEnabled());
            }
         }

         pool = new StaticPool(targetFactory, scheduledExecutor, config.getCheckPeriod(), staticConnectors);
      } else {
         throw new IllegalStateException("Pool configuration not valid");
      }

      pool.setUsername(config.getUsername());
      pool.setPassword(config.getPassword());
      pool.setQuorumSize(config.getQuorumSize());
      pool.setQuorumTimeout(config.getQuorumTimeout());

      if (config.isLocalTargetEnabled()) {
         pool.addTarget(localTarget);
      }

      return pool;
   }

   private Policy deployPolicy(PolicyConfiguration policyConfig, Pool pool) throws ClassNotFoundException {
      PolicyFactory policyFactory = PolicyFactoryResolver.getInstance().resolve(policyConfig.getName());

      Policy policy = policyFactory.createPolicy(policyConfig.getName());

      policy.init(policyConfig.getProperties());

      if (policy.getTargetProbe() != null) {
         pool.addTargetProbe(policy.getTargetProbe());
      }

      return policy;
   }

   public BrokerBalancer getBalancer(String name) {
      return balancerControllers.get(name);
   }

   @Override
   public void start() throws Exception {
      for (BrokerBalancer brokerBalancer : balancerControllers.values()) {
         brokerBalancer.start();
      }

      started = true;
   }

   @Override
   public void stop() throws Exception {
      started = false;

      for (BrokerBalancer balancer : balancerControllers.values()) {
         balancer.stop();
         server.getManagementService().unregisterBrokerBalancer(balancer.getName());
      }
   }
}
