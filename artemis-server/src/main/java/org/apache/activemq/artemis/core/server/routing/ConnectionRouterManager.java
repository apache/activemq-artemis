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

package org.apache.activemq.artemis.core.server.routing;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.config.routing.ConnectionRouterConfiguration;
import org.apache.activemq.artemis.core.config.routing.CacheConfiguration;
import org.apache.activemq.artemis.core.config.routing.NamedPropertyConfiguration;
import org.apache.activemq.artemis.core.config.routing.PoolConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.routing.caches.Cache;
import org.apache.activemq.artemis.core.server.routing.caches.LocalCache;
import org.apache.activemq.artemis.core.server.routing.policies.Policy;
import org.apache.activemq.artemis.core.server.routing.policies.PolicyFactory;
import org.apache.activemq.artemis.core.server.routing.policies.PolicyFactoryResolver;
import org.apache.activemq.artemis.core.server.routing.pools.ClusterPool;
import org.apache.activemq.artemis.core.server.routing.pools.DiscoveryGroupService;
import org.apache.activemq.artemis.core.server.routing.pools.DiscoveryPool;
import org.apache.activemq.artemis.core.server.routing.pools.DiscoveryService;
import org.apache.activemq.artemis.core.server.routing.pools.Pool;
import org.apache.activemq.artemis.core.server.routing.pools.StaticPool;
import org.apache.activemq.artemis.core.server.routing.targets.ActiveMQTargetFactory;
import org.apache.activemq.artemis.core.server.routing.targets.LocalTarget;
import org.apache.activemq.artemis.core.server.routing.targets.Target;
import org.apache.activemq.artemis.core.server.routing.targets.TargetFactory;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public final class ConnectionRouterManager implements ActiveMQComponent {
   private static final Logger logger = LoggerFactory.getLogger(ConnectionRouterManager.class);

   public static final String CACHE_ID_PREFIX = "$.BC.";


   private final Configuration config;

   private final ActiveMQServer server;

   private final ScheduledExecutorService scheduledExecutor;

   private volatile boolean started = false;

   private Map<String, ConnectionRouter> connectionRouters = new HashMap<>();


   @Override
   public boolean isStarted() {
      return started;
   }


   public ConnectionRouterManager(final Configuration config, final ActiveMQServer server, ScheduledExecutorService scheduledExecutor) {
      this.config = config;
      this.server = server;
      this.scheduledExecutor = scheduledExecutor;
   }

   public void deploy() throws Exception {
      for (ConnectionRouterConfiguration connectionRouterConfig : config.getConnectionRouters()) {
         deployConnectionRouter(connectionRouterConfig);
      }
   }

   public void deployConnectionRouter(ConnectionRouterConfiguration config) throws Exception {
      logger.debug("Deploying ConnectionRouter {}", config.getName());

      Target localTarget = new LocalTarget(null, server);


      Cache cache = null;
      CacheConfiguration cacheConfiguration = config.getCacheConfiguration();
      if (cacheConfiguration != null) {
         cache = deployCache(cacheConfiguration, config.getName());
      }

      Pool pool = null;
      final PoolConfiguration poolConfiguration = config.getPoolConfiguration();
      if (poolConfiguration != null) {
         pool = deployPool(config.getPoolConfiguration(), localTarget);
      }

      Policy policy = null;
      NamedPropertyConfiguration policyConfiguration = config.getPolicyConfiguration();
      if (policyConfiguration != null) {
         policy = deployPolicy(policyConfiguration, pool);
      }

      ConnectionRouter connectionRouter = new ConnectionRouter(config.getName(), config.getKeyType(),
         config.getKeyFilter(), localTarget, config.getLocalTargetFilter(), cache, pool, policy);

      connectionRouters.put(connectionRouter.getName(), connectionRouter);

      server.getManagementService().registerConnectionRouter(connectionRouter);
   }

   private Cache deployCache(CacheConfiguration configuration, String name) throws ClassNotFoundException {
      Cache cache = new LocalCache(CACHE_ID_PREFIX + name, configuration.isPersisted(),
         configuration.getTimeout(), server.getStorageManager());

      return cache;
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

   private Policy deployPolicy(NamedPropertyConfiguration policyConfig, Pool pool) throws ClassNotFoundException {
      PolicyFactory policyFactory = PolicyFactoryResolver.getInstance().resolve(policyConfig.getName());

      Policy policy = policyFactory.create();

      policy.init(policyConfig.getProperties());

      if (pool != null && policy.getTargetProbe() != null) {
         pool.addTargetProbe(policy.getTargetProbe());
      }

      return policy;
   }

   public ConnectionRouter getRouter(String name) {
      return connectionRouters.get(name);
   }

   @Override
   public void start() throws Exception {
      for (ConnectionRouter connectionRouter : connectionRouters.values()) {
         connectionRouter.start();
      }

      started = true;
   }

   @Override
   public void stop() throws Exception {
      started = false;

      for (ConnectionRouter connectionRouter : connectionRouters.values()) {
         connectionRouter.stop();
         server.getManagementService().unregisterConnectionRouter(connectionRouter.getName());
      }
   }
}
