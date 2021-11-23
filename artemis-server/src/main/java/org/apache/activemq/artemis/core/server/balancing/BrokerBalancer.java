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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.balancing.policies.Policy;
import org.apache.activemq.artemis.core.server.balancing.pools.Pool;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKeyResolver;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetResult;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class BrokerBalancer implements ActiveMQComponent {
   private static final Logger logger = Logger.getLogger(BrokerBalancer.class);


   public static final String CLIENT_ID_PREFIX = ActiveMQDefaultConfiguration.DEFAULT_INTERNAL_NAMING_PREFIX + "balancer.client.";


   private final String name;

   private final TargetKey targetKey;

   private final TargetKeyResolver targetKeyResolver;

   private final TargetResult localTarget;

   private final Pattern localTargetFilter;

   private final Pool pool;

   private final Policy policy;

   private final Cache<String, TargetResult> cache;

   private volatile boolean started = false;

   public String getName() {
      return name;
   }

   public TargetKey getTargetKey() {
      return targetKey;
   }

   public Target getLocalTarget() {
      return localTarget.getTarget();
   }

   public String getLocalTargetFilter() {
      return localTargetFilter != null ? localTargetFilter.pattern() : null;
   }

   public Pool getPool() {
      return pool;
   }

   public Policy getPolicy() {
      return policy;
   }

   public Cache<String, TargetResult> getCache() {
      return cache;
   }

   @Override
   public boolean isStarted() {
      return started;
   }


   public BrokerBalancer(final String name, final TargetKey targetKey, final String targetKeyFilter, final Target localTarget, final String localTargetFilter, final Pool pool, final Policy policy, final int cacheTimeout) {
      this.name = name;

      this.targetKey = targetKey;

      this.targetKeyResolver = new TargetKeyResolver(targetKey, targetKeyFilter);

      this.localTarget = new TargetResult(localTarget);

      this.localTargetFilter = localTargetFilter != null ? Pattern.compile(localTargetFilter) : null;

      this.pool = pool;

      this.policy = policy;

      if (cacheTimeout == -1) {
         this.cache = CacheBuilder.newBuilder().build();
      } else if (cacheTimeout > 0) {
         this.cache = CacheBuilder.newBuilder().expireAfterAccess(cacheTimeout, TimeUnit.MILLISECONDS).build();
      } else {
         this.cache = null;
      }
   }

   @Override
   public void start() throws Exception {
      if (pool != null) {
         pool.start();
      }

      started = true;
   }

   @Override
   public void stop() throws Exception {
      started = false;

      if (pool != null) {
         pool.stop();
      }
   }

   public TargetResult getTarget(Connection connection, String clientID, String username) {
      if (clientID != null && clientID.startsWith(BrokerBalancer.CLIENT_ID_PREFIX)) {
         if (logger.isDebugEnabled()) {
            logger.debug("The clientID [" + clientID + "] starts with BrokerBalancer.CLIENT_ID_PREFIX");
         }

         return localTarget;
      }

      return getTarget(targetKeyResolver.resolve(connection, clientID, username));
   }

   public TargetResult getTarget(String key) {

      if (this.localTargetFilter != null && this.localTargetFilter.matcher(key).matches()) {
         if (logger.isDebugEnabled()) {
            logger.debug("The " + targetKey + "[" + key + "] matches the localTargetFilter " + localTargetFilter.pattern());
         }

         return localTarget;
      }

      if (pool == null) {
         return TargetResult.REFUSED_USE_ANOTHER_RESULT;
      }

      TargetResult result = null;

      if (cache != null) {
         result = cache.getIfPresent(key);
      }

      if (result != null) {
         if (pool.isTargetReady(result.getTarget())) {
            if (logger.isDebugEnabled()) {
               logger.debug("The cache returns [" + result.getTarget() + "] ready for " + targetKey + "[" + key + "]");
            }

            return result;
         }

         if (logger.isDebugEnabled()) {
            logger.debug("The cache returns [" + result.getTarget() + "] not ready for " + targetKey + "[" + key + "]");
         }
      }

      List<Target> targets = pool.getTargets();

      Target target = policy.selectTarget(targets, key);

      if (logger.isDebugEnabled()) {
         logger.debug("The policy selects [" + target + "] from " + targets + " for " + targetKey + "[" + key + "]");
      }

      if (target != null) {
         result = new TargetResult(target);
         if (cache != null) {
            if (logger.isDebugEnabled()) {
               logger.debug("Caching " + targetKey + "[" + key + "] for [" + target + "]");
            }
            cache.put(key, result);
         }
      }

      return result != null ? result : TargetResult.REFUSED_UNAVAILABLE_RESULT;
   }
}
