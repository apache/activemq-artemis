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
package org.apache.activemq.artemis.api.core.client;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.client.loadbalance.RoundRobinConnectionLoadBalancingPolicy;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.uri.ServerLocatorParser;

import java.net.URI;

/**
 * Utility class for creating ActiveMQ Artemis {@link ClientSessionFactory} objects.
 * <p>
 * Once a {@link ClientSessionFactory} has been created, it can be further configured using its
 * setter methods before creating the sessions. Once a session is created, the factory can no longer
 * be modified (its setter methods will throw a {@link IllegalStateException}.
 */
public final class ActiveMQClient {

   public static final String DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME = RoundRobinConnectionLoadBalancingPolicy.class.getCanonicalName();

   public static final long DEFAULT_CLIENT_FAILURE_CHECK_PERIOD = ActiveMQDefaultConfiguration.getDefaultClientFailureCheckPeriod();

   public static final long DEFAULT_CLIENT_FAILURE_CHECK_PERIOD_INVM = -1;

   // 1 minute - this should be higher than ping period

   public static final long DEFAULT_CONNECTION_TTL = ActiveMQDefaultConfiguration.getDefaultConnectionTtl();

   public static final long DEFAULT_CONNECTION_TTL_INVM = -1;

   // Any message beyond this size is considered a large message (to be sent in chunks)

   public static final int DEFAULT_MIN_LARGE_MESSAGE_SIZE = 100 * 1024;

   public static final boolean DEFAULT_COMPRESS_LARGE_MESSAGES = false;

   public static final int DEFAULT_CONSUMER_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_CONSUMER_MAX_RATE = -1;

   public static final int DEFAULT_CONFIRMATION_WINDOW_SIZE = -1;

   public static final int DEFAULT_PRODUCER_WINDOW_SIZE = 64 * 1024;

   public static final int DEFAULT_PRODUCER_MAX_RATE = -1;

   public static final boolean DEFAULT_BLOCK_ON_ACKNOWLEDGE = false;

   public static final boolean DEFAULT_BLOCK_ON_DURABLE_SEND = true;

   public static final boolean DEFAULT_BLOCK_ON_NON_DURABLE_SEND = false;

   public static final boolean DEFAULT_AUTO_GROUP = false;

   public static final long DEFAULT_CALL_TIMEOUT = 30000;

   public static final long DEFAULT_CALL_FAILOVER_TIMEOUT = 30000;

   public static final int DEFAULT_ACK_BATCH_SIZE = 1024 * 1024;

   public static final boolean DEFAULT_PRE_ACKNOWLEDGE = false;

   public static final long DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT = 10000;

   public static final long DEFAULT_DISCOVERY_REFRESH_TIMEOUT = 10000;

   public static final int DEFAULT_DISCOVERY_PORT = 9876;

   public static final long DEFAULT_RETRY_INTERVAL = 2000;

   public static final double DEFAULT_RETRY_INTERVAL_MULTIPLIER = ActiveMQDefaultConfiguration.getDefaultRetryIntervalMultiplier();

   public static final long DEFAULT_MAX_RETRY_INTERVAL = ActiveMQDefaultConfiguration.getDefaultMaxRetryInterval();

   public static final int DEFAULT_RECONNECT_ATTEMPTS = 0;

   public static final int INITIAL_CONNECT_ATTEMPTS = 1;

   public static final boolean DEFAULT_FAILOVER_ON_INITIAL_CONNECTION = false;

   public static final boolean DEFAULT_IS_HA = false;

   public static final boolean DEFAULT_USE_GLOBAL_POOLS = true;

   public static final int DEFAULT_THREAD_POOL_MAX_SIZE = -1;

   public static final int DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE = 5;

   public static final boolean DEFAULT_CACHE_LARGE_MESSAGE_CLIENT = false;

   public static final int DEFAULT_INITIAL_MESSAGE_PACKET_SIZE = 1500;

   public static final boolean DEFAULT_XA = false;

   public static final boolean DEFAULT_HA = false;

   public static final String DEFAULT_CORE_PROTOCOL = "CORE";

   /**
    * Creates an ActiveMQConnectionFactory;
    *
    * @return the ActiveMQConnectionFactory
    */
   public static ServerLocator createServerLocator(final String url) throws Exception {
      ServerLocatorParser parser = new ServerLocatorParser();
      return parser.newObject(new URI(url), null);
   }

   /**
    * Create a ServerLocator which creates session factories using a static list of transportConfigurations, the ServerLocator is not updated automatically
    * as the cluster topology changes, and no HA backup information is propagated to the client
    *
    * @param transportConfigurations
    * @return the ServerLocator
    */
   public static ServerLocator createServerLocatorWithoutHA(TransportConfiguration... transportConfigurations) {
      return new ServerLocatorImpl(false, transportConfigurations);
   }

   /**
    * Create a ServerLocator which creates session factories using a static list of transportConfigurations, the ServerLocator is not updated automatically
    * as the cluster topology changes, and no HA backup information is propagated to the client
    *
    * @param ha                      The Locator will support topology updates and ha (this required the server to be clustered, otherwise the first connection will timeout)
    * @param transportConfigurations
    * @return the ServerLocator
    */
   public static ServerLocator createServerLocator(final boolean ha,
                                                   TransportConfiguration... transportConfigurations) {
      return new ServerLocatorImpl(ha, transportConfigurations);
   }

   /**
    * Create a ServerLocator which creates session factories from a set of live servers, no HA
    * backup information is propagated to the client
    * <p>
    * The UDP address and port are used to listen for live servers in the cluster
    *
    * @param groupConfiguration
    * @return the ServerLocator
    */
   public static ServerLocator createServerLocatorWithoutHA(final DiscoveryGroupConfiguration groupConfiguration) {
      return new ServerLocatorImpl(false, groupConfiguration);
   }

   /**
    * Create a ServerLocator which creates session factories from a set of live servers, no HA
    * backup information is propagated to the client The UDP address and port are used to listen for
    * live servers in the cluster
    *
    * @param ha                 The Locator will support topology updates and ha (this required the server to be
    *                           clustered, otherwise the first connection will timeout)
    * @param groupConfiguration
    * @return the ServerLocator
    */
   public static ServerLocator createServerLocator(final boolean ha,
                                                   final DiscoveryGroupConfiguration groupConfiguration) {
      return new ServerLocatorImpl(ha, groupConfiguration);
   }

   /**
    * Create a ServerLocator which will receive cluster topology updates from the cluster as servers
    * leave or join and new backups are appointed or removed.
    * <p>
    * The initial list of servers supplied in this method is simply to make an initial connection to
    * the cluster, once that connection is made, up to date cluster topology information is
    * downloaded and automatically updated whenever the cluster topology changes.
    * <p>
    * If the topology includes backup servers that information is also propagated to the client so
    * that it can know which server to failover onto in case of live server failure.
    *
    * @param initialServers The initial set of servers used to make a connection to the cluster.
    *                       Each one is tried in turn until a successful connection is made. Once a connection
    *                       is made, the cluster topology is downloaded and the rest of the list is ignored.
    * @return the ServerLocator
    */
   public static ServerLocator createServerLocatorWithHA(TransportConfiguration... initialServers) {
      return new ServerLocatorImpl(true, initialServers);
   }

   /**
    * Create a ServerLocator which will receive cluster topology updates from the cluster as servers
    * leave or join and new backups are appointed or removed.
    * <p>
    * The discoveryAddress and discoveryPort parameters in this method are used to listen for UDP
    * broadcasts which contain connection information for members of the cluster. The broadcasted
    * connection information is simply used to make an initial connection to the cluster, once that
    * connection is made, up to date cluster topology information is downloaded and automatically
    * updated whenever the cluster topology changes.
    * <p>
    * If the topology includes backup servers that information is also propagated to the client so
    * that it can know which server to failover onto in case of live server failure.
    *
    * @param groupConfiguration
    * @return the ServerLocator
    */
   public static ServerLocator createServerLocatorWithHA(final DiscoveryGroupConfiguration groupConfiguration) {
      return new ServerLocatorImpl(true, groupConfiguration);
   }

   private ActiveMQClient() {
      // Utility class
   }
}
