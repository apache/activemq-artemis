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
package org.apache.activemq.artemis.core.server.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ConfigurationUtils;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.jboss.logging.Logger;

/**
 * This class contains some utils to allow a broker to check presence and role of another broker in the cluster.
 */
final class ClusterTopologySearch {

   private ClusterTopologySearch() {

   }

   /**
    * Determines whether there is a live server already running with nodeID.<br>
    * This search isn't filtering the caller broker transport and is meant to be used
    * when the broker acceptors aren't running yet.
    */
   public static boolean searchActiveLiveNodeId(String clusterName,
                                                String nodeId,
                                                long timeout,
                                                TimeUnit unit,
                                                Configuration serverConfiguration) throws ActiveMQException {
      if (serverConfiguration.getClusterConfigurations().isEmpty())
         return false;
      final ClusterConnectionConfiguration clusterConnectionConfiguration = ConfigurationUtils.getReplicationClusterConfiguration(serverConfiguration, clusterName);

      final LiveNodeIdListener liveNodeIdListener = new LiveNodeIdListener(nodeId, serverConfiguration.getClusterUser(), serverConfiguration.getClusterPassword());

      try (ServerLocatorInternal locator = createLocator(serverConfiguration, clusterConnectionConfiguration)) {
         // if would like to filter out a transport configuration:
         // locator.setClusterTransportConfiguration(callerBrokerTransportConfiguration)
         locator.addClusterTopologyListener(liveNodeIdListener);
         locator.setReconnectAttempts(0);
         try (ClientSessionFactoryInternal ignored = locator.connectNoWarnings()) {
            return liveNodeIdListener.awaitNodePresent(timeout, unit);
         } catch (Exception notConnected) {
            if (!(notConnected instanceof ActiveMQException) || ActiveMQExceptionType.INTERNAL_ERROR.equals(((ActiveMQException) notConnected).getType())) {
               // report all exceptions that aren't ActiveMQException and all INTERNAL_ERRORs
               ActiveMQServerLogger.LOGGER.failedConnectingToCluster(notConnected);
            }
            return false;
         }
      }
   }

   private static final class LiveNodeIdListener implements ClusterTopologyListener {

      private static final Logger logger = Logger.getLogger(LiveNodeIdListener.class);
      private final String nodeId;
      private final String user;
      private final String password;
      private final CountDownLatch searchCompleted;
      private boolean isNodePresent = false;

      LiveNodeIdListener(String nodeId, String user, String password) {
         this.nodeId = nodeId;
         this.user = user;
         this.password = password;
         this.searchCompleted = new CountDownLatch(1);
      }

      @Override
      public void nodeUP(TopologyMember topologyMember, boolean last) {
         boolean isOurNodeId = nodeId != null && nodeId.equals(topologyMember.getNodeId());
         if (isOurNodeId && isActive(topologyMember.getLive())) {
            isNodePresent = true;
         }
         if (isOurNodeId || last) {
            searchCompleted.countDown();
         }
      }

      public boolean awaitNodePresent(long timeout, TimeUnit unit) throws InterruptedException {
         searchCompleted.await(timeout, unit);
         return isNodePresent;
      }

      /**
       * In a cluster of replicated live/backup pairs if a backup crashes and then its live crashes the cluster will
       * retain the topology information of the live such that when the live server restarts it will check the
       * cluster to see if its nodeID is present (which it will be) and then it will activate as a backup rather than
       * a live. To prevent this situation an additional check is necessary to see if the server with the matching
       * nodeID is actually active or not which is done by attempting to make a connection to it.
       *
       * @param transportConfiguration
       * @return
       */
      private boolean isActive(TransportConfiguration transportConfiguration) {
         try (ServerLocator serverLocator = ActiveMQClient.createServerLocator(false, transportConfiguration);
              ClientSessionFactory clientSessionFactory = serverLocator.createSessionFactory();
              ClientSession clientSession = clientSessionFactory.createSession(user, password, false, false, false, false, 0)) {
            return true;
         } catch (Exception e) {
            logger.debug("isActive check failed", e);
            return false;
         }
      }

      @Override
      public void nodeDown(long eventUID, String nodeID) {
         // no-op
      }
   }

   private static ServerLocatorInternal createLocator(Configuration configuration,
                                                      ClusterConnectionConfiguration config) throws ActiveMQException {
      final ServerLocatorInternal locator;
      if (config.getDiscoveryGroupName() != null) {
         DiscoveryGroupConfiguration dg = configuration.getDiscoveryGroupConfigurations().get(config.getDiscoveryGroupName());

         if (dg == null) {
            throw ActiveMQMessageBundle.BUNDLE.noDiscoveryGroupFound(null);
         }
         locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithHA(dg);
      } else {
         TransportConfiguration[] tcConfigs = config.getStaticConnectors() != null ? configuration.getTransportConfigurations(config.getStaticConnectors()) : null;

         locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithHA(tcConfigs);
      }
      return locator;
   }

}
