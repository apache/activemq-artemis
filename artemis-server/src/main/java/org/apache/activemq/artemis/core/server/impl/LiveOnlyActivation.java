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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LiveNodeLocator;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.cluster.ActiveMQServerSideProtocolManagerFactory;
import org.apache.activemq.artemis.core.server.cluster.ha.LiveOnlyPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ScaleDownPolicy;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.jboss.logging.Logger;

public class LiveOnlyActivation extends Activation {

   private static final Logger logger = Logger.getLogger(LiveOnlyActivation.class);

   //this is how we act when we initially start as live
   private LiveOnlyPolicy liveOnlyPolicy;

   private final ActiveMQServerImpl activeMQServer;

   private ServerLocatorInternal scaleDownServerLocator;

   private ClientSessionFactoryInternal scaleDownClientSessionFactory;

   public LiveOnlyActivation(ActiveMQServerImpl server, LiveOnlyPolicy liveOnlyPolicy) {
      this.activeMQServer = server;
      this.liveOnlyPolicy = liveOnlyPolicy;
   }

   @Override
   public void run() {
      try {
         activeMQServer.initialisePart1(false);

         activeMQServer.registerActivateCallback(activeMQServer.getNodeManager().startLiveNode());

         if (activeMQServer.getState() == ActiveMQServerImpl.SERVER_STATE.STOPPED || activeMQServer.getState() == ActiveMQServerImpl.SERVER_STATE.STOPPING) {
            return;
         }

         activeMQServer.initialisePart2(false);

         activeMQServer.completeActivation();

         if (activeMQServer.getIdentity() != null) {
            ActiveMQServerLogger.LOGGER.serverIsLive(activeMQServer.getIdentity());
         } else {
            ActiveMQServerLogger.LOGGER.serverIsLive();
         }
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.initializationError(e);
         activeMQServer.callActivationFailureListeners(e);
      }
   }

   @Override
   public void close(boolean permanently, boolean restarting) throws Exception {
      if (scaleDownServerLocator != null) {
         scaleDownServerLocator.close();
         scaleDownServerLocator = null;
      }

      NodeManager nodeManagerInUse = activeMQServer.getNodeManager();

      if (nodeManagerInUse != null) {
         if (permanently) {
            nodeManagerInUse.crashLiveServer();
         } else {
            nodeManagerInUse.pauseLiveServer();
         }
      }
   }

   @Override
   public void freezeConnections(RemotingService remotingService) {
      // connect to the scale-down target first so that when we freeze/disconnect the clients we can tell them where
      // we're sending the messages
      if (liveOnlyPolicy.getScaleDownPolicy() != null && liveOnlyPolicy.getScaleDownPolicy().isEnabled()) {
         connectToScaleDownTarget(liveOnlyPolicy.getScaleDownPolicy());
      }

      RemotingConnection rc = scaleDownClientSessionFactory == null ? null : scaleDownClientSessionFactory.getConnection();
      String nodeID = rc == null ? null : scaleDownClientSessionFactory.getServerLocator().getTopology().getMember(rc).getNodeId();
      if (remotingService != null) {
         remotingService.freeze(nodeID, null);
      }
   }

   @Override
   public void postConnectionFreeze() {
      if (liveOnlyPolicy.getScaleDownPolicy() != null && liveOnlyPolicy.getScaleDownPolicy().isEnabled() && scaleDownClientSessionFactory != null) {
         try {
            scaleDown();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.failedToScaleDown(e);
         } finally {
            scaleDownClientSessionFactory.close();
            scaleDownServerLocator.close();
         }
      }
   }

   public void connectToScaleDownTarget(ScaleDownPolicy scaleDownPolicy) {
      try {
         scaleDownServerLocator = ScaleDownPolicy.getScaleDownConnector(scaleDownPolicy, activeMQServer);
         //use a Node Locator to connect to the cluster
         scaleDownServerLocator.setProtocolManagerFactory(ActiveMQServerSideProtocolManagerFactory.getInstance(scaleDownServerLocator));
         LiveNodeLocator nodeLocator = scaleDownPolicy.getGroupName() == null ? new AnyLiveNodeLocatorForScaleDown(activeMQServer) : new NamedLiveNodeLocatorForScaleDown(scaleDownPolicy.getGroupName(), activeMQServer);
         scaleDownServerLocator.addClusterTopologyListener(nodeLocator);

         nodeLocator.connectToCluster(scaleDownServerLocator);
         // a timeout is necessary here in case we use a NamedLiveNodeLocatorForScaleDown and there's no matching node in the cluster
         // should the timeout be configurable?
         nodeLocator.locateNode(ActiveMQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT);
         ClientSessionFactoryInternal clientSessionFactory = null;
         while (clientSessionFactory == null) {
            Pair<TransportConfiguration, TransportConfiguration> possibleLive = null;
            try {
               possibleLive = nodeLocator.getLiveConfiguration();
               if (possibleLive == null)  // we've tried every connector
                  break;
               clientSessionFactory = (ClientSessionFactoryInternal) scaleDownServerLocator.createSessionFactory(possibleLive.getA(), 0, false);
            } catch (Exception e) {
               logger.trace("Failed to connect to " + possibleLive.getA());
               nodeLocator.notifyRegistrationFailed(false);
               if (clientSessionFactory != null) {
                  clientSessionFactory.close();
               }
               clientSessionFactory = null;
               // should I try the backup (i.e. getB()) from possibleLive?
            }
         }
         if (clientSessionFactory != null) {
            scaleDownClientSessionFactory = clientSessionFactory;
         } else {
            throw new ActiveMQException("Unable to connect to server for scale-down");
         }
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.failedToScaleDown(e);
      }
   }

   public long scaleDown() throws Exception {
      ScaleDownHandler scaleDownHandler = new ScaleDownHandler(activeMQServer.getPagingManager(), activeMQServer.getPostOffice(), activeMQServer.getNodeManager(), activeMQServer.getClusterManager().getClusterController(), activeMQServer.getStorageManager());
      ConcurrentMap<SimpleString, DuplicateIDCache> duplicateIDCaches = ((PostOfficeImpl) activeMQServer.getPostOffice()).getDuplicateIDCaches();
      Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap = new HashMap<>();
      for (SimpleString address : duplicateIDCaches.keySet()) {
         DuplicateIDCache duplicateIDCache = activeMQServer.getPostOffice().getDuplicateIDCache(address);
         duplicateIDMap.put(address, duplicateIDCache.getMap());
      }
      return scaleDownHandler.scaleDown(scaleDownClientSessionFactory, activeMQServer.getResourceManager(), duplicateIDMap, activeMQServer.getManagementService().getManagementAddress(), null);
   }
}
