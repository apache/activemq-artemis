/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.core.server.impl;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.Pair;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.HornetQClient;
import org.apache.activemq.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.core.postoffice.DuplicateIDCache;
import org.apache.activemq.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.core.remoting.server.RemotingService;
import org.apache.activemq.core.server.HornetQServerLogger;
import org.apache.activemq.core.server.LiveNodeLocator;
import org.apache.activemq.core.server.cluster.HornetQServerSideProtocolManagerFactory;
import org.apache.activemq.core.server.cluster.ha.LiveOnlyPolicy;
import org.apache.activemq.core.server.cluster.ha.ScaleDownPolicy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class LiveOnlyActivation extends Activation
{
   //this is how we act when we initially start as live
   private LiveOnlyPolicy liveOnlyPolicy;

   private final HornetQServerImpl hornetQServer;

   private ServerLocatorInternal scaleDownServerLocator;

   private ClientSessionFactoryInternal scaleDownClientSessionFactory;

   public LiveOnlyActivation(HornetQServerImpl server, LiveOnlyPolicy liveOnlyPolicy)
   {
      this.hornetQServer = server;
      this.liveOnlyPolicy = liveOnlyPolicy;
   }

   public void run()
   {
      try
      {
         hornetQServer.initialisePart1(false);

         hornetQServer.initialisePart2(false);

         if (hornetQServer.getIdentity() != null)
         {
            HornetQServerLogger.LOGGER.serverIsLive(hornetQServer.getIdentity());
         }
         else
         {
            HornetQServerLogger.LOGGER.serverIsLive();
         }
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.initializationError(e);
      }
   }

   @Override
   public void close(boolean permanently, boolean restarting) throws Exception
   {
      if (scaleDownServerLocator != null)
      {
         scaleDownServerLocator.close();
         scaleDownServerLocator = null;
      }
   }

   public void freezeConnections(RemotingService remotingService)
   {
      // connect to the scale-down target first so that when we freeze/disconnect the clients we can tell them where
      // we're sending the messages
      if (liveOnlyPolicy.getScaleDownPolicy() != null && liveOnlyPolicy.getScaleDownPolicy().isEnabled())
      {
         connectToScaleDownTarget(liveOnlyPolicy.getScaleDownPolicy());
      }

      TransportConfiguration tc = scaleDownClientSessionFactory == null ? null : scaleDownClientSessionFactory.getConnectorConfiguration();
      String nodeID = tc == null ? null : scaleDownClientSessionFactory.getServerLocator().getTopology().getMember(tc).getNodeId();
      if (remotingService != null)
      {
         remotingService.freeze(nodeID, null);
      }
   }

   @Override
   public void postConnectionFreeze()
   {
      if (liveOnlyPolicy.getScaleDownPolicy() != null
            && liveOnlyPolicy.getScaleDownPolicy().isEnabled()
            && scaleDownClientSessionFactory != null)
      {
         try
         {
            scaleDown();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.failedToScaleDown(e);
         }
         finally
         {
            scaleDownClientSessionFactory.close();
            scaleDownServerLocator.close();
         }
      }
   }

   public void connectToScaleDownTarget(ScaleDownPolicy scaleDownPolicy)
   {
      try
      {
         scaleDownServerLocator = ScaleDownPolicy.getScaleDownConnector(scaleDownPolicy, hornetQServer);
         //use a Node Locator to connect to the cluster
         scaleDownServerLocator.setProtocolManagerFactory(HornetQServerSideProtocolManagerFactory.getInstance());
         LiveNodeLocator nodeLocator = scaleDownPolicy.getGroupName() == null ?
               new AnyLiveNodeLocatorForScaleDown(hornetQServer) :
               new NamedLiveNodeLocatorForScaleDown(scaleDownPolicy.getGroupName(), hornetQServer);
         scaleDownServerLocator.addClusterTopologyListener(nodeLocator);

         nodeLocator.connectToCluster(scaleDownServerLocator);
         // a timeout is necessary here in case we use a NamedLiveNodeLocatorForScaleDown and there's no matching node in the cluster
         // should the timeout be configurable?
         nodeLocator.locateNode(HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT);
         ClientSessionFactoryInternal clientSessionFactory = null;
         while (clientSessionFactory == null)
         {
            Pair<TransportConfiguration, TransportConfiguration> possibleLive = null;
            try
            {
               possibleLive = nodeLocator.getLiveConfiguration();
               if (possibleLive == null)  // we've tried every connector
                  break;
               clientSessionFactory = (ClientSessionFactoryInternal) scaleDownServerLocator.createSessionFactory(possibleLive.getA(), 0, false);
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.trace("Failed to connect to " + possibleLive.getA());
               nodeLocator.notifyRegistrationFailed(false);
               if (clientSessionFactory != null)
               {
                  clientSessionFactory.close();
               }
               clientSessionFactory = null;
               // should I try the backup (i.e. getB()) from possibleLive?
            }
         }
         if (clientSessionFactory != null)
         {
            scaleDownClientSessionFactory = clientSessionFactory;
         }
         else
         {
            throw new ActiveMQException("Unable to connect to server for scale-down");
         }
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.failedToScaleDown(e);
      }
   }


   public long scaleDown() throws Exception
   {
      ScaleDownHandler scaleDownHandler = new ScaleDownHandler(hornetQServer.getPagingManager(),
            hornetQServer.getPostOffice(),
            hornetQServer.getNodeManager(),
            hornetQServer.getClusterManager().getClusterController());
      ConcurrentMap<SimpleString, DuplicateIDCache> duplicateIDCaches = ((PostOfficeImpl) hornetQServer.getPostOffice()).getDuplicateIDCaches();
      Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap = new HashMap<>();
      for (SimpleString address : duplicateIDCaches.keySet())
      {
         DuplicateIDCache duplicateIDCache = hornetQServer.getPostOffice().getDuplicateIDCache(address);
         duplicateIDMap.put(address, duplicateIDCache.getMap());
      }
      return scaleDownHandler.scaleDown(scaleDownClientSessionFactory, hornetQServer.getResourceManager(), duplicateIDMap,
            hornetQServer.getManagementService().getManagementAddress(), null);
   }
}
