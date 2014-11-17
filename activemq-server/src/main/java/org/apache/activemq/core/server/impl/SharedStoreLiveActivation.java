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
package org.apache.activemq6.core.server.impl;

import org.apache.activemq6.core.server.HornetQServerLogger;
import org.apache.activemq6.core.server.NodeManager;
import org.apache.activemq6.core.server.cluster.ha.SharedStoreMasterPolicy;

/**
* Created by andy on 04/09/14.
*/
public final class SharedStoreLiveActivation extends LiveActivation
{
   //this is how we act when we initially start as live
   private SharedStoreMasterPolicy sharedStoreMasterPolicy;

   private HornetQServerImpl hornetQServer;

   public SharedStoreLiveActivation(HornetQServerImpl server, SharedStoreMasterPolicy sharedStoreMasterPolicy)
   {
      this.hornetQServer = server;
      this.sharedStoreMasterPolicy = sharedStoreMasterPolicy;
   }

   public void run()
   {
      try
      {
         HornetQServerLogger.LOGGER.awaitingLiveLock();

         hornetQServer.checkJournalDirectory();

         if (HornetQServerLogger.LOGGER.isDebugEnabled())
         {
            HornetQServerLogger.LOGGER.debug("First part initialization on " + this);
         }

         if (!hornetQServer.initialisePart1(false))
            return;

         if (hornetQServer.getNodeManager().isBackupLive())
         {
            /*
             * looks like we've failed over at some point need to inform that we are the backup
             * so when the current live goes down they failover to us
             */
            if (HornetQServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQServerLogger.LOGGER.debug("announcing backup to the former live" + this);
            }
            hornetQServer.getBackupManager().start();
            hornetQServer.getBackupManager().announceBackup();
            Thread.sleep(sharedStoreMasterPolicy.getFailbackDelay());
         }

         hornetQServer.getNodeManager().startLiveNode();

         if (hornetQServer.getState() == HornetQServerImpl.SERVER_STATE.STOPPED || hornetQServer.getState() == HornetQServerImpl.SERVER_STATE.STOPPING)
         {
            return;
         }

         hornetQServer.initialisePart2(false);

         HornetQServerLogger.LOGGER.serverIsLive();
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.initializationError(e);
      }
   }

   public void close(boolean permanently, boolean restarting) throws Exception
   {
      // TO avoid a NPE from stop
      NodeManager nodeManagerInUse = hornetQServer.getNodeManager();

      if (nodeManagerInUse != null)
      {
         if (sharedStoreMasterPolicy.isFailoverOnServerShutdown() || permanently)
         {
            nodeManagerInUse.crashLiveServer();
         }
         else
         {
            nodeManagerInUse.pauseLiveServer();
         }
      }
   }
}
