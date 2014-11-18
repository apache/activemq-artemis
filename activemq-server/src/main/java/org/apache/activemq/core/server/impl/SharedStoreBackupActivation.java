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
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.paging.PagingManager;
import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.postoffice.PostOffice;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.HornetQServerLogger;
import org.apache.activemq.core.server.NodeManager;
import org.apache.activemq.core.server.QueueFactory;
import org.apache.activemq.core.server.cluster.ha.ScaleDownPolicy;
import org.apache.activemq.core.server.cluster.ha.SharedStoreSlavePolicy;
import org.apache.activemq.core.server.group.GroupingHandler;
import org.apache.activemq.core.server.management.ManagementService;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeUnit;

public final class SharedStoreBackupActivation extends Activation
{
   //this is how we act as a backup
   private SharedStoreSlavePolicy sharedStoreSlavePolicy;

   private HornetQServerImpl hornetQServer;

   private final Object failbackCheckerGuard = new Object();

   private boolean cancelFailBackChecker;

   public SharedStoreBackupActivation(HornetQServerImpl server, SharedStoreSlavePolicy sharedStoreSlavePolicy)
   {
      this.hornetQServer = server;
      this.sharedStoreSlavePolicy = sharedStoreSlavePolicy;
      synchronized (failbackCheckerGuard)
      {
         cancelFailBackChecker = false;
      }
   }

   public void run()
   {
      try
      {
         hornetQServer.getNodeManager().startBackup();

         boolean scalingDown = sharedStoreSlavePolicy.getScaleDownPolicy() != null;

         if (!hornetQServer.initialisePart1(scalingDown))
            return;

         hornetQServer.getBackupManager().start();

         hornetQServer.setState(HornetQServerImpl.SERVER_STATE.STARTED);

         HornetQServerLogger.LOGGER.backupServerStarted(hornetQServer.getVersion().getFullVersion(), hornetQServer.getNodeManager().getNodeId());

         hornetQServer.getNodeManager().awaitLiveNode();

         sharedStoreSlavePolicy.getSharedStoreMasterPolicy().setSharedStoreSlavePolicy(sharedStoreSlavePolicy);

         hornetQServer.setHAPolicy(sharedStoreSlavePolicy.getSharedStoreMasterPolicy());

         //hornetQServer.configuration.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);

         hornetQServer.getBackupManager().activated();
         if (hornetQServer.getState() != HornetQServerImpl.SERVER_STATE.STARTED)
         {
            return;
         }

         hornetQServer.initialisePart2(scalingDown);

         if (scalingDown)
         {
            HornetQServerLogger.LOGGER.backupServerScaledDown();
            Thread t = new Thread(new Runnable()
            {
               @Override
               public void run()
               {
                  try
                  {
                     hornetQServer.stop();
                     //we are shared store but if we were started by a parent server then we shouldn't restart
                     if (sharedStoreSlavePolicy.isRestartBackup())
                     {
                        hornetQServer.start();
                     }
                  }
                  catch (Exception e)
                  {
                     HornetQServerLogger.LOGGER.serverRestartWarning();
                  }
               }
            });
            t.start();
            return;
         }
         else
         {
            HornetQServerLogger.LOGGER.backupServerIsLive();

            hornetQServer.getNodeManager().releaseBackup();
         }
         if (sharedStoreSlavePolicy.isAllowAutoFailBack())
         {
            startFailbackChecker();
         }
      }
      catch (InterruptedException e)
      {
         // this is ok, we are being stopped
      }
      catch (ClosedChannelException e)
      {
         // this is ok too, we are being stopped
      }
      catch (Exception e)
      {
         if (!(e.getCause() instanceof InterruptedException))
         {
            HornetQServerLogger.LOGGER.initializationError(e);
         }
      }
      catch (Throwable e)
      {
         HornetQServerLogger.LOGGER.initializationError(e);
      }
   }

   public void close(boolean permanently, boolean restarting) throws Exception
   {
      if (!restarting)
      {
         synchronized (failbackCheckerGuard)
         {
            cancelFailBackChecker = true;
         }
      }
      // To avoid a NPE cause by the stop
      NodeManager nodeManagerInUse = hornetQServer.getNodeManager();

      //we need to check as the servers policy may have changed
      if (hornetQServer.getHAPolicy().isBackup())
      {

         hornetQServer.interrupBackupThread(nodeManagerInUse);


         if (nodeManagerInUse != null)
         {
            nodeManagerInUse.stopBackup();
         }
      }
      else
      {

         if (nodeManagerInUse != null)
         {
            // if we are now live, behave as live
            // We need to delete the file too, otherwise the backup will failover when we shutdown or if the backup is
            // started before the live
            if (sharedStoreSlavePolicy.isFailoverOnServerShutdown() || permanently)
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

   @Override
   public JournalLoader createJournalLoader(PostOffice postOffice, PagingManager pagingManager, StorageManager storageManager, QueueFactory queueFactory, NodeManager nodeManager, ManagementService managementService, GroupingHandler groupingHandler, Configuration configuration, HornetQServer parentServer) throws ActiveMQException
   {
      if (sharedStoreSlavePolicy.getScaleDownPolicy() != null)
      {
         return new BackupRecoveryJournalLoader(postOffice,
               pagingManager,
               storageManager,
               queueFactory,
               nodeManager,
               managementService,
               groupingHandler,
               configuration,
               parentServer,
               ScaleDownPolicy.getScaleDownConnector(sharedStoreSlavePolicy.getScaleDownPolicy(), hornetQServer),
               hornetQServer.getClusterManager().getClusterController());
      }
      else
      {
         return super.createJournalLoader(postOffice,
               pagingManager,
               storageManager,
               queueFactory,
               nodeManager,
               managementService,
               groupingHandler,
               configuration,
               parentServer);
      }
   }

   /**
    * To be called by backup trying to fail back the server
    */
   private void startFailbackChecker()
   {
      hornetQServer.getScheduledPool().scheduleAtFixedRate(new FailbackChecker(), 1000L, 1000L, TimeUnit.MILLISECONDS);
   }
   private class FailbackChecker implements Runnable
   {
      private boolean restarting = false;

      public void run()
      {
         try
         {
            if (!restarting && hornetQServer.getNodeManager().isAwaitingFailback())
            {
               HornetQServerLogger.LOGGER.awaitFailBack();
               restarting = true;
               Thread t = new Thread(new Runnable()
               {
                  public void run()
                  {
                     try
                     {
                        HornetQServerLogger.LOGGER.debug(hornetQServer + "::Stopping live node in favor of failback");

                        hornetQServer.stop(true, false, true);
                        // We need to wait some time before we start the backup again
                        // otherwise we may eventually start before the live had a chance to get it
                        Thread.sleep(sharedStoreSlavePolicy.getFailbackDelay());
                        synchronized (failbackCheckerGuard)
                        {
                           if (cancelFailBackChecker || !sharedStoreSlavePolicy.isRestartBackup())
                              return;

                           hornetQServer.setHAPolicy(sharedStoreSlavePolicy);
                           HornetQServerLogger.LOGGER.debug(hornetQServer +
                                 "::Starting backup node now after failback");
                           hornetQServer.start();
                        }
                     }
                     catch (Exception e)
                     {
                        HornetQServerLogger.LOGGER.serverRestartWarning();
                     }
                  }
               });
               t.start();
            }
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.serverRestartWarning(e);
         }
      }
   }
}
