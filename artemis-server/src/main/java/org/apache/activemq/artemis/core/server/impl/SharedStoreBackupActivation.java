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

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.cluster.ha.ScaleDownPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreSlavePolicy;
import org.apache.activemq.artemis.core.server.group.GroupingHandler;
import org.apache.activemq.artemis.core.server.management.ManagementService;

public final class SharedStoreBackupActivation extends Activation {

   //this is how we act as a backup
   private SharedStoreSlavePolicy sharedStoreSlavePolicy;

   private ActiveMQServerImpl activeMQServer;

   private final Object failbackCheckerGuard = new Object();

   private boolean cancelFailBackChecker;

   public SharedStoreBackupActivation(ActiveMQServerImpl server, SharedStoreSlavePolicy sharedStoreSlavePolicy) {
      this.activeMQServer = server;
      this.sharedStoreSlavePolicy = sharedStoreSlavePolicy;
      synchronized (failbackCheckerGuard) {
         cancelFailBackChecker = false;
      }
   }

   public void run() {
      try {
         activeMQServer.getNodeManager().startBackup();

         ScaleDownPolicy scaleDownPolicy = sharedStoreSlavePolicy.getScaleDownPolicy();

         boolean scalingDown = scaleDownPolicy != null && scaleDownPolicy.isEnabled();

         if (!activeMQServer.initialisePart1(scalingDown))
            return;

         activeMQServer.getBackupManager().start();

         activeMQServer.setState(ActiveMQServerImpl.SERVER_STATE.STARTED);

         ActiveMQServerLogger.LOGGER.backupServerStarted(activeMQServer.getVersion().getFullVersion(), activeMQServer.getNodeManager().getNodeId());

         activeMQServer.getNodeManager().awaitLiveNode();

         sharedStoreSlavePolicy.getSharedStoreMasterPolicy().setSharedStoreSlavePolicy(sharedStoreSlavePolicy);

         activeMQServer.setHAPolicy(sharedStoreSlavePolicy.getSharedStoreMasterPolicy());

         //activeMQServer.configuration.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);

         activeMQServer.getBackupManager().activated();
         if (activeMQServer.getState() != ActiveMQServerImpl.SERVER_STATE.STARTED) {
            return;
         }

         activeMQServer.initialisePart2(scalingDown);

         activeMQServer.completeActivation();

         if (scalingDown) {
            ActiveMQServerLogger.LOGGER.backupServerScaledDown();
            Thread t = new Thread(new Runnable() {
               @Override
               public void run() {
                  try {
                     activeMQServer.stop();
                     //we are shared store but if we were started by a parent server then we shouldn't restart
                     if (sharedStoreSlavePolicy.isRestartBackup()) {
                        activeMQServer.start();
                     }
                  }
                  catch (Exception e) {
                     ActiveMQServerLogger.LOGGER.serverRestartWarning();
                  }
               }
            });
            t.start();
            return;
         }
         else {
            ActiveMQServerLogger.LOGGER.backupServerIsLive();

            activeMQServer.getNodeManager().releaseBackup();
         }
         if (sharedStoreSlavePolicy.isAllowAutoFailBack()) {
            startFailbackChecker();
         }
      }
      catch (InterruptedException e) {
         // this is ok, we are being stopped
      }
      catch (ClosedChannelException e) {
         // this is ok too, we are being stopped
      }
      catch (Exception e) {
         if (!(e.getCause() instanceof InterruptedException)) {
            ActiveMQServerLogger.LOGGER.initializationError(e);
         }
      }
      catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.initializationError(e);
      }
   }

   public void close(boolean permanently, boolean restarting) throws Exception {
      if (!restarting) {
         synchronized (failbackCheckerGuard) {
            cancelFailBackChecker = true;
         }
      }
      // To avoid a NPE cause by the stop
      NodeManager nodeManagerInUse = activeMQServer.getNodeManager();

      //we need to check as the servers policy may have changed
      if (activeMQServer.getHAPolicy().isBackup()) {

         activeMQServer.interrupBackupThread(nodeManagerInUse);

         if (nodeManagerInUse != null) {
            nodeManagerInUse.stopBackup();
         }
      }
      else {

         if (nodeManagerInUse != null) {
            // if we are now live, behave as live
            // We need to delete the file too, otherwise the backup will failover when we shutdown or if the backup is
            // started before the live
            if (sharedStoreSlavePolicy.isFailoverOnServerShutdown() || permanently) {
               nodeManagerInUse.crashLiveServer();
            }
            else {
               nodeManagerInUse.pauseLiveServer();
            }
         }
      }
   }

   @Override
   public JournalLoader createJournalLoader(PostOffice postOffice,
                                            PagingManager pagingManager,
                                            StorageManager storageManager,
                                            QueueFactory queueFactory,
                                            NodeManager nodeManager,
                                            ManagementService managementService,
                                            GroupingHandler groupingHandler,
                                            Configuration configuration,
                                            ActiveMQServer parentServer) throws ActiveMQException {
      if (sharedStoreSlavePolicy.getScaleDownPolicy() != null && sharedStoreSlavePolicy.getScaleDownPolicy().isEnabled()) {
         return new BackupRecoveryJournalLoader(postOffice, pagingManager, storageManager, queueFactory, nodeManager, managementService, groupingHandler, configuration, parentServer, ScaleDownPolicy.getScaleDownConnector(sharedStoreSlavePolicy.getScaleDownPolicy(), activeMQServer), activeMQServer.getClusterManager().getClusterController());
      }
      else {
         return super.createJournalLoader(postOffice, pagingManager, storageManager, queueFactory, nodeManager, managementService, groupingHandler, configuration, parentServer);
      }
   }

   /**
    * To be called by backup trying to fail back the server
    */
   private void startFailbackChecker() {
      activeMQServer.getScheduledPool().scheduleAtFixedRate(new FailbackChecker(), 1000L, 1000L, TimeUnit.MILLISECONDS);
   }

   private class FailbackChecker implements Runnable {
      BackupTopologyListener backupListener;

      FailbackChecker() {
         backupListener = new BackupTopologyListener(activeMQServer.getNodeID().toString());
         activeMQServer.getClusterManager().getDefaultConnection(null).addClusterTopologyListener(backupListener);
      }

      private boolean restarting = false;

      public void run() {
         try {
            if (!restarting && activeMQServer.getNodeManager().isAwaitingFailback()) {
               if (backupListener.waitForBackup()) {
                  ActiveMQServerLogger.LOGGER.awaitFailBack();
                  restarting = true;
                  Thread t = new Thread(new Runnable() {
                     public void run() {
                        try {
                           ActiveMQServerLogger.LOGGER.debug(activeMQServer + "::Stopping live node in favor of failback");

                           NodeManager nodeManager = activeMQServer.getNodeManager();
                           activeMQServer.stop(true, false, true);

                           // ensure that the server to which we are failing back actually starts fully before we restart
                           nodeManager.start();
                           nodeManager.awaitLiveStatus();
                           nodeManager.stop();

                           synchronized (failbackCheckerGuard) {
                              if (cancelFailBackChecker || !sharedStoreSlavePolicy.isRestartBackup())
                                 return;

                              activeMQServer.setHAPolicy(sharedStoreSlavePolicy);
                              ActiveMQServerLogger.LOGGER.debug(activeMQServer + "::Starting backup node now after failback");
                              activeMQServer.start();
                           }
                        }
                        catch (Exception e) {
                           ActiveMQServerLogger.LOGGER.serverRestartWarning();
                           e.printStackTrace();
                        }
                     }
                  });
                  t.start();
               }
            }
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.serverRestartWarning(e);
         }
      }
   }
}
