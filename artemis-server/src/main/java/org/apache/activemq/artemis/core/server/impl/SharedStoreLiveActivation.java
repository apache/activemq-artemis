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

import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreMasterPolicy;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager.LockListener;
import org.jboss.logging.Logger;

public final class SharedStoreLiveActivation extends LiveActivation {

   private static final Logger logger = Logger.getLogger(SharedStoreLiveActivation.class);

   // this is how we act when we initially start as live
   private SharedStoreMasterPolicy sharedStoreMasterPolicy;

   private ActiveMQServerImpl activeMQServer;

   private volatile FileLockNodeManager.LockListener activeLockListener;

   private volatile ActivateCallback nodeManagerActivateCallback;

   public SharedStoreLiveActivation(ActiveMQServerImpl server, SharedStoreMasterPolicy sharedStoreMasterPolicy) {
      this.activeMQServer = server;
      this.sharedStoreMasterPolicy = sharedStoreMasterPolicy;
   }

   @Override
   public void run() {
      try {
         ActiveMQServerLogger.LOGGER.awaitingLiveLock();

         activeMQServer.checkJournalDirectory();

         if (logger.isDebugEnabled()) {
            logger.debug("First part initialization on " + this);
         }

         if (!activeMQServer.initialisePart1(false))
            return;

         if (activeMQServer.getNodeManager().isBackupLive()) {
            /*
             * looks like we've failed over at some point need to inform that we are the
             * backup so when the current live goes down they failover to us
             */
            if (logger.isDebugEnabled()) {
               logger.debug("announcing backup to the former live" + this);
            }
            activeMQServer.getBackupManager().start();

            if (!sharedStoreMasterPolicy.isWaitForActivation())
               activeMQServer.setState(ActiveMQServerImpl.SERVER_STATE.STARTED);

            activeMQServer.getBackupManager().announceBackup();
         }

         nodeManagerActivateCallback = activeMQServer.getNodeManager().startLiveNode();
         activeMQServer.registerActivateCallback(nodeManagerActivateCallback);
         addLockListener(activeMQServer, activeMQServer.getNodeManager());

         if (activeMQServer.getState() == ActiveMQServerImpl.SERVER_STATE.STOPPED
               || activeMQServer.getState() == ActiveMQServerImpl.SERVER_STATE.STOPPING) {
            return;
         }

         activeMQServer.initialisePart2(false);

         activeMQServer.completeActivation();

         ActiveMQServerLogger.LOGGER.serverIsLive();
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.initializationError(e);
         activeMQServer.callActivationFailureListeners(e);
      }
   }

   private void addLockListener(ActiveMQServerImpl activeMQServer, NodeManager nodeManager) {
      if (nodeManager instanceof FileLockNodeManager) {
         FileLockNodeManager fileNodeManager = (FileLockNodeManager) nodeManager;

         activeLockListener = fileNodeManager.new LockListener() {

            @Override
            public void lostLock() {
               stopStartServerInSeperateThread(activeMQServer);
            }

         };
         fileNodeManager.registerLockListener(activeLockListener);
      } // else no business registering a listener
   }

   /**
    * We need to do this in a new thread because this takes to long to finish in
    * the scheduled thread Also this is not the responsibility of the scheduled
    * thread
    * @param activeMQServer
    */
   private void stopStartServerInSeperateThread(ActiveMQServerImpl activeMQServer) {
      try {

         Runnable startServerRunnable = new Runnable() {

            @Override
            public void run() {
               try {
                  activeMQServer.stop(true, false);
               } catch (Exception e) {
                  logger.warn("Failed to stop artemis server after loosing the lock", e);
               }

               try {
                  activeMQServer.start();
               } catch (Exception e) {
                  logger.error("Failed to start artemis server after recovering from loosing the lock", e);
               }
            }

         };
         Thread startServer = new Thread(startServerRunnable);
         startServer.start();
      } catch (Exception e) {
         logger.error(e.getMessage());
      }
   }

   @Override
   public void close(boolean permanently, boolean restarting) throws Exception {
      // TO avoid a NPE from stop
      NodeManager nodeManagerInUse = activeMQServer.getNodeManager();

      if (nodeManagerInUse != null) {
         LockListener closeLockListener = activeLockListener;
         if (closeLockListener != null) {
            closeLockListener.unregisterListener();
         }
         ActivateCallback activateCallback = nodeManagerActivateCallback;
         if (activateCallback != null) {
            activeMQServer.unregisterActivateCallback(activateCallback);
         }
         if (sharedStoreMasterPolicy.isFailoverOnServerShutdown() || permanently) {
            nodeManagerInUse.crashLiveServer();
         } else {
            nodeManagerInUse.pauseLiveServer();
         }

      }
   }
}
