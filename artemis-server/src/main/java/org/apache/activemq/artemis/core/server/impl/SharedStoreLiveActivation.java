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

import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreMasterPolicy;
import org.jboss.logging.Logger;

public final class SharedStoreLiveActivation extends LiveActivation {

   private static final Logger logger = Logger.getLogger(SharedStoreLiveActivation.class);

   //this is how we act when we initially start as live
   private SharedStoreMasterPolicy sharedStoreMasterPolicy;

   private ActiveMQServerImpl activeMQServer;

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
             * looks like we've failed over at some point need to inform that we are the backup
             * so when the current live goes down they failover to us
             */
            if (logger.isDebugEnabled()) {
               logger.debug("announcing backup to the former live" + this);
            }
            activeMQServer.getBackupManager().start();

            if (!sharedStoreMasterPolicy.isWaitForActivation())
               activeMQServer.setState(ActiveMQServerImpl.SERVER_STATE.STARTED);

            activeMQServer.getBackupManager().announceBackup();
         }

         activeMQServer.registerActivateCallback(activeMQServer.getNodeManager().startLiveNode());

         if (activeMQServer.getState() == ActiveMQServerImpl.SERVER_STATE.STOPPED || activeMQServer.getState() == ActiveMQServerImpl.SERVER_STATE.STOPPING) {
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

   @Override
   public void close(boolean permanently, boolean restarting) throws Exception {
      // TO avoid a NPE from stop
      NodeManager nodeManagerInUse = activeMQServer.getNodeManager();

      if (nodeManagerInUse != null) {
         if (sharedStoreMasterPolicy.isFailoverOnServerShutdown() || permanently) {
            nodeManagerInUse.crashLiveServer();
         } else {
            nodeManagerInUse.pauseLiveServer();
         }
      }
   }
}
