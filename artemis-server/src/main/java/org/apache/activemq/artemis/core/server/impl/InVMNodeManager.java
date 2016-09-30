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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.utils.UUIDGenerator;

import static org.apache.activemq.artemis.core.server.impl.InVMNodeManager.State.FAILING_BACK;
import static org.apache.activemq.artemis.core.server.impl.InVMNodeManager.State.LIVE;
import static org.apache.activemq.artemis.core.server.impl.InVMNodeManager.State.NOT_STARTED;
import static org.apache.activemq.artemis.core.server.impl.InVMNodeManager.State.PAUSED;

/**
 * NodeManager used to run multiple servers in the same VM.
 * <p>
 * We use the {@link org.apache.activemq.artemis.core.server.impl.InVMNodeManager} instead of {@link org.apache.activemq.artemis.core.server.impl.FileLockNodeManager} when
 * multiple servers are run inside the same VM and File Locks can not be shared in the
 * same VM (it would cause a shared lock violation).
 */
public final class InVMNodeManager extends NodeManager {

   private final Semaphore liveLock;

   private final Semaphore backupLock;

   public enum State {
      LIVE, PAUSED, FAILING_BACK, NOT_STARTED
   }

   public State state = NOT_STARTED;

   public long failoverPause = 0L;

   public InVMNodeManager(boolean replicatedBackup) {
      this(replicatedBackup, null);
      if (replicatedBackup)
         throw new RuntimeException("if replicated-backup, we need its journal directory");
   }

   public InVMNodeManager(boolean replicatedBackup, File directory) {
      super(replicatedBackup, directory);
      liveLock = new Semaphore(1);
      backupLock = new Semaphore(1);
      setUUID(UUIDGenerator.getInstance().generateUUID());
   }

   @Override
   public void awaitLiveNode() throws Exception {
      do {
         while (state == NOT_STARTED) {
            Thread.sleep(2000);
         }

         liveLock.acquire();

         if (state == PAUSED) {
            liveLock.release();
            Thread.sleep(2000);
         } else if (state == FAILING_BACK) {
            liveLock.release();
            Thread.sleep(2000);
         } else if (state == LIVE) {
            break;
         }
      } while (true);
      if (failoverPause > 0L) {
         Thread.sleep(failoverPause);
      }
   }

   @Override
   public void awaitLiveStatus() throws Exception {
      while (state != LIVE) {
         Thread.sleep(10);
      }
   }

   @Override
   public void startBackup() throws Exception {
      backupLock.acquire();
   }

   @Override
   public ActivateCallback startLiveNode() throws Exception {
      state = FAILING_BACK;
      liveLock.acquire();
      return new ActivateCallback() {
         @Override
         public void preActivate() {
         }

         @Override
         public void activated() {
         }

         @Override
         public void deActivate() {
         }

         @Override
         public void activationComplete() {
            try {
               state = LIVE;
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }
      };
   }

   @Override
   public void pauseLiveServer() throws Exception {
      state = PAUSED;
      liveLock.release();
   }

   @Override
   public void crashLiveServer() throws Exception {
      liveLock.release();
   }

   @Override
   public boolean isAwaitingFailback() throws Exception {
      return state == FAILING_BACK;
   }

   @Override
   public boolean isBackupLive() throws Exception {
      return liveLock.availablePermits() == 0;
   }

   @Override
   public void interrupt() {
      //
   }

   @Override
   public void releaseBackup() {
      if (backupLock != null) {
         backupLock.release();
      }
   }

   @Override
   public SimpleString readNodeId() throws ActiveMQIllegalStateException, IOException {
      return getNodeId();
   }
}
