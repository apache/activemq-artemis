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
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;

import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.utils.UUID;
import org.jboss.logging.Logger;

public class FileLockNodeManager extends NodeManager {

   private static final Logger logger = Logger.getLogger(FileLockNodeManager.class);

   private static final int LIVE_LOCK_POS = 1;

   private static final int BACKUP_LOCK_POS = 2;

   private static final int LOCK_LENGTH = 1;

   private static final byte LIVE = 'L';

   private static final byte FAILINGBACK = 'F';

   private static final byte PAUSED = 'P';

   private static final byte NOT_STARTED = 'N';

   private FileLock liveLock;

   private FileLock backupLock;

   protected long lockAcquisitionTimeout = -1;

   protected boolean interrupted = false;

   public FileLockNodeManager(final File directory, boolean replicatedBackup) {
      super(replicatedBackup, directory);
   }

   public FileLockNodeManager(final File directory, boolean replicatedBackup, long lockAcquisitionTimeout) {
      super(replicatedBackup, directory);

      this.lockAcquisitionTimeout = lockAcquisitionTimeout;
   }

   @Override
   public synchronized void start() throws Exception {
      if (isStarted()) {
         return;
      }
      if (!replicatedBackup) {
         setUpServerLockFile();
      }

      super.start();
   }

   @Override
   public boolean isAwaitingFailback() throws Exception {
      return getState() == FileLockNodeManager.FAILINGBACK;
   }

   @Override
   public boolean isBackupLive() throws Exception {
      FileLock liveAttemptLock;
      liveAttemptLock = tryLock(FileLockNodeManager.LIVE_LOCK_POS);
      if (liveAttemptLock == null) {
         return true;
      } else {
         liveAttemptLock.release();
         return false;
      }
   }

   public boolean isLiveLocked() {
      return liveLock != null;
   }

   @Override
   public void interrupt() {
      interrupted = true;
   }

   @Override
   public final void releaseBackup() throws Exception {
      if (backupLock != null) {
         backupLock.release();
         backupLock = null;
      }
   }

   @Override
   public void awaitLiveNode() throws Exception {
      do {
         byte state = getState();
         while (state == FileLockNodeManager.NOT_STARTED || state == FIRST_TIME_START) {
            logger.debug("awaiting live node startup state='" + state + "'");
            Thread.sleep(2000);
            state = getState();
         }

         liveLock = lock(FileLockNodeManager.LIVE_LOCK_POS);
         if (interrupted) {
            interrupted = false;
            throw new InterruptedException("Lock was interrupted");
         }
         state = getState();
         if (state == FileLockNodeManager.PAUSED) {
            liveLock.release();
            logger.debug("awaiting live node restarting");
            Thread.sleep(2000);
         } else if (state == FileLockNodeManager.FAILINGBACK) {
            liveLock.release();
            logger.debug("awaiting live node failing back");
            Thread.sleep(2000);
         } else if (state == FileLockNodeManager.LIVE) {
            logger.debug("acquired live node lock state = " + (char) state);
            break;
         }
      } while (true);
   }

   @Override
   public void startBackup() throws Exception {
      assert !replicatedBackup; // should not be called if this is a replicating backup
      ActiveMQServerLogger.LOGGER.waitingToBecomeBackup();

      backupLock = lock(FileLockNodeManager.BACKUP_LOCK_POS);
      ActiveMQServerLogger.LOGGER.gotBackupLock();
      if (getUUID() == null)
         readNodeId();
   }

   @Override
   public ActivateCallback startLiveNode() throws Exception {
      setFailingBack();

      String timeoutMessage = lockAcquisitionTimeout == -1 ? "indefinitely" : lockAcquisitionTimeout + " milliseconds";

      ActiveMQServerLogger.LOGGER.waitingToObtainLiveLock(timeoutMessage);

      liveLock = lock(FileLockNodeManager.LIVE_LOCK_POS);

      ActiveMQServerLogger.LOGGER.obtainedLiveLock();

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
               setLive();
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }
      };
   }

   @Override
   public void pauseLiveServer() throws Exception {
      setPaused();
      if (liveLock != null) {
         liveLock.release();
      }
   }

   @Override
   public void crashLiveServer() throws Exception {
      if (liveLock != null) {
         liveLock.release();
         liveLock = null;
      }
   }

   @Override
   public void awaitLiveStatus() throws Exception {
      while (getState() != LIVE) {
         Thread.sleep(2000);
      }
   }

   private void setLive() throws Exception {
      writeFileLockStatus(FileLockNodeManager.LIVE);
   }

   private void setFailingBack() throws Exception {
      writeFileLockStatus(FAILINGBACK);
   }

   private void setPaused() throws Exception {
      writeFileLockStatus(PAUSED);
   }

   /**
    * @param status
    * @throws IOException
    */
   private void writeFileLockStatus(byte status) throws IOException {
      if (replicatedBackup && channel == null)
         return;
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(status);
      bb.position(0);
      if (!channel.isOpen()) {
         setUpServerLockFile();
      }
      channel.write(bb, 0);
      channel.force(true);
   }

   private byte getState() throws Exception {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      int read;
      read = channel.read(bb, 0);
      if (read <= 0) {
         return FileLockNodeManager.NOT_STARTED;
      } else {
         return bb.get(0);
      }
   }

   @Override
   public final SimpleString readNodeId() throws ActiveMQIllegalStateException, IOException {
      ByteBuffer id = ByteBuffer.allocateDirect(16);
      int read = channel.read(id, 3);
      if (read != 16) {
         throw new ActiveMQIllegalStateException("live server did not write id to file");
      }
      byte[] bytes = new byte[16];
      id.position(0);
      id.get(bytes);
      setUUID(new UUID(UUID.TYPE_TIME_BASED, bytes));
      return getNodeId();
   }

   protected FileLock tryLock(final int lockPos) throws Exception {
      try {
         return channel.tryLock(lockPos, LOCK_LENGTH, false);
      } catch (java.nio.channels.OverlappingFileLockException ex) {
         // This just means that another object on the same JVM is holding the lock
         return null;
      }
   }

   protected FileLock lock(final int liveLockPos) throws Exception {
      long start = System.currentTimeMillis();

      while (!interrupted) {
         FileLock lock = null;
         try {
            lock = channel.tryLock(liveLockPos, 1, false);
         } catch (java.nio.channels.OverlappingFileLockException ex) {
            // This just means that another object on the same JVM is holding the lock
         }

         if (lock == null) {
            try {
               Thread.sleep(500);
            } catch (InterruptedException e) {
               return null;
            }

            if (lockAcquisitionTimeout != -1 && (System.currentTimeMillis() - start) > lockAcquisitionTimeout) {
               throw new Exception("timed out waiting for lock");
            }
         } else {
            return lock;
         }
      }

      // todo this is here because sometimes channel.lock throws a resource deadlock exception but trylock works,
      // need to investigate further and review
      FileLock lock;
      do {
         lock = channel.tryLock(liveLockPos, 1, false);
         if (lock == null) {
            try {
               Thread.sleep(500);
            } catch (InterruptedException e1) {
               //
            }
         }
         if (interrupted) {
            interrupted = false;
            throw new IOException("Lock was interrupted");
         }
      } while (lock == null);
      return lock;
   }

}
