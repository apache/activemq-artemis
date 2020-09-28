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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActiveMQLockAcquisitionTimeoutException;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.utils.UUID;
import org.jboss.logging.Logger;

public class FileLockNodeManager extends FileBasedNodeManager {

   private static final Logger logger = Logger.getLogger(FileLockNodeManager.class);

   private static final int STATE_LOCK_POS = 0;

   private static final int LIVE_LOCK_POS = 1;

   private static final int BACKUP_LOCK_POS = 2;

   private static final long LOCK_LENGTH = 1;

   private static final byte LIVE = 'L';

   private static final byte FAILINGBACK = 'F';

   private static final byte PAUSED = 'P';

   private static final byte NOT_STARTED = 'N';

   private static final long LOCK_ACCESS_FAILURE_WAIT_TIME_NANOS = TimeUnit.SECONDS.toNanos(2);

   private static final long LOCK_MONITOR_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(2);

   private volatile FileLock liveLock;

   private FileLock backupLock;

   private final FileChannel[] lockChannels = new FileChannel[3];

   private final long lockAcquisitionTimeoutNanos;

   protected boolean interrupted = false;

   private final ScheduledExecutorService scheduledPool;

   public FileLockNodeManager(final File directory, boolean replicatedBackup, ScheduledExecutorService scheduledPool) {
      super(replicatedBackup, directory);
      this.scheduledPool = scheduledPool;
      this.lockAcquisitionTimeoutNanos = -1;
   }

   public FileLockNodeManager(final File directory, boolean replicatedBackup) {
      super(replicatedBackup, directory);
      this.scheduledPool = null;
      this.lockAcquisitionTimeoutNanos = -1;
   }

   public FileLockNodeManager(final File directory,
                              boolean replicatedBackup,
                              long lockAcquisitionTimeout,
                              ScheduledExecutorService scheduledPool) {
      super(replicatedBackup, directory);
      this.scheduledPool = scheduledPool;
      this.lockAcquisitionTimeoutNanos = lockAcquisitionTimeout == -1 ? -1 : TimeUnit.MILLISECONDS.toNanos(lockAcquisitionTimeout);
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
   protected synchronized void setUpServerLockFile() throws IOException {
      super.setUpServerLockFile();

      lockChannels[0] = channel;

      for (int i = 1; i < 3; i++) {
         if (lockChannels[i] != null && lockChannels[i].isOpen()) {
            continue;
         }
         File fileLock = newFile("serverlock." + i);
         if (!fileLock.exists()) {
            fileLock.createNewFile();
         }
         RandomAccessFile randomFileLock = new RandomAccessFile(fileLock, "rw");
         lockChannels[i] = randomFileLock.getChannel();
      }
   }

   @Override
   public synchronized void stop() throws Exception {
      for (FileChannel channel : lockChannels) {
         if (channel != null && channel.isOpen()) {
            try {
               channel.close();
            } catch (Throwable e) {
               // I do not want to interrupt a shutdown. If anything is wrong here, just log it
               // it could be a critical error or something like that throwing the system down
               logger.warn(e.getMessage(), e);
            }
         }
      }

      super.stop();
   }

   @Override
   public boolean isAwaitingFailback() throws NodeManagerException {
      return getState() == FileLockNodeManager.FAILINGBACK;
   }

   @Override
   public boolean isBackupLive() throws NodeManagerException {
      try {
         FileLock liveAttemptLock;
         liveAttemptLock = tryLock(FileLockNodeManager.LIVE_LOCK_POS);
         if (liveAttemptLock == null) {
            return true;
         } else {
            liveAttemptLock.release();
            return false;
         }
      } catch (IOException e) {
         throw new NodeManagerException(e);
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
   public final void releaseBackup() throws NodeManagerException {
      try {
         if (backupLock != null) {
            backupLock.release();
            backupLock = null;
         }
      } catch (IOException e) {
         throw new NodeManagerException(e);
      }
   }

   @Override
   public void awaitLiveNode() throws NodeManagerException, InterruptedException {
      try {
         logger.debug("awaiting live node...");
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
         }
         while (true);
      } catch (IOException | ActiveMQLockAcquisitionTimeoutException e) {
         throw new NodeManagerException(e);
      }
   }

   @Override
   public void startBackup() throws NodeManagerException {
      assert !replicatedBackup; // should not be called if this is a replicating backup
      ActiveMQServerLogger.LOGGER.waitingToBecomeBackup();
      try {
         backupLock = lock(FileLockNodeManager.BACKUP_LOCK_POS);
      } catch (ActiveMQLockAcquisitionTimeoutException e) {
         throw new NodeManagerException(e);
      }
      ActiveMQServerLogger.LOGGER.gotBackupLock();
      if (getUUID() == null)
         readNodeId();
   }

   @Override
   public ActivateCallback startLiveNode() throws NodeManagerException {
      try {
         setFailingBack();

         String timeoutMessage = lockAcquisitionTimeoutNanos == -1 ? "indefinitely" : TimeUnit.NANOSECONDS.toMillis(lockAcquisitionTimeoutNanos) + " milliseconds";

         ActiveMQServerLogger.LOGGER.waitingToObtainLiveLock(timeoutMessage);

         liveLock = lock(FileLockNodeManager.LIVE_LOCK_POS);

         ActiveMQServerLogger.LOGGER.obtainedLiveLock();

         return new CleaningActivateCallback() {
            @Override
            public void activationComplete() {
               try {
                  setLive();
                  startLockMonitoring();
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
                  // that allows to restart/stop the broker if needed
                  throw e;
               }
            }
         };
      } catch (ActiveMQLockAcquisitionTimeoutException e) {
         throw new NodeManagerException(e);
      }
   }

   @Override
   public void pauseLiveServer() throws NodeManagerException {
      stopLockMonitoring();
      setPaused();
      try {
         if (liveLock != null) {
            liveLock.release();
         }
      } catch (IOException e) {
         throw new NodeManagerException(e);
      }
   }

   @Override
   public void crashLiveServer() throws NodeManagerException {
      stopLockMonitoring();
      if (liveLock != null) {
         try {
            liveLock.release();
         } catch (IOException e) {
            throw new NodeManagerException(e);
         } finally {
            liveLock = null;
         }
      }
   }

   @Override
   public void awaitLiveStatus() throws NodeManagerException, InterruptedException {
      while (getState() != LIVE) {
         Thread.sleep(2000);
      }
   }

   private void setLive() throws NodeManagerException {
      writeFileLockStatus(FileLockNodeManager.LIVE);
   }

   private void setFailingBack() throws NodeManagerException {
      writeFileLockStatus(FAILINGBACK);
   }

   private void setPaused() throws NodeManagerException {
      writeFileLockStatus(PAUSED);
   }

   /**
    * @param status
    * @throws ActiveMQLockAcquisitionTimeoutException,IOException
    */
   private void writeFileLockStatus(byte status) throws NodeManagerException {
      if (replicatedBackup && channel == null)
         return;
      logger.debug("writing status: " + status);
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(status);
      bb.position(0);
      try {
         if (!channel.isOpen()) {
            setUpServerLockFile();
         }
         FileLock lock = null;
         try {
            lock = lock(STATE_LOCK_POS);
            channel.write(bb, 0);
            channel.force(true);
         } finally {
            if (lock != null) {
               lock.release();
            }
         }
      } catch (IOException | ActiveMQLockAcquisitionTimeoutException e) {
         throw new NodeManagerException(e);
      }
   }

   private byte getState() throws NodeManagerException {
      try {
         byte result;
         logger.debug("getting state...");
         ByteBuffer bb = ByteBuffer.allocateDirect(1);
         int read;
         FileLock lock = null;
         try {
            lock = lock(STATE_LOCK_POS);
            read = channel.read(bb, 0);
            if (read <= 0) {
               result = FileLockNodeManager.NOT_STARTED;
            } else {
               result = bb.get(0);
            }
         } finally {
            if (lock != null) {
               lock.release();
            }
         }
         logger.debug("state: " + result);
         return result;
      } catch (IOException | ActiveMQLockAcquisitionTimeoutException e) {
         throw new NodeManagerException(e);
      }
   }

   @Override
   public final SimpleString readNodeId() throws NodeManagerException {
      try {
         ByteBuffer id = ByteBuffer.allocateDirect(16);
         int read = channel.read(id, 3);
         if (read != 16) {
            throw new IOException("live server did not write id to file");
         }
         byte[] bytes = new byte[16];
         id.position(0);
         id.get(bytes);
         setUUID(new UUID(UUID.TYPE_TIME_BASED, bytes));
         return getNodeId();
      } catch (IOException e) {
         throw new NodeManagerException(e);
      }
   }

   protected FileLock tryLock(final int lockPos) throws IOException {
      try {
         logger.debug("trying to lock position: " + lockPos);
         FileLock lock = lockChannels[lockPos].tryLock();
         if (lock != null) {
            logger.debug("locked position: " + lockPos);
         } else {
            logger.debug("failed to lock position: " + lockPos);
         }
         return lock;
      } catch (java.nio.channels.OverlappingFileLockException ex) {
         // This just means that another object on the same JVM is holding the lock
         return null;
      }
   }

   protected FileLock lock(final int lockPosition) throws ActiveMQLockAcquisitionTimeoutException {
      long start = System.nanoTime();
      boolean isRecurringFailure = false;

      while (!interrupted) {
         try {
            FileLock lock = tryLock(lockPosition);
            isRecurringFailure = false;

            if (lock == null) {
               try {
                  Thread.sleep(500);
               } catch (InterruptedException e) {
                  return null;
               }

               if (this.lockAcquisitionTimeoutNanos != -1 && (System.nanoTime() - start) > this.lockAcquisitionTimeoutNanos) {
                  throw new ActiveMQLockAcquisitionTimeoutException("timed out waiting for lock");
               }
            } else {
               return lock;
            }
         } catch (IOException e) {
            // IOException during trylock() may be a temporary issue, e.g. NFS volume not being accessible

            logger.log(isRecurringFailure ? Logger.Level.DEBUG : Logger.Level.WARN,
                    "Failure when accessing a lock file", e);
            isRecurringFailure = true;

            long waitTime = LOCK_ACCESS_FAILURE_WAIT_TIME_NANOS;
            if (this.lockAcquisitionTimeoutNanos != -1) {
               final long remainingTime = this.lockAcquisitionTimeoutNanos - (System.nanoTime() - start);
               if (remainingTime <= 0) {
                  throw new ActiveMQLockAcquisitionTimeoutException("timed out waiting for lock");
               }
               waitTime = Math.min(waitTime, remainingTime);
            }

            try {
               TimeUnit.NANOSECONDS.sleep(waitTime);
            } catch (InterruptedException interrupt) {
               return null;
            }
         }
      }

      // presumed interrupted
      return null;
   }

   private synchronized void startLockMonitoring() {
      logger.debug("Starting the lock monitor");
      if (monitorLock == null) {
         monitorLock = new MonitorLock(scheduledPool, LOCK_MONITOR_TIMEOUT_NANOS, LOCK_MONITOR_TIMEOUT_NANOS, TimeUnit.NANOSECONDS, false);
         monitorLock.start();
      } else {
         logger.debug("Lock monitor was already started");
      }
   }

   private synchronized void stopLockMonitoring() {
      logger.debug("Stopping the lock monitor");
      if (monitorLock != null) {
         monitorLock.stop();
         monitorLock = null;
      } else {
         logger.debug("The lock monitor was already stopped");
      }
   }

   @Override
   protected synchronized void notifyLostLock() {
      if (liveLock != null) {
         super.notifyLostLock();
      }
   }

   // This has been introduced to help ByteMan test testLockMonitorInvalid on JDK 11: sun.nio.ch.FileLockImpl::isValid
   // can affecting setLive, causing an infinite loop due to java.nio.channels.OverlappingFileLockException on tryLock
   private boolean isLiveLockLost() {
      final FileLock lock = this.liveLock;
      return (lock != null && !lock.isValid()) || lock == null;
   }

   private MonitorLock monitorLock;

   public class MonitorLock extends ActiveMQScheduledComponent {
      public MonitorLock(ScheduledExecutorService scheduledExecutorService,
                            long initialDelay,
                            long checkPeriod,
                            TimeUnit timeUnit,
                            boolean onDemand) {
         super(scheduledExecutorService, initialDelay, checkPeriod, timeUnit, onDemand);
      }


      @Override
      public void run() {

         boolean lostLock = true;
         try {
            if (liveLock == null) {
               logger.debug("Livelock is null");
            }
            lostLock = isLiveLockLost();
            if (!lostLock) {
               logger.debug("Server still has the lock, double check status is live");
               // Java always thinks the lock is still valid even when there is no filesystem
               // so we do another check

               // Should be able to retrieve the status unless something is wrong
               // When EFS is gone, this locks. Which can be solved but is a lot of threading
               // work where we need to
               // manage the timeout ourselves and interrupt the thread used to claim the lock.
               byte state = getState();
               if (state == LIVE) {
                  logger.debug("Status is set to live");
               } else {
                  logger.debug("Status is not live");
               }
            }
         } catch (Exception exception) {
            // If something went wrong we probably lost the lock
            logger.error(exception.getMessage(), exception);
            lostLock = true;
         }

         if (lostLock) {
            logger.warn("Lost the lock according to the monitor, notifying listeners");
            notifyLostLock();
         }

      }

   }

}
