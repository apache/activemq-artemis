/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.impl.jdbc;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActiveMQLockAcquisitionTimeoutException;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.CleaningActivateCallback;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.jdbc.store.sql.PropertySQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.logging.Logger;

/**
 * JDBC implementation of {@link NodeManager}.
 */
public final class JdbcNodeManager extends NodeManager {

   private static final Logger LOGGER = Logger.getLogger(JdbcNodeManager.class);
   private static final long MAX_PAUSE_MILLIS = 2000L;

   private final Supplier<? extends SharedStateManager> sharedStateManagerFactory;
   private final Supplier<? extends ScheduledLeaseLock> scheduledLiveLockFactory;
   private final Supplier<? extends ScheduledLeaseLock> scheduledBackupLockFactory;
   private SharedStateManager sharedStateManager;
   private ScheduledLeaseLock scheduledLiveLock;
   private ScheduledLeaseLock scheduledBackupLock;
   private final long lockAcquisitionTimeoutMillis;
   private volatile boolean interrupted = false;
   private final LeaseLock.Pauser pauser;
   private final Duration lingerTimeBeforeRestart;

   public static JdbcNodeManager with(DatabaseStorageConfiguration configuration,
                                      ScheduledExecutorService scheduledExecutorService,
                                      ExecutorFactory executorFactory) {
      validateTimeoutConfiguration(configuration);
      final SQLProvider.Factory sqlProviderFactory;
      if (configuration.getSqlProviderFactory() != null) {
         sqlProviderFactory = configuration.getSqlProviderFactory();
      } else {
         sqlProviderFactory = new PropertySQLProvider.Factory(configuration.getConnectionProvider());
      }
      final String brokerId = java.util.UUID.randomUUID().toString();
      return usingConnectionProvider(brokerId,
                             configuration.getJdbcLockExpirationMillis(),
                             configuration.getJdbcLockRenewPeriodMillis(),
                             configuration.getJdbcLockAcquisitionTimeoutMillis(),
                             configuration.getConnectionProvider(),
                             sqlProviderFactory.create(configuration.getNodeManagerStoreTableName(), SQLProvider.DatabaseStoreType.NODE_MANAGER),
                             scheduledExecutorService,
                             executorFactory);
   }

   private static JdbcNodeManager usingConnectionProvider(String brokerId,
                                                  long lockExpirationMillis,
                                                  long lockRenewPeriodMillis,
                                                  long lockAcquisitionTimeoutMillis,
                                                  JDBCConnectionProvider connectionProvider,
                                                  SQLProvider provider,
                                                  ScheduledExecutorService scheduledExecutorService,
                                                  ExecutorFactory executorFactory) {
      return new JdbcNodeManager(
         () -> JdbcSharedStateManager.usingConnectionProvider(brokerId,
                                                      lockExpirationMillis,
                                                      connectionProvider,
                                                      provider),
         lockExpirationMillis,
         lockRenewPeriodMillis,
         lockAcquisitionTimeoutMillis,
         scheduledExecutorService,
         executorFactory);
   }

   private static void validateTimeoutConfiguration(DatabaseStorageConfiguration configuration) {
      final long lockExpiration = configuration.getJdbcLockExpirationMillis();
      if (lockExpiration <= 0) {
         throw new IllegalArgumentException("jdbc-lock-expiration should be positive");
      }
      final long lockRenewPeriod = configuration.getJdbcLockRenewPeriodMillis();
      if (lockRenewPeriod <= 0) {
         throw new IllegalArgumentException("jdbc-lock-renew-period should be positive");
      }
      if (lockRenewPeriod >= lockExpiration) {
         throw new IllegalArgumentException("jdbc-lock-renew-period should be < jdbc-lock-expiration");
      }
      final int networkTimeout = configuration.getJdbcNetworkTimeout();
      if (networkTimeout >= 0) {
         if (networkTimeout > lockExpiration) {
            LOGGER.warn("jdbc-network-timeout isn't properly configured: the recommended value is <= jdbc-lock-expiration");
         }
      } else {
         LOGGER.warn("jdbc-network-timeout isn't properly configured: the recommended value is <= jdbc-lock-expiration");
      }
   }

   private JdbcNodeManager(Supplier<? extends SharedStateManager> sharedStateManagerFactory,
                           long lockExpirationMillis,
                           long lockRenewPeriodMillis,
                           long lockAcquisitionTimeoutMillis,
                           ScheduledExecutorService scheduledExecutorService,
                           ExecutorFactory executorFactory) {
      super(false);
      this.lingerTimeBeforeRestart = Duration.ofMillis(lockExpirationMillis);
      this.lockAcquisitionTimeoutMillis = lockAcquisitionTimeoutMillis;
      this.pauser = LeaseLock.Pauser.sleep(Math.min(lockRenewPeriodMillis, MAX_PAUSE_MILLIS), TimeUnit.MILLISECONDS);
      this.sharedStateManagerFactory = sharedStateManagerFactory;
      this.scheduledLiveLockFactory = () -> ScheduledLeaseLock.of(
         scheduledExecutorService,
         executorFactory != null ? executorFactory.getExecutor() : null,
         "live",
         this.sharedStateManager.liveLock(),
         lockRenewPeriodMillis,
         this::notifyLostLock);
      this.scheduledBackupLockFactory = () -> ScheduledLeaseLock.of(
         scheduledExecutorService,
         executorFactory != null ?
            executorFactory.getExecutor() : null,
         "backup",
         this.sharedStateManager.backupLock(),
         lockRenewPeriodMillis,
         this::notifyLostLock);
      this.sharedStateManager = null;
      this.scheduledLiveLock = null;
      this.scheduledBackupLock = null;
   }

   @Override
   public Duration lingerTimeBeforeRestart() {
      return lingerTimeBeforeRestart;
   }

   // using a return value is a trick to use it as a one liner together with other checks
   private synchronized boolean checkIsStarted() {
      if (!isStarted()) {
         throw new IllegalStateException("the node manager isn't started yet");
      }
      return true;
   }

   @Override
   protected synchronized void notifyLostLock() {
      try {
         super.notifyLostLock();
      } finally {
         // if any of the notified listener has stopped the node manager or
         // the node manager was already stopped
         if (!isStarted()) {
            return;
         }
         try {
            stop();
         } catch (Exception ex) {
            LOGGER.warn("Stopping node manager has errored on lost lock notification", ex);
         }
      }
   }

   @Override
   public synchronized void start() throws Exception {
      try {
         if (isStarted()) {
            return;
         }
         this.sharedStateManager = sharedStateManagerFactory.get();
         LOGGER.debug("setup sharedStateManager on start");
         final UUID nodeId = sharedStateManager.setup(UUIDGenerator.getInstance()::generateUUID);
         setUUID(nodeId);
         this.scheduledLiveLock = scheduledLiveLockFactory.get();
         this.scheduledBackupLock = scheduledBackupLockFactory.get();
         super.start();
      } catch (IllegalStateException e) {
         this.sharedStateManager = null;
         this.scheduledLiveLock = null;
         this.scheduledBackupLock = null;
         throw e;
      }
   }

   @Override
   public synchronized void stop() throws Exception {
      if (isStarted()) {
         try {
            this.scheduledLiveLock.stop();
            this.scheduledBackupLock.stop();
         } finally {
            super.stop();
            this.sharedStateManager.close();
            this.sharedStateManager = null;
            this.scheduledLiveLock = null;
            this.scheduledBackupLock = null;
         }
      }
   }

   @Override
   protected void finalize() throws Throwable {
      stop();
   }

   @Override
   public boolean isAwaitingFailback() throws NodeManagerException {
      checkIsStarted();
      LOGGER.debug("ENTER isAwaitingFailback");
      try {
         return readSharedState() == SharedStateManager.State.FAILING_BACK;
      } catch (IllegalStateException e) {
         throw new NodeManagerException(e);
      } finally {
         LOGGER.debug("EXIT isAwaitingFailback");
      }
   }

   @Override
   public boolean isBackupLive() throws NodeManagerException {
      checkIsStarted();
      LOGGER.debug("ENTER isBackupLive");
      try {
         //is anyone holding the live lock?
         return this.scheduledLiveLock.lock().isHeld();
      } catch (IllegalStateException e) {
         throw new NodeManagerException(e);
      } finally {
         LOGGER.debug("EXIT isBackupLive");
      }
   }

   @Override
   public void interrupt() {
      LOGGER.debug("ENTER interrupted");
      //need to be volatile: must be called concurrently to work as expected
      interrupted = true;
      LOGGER.debug("EXIT interrupted");
   }

   @Override
   public void releaseBackup() throws NodeManagerException {
      checkIsStarted();
      LOGGER.debug("ENTER releaseBackup");
      try {
         if (this.scheduledBackupLock.isStarted()) {
            LOGGER.debug("scheduledBackupLock is running: stop it and release backup lock");
            this.scheduledBackupLock.stop();
            this.scheduledBackupLock.lock().release();
         } else {
            LOGGER.debug("scheduledBackupLock is not running");
         }
      } catch (IllegalStateException e) {
         throw new NodeManagerException(e);
      } finally {
         LOGGER.debug("EXIT releaseBackup");
      }
   }

   /**
    * Try to acquire a lock, failing with an exception otherwise.
    */
   private void lock(LeaseLock lock) throws ActiveMQLockAcquisitionTimeoutException, InterruptedException {
      final LeaseLock.AcquireResult acquireResult = lock.tryAcquire(this.lockAcquisitionTimeoutMillis, this.pauser, () -> !this.interrupted);
      switch (acquireResult) {
         case Timeout:
            throw new ActiveMQLockAcquisitionTimeoutException("timed out waiting for lock");
         case Exit:
            this.interrupted = false;
            throw new InterruptedException("LeaseLock was interrupted");
         case Done:
            break;
         default:
            throw new AssertionError(acquireResult + " not managed");
      }

   }

   private void checkInterrupted(Supplier<String> message) throws InterruptedException {
      if (this.interrupted) {
         interrupted = false;
         throw new InterruptedException(message.get());
      }
   }

   private void renewLiveLockIfNeeded(final long acquiredOn) {
      final long acquiredMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - acquiredOn);
      if (acquiredMillis > this.scheduledLiveLock.renewPeriodMillis()) {
         renewLock(this.scheduledLiveLock);
      }
   }

   private void renewLock(ScheduledLeaseLock lock) {
      boolean lostLock = true;
      IllegalStateException renewEx = null;
      try {
         lostLock = !this.scheduledLiveLock.lock().renew();
      } catch (IllegalStateException e) {
         renewEx = e;
      }
      if (lostLock) {
         notifyLostLock();
         if (renewEx == null) {
            renewEx = new IllegalStateException(lock.lockName() + " lock isn't renewed");
         }
         throw renewEx;
      }
   }

   /**
    * Lock live node and check for a live state, taking care to renew it (if needed) or releasing it otherwise
    */
   private boolean lockLiveAndCheckLiveState() throws ActiveMQLockAcquisitionTimeoutException, InterruptedException {
      try {
         lock(this.scheduledLiveLock.lock());
         final long acquiredOn = System.nanoTime();
         boolean liveWhileLocked = false;
         //check if the state is live
         final SharedStateManager.State stateWhileLocked;
         try {
            stateWhileLocked = readSharedState();
         } catch (IllegalStateException e) {
            LOGGER.error("error while holding the live node lock and tried to read the shared state", e);
            this.scheduledLiveLock.lock().release();
            throw e;
         }
         if (stateWhileLocked == SharedStateManager.State.LIVE) {
            renewLiveLockIfNeeded(acquiredOn);
            liveWhileLocked = true;
         } else {
            LOGGER.debugf("state is %s while holding the live lock: releasing live lock", stateWhileLocked);
            //state is not live: can (try to) release the lock
            this.scheduledLiveLock.lock().release();
         }
         return liveWhileLocked;
      } catch (InterruptedException e) {
         throw e;
      }
   }

   @Override
   public void awaitLiveNode() throws NodeManagerException, InterruptedException {
      checkIsStarted();
      LOGGER.debug("ENTER awaitLiveNode");
      try {
         boolean liveWhileLocked = false;
         while (!liveWhileLocked) {
            //check first without holding any lock
            final SharedStateManager.State state = readSharedState();
            if (state == SharedStateManager.State.LIVE) {
               //verify if the state is live while holding the live node lock too
               liveWhileLocked = lockLiveAndCheckLiveState();
            } else {
               LOGGER.debugf("state while awaiting live node: %s", state);
            }
            if (!liveWhileLocked) {
               checkInterrupted(() -> "awaitLiveNode got interrupted!");
               pauser.idle();
            }
         }
         //state is LIVE and live lock is acquired and valid
         LOGGER.debugf("acquired live node lock while state is %s: starting scheduledLiveLock", SharedStateManager.State.LIVE);
         this.scheduledLiveLock.start();
      } catch (InterruptedException e) {
         throw e;
      } catch (ActiveMQLockAcquisitionTimeoutException | IllegalStateException e) {
         throw new NodeManagerException(e);
      } finally {
         LOGGER.debug("EXIT awaitLiveNode");
      }
   }

   @Override
   public void startBackup() throws NodeManagerException, InterruptedException {
      checkIsStarted();
      LOGGER.debug("ENTER startBackup");
      try {
         ActiveMQServerLogger.LOGGER.waitingToBecomeBackup();

         lock(scheduledBackupLock.lock());
         scheduledBackupLock.start();
         ActiveMQServerLogger.LOGGER.gotBackupLock();
         if (getUUID() == null)
            readNodeId();
      } catch (InterruptedException ie) {
         throw ie;
      } catch (ActiveMQLockAcquisitionTimeoutException | IllegalStateException e) {
         throw new NodeManagerException(e);
      } finally {
         LOGGER.debug("EXIT startBackup");
      }
   }

   @Override
   public ActivateCallback startLiveNode() throws NodeManagerException, InterruptedException {
      checkIsStarted();
      LOGGER.debug("ENTER startLiveNode");
      try {
         setFailingBack();

         final String timeoutMessage = lockAcquisitionTimeoutMillis == -1 ? "indefinitely" : lockAcquisitionTimeoutMillis + " milliseconds";

         ActiveMQServerLogger.LOGGER.waitingToObtainLiveLock(timeoutMessage);

         lock(this.scheduledLiveLock.lock());

         this.scheduledLiveLock.start();

         ActiveMQServerLogger.LOGGER.obtainedLiveLock();

         return new CleaningActivateCallback() {
            @Override
            public void activationComplete() {
               LOGGER.debug("ENTER activationComplete");
               try {
                  //state can be written only if the live renew task is running
                  setLive();
               } catch (IllegalStateException e) {
                  ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
                  throw new NodeManagerException(e);
               } finally {
                  LOGGER.debug("EXIT activationComplete");
               }
            }
         };
      } catch (InterruptedException ie) {
         throw ie;
      } catch (ActiveMQLockAcquisitionTimeoutException | IllegalStateException e) {
         throw new NodeManagerException(e);
      } finally {
         LOGGER.debug("EXIT startLiveNode");
      }
   }

   @Override
   public void pauseLiveServer() throws NodeManagerException {
      checkIsStarted();
      LOGGER.debug("ENTER pauseLiveServer");
      try {
         if (scheduledLiveLock.isStarted()) {
            LOGGER.debug("scheduledLiveLock is running: set paused shared state, stop it and release live lock");
            setPaused();
            scheduledLiveLock.stop();
            scheduledLiveLock.lock().release();
         } else {
            LOGGER.debug("scheduledLiveLock is not running: try renew live lock");
            renewLock(scheduledLiveLock);
            LOGGER.debug("live lock renewed: set paused shared state and release live lock");
            setPaused();
            scheduledLiveLock.lock().release();
         }
      } catch (IllegalStateException e) {
         throw new NodeManagerException(e);
      } finally {
         LOGGER.debug("EXIT pauseLiveServer");
      }
   }

   @Override
   public void crashLiveServer() throws NodeManagerException {
      checkIsStarted();
      LOGGER.debug("ENTER crashLiveServer");
      try {
         if (this.scheduledLiveLock.isStarted()) {
            LOGGER.debug("scheduledLiveLock is running: request stop it and release live lock");
            this.scheduledLiveLock.stop();
            this.scheduledLiveLock.lock().release();
         } else {
            LOGGER.debug("scheduledLiveLock is not running");
         }
      } finally {
         LOGGER.debug("EXIT crashLiveServer");
      }
   }

   @Override
   public void awaitLiveStatus() {
      checkIsStarted();
      LOGGER.debug("ENTER awaitLiveStatus");
      try {
         while (readSharedState() != SharedStateManager.State.LIVE) {
            pauser.idle();
         }
      } finally {
         LOGGER.debug("EXIT awaitLiveStatus");
      }
   }

   private void setLive() {
      writeSharedState(SharedStateManager.State.LIVE);
   }

   private void setFailingBack() {
      writeSharedState(SharedStateManager.State.FAILING_BACK);
   }

   private void setPaused() {
      writeSharedState(SharedStateManager.State.PAUSED);
   }

   private void writeSharedState(SharedStateManager.State state) {
      LOGGER.debugf("writeSharedState state = %s", state);
      this.sharedStateManager.writeState(state);
   }

   private SharedStateManager.State readSharedState() {
      final SharedStateManager.State state = this.sharedStateManager.readState();
      LOGGER.debugf("readSharedState state = %s", state);
      return state;
   }

   @Override
   public SimpleString readNodeId() {
      checkIsStarted();
      final UUID nodeId = this.sharedStateManager.readNodeId();
      LOGGER.debugf("readNodeId nodeId = %s", nodeId);
      setUUID(nodeId);
      return getNodeId();
   }

}
