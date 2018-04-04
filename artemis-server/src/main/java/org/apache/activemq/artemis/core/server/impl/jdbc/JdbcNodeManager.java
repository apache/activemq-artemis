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

import javax.sql.DataSource;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
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
   private final IOCriticalErrorListener ioCriticalErrorListener;

   public static JdbcNodeManager with(DatabaseStorageConfiguration configuration,
                                      ScheduledExecutorService scheduledExecutorService,
                                      ExecutorFactory executorFactory,
                                      IOCriticalErrorListener ioCriticalErrorListener) {
      validateTimeoutConfiguration(configuration);
      if (configuration.getDataSource() != null) {
         final SQLProvider.Factory sqlProviderFactory;
         if (configuration.getSqlProviderFactory() != null) {
            sqlProviderFactory = configuration.getSqlProviderFactory();
         } else {
            sqlProviderFactory = new PropertySQLProvider.Factory(configuration.getDataSource());
         }
         final String brokerId = java.util.UUID.randomUUID().toString();
         return usingDataSource(brokerId,
                                configuration.getJdbcNetworkTimeout(),
                                configuration.getJdbcLockExpirationMillis(),
                                configuration.getJdbcLockRenewPeriodMillis(),
                                configuration.getJdbcLockAcquisitionTimeoutMillis(),
                                configuration.getDataSource(),
                                sqlProviderFactory.create(configuration.getNodeManagerStoreTableName(), SQLProvider.DatabaseStoreType.NODE_MANAGER),
                                scheduledExecutorService,
                                executorFactory,
                                ioCriticalErrorListener);
      } else {
         final SQLProvider sqlProvider = JDBCUtils.getSQLProvider(configuration.getJdbcDriverClassName(), configuration.getNodeManagerStoreTableName(), SQLProvider.DatabaseStoreType.NODE_MANAGER);
         final String brokerId = java.util.UUID.randomUUID().toString();
         return usingConnectionUrl(brokerId,
                                   configuration.getJdbcNetworkTimeout(),
                                   configuration.getJdbcLockExpirationMillis(),
                                   configuration.getJdbcLockRenewPeriodMillis(),
                                   configuration.getJdbcLockAcquisitionTimeoutMillis(),
                                   configuration.getJdbcConnectionUrl(),
                                   configuration.getJdbcDriverClassName(),
                                   sqlProvider,
                                   scheduledExecutorService,
                                   executorFactory,
                                   ioCriticalErrorListener);
      }
   }

   private static JdbcNodeManager usingDataSource(String brokerId,
                                                  int networkTimeoutMillis,
                                                  long lockExpirationMillis,
                                                  long lockRenewPeriodMillis,
                                                  long lockAcquisitionTimeoutMillis,
                                                  DataSource dataSource,
                                                  SQLProvider provider,
                                                  ScheduledExecutorService scheduledExecutorService,
                                                  ExecutorFactory executorFactory,
                                                  IOCriticalErrorListener ioCriticalErrorListener) {
      return new JdbcNodeManager(
         () -> JdbcSharedStateManager.usingDataSource(brokerId,
                                                      networkTimeoutMillis,
                                                      executorFactory == null ? null : executorFactory.getExecutor(),
                                                      lockExpirationMillis,
                                                      dataSource,
                                                      provider),
         lockRenewPeriodMillis,
         lockAcquisitionTimeoutMillis,
         scheduledExecutorService,
         executorFactory,
         ioCriticalErrorListener);
   }

   private static JdbcNodeManager usingConnectionUrl(String brokerId,
                                                     int networkTimeoutMillis,
                                                     long lockExpirationMillis,
                                                     long lockRenewPeriodMillis,
                                                     long lockAcquisitionTimeoutMillis,
                                                     String jdbcUrl,
                                                     String driverClass,
                                                     SQLProvider provider,
                                                     ScheduledExecutorService scheduledExecutorService,
                                                     ExecutorFactory executorFactory,
                                                     IOCriticalErrorListener ioCriticalErrorListener) {
      return new JdbcNodeManager(
         () -> JdbcSharedStateManager.usingConnectionUrl(brokerId,
                                                         networkTimeoutMillis,
                                                         executorFactory == null ? null : executorFactory.getExecutor(),
                                                         lockExpirationMillis,
                                                         jdbcUrl,
                                                         driverClass,
                                                         provider),
         lockRenewPeriodMillis,
         lockAcquisitionTimeoutMillis,
         scheduledExecutorService,
         executorFactory,
         ioCriticalErrorListener);
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
                           long lockRenewPeriodMillis,
                           long lockAcquisitionTimeoutMillis,
                           ScheduledExecutorService scheduledExecutorService,
                           ExecutorFactory executorFactory,
                           IOCriticalErrorListener ioCriticalErrorListener) {
      super(false, null);
      this.lockAcquisitionTimeoutMillis = lockAcquisitionTimeoutMillis;
      this.pauser = LeaseLock.Pauser.sleep(Math.min(lockRenewPeriodMillis, MAX_PAUSE_MILLIS), TimeUnit.MILLISECONDS);
      this.sharedStateManagerFactory = sharedStateManagerFactory;
      this.scheduledLiveLockFactory = () -> ScheduledLeaseLock.of(
         scheduledExecutorService,
         executorFactory != null ? executorFactory.getExecutor() : null,
         "live",
         this.sharedStateManager.liveLock(),
         lockRenewPeriodMillis,
         ioCriticalErrorListener);
      this.scheduledBackupLockFactory = () -> ScheduledLeaseLock.of(
         scheduledExecutorService,
         executorFactory != null ?
            executorFactory.getExecutor() : null,
         "backup",
         this.sharedStateManager.backupLock(),
         lockRenewPeriodMillis,
         ioCriticalErrorListener);
      this.ioCriticalErrorListener = ioCriticalErrorListener;
      this.sharedStateManager = null;
      this.scheduledLiveLock = null;
      this.scheduledBackupLock = null;
   }

   @Override
   public void start() throws Exception {
      try {
         synchronized (this) {
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
         }
      } catch (IllegalStateException e) {
         this.sharedStateManager = null;
         this.scheduledLiveLock = null;
         this.scheduledBackupLock = null;
         if (this.ioCriticalErrorListener != null) {
            this.ioCriticalErrorListener.onIOException(e, "Failed to setup the JdbcNodeManager", null);
         }
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
   public boolean isAwaitingFailback() throws Exception {
      LOGGER.debug("ENTER isAwaitingFailback");
      try {
         return readSharedState() == SharedStateManager.State.FAILING_BACK;
      } finally {
         LOGGER.debug("EXIT isAwaitingFailback");
      }
   }

   @Override
   public boolean isBackupLive() throws Exception {
      LOGGER.debug("ENTER isBackupLive");
      try {
         //is anyone holding the live lock?
         return this.scheduledLiveLock.lock().isHeld();
      } finally {
         LOGGER.debug("EXIT isBackupLive");
      }
   }

   @Override
   public void stopBackup() throws Exception {
      LOGGER.debug("ENTER stopBackup");
      try {
         if (this.scheduledBackupLock.isStarted()) {
            LOGGER.debug("scheduledBackupLock is running: stop it and release backup lock");
            this.scheduledBackupLock.stop();
            this.scheduledBackupLock.lock().release();
         } else {
            LOGGER.debug("scheduledBackupLock is not running");
         }
      } finally {
         LOGGER.debug("EXIT stopBackup");
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
   public void releaseBackup() throws Exception {
      LOGGER.debug("ENTER releaseBackup");
      try {
         if (this.scheduledBackupLock.isStarted()) {
            LOGGER.debug("scheduledBackupLock is running: stop it and release backup lock");
            this.scheduledBackupLock.stop();
            this.scheduledBackupLock.lock().release();
         } else {
            LOGGER.debug("scheduledBackupLock is not running");
         }
      } finally {
         LOGGER.debug("EXIT releaseBackup");
      }
   }

   /**
    * Try to acquire a lock, failing with an exception otherwise.
    */
   private void lock(LeaseLock lock) throws Exception {
      final LeaseLock.AcquireResult acquireResult = lock.tryAcquire(this.lockAcquisitionTimeoutMillis, this.pauser, () -> !this.interrupted);
      switch (acquireResult) {
         case Timeout:
            throw new Exception("timed out waiting for lock");
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
         if (!this.scheduledLiveLock.lock().renew()) {
            final IllegalStateException e = new IllegalStateException("live lock can't be renewed");
            ioCriticalErrorListener.onIOException(e, "live lock can't be renewed", null);
            throw e;
         }
      }
   }

   /**
    * Lock live node and check for a live state, taking care to renew it (if needed) or releasing it otherwise
    */
   private boolean lockLiveAndCheckLiveState() throws Exception {
      lock(this.scheduledLiveLock.lock());
      final long acquiredOn = System.nanoTime();
      boolean liveWhileLocked = false;
      //check if the state is live
      final SharedStateManager.State stateWhileLocked;
      try {
         stateWhileLocked = readSharedState();
      } catch (Throwable t) {
         LOGGER.error("error while holding the live node lock and tried to read the shared state", t);
         this.scheduledLiveLock.lock().release();
         throw t;
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
   }

   @Override
   public void awaitLiveNode() throws Exception {
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
      } finally {
         LOGGER.debug("EXIT awaitLiveNode");
      }
   }

   @Override
   public void startBackup() throws Exception {
      LOGGER.debug("ENTER startBackup");
      try {
         ActiveMQServerLogger.LOGGER.waitingToBecomeBackup();

         lock(scheduledBackupLock.lock());
         scheduledBackupLock.start();
         ActiveMQServerLogger.LOGGER.gotBackupLock();
         if (getUUID() == null)
            readNodeId();
      } finally {
         LOGGER.debug("EXIT startBackup");
      }
   }

   @Override
   public ActivateCallback startLiveNode() throws Exception {
      LOGGER.debug("ENTER startLiveNode");
      try {
         setFailingBack();

         final String timeoutMessage = lockAcquisitionTimeoutMillis == -1 ? "indefinitely" : lockAcquisitionTimeoutMillis + " milliseconds";

         ActiveMQServerLogger.LOGGER.waitingToObtainLiveLock(timeoutMessage);

         lock(this.scheduledLiveLock.lock());

         this.scheduledLiveLock.start();

         ActiveMQServerLogger.LOGGER.obtainedLiveLock();

         return new ActivateCallback() {
            @Override
            public void activationComplete() {
               LOGGER.debug("ENTER activationComplete");
               try {
                  //state can be written only if the live renew task is running
                  setLive();
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
               } finally {
                  LOGGER.debug("EXIT activationComplete");
               }
            }
         };
      } finally {
         LOGGER.debug("EXIT startLiveNode");
      }
   }

   @Override
   public void pauseLiveServer() throws Exception {
      LOGGER.debug("ENTER pauseLiveServer");
      try {
         if (scheduledLiveLock.isStarted()) {
            LOGGER.debug("scheduledLiveLock is running: set paused shared state, stop it and release live lock");
            setPaused();
            scheduledLiveLock.stop();
            scheduledLiveLock.lock().release();
         } else {
            LOGGER.debug("scheduledLiveLock is not running: try renew live lock");
            if (scheduledLiveLock.lock().renew()) {
               LOGGER.debug("live lock renewed: set paused shared state and release live lock");
               setPaused();
               scheduledLiveLock.lock().release();
            } else {
               final IllegalStateException e = new IllegalStateException("live lock can't be renewed");
               ioCriticalErrorListener.onIOException(e, "live lock can't be renewed on pauseLiveServer", null);
               throw e;
            }
         }
      } finally {
         LOGGER.debug("EXIT pauseLiveServer");
      }
   }

   @Override
   public void crashLiveServer() throws Exception {
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
      final UUID nodeId = this.sharedStateManager.readNodeId();
      LOGGER.debugf("readNodeId nodeId = %s", nodeId);
      setUUID(nodeId);
      return getNodeId();
   }

}
