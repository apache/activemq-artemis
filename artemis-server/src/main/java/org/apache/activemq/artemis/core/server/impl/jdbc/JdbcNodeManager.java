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

   private static final Logger logger = Logger.getLogger(JdbcNodeManager.class);
   private static final long MAX_PAUSE_MILLIS = 2000L;

   private final Supplier<? extends SharedStateManager> sharedStateManagerFactory;
   private final Supplier<? extends ScheduledLeaseLock> scheduledLiveLockFactory;
   private final Supplier<? extends ScheduledLeaseLock> scheduledBackupLockFactory;
   private SharedStateManager sharedStateManager;
   private ScheduledLeaseLock scheduledLiveLock;
   private ScheduledLeaseLock scheduledBackupLock;
   private final long lockRenewPeriodMillis;
   private final long lockAcquisitionTimeoutMillis;
   private volatile boolean interrupted = false;
   private final LeaseLock.Pauser pauser;
   private final IOCriticalErrorListener ioCriticalErrorListener;

   public static JdbcNodeManager with(DatabaseStorageConfiguration configuration,
                                      ScheduledExecutorService scheduledExecutorService,
                                      ExecutorFactory executorFactory,
                                      IOCriticalErrorListener ioCriticalErrorListener) {
      if (configuration.getDataSource() != null) {
         final SQLProvider.Factory sqlProviderFactory;
         if (configuration.getSqlProviderFactory() != null) {
            sqlProviderFactory = configuration.getSqlProviderFactory();
         } else {
            sqlProviderFactory = new PropertySQLProvider.Factory(configuration.getDataSource());
         }
         final String brokerId = java.util.UUID.randomUUID().toString();
         return usingDataSource(brokerId, configuration.getJdbcLockExpirationMillis(), configuration.getJdbcLockRenewPeriodMillis(), configuration.getJdbcLockAcquisitionTimeoutMillis(), configuration.getDataSource(), sqlProviderFactory.create(configuration.getNodeManagerStoreTableName(), SQLProvider.DatabaseStoreType.NODE_MANAGER), scheduledExecutorService, executorFactory, ioCriticalErrorListener);
      } else {
         final SQLProvider sqlProvider = JDBCUtils.getSQLProvider(configuration.getJdbcDriverClassName(), configuration.getNodeManagerStoreTableName(), SQLProvider.DatabaseStoreType.NODE_MANAGER);
         final String brokerId = java.util.UUID.randomUUID().toString();
         return usingConnectionUrl(brokerId, configuration.getJdbcLockExpirationMillis(), configuration.getJdbcLockRenewPeriodMillis(), configuration.getJdbcLockAcquisitionTimeoutMillis(), configuration.getJdbcConnectionUrl(), configuration.getJdbcDriverClassName(), sqlProvider, scheduledExecutorService, executorFactory, ioCriticalErrorListener);
      }
   }

   static JdbcNodeManager usingDataSource(String brokerId,
                                          long lockExpirationMillis,
                                          long lockRenewPeriodMillis,
                                          long lockAcquisitionTimeoutMillis,
                                          DataSource dataSource,
                                          SQLProvider provider,
                                          ScheduledExecutorService scheduledExecutorService,
                                          ExecutorFactory executorFactory,
                                          IOCriticalErrorListener ioCriticalErrorListener) {
      return new JdbcNodeManager(
         () -> JdbcSharedStateManager.usingDataSource(brokerId, lockExpirationMillis, dataSource, provider),
         false,
         lockRenewPeriodMillis,
         lockAcquisitionTimeoutMillis,
         scheduledExecutorService,
         executorFactory,
         ioCriticalErrorListener);
   }

   public static JdbcNodeManager usingConnectionUrl(String brokerId,
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
         () -> JdbcSharedStateManager.usingConnectionUrl(brokerId, lockExpirationMillis, jdbcUrl, driverClass, provider),
         false,
         lockRenewPeriodMillis,
         lockAcquisitionTimeoutMillis,
         scheduledExecutorService,
         executorFactory,
         ioCriticalErrorListener);
   }

   private JdbcNodeManager(Supplier<? extends SharedStateManager> sharedStateManagerFactory,
                           boolean replicatedBackup,
                           long lockRenewPeriodMillis,
                           long lockAcquisitionTimeoutMillis,
                           ScheduledExecutorService scheduledExecutorService,
                           ExecutorFactory executorFactory,
                           IOCriticalErrorListener ioCriticalErrorListener) {
      super(replicatedBackup, null);
      this.lockAcquisitionTimeoutMillis = lockAcquisitionTimeoutMillis;
      this.lockRenewPeriodMillis = lockRenewPeriodMillis;
      this.pauser = LeaseLock.Pauser.sleep(Math.min(this.lockRenewPeriodMillis, MAX_PAUSE_MILLIS), TimeUnit.MILLISECONDS);
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
            if (!replicatedBackup) {
               final UUID nodeId = sharedStateManager.setup(UUIDGenerator.getInstance()::generateUUID);
               setUUID(nodeId);
            }
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
      return readSharedState() == SharedStateManager.State.FAILING_BACK;
   }

   @Override
   public boolean isBackupLive() throws Exception {
      //is anyone holding the live lock?
      return this.scheduledLiveLock.lock().isHeld();
   }

   @Override
   public void stopBackup() throws Exception {
      if (replicatedBackup) {
         final UUID nodeId = getUUID();
         sharedStateManager.writeNodeId(nodeId);
      }
      releaseBackup();
   }

   @Override
   public void interrupt() {
      //need to be volatile: must be called concurrently to work as expected
      interrupted = true;
   }

   @Override
   public void releaseBackup() throws Exception {
      if (this.scheduledBackupLock.lock().isHeldByCaller()) {
         this.scheduledBackupLock.stop();
         this.scheduledBackupLock.lock().release();
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
            try {
               ioCriticalErrorListener.onIOException(e, "live lock can't be renewed", null);
            } finally {
               throw e;
            }
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
         logger.error("error while holding the live node lock and tried to read the shared state", t);
         this.scheduledLiveLock.lock().release();
         throw t;
      }
      if (stateWhileLocked == SharedStateManager.State.LIVE) {
         renewLiveLockIfNeeded(acquiredOn);
         liveWhileLocked = true;
      } else {
         if (logger.isDebugEnabled()) {
            logger.debug("state is " + stateWhileLocked + " while holding the live lock");
         }
         //state is not live: can (try to) release the lock
         this.scheduledLiveLock.lock().release();
      }
      return liveWhileLocked;
   }

   @Override
   public void awaitLiveNode() throws Exception {
      boolean liveWhileLocked = false;
      while (!liveWhileLocked) {
         //check first without holding any lock
         final SharedStateManager.State state = readSharedState();
         if (state == SharedStateManager.State.LIVE) {
            //verify if the state is live while holding the live node lock too
            liveWhileLocked = lockLiveAndCheckLiveState();
         } else {
            if (logger.isDebugEnabled()) {
               logger.debug("awaiting live node...state: " + state);
            }
         }
         if (!liveWhileLocked) {
            checkInterrupted(() -> "awaitLiveNode got interrupted!");
            pauser.idle();
         }
      }
      //state is LIVE and live lock is acquired and valid
      logger.debug("acquired live node lock");
      this.scheduledLiveLock.start();
   }

   @Override
   public void startBackup() throws Exception {
      assert !replicatedBackup; // should not be called if this is a replicating backup
      ActiveMQServerLogger.LOGGER.waitingToBecomeBackup();

      lock(scheduledBackupLock.lock());
      scheduledBackupLock.start();
      ActiveMQServerLogger.LOGGER.gotBackupLock();
      if (getUUID() == null)
         readNodeId();
   }

   @Override
   public ActivateCallback startLiveNode() throws Exception {
      setFailingBack();

      final String timeoutMessage = lockAcquisitionTimeoutMillis == -1 ? "indefinitely" : lockAcquisitionTimeoutMillis + " milliseconds";

      ActiveMQServerLogger.LOGGER.waitingToObtainLiveLock(timeoutMessage);

      lock(this.scheduledLiveLock.lock());

      this.scheduledLiveLock.start();

      ActiveMQServerLogger.LOGGER.obtainedLiveLock();

      return new ActivateCallback() {
         @Override
         public void activationComplete() {
            try {
               //state can be written only if the live renew task is running
               setLive();
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }
      };
   }

   @Override
   public void pauseLiveServer() throws Exception {
      if (scheduledLiveLock.isStarted()) {
         setPaused();
         scheduledLiveLock.stop();
         scheduledLiveLock.lock().release();
      } else if (scheduledLiveLock.lock().renew()) {
         setPaused();
         scheduledLiveLock.lock().release();
      } else {
         final IllegalStateException e = new IllegalStateException("live lock can't be renewed");
         try {
            ioCriticalErrorListener.onIOException(e, "live lock can't be renewed on pauseLiveServer", null);
         } finally {
            throw e;
         }
      }
   }

   @Override
   public void crashLiveServer() throws Exception {
      if (this.scheduledLiveLock.lock().isHeldByCaller()) {
         scheduledLiveLock.stop();
         this.scheduledLiveLock.lock().release();
      }
   }

   @Override
   public void awaitLiveStatus() {
      while (readSharedState() != SharedStateManager.State.LIVE) {
         pauser.idle();
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
      assert !this.replicatedBackup : "the replicated backup can't write the shared state!";
      this.sharedStateManager.writeState(state);
   }

   private SharedStateManager.State readSharedState() {
      return this.sharedStateManager.readState();
   }

   @Override
   public SimpleString readNodeId() {
      final UUID nodeId = this.sharedStateManager.readNodeId();
      setUUID(nodeId);
      return getNodeId();
   }

}
