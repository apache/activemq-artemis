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

import javax.annotation.concurrent.GuardedBy;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.replication.ReplicationEndpoint;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LiveNodeLocator;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.cluster.ClusterControl;
import org.apache.activemq.artemis.core.server.cluster.ClusterController;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicationBackupPolicy;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.MutableLong;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.core.server.impl.ReplicationObserver.ReplicationFailure;

/**
 * This activation can be used by a primary while trying to fail-back ie {@code failback == true} or
 * by a natural-born backup ie {@code failback == false}.<br>
 */
public final class ReplicationBackupActivation extends Activation implements DistributedPrimitiveManager.UnavailableManagerListener {

   private static final long CHECK_ACTIVATION_SEQUENCE_WAIT_MILLIS = 200;
   private static final long CHECK_REPAIRED_ACTIVATION_SEQUENCE_WAIT_MILLIS = 2000;
   private static final long LIVE_LOCK_ACQUIRE_TIMEOUT_MILLIS = 2000;
   private static final Logger LOGGER = Logger.getLogger(ReplicationBackupActivation.class);

   private final boolean wasLive;
   private final ReplicationBackupPolicy policy;
   private final ActiveMQServerImpl activeMQServer;
   // This field is != null iff this node is a primary during a fail-back ie acting as a backup in order to become live again.
   private final String expectedNodeID;
   @GuardedBy("this")
   private boolean closed;
   private final DistributedPrimitiveManager distributedManager;
   // Used for monitoring purposes
   private volatile ReplicationObserver replicationObserver;
   // Used for testing purposes
   private volatile ReplicationEndpoint replicationEndpoint;
   // Used for testing purposes
   private Consumer<ReplicationEndpoint> onReplicationEndpointCreation;
   // Used to arbiter one-shot server stop/restart
   private final AtomicBoolean stopping;

   public ReplicationBackupActivation(final ActiveMQServerImpl activeMQServer,
                                      final boolean wasLive,
                                      final DistributedPrimitiveManager distributedManager,
                                      final ReplicationBackupPolicy policy) {
      this.wasLive = wasLive;
      this.activeMQServer = activeMQServer;
      if (policy.isTryFailback()) {
         final SimpleString serverNodeID = activeMQServer.getNodeID();
         if (serverNodeID == null || serverNodeID.isEmpty()) {
            throw new IllegalStateException("A failback activation must be biased around a specific NodeID");
         }
         this.expectedNodeID = serverNodeID.toString();
      } else {
         this.expectedNodeID = null;
      }
      this.distributedManager = distributedManager;
      this.policy = policy;
      this.replicationObserver = null;
      this.replicationEndpoint = null;
      this.stopping = new AtomicBoolean(false);
   }

   /**
    * used for testing purposes.
    */
   public DistributedPrimitiveManager getDistributedManager() {
      return distributedManager;
   }

   @Override
   public void onUnavailableManagerEvent() {
      synchronized (this) {
         if (closed) {
            return;
         }
      }
      LOGGER.info("Unavailable quorum service detected: try restart server");
      asyncRestartServer(activeMQServer, true);
   }

   /**
    * This util class exists because {@link LiveNodeLocator} need a {@link LiveNodeLocator.BackupRegistrationListener}
    * to forward backup registration failure events: this is used to switch on/off backup registration event listening
    * on an existing locator.
    */
   private static final class RegistrationFailureForwarder implements LiveNodeLocator.BackupRegistrationListener, AutoCloseable {

      private static final LiveNodeLocator.BackupRegistrationListener NOOP_LISTENER = ignore -> {
      };
      private volatile LiveNodeLocator.BackupRegistrationListener listener = NOOP_LISTENER;

      public RegistrationFailureForwarder to(LiveNodeLocator.BackupRegistrationListener listener) {
         this.listener = listener;
         return this;
      }

      @Override
      public void onBackupRegistrationFailed(boolean alreadyReplicating) {
         listener.onBackupRegistrationFailed(alreadyReplicating);
      }

      @Override
      public void close() {
         listener = NOOP_LISTENER;
      }
   }

   @Override
   public void run() {
      synchronized (this) {
         if (closed) {
            return;
         }
      }
      try {
         LOGGER.infof("Trying to reach the majority of quorum nodes");
         distributedManager.start();
         LOGGER.debug("Quorum service available");

         if (policy.isTryFailback()) {
            // we are replicating to overwrite our data, transient backup state while trying to be the primary
         } else {
            // we may be a valid insync_replica (backup) if our activation sequence is largest for a nodeId
            // verify that before removing data..
            final DistributedLock liveLockWithInSyncReplica = tryActivate();
            if (liveLockWithInSyncReplica != null) {
               // retain state and start as live
               if (!activeMQServer.initialisePart1(false)) {
                  return;
               }
               activeMQServer.setState(ActiveMQServerImpl.SERVER_STATE.STARTED);
               startAsLive(liveLockWithInSyncReplica);
               return;
            }
         }
         distributedManager.addUnavailableManagerListener(this);
         // Stop the previous node manager and create a new one with NodeManager::replicatedBackup == true:
         // NodeManager::start skip setup lock file with NodeID, until NodeManager::stopBackup is called.
         activeMQServer.resetNodeManager();
         // A primary need to preserve NodeID across runs
         activeMQServer.moveServerData(policy.getMaxSavedReplicatedJournalsSize(), policy.isTryFailback());
         activeMQServer.getNodeManager().start();
         if (!activeMQServer.initialisePart1(false)) {
            return;
         }
         synchronized (this) {
            if (closed)
               return;
         }


         final ClusterController clusterController = activeMQServer.getClusterManager().getClusterController();

         LOGGER.infof("Apache ActiveMQ Artemis Backup Server version %s [%s] started, awaiting connection to a live cluster member to start replication", activeMQServer.getVersion().getFullVersion(),
                      activeMQServer.toString());

         clusterController.awaitConnectionToReplicationCluster();
         activeMQServer.getBackupManager().start();
         activeMQServer.setState(ActiveMQServerImpl.SERVER_STATE.STARTED);
         final DistributedLock liveLock = replicateAndFailover(clusterController);
         if (liveLock == null) {
            return;
         }
         startAsLive(liveLock);
      } catch (Exception e) {
         if ((e instanceof InterruptedException || e instanceof IllegalStateException) && !activeMQServer.isStarted()) {
            // do not log these errors if the server is being stopped.
            return;
         }
         ActiveMQServerLogger.LOGGER.initializationError(e);
      }
   }

   private DistributedLock tryActivate() throws InterruptedException, ExecutionException, TimeoutException, UnavailableStateException {
      final long nodeActivationSequence = activeMQServer.getNodeManager().readNodeActivationSequence();
      if (nodeActivationSequence <= 0) {
         LOGGER.debugf("Activation sequence is 0");
         return null;
      }
      // not an empty backup (first start), let see if we can get the lock and verify our data activation sequence
      final String lockAndLongId = activeMQServer.getNodeManager().getNodeId().toString();
      final DistributedLock activationLock = distributedManager.getDistributedLock(lockAndLongId);
      try (MutableLong coordinatedNodeSequence = distributedManager.getMutableLong(lockAndLongId)) {
         while (true) {
            // dirty read is sufficient to know if we are *not* an in sync replica
            // typically the lock owner will increment to signal our data is stale and we are happy without any
            // further coordination at this point
            switch (validateCoordinatedActivationSequence(coordinatedNodeSequence, activationLock,
                                                          lockAndLongId, nodeActivationSequence)) {

               case Stale:
                  activationLock.close();
                  return null;
               case SelfRepair:
               case InSync:
                  break;
               case MaybeInSync:
                  if (activationLock.tryLock()) {
                     // BAD: where's the broker that should commit it?
                     activationLock.unlock();
                     LOGGER.warnf("Cannot assume live role for NodeID = %s: claimed activation sequence need to be repaired", lockAndLongId);
                     TimeUnit.MILLISECONDS.sleep(CHECK_REPAIRED_ACTIVATION_SEQUENCE_WAIT_MILLIS);
                     continue;
                  }
                  // quick path while data is still valuable: wait until something change (commit/repair)
                  TimeUnit.MILLISECONDS.sleep(CHECK_ACTIVATION_SEQUENCE_WAIT_MILLIS);
                  continue;
            }
            // SelfRepair, InSync
            if (!activationLock.tryLock(LIVE_LOCK_ACQUIRE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
               LOGGER.debugf("Candidate for Node ID = %s, with local activation sequence: %d, cannot acquire live lock within %dms; retrying",
                             lockAndLongId, nodeActivationSequence, LIVE_LOCK_ACQUIRE_TIMEOUT_MILLIS);
               continue;
            }
            switch (validateCoordinatedActivationSequence(coordinatedNodeSequence, activationLock, lockAndLongId, nodeActivationSequence)) {

               case Stale:
                  activationLock.close();
                  return null;
               case SelfRepair:
                  // Self-repair sequence ie we were the only one with the most up to date data.
                  // NOTE: We cannot move the sequence now, let's delay it on ensureSequentialAccessToNodeData
                  LOGGER.infof("Assuming live role for NodeID = %s: local activation sequence %d matches claimed coordinated activation sequence %d. Repairing sequence",
                               lockAndLongId, nodeActivationSequence, nodeActivationSequence);
                  return activationLock;
               case InSync:
                  // we are an in_sync_replica, good to go live as UNREPLICATED
                  LOGGER.infof("Assuming live role for NodeID = %s, local activation sequence %d matches current coordinated activation sequence %d",
                               lockAndLongId, nodeActivationSequence, nodeActivationSequence);
                  return activationLock;
               case MaybeInSync:
                  activationLock.unlock();
                  LOGGER.warnf("Cannot assume live role for NodeID = %s: claimed activation sequence need to be repaired", lockAndLongId);
                  TimeUnit.MILLISECONDS.sleep(CHECK_REPAIRED_ACTIVATION_SEQUENCE_WAIT_MILLIS);
                  continue;
            }
         }
      }
   }

   private enum ValidationResult {
      /**
       * coordinated activation sequence (claimed/committed) is far beyond the local one: data is not valuable anymore
       **/
      Stale,
      /**
       * coordinated activation sequence is the same as local one: data is in sync
       **/
      InSync,
      /**
       * next coordinated activation sequence is not committed yet: maybe data is in sync
       **/
      MaybeInSync,
      /**
       * next coordinated activation sequence is not committed yet, but this broker can self-repair: data is in sync
       **/
      SelfRepair
   }

   private ValidationResult validateCoordinatedActivationSequence(final MutableLong coordinatedNodeSequence,
                                                                  final DistributedLock activationLock,
                                                                  final String lockAndLongId,
                                                                  final long nodeActivationSequence) throws UnavailableStateException {
      assert coordinatedNodeSequence.getMutableLongId().equals(lockAndLongId);
      assert activationLock.getLockId().equals(lockAndLongId);
      final long currentCoordinatedNodeSequence = coordinatedNodeSequence.get();
      if (nodeActivationSequence == currentCoordinatedNodeSequence) {
         return ValidationResult.InSync;
      }
      if (currentCoordinatedNodeSequence > 0) {
         LOGGER.infof("Not a candidate for NodeID = %s activation, local activation sequence %d does not match coordinated activation sequence %d",
                      lockAndLongId, nodeActivationSequence, currentCoordinatedNodeSequence);
         return ValidationResult.Stale;
      }
      // claimed activation sequence
      final long claimedCoordinatedNodeSequence = -currentCoordinatedNodeSequence;
      final long sequenceGap = claimedCoordinatedNodeSequence - nodeActivationSequence;
      if (sequenceGap == 0) {
         return ValidationResult.SelfRepair;
      }
      if (sequenceGap == 1) {
         // maybe data is still valuable
         return ValidationResult.MaybeInSync;
      }
      assert sequenceGap > 1;
      // sequence is moved so much that data is no longer valuable
      LOGGER.infof("Not a candidate for NodeID = %s activation, local activation sequence %d does not match coordinated activation sequence %d",
                   lockAndLongId, nodeActivationSequence, claimedCoordinatedNodeSequence);
      return ValidationResult.Stale;
   }

   /**
    * This is going to increment the coordinated activation sequence while holding the live lock, failing with some exception otherwise.<br>
    *
    * The acceptable states are {@link ValidationResult#InSync} and {@link ValidationResult#SelfRepair}, throwing some exception otherwise.
    */
   static void ensureSequentialAccessToNodeData(ActiveMQServer activeMQServer,
                                                DistributedPrimitiveManager distributedPrimitiveManager, final Logger logger) throws ActiveMQException, InterruptedException, UnavailableStateException, ExecutionException, TimeoutException {

      final NodeManager nodeManager = activeMQServer.getNodeManager();
      final String lockAndLongId = nodeManager.getNodeId().toString();
      final DistributedLock liveLock = distributedPrimitiveManager.getDistributedLock(lockAndLongId);
      if (!liveLock.isHeldByCaller()) {
         final String message = String.format("Server [%s], live lock for NodeID = %s, not held, activation sequence cannot be safely changed",
                                              activeMQServer, lockAndLongId);
         logger.info(message);
         throw new UnavailableStateException(message);
      }
      final long nodeActivationSequence = nodeManager.readNodeActivationSequence();
      final MutableLong coordinatedNodeActivationSequence = distributedPrimitiveManager.getMutableLong(lockAndLongId);
      final long currentCoordinatedActivationSequence = coordinatedNodeActivationSequence.get();
      final long nextActivationSequence;
      if (currentCoordinatedActivationSequence < 0) {
         // Check Self-Repair
         if (nodeActivationSequence != -currentCoordinatedActivationSequence) {
            final String message = String.format("Server [%s], cannot assume live role for NodeID = %s, local activation sequence %d does not match current claimed coordinated sequence %d: need repair",
                                                 activeMQServer, lockAndLongId, nodeActivationSequence, -currentCoordinatedActivationSequence);
            logger.info(message);
            throw new ActiveMQException(message);
         }
         // auto-repair: this is the same server that failed to commit its claimed sequence
         nextActivationSequence = nodeActivationSequence;
      } else {
         // Check InSync
         if (nodeActivationSequence != currentCoordinatedActivationSequence) {
            final String message = String.format("Server [%s], cannot assume live role for NodeID = %s, local activation sequence %d does not match current coordinated sequence %d",
                                                 activeMQServer, lockAndLongId, nodeActivationSequence, currentCoordinatedActivationSequence);
            logger.info(message);
            throw new ActiveMQException(message);
         }
         nextActivationSequence = nodeActivationSequence + 1;
      }
      // UN_REPLICATED STATE ENTER: auto-repair doesn't need to claim and write locally
      if (nodeActivationSequence != nextActivationSequence) {
         // claim
         if (!coordinatedNodeActivationSequence.compareAndSet(nodeActivationSequence, -nextActivationSequence)) {
            final String message = String.format("Server [%s], cannot assume live role for NodeID = %s, activation sequence claim failed, local activation sequence %d no longer matches current coordinated sequence %d",
                                                 activeMQServer, lockAndLongId, nodeActivationSequence, coordinatedNodeActivationSequence.get());
            logger.infof(message);
            throw new ActiveMQException(message);
         }
         // claim success: write locally
         try {
            nodeManager.writeNodeActivationSequence(nextActivationSequence);
         } catch (NodeManager.NodeManagerException fatal) {
            logger.errorf("Server [%s] failed to set local activation sequence to: %d for NodeId =%s. Cannot continue committing coordinated activation sequence: REQUIRES ADMIN INTERVENTION",
                          activeMQServer, nextActivationSequence, lockAndLongId);
            throw new UnavailableStateException(fatal);
         }
         logger.infof("Server [%s], incremented local activation sequence to: %d for NodeId = %s",
                      activeMQServer, nextActivationSequence, lockAndLongId);
      }
      // commit
      if (!coordinatedNodeActivationSequence.compareAndSet(-nextActivationSequence, nextActivationSequence)) {
         final String message = String.format("Server [%s], cannot assume live role for NodeID = %s, activation sequence commit failed, local activation sequence %d no longer matches current coordinated sequence %d",
                                              activeMQServer, lockAndLongId, nodeActivationSequence, coordinatedNodeActivationSequence.get());
         logger.infof(message);
         throw new ActiveMQException(message);
      }
      logger.infof("Server [%s], incremented coordinated activation sequence to: %d for NodeId = %s",
                   activeMQServer, nextActivationSequence, lockAndLongId);
   }

   private void startAsLive(final DistributedLock liveLock) throws Exception {
      activeMQServer.setHAPolicy(policy.getLivePolicy());

      synchronized (activeMQServer) {
         if (!activeMQServer.isStarted()) {
            liveLock.close();
            return;
         }
         ensureSequentialAccessToNodeData(activeMQServer, distributedManager, LOGGER);

         ActiveMQServerLogger.LOGGER.becomingLive(activeMQServer);
         // stopBackup is going to write the NodeID previously set on the NodeManager,
         // because activeMQServer.resetNodeManager() has created a NodeManager with replicatedBackup == true.
         activeMQServer.getNodeManager().stopBackup();
         activeMQServer.getStorageManager().start();
         activeMQServer.getBackupManager().activated();
         // IMPORTANT:
         // we're setting this activation JUST because it would allow the server to use its
         // getActivationChannelHandler to handle replication
         final ReplicationPrimaryActivation primaryActivation = new ReplicationPrimaryActivation(activeMQServer, distributedManager, policy.getLivePolicy());
         liveLock.addListener(primaryActivation);
         activeMQServer.setActivation(primaryActivation);
         activeMQServer.initialisePart2(false);
         // calling primaryActivation.stateChanged !isHelByCaller is necessary in case the lock was unavailable
         // before liveLock.addListener: just throwing an exception won't stop the broker.
         final boolean stillLive;
         try {
            stillLive = liveLock.isHeldByCaller();
         } catch (UnavailableStateException e) {
            LOGGER.warn(e);
            primaryActivation.onUnavailableLockEvent();
            throw new ActiveMQIllegalStateException("This server cannot check its role as a live: activation is failed");
         }
         if (!stillLive) {
            primaryActivation.onUnavailableLockEvent();
            throw new ActiveMQIllegalStateException("This server is not live anymore: activation is failed");
         }
         if (activeMQServer.getIdentity() != null) {
            ActiveMQServerLogger.LOGGER.serverIsLive(activeMQServer.getIdentity());
         } else {
            ActiveMQServerLogger.LOGGER.serverIsLive();
         }
         activeMQServer.completeActivation(true);
      }
   }

   private LiveNodeLocator createLiveNodeLocator(final LiveNodeLocator.BackupRegistrationListener registrationListener) {
      if (expectedNodeID != null) {
         assert policy.isTryFailback();
         return new NamedLiveNodeIdLocatorForReplication(expectedNodeID, registrationListener, policy.getRetryReplicationWait());
      }
      return policy.getGroupName() == null ?
         new AnyLiveNodeLocatorForReplication(registrationListener, activeMQServer, policy.getRetryReplicationWait()) :
         new NamedLiveNodeLocatorForReplication(policy.getGroupName(), registrationListener, policy.getRetryReplicationWait());
   }

   private DistributedLock replicateAndFailover(final ClusterController clusterController) throws ActiveMQException, InterruptedException {
      final RegistrationFailureForwarder registrationFailureForwarder = new RegistrationFailureForwarder();
      // node locator isn't stateless and contains a live-list of candidate nodes to connect too, hence
      // it MUST be reused for each replicateLive attempt
      final LiveNodeLocator nodeLocator = createLiveNodeLocator(registrationFailureForwarder);
      clusterController.addClusterTopologyListenerForReplication(nodeLocator);
      try {
         while (true) {
            synchronized (this) {
               if (closed) {
                  return null;
               }
            }
            final ReplicationFailure failure = replicateLive(clusterController, nodeLocator, registrationFailureForwarder);
            if (failure == null) {
               Thread.sleep(clusterController.getRetryIntervalForReplicatedCluster());
               continue;
            }
            if (!activeMQServer.isStarted()) {
               return null;
            }
            LOGGER.debugf("ReplicationFailure = %s", failure);
            boolean voluntaryFailOver = false;
            switch (failure) {
               case VoluntaryFailOver:
                  voluntaryFailOver = true;
               case NonVoluntaryFailover:
                  // disable quorum service unavailability handling and just treat this imperatively
                  if (!stopping.compareAndSet(false, true)) {
                     return null;
                  }
                  // from now on we're meant to stop:
                  // - due to failover
                  // - due to restart/stop
                  DistributedLock liveLockWithInSyncReplica;
                  try {
                     liveLockWithInSyncReplica = tryActivate();
                  } catch (Throwable error) {
                     LOGGER.debug("Errored while attempting activation", error);
                     liveLockWithInSyncReplica = null;
                  }
                  assert stopping.get();
                  if (liveLockWithInSyncReplica != null) {
                     return liveLockWithInSyncReplica;
                  }
                  boolean restart = true;
                  if (voluntaryFailOver && isFirstFailbackAttempt()) {
                     restart = false;
                     LOGGER.error("Failed to fail-back: stopping broker based on quorum results");
                  } else {
                     ActiveMQServerLogger.LOGGER.restartingAsBackupBasedOnQuorumVoteResults();
                  }
                  // let's ignore the stopping flag here, we're already in control of it
                  asyncRestartServer(activeMQServer, restart, false);
                  return null;
               case RegistrationError:
                  LOGGER.error("Stopping broker because of critical registration error");
                  asyncRestartServer(activeMQServer, false);
                  return null;
               case AlreadyReplicating:
                  // can just retry here, data should be clean and nodeLocator
                  // should remove the live node that has answered this
                  LOGGER.info("Live broker was already replicating: retry sync with another live");
                  continue;
               case ClosedObserver:
                  return null;
               case BackupNotInSync:
                  LOGGER.info("Replication failure while initial sync not yet completed: restart as backup");
                  asyncRestartServer(activeMQServer, true);
                  return null;
               case WrongNodeId:
                  LOGGER.error("Stopping broker because of wrong node ID communication from live: maybe a misbehaving live?");
                  asyncRestartServer(activeMQServer, false);
                  return null;
               default:
                  throw new AssertionError("Unsupported failure " + failure);
            }
         }
      } finally {
         silentExecution("Error on cluster topology listener for replication cleanup", () -> clusterController.removeClusterTopologyListenerForReplication(nodeLocator));
      }
   }

   /**
    * {@code wasLive} is {code true} only while transitioning from primary to backup.<br>
    * If a natural born backup become live and allows failback, while transitioning to back again
    * {@code wasLive} is still {@code false}.<br>
    * The check on {@link ReplicationBackupPolicy#isTryFailback()} is redundant but still useful for correctness.
    * <p>
    * In case of fail-back, any event that's going to restart this broker as backup (eg quorum service unavailable
    * or some replication failures) will cause {@code wasLive} to be {@code false}, because the HA policy set isn't
    * a primary anymore.
    */
   private boolean isFirstFailbackAttempt() {
      return wasLive && policy.isTryFailback();
   }

   private ReplicationObserver replicationObserver() {
      if (policy.isTryFailback()) {
         return ReplicationObserver.failbackObserver(activeMQServer.getNodeManager(), activeMQServer.getBackupManager(), activeMQServer.getScheduledPool(), expectedNodeID);
      }
      return ReplicationObserver.failoverObserver(activeMQServer.getNodeManager(), activeMQServer.getBackupManager(), activeMQServer.getScheduledPool());
   }

   private ReplicationFailure replicateLive(final ClusterController clusterController,
                                            final LiveNodeLocator liveLocator,
                                            final RegistrationFailureForwarder registrationFailureForwarder) throws ActiveMQException {
      try (ReplicationObserver replicationObserver = replicationObserver();
           RegistrationFailureForwarder ignored = registrationFailureForwarder.to(replicationObserver)) {
         this.replicationObserver = replicationObserver;
         clusterController.addClusterTopologyListener(replicationObserver);
         // ReplicationError notifies backup registration failures to live locator -> forwarder -> observer
         final ReplicationError replicationError = new ReplicationError(liveLocator);
         clusterController.addIncomingInterceptorForReplication(replicationError);
         try {
            final ClusterControl liveControl = tryLocateAndConnectToLive(liveLocator, clusterController);
            if (liveControl == null) {
               return null;
            }
            try {
               final ReplicationEndpoint replicationEndpoint = tryAuthorizeAndAsyncRegisterAsBackupToLive(liveControl, replicationObserver);
               if (replicationEndpoint == null) {
                  return ReplicationFailure.RegistrationError;
               }
               this.replicationEndpoint = replicationEndpoint;
               try {
                  return replicationObserver.awaitReplicationFailure();
               } finally {
                  this.replicationEndpoint = null;
                  ActiveMQServerImpl.stopComponent(replicationEndpoint);
                  closeChannelOf(replicationEndpoint);
               }
            } finally {
               silentExecution("Error on live control close", liveControl::close);
            }
         } finally {
            silentExecution("Error on cluster topology listener cleanup", () -> clusterController.removeClusterTopologyListener(replicationObserver));
            silentExecution("Error while removing incoming interceptor for replication", () -> clusterController.removeIncomingInterceptorForReplication(replicationError));
         }
      } finally {
         this.replicationObserver = null;
      }
   }

   private static void silentExecution(String debugErrorMessage, Runnable task) {
      try {
         task.run();
      } catch (Throwable ignore) {
         LOGGER.debug(debugErrorMessage, ignore);
      }
   }

   private static void closeChannelOf(final ReplicationEndpoint replicationEndpoint) {
      if (replicationEndpoint == null) {
         return;
      }
      if (replicationEndpoint.getChannel() != null) {
         silentExecution("Error while closing replication endpoint channel", () -> replicationEndpoint.getChannel().close());
         replicationEndpoint.setChannel(null);
      }
   }

   private boolean asyncRestartServer(final ActiveMQServer server, boolean restart) {
      return asyncRestartServer(server, restart, true);
   }

   private boolean asyncRestartServer(final ActiveMQServer server, boolean restart, boolean checkStopping) {
      if (checkStopping) {
         if (!stopping.compareAndSet(false, true)) {
            return false;
         }
      }
      new Thread(() -> {
         if (server.getState() != ActiveMQServer.SERVER_STATE.STOPPED && server.getState() != ActiveMQServer.SERVER_STATE.STOPPING) {
            try {
               server.stop(!restart);
               if (restart) {
                  server.start();
               }
            } catch (Exception e) {
               if (restart) {
                  ActiveMQServerLogger.LOGGER.errorRestartingBackupServer(e, server);
               } else {
                  ActiveMQServerLogger.LOGGER.errorStoppingServer(e);
               }
            }
         }
      }).start();
      return true;
   }

   private ClusterControl tryLocateAndConnectToLive(final LiveNodeLocator liveLocator,
                                                    final ClusterController clusterController) throws ActiveMQException {
      liveLocator.locateNode();
      final Pair<TransportConfiguration, TransportConfiguration> possibleLive = liveLocator.getLiveConfiguration();
      final String nodeID = liveLocator.getNodeID();
      if (nodeID == null) {
         throw new RuntimeException("Could not establish the connection with any live");
      }
      if (!policy.isTryFailback()) {
         assert expectedNodeID == null;
         activeMQServer.getNodeManager().setNodeID(nodeID);
      } else {
         assert expectedNodeID.equals(nodeID);
      }
      if (possibleLive == null) {
         return null;
      }
      final ClusterControl liveControl = tryConnectToNodeInReplicatedCluster(clusterController, possibleLive.getA());
      if (liveControl != null) {
         return liveControl;
      }
      return tryConnectToNodeInReplicatedCluster(clusterController, possibleLive.getB());
   }

   private static ClusterControl tryConnectToNodeInReplicatedCluster(final ClusterController clusterController,
                                                                     final TransportConfiguration tc) {
      try {
         if (tc != null) {
            return clusterController.connectToNodeInReplicatedCluster(tc);
         }
      } catch (Exception e) {
         LOGGER.debug(e.getMessage(), e);
      }
      return null;
   }

   @Override
   public void close(final boolean permanently, final boolean restarting) throws Exception {
      synchronized (this) {
         closed = true;
         final ReplicationObserver replicationObserver = this.replicationObserver;
         if (replicationObserver != null) {
            replicationObserver.close();
         }
      }
      //we have to check as the server policy may have changed
      try {
         if (activeMQServer.getHAPolicy().isBackup()) {
            // To avoid a NPE cause by the stop
            final NodeManager nodeManager = activeMQServer.getNodeManager();

            activeMQServer.interruptActivationThread(nodeManager);

            if (nodeManager != null) {
               nodeManager.stopBackup();
            }
         }
      } finally {
         // this one need to happen after interrupting the activation thread
         // in order to unblock distributedManager::start
         distributedManager.stop();
      }
   }

   @Override
   public void preStorageClose() throws Exception {
      // TODO replication endpoint close?
   }

   private ReplicationEndpoint tryAuthorizeAndAsyncRegisterAsBackupToLive(final ClusterControl liveControl,
                                                                          final ReplicationObserver liveObserver) {
      ReplicationEndpoint replicationEndpoint = null;
      try {
         liveControl.getSessionFactory().setReconnectAttempts(1);
         liveObserver.listenConnectionFailuresOf(liveControl.getSessionFactory());
         liveControl.authorize();
         replicationEndpoint = new ReplicationEndpoint(activeMQServer, policy.isTryFailback(), liveObserver);
         final Consumer<ReplicationEndpoint> onReplicationEndpointCreation = this.onReplicationEndpointCreation;
         if (onReplicationEndpointCreation != null) {
            onReplicationEndpointCreation.accept(replicationEndpoint);
         }
         replicationEndpoint.setExecutor(activeMQServer.getExecutorFactory().getExecutor());
         connectToReplicationEndpoint(liveControl, replicationEndpoint);
         replicationEndpoint.start();
         liveControl.announceReplicatingBackupToLive(policy.isTryFailback(), policy.getClusterName());
         return replicationEndpoint;
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.replicationStartProblem(e);
         ActiveMQServerImpl.stopComponent(replicationEndpoint);
         closeChannelOf(replicationEndpoint);
         return null;
      }
   }

   private static boolean connectToReplicationEndpoint(final ClusterControl liveControl,
                                                final ReplicationEndpoint replicationEndpoint) {
      final Channel replicationChannel = liveControl.createReplicationChannel();
      replicationChannel.setHandler(replicationEndpoint);
      replicationEndpoint.setChannel(replicationChannel);
      return true;
   }

   @Override
   public boolean isReplicaSync() {
      // NOTE: this method is just for monitoring purposes, not suitable to perform logic!
      // During a failover this backup won't have any active liveObserver and will report `false`!!
      final ReplicationObserver liveObserver = this.replicationObserver;
      if (liveObserver == null) {
         return false;
      }
      return liveObserver.isBackupUpToDate();
   }

   public ReplicationEndpoint getReplicationEndpoint() {
      return replicationEndpoint;
   }

   /**
    * This must be used just for testing purposes.
    */
   public void spyReplicationEndpointCreation(Consumer<ReplicationEndpoint> onReplicationEndpointCreation) {
      Objects.requireNonNull(onReplicationEndpointCreation);
      this.onReplicationEndpointCreation = onReplicationEndpointCreation;
   }
}
