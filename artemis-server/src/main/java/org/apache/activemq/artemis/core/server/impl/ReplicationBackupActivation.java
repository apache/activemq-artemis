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
import org.apache.activemq.artemis.core.server.NodeManager;;
import org.apache.activemq.artemis.core.server.cluster.ClusterControl;
import org.apache.activemq.artemis.core.server.cluster.ClusterController;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicationBackupPolicy;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.core.server.impl.ReplicationObserver.ReplicationFailure;

/**
 * This activation can be used by a primary while trying to fail-back ie {@code failback == true} or
 * by a natural-born backup ie {@code failback == false}.<br>
 */
public final class ReplicationBackupActivation extends Activation implements DistributedPrimitiveManager.UnavailableManagerListener {

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
         LOGGER.info("Trying to reach majority of quorum service nodes");
         distributedManager.start();
         LOGGER.info("Quorum service available: starting broker");
         distributedManager.addUnavailableManagerListener(this);
         // Stop the previous node manager and create a new one with NodeManager::replicatedBackup == true:
         // NodeManager::start skip setup lock file with NodeID, until NodeManager::stopBackup is called.
         activeMQServer.resetNodeManager();
         activeMQServer.getNodeManager().stop();
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
         clusterController.awaitConnectionToReplicationCluster();
         activeMQServer.getBackupManager().start();
         ActiveMQServerLogger.LOGGER.backupServerStarted(activeMQServer.getVersion().getFullVersion(),
                                                         activeMQServer.getNodeManager().getNodeId());
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

   private void startAsLive(final DistributedLock liveLock) throws Exception {
      activeMQServer.setHAPolicy(policy.getLivePolicy());

      synchronized (activeMQServer) {
         if (!activeMQServer.isStarted()) {
            liveLock.close();
            return;
         }
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
                  final DistributedLock liveLock = tryAcquireLiveLock();
                  // from now on we're meant to stop:
                  // - due to failover
                  // - due to restart/stop
                  assert stopping.get();
                  if (liveLock != null) {
                     return liveLock;
                  }
                  boolean restart = true;
                  if (voluntaryFailOver && isFirstFailbackAttempt()) {
                     restart = false;
                     LOGGER.error("Failed to fail-back: stopping broker based on quorum results");
                  } else {
                     ActiveMQServerLogger.LOGGER.restartingAsBackupBasedOnQuorumVoteResults();
                  }
                  // let's ignore the stopping flag here, we're in control of it
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
         silentExecution("Errored on cluster topology listener for replication cleanup", () -> clusterController.removeClusterTopologyListenerForReplication(nodeLocator));
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

   private DistributedLock tryAcquireLiveLock() throws InterruptedException {
      // disable quorum service unavailability handling and just treat this imperatively
      if (!stopping.compareAndSet(false, true)) {
         // already unavailable quorum service: fail fast
         return null;
      }
      distributedManager.removeUnavailableManagerListener(this);
      assert activeMQServer.getNodeManager().getNodeId() != null;
      final String liveID = activeMQServer.getNodeManager().getNodeId().toString();
      final int voteRetries = policy.getVoteRetries();
      final long maxAttempts = voteRetries >= 0 ? (voteRetries + 1) : -1;
      if (maxAttempts == -1) {
         LOGGER.error("It's not safe to retry an infinite amount of time to acquire a live lock: please consider setting a vote-retries value");
      }
      final long voteRetryWait = policy.getVoteRetryWait();
      final DistributedLock liveLock = getLock(distributedManager, liveID);
      if (liveLock == null) {
         return null;
      }
      for (long attempt = 0; maxAttempts >= 0 ? (attempt < maxAttempts) : true; attempt++) {
         try {
            if (liveLock.tryLock(voteRetryWait, TimeUnit.MILLISECONDS)) {
               LOGGER.debugf("%s live lock acquired after %d attempts.", liveID, (attempt + 1));
               return liveLock;
            }
         } catch (UnavailableStateException e) {
            LOGGER.warnf(e, "Failed to acquire live lock %s because of unavailable quorum service: stop trying", liveID);
            distributedManager.stop();
            return null;
         }
      }
      LOGGER.warnf("Failed to acquire live lock %s after %d tries", liveID, maxAttempts);
      distributedManager.stop();
      return null;
   }

   private DistributedLock getLock(final DistributedPrimitiveManager manager,
                                   final String lockId) throws InterruptedException {
      if (!manager.isStarted()) {
         return null;
      }
      try {
         return manager.getDistributedLock(lockId);
      } catch (ExecutionException e) {
         LOGGER.warnf(e, "Errored while getting lock %s", lockId);
         return null;
      } catch (TimeoutException te) {
         LOGGER.warnf(te, "Timeout while getting lock %s", lockId);
         return null;
      }
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
               assert replicationEndpoint != null;
               try {
                  return replicationObserver.awaitReplicationFailure();
               } finally {
                  this.replicationEndpoint = null;
                  ActiveMQServerImpl.stopComponent(replicationEndpoint);
                  closeChannelOf(replicationEndpoint);
               }
            } finally {
               silentExecution("Errored on live control close", liveControl::close);
            }
         } finally {
            silentExecution("Errored on cluster topology listener cleanup", () -> clusterController.removeClusterTopologyListener(replicationObserver));
            silentExecution("Errored while removing incoming interceptor for replication", () -> clusterController.removeIncomingInterceptorForReplication(replicationError));
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
         silentExecution("Errored while closing replication endpoint channel", () -> replicationEndpoint.getChannel().close());
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
