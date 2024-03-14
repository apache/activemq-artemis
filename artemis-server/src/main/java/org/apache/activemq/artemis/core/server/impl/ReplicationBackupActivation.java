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
import org.apache.activemq.artemis.core.server.NodeLocator;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.cluster.ClusterControl;
import org.apache.activemq.artemis.core.server.cluster.ClusterController;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicationBackupPolicy;
import org.apache.activemq.artemis.lockmanager.DistributedLock;
import org.apache.activemq.artemis.lockmanager.DistributedLockManager;
import org.apache.activemq.artemis.lockmanager.UnavailableStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

import static org.apache.activemq.artemis.core.server.NodeManager.NULL_NODE_ACTIVATION_SEQUENCE;
import static org.apache.activemq.artemis.core.server.impl.ReplicationObserver.ReplicationFailure;
import static org.apache.activemq.artemis.core.server.impl.quorum.ActivationSequenceStateMachine.ensureSequentialAccessToNodeData;
import static org.apache.activemq.artemis.core.server.impl.quorum.ActivationSequenceStateMachine.tryActivate;

/**
 * This activation can be used by a primary while trying to fail-back ie {@code failback == true} or
 * by a natural-born backup ie {@code failback == false}.<br>
 */
public final class ReplicationBackupActivation extends Activation implements DistributedLockManager.UnavailableManagerListener {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final ReplicationBackupPolicy policy;
   private final ActiveMQServerImpl activeMQServer;
   // This field is != null iff this node is a primary during a fail-back (i.e. acting as a backup in order to become active again).
   private final String expectedNodeID;
   @GuardedBy("this")
   private boolean closed;
   private final DistributedLockManager distributedManager;
   // Used for monitoring purposes
   private volatile ReplicationObserver replicationObserver;
   // Used for testing purposes
   private volatile ReplicationEndpoint replicationEndpoint;
   // Used for testing purposes
   private Consumer<ReplicationEndpoint> onReplicationEndpointCreation;
   // Used to arbiter one-shot server stop/restart
   private final AtomicBoolean stopping;

   public ReplicationBackupActivation(final ActiveMQServerImpl activeMQServer,
                                      final DistributedLockManager distributedManager,
                                      final ReplicationBackupPolicy policy) {
      this.activeMQServer = activeMQServer;
      if (policy.isTryFailback()) {
         // patch expectedNodeID
         final String coordinationId = policy.getPrimaryPolicy().getCoordinationId();
         if (coordinationId != null) {
            expectedNodeID = coordinationId;
         } else {
            final SimpleString serverNodeID = activeMQServer.getNodeID();
            if (serverNodeID == null || serverNodeID.isEmpty()) {
               throw new IllegalStateException("A failback activation must be biased around a specific NodeID");
            }
            this.expectedNodeID = serverNodeID.toString();
         }
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
   public DistributedLockManager getDistributedManager() {
      return distributedManager;
   }

   @Override
   public void onUnavailableManagerEvent() {
      synchronized (this) {
         if (closed) {
            return;
         }
      }
      logger.info("Unavailable quorum service detected: try restart server");
      asyncRestartServer(activeMQServer, true);
   }

   /**
    * This util class exists because {@link NodeLocator} need a {@link NodeLocator.BackupRegistrationListener}
    * to forward backup registration failure events: this is used to switch on/off backup registration event listening
    * on an existing locator.
    */
   private static final class RegistrationFailureForwarder implements NodeLocator.BackupRegistrationListener, AutoCloseable {

      private static final NodeLocator.BackupRegistrationListener NOOP_LISTENER = ignore -> {
      };
      private volatile NodeLocator.BackupRegistrationListener listener = NOOP_LISTENER;

      public RegistrationFailureForwarder to(NodeLocator.BackupRegistrationListener listener) {
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
         synchronized (activeMQServer) {
            activeMQServer.setState(ActiveMQServerImpl.SERVER_STATE.STARTED);
         }
         // restart of server due to unavailable quorum can cause NM restart losing the coordination-id
         final String coordinationId = policy.getPrimaryPolicy().getCoordinationId();
         if (coordinationId != null) {
            final String nodeId = activeMQServer.getNodeManager().getNodeId().toString();
            if (!coordinationId.equals(nodeId)) {
               ReplicationPrimaryActivation.applyCoordinationId(coordinationId, activeMQServer);
            }
         }
         distributedManager.start();
         final NodeManager nodeManager = activeMQServer.getNodeManager();
         // only a backup with positive local activation sequence could contain valuable data
         if (nodeManager.getNodeActivationSequence() > 0) {
            DistributedLock primaryLockWithInSyncReplica;
            while (true) {
               distributedManager.start();
               try {
                  primaryLockWithInSyncReplica = tryActivate(activeMQServer.getNodeManager(), distributedManager, logger);
                  break;
               } catch (UnavailableStateException canRecoverEx) {
                  distributedManager.stop();
               } catch (NodeManager.NodeManagerException fatalEx) {
                  logger.warn("Failed while auto-repairing activation sequence: stop server now", fatalEx);
                  asyncRestartServer(activeMQServer, false);
                  return;
               }
            }
            if (primaryLockWithInSyncReplica != null) {
               // retain state and start as live
               if (!activeMQServer.initialisePart1(false)) {
                  return;
               }
               startAsPrimary(primaryLockWithInSyncReplica);
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
         // allow JMX to query Artemis state
         if (!activeMQServer.initialisePart1(false)) {
            return;
         }
         synchronized (this) {
            if (closed)
               return;
         }


         final ClusterController clusterController = activeMQServer.getClusterManager().getClusterController();

         logger.info("Apache ActiveMQ Artemis Backup Server version {} [{}] started, awaiting connection to a primary to start replication", activeMQServer.getVersion().getFullVersion(),
                      activeMQServer.toString());

         clusterController.awaitConnectionToReplicationCluster();
         activeMQServer.getBackupManager().start();
         final DistributedLock primaryLock = replicateAndFailover(clusterController);
         if (primaryLock == null) {
            return;
         }
         startAsPrimary(primaryLock);
      } catch (Exception e) {
         if ((e instanceof InterruptedException || e instanceof IllegalStateException) && !activeMQServer.isStarted()) {
            // do not log these errors if the server is being stopped.
            return;
         }
         ActiveMQServerLogger.LOGGER.initializationError(e);
      }
   }

   private void startAsPrimary(final DistributedLock primaryLock) throws Exception {
      activeMQServer.setHAPolicy(policy.getPrimaryPolicy());

      synchronized (activeMQServer) {
         if (!activeMQServer.isStarted()) {
            primaryLock.close();
            return;
         }
         final NodeManager nodeManager = activeMQServer.getNodeManager();
         try {
            // stopBackup is going to write the NodeID and activation sequence previously set on the NodeManager,
            // because activeMQServer.resetNodeManager() has created a NodeManager with replicatedBackup == true.
            nodeManager.stopBackup();
            ensureSequentialAccessToNodeData(activeMQServer.toString(), nodeManager, distributedManager, logger);
         } catch (Throwable fatal) {
            logger.warn(fatal.getMessage());
            // policy is already primary one, but there's no activation yet: we can just stop
            asyncRestartServer(activeMQServer, false, false);
            throw new ActiveMQIllegalStateException("This server cannot ensure sequential access to broker data: activation is failed");
         }
         ActiveMQServerLogger.LOGGER.becomingActive(activeMQServer);
         activeMQServer.getStorageManager().start();
         activeMQServer.getBackupManager().activated();
         // IMPORTANT:
         // we're setting this activation JUST because it would allow the server to use its
         // getActivationChannelHandler to handle replication
         final ReplicationPrimaryActivation primaryActivation = new ReplicationPrimaryActivation(activeMQServer, distributedManager, policy.getPrimaryPolicy());
         primaryLock.addListener(primaryActivation);
         activeMQServer.setActivation(primaryActivation);
         activeMQServer.initialisePart2(false);
         // calling primaryActivation.stateChanged !isHelByCaller is necessary in case the lock was unavailable
         // before primaryLock.addListener: just throwing an exception won't stop the broker.
         final boolean stillPrimary;
         try {
            stillPrimary = primaryLock.isHeldByCaller();
         } catch (UnavailableStateException e) {
            logger.warn(e.getMessage(), e);
            primaryActivation.onUnavailableLockEvent();
            throw new ActiveMQIllegalStateException("This server cannot check its role as a primary: activation is failed");
         }
         if (!stillPrimary) {
            primaryActivation.onUnavailableLockEvent();
            throw new ActiveMQIllegalStateException("This server is not primary anymore: activation is failed");
         }
         if (activeMQServer.getIdentity() != null) {
            ActiveMQServerLogger.LOGGER.serverIsActive(activeMQServer.getIdentity());
         } else {
            ActiveMQServerLogger.LOGGER.serverIsActive();
         }
         activeMQServer.completeActivation(true);
      }
   }

   private NodeLocator createNodeLocator(final NodeLocator.BackupRegistrationListener registrationListener) {
      if (expectedNodeID != null) {
         assert policy.isTryFailback();
         return new NamedNodeIdLocatorForReplication(expectedNodeID, registrationListener, policy.getRetryReplicationWait());
      }
      return policy.getGroupName() == null ?
         new AnyNodeLocatorForReplication(registrationListener, activeMQServer, policy.getRetryReplicationWait()) :
         new NamedNodeLocatorForReplication(policy.getGroupName(), registrationListener, policy.getRetryReplicationWait());
   }

   private DistributedLock replicateAndFailover(final ClusterController clusterController) throws ActiveMQException, InterruptedException {
      final RegistrationFailureForwarder registrationFailureForwarder = new RegistrationFailureForwarder();
      // node locator isn't stateless and contains a list of candidate nodes to connect too, hence
      // it MUST be reused for each replicatePrimary attempt
      final NodeLocator nodeLocator = createNodeLocator(registrationFailureForwarder);
      clusterController.addClusterTopologyListenerForReplication(nodeLocator);
      try {
         while (true) {
            synchronized (this) {
               if (closed) {
                  return null;
               }
            }
            if (expectedNodeID != null) {
               logger.info("awaiting connecting to node with NodeID = {}", expectedNodeID);
            }
            final ReplicationFailure failure = replicatePrimary(clusterController, nodeLocator, registrationFailureForwarder);
            if (failure == null) {
               Thread.sleep(clusterController.getRetryIntervalForReplicatedCluster());
               continue;
            }
            if (!activeMQServer.isStarted()) {
               return null;
            }
            logger.debug("ReplicationFailure = {}", failure);
            switch (failure) {
               case VoluntaryFailOver:
               case NonVoluntaryFailover:
                  // from now on we're meant to stop:
                  // - due to failover
                  // - due to restart/stop
                  if (!stopping.compareAndSet(false, true)) {
                     return null;
                  }
                  // no more interested into these events: handling it manually from here
                  distributedManager.removeUnavailableManagerListener(this);
                  final NodeManager nodeManager = activeMQServer.getNodeManager();
                  DistributedLock primaryLockWithInSyncReplica = null;
                  if (nodeManager.getNodeActivationSequence() > 0) {
                     try {
                        primaryLockWithInSyncReplica = tryActivate(nodeManager, distributedManager, logger);
                     } catch (Throwable error) {
                        // no need to retry here, can just restart as backup that will handle a more resilient tryActivate
                        logger.warn("Errored while attempting failover", error);
                        primaryLockWithInSyncReplica = null;
                     }
                  } else {
                     logger.error("Expected positive local activation sequence for NodeID = {} during fail-over, but was {}: restarting as backup",
                                   nodeManager.getNodeId(), nodeManager.getNodeActivationSequence());
                  }
                  assert stopping.get();
                  if (primaryLockWithInSyncReplica != null) {
                     return primaryLockWithInSyncReplica;
                  }
                  ActiveMQServerLogger.LOGGER.restartingAsBackupBasedOnQuorumVoteResults();
                  // let's ignore the stopping flag here, we're already in control of it
                  asyncRestartServer(activeMQServer, true, false);
                  return null;
               case RegistrationError:
                  logger.error("Stopping broker because of critical registration error");
                  asyncRestartServer(activeMQServer, false);
                  return null;
               case AlreadyReplicating:
                  // can just retry here, data should be clean and nodeLocator
                  // should remove the primary node that has answered this
                  logger.info("Primary broker was already replicating: retry sync with another primary");
                  continue;
               case ClosedObserver:
                  return null;
               case BackupNotInSync:
                  // cleanup any dirty activation sequence to save a leaked activation sequence/nodeID to cause activation
                  final long activationSequence = activeMQServer.getNodeManager().getNodeActivationSequence();
                  boolean restart = true;
                  if (activationSequence != 0) {
                     final SimpleString syncNodeId = activeMQServer.getNodeManager().getNodeId();
                     try {
                        activeMQServer.getNodeManager().setNodeActivationSequence(NULL_NODE_ACTIVATION_SEQUENCE);
                     } catch (Throwable fatal) {
                        logger.error("Errored while resetting local activation sequence {} for NodeID = {}: stopping broker",
                                      activationSequence, syncNodeId, fatal);
                        restart = false;
                     }
                  }
                  if (restart) {
                     logger.info("Replication failure while initial sync not yet completed: restart as backup");
                  }
                  asyncRestartServer(activeMQServer, restart);
                  return null;
               case WrongNodeId:
                  logger.error("Stopping broker because of wrong node ID communication from primary: maybe a misbehaving primary?");
                  asyncRestartServer(activeMQServer, false);
                  return null;
               case WrongActivationSequence:
                  logger.error("Stopping broker because of wrong activation sequence communication from primary: maybe a misbehaving primary?");
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

   private ReplicationObserver replicationObserver() {
      if (policy.isTryFailback()) {
         return ReplicationObserver.failbackObserver(activeMQServer.getNodeManager(), activeMQServer.getBackupManager(), activeMQServer.getScheduledPool(), expectedNodeID);
      }
      return ReplicationObserver.failoverObserver(activeMQServer.getNodeManager(), activeMQServer.getBackupManager(), activeMQServer.getScheduledPool());
   }

   private ReplicationFailure replicatePrimary(final ClusterController clusterController,
                                               final NodeLocator nodeLocator,
                                               final RegistrationFailureForwarder registrationFailureForwarder) throws ActiveMQException {
      try (ReplicationObserver replicationObserver = replicationObserver();
           RegistrationFailureForwarder ignored = registrationFailureForwarder.to(replicationObserver)) {
         this.replicationObserver = replicationObserver;
         clusterController.addClusterTopologyListener(replicationObserver);
         // ReplicationError notifies backup registration failures to node locator -> forwarder -> observer
         final ReplicationError replicationError = new ReplicationError(nodeLocator);
         clusterController.addIncomingInterceptorForReplication(replicationError);
         try {
            final ClusterControl primaryControl = tryLocateAndConnectToPrimary(nodeLocator, clusterController);
            if (primaryControl == null) {
               return null;
            }
            try {
               final ReplicationEndpoint replicationEndpoint = tryAuthorizeAndAsyncRegisterAsBackupToPrimary(primaryControl, replicationObserver);
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
               silentExecution("Error on primary control close", primaryControl::close);
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
         logger.debug(debugErrorMessage, ignore);
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
            // this is necessary to make the restart an atomic operation from the server perspective
            synchronized (server) {
               if (server.getState() == ActiveMQServer.SERVER_STATE.STOPPED) {
                  return;
               }
               try {
                  server.stop(!restart);
                  if (restart) {
                     server.start();
                  }
               } catch (Exception e) {
                  if (restart) {
                     ActiveMQServerLogger.LOGGER.errorRestartingBackupServer(server, e);
                  } else {
                     ActiveMQServerLogger.LOGGER.errorStoppingServer(e);
                  }
               }
            }
         }
      }).start();
      return true;
   }

   private ClusterControl tryLocateAndConnectToPrimary(final NodeLocator nodeLocator,
                                                       final ClusterController clusterController) throws ActiveMQException {
      nodeLocator.locateNode();
      final Pair<TransportConfiguration, TransportConfiguration> possiblePrimary = nodeLocator.getPrimaryConfiguration();
      final String nodeID = nodeLocator.getNodeID();
      if (nodeID == null) {
         throw new RuntimeException("Could not establish the connection with any primary");
      }
      if (!policy.isTryFailback()) {
         assert expectedNodeID == null;
         activeMQServer.getNodeManager().setNodeID(nodeID);
      } else {
         assert expectedNodeID.equals(nodeID);
      }
      if (possiblePrimary == null) {
         return null;
      }
      final ClusterControl primaryControl = tryConnectToNodeInReplicatedCluster(clusterController, possiblePrimary.getA());
      if (primaryControl != null) {
         return primaryControl;
      }
      return tryConnectToNodeInReplicatedCluster(clusterController, possiblePrimary.getB());
   }

   private static ClusterControl tryConnectToNodeInReplicatedCluster(final ClusterController clusterController,
                                                                     final TransportConfiguration tc) {
      try {
         if (tc != null) {
            return clusterController.connectToNodeInReplicatedCluster(tc);
         }
      } catch (Exception e) {
         logger.debug(e.getMessage(), e);
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

   private ReplicationEndpoint tryAuthorizeAndAsyncRegisterAsBackupToPrimary(final ClusterControl primaryControl,
                                                                             final ReplicationObserver primaryObserver) {
      ReplicationEndpoint replicationEndpoint = null;
      try {
         primaryControl.getSessionFactory().setReconnectAttempts(0);
         primaryObserver.listenConnectionFailuresOf(primaryControl.getSessionFactory());
         primaryControl.authorize();
         replicationEndpoint = new ReplicationEndpoint(activeMQServer, policy.isTryFailback(), primaryObserver);
         final Consumer<ReplicationEndpoint> onReplicationEndpointCreation = this.onReplicationEndpointCreation;
         if (onReplicationEndpointCreation != null) {
            onReplicationEndpointCreation.accept(replicationEndpoint);
         }
         replicationEndpoint.setExecutor(activeMQServer.getExecutorFactory().getExecutor());
         connectToReplicationEndpoint(primaryControl, replicationEndpoint);
         replicationEndpoint.start();
         primaryControl.announceReplicatingBackupToPrimary(policy.isTryFailback(), policy.getClusterName());
         return replicationEndpoint;
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.replicationStartProblem(e);
         ActiveMQServerImpl.stopComponent(replicationEndpoint);
         closeChannelOf(replicationEndpoint);
         return null;
      }
   }

   private static boolean connectToReplicationEndpoint(final ClusterControl primaryControl,
                                                       final ReplicationEndpoint replicationEndpoint) {
      final Channel replicationChannel = primaryControl.createReplicationChannel();
      replicationChannel.setHandler(replicationEndpoint);
      replicationEndpoint.setChannel(replicationChannel);
      return true;
   }

   @Override
   public boolean isReplicaSync() {
      // NOTE: this method is just for monitoring purposes, not suitable to perform logic!
      // During a failover this backup won't have any active primaryObserver and will report `false`!!
      final ReplicationObserver primaryObserver = this.replicationObserver;
      if (primaryObserver == null) {
         return false;
      }
      return primaryObserver.isBackupUpToDate();
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
