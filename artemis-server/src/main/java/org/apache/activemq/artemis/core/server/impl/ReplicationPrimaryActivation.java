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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQAlreadyReplicatingException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.ChannelHandler;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.BackupRegistrationMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.BackupReplicationStartFailedMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicationPrimaryPolicy;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.core.server.ActiveMQServer.SERVER_STATE.STARTED;
import static org.apache.activemq.artemis.core.server.impl.ClusterTopologySearch.searchActiveLiveNodeId;

/**
 * This is going to be {@link #run()} just by natural born primary, at the first start.
 * Both during a failover or a failback, {@link #run()} isn't going to be used, but only {@link #getActivationChannelHandler(Channel, Acceptor)}.
 */
public class ReplicationPrimaryActivation extends LiveActivation implements DistributedLock.UnavailableLockListener {

   private static final Logger LOGGER = Logger.getLogger(ReplicationPrimaryActivation.class);
   private static final long DISTRIBUTED_MANAGER_START_TIMEOUT_MILLIS = 20_000;
   private static final long BLOCKING_CALLS_TIMEOUT_MILLIS = 5_000;

   private final ReplicationPrimaryPolicy policy;

   private final ActiveMQServerImpl activeMQServer;

   @GuardedBy("replicationLock")
   private ReplicationManager replicationManager;

   private final Object replicationLock;

   private final DistributedPrimitiveManager distributedManager;

   private volatile boolean stoppingServer;

   public ReplicationPrimaryActivation(final ActiveMQServerImpl activeMQServer,
                                       final DistributedPrimitiveManager distributedManager,
                                       final ReplicationPrimaryPolicy policy) {
      this.activeMQServer = activeMQServer;
      this.policy = policy;
      this.replicationLock = new Object();
      this.distributedManager = distributedManager;
   }

   /**
    * used for testing purposes.
    */
   public DistributedPrimitiveManager getDistributedManager() {
      return distributedManager;
   }

   @Override
   public void freezeConnections(RemotingService remotingService) {
      final ReplicationManager replicationManager = getReplicationManager();

      if (remotingService != null && replicationManager != null) {
         remotingService.freeze(null, replicationManager.getBackupTransportConnection());
      } else if (remotingService != null) {
         remotingService.freeze(null, null);
      }
   }

   @Override
   public void run() {
      try {

         final NodeManager nodeManager = activeMQServer.getNodeManager();

         final String nodeId = nodeManager.readNodeId().toString();

         final DistributedLock liveLock = searchLiveOrAcquireLiveLock(nodeId, BLOCKING_CALLS_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

         if (liveLock == null) {
            return;
         }

         ReplicationBackupActivation.ensureSequentialAccessToNodeData(activeMQServer, distributedManager, LOGGER);

         activeMQServer.initialisePart1(false);

         activeMQServer.initialisePart2(false);

         // must be registered before checking the caller
         liveLock.addListener(this);

         // This control is placed here because initialisePart2 is going to load the journal that
         // could pause the JVM for enough time to lose lock ownership
         if (!liveLock.isHeldByCaller()) {
            throw new IllegalStateException("This broker isn't live anymore, probably due to application pauses eg GC, OS etc: failing now");
         }

         activeMQServer.completeActivation(true);

         if (activeMQServer.getIdentity() != null) {
            ActiveMQServerLogger.LOGGER.serverIsLive(activeMQServer.getIdentity());
         } else {
            ActiveMQServerLogger.LOGGER.serverIsLive();
         }
      } catch (Exception e) {
         // async stop it, we don't need to await this to complete
         distributedManager.stop();
         ActiveMQServerLogger.LOGGER.initializationError(e);
         activeMQServer.callActivationFailureListeners(e);
      }
   }

   private DistributedLock searchLiveOrAcquireLiveLock(final String nodeId,
                                                       final long blockingCallTimeout,
                                                       final TimeUnit unit) throws ActiveMQException, InterruptedException {
      if (policy.isCheckForLiveServer()) {
         LOGGER.infof("Searching a live server with NodeID = %s", nodeId);
         if (searchActiveLiveNodeId(policy.getClusterName(), nodeId, blockingCallTimeout, unit, activeMQServer.getConfiguration())) {
            LOGGER.infof("Found a live server with  NodeID = %s: restarting as backup", nodeId);
            activeMQServer.setHAPolicy(policy.getBackupPolicy());
            return null;
         }
      }
      startDistributedPrimitiveManager();
      return acquireDistributeLock(getDistributeLock(nodeId), blockingCallTimeout, unit);
   }

   private void startDistributedPrimitiveManager() throws InterruptedException, ActiveMQException {
      LOGGER.infof("Trying to reach the majority of quorum nodes in %d ms.", DISTRIBUTED_MANAGER_START_TIMEOUT_MILLIS);
      try {
         if (distributedManager.start(DISTRIBUTED_MANAGER_START_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            return;
         }
      } catch (InterruptedException ie) {
         throw ie;
      } catch (Throwable t) {
         LOGGER.debug(t);
      }
      assert !distributedManager.isStarted();
      throw new ActiveMQException("Cannot reach the majority of quorum nodes");
   }

   private DistributedLock getDistributeLock(final String nodeId) throws InterruptedException, ActiveMQException {
      try {
         return distributedManager.getDistributedLock(nodeId);
      } catch (Throwable t) {
         try {
            distributedManager.stop();
         } catch (Throwable ignore) {
            // don't care
         }
         if (t instanceof InterruptedException) {
            throw (InterruptedException) t;
         }
         throw new ActiveMQException("Cannot obtain a live lock instance");
      }
   }

   private DistributedLock acquireDistributeLock(final DistributedLock liveLock,
                                                 final long acquireLockTimeout,
                                                 final TimeUnit unit) throws InterruptedException, ActiveMQException {
      try {
         if (liveLock.tryLock(acquireLockTimeout, unit)) {
            return liveLock;
         }
      } catch (UnavailableStateException e) {
         LOGGER.debug(e);
      }
      try {
         distributedManager.stop();
      } catch (Throwable ignore) {
         // don't care
      }
      throw new ActiveMQException("Failed to become live");
   }

   @Override
   public ChannelHandler getActivationChannelHandler(final Channel channel, final Acceptor acceptorUsed) {
      if (stoppingServer) {
         return null;
      }
      return packet -> {
         if (packet.getType() == PacketImpl.BACKUP_REGISTRATION) {
            onBackupRegistration(channel, acceptorUsed, (BackupRegistrationMessage) packet);
         }
      };
   }

   private void onBackupRegistration(final Channel channel,
                                     final Acceptor acceptorUsed,
                                     final BackupRegistrationMessage msg) {
      try {
         startAsyncReplication(channel.getConnection(), acceptorUsed.getClusterConnection(), msg.getConnector(), msg.isFailBackRequest());
      } catch (ActiveMQAlreadyReplicatingException are) {
         channel.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.ALREADY_REPLICATING));
      } catch (ActiveMQException e) {
         LOGGER.debug("Failed to process backup registration packet", e);
         channel.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.EXCEPTION));
      }
   }

   private void startAsyncReplication(final CoreRemotingConnection remotingConnection,
                                      final ClusterConnection clusterConnection,
                                      final TransportConfiguration backupTransport,
                                      final boolean isFailBackRequest) throws ActiveMQException {
      synchronized (replicationLock) {
         if (replicationManager != null) {
            throw new ActiveMQAlreadyReplicatingException();
         }
         if (!activeMQServer.isStarted()) {
            throw new ActiveMQIllegalStateException();
         }
         final ReplicationFailureListener listener = new ReplicationFailureListener();
         remotingConnection.addCloseListener(listener);
         remotingConnection.addFailureListener(listener);
         final ReplicationManager replicationManager = new ReplicationManager(activeMQServer, remotingConnection, clusterConnection.getCallTimeout(), policy.getInitialReplicationSyncTimeout(), activeMQServer.getIOExecutorFactory());
         this.replicationManager = replicationManager;
         replicationManager.start();
         final Thread replicatingThread = new Thread(() -> replicate(replicationManager, clusterConnection, isFailBackRequest, backupTransport));
         replicatingThread.setName("async-replication-thread");
         replicatingThread.start();
      }
   }

   private void replicate(final ReplicationManager replicationManager,
                          final ClusterConnection clusterConnection,
                          final boolean isFailBackRequest,
                          final TransportConfiguration backupTransport) {
      try {
         final String nodeID = activeMQServer.getNodeID().toString();
         activeMQServer.getStorageManager().startReplication(replicationManager, activeMQServer.getPagingManager(), nodeID, isFailBackRequest && policy.isAllowAutoFailBack(), policy.getInitialReplicationSyncTimeout());

         clusterConnection.nodeAnnounced(System.currentTimeMillis(), nodeID, policy.getGroupName(), policy.getScaleDownGroupName(), new Pair<>(null, backupTransport), true);

         if (isFailBackRequest && policy.isAllowAutoFailBack()) {
            awaitBackupAnnouncementOnFailbackRequest(clusterConnection);
         }
      } catch (Exception e) {
         if (activeMQServer.getState() == STARTED) {
            /*
             * The reasoning here is that the exception was either caused by (1) the
             * (interaction with) the backup, or (2) by an IO Error at the storage. If (1), we
             * can swallow the exception and ignore the replication request. If (2) the live
             * will crash shortly.
             */
            ActiveMQServerLogger.LOGGER.errorStartingReplication(e);
         }
         try {
            ActiveMQServerImpl.stopComponent(replicationManager);
         } catch (Exception amqe) {
            ActiveMQServerLogger.LOGGER.errorStoppingReplication(amqe);
         } finally {
            synchronized (replicationLock) {
               this.replicationManager = null;
            }
         }
      }
   }

   /**
    * This is handling awaiting backup announcement before trying to failover.
    * This broker is a backup broker, acting as a live and ready to restart as a backup
    */
   private void awaitBackupAnnouncementOnFailbackRequest(ClusterConnection clusterConnection) throws Exception {
      final String nodeID = activeMQServer.getNodeID().toString();
      final BackupTopologyListener topologyListener = new BackupTopologyListener(nodeID, clusterConnection.getConnector());
      clusterConnection.addClusterTopologyListener(topologyListener);
      try {
         if (topologyListener.waitForBackup()) {
            restartAsBackupAfterFailback();
         } else {
            ActiveMQServerLogger.LOGGER.failbackMissedBackupAnnouncement();
         }
      } finally {
         clusterConnection.removeClusterTopologyListener(topologyListener);
      }
   }

   /**
    * If {@link #asyncStopServer()} happens before this call, the restart just won't happen.
    * If {@link #asyncStopServer()} happens after this call, will make the server to stop right after being restarted.
    */
   private void restartAsBackupAfterFailback() throws Exception {
      if (stoppingServer) {
         return;
      }
      synchronized (this) {
         if (stoppingServer) {
            return;
         }
         distributedManager.stop();
         activeMQServer.fail(true);
         ActiveMQServerLogger.LOGGER.restartingReplicatedBackupAfterFailback();
         activeMQServer.setHAPolicy(policy.getBackupPolicy());

         // we have matching activation sequence with the failback server so we
         // can compete to get the lock that we have just released.
         // we need to delay to give it a chance to get the lock, it is waiting on
         // the server shutdown command on the cluster connection that has just been delivered
         // by fail(..)
         //
         Thread.sleep(policy.getBackupPolicy().getVoteRetryWait());
         activeMQServer.start();
      }
   }

   private void asyncStopServer() {
      if (stoppingServer) {
         return;
      }
      synchronized (this) {
         if (stoppingServer) {
            return;
         }
         stoppingServer = true;
         new Thread(() -> {
            try {
               activeMQServer.stop();
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorRestartingBackupServer(e, activeMQServer);
            }
         }).start();
      }
   }

   @Override
   public void onUnavailableLockEvent() {
      LOGGER.error("Quorum UNAVAILABLE: async stopping broker.");
      asyncStopServer();
   }

   private final class ReplicationFailureListener implements FailureListener, CloseListener {

      @Override
      public void connectionFailed(ActiveMQException exception, boolean failedOver) {
         onReplicationConnectionClose();
      }

      @Override
      public void connectionFailed(final ActiveMQException me, boolean failedOver, String scaleDownTargetNodeID) {
         connectionFailed(me, failedOver);
      }

      @Override
      public void connectionClosed() {
         onReplicationConnectionClose();
      }
   }

   private void onReplicationConnectionClose() {
      ExecutorService executorService = activeMQServer.getThreadPool();
      if (executorService != null) {
         synchronized (replicationLock) {
            if (replicationManager == null) {
               return;
            }
         }
         executorService.execute(() -> {
            synchronized (replicationLock) {
               // we increment only if we are staying alive
               if (STARTED.equals(activeMQServer.getState())) {
                  try {
                     ReplicationBackupActivation.ensureSequentialAccessToNodeData(activeMQServer, distributedManager, LOGGER);
                  } catch (Throwable fatal) {
                     LOGGER.errorf(fatal, "Unexpected exception: %s on attempted activation sequence increment; stopping server async", fatal.getLocalizedMessage());
                     asyncStopServer();
                  }
               }
               if (replicationManager == null) {
                  return;
               }
               // this is going to stop the replication manager
               activeMQServer.getStorageManager().stopReplication();
               assert !replicationManager.isStarted();
               replicationManager = null;
            }
         });
      }
   }

   @Override
   public void close(boolean permanently, boolean restarting) throws Exception {
      synchronized (replicationLock) {
         replicationManager = null;
      }
      distributedManager.stop();
      // To avoid a NPE cause by the stop
      final NodeManager nodeManager = activeMQServer.getNodeManager();
      if (nodeManager != null) {
         if (permanently) {
            nodeManager.crashLiveServer();
         } else {
            nodeManager.pauseLiveServer();
         }
      }
   }

   @Override
   public void sendLiveIsStopping() {
      final ReplicationManager replicationManager = getReplicationManager();
      if (replicationManager == null) {
         return;
      }
      replicationManager.sendLiveIsStopping(ReplicationLiveIsStoppingMessage.LiveStopping.STOP_CALLED);
      // this pool gets a 'hard' shutdown, no need to manage the Future of this Runnable.
      activeMQServer.getScheduledPool().schedule(replicationManager::clearReplicationTokens, 30, TimeUnit.SECONDS);
   }

   @Override
   public ReplicationManager getReplicationManager() {
      synchronized (replicationLock) {
         return replicationManager;
      }
   }

   @Override
   public boolean isReplicaSync() {
      final ReplicationManager replicationManager = getReplicationManager();
      if (replicationManager == null) {
         return false;
      }
      return !replicationManager.isSynchronizing();
   }
}
