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
package org.apache.activemq.artemis.core.server.cluster.qourum;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.SessionFailureListener;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.Topology;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NetworkHealthCheck;
import org.apache.activemq.artemis.core.server.NodeManager;

public class SharedNothingBackupQuorum implements Quorum, SessionFailureListener {

   private TransportConfiguration liveTransportConfiguration;

   public enum BACKUP_ACTIVATION {
      FAIL_OVER, FAILURE_REPLICATING, ALREADY_REPLICATING, STOP;
   }

   private QuorumManager quorumManager;

   private String targetServerID = "";

   private final NodeManager nodeManager;

   private final StorageManager storageManager;
   private final ScheduledExecutorService scheduledPool;
   private final int quorumSize;

   private final int voteRetries;

   private final long voteRetryWait;

   private final Object voteGuard = new Object();

   private CountDownLatch latch;

   private ClientSessionFactoryInternal sessionFactory;

   private CoreRemotingConnection connection;

   private final NetworkHealthCheck networkHealthCheck;

   private volatile boolean stopped = false;

   /**
    * This is a safety net in case the live sends the first {@link ReplicationLiveIsStoppingMessage}
    * with code {@link org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage.LiveStopping#STOP_CALLED} and crashes before sending the second with
    * {@link org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage.LiveStopping#FAIL_OVER}.
    * <p>
    * If the second message does come within this dead line, we fail over anyway.
    */
   public static final int WAIT_TIME_AFTER_FIRST_LIVE_STOPPING_MSG = 60;

   public SharedNothingBackupQuorum(StorageManager storageManager,
                                    NodeManager nodeManager,
                                    ScheduledExecutorService scheduledPool,
                                    NetworkHealthCheck networkHealthCheck,
                                    int quorumSize,
                                    int voteRetries,
                                    long voteRetryWait) {
      this.storageManager = storageManager;
      this.scheduledPool = scheduledPool;
      this.quorumSize = quorumSize;
      this.latch = new CountDownLatch(1);
      this.nodeManager = nodeManager;
      this.networkHealthCheck = networkHealthCheck;
      this.voteRetries = voteRetries;
      this.voteRetryWait = voteRetryWait;
   }

   private volatile BACKUP_ACTIVATION signal;

   /**
    * safety parameter to make _sure_ we get out of await()
    */
   private static final int LATCH_TIMEOUT = 30;

   private final Object decisionGuard = new Object();

   @Override
   public String getName() {
      return "SharedNothingBackupQuorum";
   }

   public void decideOnAction(Topology topology) {
      //we may get called via multiple paths so need to guard
      synchronized (decisionGuard) {
         if (signal == BACKUP_ACTIVATION.FAIL_OVER) {
            if (networkHealthCheck != null && !networkHealthCheck.check()) {
               signal = BACKUP_ACTIVATION.FAILURE_REPLICATING;
            }
            return;
         }
         if (!isLiveDown()) {
            //lost connection but don't know if live is down so restart as backup as we can't replicate any more
            ActiveMQServerLogger.LOGGER.restartingAsBackupBasedOnQuorumVoteResults();
            signal = BACKUP_ACTIVATION.FAILURE_REPLICATING;
         } else {
            // live is assumed to be down, backup fails-over
            ActiveMQServerLogger.LOGGER.failingOverBasedOnQuorumVoteResults();
            signal = BACKUP_ACTIVATION.FAIL_OVER;
         }

         /* use NetworkHealthCheck to determine if node is isolated
          * if there are no addresses/urls configured then ignore and rely on quorum vote only
          */
         if (networkHealthCheck != null && !networkHealthCheck.isEmpty()) {
            if (networkHealthCheck.check()) {
               // live is assumed to be down, backup fails-over
               signal = BACKUP_ACTIVATION.FAIL_OVER;
            } else {
               ActiveMQServerLogger.LOGGER.serverIsolatedOnNetwork();
               signal = BACKUP_ACTIVATION.FAILURE_REPLICATING;
            }
         }
      }
      latch.countDown();
   }

   public void liveIDSet(String liveID) {
      targetServerID = liveID;
      nodeManager.setNodeID(liveID);
      liveTransportConfiguration = quorumManager.getLiveTransportConfiguration(targetServerID);
      //now we are replicating we can start waiting for disconnect notifications so we can fail over
      // sessionFactory.addFailureListener(this);
   }

   @Override
   public void setQuorumManager(QuorumManager quorumManager) {
      this.quorumManager = quorumManager;
   }

   /**
    * if the node going down is the node we are replicating from then decide on an action.
    *
    * @param topology
    * @param eventUID
    * @param nodeID
    */
   @Override
   public void nodeDown(Topology topology, long eventUID, String nodeID) {
      if (targetServerID.equals(nodeID)) {
         decideOnAction(topology);
      }
   }

   @Override
   public void nodeUp(Topology topology) {
      //noop
   }

   /**
    * if the connection to our replicated live goes down then decide on an action
    */
   @Override
   public void connectionFailed(ActiveMQException exception, boolean failedOver) {
      decideOnAction(sessionFactory.getServerLocator().getTopology());
   }

   @Override
   public void connectionFailed(final ActiveMQException me, boolean failedOver, String scaleDownTargetNodeID) {
      connectionFailed(me, failedOver);
   }

   @Override
   public void beforeReconnect(ActiveMQException exception) {
      //noop
   }

   @Override
   public void close() {
      causeExit(BACKUP_ACTIVATION.STOP);
   }

   /**
    * @param sessionFactory the session factory used to connect to the live server
    */
   public void setSessionFactory(final ClientSessionFactoryInternal sessionFactory) {
      this.sessionFactory = sessionFactory;
      this.connection = (CoreRemotingConnection) sessionFactory.getConnection();
      connection.addFailureListener(this);
      //belts and braces, there are circumstances where the connection listener doesn't get called but the session does.
      sessionFactory.addFailureListener(this);
   }

   /**
    * Releases the latch, causing the backup activation thread to fail-over.
    * <p>
    * The use case is for when the 'live' has an orderly shutdown, in which case it informs the
    * backup that it should fail-over.
    */
   public synchronized void failOver(ReplicationLiveIsStoppingMessage.LiveStopping finalMessage) {
      removeListener();
      signal = BACKUP_ACTIVATION.FAIL_OVER;
      if (finalMessage == ReplicationLiveIsStoppingMessage.LiveStopping.FAIL_OVER) {
         latch.countDown();
      }
      if (finalMessage == ReplicationLiveIsStoppingMessage.LiveStopping.STOP_CALLED) {
         final CountDownLatch localLatch = latch;
         scheduledPool.schedule(new Runnable() {
            @Override
            public void run() {
               localLatch.countDown();
            }

         }, WAIT_TIME_AFTER_FIRST_LIVE_STOPPING_MSG, TimeUnit.SECONDS);
      }
   }

   public void notifyRegistrationFailed() {
      signal = BACKUP_ACTIVATION.FAILURE_REPLICATING;
      latch.countDown();
   }

   public void notifyAlreadyReplicating() {
      signal = BACKUP_ACTIVATION.ALREADY_REPLICATING;
      latch.countDown();
   }

   private void removeListener() {
      if (connection != null) {
         connection.removeFailureListener(this);
      }
   }

   /**
    * Called by the replicating backup (i.e. "SharedNothing" backup) to wait for the signal to
    * fail-over or to stop.
    *
    * @return signal, indicating whether to stop or to fail-over
    */
   public BACKUP_ACTIVATION waitForStatusChange() {
      try {
         latch.await();
      } catch (InterruptedException e) {
         return BACKUP_ACTIVATION.STOP;
      }
      return signal;
   }

   /**
    * Cause the Activation thread to exit and the server to be stopped.
    *
    * @param explicitSignal the state we want to set the quorum manager to return
    */
   public synchronized void causeExit(BACKUP_ACTIVATION explicitSignal) {
      stopped = true;
      removeListener();
      this.signal = explicitSignal;
      latch.countDown();
   }

   public synchronized void reset() {
      latch = new CountDownLatch(1);
   }

   /**
    * we need to vote on the quorum to decide if we should become live
    *
    * @return the voting decision
    */
   private boolean isLiveDown() {
      //lets assume live is not down
      Boolean decision = false;
      int voteAttempts = 0;
      int size = quorumSize == -1 ? quorumManager.getMaxClusterSize() : quorumSize;

      synchronized (voteGuard) {
         while (!stopped && voteAttempts++ < voteRetries) {
            //the live is dead so lets vote for quorum
            QuorumVoteServerConnect quorumVote = new QuorumVoteServerConnect(size, targetServerID);

            quorumManager.vote(quorumVote);

            try {
               quorumVote.await(LATCH_TIMEOUT, TimeUnit.SECONDS);
            } catch (InterruptedException interruption) {
               // No-op. The best the quorum can do now is to return the latest number it has
            }

            quorumManager.voteComplete(quorumVote);

            decision = quorumVote.getDecision();

            if (decision) {
               return decision;
            }
            try {
               voteGuard.wait(voteRetryWait);
            } catch (InterruptedException e) {
               //nothing to do here
            }
         }
      }

      return decision;
   }
}
