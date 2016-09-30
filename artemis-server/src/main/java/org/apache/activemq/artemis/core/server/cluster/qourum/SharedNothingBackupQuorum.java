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
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.client.SessionFailureListener;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.Topology;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.NodeManager;

public class SharedNothingBackupQuorum implements Quorum, SessionFailureListener {

   public enum BACKUP_ACTIVATION {
      FAIL_OVER, FAILURE_REPLICATING, ALREADY_REPLICATING, STOP;
   }

   private QuorumManager quorumManager;

   private String targetServerID = "";

   private final NodeManager nodeManager;

   private final StorageManager storageManager;
   private final ScheduledExecutorService scheduledPool;

   private CountDownLatch latch;

   private ClientSessionFactoryInternal sessionFactory;

   private CoreRemotingConnection connection;

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
                                    ScheduledExecutorService scheduledPool) {
      this.storageManager = storageManager;
      this.scheduledPool = scheduledPool;
      this.latch = new CountDownLatch(1);
      this.nodeManager = nodeManager;
   }

   private volatile BACKUP_ACTIVATION signal;

   /**
    * safety parameter to make _sure_ we get out of await()
    */
   private static final int LATCH_TIMEOUT = 30;

   private static final int RECONNECT_ATTEMPTS = 5;

   private final Object decisionGuard = new Object();

   @Override
   public String getName() {
      return "SharedNothingBackupQuorum";
   }

   public void decideOnAction(Topology topology) {
      //we may get called via multiple paths so need to guard
      synchronized (decisionGuard) {
         if (signal == BACKUP_ACTIVATION.FAIL_OVER) {
            return;
         }
         if (!isLiveDown()) {
            try {
               // no point in repeating all the reconnection logic
               sessionFactory.connect(RECONNECT_ATTEMPTS, false);
               return;
            } catch (ActiveMQException e) {
               if (e.getType() != ActiveMQExceptionType.NOT_CONNECTED)
                  ActiveMQServerLogger.LOGGER.errorReConnecting(e);
            }
         }
         // live is assumed to be down, backup fails-over
         signal = BACKUP_ACTIVATION.FAIL_OVER;
      }
      latch.countDown();
   }

   public void liveIDSet(String liveID) {
      targetServerID = liveID;
      nodeManager.setNodeID(liveID);
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
      // we use 1 less than the max cluste size as we arent bothered about the replicated live node
      int size = quorumManager.getMaxClusterSize();

      QuorumVoteServerConnect quorumVote = new QuorumVoteServerConnect(size, storageManager);

      quorumManager.vote(quorumVote);

      try {
         quorumVote.await(LATCH_TIMEOUT, TimeUnit.SECONDS);
      } catch (InterruptedException interruption) {
         // No-op. The best the quorum can do now is to return the latest number it has
      }

      quorumManager.voteComplete(quorumVote);

      return quorumVote.getDecision();
   }
}
