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

package org.apache.activemq.artemis.core.server.impl.quorum;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.MutableLong;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.jboss.logging.Logger;

/**
 * This class contains the activation sequence logic of the pluggable quorum vote:
 * it should be used by {@link org.apache.activemq.artemis.core.server.impl.ReplicationBackupActivation}
 * and {@link org.apache.activemq.artemis.core.server.impl.ReplicationPrimaryActivation} to coordinate
 * for replication.
 */
public final class ActivationSequenceStateMachine {

   private static final long CHECK_ACTIVATION_SEQUENCE_WAIT_MILLIS = 200;
   private static final long CHECK_REPAIRED_ACTIVATION_SEQUENCE_WAIT_MILLIS = 2000;
   private static final long LIVE_LOCK_ACQUIRE_TIMEOUT_MILLIS = 2000;

   private ActivationSequenceStateMachine() {

   }

   /**
    * It loops if the data of the broker is still valuable, but cannot become live.
    * It loops (temporarly) if data is in sync or can self-heal, but cannot yet acquire the live lock.
    * <p>
    * It stops loop and return:
    * <p><ul>
    * <li>{@code null}: if data is stale (and there are no rights to become live)
    * <li>{@code !=null}: if data is in sync and the {@link DistributedLock} is correctly acquired
    * </ul><p>
    * <p>
    * After successfully returning from this method ie not null return value, a broker should use
    * {@link #ensureSequentialAccessToNodeData(ActiveMQServer, DistributedPrimitiveManager, Logger)} to complete
    * the activation and guarantee the initial not-replicated ownership of data.
    */
   public static DistributedLock tryActivate(final String nodeId,
                                             final long nodeActivationSequence,
                                             final DistributedPrimitiveManager distributedManager,
                                             final Logger logger) throws InterruptedException, ExecutionException, TimeoutException, UnavailableStateException {
      final DistributedLock activationLock = distributedManager.getDistributedLock(nodeId);
      try (MutableLong coordinatedNodeSequence = distributedManager.getMutableLong(nodeId)) {
         while (true) {
            // dirty read is sufficient to know if we are *not* an in sync replica
            // typically the lock owner will increment to signal our data is stale and we are happy without any
            // further coordination at this point
            switch (validateActivationSequence(coordinatedNodeSequence, activationLock, nodeId, nodeActivationSequence, logger)) {

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
                     logger.warnf("Cannot assume live role for NodeID = %s: claimed activation sequence need to be repaired",
                                  nodeId);
                     TimeUnit.MILLISECONDS.sleep(CHECK_REPAIRED_ACTIVATION_SEQUENCE_WAIT_MILLIS);
                     continue;
                  }
                  // quick path while data is still valuable: wait until something change (commit/repair)
                  TimeUnit.MILLISECONDS.sleep(CHECK_ACTIVATION_SEQUENCE_WAIT_MILLIS);
                  continue;
            }
            // SelfRepair, InSync
            if (!activationLock.tryLock(LIVE_LOCK_ACQUIRE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
               logger.debugf("Candidate for Node ID = %s, with local activation sequence: %d, cannot acquire live lock within %dms; retrying",
                             nodeId, nodeActivationSequence, LIVE_LOCK_ACQUIRE_TIMEOUT_MILLIS);
               continue;
            }
            switch (validateActivationSequence(coordinatedNodeSequence, activationLock, nodeId, nodeActivationSequence, logger)) {

               case Stale:
                  activationLock.close();
                  return null;
               case SelfRepair:
                  // Self-repair sequence ie we were the only one with the most up to date data.
                  // NOTE: We cannot move the sequence now, let's delay it on ensureSequentialAccessToNodeData
                  logger.infof("Assuming live role for NodeID = %s: local activation sequence %d matches claimed coordinated activation sequence %d. Repairing sequence", nodeId, nodeActivationSequence, nodeActivationSequence);
                  return activationLock;
               case InSync:
                  // we are an in_sync_replica, good to go live as UNREPLICATED
                  logger.infof("Assuming live role for NodeID = %s, local activation sequence %d matches current coordinated activation sequence %d", nodeId, nodeActivationSequence, nodeActivationSequence);
                  return activationLock;
               case MaybeInSync:
                  activationLock.unlock();
                  logger.warnf("Cannot assume live role for NodeID = %s: claimed activation sequence need to be repaired", nodeId);
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

   private static ValidationResult validateActivationSequence(final MutableLong coordinatedNodeSequence,
                                                              final DistributedLock activationLock,
                                                              final String lockAndLongId,
                                                              final long nodeActivationSequence,
                                                              final Logger logger) throws UnavailableStateException {
      assert coordinatedNodeSequence.getMutableLongId().equals(lockAndLongId);
      assert activationLock.getLockId().equals(lockAndLongId);
      final long currentCoordinatedNodeSequence = coordinatedNodeSequence.get();
      if (nodeActivationSequence == currentCoordinatedNodeSequence) {
         return ValidationResult.InSync;
      }
      if (currentCoordinatedNodeSequence > 0) {
         logger.infof("Not a candidate for NodeID = %s activation, local activation sequence %d does not match coordinated activation sequence %d",
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
      logger.infof("Not a candidate for NodeID = %s activation, local activation sequence %d does not match coordinated activation sequence %d",
                   lockAndLongId, nodeActivationSequence, claimedCoordinatedNodeSequence);
      return ValidationResult.Stale;
   }

   /**
    * It wait until {@code timeoutMillis ms} has passed or the coordinated activation sequence has progressed enough
    */
   public static boolean awaitNextCommittedActivationSequence(final DistributedPrimitiveManager distributedManager,
                                                              final String coordinatedLockAndNodeId,
                                                              final long activationSequence,
                                                              final long timeoutMills,
                                                              final Logger logger)
                           throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      Objects.requireNonNull(distributedManager);
      Objects.requireNonNull(logger);
      Objects.requireNonNull(coordinatedLockAndNodeId);
      if (activationSequence < 0) {
         throw new IllegalArgumentException("activationSequence must be >= 0, while is " + activationSequence);
      }
      if (!distributedManager.isStarted()) {
         throw new IllegalStateException("manager must be started");
      }
      final MutableLong coordinatedActivationSequence = distributedManager.getMutableLong(coordinatedLockAndNodeId);
      // wait for the live to activate and run un replicated with a sequence > inSyncReplicaActivation
      // this read can be dirty b/c we are just looking for an increment.
      boolean anyNext = false;
      final long timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMills);
      final long started = System.nanoTime();
      long elapsedNs;
      do {
         final long coordinatedValue = coordinatedActivationSequence.get();
         if (coordinatedValue > activationSequence) {
            // all good, some activation has gone ahead
            logger.infof("Detected a new activation sequence with NodeID = %s: and sequence: %d", coordinatedLockAndNodeId, coordinatedValue);
            anyNext = true;
            break;
         }
         if (coordinatedValue < 0) {
            // commit claim
            final long claimedSequence = -coordinatedValue;
            final long activationsGap = claimedSequence - activationSequence;
            if (activationsGap > 1) {
               // all good, some activation has gone ahead
               logger.infof("Detected furthers sequential server activations from sequence %d, with NodeID = %s: and claimed sequence: %d", activationSequence, coordinatedLockAndNodeId, claimedSequence);
               anyNext = true;
               break;
            }
            // activation is still in progress
            logger.debugf("Detected claiming of activation sequence = %d for NodeID = %s", claimedSequence, coordinatedLockAndNodeId);
         }
         try {
            TimeUnit.MILLISECONDS.sleep(CHECK_ACTIVATION_SEQUENCE_WAIT_MILLIS);
         } catch (InterruptedException ignored) {
         }
         elapsedNs = System.nanoTime() - started;
      }
      while (elapsedNs < timeoutNs);
      return anyNext;
   }

   /**
    * This is going to increment the coordinated activation sequence while holding the live lock, failing with some exception otherwise.<br>
    * <p>
    * The acceptable states are {@link ValidationResult#InSync} and {@link ValidationResult#SelfRepair}, throwing some exception otherwise.
    * <p>
    * This must be used while holding a live lock to ensure not-exclusive ownership of data ie can be both used
    * while loosing connectivity with a replica or after successfully {@link #tryActivate(String, long, DistributedPrimitiveManager, Logger)}.
    */
   public static void ensureSequentialAccessToNodeData(ActiveMQServer activeMQServer,
                                                       DistributedPrimitiveManager distributedPrimitiveManager,
                                                       final Logger logger) throws ActiveMQException, InterruptedException, UnavailableStateException, ExecutionException, TimeoutException {

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
      } else {
         // self-heal need to update the in-memory sequence, because no writes will do it
         nodeManager.setNodeActivationSequence(nextActivationSequence);
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

}
