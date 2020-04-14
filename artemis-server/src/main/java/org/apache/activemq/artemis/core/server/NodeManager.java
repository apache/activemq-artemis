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
package org.apache.activemq.artemis.core.server;

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.utils.UUID;
import org.jboss.logging.Logger;

public abstract class NodeManager implements ActiveMQComponent {

   @FunctionalInterface
   public interface LockListener {

      void lostLock();
   }

   private static final Logger LOGGER = Logger.getLogger(NodeManager.class);
   protected final boolean replicatedBackup;
   protected final Object nodeIDGuard = new Object();
   private SimpleString nodeID;
   private UUID uuid;
   private boolean isStarted = false;
   private final Set<FileLockNodeManager.LockListener> lockListeners;

   public NodeManager(final boolean replicatedBackup) {
      this.replicatedBackup = replicatedBackup;
      this.lockListeners = new HashSet<>();
   }

   // --------------------------------------------------------------------

   public abstract void awaitLiveNode() throws NodeManagerException, InterruptedException;

   public abstract void awaitLiveStatus() throws NodeManagerException, InterruptedException;

   public abstract void startBackup() throws NodeManagerException, InterruptedException;

   public abstract ActivateCallback startLiveNode() throws NodeManagerException, InterruptedException;

   public abstract void pauseLiveServer() throws NodeManagerException;

   public abstract void crashLiveServer() throws NodeManagerException;

   public abstract void releaseBackup() throws NodeManagerException;

   // --------------------------------------------------------------------

   @Override
   public synchronized void start() throws Exception {
      isStarted = true;
   }

   @Override
   public boolean isStarted() {
      return isStarted;
   }

   public SimpleString getNodeId() {
      synchronized (nodeIDGuard) {
         return nodeID;
      }
   }

   public long readDataVersion() throws NodeManagerException {
      // TODO make it abstract
      throw new UnsupportedOperationException("TODO");
   }

   public void writeDataVersion(long version) throws NodeManagerException {
      // TODO make it abstract
      throw new UnsupportedOperationException("TODO");
   }

   public abstract SimpleString readNodeId() throws NodeManagerException;

   public UUID getUUID() {
      synchronized (nodeIDGuard) {
         return uuid;
      }
   }

   /**
    * Sets the nodeID.
    * <p>
    * Only used by replicating backups.
    *
    * @param nodeID
    */
   public void setNodeID(String nodeID) {
      synchronized (nodeIDGuard) {
         this.nodeID = new SimpleString(nodeID);
         this.uuid = new UUID(UUID.TYPE_TIME_BASED, UUID.stringToBytes(nodeID));
      }
   }

   /**
    * @param generateUUID
    */
   protected void setUUID(UUID generateUUID) {
      synchronized (nodeIDGuard) {
         uuid = generateUUID;
         nodeID = new SimpleString(uuid.toString());
      }
   }

   public abstract boolean isAwaitingFailback() throws NodeManagerException;

   public abstract boolean isBackupLive() throws NodeManagerException;

   public abstract void interrupt();

   @Override
   public synchronized void stop() throws Exception {
      // force any running threads on node manager to stop
      isStarted = false;
      lockListeners.clear();
   }

   public void stopBackup() throws NodeManagerException {
      releaseBackup();
   }

   protected synchronized void checkStarted() {
      if (!isStarted) {
         throw new IllegalStateException("the node manager is supposed to be started");
      }
   }

   protected synchronized void notifyLostLock() {
      if (!isStarted) {
         return;
      }
      lockListeners.forEach(lockListener -> {
         try {
            lockListener.lostLock();
         } catch (Exception e) {
            LOGGER.warn("On notify lost lock", e);
            // Need to notify everyone so ignore any exception
         }
      });
   }

   public synchronized void registerLockListener(FileLockNodeManager.LockListener lockListener) {
      lockListeners.add(lockListener);
   }

   public synchronized void unregisterLockListener(FileLockNodeManager.LockListener lockListener) {
      lockListeners.remove(lockListener);
   }

   public static final class NodeManagerException extends RuntimeException {

      public NodeManagerException(String message) {
         super(message);
      }

      public NodeManagerException(Throwable cause) {
         super(cause);
      }

      public NodeManagerException(String message, Throwable cause) {
         super(message, cause);
      }
   }
}
