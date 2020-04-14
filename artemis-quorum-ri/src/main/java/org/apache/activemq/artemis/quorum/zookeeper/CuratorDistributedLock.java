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
package org.apache.activemq.artemis.quorum.zookeeper;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;

final class CuratorDistributedLock implements DistributedLock {

   // this is used to prevent deadlocks on close
   private final CuratorDistributedPrimitiveManager manager;
   private final String lockId;
   private final InterProcessSemaphoreV2 ipcSem;
   private final Consumer<CuratorDistributedLock> onClose;
   private final CopyOnWriteArrayList<UnavailableLockListener> listeners;
   private Lease lease;
   private boolean unavailable;
   private boolean closed;
   private byte[] leaseVersion;

   CuratorDistributedLock(CuratorDistributedPrimitiveManager manager,
                          String lockId,
                          InterProcessSemaphoreV2 ipcSem,
                          Consumer<CuratorDistributedLock> onClose) {
      this.manager = manager;
      this.lockId = lockId;
      this.ipcSem = ipcSem;
      this.onClose = onClose;
      this.listeners = new CopyOnWriteArrayList<>();
      this.closed = false;
      this.unavailable = false;
      this.leaseVersion = null;
   }

   protected void onReconnected() {
      synchronized (manager) {
         if (closed || unavailable) {
            return;
         }
         if (leaseVersion != null) {
            assert lease != null;
            try {
               if (Arrays.equals(lease.getData(), leaseVersion)) {
                  return;
               }
               onLost();
            } catch (Exception e) {
               onLost();
            }
         }
      }
   }

   protected void onLost() {
      synchronized (manager) {
         if (closed || unavailable) {
            return;
         }
         lease = null;
         leaseVersion = null;
         unavailable = true;
         for (UnavailableLockListener listener : listeners) {
            listener.onUnavailableLockEvent();
         }
      }
   }

   protected void onSuspended() {
      synchronized (manager) {
         if (closed || unavailable) {
            return;
         }
      }
   }

   @Override
   public String getLockId() {
      return lockId;
   }

   private void checkNotClosed() {
      if (closed) {
         throw new IllegalStateException("This lock is closed");
      }
   }

   @Override
   public boolean isHeldByCaller() throws UnavailableStateException {
      synchronized (manager) {
         manager.checkHandlingEvents();
         checkNotClosed();
         if (unavailable) {
            throw new UnavailableStateException(lockId + " lock state isn't available");
         }
         if (lease == null) {
            return false;
         }
         assert leaseVersion != null;
         try {
            return Arrays.equals(lease.getData(), leaseVersion);
         } catch (Throwable t) {
            throw new UnavailableStateException(t);
         }
      }
   }

   @Override
   public boolean tryLock() throws UnavailableStateException, InterruptedException {
      synchronized (manager) {
         manager.checkHandlingEvents();
         checkNotClosed();
         if (lease != null) {
            throw new IllegalStateException("unlock first");
         }
         if (unavailable) {
            throw new UnavailableStateException(lockId + " lock state isn't available");
         }
         try {
            final byte[] leaseVersion = UUID.randomUUID().toString().getBytes();
            ipcSem.setNodeData(leaseVersion);
            lease = ipcSem.acquire(0, TimeUnit.NANOSECONDS);
            if (lease == null) {
               ipcSem.setNodeData(null);
               return false;
            }
            this.leaseVersion = leaseVersion;
            assert Arrays.equals(lease.getData(), leaseVersion);
            return true;
         } catch (InterruptedException ie) {
            throw ie;
         } catch (Throwable e) {
            throw new UnavailableStateException(e);
         }
      }
   }

   @Override
   public void unlock() throws UnavailableStateException {
      synchronized (manager) {
         manager.checkHandlingEvents();
         checkNotClosed();
         if (unavailable) {
            throw new UnavailableStateException(lockId + " lock state isn't available");
         }
         final Lease lease = this.lease;
         if (lease != null) {
            this.lease = null;
            this.leaseVersion = null;
            try {
               ipcSem.returnLease(lease);
            } catch (Throwable e) {
               throw new UnavailableStateException(e);
            }
         }
      }
   }

   @Override
   public void addListener(UnavailableLockListener listener) {
      synchronized (manager) {
         manager.checkHandlingEvents();
         checkNotClosed();
         listeners.add(listener);
         if (unavailable) {
            manager.startHandlingEvents();
            try {
               listener.onUnavailableLockEvent();
            } finally {
               manager.completeHandlingEvents();
            }
         }
      }
   }

   @Override
   public void removeListener(UnavailableLockListener listener) {
      synchronized (manager) {
         manager.checkHandlingEvents();
         checkNotClosed();
         listeners.remove(listener);
      }
   }

   public void close(boolean useCallback) {
      synchronized (manager) {
         manager.checkHandlingEvents();
         if (closed) {
            return;
         }
         closed = true;
         listeners.clear();
         if (useCallback) {
            onClose.accept(this);
         }
         final Lease lease = this.lease;
         if (lease == null) {
            return;
         }
         this.lease = null;
         if (unavailable) {
            return;
         }
         try {
            ipcSem.returnLease(lease);
         } catch (Throwable t) {
            // TODO silent, but debug ;)
         }
      }
   }

   @Override
   public void close() {
      close(true);
   }
}
