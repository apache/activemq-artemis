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
package org.apache.activemq.artemis.quorum.etcd;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.LockResponse;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.lease.PersistentLease;
import com.ibm.etcd.client.lock.LockClient;
import io.grpc.Deadline;
import io.grpc.Status;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.apache.activemq.artemis.quorum.etcd.EtcdDistributedPrimitiveManager.PrimitiveId;
import org.jboss.logging.Logger;


final class EtcdDistributedLock extends EtcdDistributedPrimitive implements DistributedLock {

   private static final Logger logger = Logger.getLogger(EtcdDistributedLock.class);

   private static final String ETCD_LOCK_KEYSPACE = "lock";

   private final CopyOnWriteArrayList<UnavailableLockListener> listeners;
   private final LockClient etcdLockClient;
   private final KvClient etcdKVClient;

   /** The prefix of the key used to hold the lock in ETCD */
   private final ByteString etcdKvLockKey;

   private final EtcdDistributedPrimitiveManager.Config config;

   enum Ids {
      /** fully qualified key name to hold the lock (concatenation of lockName + leaseId) */
      LOCKIDKV
   }
   private final ConcurrentMap<Ids, ByteString> ids = new ConcurrentHashMap<>(1);
   AtomicReference<Object> r = new AtomicReference<>(null);

   EtcdDistributedLock(PrimitiveId id, EtcdDistributedPrimitiveManager manager, EtcdDistributedPrimitiveManager.Config config) {
      super(id, manager);
      this.listeners = new CopyOnWriteArrayList<>();
      this.etcdLockClient = manager.getEtcd().getLockClient();
      this.etcdKVClient = manager.getEtcd().getKvClient();
      this.etcdKvLockKey = config.buildKeyPath(ETCD_LOCK_KEYSPACE, id.id);
      this.config = config;
   }

   @Override
   public String getLockId() {
      return getId().id;
   }

   private boolean lockExists() {
      return !ids.isEmpty(); // ids must be present
   }

   @Override
   public boolean tryLock() throws UnavailableStateException, InterruptedException {
      logger.infof("[%s] tryLock", this.manager.managerId);
      return tryRun(() -> {
         if (lockExists()) {
            throw new IllegalStateException("unlock first");
         }
         checkUnavailable();
         try {
            // lock with the lease:
            logger.infof("[%s] Acquire lock: %s", this.manager.managerId, etcdKvLockKey);
            final LockResponse lr = etcdLockClient
               .lock(etcdKvLockKey)
               //.timeout(this.etcdGrpcCallTimeout.toMillis()) // defaulted in EtcdClient
               .withLease(manager.getLease())
               .deadline(Deadline.after(this.config.getRequestDeadline().toMillis(), TimeUnit.MILLISECONDS))
               .sync();

            logger.infof("[%s] Lock acquired (lockname=%s, memberid=%s)", this.manager.managerId, lr.getKey().toStringUtf8(), getId().id);
            this.ids.put(Ids.LOCKIDKV, lr.getKey());
            return true;
         }
         catch (Throwable e) {
            final Status status = Status.fromThrowable(e);
            if (status.getCode() == Status.Code.DEADLINE_EXCEEDED) {
               logger.infof("[%s] Lock cannot be acquired (lockname=%s, memberid=%s", this.manager.managerId , this.etcdKvLockKey, getId().id);
               return false;
            }
            EtcdUtils.assertNoNestedInterruptedException(e);
            EtcdUtils.logGrpcError(logger, e, manager.managerId, "trying to acquire a lock (lockname=" + this.etcdKvLockKey + ", memberid=" + this.getId().id + ")");
            throw new UnavailableStateException(e);
         }
      });
   }

   @Override
   public void unlock() throws UnavailableStateException {
      run(() -> {
         checkUnavailable();
         if (this.manager.getLease() != null
            && !this.ids.isEmpty()
            && (this.manager.getLease().getState() == PersistentLease.LeaseState.ACTIVE
            ||  this.manager.getLease().getState() == PersistentLease.LeaseState.ACTIVE_NO_CONN)) {
            try {
               logger.infof("[%s] Unlock: %s, %s, %s, %s", this.manager.managerId, this.etcdKvLockKey,
                            this.ids.get(Ids.LOCKIDKV).toStringUtf8(), getId().id, this.manager.getLease().getLeaseId());
               this.etcdLockClient
                  .unlock(this.ids.get(Ids.LOCKIDKV))
                  .sync();
            }
            catch (Throwable e) {
               EtcdUtils.logGrpcError(logger, e, this.getId().id, "trying to release a lock ("+ this.etcdKvLockKey + ")");
               throw new UnavailableStateException(e);
            }
            finally {
               this.ids.remove(Ids.LOCKIDKV);
            }
         }
         else {
            logger.infof("[%s] unlock: cannot unlock due to failing pre-conditions", this.manager.managerId);
         }
         return null;
      });
   }

   @Override
   protected void handleReconnected() {
      super.handleReconnected();
      if (!(this.manager.getLease().getState() == PersistentLease.LeaseState.ACTIVE
         || this.manager.getLease().getState() == PersistentLease.LeaseState.ACTIVE_NO_CONN)) {
         logger.infof("[%s] handleReconnected: call onLost", this.manager.managerId);
         onLost();
      }
      // check the value associated to the lock and make sure it still belongs
      // to the current member:
      try {
         logger.infof( "[%s] %s, %s", this.manager.managerId, this.manager.getLease().getState().name(), ids.get(Ids.LOCKIDKV));
         final ByteString lock = ids.get(Ids.LOCKIDKV);
         if (lock == null) {
            // active but no lock acquired yet
            return;
         }
         if (!this.etcdKVClient
               .txnIf()
               .cmpEqual(lock)
               .lease(this.manager.getLease().getLeaseId())
               .exists(lock)
               .sync()
               .getSucceeded()) {
            // if null, it means that the remote lock does not exist or does not
            // belong to the current member:
            onLost();
         }
      }
      catch (Throwable e) {
         EtcdUtils.logGrpcError(logger, e, this.getId().id, "trying to check a lock ("+ this.etcdKvLockKey + ")");
         onLost();
      }
   }

   @Override
   protected void handleLost() {
      logger.infof("[%s] handleLost: Handle lost", this.manager.managerId);
      super.handleLost();
      this.ids.clear();
      for (UnavailableLockListener listener : listeners) {
         listener.onUnavailableLockEvent();
      }
   }

   @Override
   protected void handleSuspended() {
      logger.infof("[%s] handleSuspended: Handle suspended", this.manager.managerId);
      if (!this.ids.isEmpty()) {
         // in case it flipped from ACTIVE to CLOSE
         handleLost();
      }
   }

   @Override
   public boolean isHeldByCaller() throws UnavailableStateException {
      return run(() -> {
         checkUnavailable();
         if (!(this.manager.getLease().getState() == PersistentLease.LeaseState.ACTIVE
            || this.manager.getLease().getState() == PersistentLease.LeaseState.ACTIVE_NO_CONN)) {
            logger.infof("[%s] isHeldByCaller: not held by caller", this.manager.managerId);
            return false;
         }
         try {
            // check the value associated to the lock and make sure it still belongs
            // to the current member:
            final ByteString localNodeLockKV = ids.computeIfPresent(Ids.LOCKIDKV, (k, v) -> {
               boolean hasKey = this.etcdKVClient.txnIf().exists(v).sync().getSucceeded();
               return hasKey ? v : null;
            });
            // if null, it means that the remote lock does not exist or does not
            // belong to the current member:
            return !(localNodeLockKV == null);
         }
         catch (Throwable t) {
            EtcdUtils.logGrpcError(logger, t, this.getId().id, "trying to check lock ownership ("+ this.etcdKvLockKey + ")");
            throw new UnavailableStateException(t);
         }
      });
   }

   @Override
   public void addListener(UnavailableLockListener listener) {
      run(() -> {
         listeners.add(listener);
         fireUnavailableListener(listener::onUnavailableLockEvent);
         return null;
      });
   }

   @Override
   public void removeListener(UnavailableLockListener listener) {
      run(() -> {
         listeners.remove(listener);
         return null;
      });
   }

   @Override
   protected void handleClosed() {
      logger.infof("[%s] handleClosed: Handle closed", this.manager.managerId);
      super.handleClosed();
      listeners.clear();
      if (isUnavailable()) {
         return;
      }
      try {
         this.etcdLockClient.unlock(this.etcdKvLockKey);
      }
      catch (Throwable t) {
         EtcdUtils.logGrpcError(logger, t, this.getId().id, "trying to release a lock ("+ this.etcdKvLockKey + ")");
      }
   }

}
