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
package org.apache.activemq.artemis.quorum.db;

import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.UnavailableStateException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class DatabaseDistributedLock implements DistributedLock {
   private final CopyOnWriteArrayList<UnavailableLockListener> listeners;
   private final BaseDatabaseAdapter adapter;

   private final Connection c;
   private final String lockId;
   private final Consumer<String> onClosedLock;
   private final DatabasePrimitiveManager manager;
   private boolean closed;
   private boolean locked;
   private Object adapterLockContext;


   DatabaseDistributedLock(Consumer<String> onClosedLock, DatabasePrimitiveManager manager, BaseDatabaseAdapter adapter, String lockId) throws SQLException {
      this.onClosedLock = onClosedLock;
      this.manager = manager;
      this.adapter = adapter;
      this.lockId = lockId;
      this.closed = false;
      this.listeners = new CopyOnWriteArrayList<>();

      // Most databases bind the locks to the session/connection and release the lock if that session is disconnected.
      // Hold the connection at the lock level.
      c = adapter.getConnection();
   }


   // should be called from within a call to the adapter, which should already be synchronized on the manager
   public Connection getAssociatedConnection() {
      return c;
   }

   private void checkNotClosed() {
      if (closed) {
         throw new IllegalStateException("This lock is closed");
      }
   }

   public void handleLost() {
      for (UnavailableLockListener listener : listeners) {
         listener.onUnavailableLockEvent();
      }
   }

   @FunctionalInterface
   protected interface Action<R, T extends Throwable> {
      R call() throws T;
   }

   @FunctionalInterface
   protected interface InterruptableAction<R, T extends Throwable> {

      R call() throws InterruptedException, T;
   }

   protected final <R, T extends Throwable> R tryRun(InterruptableAction<R, T> action) throws InterruptedException, T {
      synchronized (manager) {
         checkNotClosed();
         return action.call();
      }
   }

   protected final <R, T extends Throwable> R run(Action<R, T> action) throws T {
      synchronized (manager) {
         checkNotClosed();
         return action.call();
      }
   }

   @Override
   public String getLockId() {
      return run(() -> {
         checkNotClosed();
         return lockId;
      });
   }

   @Override
   public boolean isHeldByCaller() {
      return run(() -> {
         checkNotClosed();
         return locked;
      });
   }

   @Override
   public boolean tryLock() throws UnavailableStateException, InterruptedException {
      return tryRun(() -> {
         checkNotClosed();
         if (locked)
            throw new IllegalStateException("Already have the lock");
         locked = adapter.tryLock(this);
         return locked;
      });
   }

   @Override
   public void unlock() {
      run(() -> {
         checkNotClosed();
         if (locked) {
            locked = false;
            adapter.releaseLock(this);
         }
         return null;
      });
   }

   @Override
   public void addListener(UnavailableLockListener listener) {
      run(() -> {
         listeners.add(listener);
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

   public boolean isClosed() {
      return run(() -> {
         return closed;
      });
   }

   public void close(boolean useCallback) {
      synchronized (manager) {
         if (closed) {
            return;
         }
         try {
            if (useCallback) {
               onClosedLock.accept(lockId);
            }
            unlock();
            adapter.close(this);
         } finally {
            closed = true;
         }
      }
   }

   @Override
   public void close() {
      close(true);
   }

   public long getLong() throws SQLException {
      return run(() -> {
         checkNotClosed();
         return adapter.getLong(this);
      });
   }

   public void setLong(long val) throws SQLException {
      run(() -> {
         checkNotClosed();
         adapter.setLong(this, val);
         return null;
      });
   }

   public Object getAdapterLockContext() {
      return adapterLockContext;
   }

   public void setAdapterLockContext(Object adapterLockContext) {
      this.adapterLockContext = adapterLockContext;
   }
}
