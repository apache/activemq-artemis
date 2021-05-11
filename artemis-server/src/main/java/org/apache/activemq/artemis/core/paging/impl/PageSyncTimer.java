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
package org.apache.activemq.artemis.core.paging.impl;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;

/**
 * This will batch multiple calls waiting to perform a sync in a single call.
 */
final class PageSyncTimer extends ActiveMQScheduledComponent {

   private final PagingStore store;

   private final Queue<OperationContext> pendingSyncOperations;

   private final Queue<OperationContext> syncedOperations;

   PageSyncTimer(PagingStore store, ScheduledExecutorService scheduledExecutor, Executor executor, long timeSync) {
      super(scheduledExecutor, executor, timeSync, TimeUnit.NANOSECONDS, true);
      this.store = store;
      this.pendingSyncOperations = PlatformDependent.newMpscQueue();
      this.syncedOperations = new ArrayDeque<>();
   }

   void addSync(OperationContext ctx) {
      ctx.pageSyncLineUp();
      pendingSyncOperations.add(ctx);
      delay();
   }

   @Override
   public void run() {
      tick();
   }

   private boolean prepareSyncOperations() {
      assert syncedOperations.isEmpty();
      boolean ioSync = false;
      while (true) {
         final OperationContext ctx = pendingSyncOperations.poll();
         if (ctx == null) {
            return ioSync;
         }
         ioSync = true;
         syncedOperations.add(ctx);
      }
   }

   private synchronized void tick() {
      if (!prepareSyncOperations()) {
         return;
      }
      assert !syncedOperations.isEmpty();
      try {
         store.ioSync();
      } catch (Exception e) {
         for (int i = 0, size = syncedOperations.size(); i < size; i++) {
            syncedOperations.poll().onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getMessage());
         }
      } finally {
         // In case of failure, The context should propagate an exception to the client
         // We send an exception to the client even on the case of a failure
         // to avoid possible locks and the client not getting the exception back
         for (int i = 0, size = syncedOperations.size(); i < size; i++) {
            syncedOperations.poll().pageSyncDone();
         }
         assert syncedOperations.isEmpty();
      }
   }
}
