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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.ArtemisCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageTimedWriter extends ActiveMQScheduledComponent {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final PagingStoreImpl pagingStore;

   private final StorageManager storageManager;

   protected final List<PageEvent> pageEvents = new ArrayList<>();

   protected volatile int pendingTasks = 0;

   protected final boolean syncNonTX;

   private final Semaphore writeCredits;

   private final int maxCredits;

   private static final AtomicIntegerFieldUpdater<PageTimedWriter> pendingTasksUpdater = AtomicIntegerFieldUpdater.newUpdater(PageTimedWriter.class, "pendingTasks");

   public boolean hasPendingIO() {
      return pendingTasksUpdater.get(this) > 0;
   }

   public static class PageEvent {

      PageEvent(OperationContext context, PagedMessage message, Transaction tx, RouteContextList listCtx, int credits, boolean replicated) {
         this.context = context;
         this.message = message;
         this.listCtx = listCtx;
         this.replicated = replicated;
         this.credits = credits;
         this.tx = tx;
      }

      final boolean replicated;
      final PagedMessage message;
      final OperationContext context;
      final RouteContextList listCtx;
      final Transaction tx;
      final int credits;
   }

   public PageTimedWriter(int writeCredits, StorageManager storageManager, PagingStoreImpl pagingStore, ScheduledExecutorService scheduledExecutor, Executor executor, boolean syncNonTX, long timeSync) {
      super(scheduledExecutor, executor, timeSync, TimeUnit.NANOSECONDS, true);
      this.pagingStore = pagingStore;
      this.storageManager = storageManager;
      this.syncNonTX = syncNonTX;
      this.writeCredits = new Semaphore(writeCredits);
      this.maxCredits = writeCredits;
   }

   public int getMaxCredits() {
      return maxCredits;
   }

   @Override
   public synchronized void stop() {
      super.stop();
      processMessages();
   }

   /**
    * We increment task while holding the readLock. This is because we verify if the system is paging, and we get out of
    * paging when no pending tasks and no pending messages. We allocate a task while holding the read Lock. We cannot
    * call addTask within the lock as if the semaphore gets out of credits we would deadlock in certain cases.
    */
   public void incrementTask() {
      pendingTasksUpdater.incrementAndGet(this);
   }

   public void addTask(OperationContext context,
                                    PagedMessage message,
                                    Transaction tx,
                                    RouteContextList listCtx) {

      if (!isStarted()) {
         throw new IllegalStateException("PageWriter Service is stopped");
      }
      int credits = Math.min(message.getEncodeSize() + PageReadWriter.SIZE_RECORD, maxCredits);
      writeCredits.acquireUninterruptibly(credits);
      synchronized (this) {
         final boolean replicated = storageManager.isReplicated();
         PageEvent event = new PageEvent(context, message, tx, listCtx, credits, replicated);
         context.storeLineUp();
         if (replicated) {
            context.replicationLineUp();
         }
         this.pageEvents.add(event);
         delay();
      }

   }

   private synchronized  PageEvent[] extractPendingEvents() {
      if (pageEvents.isEmpty()) {
         return null;
      }
      PageEvent[] pendingsWrites = new PageEvent[pageEvents.size()];
      pendingsWrites = pageEvents.toArray(pendingsWrites);
      pageEvents.clear();
      return pendingsWrites;
   }

   @Override
   public void run() {
      ArtemisCloseable closeable = storageManager.closeableReadLock(true);
      if (closeable == null) {
         logger.trace("Delaying PagedTimedWriter as it's currently locked");
         delay();
      } else {
         try {
            processMessages();
         } finally {
            closeable.close();
         }
      }
   }

   protected void processMessages() {
      PageEvent[] pendingEvents = extractPendingEvents();
      if (pendingEvents == null) {
         return;
      }
      OperationContext beforeContext = OperationContextImpl.getContext();

      try {
         boolean requireSync = false;
         for (PageEvent event : pendingEvents) {
            OperationContextImpl.setContext(event.context);
            pagingStore.directWritePage(event.message, false, event.replicated);

            if (event.tx != null || syncNonTX) {
               requireSync = true;
            }
         }
         if (requireSync) {
            performSync();
         }

      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         // In case of failure, The context should propagate an exception to the client
         // We send an exception to the client even on the case of a failure
         // to avoid possible locks and the client not getting the exception back
         for (PageEvent event : pendingEvents) {
            event.context.onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getClass() + " during ioSync for paging on " + pagingStore.getStoreName() + ": " + e.getMessage());
         }
      } finally {
         for (PageEvent event : pendingEvents) {
            event.context.done();
            pendingTasksUpdater.decrementAndGet(this);
            writeCredits.release(event.credits);
         }
         OperationContextImpl.setContext(beforeContext);
      }
   }

   protected void performSync() throws Exception {
      pagingStore.ioSync();
   }
}
