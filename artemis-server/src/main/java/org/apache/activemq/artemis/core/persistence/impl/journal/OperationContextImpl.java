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
package org.apache.activemq.artemis.core.persistence.impl.journal;

import java.util.LinkedList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.journal.impl.SimpleWaitIOCallback;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.utils.ExecutorFactory;

/**
 * Each instance of OperationContextImpl is associated with an executor (usually an ordered Executor).
 *
 * Tasks are hold until the operations are complete and executed in the natural order as soon as the operations are returned
 * from replication and storage.
 *
 * If there are no pending IO operations, the tasks are just executed at the callers thread without any context switch.
 *
 * So, if you are doing operations that are not dependent on IO (e.g NonPersistentMessages) you wouldn't have any context switch.
 */
public class OperationContextImpl implements OperationContext {

   private static final ThreadLocal<OperationContext> threadLocalContext = new ThreadLocal<>();

   public static void clearContext() {
      OperationContextImpl.threadLocalContext.set(null);
   }

   public static final OperationContext getContext() {
      return getContext(null);
   }

   public static OperationContext getContext(final ExecutorFactory executorFactory) {
      OperationContext token = OperationContextImpl.threadLocalContext.get();
      if (token == null) {
         if (executorFactory == null) {
            return null;
         } else {
            token = new OperationContextImpl(executorFactory.getExecutor());
            OperationContextImpl.threadLocalContext.set(token);
         }
      }
      return token;
   }

   public static void setContext(final OperationContext context) {
      OperationContextImpl.threadLocalContext.set(context);
   }

   LinkedList<TaskHolder> tasks;
   LinkedList<StoreOnlyTaskHolder> storeOnlyTasks;

   long minimalStore = Long.MAX_VALUE;
   long minimalReplicated = Long.MAX_VALUE;
   long minimalPage = Long.MAX_VALUE;


   static final AtomicIntegerFieldUpdater<OperationContextImpl> EXECUTORS_PENDING_UPDATER = AtomicIntegerFieldUpdater
      .newUpdater(OperationContextImpl.class, "executorsPendingField");

   static final AtomicLongFieldUpdater<OperationContextImpl> STORE_LINEUP_UPDATER = AtomicLongFieldUpdater
      .newUpdater(OperationContextImpl.class, "storeLineUpField");

   static final AtomicLongFieldUpdater<OperationContextImpl> REPLICATION_LINEUP_UPDATER = AtomicLongFieldUpdater
      .newUpdater(OperationContextImpl.class, "replicationLineUpField");

   static final AtomicLongFieldUpdater<OperationContextImpl> PAGE_LINEUP_UPDATER = AtomicLongFieldUpdater
      .newUpdater(OperationContextImpl.class, "pageLineUpField");


   volatile int executorsPendingField = 0;
   volatile long storeLineUpField = 0;
   volatile long replicationLineUpField = 0;
   volatile long pageLineUpField = 0;

   long stored = 0;
   long replicated = 0;
   long paged = 0;

   private int errorCode = -1;

   private String errorMessage = null;

   private final Executor executor;

   public OperationContextImpl(final Executor executor) {
      super();
      this.executor = executor;
   }

   @Override
   public void pageSyncLineUp() {
      PAGE_LINEUP_UPDATER.incrementAndGet(this);
   }

   @Override
   public synchronized void pageSyncDone() {
      paged++;
      checkTasks();
   }

   @Override
   public void storeLineUp() {
      STORE_LINEUP_UPDATER.incrementAndGet(this);
   }

   @Override
   public void replicationLineUp() {
      REPLICATION_LINEUP_UPDATER.incrementAndGet(this);
   }

   @Override
   public synchronized void replicationDone() {
      replicated++;
      checkTasks();
   }

   @Override
   public void executeOnCompletion(IOCallback runnable) {
      executeOnCompletion(runnable, false);
   }

   @Override
   public void executeOnCompletion(final IOCallback completion, final boolean storeOnly) {
      boolean executeNow = false;

      synchronized (this) {
         if (errorCode == -1) {
            final long storeLined = STORE_LINEUP_UPDATER.get(this);
            final long pageLined = PAGE_LINEUP_UPDATER.get(this);
            final long replicationLined = REPLICATION_LINEUP_UPDATER.get(this);
            if (storeOnly) {
               if (storeOnlyTasks == null) {
                  storeOnlyTasks = new LinkedList<>();
               }
            } else {
               if (tasks == null) {
                  tasks = new LinkedList<>();
                  minimalReplicated = replicationLined;
                  minimalStore = storeLined;
                  minimalPage = pageLined;
               }
            }
            // On this case, we can just execute the context directly

            if (replicationLined == replicated && storeLined == stored && pageLined == paged) {
               // We want to avoid the executor if everything is complete...
               // However, we can't execute the context if there are executions pending
               // We need to use the executor on this case
               if (EXECUTORS_PENDING_UPDATER.get(this) == 0) {
                  // No need to use an executor here or a context switch
                  // there are no actions pending.. hence we can just execute the task directly on the same thread
                  executeNow = true;
               } else {
                  execute(completion);
               }
            } else {
               if (storeOnly) {
                  assert !storeOnlyTasks.isEmpty() ? storeOnlyTasks.peekLast().storeLined <= storeLined : true;
                  storeOnlyTasks.add(new StoreOnlyTaskHolder(completion, storeLined));
               } else {
                  // ensure total ordering
                  assert validateTasksAdd(storeLined, replicationLined, pageLined);
                  tasks.add(new TaskHolder(completion, storeLined, replicationLined, pageLined));
               }
            }
         }
      }

      // Executing outside of any locks
      if (errorCode != -1) {
         completion.onError(errorCode, errorMessage);
      } else if (executeNow) {
         completion.done();
      }

   }

   private boolean validateTasksAdd(long storeLined, long replicationLined, long pageLined) {
      if (tasks.isEmpty()) {
         return true;
      }
      final TaskHolder holder = tasks.peekLast();
      if (holder.storeLined > storeLined ||
         holder.replicationLined > replicationLined ||
         holder.pageLined > pageLined) {
         return false;
      }
      return true;
   }

   @Override
   public synchronized void done() {
      stored++;
      checkTasks();
   }

   private void checkStoreTasks() {
      final LinkedList<StoreOnlyTaskHolder> storeOnlyTasks = this.storeOnlyTasks;
      assert storeOnlyTasks != null;
      final int size = storeOnlyTasks.size();
      if (size == 0) {
         return;
      }
      final long stored = this.stored;
      for (int i = 0; i < size; i++) {
         final StoreOnlyTaskHolder holder = storeOnlyTasks.peek();
         if (stored < holder.storeLined) {
            // fail fast: storeOnlyTasks are ordered by storeLined, there is no need to continue
            return;
         }
         // If set, we use an executor to avoid the server being single threaded
         execute(holder.task);
         final StoreOnlyTaskHolder removed = storeOnlyTasks.poll();
         assert removed == holder;
      }
   }

   private void checkCompleteContext() {
      final LinkedList<TaskHolder> tasks = this.tasks;
      assert tasks != null;
      final int size = this.tasks.size();
      if (size == 0) {
         return;
      }
      assert size >= 1;
      // no need to use an iterator here, we can save that cost
      for (int i = 0; i < size; i++) {
         final TaskHolder holder = tasks.peek();
         if (stored < holder.storeLined || replicated < holder.replicationLined || paged < holder.pageLined) {
            // End of list here. No other task will be completed after this
            return;
         }
         execute(holder.task);
         final TaskHolder removed = tasks.poll();
         assert removed == holder;
      }
   }

   private void checkTasks() {

      if (storeOnlyTasks != null) {
         checkStoreTasks();
      }

      if (stored >= minimalStore && replicated >= minimalReplicated && paged >= minimalPage) {
         checkCompleteContext();
      }
   }

   /**
    * @param task
    */
   private void execute(final IOCallback task) {
      EXECUTORS_PENDING_UPDATER.incrementAndGet(this);
      try {
         executor.execute(new Runnable() {
            @Override
            public void run() {
               try {
                  // If any IO is done inside the callback, it needs to be done on a new context
                  OperationContextImpl.clearContext();
                  task.done();
               } finally {
                  EXECUTORS_PENDING_UPDATER.decrementAndGet(OperationContextImpl.this);
               }
            }
         });
      } catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.errorExecutingAIOCallback(e);
         EXECUTORS_PENDING_UPDATER.decrementAndGet(this);
         task.onError(ActiveMQExceptionType.INTERNAL_ERROR.getCode(), "It wasn't possible to complete IO operation - " + e.getMessage());
      }
   }

   /*
    * (non-Javadoc)
    * @see org.apache.activemq.artemis.core.replication.ReplicationToken#complete()
    */
   public void complete() {
   }

   @Override
   public synchronized void onError(final int errorCode, final String errorMessage) {
      this.errorCode = errorCode;
      this.errorMessage = errorMessage;

      if (tasks != null) {
         // it's saving the Iterator allocation cost
         final int size = tasks.size();
         for (int i = 0; i < size; i++) {
            final TaskHolder holder = tasks.poll();
            holder.task.onError(errorCode, errorMessage);
         }
      }
   }

   static final class TaskHolder {

      @Override
      public String toString() {
         return "TaskHolder [storeLined=" + storeLined +
            ", replicationLined=" +
            replicationLined +
            ", pageLined=" +
            pageLined +
            ", task=" +
            task +
            "]";
      }

      long storeLined;
      long replicationLined;
      long pageLined;

      final IOCallback task;

      TaskHolder(final IOCallback task, long storeLined, long replicationLined, long pageLined) {
         this.storeLined = storeLined;
         this.replicationLined = replicationLined;
         this.pageLined = pageLined;
         this.task = task;
      }
   }

   /**
    * This class has been created to both better capture the intention that the {@link IOCallback} is related to a
    * store-only operation and to reduce the memory footprint for store-only cases, given that many fields of
    * {@link TaskHolder} are not necessary for this to work. Inheritance proved to not as effective especially without
    * COOPS and with a 64 bit JVM so we've used different classes.
    */
   static final class StoreOnlyTaskHolder {

      @Override
      public String toString() {
         return "StoreOnlyTaskHolder [storeLined=" + storeLined + ", task=" + task + "]";
      }

      long storeLined;
      final IOCallback task;

      StoreOnlyTaskHolder(final IOCallback task, long storeLined) {
         this.storeLined = storeLined;
         this.task = task;
      }
   }

   @Override
   public void waitCompletion() throws Exception {
      waitCompletion(0);
   }

   @Override
   public boolean waitCompletion(final long timeout) throws InterruptedException, ActiveMQException {
      SimpleWaitIOCallback waitCallback = new SimpleWaitIOCallback();
      executeOnCompletion(waitCallback);
      complete();
      if (timeout == 0) {
         waitCallback.waitCompletion();
         return true;
      } else {
         return waitCallback.waitCompletion(timeout);
      }
   }

   @Override
   public String toString() {
      return "OperationContextImpl [" + hashCode() + "] [minimalStore=" + minimalStore +
         ", storeLineUp=" +
         storeLineUpField +
         ", stored=" +
         stored +
         ", minimalReplicated=" +
         minimalReplicated +
         ", replicationLineUp=" +
         replicationLineUpField +
         ", replicated=" +
         replicated +
         ", paged=" +
         paged +
         ", minimalPage=" +
         minimalPage +
         ", pageLineUp=" +
         pageLineUpField +
         ", errorCode=" +
         errorCode +
         ", errorMessage=" +
         errorMessage +
         ", executorsPending=" +
         executorsPendingField +
         "]";
   }
}