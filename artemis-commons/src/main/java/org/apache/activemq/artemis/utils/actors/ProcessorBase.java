/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.utils.actors;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.jboss.logging.Logger;

public abstract class ProcessorBase<T> extends HandlerBase {

   private static final Logger logger = Logger.getLogger(ProcessorBase.class);
   public static final int STATE_NOT_RUNNING = 0;
   public static final int STATE_RUNNING = 1;
   public static final int STATE_FORCED_SHUTDOWN = 2;

   protected final Queue<T> tasks = new ConcurrentLinkedQueue<>();

   private final Executor delegate;
   /**
    * Using a method reference instead of an inner classes allows the caller to reduce the pointer chasing
    * when accessing ProcessorBase.this fields/methods.
    */
   private final Runnable task = this::executePendingTasks;

   // used by stateUpdater
   @SuppressWarnings("unused")
   private volatile int state = STATE_NOT_RUNNING;
   // Request of forced shutdown
   private volatile boolean requestedForcedShutdown = false;
   // Request of educated shutdown:
   private volatile boolean requestedShutdown = false;

   private static final AtomicIntegerFieldUpdater<ProcessorBase> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(ProcessorBase.class, "state");

   private void executePendingTasks() {
      do {
         //if there is no thread active and is not already dead then we run
         if (stateUpdater.compareAndSet(this, STATE_NOT_RUNNING, STATE_RUNNING)) {
            enter();
            try {
               T task;
               //while the queue is not empty we process in order:
               //if requestedForcedShutdown==true than no new tasks will be drained from the tasks q.
               while (!requestedForcedShutdown && (task = tasks.poll()) != null) {
                  doTask(task);
               }
            } finally {
               leave();
               //set state back to not running if possible: shutdownNow could be called by doTask(task).
               //If a shutdown has happened there is no need to continue polling tasks
               if (!stateUpdater.compareAndSet(this, STATE_RUNNING, STATE_NOT_RUNNING)) {
                  return;
               }
            }
         } else {
            return;
         }
         //we loop again based on tasks not being empty. Otherwise there is a window where the state is running,
         //but poll() has returned null, so a submitting thread will believe that it does not need re-execute.
         //this check fixes the issue
      }
      while (!tasks.isEmpty() && !requestedShutdown);
   }

   /**
    * It will shutdown and wait 30 seconds for timeout.
    */
   public void shutdown() {
      shutdown(30, TimeUnit.SECONDS);
   }

   public void shutdown(long timeout, TimeUnit unit) {
      requestedShutdown = true;

      if (!inHandler()) {
         // if it's in handler.. we just return
         flush(timeout, unit);
      }
   }

   /**
    * It will wait the current execution (if there is one) to finish
    * but will not complete any further executions
    */
   public int shutdownNow(Consumer<? super T> onPendingItem) {
      //alert anyone that has been requested (at least) an immediate shutdown
      requestedForcedShutdown = true;
      requestedShutdown = true;

      if (inHandler()) {
         stateUpdater.set(this, STATE_FORCED_SHUTDOWN);
      } else {
         //it could take a very long time depending on the current executing task
         do {
            //alert the ExecutorTask (if is running) to just drain the current backlog of tasks
            final int startState = stateUpdater.get(this);
            if (startState == STATE_FORCED_SHUTDOWN) {
               //another thread has completed a forced shutdown: let it to manage the tasks cleanup
               break;
            }
            if (startState == STATE_RUNNING) {
               //wait 100 ms to avoid burning CPU while waiting and
               //give other threads a chance to make progress
               LockSupport.parkNanos(100_000_000L);
            }
         }
         while (!stateUpdater.compareAndSet(this, STATE_NOT_RUNNING, STATE_FORCED_SHUTDOWN));
         //this could happen just one time: the forced shutdown state is the last one and
         //can be set by just one caller.
         //As noted on the execute method there is a small chance that some tasks would be enqueued
      }
      int pendingItems = 0;
      //there is a small chance that execute() could race with this cleanup: the lock allow an all-or-nothing behaviour between them
      synchronized (tasks) {
         T item;
         while ((item = tasks.poll()) != null) {
            onPendingItem.accept(item);
            pendingItems++;
         }
      }
      return pendingItems;
   }

   protected abstract void doTask(T task);

   public ProcessorBase(Executor parent) {
      this.delegate = parent;
   }

   public final boolean isFlushed() {
      return this.state == STATE_NOT_RUNNING;
   }

   /**
    * WARNING: This will only flush when all the activity is suspended.
    * don't expect success on this call if another thread keeps feeding the queue
    * this is only valid on situations where you are not feeding the queue,
    * like in shutdown and failover situations.
    */
   public final boolean flush(long timeout, TimeUnit unit) {
      if (this.state == STATE_NOT_RUNNING) {
         // quick test, most of the time it will be empty anyways
         return true;
      }

      long timeLimit = System.currentTimeMillis() + unit.toMillis(timeout);
      try {
         while (this.state == STATE_RUNNING && timeLimit > System.currentTimeMillis()) {

            if (tasks.isEmpty()) {
               return true;
            }

            Thread.sleep(10);
         }
      } catch (InterruptedException e) {
         // ignored
      }

      return this.state == STATE_NOT_RUNNING;
   }

   protected void task(T command) {
      if (requestedShutdown) {
         logAddOnShutdown();
      }
      //The shutdown process could finish right after the above check: shutdownNow can drain the remaining tasks
      tasks.add(command);
      //cache locally the state to avoid multiple volatile loads
      final int state = stateUpdater.get(this);
      if (state != STATE_RUNNING) {
         onAddedTaskIfNotRunning(state);
      }
   }

   /**
    * This has to be called on the assumption that state!=STATE_RUNNING.
    * It is packed separately from {@link #task(Object)} just for performance reasons: it
    * handles the uncommon execution cases for bursty scenarios i.e. the slowest execution path.
    */
   private void onAddedTaskIfNotRunning(int state) {
      if (state == STATE_NOT_RUNNING) {
         //startPoller could be deleted but is maintained because is inherited
         delegate.execute(task);
      } else if (state == STATE_FORCED_SHUTDOWN) {
         //help the GC by draining any task just submitted: it helps to cover the case of a shutdownNow finished before tasks.add
         synchronized (tasks) {
            tasks.clear();
         }
      }
   }

   private static void logAddOnShutdown() {
      if (logger.isDebugEnabled()) {
         logger.debug("Ordered executor has been gently shutdown at", new Exception("debug"));
      }
   }

   /**
    * Returns the remaining items to be processed.
    * <p>
    * This method is safe to be called by different threads and its accuracy is subject to concurrent modifications.<br>
    * It is meant to be used only for test purposes, because of its {@code O(n)} cost.
    */
   public final int remaining() {
      return tasks.size();
   }

   public final int status() {
      //avoid using the updater because in older version of JDK 8 isn't optimized as a vanilla volatile get
      return this.state;
   }

}
