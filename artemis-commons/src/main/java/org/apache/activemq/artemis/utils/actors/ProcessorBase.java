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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;

public abstract class ProcessorBase<T> {

   private static final int STATE_NOT_RUNNING = 0;
   private static final int STATE_RUNNING = 1;
   private static final int STATE_FORCED_SHUTDOWN = 2;

   protected final Queue<T> tasks = new ConcurrentLinkedQueue<>();

   private final Executor delegate;

   private final ExecutorTask task = new ExecutorTask();

   // used by stateUpdater
   @SuppressWarnings("unused")
   private volatile int state = STATE_NOT_RUNNING;

   private volatile boolean requestedShutdown = false;

   private static final AtomicIntegerFieldUpdater<ProcessorBase> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(ProcessorBase.class, "state");

   private final class ExecutorTask implements Runnable {

      @Override
      public void run() {
         do {
            //if there is no thread active and is not already dead then we run
            if (stateUpdater.compareAndSet(ProcessorBase.this, STATE_NOT_RUNNING, STATE_RUNNING)) {
               try {
                  T task = tasks.poll();
                  //while the queue is not empty we process in order
                  while (task != null) {
                     //just drain the tasks if has been requested a shutdown to help the shutdown process
                     if (!requestedShutdown) {
                        doTask(task);
                     }
                     task = tasks.poll();
                  }
               } finally {
                  //set state back to not running.
                  stateUpdater.set(ProcessorBase.this, STATE_NOT_RUNNING);
               }
            } else {
               return;
            }
            //we loop again based on tasks not being empty. Otherwise there is a window where the state is running,
            //but poll() has returned null, so a submitting thread will believe that it does not need re-execute.
            //this check fixes the issue
         }
         while (!tasks.isEmpty());
      }
   }

   /** It will wait the current execution (if there is one) to finish
    *  but will not complete any further executions */
   public void shutdownNow() {
      //alert anyone that has been requested (at least) an immediate shutdown
      requestedShutdown = true;
      //it could take a very long time depending on the current executing task
      do {
         //alert the ExecutorTask (if is running) to just drain the current backlog of tasks
         final int startState = stateUpdater.get(this);
         if (startState == STATE_FORCED_SHUTDOWN) {
            //another thread has completed a forced shutdown
            return;
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
      tasks.clear();
      //we can report the killed tasks somehow: ExecutorService do the same on shutdownNow
   }

   protected abstract void doTask(T task);

   public ProcessorBase(Executor parent) {
      this.delegate = parent;
   }

   public final boolean isFlushed() {
      return stateUpdater.get(this) == STATE_NOT_RUNNING;
   }

   protected void task(T command) {
      if (stateUpdater.get(this) != STATE_FORCED_SHUTDOWN) {
         //The shutdown process could finish right after the above check: shutdownNow can drain the remaining tasks
         tasks.add(command);
         //cache locally the state to avoid multiple volatile loads
         final int state = stateUpdater.get(this);
         if (state == STATE_FORCED_SHUTDOWN) {
            //help the GC by draining any task just submitted: it help to cover the case of a shutdownNow finished before tasks.add
            tasks.clear();
         } else if (state == STATE_NOT_RUNNING) {
            //startPoller could be deleted but is maintained because is inherited
            delegate.execute(task);
         }
      }
   }

   protected void startPoller() {
      if (stateUpdater.get(this) == STATE_NOT_RUNNING) {
         //note that this can result in multiple tasks being queued
         //this is not an issue as the CAS will mean that the second (and subsequent) execution is ignored
         delegate.execute(task);
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

}
