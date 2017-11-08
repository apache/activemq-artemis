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

public abstract class ProcessorBase<T> {

   private static final int STATE_NOT_RUNNING = 0;
   private static final int STATE_RUNNING = 1;

   protected final Queue<T> tasks = new ConcurrentLinkedQueue<>();

   private final Executor delegate;

   private final ExecutorTask task = new ExecutorTask();

   private final Object startedGuard = new Object();
   private volatile boolean started = true;

   // used by stateUpdater
   @SuppressWarnings("unused")
   private volatile int state = 0;

   private static final AtomicIntegerFieldUpdater<ProcessorBase> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(ProcessorBase.class, "state");

   private final class ExecutorTask implements Runnable {

      @Override
      public void run() {
         do {
            //if there is no thread active then we run
            if (stateUpdater.compareAndSet(ProcessorBase.this, STATE_NOT_RUNNING, STATE_RUNNING)) {
               T task = tasks.poll();
               //while the queue is not empty we process in order

               // All we care on started, is that a current task is not running as we call shutdown.
               // for that reason this first run doesn't need to be under any lock
               while (task != null && started) {

                  // Synchronized here is just to guarantee that a current task is finished before
                  // the started update can be taken as false
                  synchronized (startedGuard) {
                     if (started) {
                        doTask(task);
                     }
                  }
                  task = tasks.poll();
               }
               //set state back to not running.
               stateUpdater.set(ProcessorBase.this, STATE_NOT_RUNNING);
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
      synchronized (startedGuard) {
         started = false;
      }
      tasks.clear();
   }

   protected abstract void doTask(T task);

   public ProcessorBase(Executor parent) {
      this.delegate = parent;
   }

   public final boolean isFlushed() {
      return stateUpdater.get(this) == STATE_NOT_RUNNING;
   }

   protected void task(T command) {
      // There is no need to verify the lock here.
      // you can only turn of running once
      if (started) {
         tasks.add(command);
         startPoller();
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
