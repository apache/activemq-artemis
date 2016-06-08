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
package org.apache.activemq.artemis.utils;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.jboss.logging.Logger;

/**
 * A factory for producing executors that run all tasks in order, which delegate to a single common executor instance.
 */
public final class OrderedExecutorFactory implements ExecutorFactory {

   private static final Logger logger = Logger.getLogger(OrderedExecutorFactory.class);

   private final Executor parent;

   /**
    * Construct a new instance delegating to the given parent executor.
    *
    * @param parent the parent executor
    */
   public OrderedExecutorFactory(final Executor parent) {
      this.parent = parent;
   }

   /**
    * Get an executor that always executes tasks in order.
    *
    * @return an ordered executor
    */
   @Override
   public Executor getExecutor() {
      return new OrderedExecutor(parent);
   }

   /**
    * An executor that always runs all tasks in order, using a delegate executor to run the tasks.
    * <br>
    * More specifically, any call B to the {@link #execute(Runnable)} method that happens-after another call A to the
    * same method, will result in B's task running after A's.
    */
   private static class OrderedExecutor implements Executor {

      private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
      private final Executor delegate;
      private final ExecutorTask task = new ExecutorTask();

      // used by stateUpdater
      @SuppressWarnings("unused")
      private volatile int state = 0;

      private static final AtomicIntegerFieldUpdater<OrderedExecutor> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(OrderedExecutor.class, "state");

      private static final int STATE_NOT_RUNNING = 0;
      private static final int STATE_RUNNING = 1;

      public OrderedExecutor(Executor delegate) {
         this.delegate = delegate;
      }


      @Override
      public void execute(Runnable command) {
         tasks.add(command);
         if (stateUpdater.get(this) == STATE_NOT_RUNNING) {
            //note that this can result in multiple tasks being queued
            //this is not an issue as the CAS will mean that the second (and subsequent) execution is ignored
            delegate.execute(task);
         }
      }

      private final class ExecutorTask implements Runnable {

         @Override
         public void run() {
            do {
               //if there is no thread active then we run
               if (stateUpdater.compareAndSet(OrderedExecutor.this, STATE_NOT_RUNNING, STATE_RUNNING)) {
                  Runnable task = tasks.poll();
                  //while the queue is not empty we process in order
                  while (task != null) {
                     try {
                        task.run();
                     }
                     catch (ActiveMQInterruptedException e) {
                        // This could happen during shutdowns. Nothing to be concerned about here
                        logger.debug("Interrupted Thread", e);
                     }
                     catch (Throwable t) {
                        ActiveMQClientLogger.LOGGER.caughtunexpectedThrowable(t);
                     }
                     task = tasks.poll();
                  }
                  //set state back to not running.
                  stateUpdater.set(OrderedExecutor.this, STATE_NOT_RUNNING);
               }
               else {
                  return;
               }
               //we loop again based on tasks not being empty. Otherwise there is a window where the state is running,
               //but poll() has returned null, so a submitting thread will believe that it does not need re-execute.
               //this check fixes the issue
            } while (!tasks.isEmpty());
         }
      }

      @Override
      public String toString() {
         return "OrderedExecutor(tasks=" + tasks + ")";
      }
   }
}
