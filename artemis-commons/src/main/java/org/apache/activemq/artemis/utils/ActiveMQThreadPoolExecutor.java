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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
 * ActiveMQThreadPoolExecutor: a special ThreadPoolExecutor that combines
 * the benefits of a cached executor and a fixed size executor.
 * Similar to a cached executor, threads exceeding the core size are only created on demand,
 * and will be removed after idling for a specified keep time.
 * But in contrast to a standard cached executor, tasks are queued if the
 * maximum pool size if reached, instead of rejected.
 *
 * This is achieved by using a specialized blocking queue, which checks the
 * state of the associated executor in the offer method to decide whether to
 * queue a task or have the executor create another thread.
 *
 * Since the thread pool's execute method is reentrant, more than one caller
 * could try to offer a task into the queue. There is a small chance that
 * (a few) more threads are created as it should be limited by max pool size.
 * To allow for such a case not to reject a task, the underlying thread pool
 * executor is not limited. Only the offer method checks the configured limit.
 */
public class ActiveMQThreadPoolExecutor extends ThreadPoolExecutor {

   @SuppressWarnings("serial")
   private static class ThreadPoolQueue extends LinkedBlockingQueue<Runnable> {

      private ActiveMQThreadPoolExecutor executor = null;

      // lock object to synchronize on
      private final Object lock = new Object();

      // keep track of the difference between the number of idle threads and
      // the number of queued tasks. If the delta is > 0, we have more
      // idle threads than queued tasks and can add more tasks into the queue.
      // The delta is incremented if a thread becomes idle or if a task is taken from the queue.
      // The delta is decremented if a thread leaves idle state or if a task is added to the queue.
      private int threadTaskDelta = 0;

      public void setExecutor(ActiveMQThreadPoolExecutor executor) {
         this.executor = executor;
      }

      @Override
      public boolean offer(Runnable runnable) {
         boolean retval = false;

         // Need to lock for 2 reasons:
         // 1. to safely handle poll timeouts
         // 2. to protect the delta from parallel updates
         synchronized (lock) {
            if ((executor.getPoolSize() >= executor.getMaximumPoolSize()) || (threadTaskDelta > 0)) {
               // A new task will be added to the queue if the maximum number of threads has been reached
               // or if the delta is > 0, which means that there are enough idle threads.

               retval = super.offer(runnable);

               // Only decrement the delta if the task has actually been added to the queue
               if (retval)
                  threadTaskDelta--;
            }
         }

         return retval;
      }

      @Override
      public Runnable take() throws InterruptedException {
         // Increment the delta as a thread becomes idle
         // by waiting for a task to take from the queue
         synchronized (lock) {
            threadTaskDelta++;
         }

         Runnable runnable = null;

         try {
            runnable = super.take();
            return runnable;
         } finally {
            // Now the thread is no longer idle waiting for a task
            // If it had taken a task, the delta remains the same
            // (decremented by the thread and incremented by the taken task)
            // Only if no task had been taken, we have to decrement the delta.
            if (runnable == null) {
               synchronized (lock) {
                  threadTaskDelta--;
               }
            }
         }
      }

      @Override
      public Runnable poll(long arg0, TimeUnit arg2) throws InterruptedException {
         // Increment the delta as a thread becomes idle
         // by waiting for a task to poll from the queue
         synchronized (lock) {
            threadTaskDelta++;
         }

         Runnable runnable = null;
         boolean timedOut = false;

         try {
            runnable = super.poll(arg0, arg2);
            timedOut = (runnable == null);
         } finally {
            // Now the thread is no longer idle waiting for a task
            // If it had taken a task, the delta remains the same
            // (decremented by the thread and incremented by the taken task)
            if (runnable == null) {
               synchronized (lock) {
                  // If the poll called timed out, we check again within a synchronized block
                  // to make sure all offer calls have been completed.
                  // This is to handle a newly queued task if the timeout occurred while an offer call
                  // added that task to the queue instead of creating a new thread.
                  if (timedOut)
                     runnable = super.poll();

                  // Only if no task had been taken (either no timeout, or no task from after-timeout poll),
                  // we have to decrement the delta.
                  if (runnable == null)
                     threadTaskDelta--;
               }
            }
         }

         return runnable;
      }
   }

   private int maxPoolSize;

   public ActiveMQThreadPoolExecutor(int coreSize, int maxSize, long keep, TimeUnit keepUnits, ThreadFactory factory) {
      this(coreSize, maxSize, keep, keepUnits, new ThreadPoolQueue(), factory);
   }

   // private constructor is needed to inject 'this' into the ThreadPoolQueue instance
   private ActiveMQThreadPoolExecutor(int coreSize,
                                      int maxSize,
                                      long keep,
                                      TimeUnit keepUnits,
                                      ThreadPoolQueue myQueue,
                                      ThreadFactory factory) {
      super(coreSize, Integer.MAX_VALUE, keep, keepUnits, myQueue, factory);
      maxPoolSize = maxSize;
      myQueue.setExecutor(this);
   }

   @Override
   public int getMaximumPoolSize() {
      return maxPoolSize;
   }

   @Override
   public void setMaximumPoolSize(int maxSize) {
      maxPoolSize = maxSize;
   }
}
