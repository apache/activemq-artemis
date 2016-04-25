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
import java.util.concurrent.atomic.AtomicInteger;

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

      public void setExecutor(ActiveMQThreadPoolExecutor executor) {
         this.executor = executor;
      }

      @Override
      public boolean offer(Runnable runnable) {
         int poolSize = executor.getPoolSize();

         // If the are less threads than the configured maximum, then the tasks is
         // only queued if there are some idle threads that can run that tasks.
         // We have to add the queue size, since some tasks might just have been queued
         // but not yet taken by an idle thread.
         if (poolSize < executor.getMaximumPoolSize() && (size() + executor.getActive()) >= poolSize)
            return false;

         return super.offer(runnable);
      }
   }

   private int maxPoolSize;

   // count the active threads with before-/afterExecute, since the .getActiveCount is not very
   // efficient.
   private final AtomicInteger active = new AtomicInteger(0);

   public ActiveMQThreadPoolExecutor(int coreSize, int maxSize, long keep, TimeUnit keepUnits, ThreadFactory factory) {
      this(coreSize, maxSize, keep, keepUnits, new ThreadPoolQueue(), factory);
   }

   // private constructor is needed to inject 'this' into the ThreadPoolQueue instance
   private ActiveMQThreadPoolExecutor(int coreSize, int maxSize, long keep, TimeUnit keepUnits, ThreadPoolQueue myQueue, ThreadFactory factory) {
      super(coreSize, Integer.MAX_VALUE, keep, keepUnits, myQueue, factory);
      maxPoolSize = maxSize;
      myQueue.setExecutor(this);
   }

   private int getActive() {
      return active.get();
   }

   @Override
   public int getMaximumPoolSize() {
      return maxPoolSize;
   }

   @Override
   public void setMaximumPoolSize(int maxSize) {
      maxPoolSize = maxSize;
   }

   @Override
   protected void beforeExecute(Thread thread, Runnable runnable) {
      super.beforeExecute(thread, runnable);
      active.incrementAndGet();
   }

   @Override
   protected void afterExecute(Runnable runnable, Throwable throwable) {
      active.decrementAndGet();
      super.afterExecute(runnable, throwable);
   }
}
