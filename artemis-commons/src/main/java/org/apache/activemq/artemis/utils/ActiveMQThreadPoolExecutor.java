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
import java.util.concurrent.RejectedExecutionHandler;
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
 */
public class ActiveMQThreadPoolExecutor extends ThreadPoolExecutor {

   // Handler executed when a task is submitted and a new thread cannot be created (because maxSize was reached)
   // It queues the task on the executors's queue (using the add() method, see ThreadPoolQueue class below)
   private static final RejectedExecutionHandler QUEUE_EXECUTION_HANDLER = (r, e) -> {
      if (!e.isShutdown()) {
         e.getQueue().add(r);
      }
   };

   // A specialized LinkedBlockingQueue that takes new elements by calling add() but not offer()
   // This is to force the ThreadPoolExecutor to always create new threads and never queue
   private static class ThreadPoolQueue extends LinkedBlockingQueue<Runnable> {

      @Override
      public boolean offer(Runnable runnable) {
         return false;
      }

      @Override
      public boolean add(Runnable runnable) {
         return super.offer( runnable );
      }
   }

   public ActiveMQThreadPoolExecutor(int coreSize, int maxSize, long keep, TimeUnit keepUnits, ThreadFactory factory) {
      super( coreSize, maxSize, keep, keepUnits, new ThreadPoolQueue(), factory, QUEUE_EXECUTION_HANDLER );
   }
}
