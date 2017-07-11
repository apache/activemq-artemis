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
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.jboss.logging.Logger;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.MpscChunkedArrayQueue;
import org.jctools.queues.SpscArrayQueue;
import org.jctools.queues.SpscChunkedArrayQueue;
import org.jctools.util.Pow2;

/**
 * A factory for producing executors that run all tasks in order, which delegate to a single common executor instance.
 */
public final class BoundedOrderedExecutorFactory implements ExecutorFactory {

   public enum ProducerType {
      Single, Multi
   }

   public static final int DEFAULT_INITIAL_CAPACITY = Integer.getInteger("bounded.executor.initial.capacity", 1024);
   public static final int DEFAULT_MAX_CAPACITY = Integer.getInteger("bounded.executor.max.capacity", Pow2.MAX_POW2);
   public static final int DEFAULT_MAX_BURST_SIZE = Integer.getInteger("bounded.executor.max.burst", Integer.MAX_VALUE);
   private static final Logger logger = Logger.getLogger(BoundedOrderedExecutorFactory.class);

   private final ProducerType producerType;
   private final Executor parent;
   private final int initialCapacity;
   private final int maxCapacity;
   private final int maxBurstSize;

   public BoundedOrderedExecutorFactory(Executor parent) {
      this(ProducerType.Multi, parent, DEFAULT_INITIAL_CAPACITY, DEFAULT_MAX_CAPACITY, DEFAULT_MAX_BURST_SIZE);
   }

   /**
    * Construct a new instance delegating to the given parent executor.
    *
    * @param parent the parent executor
    */
   public BoundedOrderedExecutorFactory(ProducerType producerType,
                                        Executor parent,
                                        int initialCapacity,
                                        int maxCapacity,
                                        int maxBurstSize) {
      this.producerType = producerType;
      this.parent = parent;
      this.initialCapacity = initialCapacity;
      this.maxCapacity = maxCapacity;
      this.maxBurstSize = maxBurstSize;
   }

   private static int drainCommands(final Queue<Runnable> commands, int maxBurstSize) {
      for (int i = 0; i < maxBurstSize; i++) {
         final Runnable command = commands.poll();
         if (command == null) {
            return i;
         }
         try {
            command.run();
         } catch (ActiveMQInterruptedException e) {
            // This could happen during shutdowns. Nothing to be concerned about here
            logger.debug("Interrupted Thread", e);
         } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
         }
      }
      return maxBurstSize;
   }

   /**
    * Get an executor that always executes tasks in order.
    *
    * @return an ordered executor
    */
   @Override
   public BoundedExecutor getExecutor() {
      if (this.initialCapacity == this.maxCapacity) {
         return createFixedExecutor(this.producerType, this.maxCapacity, this.maxBurstSize, this.parent);
      } else {
         return createGrowableExecutor(this.producerType, this.initialCapacity, this.maxCapacity, this.maxBurstSize, this.parent);
      }
   }

   public static BoundedExecutor createFixedExecutor(ProducerType producerType,
                                                     int requiredCapacity,
                                                     int maxBurstSize,
                                                     Executor parent) {
      final Queue<Runnable> queue;
      final int capacity;
      switch (producerType) {
         case Multi:
            final MpscArrayQueue<Runnable> mpscChunkedArrayQueue = new MpscArrayQueue<>(requiredCapacity);
            queue = mpscChunkedArrayQueue;
            capacity = mpscChunkedArrayQueue.capacity();
            break;
         case Single:
            final SpscArrayQueue<Runnable> spscChunkedArrayQueue = new SpscArrayQueue<>(requiredCapacity);
            queue = spscChunkedArrayQueue;
            capacity = spscChunkedArrayQueue.capacity();
            break;
         default:
            throw new AssertionError("producerType not supported");
      }
      return new BoundedOrderedExecutor(queue, capacity, maxBurstSize, parent);
   }

   public static BoundedExecutor createGrowableExecutor(ProducerType producerType,
                                                        int initialCapacity,
                                                        int maxCapacity,
                                                        int maxBurstSize,
                                                        Executor parent) {
      final Queue<Runnable> queue;
      final int capacity;
      switch (producerType) {
         case Multi:
            final MpscChunkedArrayQueue<Runnable> mpscChunkedArrayQueue = new MpscChunkedArrayQueue<>(initialCapacity, maxCapacity);
            queue = mpscChunkedArrayQueue;
            capacity = mpscChunkedArrayQueue.capacity();
            break;
         case Single:
            final SpscChunkedArrayQueue<Runnable> spscChunkedArrayQueue = new SpscChunkedArrayQueue<>(initialCapacity, maxCapacity);
            queue = spscChunkedArrayQueue;
            //SpscChunkedArrayQueue doesn't support the MessagePassingQueue interface yet!
            capacity = Pow2.roundToPowerOfTwo(maxCapacity);
            break;
         default:
            throw new AssertionError("producerType not supported");
      }
      return new BoundedOrderedExecutor(queue, capacity, maxBurstSize, parent);
   }

   /**
    * The padding is employed for false sharing protection: separate the cold fields (load only) from the hot one (the state, store and load)
    */
   private abstract static class BoundedOrderedExecutorL0Pad {

      protected long p00, p01, p02, p03, p04, p05, p06;
      protected long p10, p11, p12, p13, p14, p15, p16, p17;
   }

   private abstract static class OrderedExecutorState extends BoundedOrderedExecutorL0Pad {

      protected static final int RELEASED = 0;
      public static final AtomicIntegerFieldUpdater<OrderedExecutorState> STATE_UPDATER;

      static {
         STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(OrderedExecutorState.class, "state");
      }

      private volatile int state = 0;

   }

   private abstract static class BoundedOrderedExecutorL1Pad extends OrderedExecutorState {

      protected long p01, p02, p03, p04, p05, p06, p07;
      protected long p10, p11, p12, p13, p14, p15, p16, p17;
   }

   private static final class BoundedOrderedExecutor extends BoundedOrderedExecutorL1Pad implements BoundedExecutor {

      private final Queue<Runnable> commands;
      private final Runnable executeCommandsTask;
      private final Executor delegate;
      private final int maxBurstSize;
      private final int capacity;

      BoundedOrderedExecutor(Queue<Runnable> commands, int capacity, int maxBurstSize, Executor delegate) {
         this.commands = commands;
         this.executeCommandsTask = this::executeCommands;
         this.delegate = delegate;
         this.capacity = capacity;
         this.maxBurstSize = maxBurstSize;
      }

      private boolean tryAcquire() {
         //much cheaper than CAS when highly contended
         final long oldState = STATE_UPDATER.getAndIncrement(this);
         final boolean isAcquired = oldState == RELEASED;
         return isAcquired;
      }

      private boolean isReleased() {
         return STATE_UPDATER.get(this) == RELEASED;
      }

      private void release() {
         //StoreStore + LoadStore: much cheaper than a volatile store
         STATE_UPDATER.lazySet(this, RELEASED);
      }

      private void executeCommands() {
         final Queue<Runnable> commands = this.commands;
         final int maxBurstSize = this.maxBurstSize;
         //let others consumers to try to acquire the lock and drain the tasks
         while (!commands.isEmpty() && tryAcquire()) {
            try {
               drainCommands(commands, maxBurstSize);
            } finally {
               release();
            }
         }
      }

      @Override
      public int capacity() {
         return this.capacity;
      }

      @Override
      public int pendingTasks() {
         return this.commands.size();
      }

      @Override
      public boolean isEmpty() {
         return this.commands.isEmpty();
      }

      @Override
      public boolean tryExecute(Runnable command) {
         //no optimisations on recursive offers
         if (commands.offer(command)) {
            if (isReleased() && !commands.isEmpty()) {
               this.delegate.execute(executeCommandsTask);
            }
            return true;
         } else {
            return false;
         }
      }

      @Override
      public void execute(Runnable command) {
         //no optimisations on recursive offers
         if (!commands.offer(command)) {
            throw new RejectedExecutionException("can't submit the task: max capacity reached");
         } else if (isReleased() && !commands.isEmpty()) {
            this.delegate.execute(executeCommandsTask);
         }
      }

      @Override
      public String toString() {
         return "BoundedOrderedExecutor{" + "delegate=" + delegate + ", maxBurstSize=" + maxBurstSize + ", capacity=" + capacity + ", pending=" + pendingTasks() + '}';
      }
   }
}
