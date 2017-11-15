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

package org.apache.activemq.artemis.utils.collections;

import java.lang.reflect.Array;
import java.util.function.Consumer;

/**
 * A type of chunked queue which maintains items according to a priority.<br>
 * Differently from a {@link PriorityLinkedList} it doesn't allows
 * {@link PriorityLinkedListImpl.PriorityLinkedListIterator#remove()} operations, but it is optimized for
 * fast {@link #offer(Object, int)}, {@link #offerFirst(Object, int)} and {@link #poll()}, providing batch
 * versions too (ie {@link #drain(Consumer)}).
 */
public final class PriorityChunkedQueue<E> {

   private final ChunkedQueue<E>[] queues;
   private final int chunkSize;
   //it is used to reduce pointer chasing on burst of offers/polls with the same priority
   private ChunkedQueue<E> highestPriorityQueueNotEmpty;
   private int highestPriorityNotEmpty;

   private PriorityChunkedQueue(int priorities, int chunkSize) {
      this.queues = (ChunkedQueue<E>[]) Array.newInstance(ChunkedQueue.class, priorities);
      this.chunkSize = chunkSize;
      this.highestPriorityQueueNotEmpty = null;
      this.highestPriorityNotEmpty = -1;
   }

   private ChunkedQueue<E> allocateQueue(int priority) {
      final ChunkedQueue<E> queue = ChunkedQueue.with(chunkSize);
      queues[priority] = queue;
      return queue;
   }

   /**
    * Inserts the specified element with the specified priority at the start of this queue.
    *
    * @param e        the element to append
    * @param priority the priority of the element
    */
   public void offerFirst(E e, int priority) {
      if (e == null) {
         throw new NullPointerException("e can't be null!");
      }
      if (priority == this.highestPriorityNotEmpty) {
         this.highestPriorityQueueNotEmpty.offerFirst(e);
      } else {
         offerFirstSlowPath(e, priority);
      }
   }

   /**
    * Inserts the specified element with the specified priority at the end of this queue.
    *
    * @param e        the element to append
    * @param priority the priority of the element
    */
   public void offer(E e, int priority) {
      if (e == null) {
         throw new NullPointerException("e can't be null!");
      }
      if (priority == this.highestPriorityNotEmpty) {
         this.highestPriorityQueueNotEmpty.offer(e);
      } else {
         offerSlowPath(e, priority);
      }
   }

   private void offerSlowPath(E e, int priority) {
      ChunkedQueue<E> queue = this.queues[priority];
      if (queue == null) {
         queue = allocateQueue(priority);
      }
      queue.offer(e);
      if (priority > this.highestPriorityNotEmpty) {
         this.highestPriorityNotEmpty = priority;
         this.highestPriorityQueueNotEmpty = queue;
      }
   }

   private void offerFirstSlowPath(E e, int priority) {
      ChunkedQueue<E> queue = this.queues[priority];
      if (queue == null) {
         queue = allocateQueue(priority);
      }
      queue.offerFirst(e);
      if (priority > this.highestPriorityNotEmpty) {
         this.highestPriorityNotEmpty = priority;
         this.highestPriorityQueueNotEmpty = queue;
      }
   }

   /**
    * Clear each element of the queue.
    */
   public void clear() {
      final ChunkedQueue<E>[] queues = this.queues;
      for (ChunkedQueue<E> queue : queues) {
         if (queue != null) {
            queue.clear();
         }
      }
      this.highestPriorityQueueNotEmpty = null;
      this.highestPriorityNotEmpty = -1;
   }

   private void updateNextHighestPriorityQueueNotEmpty(int maxPriority) {
      final ChunkedQueue<E>[] queues = this.queues;
      for (int priority = maxPriority; priority >= 0; priority--) {
         final ChunkedQueue<E> queue = queues[priority];
         if (queue != null && queue.size() > 0) {
            this.highestPriorityQueueNotEmpty = queue;
            this.highestPriorityNotEmpty = priority;
            return;
         }
      }
      this.highestPriorityQueueNotEmpty = null;
      this.highestPriorityNotEmpty = -1;
   }

   /**
    * Retrieves the head (ie the first element) of the queue, or returns
    * {@code null} if this queue is empty.<br>
    * The elements peeked maintain the order of offering only per priority and will be preferred
    * higher priorities first (ie it could cause starvation for the lower priority elements).
    *
    * @return the head of the queue represented by this queue, or
    * {@code null} if this queue is empty
    */
   public E peek() {
      if (this.highestPriorityQueueNotEmpty != null) {
         return this.highestPriorityQueueNotEmpty.peek();
      } else {
         return peekSlowPath();
      }
   }

   private E peekSlowPath() {
      final ChunkedQueue<E>[] queues = this.queues;
      final int queuesLength = this.queues.length;
      final int maxPriority = queuesLength - 1;

      for (int priority = maxPriority; priority >= 0; priority--) {
         final ChunkedQueue<E> queue = queues[priority];
         if (queue != null && queue.size() > 0) {
            this.highestPriorityQueueNotEmpty = queue;
            this.highestPriorityNotEmpty = priority;
            return queue.peek();
         }
      }
      return null;
   }

   /**
    * Retrieves and removes the head (ie the first element) of the queue, or returns
    * {@code null} if this queue is empty.<br>
    * The elements removed maintain the order of offering only per priority and will be preferred
    * higher priorities first (ie it could cause starvation for the lower priority elements).
    *
    * @return the head of the queue represented by this queue, or
    * {@code null} if this queue is empty
    */
   public E poll() {
      if (this.highestPriorityQueueNotEmpty != null) {
         final E e = this.highestPriorityQueueNotEmpty.poll();
         if (this.highestPriorityQueueNotEmpty.size() == 0) {
            updateNextHighestPriorityQueueNotEmpty(this.highestPriorityNotEmpty);
         }
         return e;
      } else {
         return pollSlowPath();
      }
   }

   private E pollSlowPath() {
      final ChunkedQueue<E>[] queues = this.queues;
      final int queuesLength = this.queues.length;
      final int maxPriority = queuesLength - 1;

      for (int priority = maxPriority; priority >= 0; priority--) {
         final ChunkedQueue<E> queue = queues[priority];
         if (queue != null && queue.size() > 0) {
            final E e = queue.poll();
            if (queue.size() == 0) {
               //search (if any) of the rest of the queues has any item
               updateNextHighestPriorityQueueNotEmpty(priority);
            }
            return e;
         }
      }
      return null;
   }

   /**
    * Remove all available elements from the queue and hand to consume. This should be semantically similar to:
    * <code><br/>
    * M m;</br>
    * while((m = poll()) != null){</br>
    * c.accept(m);</br>
    * }</br>
    * </code>
    * At the end of the operation the queue must be empty.
    *
    * @return the number of polled elements
    */
   public long drain(Consumer<? super E> onMessage) {
      long messages = 0;
      final ChunkedQueue<E>[] queues = this.queues;
      final int queuesLength = this.queues.length;
      final int maxPriority = queuesLength - 1;

      for (int priority = maxPriority; priority >= 0; priority--) {
         final ChunkedQueue<E> queue = queues[priority];
         if (queue != null && queue.size() > 0) {
            messages += queue.drain(onMessage);
         }
      }
      this.highestPriorityQueueNotEmpty = null;
      this.highestPriorityNotEmpty = -1;
      return messages;
   }

   /**
    * Iterates for all available elements from the queue and hand to consume.
    *
    * @return the number of consumed elements
    */
   public long forEach(Consumer<? super E> onMessage) {
      long messages = 0;
      final ChunkedQueue<E>[] queues = this.queues;
      final int queuesLength = this.queues.length;
      final int maxPriority = queuesLength - 1;

      for (int priority = maxPriority; priority >= 0; priority--) {
         final ChunkedQueue<E> queue = queues[priority];
         if (queue != null && queue.size() > 0) {
            messages += queue.forEach(onMessage);
         }
      }
      return messages;
   }

   /**
    * Returns the number of elements available in the queue according to the specified {@code priority}.
    *
    * @param priority the priority of the elements
    * @return the number of elements in the queue according to the specified {@code priority}
    */
   public long sizeOf(int priority) {
      final ChunkedQueue<E> queue = this.queues[priority];
      if (queue == null) {
         return 0;
      } else {
         return queue.size();
      }
   }

   /**
    * Returns the number of elements available in the queue
    *
    * @return the number of elements in the queue
    */
   public long size() {
      long totalSize = 0;
      final ChunkedQueue<E>[] queues = this.queues;
      for (ChunkedQueue<E> queue : queues) {
         if (queue != null) {
            totalSize += queue.size();
         }
      }
      return totalSize;
   }

   /**
    * Create a {@link PriorityChunkedQueue} with priorities in the range: [0, {@code priorities}) and with a fixed {@code} chunkSize.
    *
    * @param priorities the number of priorities allowed
    * @param chunkSize  the number of elements of each chunk
    * @param <E>        the element type
    * @return a new instance of {@link PriorityChunkedQueue}
    */
   public static <E> PriorityChunkedQueue<E> with(int priorities, int chunkSize) {
      return new PriorityChunkedQueue<>(priorities, chunkSize);

   }
}
