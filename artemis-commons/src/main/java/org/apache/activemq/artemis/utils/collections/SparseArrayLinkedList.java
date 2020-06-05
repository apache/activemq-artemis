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
package org.apache.activemq.artemis.utils.collections;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * This list share the same motivation and structure of https://en.wikipedia.org/wiki/Unrolled_linked_list:
 * it's a linked list of arrays/chunks of {@code T}.<br>
 * Differently from an {@code UnrolledLinkedList} this list doesn't optimize addition and removal to achieve a balanced
 * utilization among chunks ie a chunk is removed only if empty and chunks can't be merged.
 * This list has been optimized for small-sized chunks (ideally &lt;= 32 elements): this allow search/removal to
 * be performed with a greedy approach despite a sparse chunk utilization (ie chunks contains few sparse elements).<br>
 *
 * From the memory footprint's point of view, this list won't remove the last remaining array although empty to optimize
 * the case where its capacity would be enough to hold incoming elements, hence saving a new array allocation.
 */
public final class SparseArrayLinkedList<T> {

   // the whole chunk fit into 1 or 2 cache lines depending if JVM COOPS are used
   private static final int SPARSE_ARRAY_DEFAULT_CAPACITY = 16;

   private static final class SparseArray<T> {

      private final Object[] elements;
      private int size;
      // index next to the last non null element
      private int tail;

      private SparseArray(int capacity) {
         elements = new Object[capacity];
         size = 0;
         tail = 0;
      }

      private boolean add(T e) {
         final int capacity = elements.length;
         if (tail == capacity) {
            return false;
         }
         elements[tail] = (T) e;
         tail++;
         size++;
         return true;
      }

      private int remove(Predicate<? super T> filter) {
         if (size == 0) {
            // this shouldn't happen: the chunk should be removed if empty
            return 0;
         }
         // this is allowed to make holes
         // to save System::arrayCopy while removing elements
         int removed = 0;
         final Object[] elements = this.elements;
         int visited = 0;
         final int originalSize = size;
         for (int i = 0, capacity = elements.length; i < capacity; i++) {
            final T e = (T) elements[i];
            if (e != null) {
               if (filter.test(e)) {
                  elements[i] = null;
                  removed++;
               } else {
                  // allows a weak form of compaction: incoming elements
                  // will be placed right after it
                  tail = i + 1;
               }
               visited++;
               if (visited == originalSize) {
                  break;
               }
            }
         }
         size -= removed;
         // reset the tail in case of no elements left:
         // tail is set to be the next of the last
         if (size == 0) {
            tail = 0;
         }
         return removed;
      }

      public int clear(Consumer<? super T> consumer) {
         final int originalSize = size;
         if (originalSize == 0) {
            return 0;
         }
         int visited = 0;
         final Object[] elements = this.elements;
         for (int i = 0, capacity = elements.length; i < capacity; i++) {
            final T e = (T) elements[i];
            if (e != null) {
               if (consumer != null) {
                  consumer.accept(e);
               }
               elements[i] = null;
               size--;
               visited++;
               if (visited == originalSize) {
                  break;
               }
            }
         }
         assert size == 0;
         tail = 0;
         return originalSize;
      }

      private int size() {
         return size;
      }
   }

   public static <T> long removeFromSparseArrayList(List<SparseArray<T>> sparseArrayList, Predicate<? super T> filter) {
      if (filter == null) {
         return 0;
      }
      long removed = 0;
      Iterator<SparseArray<T>> iter = sparseArrayList.iterator();
      while (iter.hasNext()) {
         final SparseArray<T> sparseArray = iter.next();
         final int justRemoved = sparseArray.remove(filter);
         removed += justRemoved;
         if (justRemoved > 0) {
            // remove the array only if empty and not the last one:
            // it means that there is a chance of fragmentation
            // proportional with the array capacity
            if (sparseArrayList.size() > 1 && sparseArray.size() == 0) {
               iter.remove();
            }
         }
      }
      return removed;
   }

   public static <T> void addToSparseArrayList(List<SparseArray<T>> sparseArrayList, T e, int sparseArrayCapacity) {
      final int size = sparseArrayList.size();
      // LinkedList::get(size-1) is fast as LinkedList::getLast
      if (size == 0 || !sparseArrayList.get(size - 1).add(e)) {
         final SparseArray<T> sparseArray = new SparseArray<>(sparseArrayCapacity);
         sparseArray.add(e);
         sparseArrayList.add(sparseArray);
      }
   }

   public static <T> long clearSparseArrayList(List<SparseArray<T>> sparseArrayList, Consumer<? super T> consumer) {
      final int size = sparseArrayList.size();
      long count = 0;
      if (size > 0) {
         for (int i = 0; i < size - 1; i++) {
            // LinkedList::remove(0) is fast as LinkedList::getFirst
            final SparseArray<T> removed = sparseArrayList.remove(0);
            count += removed.clear(consumer);
         }
         // LinkedList::get(0) is fast as LinkedList::getFirst
         count += sparseArrayList.get(0).clear(consumer);
      }
      return count;
   }

   private final LinkedList<SparseArray<T>> list;
   private final int sparseArrayCapacity;
   private long size;

   public SparseArrayLinkedList() {
      this(SPARSE_ARRAY_DEFAULT_CAPACITY);
   }

   public SparseArrayLinkedList(int sparseArrayCapacity) {
      if (sparseArrayCapacity <= 0) {
         throw new IllegalArgumentException("sparseArrayCapacity must be > 0");
      }
      list = new LinkedList<>();
      size = 0;
      this.sparseArrayCapacity = sparseArrayCapacity;
   }

   /**
    * Appends {@code e} to the end of this list.
    */
   public void add(T e) {
      Objects.requireNonNull(e, "e cannot be null");
      addToSparseArrayList(list, e, sparseArrayCapacity);
      size++;
   }

   /**
    * Removes any element of the list matching the given predicate.
    */
   public long remove(Predicate<? super T> filter) {
      if (size == 0) {
         return 0;
      }
      final long removed = removeFromSparseArrayList(list, filter);
      size -= removed;
      assert size >= 0;
      return removed;
   }

   /**
    * Clear while consuming (using the given {@code consumer} all the elements of this list.
    */
   public long clear(Consumer<? super T> consumer) {
      if (size == 0) {
         return 0;
      }
      final long removed = clearSparseArrayList(list, consumer);
      assert removed == size;
      size = 0;
      return removed;
   }

   /**
    * Returns the number of elements of this list.
    */
   public long size() {
      return size;
   }

   /**
    * Returns the configured capacity of each sparse array/chunk.
    */
   public int sparseArrayCapacity() {
      return sparseArrayCapacity;
   }

   /**
    * Returns the number of sparse arrays/chunks of this list.
    */
   public int sparseArraysCount() {
      return list.size();
   }
}
