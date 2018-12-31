/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.impl;

import org.apache.activemq.artemis.core.server.PriorityAware;

import java.lang.reflect.Array;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * This class's purpose is to hold the consumers, it models around multi getPriority (getPriority) varient of
 * java.util.concurrent.CopyOnWriteArrayList, so that reads are concurrent safe and non blocking.
 *
 * N.b. we could have made Level extend CopyOnWriteArrayList but due to the need to access the internal Array structure,
 * which is privileged to package java.util.concurrent. As such much of Level is is taken from here.
 *
 * Modifications like in CopyOnWriteArrayList are single threaded via a single re-entrant lock.
 *
 * Iterators iterate over a snapshot of the internal array structure, so will not see mutations.
 *
 * There can only be one resettable iterable view, this is exposed at the top getPriority,
 *     and is intended for use in QueueImpl only.
 * All other iterators are not reset-able and are created on calling iterator().
 *
 * Methods getArray, setArray MUST never be exposed, and all array modifications must go through these.
 *
 * @param <T> The type this class may hold, this is generic as can be anything that extends PriorityAware,
 *         but intent is this is the QueueImpl:ConsumerHolder.
 */
public class QueueConsumersImpl<T extends PriorityAware> extends AbstractCollection<T> implements QueueConsumers<T> {

   private final QueueConsumersIterator<T> iterator = new QueueConsumersIterator<>(this, true);

   private volatile Level<T>[] levels;
   private volatile int size;

   private void setArray(Level<T>[] array) {
      this.levels = array;
   }

   private Level<T>[] getArray() {
      return levels;
   }


   public QueueConsumersImpl() {
      levels = newLevelArrayInstance(0);
   }

   @SuppressWarnings("unchecked")
   private static <T> Level<T>[] newLevelArrayInstance(int length) {
      return (Level<T>[]) Array.newInstance(Level.class, length);
   }

   @Override
   public int size() {
      return size;
   }

   @Override
   public boolean isEmpty() {
      return size() == 0;
   }

   @Override
   public Set<Integer> getPriorites() {
      Level<T>[] levels = getArray();
      return Arrays.stream(levels).map(Level::level).collect(Collectors.toSet());
   }

   @Override
   public Iterator<T> iterator() {
      return new QueueConsumersIterator<>(this, false);
   }

   @Override
   public boolean hasNext() {
      return iterator.hasNext();
   }

   @Override
   public T next() {
      return iterator.next();
   }

   @Override
   public QueueConsumers<T> reset() {
      iterator.reset();
      return this;
   }

   @Override
   public void forEach(Consumer<? super T> action) {
      Objects.requireNonNull(action);
      Level<T>[] current = getArray();
      int len = current.length;
      for (int i = 0; i < len; ++i) {
         current[i].forEach(action);
      }
   }

   private Level<T> getLevel(int level, boolean createIfMissing) {
      Level<T>[] current = getArray();
      int low = 0;
      int high = current.length - 1;

      while (low <= high) {
         int mid = (low + high) >>> 1;
         Level<T> midVal = current[mid];

         if (midVal.level() > level)
            low = mid + 1;
         else if (midVal.level() < level)
            high = mid - 1;
         else
            return midVal; //key found
      }

      if (createIfMissing) {
         Level<T>[] newLevels = newLevelArrayInstance(current.length + 1);
         if (low > 0) {
            System.arraycopy(current, 0, newLevels, 0, low);
         }
         if (current.length - low > 0) {
            System.arraycopy(current, low, newLevels, low + 1, current.length - low);
         }
         newLevels[low] = new Level<T>(level);
         setArray(newLevels);
         return newLevels[low];
      }
      return null;
   }

   @Override
   public synchronized boolean add(T t) {
      boolean result = addInternal(t);
      calcSize();
      return result;
   }

   private boolean addInternal(T t) {
      if (t == null) return false;
      Level<T> level = getLevel(t.getPriority(), true);
      return level.add(t);
   }

   @Override
   public boolean remove(Object o) {
      return o instanceof PriorityAware && remove((PriorityAware) o);
   }

   public synchronized boolean remove(PriorityAware priorityAware) {
      boolean result = removeInternal(priorityAware);
      calcSize();
      return result;
   }

   private boolean removeInternal(PriorityAware priorityAware) {
      if ( priorityAware == null) return false;
      Level<T> level = getLevel(priorityAware.getPriority(), false);
      boolean result = level != null && level.remove(priorityAware);
      if (level != null && level.size() == 0) {
         removeLevel(level.level);
      }
      return result;
   }

   private Level<T> removeLevel(int level) {
      Level<T>[] current = getArray();
      int len = current.length;
      int low = 0;
      int high = len - 1;

      while (low <= high) {
         int mid = (low + high) >>> 1;
         Level<T> midVal = current[mid];

         if (midVal.level() > level)
            low = mid + 1;
         else if (midVal.level() < level)
            high = mid - 1;
         else {
            Level<T>[] newLevels = newLevelArrayInstance(len - 1);
            System.arraycopy(current, 0, newLevels, 0, mid);
            System.arraycopy(current, mid + 1, newLevels, mid, len - mid - 1);
            setArray(newLevels);
            return midVal; //key found
         }
      }
      return null;
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      Objects.requireNonNull(c);
      for (Object e : c)
         if (!contains(e))
            return false;
      return true;
   }

   @Override
   public synchronized boolean addAll(Collection<? extends T> c) {
      Objects.requireNonNull(c);
      boolean modified = false;
      for (T e : c)
         if (addInternal(e))
            modified = true;
      calcSize();
      return modified;
   }

   @Override
   public synchronized boolean removeAll(Collection<?> c) {
      Objects.requireNonNull(c);
      boolean modified = false;
      for (Object o : c) {
         if (remove(o)) {
            modified = true;
         }
      }
      calcSize();
      return modified;
   }

   @Override
   public synchronized boolean retainAll(Collection<?> c) {
      Objects.requireNonNull(c);
      boolean modified = false;
      Level<T>[] levels = getArray();
      for (Level<T> level : levels) {
         if (level.retainAll(c)) {
            modified = true;
         }
      }
      calcSize();
      return modified;
   }

   @Override
   public synchronized void clear() {
      Level<T>[] levels = getArray();
      for (Level<T> level : levels) {
         level.clear();
      }
      calcSize();
   }



   @Override
   public boolean contains(Object o) {
      return o instanceof PriorityAware && contains((PriorityAware) o);
   }

   public boolean contains(PriorityAware priorityAware) {
      if (priorityAware == null) return false;
      Level<T> level = getLevel(priorityAware.getPriority(), false);
      return level != null && level.contains(priorityAware);
   }

   private void calcSize() {
      Level<T>[] current = getArray();
      int size = 0;
      for (Level<T> level : current) {
         size += level.size();
      }
      this.size = size;
   }

   private static class QueueConsumersIterator<T extends PriorityAware> implements ResetableIterator<T> {

      private final QueueConsumersImpl<T> queueConsumers;
      private final boolean resetable;
      private Level<T>[] levels;
      int level = -1;
      private ResetableIterator<T> currentIterator;

      private QueueConsumersIterator(QueueConsumersImpl<T> queueConsumers, boolean resetable) {
         this.queueConsumers = queueConsumers;
         this.levels = queueConsumers.getArray();
         this.resetable = resetable;

      }

      @Override
      public boolean hasNext() {
         while (true) {
            if (currentIterator != null) {
               if (currentIterator.hasNext()) {
                  return true;
               }
            }
            int nextLevel = level + 1;
            if (levels != null && nextLevel < levels.length) {
               moveToLevel(nextLevel);
            } else {
               return false;
            }
         }
      }

      @Override
      public T next() {
         while (true) {
            if (currentIterator != null) {
               if (currentIterator.hasNext()) {
                  return currentIterator.next();
               }
            }
            int nextLevel = level + 1;
            if (levels != null && nextLevel < levels.length) {
               moveToLevel(nextLevel);
            } else {
               return null;
            }
         }
      }

      private void moveToLevel(int level) {
         Level<T> level0 = levels[level];
         if (resetable) {
            currentIterator = level0.resetableIterator().reset();
         } else {
            currentIterator = level0.iterator();
         }
         this.level = level;
      }

      @Override
      public ResetableIterator<T> reset() {
         if (!resetable) {
            throw new IllegalStateException("Iterator is not resetable");
         }
         levels = queueConsumers.getArray();
         level = -1;
         currentIterator = null;
         return this;
      }
   }

   /**
    * This is represents a getPriority and is modeled on {@link java.util.concurrent.CopyOnWriteArrayList}.
    *
    * @param <E>
    */
   private static class Level<E> {

      /** The array, accessed only via getArray/setArray. */
      private transient volatile Object[] array;

      private transient volatile ResetableIterator<E> resetableIterator;

      private final int level;

      /**
       * Gets the array.  Non-private so as to also be accessible
       * from CopyOnWriteArraySet class.
       */
      private Object[] getArray() {
         return array;
      }

      /**
       * Sets the array.
       */
      private void setArray(Object[] a) {
         array = a;
         resetableIterator = new LevelResetableIterator<>(a);
      }

      /**
       * Creates an empty list.
       */
      private Level(int level) {
         setArray(new Object[0]);
         this.level = level;
      }

      public int level() {
         return level;
      }

      public void forEach(Consumer<? super E> action) {
         if (action == null) throw new NullPointerException();
         Object[] elements = getArray();
         for (Object element : elements) {
            @SuppressWarnings("unchecked") E e = (E) element;
            action.accept(e);
         }
      }

      /**
       * Returns the number of elements in this list.
       *
       * @return the number of elements in this list
       */
      public int size() {
         return getArray().length;
      }

      /**
       * Returns {@code true} if this list contains no elements.
       *
       * @return {@code true} if this list contains no elements
       */
      public boolean isEmpty() {
         return size() == 0;
      }

      /**
       * Returns {@code true} if this list contains the specified element.
       * More formally, returns {@code true} if and only if this list contains
       * at least one element {@code e} such that
       * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
       *
       * @param o element whose presence in this list is to be tested
       * @return {@code true} if this list contains the specified element
       */
      public boolean contains(Object o) {
         Object[] elements = getArray();
         return indexOf(o, elements, 0, elements.length) >= 0;
      }

      /**
       * Tests for equality, coping with nulls.
       */
      private static boolean eq(Object o1, Object o2) {
         return (o1 == null) ? o2 == null : o1.equals(o2);
      }

      /**
       * static version of indexOf, to allow repeated calls without
       * needing to re-acquire array each time.
       * @param o element to search for
       * @param elements the array
       * @param index first index to search
       * @param fence one past last index to search
       * @return index of element, or -1 if absent
       */
      private static int indexOf(Object o, Object[] elements,
                           int index, int fence) {
         if (o == null) {
            for (int i = index; i < fence; i++)
               if (elements[i] == null)
                  return i;
         } else {
            for (int i = index; i < fence; i++)
               if (o.equals(elements[i]))
                  return i;
         }
         return -1;
      }

      /**
       * Appends the specified element to the end of this list.
       *
       * @param e element to be appended to this list
       * @return {@code true} (as specified by {@link Collection#add})
       */
      public boolean add(E e) {
         Object[] elements = getArray();
         int len = elements.length;
         Object[] newElements = Arrays.copyOf(elements, len + 1);
         newElements[len] = e;
         setArray(newElements);
         return true;
      }

      /**
       * Removes the first occurrence of the specified element from this list,
       * if it is present.  If this list does not contain the element, it is
       * unchanged.  More formally, removes the element with the lowest index
       * {@code i} such that
       * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>
       * (if such an element exists).  Returns {@code true} if this list
       * contained the specified element (or equivalently, if this list
       * changed as a result of the call).
       *
       * @param o element to be removed from this list, if present
       * @return {@code true} if this list contained the specified element
       */
      public boolean remove(Object o) {
         Object[] snapshot = getArray();
         int index = indexOf(o, snapshot, 0, snapshot.length);
         return (index >= 0) && remove(o, snapshot, index);
      }

      /**
       * A version of remove(Object) using the strong hint that given
       * recent snapshot contains o at the given index.
       */
      private boolean remove(Object o, Object[] snapshot, int index) {
         Object[] current = getArray();
         int len = current.length;
         if (snapshot != current)
            findIndex: {
               int prefix = Math.min(index, len);
               for (int i = 0; i < prefix; i++) {
                  if (current[i] != snapshot[i] && eq(o, current[i])) {
                     index = i;
                     break findIndex;
                  }
               }
               if (index >= len)
                  return false;
               if (current[index] == o)
                  break findIndex;
               index = indexOf(o, current, index, len);
               if (index < 0)
                  return false;
            }
         Object[] newElements = new Object[len - 1];
         System.arraycopy(current, 0, newElements, 0, index);
         System.arraycopy(current, index + 1,
               newElements, index,
               len - index - 1);
         setArray(newElements);
         return true;
      }

      /**
       * Retains only the elements in this list that are contained in the
       * specified collection.  In other words, removes from this list all of
       * its elements that are not contained in the specified collection.
       *
       * @param c collection containing elements to be retained in this list
       * @return {@code true} if this list changed as a result of the call
       * @throws ClassCastException if the class of an element of this list
       *       is incompatible with the specified collection
       *       (<a href="../Collection.html#optional-restrictions">optional</a>)
       * @throws NullPointerException if this list contains a null element and the
       *       specified collection does not permit null elements
       *       (<a href="../Collection.html#optional-restrictions">optional</a>),
       *       or if the specified collection is null
       * @see #remove(Object)
       */
      public boolean retainAll(Collection<?> c) {
         if (c == null) throw new NullPointerException();
         Object[] elements = getArray();
         int len = elements.length;
         if (len != 0) {
            // temp array holds those elements we know we want to keep
            int newlen = 0;
            Object[] temp = new Object[len];
            for (int i = 0; i < len; ++i) {
               Object element = elements[i];
               if (c.contains(element))
                  temp[newlen++] = element;
            }
            if (newlen != len) {
               setArray(Arrays.copyOf(temp, newlen));
               return true;
            }
         }
         return false;
      }

      /**
       * Removes all of the elements from this list.
       * The list will be empty after this call returns.
       */
      public void clear() {
         setArray(new Object[0]);
      }

      private ResetableIterator<E> resetableIterator() {
         return resetableIterator;
      }

      public ResetableIterator<E> iterator() {
         return new LevelResetableIterator<>(getArray());
      }

      private static class LevelResetableIterator<T> implements ResetableIterator<T> {

         private final Object[] array;
         private int cursor = 0;
         private int endPos = -1;
         private boolean hasNext;

         private LevelResetableIterator(Object[] array) {
            this.array = array;
            reset();
         }

         @Override
         public ResetableIterator<T> reset() {
            endPos = cursor;
            hasNext = array.length > 0;
            return this;
         }

         @Override
         public boolean hasNext() {
            return hasNext;
         }

         @Override
         public T next() {
            if (!hasNext) {
               throw new IllegalStateException();
            }
            @SuppressWarnings("unchecked") T result = (T) array[cursor];
            cursor++;
            if (cursor == array.length) {
               cursor = 0;
            }
            if (cursor == endPos) {
               hasNext = false;
            }
            return result;
         }
      }
   }

}
