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

import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A priority linked list implementation
 * <p>
 * It implements this by maintaining an individual LinkedBlockingDeque for each priority level.
 */
public class PriorityLinkedListImpl<E> implements PriorityLinkedList<E> {

   private static final AtomicIntegerFieldUpdater<PriorityLinkedListImpl> SIZE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(PriorityLinkedListImpl.class, "size");

   protected final LinkedListImpl<E>[] levels;

   private volatile int size;

   private int lastReset;

   private int highestPriority = -1;

   private int lastPriority = -1;

   public PriorityLinkedListImpl(final int priorities) {
      this(priorities, null);
   }


   public PriorityLinkedListImpl(final int priorities, Comparator<E> comparator) {
      levels = (LinkedListImpl<E>[]) Array.newInstance(LinkedListImpl.class, priorities);

      for (int i = 0; i < priorities; i++) {
         levels[i] = new LinkedListImpl<>(comparator);
      }
   }

   private void checkHighest(final int priority) {
      if (lastPriority != priority || priority > highestPriority) {
         lastPriority = priority;
         if (lastReset == Integer.MAX_VALUE) {
            lastReset = 0;
         } else {
            lastReset++;
         }
      }

      if (priority > highestPriority) {
         highestPriority = priority;
      }
   }

   @Override
   public void addHead(final E e, final int priority) {
      checkHighest(priority);

      levels[priority].addHead(e);

      exclusiveIncrementSize(1);
   }

   @Override
   public void addTail(final E e, final int priority) {
      checkHighest(priority);

      levels[priority].addTail(e);

      exclusiveIncrementSize(1);
   }

   @Override
   public void addSorted(E e, int priority) {
      checkHighest(priority);

      levels[priority].addSorted(e);

      exclusiveIncrementSize(1);
   }

   @Override
   public void setNodeStore(NodeStore<E> supplier) {
      for (LinkedList<E> list : levels) {
         list.setNodeStore(supplier);
      }
   }

   @Override
   public E removeWithID(String listID, long id) {
      // we start at 4 just as an optimization, since most times we only use level 4 as the level on messages
      if (levels.length > 4) {
         for (int l = 4; l < levels.length; l++) {
            E removed = levels[l].removeWithID(listID, id);
            if (removed != null) {
               exclusiveIncrementSize(-1);
               return removed;
            }
         }
      }

      for (int l = Math.min(3, levels.length); l >= 0; l--) {
         E removed = levels[l].removeWithID(listID, id);
         if (removed != null) {
            exclusiveIncrementSize(-1);
            return removed;
         }
      }

      return null;
   }


   @Override
   public E poll() {
      E e = null;

      // We are just using a simple prioritization algorithm:
      // Highest priority refs always get returned first.
      // This could cause starvation of lower priority refs.

      // TODO - A better prioritization algorithm

      for (int i = highestPriority; i >= 0; i--) {
         LinkedListImpl<E> ll = levels[i];

         if (ll.size() != 0) {
            e = ll.poll();

            if (e != null) {
               exclusiveIncrementSize(-1);

               if (ll.size() == 0) {
                  if (highestPriority == i) {
                     highestPriority--;
                  }
               }
            }

            break;
         }
      }

      return e;
   }

   @Override
   public void clear() {
      for (LinkedListImpl<E> list : levels) {
         list.clear();
      }

      exclusiveSetSize(0);
   }

   private void exclusiveIncrementSize(int amount) {
      SIZE_UPDATER.lazySet(this, this.size + amount);
   }

   private void exclusiveSetSize(int value) {
      SIZE_UPDATER.lazySet(this, value);
   }

   @Override
   public int size() {
      return size;
   }

   @Override
   public boolean isEmpty() {
      return size == 0;
   }

   @Override
   public LinkedListIterator<E> iterator() {
      return new PriorityLinkedListIterator();
   }

   private class PriorityLinkedListIterator implements LinkedListIterator<E> {

      private int index;

      private final LinkedListIterator<E>[] cachedIters = new LinkedListIterator[levels.length];

      private LinkedListIterator<E> lastIter;

      private int resetCount = lastReset;

      volatile boolean closed = false;

      PriorityLinkedListIterator() {
         index = levels.length - 1;
      }

      @Override
      public void repeat() {
         if (lastIter == null) {
            throw new NoSuchElementException();
         }

         lastIter.repeat();
      }

      @Override
      public void close() {
         if (!closed) {
            closed = true;
            lastIter = null;

            for (LinkedListIterator<E> iter : cachedIters) {
               if (iter != null) {
                  iter.close();
               }
            }
         }
      }

      private void checkReset() {
         if (lastReset != resetCount) {
            index = highestPriority;

            resetCount = lastReset;
         }
      }

      @Override
      public boolean hasNext() {
         checkReset();

         while (index >= 0) {
            lastIter = cachedIters[index];

            if (lastIter == null) {
               lastIter = cachedIters[index] = levels[index].iterator();
            }

            boolean b = lastIter.hasNext();

            if (b) {
               return true;
            }

            index--;

            if (index < 0) {
               index = levels.length - 1;

               break;
            }
         }
         return false;
      }

      @Override
      public E next() {
         if (lastIter == null) {
            throw new NoSuchElementException();
         }

         return lastIter.next();
      }

      @Override
      public void remove() {
         if (lastIter == null) {
            throw new NoSuchElementException();
         }

         lastIter.remove();

         // This next statement would be the equivalent of:
         // if (index == highestPriority && levels[index].size() == 0)
         // However we have to keep checking all the previous levels
         // otherwise we would cache a max that will not exist
         // what would make us eventually having hasNext() returning false
         // as a bug
         // Part of the fix for HORNETQ-705
         for (int i = index; i >= 0 && levels[index].size() == 0; i--) {
            highestPriority = i;
         }

         exclusiveIncrementSize(-1);
      }
   }
}
