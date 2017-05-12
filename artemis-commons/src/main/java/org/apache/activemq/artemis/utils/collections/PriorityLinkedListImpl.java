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
import java.util.NoSuchElementException;

/**
 * A priority linked list implementation
 * <p>
 * It implements this by maintaining an individual LinkedBlockingDeque for each priority level.
 */
public class PriorityLinkedListImpl<T> implements PriorityLinkedList<T> {

   protected LinkedListImpl<T>[] levels;

   private int size;

   private int lastReset;

   private int highestPriority = -1;

   private int lastPriority = -1;

   public PriorityLinkedListImpl(final int priorities) {
      levels = (LinkedListImpl<T>[]) Array.newInstance(LinkedListImpl.class, priorities);

      for (int i = 0; i < priorities; i++) {
         levels[i] = new LinkedListImpl<>();
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
   public void addHead(final T t, final int priority) {
      checkHighest(priority);

      levels[priority].addHead(t);

      size++;
   }

   @Override
   public void addTail(final T t, final int priority) {
      checkHighest(priority);

      levels[priority].addTail(t);

      size++;
   }

   @Override
   public T poll() {
      T t = null;

      // We are just using a simple prioritization algorithm:
      // Highest priority refs always get returned first.
      // This could cause starvation of lower priority refs.

      // TODO - A better prioritization algorithm

      for (int i = highestPriority; i >= 0; i--) {
         LinkedListImpl<T> ll = levels[i];

         if (ll.size() != 0) {
            t = ll.poll();

            if (t != null) {
               size--;

               if (ll.size() == 0) {
                  if (highestPriority == i) {
                     highestPriority--;
                  }
               }
            }

            break;
         }
      }

      return t;
   }

   @Override
   public void clear() {
      for (LinkedListImpl<T> list : levels) {
         list.clear();
      }

      size = 0;
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
   public LinkedListIterator<T> iterator() {
      return new PriorityLinkedListIterator();
   }

   private class PriorityLinkedListIterator implements LinkedListIterator<T> {

      private int index;

      private final LinkedListIterator<T>[] cachedIters = new LinkedListIterator[levels.length];

      private LinkedListIterator<T> lastIter;

      private int resetCount = lastReset;

      volatile boolean closed = false;

      PriorityLinkedListIterator() {
         index = levels.length - 1;
      }

      @Override
      protected void finalize() {
         close();
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

            for (LinkedListIterator<T> iter : cachedIters) {
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
      public T next() {
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

         size--;
      }
   }
}
