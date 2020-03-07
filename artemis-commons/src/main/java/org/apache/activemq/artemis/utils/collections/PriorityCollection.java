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
package org.apache.activemq.artemis.utils.collections;

import org.apache.activemq.artemis.core.PriorityAware;

import java.lang.reflect.Array;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class's purpose is to hold the the different collections used for each priority level.
 *
 * A supplier is required to provide the underlying collection needed when a new priority level is seen,
 * and the end behaviour is that of the underlying collection, e.g. if set add will follow set's add semantics,
 * if list, then list semantics.
 *
 * Methods getArray, setArray MUST never be exposed, and all array modifications must go through these.
 *
 * @param <T> The type this class may hold, this is generic as can be anything that extends PriorityAware.
 */
public class PriorityCollection<T extends PriorityAware> extends AbstractCollection<T> {

   private final Supplier<Collection<T>> supplier;
   private volatile PriorityHolder<T>[] priorityHolders = newPrioritySetArrayInstance(0);
   private volatile int size;

   private void setArray(PriorityHolder<T>[] priorityHolders) {
      this.priorityHolders = priorityHolders;
   }

   private PriorityHolder<T>[] getArray() {
      return priorityHolders;
   }


   public PriorityCollection(Supplier<Collection<T>> supplier) {
      this.supplier = supplier;
   }

   @SuppressWarnings("unchecked")
   private static <T> PriorityHolder<T>[] newPrioritySetArrayInstance(int length) {
      return (PriorityHolder<T>[]) Array.newInstance(PriorityHolder.class, length);
   }

   @Override
   public int size() {
      return size;
   }

   @Override
   public boolean isEmpty() {
      return size() == 0;
   }

   public Set<Integer> getPriorites() {
      PriorityHolder<T>[] snapshot = getArray();
      return Arrays.stream(snapshot).map(PriorityAware::getPriority).collect(Collectors.toSet());
   }

   @Override
   public Iterator<T> iterator() {
      Iterator<T>[] iterators = getIterators();
      return new MultiIterator<>(iterators);
   }

   private Iterator<T>[] getIterators() {
      PriorityHolder<T>[] snapshot = this.getArray();
      int size = snapshot.length;
      Iterator<T>[] iterators = newIteratorArrayInstance(size);
      for (int i = 0; i < size; i++) {
         iterators[i] = snapshot[i].getValues().iterator();
      }
      return iterators;
   }

   @SuppressWarnings("unchecked")
   private static <T> Iterator<T>[] newIteratorArrayInstance(int length) {
      return (Iterator<T>[]) Array.newInstance(Iterator.class, length);
   }

   public ResettableIterator<T> resettableIterator() {
      return new MultiResettableIterator<T>(getResettableIterators());
   }

   private ResettableIterator<T>[] getResettableIterators() {
      PriorityHolder<T>[] snapshot = this.getArray();
      int size = snapshot.length;
      ResettableIterator<T>[] iterators = newResettableIteratorArrayInstance(size);
      for (int i = 0; i < size; i++) {
         iterators[i] = ArrayResettableIterator.iterator(snapshot[i].getValues());
      }
      return iterators;
   }

   @SuppressWarnings("unchecked")
   private static <T> ResettableIterator<T>[] newResettableIteratorArrayInstance(int length) {
      return (ResettableIterator<T>[]) Array.newInstance(ResettableIterator.class, length);
   }

   @Override
   public void forEach(Consumer<? super T> action) {
      Objects.requireNonNull(action);
      PriorityHolder<T>[] current = getArray();
      int len = current.length;
      for (int i = 0; i < len; ++i) {
         current[i].getValues().forEach(action);
      }
   }

   private Collection<T> getCollection(int priority, boolean createIfMissing) {
      PriorityHolder<T>[] current = getArray();
      int low = 0;
      int high = current.length - 1;

      while (low <= high) {
         int mid = (low + high) >>> 1;
         PriorityHolder<T> midVal = current[mid];

         if (midVal.getPriority() > priority)
            low = mid + 1;
         else if (midVal.getPriority() < priority)
            high = mid - 1;
         else
            return midVal.getValues(); //key found
      }

      if (createIfMissing) {
         PriorityHolder<T>[] newArray = newPrioritySetArrayInstance(current.length + 1);
         if (low > 0) {
            System.arraycopy(current, 0, newArray, 0, low);
         }
         if (current.length - low > 0) {
            System.arraycopy(current, low, newArray, low + 1, current.length - low);
         }
         newArray[low] = new PriorityHolder<T>(priority, supplier);
         setArray(newArray);
         return newArray[low].getValues();
      }
      return null;
   }

   @Override
   public synchronized boolean add(T t) {
      if (size() == Integer.MAX_VALUE) return false;
      boolean result = addInternal(t);
      calcSize();
      return result;
   }

   private boolean addInternal(T t) {
      if (t == null) return false;
      Collection<T> priority = getCollection(t.getPriority(), true);
      return priority.add(t);
   }

   @Override
   public synchronized boolean remove(Object o) {
      boolean result = removeInternal(o);
      calcSize();
      return result;
   }

   private boolean removeInternal(Object o) {
      if (o instanceof PriorityAware) {
         PriorityAware priorityAware = (PriorityAware) o;
         Collection<T> priority = getCollection(priorityAware.getPriority(), false);
         boolean result = priority != null && priority.remove(priorityAware);
         if (priority != null && priority.isEmpty()) {
            removeCollection(priorityAware.getPriority());
         }
         return result;
      } else {
         return false;
      }
   }

   private Collection<T> removeCollection(int priority) {
      PriorityHolder<T>[] current = getArray();
      int len = current.length;
      int low = 0;
      int high = len - 1;

      while (low <= high) {
         int mid = (low + high) >>> 1;
         PriorityHolder<T> midVal = current[mid];

         if (midVal.getPriority() > priority)
            low = mid + 1;
         else if (midVal.getPriority() < priority)
            high = mid - 1;
         else {
            PriorityHolder<T>[] newArray = newPrioritySetArrayInstance(len - 1);
            System.arraycopy(current, 0, newArray, 0, mid);
            System.arraycopy(current, mid + 1, newArray, mid, len - mid - 1);
            setArray(newArray);
            return midVal.getValues(); //key found
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
      if (size() >= Integer.MAX_VALUE - c.size()) return false;
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
         if (removeInternal(o)) {
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
      PriorityHolder<T>[] snapshot = getArray();
      for (PriorityHolder<T> priorityHolder : snapshot) {
         if (priorityHolder.getValues().retainAll(c)) {
            modified = true;
            if (priorityHolder.getValues().isEmpty()) {
               removeCollection(priorityHolder.getPriority());
            }
         }
      }
      calcSize();
      return modified;
   }

   @Override
   public synchronized void clear() {
      PriorityHolder<T>[] snapshot = getArray();
      for (PriorityHolder<T> priorityHolder : snapshot) {
         priorityHolder.getValues().clear();
      }
      calcSize();
   }

   @Override
   public boolean contains(Object o) {
      return o instanceof PriorityAware && contains((PriorityAware) o);
   }

   public boolean contains(PriorityAware priorityAware) {
      if (priorityAware == null) return false;
      Collection<T> prioritySet = getCollection(priorityAware.getPriority(), false);
      return prioritySet != null && prioritySet.contains(priorityAware);
   }

   private void calcSize() {
      PriorityHolder<T>[] current = getArray();
      int size = 0;
      for (PriorityHolder<T> priorityHolder : current) {
         size += priorityHolder.getValues().size();
      }
      this.size = size;
   }

   public static class PriorityHolder<E> implements PriorityAware {

      private final int priority;

      private final Collection<E> values;

      public PriorityHolder(int priority, Supplier<Collection<E>> supplier) {
         this.priority = priority;
         this.values = supplier.get();
      }

      @Override
      public int getPriority() {
         return priority;
      }

      public Collection<E> getValues() {
         return values;
      }
   }
}
