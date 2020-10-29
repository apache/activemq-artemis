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
import java.util.Objects;
import java.util.function.ToLongFunction;

import io.netty.util.collection.LongObjectHashMap;

/**
 * A linked list implementation which allows multiple iterators to exist at the same time on the queue, and which see any
 * elements added or removed from the queue either directly or via iterators.
 * <p>
 * This class is not thread safe.
 */
public class LinkedListImpl<E> implements LinkedList<E> {

   private static final int INITIAL_ITERATOR_ARRAY_SIZE = 10;

   private final Node<E> head = new NodeHolder<>(null);
   private final Comparator<E> comparator;
   LongObjectHashMap<Node<E>> nodeMap;
   private Node<E> tail = null;
   private int size;
   // We store in an array rather than a Map for the best performance
   private volatile Iterator[] iters;
   private int numIters;
   private int nextIndex;
   private ToLongFunction<E> idSupplier;

   public LinkedListImpl() {
      this(null, null);
   }

   public LinkedListImpl(Comparator<E> comparator) {
      this(comparator, null);
   }

   public LinkedListImpl(Comparator<E> comparator, ToLongFunction<E> supplier) {
      iters = createIteratorArray(INITIAL_ITERATOR_ARRAY_SIZE);
      this.comparator = comparator;
      this.idSupplier = supplier;
      if (idSupplier != null) {
         this.nodeMap = newLongHashMap();
      } else {
         this.nodeMap = null;
      }
   }

   @Override
   public void clearID() {
      idSupplier = null;
      if (nodeMap != null) {
         nodeMap.clear();
         nodeMap = null;
      }
   }

   @Override
   public void setIDSupplier(ToLongFunction<E> supplier) {
      this.idSupplier = supplier;
      nodeMap = newLongHashMap();

      try (Iterator iterator = (Iterator) iterator()) {
         while (iterator.hasNext()) {
            E value = iterator.next();
            Node<E> position = iterator.last;
            putID(value, position);
         }
      }
   }

   private LongObjectHashMap<Node<E>> newLongHashMap() {
      return new LongObjectHashMap<>(Math.max(8, this.size));
   }

   private void putID(E value, Node<E> position) {
      long id = idSupplier.applyAsLong(value);
      if (id >= 0) {
         nodeMap.put(id, position);
      }
   }

   @Override
   public void addHead(E e) {
      Node<E> node = Node.with(e);

      node.next = head.next;

      node.prev = head;

      head.next = node;

      if (size == 0) {
         tail = node;
      } else {
         // Need to set the previous element on the former head
         node.next.prev = node;
      }

      itemAdded(node, e);

      size++;
   }

   @Override
   public E removeWithID(long id) {
      if (nodeMap == null) {
         return null;
      }

      Node<E> node = nodeMap.get(id);
      if (node == null) {
         return null;
      }

      // the node will always have a prev element
      removeAfter(node.prev);
      return node.val();
   }

   private void itemAdded(Node<E> node, E item) {
      if (nodeMap != null) {
         putID(item, node);
      }
   }

   private void itemRemoved(Node<E> node) {
      if (nodeMap != null) {
         long id = idSupplier.applyAsLong(node.val());
         if (id >= 0) {
            nodeMap.remove(id);
         }
      }
   }

   @Override
   public void addTail(E e) {
      if (size == 0) {
         addHead(e);
      } else {
         Node<E> node = Node.with(e);

         node.prev = tail;

         tail.next = node;

         tail = node;

         itemAdded(node, e);

         size++;
      }
   }

   public void addSorted(E e) {
      if (comparator == null) {
         throw new NullPointerException("comparator=null");
      }
      if (size == 0) {
         addHead(e);
      } else {
         if (comparator.compare(head.next.val(), e) < 0) {
            addHead(e);
            return;
         }

         // in our usage, most of the times we will just add to the end
         // as the QueueImpl cancellations in AMQP will return the buffer back to the queue, in the order they were consumed.
         // There is an exception to that case, when there are more messages on the queue.
         // This would be an optimization for our usage.
         // avoiding scanning the entire List just to add at the end, so we compare the end first.
         if (comparator.compare(tail.val(), e) >= 0) {
            addTail(e);
            return;
         }

         Node<E> fetching = head.next;
         while (fetching.next != null) {
            int compareNext = comparator.compare(fetching.next.val(), e);
            if (compareNext <= 0) {
               addAfter(fetching, e);
               return;
            }
            fetching = fetching.next;
         }

         // this shouldn't happen as the tail was compared before iterating
         // the only possibilities for this to happen are:
         // - there is a bug on the comparator
         // - This method is buggy
         // - The list wasn't properly synchronized as this list does't support concurrent access
         //
         // Also I'm not bothering about creating a Logger ID for this, because the only reason for this code to exist
         //      is because my OCD level is not letting this out.
         throw new IllegalStateException("Cannot find a suitable place for your element, There's a mismatch in the comparator or there was concurrent adccess on the queue");
      }
   }

   private void addAfter(Node<E> node, E e) {
      Node<E> newNode = Node.with(e);
      Node<E> nextNode = node.next;
      node.next = newNode;
      newNode.prev = node;
      newNode.next = nextNode;
      nextNode.prev = newNode;
      itemAdded(node, e);
      size++;
   }

   @Override
   public E poll() {
      Node<E> ret = head.next;

      if (ret != null) {
         removeAfter(head);

         return ret.val();
      } else {
         return null;
      }
   }

   @Override
   public void clear() {
      // Clearing all of the links between nodes is "unnecessary", but:
      // - helps a generational GC if the discarded nodes inhabit
      //   more than one generation
      // - is sure to free memory even if there is a reachable Iterator
      while (poll() != null) {

      }
   }

   @Override
   public int size() {
      return size;
   }

   /**
    * Return the number of elements we have on suppliedIDs
    */
   public int getSizeOfSuppliedIDs() {
      return nodeMap == null ? 0 : nodeMap.size();
   }

   @Override
   public LinkedListIterator<E> iterator() {
      return new Iterator();
   }

   @Override
   public String toString() {
      StringBuilder str = new StringBuilder("LinkedListImpl [ ");

      Node<E> node = head;

      while (node != null) {
         str.append(node.toString());

         if (node.next != null) {
            str.append(", ");
         }

         node = node.next;
      }

      return str.toString();
   }

   public int numIters() {
      return numIters;
   }

   private Iterator[] createIteratorArray(int size) {
      return (Iterator[]) Array.newInstance(Iterator.class, size);
   }

   private void removeAfter(Node<E> node) {
      Node<E> toRemove = node.next;

      node.next = toRemove.next;

      if (toRemove.next != null) {
         toRemove.next.prev = node;
      }

      itemRemoved(toRemove);

      if (toRemove == tail) {
         tail = node;
      }

      size--;

      if (toRemove.iterCount != 0) {
         LinkedListImpl.this.nudgeIterators(toRemove);
      }

      //Help GC - otherwise GC potentially has to traverse a very long list to see if elements are reachable, this can result in OOM
      //https://jira.jboss.org/browse/HORNETQ-469
      toRemove.next = toRemove.prev = null;
   }

   private synchronized void nudgeIterators(Node<E> node) {
      for (int i = 0; i < numIters; i++) {
         Iterator iter = iters[i];
         if (iter != null) {
            iter.nudged(node);
         }
      }
   }

   private synchronized void addIter(Iterator iter) {
      if (numIters == iters.length) {
         resize(2 * numIters);
      }

      iters[nextIndex++] = iter;

      numIters++;
   }

   private synchronized void resize(int newSize) {
      Iterator[] newIters = createIteratorArray(newSize);

      System.arraycopy(iters, 0, newIters, 0, numIters);

      iters = newIters;
   }

   private synchronized void removeIter(Iterator iter) {
      for (int i = 0; i < numIters; i++) {
         if (iter == iters[i]) {
            iters[i] = null;

            if (i != numIters - 1) {
               // Fill in the hole

               System.arraycopy(iters, i + 1, iters, i, numIters - i - 1);
            }

            numIters--;

            if (numIters >= INITIAL_ITERATOR_ARRAY_SIZE && numIters == iters.length / 2) {
               resize(numIters);
            }

            nextIndex--;

            return;
         }
      }

      throw new IllegalStateException("Cannot find iter to remove");
   }

   private static final class NodeHolder<T> extends Node<T> {

      private final T val;

      //only the head is allowed to hold a null
      private NodeHolder(T e) {
         val = e;
      }

      @Override
      protected T val() {
         return val;
      }
   }

   public static class Node<T> {

      private Node<T> next;

      private Node<T> prev;

      private int iterCount;

      private static <T> Node<T> with(final T o) {
         Objects.requireNonNull(o, "Only HEAD nodes are allowed to hold null values");
         if (o instanceof Node) {
            final Node node = (Node) o;
            //only a node that not belong already to a list is allowed to be reused
            if (node.prev == null && node.next == null) {
               //reset the iterCount
               node.iterCount = 0;
               return node;
            }
         }
         return new NodeHolder<>(o);
      }

      @SuppressWarnings("unchecked")
      protected T val() {
         return (T) this;
      }

      protected final LinkedListImpl.Node<T> next() {
         return next;
      }

      protected final LinkedListImpl.Node<T> prev() {
         return prev;
      }

      @Override
      public String toString() {
         return val() == this ? "Intrusive Node" : "Node, value = " + val();
      }
   }

   private class Iterator implements LinkedListIterator<E> {

      Node<E> last;

      Node<E> current = head.next;

      boolean repeat;

      Iterator() {
         if (current != null) {
            current.iterCount++;
         }

         addIter(this);
      }

      @Override
      public void repeat() {
         repeat = true;
      }

      @Override
      public boolean hasNext() {
         Node<E> e = getNode();

         if (e != null && (e != last || repeat)) {
            return true;
         }

         return canAdvance();
      }

      @Override
      public E next() {
         Node<E> e = getNode();

         if (repeat) {
            repeat = false;

            if (e != null) {
               return e.val();
            } else {
               if (canAdvance()) {
                  advance();

                  e = getNode();

                  return e.val();
               } else {
                  throw new NoSuchElementException();
               }
            }
         }

         if (e == null || e == last) {
            if (canAdvance()) {
               advance();

               e = getNode();
            } else {
               throw new NoSuchElementException();
            }
         }

         last = e;

         repeat = false;

         return e.val();
      }

      @Override
      public void remove() {
         if (last == null) {
            throw new NoSuchElementException();
         }

         if (current == null) {
            throw new NoSuchElementException();
         }

         LinkedListImpl.this.removeAfter(current.prev);

         last = null;
      }

      @Override
      public void close() {
         removeIter(this);
      }

      public void nudged(Node<E> node) {
         if (current == node) {
            if (canAdvance()) {
               advance();
            } else {
               if (current.prev != head) {
                  current.iterCount--;

                  current = current.prev;

                  current.iterCount++;
               } else {
                  current = null;
               }
            }
         }
      }

      private Node<E> getNode() {
         if (current == null) {
            current = head.next;

            if (current != null) {
               current.iterCount++;
            }
         }

         if (current != null) {
            return current;
         } else {
            return null;
         }
      }

      private boolean canAdvance() {
         if (current == null) {
            current = head.next;

            if (current != null) {
               current.iterCount++;
            }
         }

         return current != null && current.next != null;
      }

      private void advance() {
         if (current == null || current.next == null) {
            throw new NoSuchElementException();
         }

         current.iterCount--;

         current = current.next;

         current.iterCount++;
      }

   }
}
