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
import java.util.Objects;

/**
 * A linked list implementation which allows multiple iterators to exist at the same time on the queue, and which see any
 * elements added or removed from the queue either directly or via iterators.
 *
 * This class is not thread safe.
 */
public class LinkedListImpl<E> implements LinkedList<E> {

   private static final int INITIAL_ITERATOR_ARRAY_SIZE = 10;

   private final Node<E> head = new NodeHolder<>(null);

   private Node<E> tail = null;

   private int size;

   // We store in an array rather than a Map for the best performance
   private volatile Iterator[] iters;

   private int numIters;

   private int nextIndex;

   public LinkedListImpl() {
      iters = createIteratorArray(INITIAL_ITERATOR_ARRAY_SIZE);
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

      size++;
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

         size++;
      }
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
      tail = head.next = null;

      size = 0;
   }

   @Override
   public int size() {
      return size;
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

      @SuppressWarnings("unchecked")
      protected T val() {
         return (T) this;
      }

      @Override
      public String toString() {
         return val() == this ? "Intrusive Node" : "Node, value = " + val();
      }

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
