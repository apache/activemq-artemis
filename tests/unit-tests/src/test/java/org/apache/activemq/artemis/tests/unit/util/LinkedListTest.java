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
package org.apache.activemq.artemis.tests.unit.util;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.utils.collections.LinkedListImpl;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LinkedListTest extends ActiveMQTestBase {

   private LinkedListImpl<Integer> list;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      list = new LinkedListImpl<>(integerComparator);
   }

   Comparator<Integer> integerComparator = new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
         if (o1.intValue() == o2.intValue()) {
            return 0;
         }
         if (o2.intValue() > o1.intValue()) {
            return 1;
         } else {
            return -1;
         }
      }
   };

   @Test
   public void addSorted() {

      list.addSorted(1);
      list.addSorted(3);
      list.addSorted(2);
      list.addSorted(0);
      validateOrder(null);
      Assert.assertEquals(4, list.size());

   }


   @Test
   public void randomSorted() {

      HashSet<Integer> values = new HashSet<>();
      for (int i = 0; i < 1000; i++) {

         int value = RandomUtil.randomInt();
         if (!values.contains(value)) {
            values.add(value);
            list.addSorted(value);
         }
      }

      Assert.assertEquals(values.size(), list.size());

      validateOrder(values);

      Assert.assertEquals(0, values.size());

   }

   private void validateOrder(HashSet<Integer> values) {
      Integer previous = null;
      LinkedListIterator<Integer> integerIterator = list.iterator();
      while (integerIterator.hasNext()) {

         Integer value = integerIterator.next();
         if (previous != null) {
            Assert.assertTrue(value + " should be > " + previous, integerComparator.compare(previous, value) > 0);
            Assert.assertTrue(value + " should be > " + previous, value.intValue() > previous.intValue());
         }

         if (values != null) {
            values.remove(value);
         }
         previous = value;
      }
      integerIterator.close();
   }

   private static final class ObservableNode extends LinkedListImpl.Node<ObservableNode> {

      public int id;
      ObservableNode(int id) {
         this.id = id;
      }

      public LinkedListImpl.Node<ObservableNode> publicNext() {
         return next();
      }

      public LinkedListImpl.Node<ObservableNode> publicPrev() {
         return prev();
      }

   }


   @Test
   public void testAddAndRemove() {
      LinkedListImpl<ObservableNode> objs = new LinkedListImpl<>();

      // Initial add
      for (int i = 0; i < 100; i++) {
         final ObservableNode o = new ObservableNode(i);
         objs.addTail(o);
      }

      try (LinkedListIterator<ObservableNode> iter = objs.iterator()) {

         for (int i = 0; i < 500; i++) {

            for (int add = 0; add < 1000; add++) {
               final ObservableNode o = new ObservableNode(add);
               objs.addTail(o);
               assertNotNull("prev", o.publicPrev());
               assertNull("next", o.publicNext());
            }

            for (int remove = 0; remove < 1000; remove++) {
               final ObservableNode next = iter.next();
               assertNotNull(next);
               assertNotNull("prev", next.publicPrev());
               //it's ok to check this, because we've *at least* 100 elements left!
               assertNotNull("next", next.publicNext());
               iter.remove();
               assertNull("prev", next.publicPrev());
               assertNull("next", next.publicNext());
            }
            assertEquals(100, objs.size());
         }

         while (iter.hasNext()) {
            final ObservableNode next = iter.next();
            assertNotNull(next);
            iter.remove();
            assertNull("prev", next.publicPrev());
            assertNull("next", next.publicNext());
         }
      }
      assertEquals(0, objs.size());

   }


   @Test
   public void testAddAndRemoveWithIDs() {
      internalAddWithID(true);
   }

   @Test
   public void testAddAndRemoveWithIDsDeferredSupplier() {
      internalAddWithID(false);
   }

   private void internalAddWithID(boolean deferSupplier) {
      LinkedListImpl<ObservableNode> objs = new LinkedListImpl<>();

      if (!deferSupplier) {
         objs.setIDSupplier(source -> source.id);
      }

      // Initial add
      for (int i = 0; i < 1000; i++) {
         final ObservableNode o = new ObservableNode(i);
         objs.addTail(o);
      }

      Assert.assertEquals(1000, objs.size());


      if (deferSupplier) {
         Assert.assertEquals(0, objs.getSizeOfSuppliedIDs());
         objs.setIDSupplier(source -> source.id);
      } else {
         // clear the ID supplier
         objs.clearID();
         // and redo it
         Assert.assertEquals(0, objs.getSizeOfSuppliedIDs());
         objs.setIDSupplier(source -> source.id);
         Assert.assertEquals(1000, objs.size());
      }

      Assert.assertEquals(1000, objs.getSizeOfSuppliedIDs());


      /** remove all even items */
      for (int i = 0; i < 1000; i += 2) {
         objs.removeWithID(i);
      }

      Assert.assertEquals(500, objs.size());
      Assert.assertEquals(500, objs.getSizeOfSuppliedIDs());

      Iterator<ObservableNode> iterator = objs.iterator();

      {
         int i = 1;
         while (iterator.hasNext()) {
            ObservableNode value = iterator.next();
            Assert.assertEquals(i, value.id);
            i += 2;
         }
      }

      for (int i = 1; i < 1000; i += 2) {
         objs.removeWithID(i);
      }

      Assert.assertEquals(0, objs.getSizeOfSuppliedIDs());
      Assert.assertEquals(0, objs.size());


   }

   @Test
   public void testAddHeadAndRemove() {
      LinkedListImpl<ObservableNode> objs = new LinkedListImpl<>();

      // Initial add
      for (int i = 0; i < 1001; i++) {
         final ObservableNode o = new ObservableNode(i);
         objs.addHead(o);
      }
      assertEquals(1001, objs.size());

      int countLoop = 0;

      try (LinkedListIterator<ObservableNode> iter = objs.iterator()) {
         int removed = 0;
         for (countLoop = 0; countLoop <= 1000; countLoop++) {
            final ObservableNode obj = iter.next();
            Assert.assertNotNull(obj);
            if (countLoop == 500 || countLoop == 1000) {
               assertNotNull("prev", obj.publicPrev());
               iter.remove();
               assertNull("prev", obj.publicPrev());
               assertNull("next", obj.publicNext());
               removed++;
            }
         }
         assertEquals(1001 - removed, objs.size());
      }

      final int expectedSize = objs.size();
      try (LinkedListIterator<ObservableNode> iter = objs.iterator()) {
         countLoop = 0;
         while (iter.hasNext()) {
            final ObservableNode obj = iter.next();
            assertNotNull(obj);
            countLoop++;
         }
         Assert.assertEquals(expectedSize, countLoop);
      }
      // it's needed to add this line here because IBM JDK calls finalize on all objects in list
      // before previous assert is called and fails the test, this will prevent it
      objs.clear();
   }

   @Test
   public void testAddTail() {
      int num = 10;

      assertEquals(0, list.size());

      for (int i = 0; i < num; i++) {
         list.addTail(i);

         assertEquals(i + 1, list.size());
      }

      for (int i = 0; i < num; i++) {
         assertEquals(i, list.poll().intValue());

         assertEquals(num - i - 1, list.size());
      }
   }

   @Test
   public void testAddHead() {
      int num = 10;

      assertEquals(0, list.size());

      for (int i = 0; i < num; i++) {
         list.addHead(i);

         assertEquals(i + 1, list.size());
      }

      for (int i = num - 1; i >= 0; i--) {
         assertEquals(i, list.poll().intValue());

         assertEquals(i, list.size());
      }
   }

   @Test
   public void testAddHeadAndTail() {
      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addHead(i);
      }

      for (int i = num; i < num * 2; i++) {
         list.addTail(i);
      }

      for (int i = num * 2; i < num * 3; i++) {
         list.addHead(i);
      }

      for (int i = num * 3; i < num * 4; i++) {
         list.addTail(i);
      }

      for (int i = num * 3 - 1; i >= num * 2; i--) {
         assertEquals(i, list.poll().intValue());
      }

      for (int i = num - 1; i >= 0; i--) {
         assertEquals(i, list.poll().intValue());
      }

      for (int i = num; i < num * 2; i++) {
         assertEquals(i, list.poll().intValue());
      }

      for (int i = num * 3; i < num * 4; i++) {
         assertEquals(i, list.poll().intValue());
      }

   }

   @Test
   public void testPoll() {
      int num = 10;

      assertNull(list.poll());
      assertNull(list.poll());
      assertNull(list.poll());

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      for (int i = 0; i < num; i++) {
         assertEquals(i, list.poll().intValue());
      }

      assertNull(list.poll());
      assertNull(list.poll());
      assertNull(list.poll());

      for (int i = num; i < num * 2; i++) {
         list.addHead(i);
      }

      for (int i = num * 2 - 1; i >= num; i--) {
         assertEquals(i, list.poll().intValue());
      }

      assertNull(list.poll());
      assertNull(list.poll());
      assertNull(list.poll());

   }

   @Test
   public void testIterateNoElements() {
      LinkedListIterator<Integer> iter = list.iterator();

      assertNotNull(iter);

      assertNoSuchElementIsThrown(iter);

      try {
         iter.remove();

         fail("Should throw NoSuchElementException");
      } catch (NoSuchElementException e) {
         // OK
      }
   }

   @Test
   public void testCreateIteratorBeforeAddElements() {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      assertNotNull(iter);

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      testIterate1(num, iter);
   }

   @Test
   public void testCreateIteratorAfterAddElements() {
      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      assertNotNull(iter);

      testIterate1(num, iter);
   }

   @Test
   public void testIterateThenAddMoreAndIterateAgain() {
      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      assertNotNull(iter);

      for (int i = 0; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      assertFalse(iter.hasNext());

      assertNoSuchElementIsThrown(iter);

      // Add more

      for (int i = num; i < num * 2; i++) {
         list.addTail(i);
      }

      for (int i = num; i < num * 2; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      assertFalse(iter.hasNext());

      assertNoSuchElementIsThrown(iter);

      // Add some more at head

      for (int i = num * 2; i < num * 3; i++) {
         list.addHead(i);
      }

      iter = list.iterator();

      for (int i = num * 3 - 1; i >= num * 2; i--) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      for (int i = 0; i < num * 2; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      assertFalse(iter.hasNext());
   }

   private void testIterate1(int num, LinkedListIterator<Integer> iter) {
      for (int i = 0; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      assertFalse(iter.hasNext());

      assertNoSuchElementIsThrown(iter);
   }

   /**
    * @param iter
    */
   private void assertNoSuchElementIsThrown(LinkedListIterator<Integer> iter) {
      try {
         iter.next();

         fail("Should throw NoSuchElementException");
      } catch (NoSuchElementException e) {
         // OK
      }
   }

   @Test
   public void testRemoveAll() {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      try {
         iter.remove();

         fail("Should throw NoSuchElementException");
      } catch (NoSuchElementException e) {
         // OK
      }

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      assertEquals(num, list.size());

      try {
         iter.remove();

         fail("Should throw NoSuchElementException");
      } catch (NoSuchElementException e) {
         // OK
      }

      for (int i = 0; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
         assertEquals(num - i - 1, list.size());
      }

      assertFalse(iter.hasNext());
   }

   @Test
   public void testRemoveOdd() {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      try {
         iter.remove();

         fail("Should throw NoSuchElementException");
      } catch (NoSuchElementException e) {
         // OK
      }

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      try {
         iter.remove();

         fail("Should throw NoSuchElementException");
      } catch (NoSuchElementException e) {
         // OK
      }

      int size = num;
      for (int i = 0; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         if (i % 2 == 0) {
            iter.remove();
            size--;
         }
         assertEquals(list.size(), size);
      }

      iter = list.iterator();
      for (int i = 0; i < num; i++) {
         if (i % 2 == 1) {
            assertTrue(iter.hasNext());
            assertEquals(i, iter.next().intValue());
         }
      }

      assertFalse(iter.hasNext());
   }

   @Test
   public void testRemoveHead1() {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      iter.next();
      iter.remove();

      for (int i = 1; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());
   }

   @Test
   public void testRemoveHead2() {
      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      iter.next();
      iter.remove();

      iter = list.iterator();

      for (int i = 1; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());
   }

   @Test
   public void testRemoveHead3() {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      for (int i = 0; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

      for (int i = num; i < num * 2; i++) {
         list.addTail(i);
      }

      for (int i = num; i < num * 2; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

   }

   @Test
   public void testRemoveTail1() {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      for (int i = 0; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());

      // Remove the last one, that's element 9
      iter.remove();

      iter = list.iterator();

      for (int i = 0; i < num - 1; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());
   }

   @Test
   public void testRemoveMiddle() {
      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num / 2; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      // Remove the 4th element
      iter.remove();

      iter = list.iterator();

      for (int i = 0; i < num; i++) {
         if (i != num / 2 - 1) {
            assertTrue(iter.hasNext());
            assertEquals(i, iter.next().intValue());
         }
      }

      assertFalse(iter.hasNext());
   }

   @Test
   public void testRemoveTail2() {
      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());

      // Remove the last one, that's element 9
      iter.remove();

      try {
         iter.remove();
         fail("Should throw exception");
      } catch (NoSuchElementException e) {
      }

      iter = list.iterator();

      for (int i = 0; i < num - 1; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());
   }

   @Test
   public void testRemoveTail3() {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      for (int i = 0; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());

      // This should remove the 9th element and move the iterator back to position 8
      iter.remove();

      for (int i = num; i < num * 2; i++) {
         list.addTail(i);
      }

      assertTrue(iter.hasNext());
      assertEquals(8, iter.next().intValue());

      for (int i = num; i < num * 2; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
   }

   @Test
   public void testRemoveHeadAndTail1() {
      LinkedListIterator<Integer> iter = list.iterator();

      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

   }

   @Test
   public void testRemoveHeadAndTail2() {
      LinkedListIterator<Integer> iter = list.iterator();

      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addHead(i);
         assertEquals(1, list.size());
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

   }

   @Test
   public void testRemoveHeadAndTail3() {
      LinkedListIterator<Integer> iter = list.iterator();

      int num = 10;

      for (int i = 0; i < num; i++) {
         if (i % 2 == 0) {
            list.addHead(i);
         } else {
            list.addTail(i);
         }
         assertEquals(1, list.size());
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

   }

   @Test
   public void testRemoveInTurn() {
      LinkedListIterator<Integer> iter = list.iterator();

      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      for (int i = 0; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

      assertFalse(iter.hasNext());
      assertEquals(0, list.size());

   }

   @Test
   public void testGCNepotismPoll() {
      final int count = 100;
      final LinkedListImpl<ObservableNode> list = new LinkedListImpl<>();
      for (int i = 0; i < count; i++) {
         final ObservableNode node = new ObservableNode(i);
         assertNull(node.publicPrev());
         assertNull(node.publicNext());
         list.addTail(node);
         assertNotNull(node.publicPrev());
      }
      ObservableNode node;
      int removed = 0;
      while ((node = list.poll()) != null) {
         assertNull(node.publicPrev());
         assertNull(node.publicNext());
         removed++;
      }
      assertEquals(count, removed);
      assertEquals(0, list.size());
   }

   @Test
   public void testGCNepotismClear() {
      final int count = 100;
      final ObservableNode[] nodes = new ObservableNode[count];
      final LinkedListImpl<ObservableNode> list = new LinkedListImpl<>();
      for (int i = 0; i < count; i++) {
         final ObservableNode node = new ObservableNode(i);
         assertNull(node.publicPrev());
         assertNull(node.publicNext());
         nodes[i] = node;
         list.addTail(node);
         assertNotNull(node.publicPrev());
      }
      list.clear();
      for (ObservableNode node : nodes) {
         assertNull(node.publicPrev());
         assertNull(node.publicNext());
      }
      assertEquals(0, list.size());
   }

   @Test
   public void testClear() {

      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      assertEquals(num, list.size());

      list.clear();

      assertEquals(0, list.size());

      assertNull(list.poll());

      LinkedListIterator<Integer> iter = list.iterator();

      assertFalse(iter.hasNext());

      try {
         iter.next();
      } catch (NoSuchElementException e) {
      }

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      assertEquals(num, list.size());

      iter = list.iterator();

      for (int i = 0; i < num; i++) {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      assertFalse(iter.hasNext());

      for (int i = 0; i < num; i++) {
         assertEquals(i, list.poll().intValue());
      }
      assertNull(list.poll());
      assertEquals(0, list.size());

   }

   @Test
   public void testMultipleIterators1() {
      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter1 = list.iterator();
      LinkedListIterator<Integer> iter2 = list.iterator();
      LinkedListIterator<Integer> iter3 = list.iterator();

      for (int i = 0; i < num; ) {
         assertTrue(iter1.hasNext());
         assertEquals(i++, iter1.next().intValue());
         iter1.remove();

         if (i == 10) {
            break;
         }

         assertTrue(iter2.hasNext());
         assertEquals(i++, iter2.next().intValue());
         iter2.remove();

         assertTrue(iter3.hasNext());
         assertEquals(i++, iter3.next().intValue());
         iter3.remove();
      }
   }

   @Test
   public void testRepeat() {
      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      assertTrue(iter.hasNext());
      assertEquals(0, iter.next().intValue());

      iter.repeat();
      assertTrue(iter.hasNext());
      assertEquals(0, iter.next().intValue());

      iter.next();
      iter.next();
      iter.next();
      iter.hasNext();
      assertEquals(4, iter.next().intValue());

      iter.repeat();
      assertTrue(iter.hasNext());
      assertEquals(4, iter.next().intValue());

      iter.next();
      iter.next();
      iter.next();
      iter.next();
      assertEquals(9, iter.next().intValue());
      assertFalse(iter.hasNext());

      iter.repeat();
      assertTrue(iter.hasNext());
      assertEquals(9, iter.next().intValue());
      assertFalse(iter.hasNext());
   }

   @Test
   public void testRepeatAndRemove() {
      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter1 = list.iterator();

      LinkedListIterator<Integer> iter2 = list.iterator();

      assertTrue(iter1.hasNext());
      assertEquals(0, iter1.next().intValue());

      assertTrue(iter2.hasNext());
      assertEquals(0, iter2.next().intValue());

      iter2.remove();

      iter1.repeat();

      // Should move to the next one
      assertTrue(iter1.hasNext());
      assertEquals(1, iter1.next().intValue());

      iter1.next();
      iter1.next();
      iter1.next();
      iter1.next();
      iter1.next();
      iter1.next();
      iter1.next();
      iter1.next();
      assertEquals(9, iter1.next().intValue());

      iter2.next();
      iter2.next();
      iter2.next();
      iter2.next();
      iter2.next();
      iter2.next();
      iter2.next();
      iter2.next();
      assertEquals(9, iter2.next().intValue());

      iter1.remove();

      iter2.repeat();

      // Go back one since can't go forward
      assertEquals(8, iter2.next().intValue());

   }

   @Test
   public void testMultipleIterators2() {
      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter1 = list.iterator();
      LinkedListIterator<Integer> iter2 = list.iterator();
      LinkedListIterator<Integer> iter3 = list.iterator();
      LinkedListIterator<Integer> iter4 = list.iterator();
      LinkedListIterator<Integer> iter5 = list.iterator();

      assertTrue(iter1.hasNext());
      assertTrue(iter2.hasNext());
      assertTrue(iter3.hasNext());
      assertTrue(iter4.hasNext());
      assertTrue(iter5.hasNext());

      assertEquals(0, iter2.next().intValue());
      assertTrue(iter2.hasNext());
      assertEquals(1, iter2.next().intValue());

      assertEquals(0, iter1.next().intValue());
      iter1.remove();

      assertTrue(iter1.hasNext());
      assertEquals(1, iter1.next().intValue());

      // The others should get nudged onto the next value up
      assertEquals(1, iter3.next().intValue());
      assertEquals(1, iter4.next().intValue());
      assertEquals(1, iter5.next().intValue());

      assertTrue(iter4.hasNext());
      assertEquals(2, iter4.next().intValue());
      assertEquals(3, iter4.next().intValue());
      assertEquals(4, iter4.next().intValue());
      assertEquals(5, iter4.next().intValue());
      assertEquals(6, iter4.next().intValue());
      assertEquals(7, iter4.next().intValue());
      assertEquals(8, iter4.next().intValue());
      assertEquals(9, iter4.next().intValue());
      assertFalse(iter4.hasNext());

      assertTrue(iter5.hasNext());
      assertEquals(2, iter5.next().intValue());
      assertEquals(3, iter5.next().intValue());
      assertEquals(4, iter5.next().intValue());
      assertEquals(5, iter5.next().intValue());
      assertEquals(6, iter5.next().intValue());

      assertTrue(iter3.hasNext());
      assertEquals(2, iter3.next().intValue());
      assertEquals(3, iter3.next().intValue());
      assertEquals(4, iter3.next().intValue());

      assertTrue(iter2.hasNext());
      assertEquals(2, iter2.next().intValue());
      assertEquals(3, iter2.next().intValue());
      assertEquals(4, iter2.next().intValue());

      assertTrue(iter1.hasNext());
      assertEquals(2, iter1.next().intValue());
      assertEquals(3, iter1.next().intValue());
      assertEquals(4, iter1.next().intValue());

      // 1, 2, 3 are on element 4

      iter2.remove();
      assertEquals(5, iter2.next().intValue());
      iter2.remove();

      // Should be nudged to element 6

      assertTrue(iter1.hasNext());
      assertEquals(6, iter1.next().intValue());
      assertTrue(iter2.hasNext());
      assertEquals(6, iter2.next().intValue());
      assertTrue(iter3.hasNext());
      assertEquals(6, iter3.next().intValue());

      iter5.remove();
      assertTrue(iter5.hasNext());
      assertEquals(7, iter5.next().intValue());

      // Should be nudged to 7

      assertTrue(iter1.hasNext());
      assertEquals(7, iter1.next().intValue());
      assertTrue(iter2.hasNext());
      assertEquals(7, iter2.next().intValue());
      assertTrue(iter3.hasNext());
      assertEquals(7, iter3.next().intValue());

      // Delete last element

      assertTrue(iter5.hasNext());
      assertEquals(8, iter5.next().intValue());
      assertTrue(iter5.hasNext());
      assertEquals(9, iter5.next().intValue());
      assertFalse(iter5.hasNext());

      iter5.remove();

      // iter4 should be nudged back to 8, now remove element 8
      iter4.remove();

      // add a new element on tail

      list.addTail(10);

      // should be nudged back to 7

      assertTrue(iter5.hasNext());
      assertEquals(7, iter5.next().intValue());
      assertTrue(iter5.hasNext());
      assertEquals(10, iter5.next().intValue());

      assertTrue(iter4.hasNext());
      assertEquals(7, iter4.next().intValue());
      assertTrue(iter4.hasNext());
      assertEquals(10, iter4.next().intValue());

      assertTrue(iter3.hasNext());
      assertEquals(10, iter3.next().intValue());

      assertTrue(iter2.hasNext());
      assertEquals(10, iter2.next().intValue());

      assertTrue(iter1.hasNext());
      assertEquals(10, iter1.next().intValue());

   }

   @Test
   public void testResizing() {
      int numIters = 1000;

      List<LinkedListIterator<Integer>> iters = new java.util.LinkedList<>();

      int num = 10;

      for (int i = 0; i < num; i++) {
         list.addTail(i);
      }

      for (int i = 0; i < numIters; i++) {
         LinkedListIterator<Integer> iter = list.iterator();

         iters.add(iter);

         for (int j = 0; j < num / 2; j++) {
            assertTrue(iter.hasNext());

            assertEquals(j, iter.next().intValue());
         }
      }

      assertEquals(numIters, list.numIters());

      // Close the odd ones

      boolean b = false;
      for (LinkedListIterator<Integer> iter : iters) {
         if (b) {
            iter.close();
         }
         b = !b;
      }

      assertEquals(numIters / 2, list.numIters());

      // close the even ones

      b = true;
      for (LinkedListIterator<Integer> iter : iters) {
         if (b) {
            iter.close();
         }
         b = !b;
      }

      assertEquals(0, list.numIters());

   }
}
