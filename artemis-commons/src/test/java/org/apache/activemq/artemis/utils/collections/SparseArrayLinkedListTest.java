/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.activemq.artemis.utils.collections;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class SparseArrayLinkedListTest {

   private static final int SPARSE_ARRAY_CAPACITY = 4;
   private static final int ELEMENTS = SPARSE_ARRAY_CAPACITY * 4;

   private final SparseArrayLinkedList<Integer> list;

   public SparseArrayLinkedListTest() {
      list = new SparseArrayLinkedList<>(SPARSE_ARRAY_CAPACITY);
   }

   @Test(expected = IllegalArgumentException.class)
   public void shouldFailToCreateZeroArrayCapacityCollection() {
      new SparseArrayLinkedList<>(0);
   }

   @Test(expected = IllegalArgumentException.class)
   public void shouldFailToCreateNegativeArrayCapacityCollection() {
      new SparseArrayLinkedList<>(-1);
   }

   @Test(expected = NullPointerException.class)
   public void shouldFailToAddNull() {
      list.add(null);
   }

   @Test
   public void shouldNumberOfElementsBeTheSameOfTheAddedElements() {
      final int elements = ELEMENTS;
      for (int i = 0; i < elements; i++) {
         Assert.assertEquals(i, list.size());
         list.add(i);
      }
      Assert.assertEquals(elements, list.size());
   }

   @Test
   public void shouldClearConsumeElementsInOrder() {
      final int elements = ELEMENTS;
      Assert.assertEquals(0, list.clear(null));
      final ArrayList<Integer> expected = new ArrayList<>(elements);
      for (int i = 0; i < elements; i++) {
         final Integer added = i;
         list.add(added);
         expected.add(added);
      }
      final List<Integer> removed = new ArrayList<>(elements);
      Assert.assertEquals(elements, list.clear(removed::add));
      Assert.assertEquals(1, list.sparseArraysCount());
      Assert.assertEquals(0, list.size());
      Assert.assertThat(removed, is(expected));
   }

   @Test
   public void shouldRemoveMatchingElements() {
      final int elements = ELEMENTS;
      for (int i = 0; i < elements; i++) {
         list.add(i);
      }
      Assert.assertEquals(1, list.remove(e -> e.intValue() == 0));
      Assert.assertEquals(elements - 1, list.size());
      Assert.assertEquals(0, list.remove(e -> e.intValue() == 0));
      Assert.assertEquals(elements - 1, list.size());
      Assert.assertEquals(elements - 1, list.remove(e -> true));
      Assert.assertEquals(0, list.size());
      Assert.assertEquals(1, list.sparseArraysCount());
      Assert.assertEquals(0, list.remove(e -> true));
      Assert.assertEquals(1, list.sparseArraysCount());
   }

   @Test
   public void shouldRemoveDetachSparseArrays() {
      final int elements = list.sparseArrayCapacity() * 3;
      for (int i = 0; i < elements; i++) {
         list.add(i);
      }
      // remove elements in the middle
      final int startInclusiveMiddle = list.sparseArrayCapacity();
      final int endNotInclusiveMiddle = startInclusiveMiddle + list.sparseArrayCapacity();
      Assert.assertEquals(list.sparseArrayCapacity(),
                          list.remove(e -> e.intValue() >= startInclusiveMiddle && e.intValue() < endNotInclusiveMiddle));
      Assert.assertEquals(2, list.sparseArraysCount());
      // remove elements at the beginning
      final int startInclusiveFirst = 0;
      final int endNotInclusiveFirst = startInclusiveMiddle;
      Assert.assertEquals(list.sparseArrayCapacity(),
                          list.remove(e -> e.intValue() >= startInclusiveFirst && e.intValue() < endNotInclusiveFirst));
      Assert.assertEquals(1, list.sparseArraysCount());
      // remove all elements at the end
      final int startInclusiveLast = endNotInclusiveMiddle;
      final int endNotInclusiveLast = elements;
      Assert.assertEquals(list.sparseArrayCapacity(),
                          list.remove(e -> e.intValue() >= startInclusiveLast && e.intValue() < endNotInclusiveLast));
      Assert.assertEquals(1, list.sparseArraysCount());
   }

   @Test
   public void shouldAddAfterRemoveAtTheEndReusingTheAvailableSpace() {
      final int elements = list.sparseArrayCapacity();
      for (int i = 0; i < elements; i++) {
         list.add(i);
      }
      Assert.assertEquals(1, list.sparseArraysCount());
      // removing last element
      Assert.assertEquals(1, list.remove(e -> e.intValue() == elements - 1));
      list.add(elements - 1);
      Assert.assertEquals(1, list.sparseArraysCount());
      Assert.assertEquals(1, list.remove(e -> e.intValue() == 0));
      list.add(elements);
      Assert.assertEquals(2, list.sparseArraysCount());
   }

   @Test
   public void shouldReuseAllTheAvailableSpaceInTheSameArray() {
      final int elements = list.sparseArrayCapacity();
      for (int i = 0; i < elements; i++) {
         list.add(i);
      }
      Assert.assertEquals(1, list.sparseArraysCount());
      // removing all elements
      Assert.assertEquals(elements, list.remove(e -> true));
      Assert.assertEquals(1, list.sparseArraysCount());
      for (int i = 0; i < elements; i++) {
         list.add(i);
      }
      Assert.assertEquals(1, list.sparseArraysCount());
   }

   @Test
   public void shouldClearConsumeRemainingElementsInOrder() {
      final int elements = ELEMENTS;
      final Integer zero = 0;
      list.add(zero);
      for (int i = 1; i < elements; i++) {
         list.add(i);
      }
      Assert.assertEquals(elements - 1, list.remove(e -> e != zero));
      final ArrayList<Integer> remaining = new ArrayList<>();
      Assert.assertEquals(1, list.clear(remaining::add));
      Assert.assertEquals(0, list.size());
      Assert.assertEquals(1, remaining.size());
      Assert.assertEquals(zero, remaining.get(0));
   }

}
