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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * These tests are based on <a href="https://github.com/real-logic/agrona/blob/master/agrona/src/test/java/org/agrona/collections/IntHashSetTest.java">Agrona IntHashSetTest</a>
 * to guarantee a similar coverage to what's provided for a similar collection.
 */
public class LongHashSetTest {

   private static final int INITIAL_CAPACITY = 100;

   private final LongHashSet testSet = new LongHashSet(INITIAL_CAPACITY);

   @Test
   public void initiallyContainsNoElements() {
      for (long i = 0; i < 10_000; i++) {
         Assert.assertFalse(testSet.contains(i));
      }
   }

   @Test
   public void initiallyContainsNoBoxedElements() {
      for (long i = 0; i < 10_000; i++) {
         Assert.assertFalse(testSet.contains(Long.valueOf(i)));
      }
   }

   @Test
   public void containsAddedElement() {
      Assert.assertTrue(testSet.add(1L));

      Assert.assertTrue(testSet.contains(1L));
   }

   @Test
   public void addingAnElementTwiceDoesNothing() {
      Assert.assertTrue(testSet.add(1L));

      Assert.assertFalse(testSet.add(1L));
   }

   @Test
   public void containsAddedBoxedElements() {
      Assert.assertTrue(testSet.add(1L));
      Assert.assertTrue(testSet.add(Long.valueOf(2L)));

      Assert.assertTrue(testSet.contains(Long.valueOf(1L)));
      Assert.assertTrue(testSet.contains(2L));
   }

   @Test
   public void removingAnElementFromAnEmptyListDoesNothing() {
      Assert.assertFalse(testSet.remove(0L));
   }

   @Test
   public void removingAPresentElementRemovesIt() {
      Assert.assertTrue(testSet.add(1L));

      Assert.assertTrue(testSet.remove(1L));

      Assert.assertFalse(testSet.contains(1L));
   }

   @Test
   public void sizeIsInitiallyZero() {
      Assert.assertEquals(0, testSet.size());
   }

   @Test
   public void sizeIncrementsWithNumberOfAddedElements() {
      addTwoElements(testSet);

      Assert.assertEquals(2, testSet.size());
   }

   @Test
   public void sizeContainsNumberOfNewElements() {
      testSet.add(1L);
      testSet.add(1L);

      Assert.assertEquals(1, testSet.size());
   }

   @Test
   public void iteratorsListElements() {
      addTwoElements(testSet);

      assertIteratorHasElements();
   }

   @Test
   public void iteratorsStartFromTheBeginningEveryTime() {
      iteratorsListElements();

      assertIteratorHasElements();
   }

   @Test
   public void iteratorsListElementsWithoutHasNext() {
      addTwoElements(testSet);

      assertIteratorHasElementsWithoutHasNext();
   }

   @Test
   public void iteratorsStartFromTheBeginningEveryTimeWithoutHasNext() {
      iteratorsListElementsWithoutHasNext();

      assertIteratorHasElementsWithoutHasNext();
   }

   @Test(expected = NoSuchElementException.class)
   public void iteratorsThrowNoSuchElementException() {
      addTwoElements(testSet);

      exhaustIterator();
   }

   @Test(expected = NoSuchElementException.class)
   public void iteratorsThrowNoSuchElementExceptionFromTheBeginningEveryTime() {
      addTwoElements(testSet);

      try {
         exhaustIterator();
      } catch (final NoSuchElementException ignore) {
      }

      exhaustIterator();
   }

   @Test
   public void iteratorHasNoElements() {
      Assert.assertFalse(testSet.iterator().hasNext());
   }

   @Test(expected = NoSuchElementException.class)
   public void iteratorThrowExceptionForEmptySet() {
      testSet.iterator().next();
   }

   @Test
   public void clearRemovesAllElementsOfTheSet() {
      addTwoElements(testSet);

      testSet.clear();

      Assert.assertEquals(0, testSet.size());
      Assert.assertFalse(testSet.contains(1L));
      Assert.assertFalse(testSet.contains(1001L));
   }

   @Test
   public void twoEmptySetsAreEqual() {
      final LongHashSet other = new LongHashSet(100);
      Assert.assertEquals(testSet, other);
   }

   @Test
   public void setsWithTheSameValuesAreEqual() {
      final LongHashSet other = new LongHashSet(100);

      addTwoElements(testSet);
      addTwoElements(other);

      Assert.assertEquals(testSet, other);
   }

   @Test
   public void setsWithTheDifferentSizesAreNotEqual() {
      final LongHashSet other = new LongHashSet(100);

      addTwoElements(testSet);

      other.add(1001L);

      Assert.assertNotEquals(testSet, other);
   }

   @Test
   public void setsWithTheDifferentValuesAreNotEqual() {
      final LongHashSet other = new LongHashSet(100);

      addTwoElements(testSet);

      other.add(2L);
      other.add(1001L);

      Assert.assertNotEquals(testSet, other);
   }

   @Test
   public void twoEmptySetsHaveTheSameHashcode() {
      Assert.assertEquals(testSet.hashCode(), new LongHashSet(100).hashCode());
   }

   @Test
   public void setsWithTheSameValuesHaveTheSameHashcode() {
      final LongHashSet other = new LongHashSet(100);

      addTwoElements(testSet);

      addTwoElements(other);

      Assert.assertEquals(testSet.hashCode(), other.hashCode());
   }

   @Test
   public void reducesSizeWhenElementRemoved() {
      addTwoElements(testSet);

      testSet.remove(1001L);

      Assert.assertEquals(1, testSet.size());
   }

   @SuppressWarnings("CollectionToArraySafeParameter")
   @Test(expected = ArrayStoreException.class)
   public void toArrayThrowsArrayStoreExceptionForWrongType() {
      testSet.toArray(new String[1]);
   }

   @Test(expected = NullPointerException.class)
   public void toArrayThrowsNullPointerExceptionForNullArgument() {
      final Long[] into = null;
      testSet.toArray(into);
   }

   @Test
   public void toArrayCopiesElementsIntoSufficientlySizedArray() {
      addTwoElements(testSet);

      final Long[] result = testSet.toArray(new Long[testSet.size()]);

      assertArrayContainingElements(result);
   }

   @Test
   public void toArrayCopiesElementsIntoNewArray() {
      addTwoElements(testSet);

      final Long[] result = testSet.toArray(new Long[testSet.size()]);

      assertArrayContainingElements(result);
   }

   @Test
   public void toArraySupportsEmptyCollection() {
      final Long[] result = testSet.toArray(new Long[testSet.size()]);

      Assert.assertArrayEquals(result, new Long[]{});
   }

   // Test case from usage bug.
   @Test
   public void chainCompactionShouldNotCauseElementsToBeMovedBeforeTheirHash() {
      final LongHashSet requiredFields = new LongHashSet(14);

      requiredFields.add(8L);
      requiredFields.add(9L);
      requiredFields.add(35L);
      requiredFields.add(49L);
      requiredFields.add(56L);

      Assert.assertTrue("Failed to remove 8", requiredFields.remove(8L));
      Assert.assertTrue("Failed to remove 9", requiredFields.remove(9L));

      assertThat(requiredFields, containsInAnyOrder(35L, 49L, 56L));
   }

   @Test
   public void shouldResizeWhenItHitsCapacity() {
      for (long i = 0; i < 2 * INITIAL_CAPACITY; i++) {
         Assert.assertTrue(testSet.add(i));
      }

      for (long i = 0; i < 2 * INITIAL_CAPACITY; i++) {
         Assert.assertTrue(testSet.contains(i));
      }
   }

   @Test
   public void containsEmptySet() {
      final LongHashSet other = new LongHashSet(100);

      Assert.assertTrue(testSet.containsAll(other));
      Assert.assertTrue(testSet.containsAll((Collection<?>) other));
   }

   @Test
   public void containsSubset() {
      addTwoElements(testSet);

      final LongHashSet subset = new LongHashSet(100);

      subset.add(1L);

      Assert.assertTrue(testSet.containsAll(subset));
      Assert.assertTrue(testSet.containsAll((Collection<?>) subset));
   }

   @Test
   public void doesNotContainDisjointSet() {
      addTwoElements(testSet);

      final LongHashSet other = new LongHashSet(100);

      other.add(1L);
      other.add(1002L);

      Assert.assertFalse(testSet.containsAll(other));
      Assert.assertFalse(testSet.containsAll((Collection<?>) other));
   }

   @Test
   public void doesNotContainSuperset() {
      addTwoElements(testSet);

      final LongHashSet superset = new LongHashSet(100);

      addTwoElements(superset);
      superset.add(15L);

      Assert.assertFalse(testSet.containsAll(superset));
      Assert.assertFalse(testSet.containsAll((Collection<?>) superset));
   }

   @Test
   public void addingEmptySetDoesNothing() {
      addTwoElements(testSet);

      Assert.assertFalse(testSet.addAll(new LongHashSet(100)));
      Assert.assertFalse(testSet.addAll(new HashSet<>()));
      assertContainsElements(testSet);
   }

   @Test
   public void addingSubsetDoesNothing() {
      addTwoElements(testSet);

      final LongHashSet subset = new LongHashSet(100);

      subset.add(1L);

      final HashSet<Long> subSetCollection = new HashSet<>(subset);

      Assert.assertFalse(testSet.addAll(subset));
      Assert.assertFalse(testSet.addAll(subSetCollection));
      assertContainsElements(testSet);
   }

   @Test
   public void addingEqualSetDoesNothing() {
      addTwoElements(testSet);

      final LongHashSet equal = new LongHashSet(100);

      addTwoElements(equal);

      final HashSet<Long> equalCollection = new HashSet<>(equal);

      Assert.assertFalse(testSet.addAll(equal));
      Assert.assertFalse(testSet.addAll(equalCollection));
      assertContainsElements(testSet);
   }

   @Test
   public void containsValuesAddedFromDisjointSetPrimitive() {
      addTwoElements(testSet);

      final LongHashSet disjoint = new LongHashSet(100);

      disjoint.add(2L);
      disjoint.add(1002L);

      Assert.assertTrue(testSet.addAll(disjoint));
      Assert.assertTrue(testSet.contains(1L));
      Assert.assertTrue(testSet.contains(1001L));
      Assert.assertTrue(testSet.containsAll(disjoint));
   }

   @Test
   public void containsValuesAddedFromDisjointSet() {
      addTwoElements(testSet);

      final HashSet<Long> disjoint = new HashSet<>();

      disjoint.add(2L);
      disjoint.add(1002L);

      Assert.assertTrue(testSet.addAll(disjoint));
      Assert.assertTrue(testSet.contains(1L));
      Assert.assertTrue(testSet.contains(1001L));
      Assert.assertTrue(testSet.containsAll(disjoint));
   }

   @Test
   public void containsValuesAddedFromIntersectingSetPrimitive() {
      addTwoElements(testSet);

      final LongHashSet intersecting = new LongHashSet(100);

      intersecting.add(1L);
      intersecting.add(1002L);

      Assert.assertTrue(testSet.addAll(intersecting));
      Assert.assertTrue(testSet.contains(1L));
      Assert.assertTrue(testSet.contains(1001L));
      Assert.assertTrue(testSet.containsAll(intersecting));
   }

   @Test
   public void containsValuesAddedFromIntersectingSet() {
      addTwoElements(testSet);

      final HashSet<Long> intersecting = new HashSet<>();

      intersecting.add(1L);
      intersecting.add(1002L);

      Assert.assertTrue(testSet.addAll(intersecting));
      Assert.assertTrue(testSet.contains(1L));
      Assert.assertTrue(testSet.contains(1001L));
      Assert.assertTrue(testSet.containsAll(intersecting));
   }

   @Test
   public void removingEmptySetDoesNothing() {
      addTwoElements(testSet);

      Assert.assertFalse(testSet.removeAll(new LongHashSet(100)));
      Assert.assertFalse(testSet.removeAll(new HashSet<Long>()));
      assertContainsElements(testSet);
   }

   @Test
   public void removingDisjointSetDoesNothing() {
      addTwoElements(testSet);

      final LongHashSet disjoint = new LongHashSet(100);

      disjoint.add(2L);
      disjoint.add(1002L);

      Assert.assertFalse(testSet.removeAll(disjoint));
      Assert.assertFalse(testSet.removeAll(new HashSet<Long>()));
      assertContainsElements(testSet);
   }

   @Test
   public void doesNotContainRemovedIntersectingSetPrimitive() {
      addTwoElements(testSet);

      final LongHashSet intersecting = new LongHashSet(100);

      intersecting.add(1L);
      intersecting.add(1002L);

      Assert.assertTrue(testSet.removeAll(intersecting));
      Assert.assertTrue(testSet.contains(1001L));
      Assert.assertFalse(testSet.containsAll(intersecting));
   }

   @Test
   public void doesNotContainRemovedIntersectingSet() {
      addTwoElements(testSet);

      final HashSet<Long> intersecting = new HashSet<>();

      intersecting.add(1L);
      intersecting.add(1002L);

      Assert.assertTrue(testSet.removeAll(intersecting));
      Assert.assertTrue(testSet.contains(1001L));
      Assert.assertFalse(testSet.containsAll(intersecting));
   }

   @Test
   public void isEmptyAfterRemovingEqualSetPrimitive() {
      addTwoElements(testSet);

      final LongHashSet equal = new LongHashSet(100);

      addTwoElements(equal);

      Assert.assertTrue(testSet.removeAll(equal));
      Assert.assertTrue(testSet.isEmpty());
   }

   @Test
   public void isEmptyAfterRemovingEqualSet() {
      addTwoElements(testSet);

      final HashSet<Long> equal = new HashSet<>();

      addTwoElements(equal);

      Assert.assertTrue(testSet.removeAll(equal));
      Assert.assertTrue(testSet.isEmpty());
   }

   @Test
   public void removeElementsFromIterator() {
      addTwoElements(testSet);

      final LongHashSet.LongIterator iterator = testSet.iterator();
      while (iterator.hasNext()) {
         if (iterator.nextValue() == 1L) {
            iterator.remove();
         }
      }

      assertThat(testSet, contains(1001L));
      assertThat(testSet, hasSize(1));
   }

   @Test
   public void shouldNotContainMissingValueInitially() {
      Assert.assertFalse(testSet.contains(LongHashSet.MISSING_VALUE));
   }

   @Test
   public void shouldAllowMissingValue() {
      Assert.assertTrue(testSet.add(LongHashSet.MISSING_VALUE));

      Assert.assertTrue(testSet.contains(LongHashSet.MISSING_VALUE));

      Assert.assertFalse(testSet.add(LongHashSet.MISSING_VALUE));
   }

   @Test
   public void shouldAllowRemovalOfMissingValue() {
      Assert.assertTrue(testSet.add(LongHashSet.MISSING_VALUE));

      Assert.assertTrue(testSet.remove(LongHashSet.MISSING_VALUE));

      Assert.assertFalse(testSet.contains(LongHashSet.MISSING_VALUE));

      Assert.assertFalse(testSet.remove(LongHashSet.MISSING_VALUE));
   }

   @Test
   public void sizeAccountsForMissingValue() {
      testSet.add(1L);
      testSet.add(LongHashSet.MISSING_VALUE);

      Assert.assertEquals(2, testSet.size());
   }

   @Test
   public void toArrayCopiesElementsIntoNewArrayIncludingMissingValue() {
      addTwoElements(testSet);

      testSet.add(LongHashSet.MISSING_VALUE);

      final Long[] result = testSet.toArray(new Long[testSet.size()]);

      assertThat(result, arrayContainingInAnyOrder(1L, 1001L, LongHashSet.MISSING_VALUE));
   }

   @Test
   public void toObjectArrayCopiesElementsIntoNewArrayIncludingMissingValue() {
      addTwoElements(testSet);

      testSet.add(LongHashSet.MISSING_VALUE);

      final Object[] result = testSet.toArray();

      assertThat(result, arrayContainingInAnyOrder(1L, 1001L, LongHashSet.MISSING_VALUE));
   }

   @Test
   public void equalsAccountsForMissingValue() {
      addTwoElements(testSet);
      testSet.add(LongHashSet.MISSING_VALUE);

      final LongHashSet other = new LongHashSet(100);
      addTwoElements(other);

      Assert.assertNotEquals(testSet, other);

      other.add(LongHashSet.MISSING_VALUE);
      Assert.assertEquals(testSet, other);

      testSet.remove(LongHashSet.MISSING_VALUE);

      Assert.assertNotEquals(testSet, other);
   }

   @Test
   public void consecutiveValuesShouldBeCorrectlyStored() {
      for (long i = 0; i < 10_000; i++) {
         testSet.add(i);
      }

      assertThat(testSet, hasSize(10_000));

      int distinctElements = 0;
      for (final long ignore : testSet) {
         distinctElements++;
      }

      assertThat(distinctElements, is(10_000));
   }

   @Test
   public void hashCodeAccountsForMissingValue() {
      addTwoElements(testSet);
      testSet.add(LongHashSet.MISSING_VALUE);

      final LongHashSet other = new LongHashSet(100);
      addTwoElements(other);

      Assert.assertNotEquals(testSet.hashCode(), other.hashCode());

      other.add(LongHashSet.MISSING_VALUE);
      Assert.assertEquals(testSet.hashCode(), other.hashCode());

      testSet.remove(LongHashSet.MISSING_VALUE);

      Assert.assertNotEquals(testSet.hashCode(), other.hashCode());
   }

   @Test
   public void iteratorAccountsForMissingValue() {
      addTwoElements(testSet);
      testSet.add(LongHashSet.MISSING_VALUE);

      int missingValueCount = 0;
      final LongHashSet.LongIterator iterator = testSet.iterator();
      while (iterator.hasNext()) {
         if (iterator.nextValue() == LongHashSet.MISSING_VALUE) {
            missingValueCount++;
         }
      }

      Assert.assertEquals(1, missingValueCount);
   }

   @Test
   public void iteratorCanRemoveMissingValue() {
      addTwoElements(testSet);
      testSet.add(LongHashSet.MISSING_VALUE);

      final LongHashSet.LongIterator iterator = testSet.iterator();
      while (iterator.hasNext()) {
         if (iterator.nextValue() == LongHashSet.MISSING_VALUE) {
            iterator.remove();
         }
      }

      Assert.assertFalse(testSet.contains(LongHashSet.MISSING_VALUE));
   }

   @Test
   public void shouldGenerateStringRepresentation() {
      final long[] testEntries = {3L, 1L, -2L, 19L, 7L, 11L, 12L, 7L};

      for (final long testEntry : testEntries) {
         testSet.add(testEntry);
      }

      final String mapAsAString = "{1, 19, 11, 7, 3, 12, -2}";
      assertThat(testSet.toString(), equalTo(mapAsAString));
   }

   @Test
   public void shouldRemoveMissingValueWhenCleared() {
      Assert.assertTrue(testSet.add(LongHashSet.MISSING_VALUE));

      testSet.clear();

      Assert.assertFalse(testSet.contains(LongHashSet.MISSING_VALUE));
   }

   @Test
   public void shouldHaveCompatibleEqualsAndHashcode() {
      final HashSet<Long> compatibleSet = new HashSet<>();
      final long seed = System.nanoTime();
      final Random r = new Random(seed);
      for (long i = 0; i < 1024; i++) {
         final long value = r.nextLong();
         compatibleSet.add(value);
         testSet.add(value);
      }

      if (r.nextBoolean()) {
         compatibleSet.add(LongHashSet.MISSING_VALUE);
         testSet.add(LongHashSet.MISSING_VALUE);
      }

      Assert.assertEquals("Fail with seed:" + seed, testSet, compatibleSet);
      Assert.assertEquals("Fail with seed:" + seed, compatibleSet, testSet);
      Assert.assertEquals("Fail with seed:" + seed, compatibleSet.hashCode(), testSet.hashCode());
   }

   private static void addTwoElements(final LongHashSet obj) {
      obj.add(1L);
      obj.add(1001L);
   }

   private static void addTwoElements(final HashSet<Long> obj) {
      obj.add(1L);
      obj.add(1001L);
   }

   private void assertIteratorHasElements() {
      final Iterator<Long> iter = testSet.iterator();

      final Set<Long> values = new HashSet<>();

      Assert.assertTrue(iter.hasNext());
      values.add(iter.next());
      Assert.assertTrue(iter.hasNext());
      values.add(iter.next());
      Assert.assertFalse(iter.hasNext());

      assertContainsElements(values);
   }

   private void assertIteratorHasElementsWithoutHasNext() {
      final Iterator<Long> iter = testSet.iterator();

      final Set<Long> values = new HashSet<>();

      values.add(iter.next());
      values.add(iter.next());

      assertContainsElements(values);
   }

   private static void assertArrayContainingElements(final Long[] result) {
      assertThat(result, arrayContainingInAnyOrder(1L, 1001L));
   }

   private static void assertContainsElements(final Set<Long> other) {
      assertThat(other, containsInAnyOrder(1L, 1001L));
   }

   private void exhaustIterator() {
      final Iterator iterator = testSet.iterator();
      iterator.next();
      iterator.next();
      iterator.next();
   }
}
