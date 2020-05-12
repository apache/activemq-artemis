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
package org.apache.activemq.artemis.core.journal.impl;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ObjIntIntArrayListTest {

   @Test
   public void addShouldAppendTuplesInOrder() {
      List<Integer> expectedAList = new ArrayList<>();
      List<Integer> expectedBList = new ArrayList<>();
      List<Integer> expectedCList = new ArrayList<>();
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      final int elementsToAdd = 10;
      assertEquals(0, list.size());
      for (int i = 0; i < elementsToAdd; i++) {
         Integer a = i;
         Integer b = i + 1;
         Integer c = i + 2;
         list.add(a, b, c);
         assertEquals(i + 1, list.size());
         expectedAList.add(a);
         expectedBList.add(b);
         expectedCList.add(c);
      }
      List<Integer> aList = new ArrayList<>();
      List<Integer> bList = new ArrayList<>();
      List<Integer> cList = new ArrayList<>();
      final Object expectedArg = new Object();
      list.forEach((a, b, c, arg) -> {
         aList.add(a);
         bList.add(b);
         cList.add(c);
         Assert.assertSame(expectedArg, arg);
      }, expectedArg);
      assertThat(aList, equalTo(expectedAList));
      assertThat(bList, equalTo(expectedBList));
      assertThat(cList, equalTo(expectedCList));
   }

   @Test(expected = NullPointerException.class)
   public void addShouldFailAppendNull() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      list.add(null, 1, 2);
   }

   @Test(expected = IllegalArgumentException.class)
   public void addShouldFailAppendNegativeA() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      list.add(0, -1, 1);
   }

   @Test(expected = IllegalArgumentException.class)
   public void addShouldFailAppendNegativeB() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      list.add(0, 1, -1);
   }

   @Test
   public void clearShouldDeleteAllElements() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      list.add(1, 3, 3);
      assertEquals(1, list.size());
      list.clear();
      assertEquals(0, list.size());
      list.forEach((a, b, c, ignored) -> {
         Assert.fail("the list should be empty");
      }, null);
   }

   @Test
   public void updateIfMatchShouldReturnFalseOnMissingElement() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      Integer e = 1;
      list.add(e, 2, 3);
      Assert.assertFalse(list.addToIntsIfMatch(0, 2, 1, 1));
      list.forEach((a, b, c, ignored) -> {
         Assert.assertEquals(e, a);
         Assert.assertEquals(2, b);
         Assert.assertEquals(3, c);
      }, null);
   }

   @Test
   public void updateIfMatchShouldReturnFalseOnOverflowOfA() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      Integer e = 1;
      list.add(e, Integer.MAX_VALUE, 3);
      Assert.assertFalse(list.addToIntsIfMatch(0, e, Integer.MAX_VALUE, 1));
      list.forEach((a, b, c, ignored) -> {
         Assert.assertEquals(e, a);
         Assert.assertEquals(Integer.MAX_VALUE, b);
         Assert.assertEquals(3, c);
      }, null);
   }

   @Test
   public void updateIfMatchShouldReturnFalseOnOverflowOfB() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      Integer e = 1;
      list.add(e, 2, Integer.MAX_VALUE);
      Assert.assertFalse(list.addToIntsIfMatch(0, e, 1, Integer.MAX_VALUE));
      list.forEach((a, b, c, ignored) -> {
         Assert.assertEquals(e, a);
         Assert.assertEquals(2, b);
         Assert.assertEquals(Integer.MAX_VALUE, c);
      }, null);
   }

   @Test(expected = IndexOutOfBoundsException.class)
   public void updateIfMatchShouldFailOnNegativeIndex() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      list.addToIntsIfMatch(-1, 1, 1, 1);
   }

   @Test(expected = IndexOutOfBoundsException.class)
   public void updateIfMatchShouldFailBeyondSize() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      list.addToIntsIfMatch(0, 1, 1, 1);
   }

   @Test(expected = NullPointerException.class)
   public void updateIfMatchShouldFailOnNull() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      list.add(1, 2, 3);
      list.addToIntsIfMatch(0, null, 1, 1);
   }

   @Test(expected = IllegalArgumentException.class)
   public void updateIfMatchShouldFailOnNegativeDeltaA() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      Integer e = 1;
      list.add(1, 2, 3);
      list.addToIntsIfMatch(0, e, -1, 1);
   }

   @Test(expected = IllegalArgumentException.class)
   public void updateIfMatchShouldFailOnNegativeDeltaB() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      Integer e = 1;
      list.add(e, 2, 3);
      list.addToIntsIfMatch(0, e, 1, -1);
   }

   @Test
   public void updateIfMatchShouldModifyExistingTuple() {
      ObjIntIntArrayList<Integer> list = new ObjIntIntArrayList<>(0);
      list.add(1, 2, 3);
      list.add(2, 3, 4);
      list.add(3, 4, 5);
      Assert.assertTrue(list.addToIntsIfMatch(0, 1, 1, 1));
      Assert.assertTrue(list.addToIntsIfMatch(1, 2, 1, 1));
      Assert.assertTrue(list.addToIntsIfMatch(2, 3, 1, 1));
      Assert.assertEquals(3, list.size());
      List<Integer> eList = new ArrayList<>();
      List<Integer> aList = new ArrayList<>();
      List<Integer> bList = new ArrayList<>();
      list.forEach((e, a, b, ignore) -> {
         eList.add(e);
         aList.add(a);
         bList.add(b);
      }, null);
      Assert.assertEquals(3, eList.size());
      Assert.assertEquals(3, aList.size());
      Assert.assertEquals(3, bList.size());
      for (int i = 0; i < 3; i++) {
         final int index = i;
         final Integer expectedE = index + 1;
         final Integer expectedA = expectedE + 2;
         final Integer expectedB = expectedE + 3;
         Assert.assertEquals(expectedE, eList.get(index));
         Assert.assertEquals(expectedA, aList.get(index));
         Assert.assertEquals(expectedB, bList.get(index));
      }
   }

}
