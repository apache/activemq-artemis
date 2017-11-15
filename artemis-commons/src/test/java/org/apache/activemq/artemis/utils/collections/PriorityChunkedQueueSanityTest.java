/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.utils.collections;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class PriorityChunkedQueueSanityTest {

   @Test(expected = NegativeArraySizeException.class)
   public void shouldNotAllowNegativePriorities() {
      final PriorityChunkedQueue<Integer> q = PriorityChunkedQueue.with(-1, 8);
   }

   @Test
   public void shouldClearWithMixedOffer() {
      final PriorityChunkedQueue<Integer> q = PriorityChunkedQueue.with(1, 8);
      for (int i = 5; i < 10; i++) {
         q.offer(i, 0);
      }
      Assert.assertEquals(5, q.size());
      for (int i = 4; i >= 0; i--) {
         q.offerFirst(i, 0);
      }
      Assert.assertEquals(10, q.size());
      q.clear();
      Assert.assertEquals(0, q.size());
   }

   @Test
   public void shouldPollInOrderWithMixedOffer() {
      final PriorityChunkedQueue<Integer> q = PriorityChunkedQueue.with(1, 8);
      for (int i = 5; i < 10; i++) {
         q.offer(i, 0);
      }
      Assert.assertEquals(5, q.size());
      for (int i = 4; i >= 0; i--) {
         q.offerFirst(i, 0);
      }
      Assert.assertEquals(10, q.size());
      for (int i = 0; i < 10; i++) {
         final Integer value = q.poll();
         Assert.assertEquals("poll not ordered!", (Integer) i, value);
      }
      Assert.assertEquals(0, q.size());
   }

   @Test
   public void shouldPollInOrderWithOfferFirst() {
      final PriorityChunkedQueue<Integer> q = PriorityChunkedQueue.with(1, 8);
      for (int i = 9; i >= 0; i--) {
         q.offerFirst(i, 0);
      }
      Assert.assertEquals(10, q.size());
      for (int i = 0; i < 10; i++) {
         final Integer value = q.poll();
         Assert.assertEquals("poll not ordered!", (Integer) i, value);
      }
      Assert.assertEquals(0, q.size());
   }

   @Test
   public void shouldPollInOrderWithOffer() {
      final PriorityChunkedQueue<Integer> q = PriorityChunkedQueue.with(1, 8);
      for (int i = 0; i < 10; i++) {
         q.offer(i, 0);
      }
      Assert.assertEquals(10, q.size());
      for (int i = 0; i < 10; i++) {
         final Integer value = q.poll();
         Assert.assertEquals("poll not ordered!", (Integer) i, value);
      }
      Assert.assertEquals(0, q.size());
      Assert.assertNull("poll when empty must return null!", q.poll());
   }

   @Test
   public void shouldDrainInOrderWithOffer() {
      final PriorityChunkedQueue<Integer> q = PriorityChunkedQueue.with(1, 8);
      for (int i = 0; i < 10; i++) {
         q.offer(i, 0);
      }
      Assert.assertEquals(10, q.size());
      final List<Integer> drained = new ArrayList<>(10);
      final long elements = q.drain(drained::add);
      Assert.assertEquals(0, q.size());
      Assert.assertEquals(10, elements);
      Assert.assertEquals(10, drained.size());
      for (int i = 0; i < 10; i++) {
         Assert.assertEquals("drain not ordered!", (Integer) i, drained.get(i));
      }
      Assert.assertEquals("drain when empty must return 0!", 0, q.drain(e -> Assert.fail("can't drain without elements")));
   }

   @Test
   public void shouldDrainInOrderOfPriority() {
      final PriorityChunkedQueue<Integer> q = PriorityChunkedQueue.with(8, 8);
      int e = 0;
      for (int p = 7; p >= 0; p--) {
         for (int i = 0; i < 10; i++) {
            q.offer(e, p);
            e++;
         }
      }
      Assert.assertEquals(8 * 10, q.size());
      final List<Integer> drained = new ArrayList<>(10);
      final long elements = q.drain(drained::add);
      Assert.assertEquals(0, q.size());
      Assert.assertEquals(8 * 10, elements);
      Assert.assertEquals(8 * 10, drained.size());
      for (int i = 0; i < 80; i++) {
         Assert.assertEquals("drain not ordered!", (Integer) i, drained.get(i));
      }

   }

   @Test
   public void shouldPollInOrderOfPriority() {
      final PriorityChunkedQueue<Integer> q = PriorityChunkedQueue.with(8, 8);
      int e = 0;
      for (int p = 7; p >= 0; p--) {
         for (int i = 0; i < 10; i++) {
            q.offer(e, p);
            e++;
         }
      }
      Assert.assertEquals(8 * 10, q.size());
      for (int expected = 0; expected < 80; expected++) {
         final Integer value = q.poll();
         Assert.assertEquals("poll not ordered!", (Integer) expected, value);
      }
   }

   @Test
   public void shouldIterateInOrderWithOffer() {
      final PriorityChunkedQueue<Integer> q = PriorityChunkedQueue.with(1, 8);
      for (int i = 0; i < 10; i++) {
         q.offer(i, 0);
      }
      Assert.assertEquals(10, q.size());
      final List<Integer> iterates = new ArrayList<>(10);
      final long elements = q.forEach(iterates::add);
      Assert.assertEquals(10, q.size());
      Assert.assertEquals(10, elements);
      Assert.assertEquals(10, iterates.size());
      for (int i = 0; i < 10; i++) {
         Assert.assertEquals("iterate not ordered!", (Integer) i, iterates.get(i));
      }
   }

   @Test
   public void shouldPeekInOrderWithOffer() {
      final PriorityChunkedQueue<Integer> q = PriorityChunkedQueue.with(1, 8);
      for (int i = 0; i < 10; i++) {
         q.offer(i, 0);
      }
      Assert.assertEquals(10, q.size());
      for (int i = 0; i < 10; i++) {
         final Integer peeked = q.peek();
         Assert.assertEquals((Integer) i, peeked);
         Assert.assertEquals("peek can't modify the queue!", 10 - i, q.size());
         final Integer polled = q.poll();
         Assert.assertEquals(peeked, polled);
      }
      Assert.assertEquals(0, q.size());
      Assert.assertNull("peek when empty must return null!", q.peek());
   }
}
