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

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.utils.AutomaticLatch;
import org.junit.Assert;
import org.junit.Test;

public class AutomaticLatchTest {

   @Test
   public void testWthPending() {
      AtomicInteger value = new AtomicInteger(0);
      AutomaticLatch latch = new AutomaticLatch(1);
      latch.afterCompletion(() -> value.incrementAndGet());
      Assert.assertEquals(0, value.get());

      latch.countDown();

      Assert.assertEquals(1, value.get());
   }

   @Test
   public void testWthoutPending() {
      AtomicInteger value = new AtomicInteger(0);
      AutomaticLatch latch = new AutomaticLatch(0);
      latch.afterCompletion(() -> value.incrementAndGet());
      Assert.assertEquals(1, value.get());
      latch.countUp();
      latch.countDown();

      // the previous latch completion should been cleared by now
      Assert.assertEquals(1, value.get());

      latch.afterCompletion(() -> value.addAndGet(10));
      Assert.assertEquals(11, value.get());

      latch.countUp();
      latch.countDown();

      Assert.assertEquals(11, value.get());
   }

   @Test
   public void testMultipleCallsOrder() {
      ArrayList<Integer> outcome = new ArrayList<>();
      AutomaticLatch latch = new AutomaticLatch(1);
      latch.afterCompletion(() -> outcome.add(0));
      latch.afterCompletion(() -> outcome.add(1));
      latch.afterCompletion(() -> outcome.add(2));

      latch.countDown();

      Assert.assertEquals((Integer)0, outcome.get(0));
      Assert.assertEquals((Integer)1, outcome.get(1));
      Assert.assertEquals((Integer)2, outcome.get(2));

      Assert.assertEquals(3, outcome.size());
   }


}
