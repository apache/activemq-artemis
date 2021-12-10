/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.utils.actors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Test;

public class ThresholdActorTest {

   Semaphore semaphore = new Semaphore(1);
   AtomicInteger result = new AtomicInteger(0);
   AtomicInteger lastProcessed = new AtomicInteger(0);
   AtomicInteger errors = new AtomicInteger(0);

   @Test
   public void limitedSize() throws Exception {
      lastProcessed.set(0);
      final ExecutorService executorService = Executors.newSingleThreadExecutor();
      AtomicInteger timesOpen = new AtomicInteger(0);
      AtomicInteger timesClose = new AtomicInteger(0);
      AtomicBoolean open = new AtomicBoolean(true);
      try {
         semaphore.acquire();
         ThresholdActor<Integer> actor = new ThresholdActor<>(executorService, this::limitedProcess, 10, (s) -> 1, () -> {
            timesClose.incrementAndGet();
            open.set(false);
         }, () -> {
            timesOpen.incrementAndGet();
            open.set(true);
         });

         for (int i = 0; i < 10; i++) {
            actor.act(i);
         }
         Assert.assertTrue(open.get());
         Assert.assertEquals(0, timesClose.get());

         actor.act(99);
         Assert.assertEquals(1, timesClose.get());
         Assert.assertEquals(0, timesOpen.get());

         Assert.assertFalse(open.get());

         actor.act(1000);

         actor.flush(); // a flush here shuld not change anything, as it was already called once on the previous overflow
         Assert.assertEquals(1, timesClose.get());
         Assert.assertEquals(0, timesOpen.get());
         Assert.assertFalse(open.get());

         semaphore.release();
         Wait.assertTrue(open::get);

         Assert.assertEquals(1, timesClose.get());
         Assert.assertEquals(1, timesOpen.get());
         Wait.assertEquals(1000, lastProcessed::get, 5000, 1);

         actor.flush();

         open.set(false);

         // measuring after forced flush
         Wait.assertEquals(2, timesOpen::get, 5000, 1);
         Wait.assertTrue(open::get);
      } finally {
         executorService.shutdown();
      }
   }

   public void limitedProcess(Integer i) {
      try {
         semaphore.acquire();
         result.incrementAndGet();
         lastProcessed.set(i);
         semaphore.release();
      } catch (Throwable e) {
         e.printStackTrace();
      }
   }

   static class Element {
      Element(int i, int size) {
         this.i = i;
         this.size = size;
      }
      int i;
      int size;
   }

   private static int getSize(Element e) {
      return e.size;
   }

   protected void process(Element e) {
      lastProcessed.set(e.i);
   }

   public void block() {
      try {
         if (!semaphore.tryAcquire()) {
            errors.incrementAndGet();
            System.err.println("acquire failed");
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   @Test
   public void testFlow() throws Exception {
      final ExecutorService executorService = Executors.newSingleThreadExecutor();

      try {
         ThresholdActor<Element> actor = new ThresholdActor<>(executorService, this::process, 20, (e) -> e.size, this::block, semaphore::release);

         final int LAST_ELEMENT = 1000;

         for (int i = 0; i <= LAST_ELEMENT; i++) {
            actor.act(new Element(i, i % 2 == 0 ? 20 : 1));
         }

         Wait.assertEquals(LAST_ELEMENT, lastProcessed::get);
         Assert.assertEquals(0, errors.get());
      } finally {
         executorService.shutdown();
      }
   }


}
