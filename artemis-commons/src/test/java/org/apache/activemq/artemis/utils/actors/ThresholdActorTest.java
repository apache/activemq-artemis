/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.utils.actors;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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

   public void unblock() {
      semaphore.release();
   }

   @Test
   public void testFlow() throws Exception {
      testFlow(true);
   }

   /**
    * This test will actually not respect the semaphore and keep going.
    * The blockers and unblocks should still perform ok.
    * @throws Exception
    */
   @Test
   public void testFlow2() throws Exception {
      testFlow(false);
   }

   public void testFlow(boolean respectSemaphore) throws Exception {
      final ExecutorService executorService = Executors.newFixedThreadPool(2);

      try {
         ThresholdActor<Element> actor = new ThresholdActor<>(executorService, this::process, 20, (e) -> e.size, this::block, this::unblock);

         final int LAST_ELEMENT = 1111;

         final CountDownLatch latchDone = new CountDownLatch(1);

         executorService.execute(() -> {
            for (int i = 0; i <= LAST_ELEMENT; i++) {
               try {
                  if (respectSemaphore) {
                     semaphore.acquire();
                     semaphore.release();
                  }
                  actor.act(new Element(i, i % 2 == 0 ? 20 : 1));
               } catch (Exception e) {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }
            }
            latchDone.countDown();
         });

         Assert.assertTrue(latchDone.await(10, TimeUnit.SECONDS));

         Wait.assertEquals(LAST_ELEMENT, lastProcessed::get);
         Assert.assertEquals(0, errors.get());
      } finally {
         executorService.shutdown();
      }
   }


   @Test
   public void testShutdownTask() throws Exception {
      AtomicInteger lastAcquireFailed = new AtomicInteger(0);
      lastProcessed.set(0);

      Semaphore allowedTasks = new Semaphore(10);
      CountDownLatch completedTasks = new CountDownLatch(11);
      CountDownLatch pendingTasks = new CountDownLatch(11);

      final ExecutorService executorService = Executors.newSingleThreadExecutor();

      ThresholdActor<Integer> actor = new ThresholdActor<>(executorService, (i) -> {
         try {
            pendingTasks.countDown();
            if (allowedTasks.tryAcquire(1, 200, TimeUnit.MILLISECONDS)) {
               lastProcessed.set(i);
            } else {
               lastAcquireFailed.set(i);
            }
            completedTasks.countDown();
         } catch (InterruptedException ignored) {
         }

      }, 1000, (e) -> {
         return 1;
      }, () -> {
      }, () -> {
      });

      // expect allowedTasks tasks to complete
      for (int i = 1; i < 100; i++) {
         actor.act(i);
      }
      // wait for task processing
      Assert.assertTrue(pendingTasks.await(4, TimeUnit.SECONDS));

      actor.shutdown(() -> {
         lastProcessed.set(lastProcessed.get() * 1000);
      });

      Assert.assertTrue(completedTasks.await(4, TimeUnit.SECONDS));

      // assert processing terminated at block point
      Assert.assertEquals(10000, lastProcessed.get());
      // pending task executed as expected
      Assert.assertEquals(11, lastAcquireFailed.get());
   }
}
