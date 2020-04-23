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

package org.apache.activemq.artemis.utils.actors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

public class OrderedExecutorSanityTest {
   private static final Logger log = Logger.getLogger(OrderedExecutorSanityTest.class);

   @Test
   public void shouldExecuteTasksInOrder() throws InterruptedException {
      final int threads = 3;
      final int tasks = 100;
      final long timeoutMillis = TimeUnit.SECONDS.toMillis(10);
      final ExecutorService executorService = Executors.newFixedThreadPool(threads);
      try {
         final ArtemisExecutor executor = new OrderedExecutor(executorService);
         //it can be not thread safe too
         final List<Integer> results = new ArrayList<>(tasks);
         final List<Integer> expectedResults = new ArrayList<>(tasks);
         final CountDownLatch executed = new CountDownLatch(tasks);
         for (int i = 0; i < tasks; i++) {
            final int value = i;
            executor.execute(() -> {
               results.add(value);
               executed.countDown();
            });
            expectedResults.add(value);
         }
         Assert.assertTrue("The tasks must be executed in " + timeoutMillis + " ms", executed.await(timeoutMillis, TimeUnit.MILLISECONDS));
         Assert.assertArrayEquals("The processing of tasks must be ordered", expectedResults.toArray(), results.toArray());
      } finally {
         executorService.shutdown();
      }
   }

   @Test
   public void shouldShutdownNowDoNotExecuteFurtherTasks() throws InterruptedException {
      final long timeoutMillis = TimeUnit.SECONDS.toMillis(10);
      final ExecutorService executorService = Executors.newSingleThreadExecutor();
      try {
         final OrderedExecutor executor = new OrderedExecutor(executorService);
         final CountDownLatch executed = new CountDownLatch(1);
         executor.execute(executed::countDown);
         Assert.assertTrue("The task must be executed in " + timeoutMillis + " ms", executed.await(timeoutMillis, TimeUnit.MILLISECONDS));
         executor.shutdownNow();
         Assert.assertEquals("There are no remaining tasks to be executed", 0, executor.remaining());
         //from now on new tasks won't be executed
         executor.execute(() -> System.out.println("this will never happen"));
         //to avoid memory leaks the executor must take care of the new submitted tasks immediatly
         Assert.assertEquals("Any new task submitted after death must be collected", 0, executor.remaining());
      } finally {
         executorService.shutdown();
      }
   }



   @Test
   public void shutdownNowOnDelegateExecutor() throws Exception {
      final ExecutorService executorService = Executors.newSingleThreadExecutor();
      try {
         final OrderedExecutor executor = new OrderedExecutor(executorService);
         final CyclicBarrier latch = new CyclicBarrier(2);
         final AtomicInteger numberOfTasks = new AtomicInteger(0);
         final CountDownLatch ran = new CountDownLatch(1);

         executor.execute(() -> {
            try {
               latch.await(1, TimeUnit.MINUTES);
               numberOfTasks.set(executor.shutdownNow());
               ran.countDown();
            } catch (Exception e) {
               e.printStackTrace();
            }
         });


         for (int i = 0; i < 100; i++) {
            executor.execute(() -> System.out.println("Dont worry, this will never happen"));
         }

         latch.await();
         ran.await(1, TimeUnit.SECONDS);
         Assert.assertEquals(100, numberOfTasks.get());

         Assert.assertEquals(ProcessorBase.STATE_FORCED_SHUTDOWN, executor.status());
         Assert.assertEquals(0, executor.remaining());
      } finally {
         executorService.shutdown();
      }
   }

   @Test
   public void shutdownNowWithBlocked() throws Exception {
      final ExecutorService executorService = Executors.newSingleThreadExecutor();
      try {
         final OrderedExecutor executor = new OrderedExecutor(executorService);
         final CyclicBarrier latch = new CyclicBarrier(2);
         final CyclicBarrier secondlatch = new CyclicBarrier(2);
         final CountDownLatch ran = new CountDownLatch(1);

         executor.execute(() -> {
            try {
               latch.await(1, TimeUnit.MINUTES);
               secondlatch.await(1, TimeUnit.MINUTES);
               ran.countDown();
            } catch (Exception e) {
               e.printStackTrace();
            }
         });


         for (int i = 0; i < 100; i++) {
            executor.execute(() -> System.out.println("Dont worry, this will never happen"));
         }

         latch.await();
         try {
            Assert.assertEquals(100, executor.shutdownNow());
         } finally {
            secondlatch.await();
         }

         Assert.assertEquals(ProcessorBase.STATE_FORCED_SHUTDOWN, executor.status());
         Assert.assertEquals(0, executor.remaining());
      } finally {
         executorService.shutdown();
      }
   }


   @Test
   public void testMeasure() throws InterruptedException {
      final ExecutorService executorService = Executors.newSingleThreadExecutor();
      try {
         final OrderedExecutor executor = new OrderedExecutor(executorService);
         int MAX_LOOP = 1_000_000;

         // extend the number for longer numbers
         int runs = 10;

         for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            final CountDownLatch executed = new CountDownLatch(MAX_LOOP);
            for (int l = 0; l < MAX_LOOP; l++) {
               executor.execute(executed::countDown);
            }
            Assert.assertTrue(executed.await(1, TimeUnit.MINUTES));
            long end = System.nanoTime();

            long elapsed = (end - start);

            log.info("execution " + i + " in " + TimeUnit.NANOSECONDS.toMillis(elapsed) + " milliseconds");
         }
      } finally {
         executorService.shutdown();
      }
   }

}
