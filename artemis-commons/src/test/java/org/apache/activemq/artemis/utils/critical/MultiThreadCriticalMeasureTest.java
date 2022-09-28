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

package org.apache.activemq.artemis.utils.critical;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

public class MultiThreadCriticalMeasureTest {

   private static final Logger logger = LoggerFactory.getLogger(MultiThreadCriticalMeasureTest.class);

   @Test
   public void testMultiThread() throws Throwable {
      int THREADS = 20;
      ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
      AtomicInteger errors = new AtomicInteger(0);
      AtomicBoolean running = new AtomicBoolean(true);
      AtomicBoolean load = new AtomicBoolean(true);
      ReusableLatch latchOnMeasure = new ReusableLatch(0);
      try {
         CriticalMeasure measure = new CriticalMeasure(null, 0);

         CyclicBarrier barrier = new CyclicBarrier(THREADS + 1);

         Runnable runnable = () -> {
            try {
               logger.debug("Thread " + Thread.currentThread().getName() + " waiting to Start");
               barrier.await();
               logger.debug("Thread " + Thread.currentThread().getName() + " Started");
               while (running.get()) {
                  if (!load.get()) {
                     // 1st barrier will let the unit test do its job
                     barrier.await();
                     // 2nd barrier waiting the test to finish its job
                     barrier.await();
                  }

                  try (AutoCloseable closeable = measure.measure()) {
                     latchOnMeasure.await();
                  }
               }
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }
         };

         for (int i = 0; i < THREADS; i++) {
            executorService.execute(runnable);
         }

         logger.debug("Going to release it now");
         barrier.await();

         for (int i = 0; i < 5; i++) {
            // Waiting some time to have load generated
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));

            // Disable load, so the measure threads will wait on the barrier
            load.set(false);

            // first barrier waiting the simulated load to stop
            barrier.await(10, TimeUnit.SECONDS);

            // waiting a few milliseconds as the bug was about measuring load after a no load
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
            Assert.assertFalse(measure.checkExpiration(TimeUnit.MILLISECONDS.toNanos(10), false));
            logger.debug("Count down");

            // letting load to happen again
            load.set(true);
            // Leaving barrier out so test is back on generating load
            barrier.await(10, TimeUnit.SECONDS);
         }

         latchOnMeasure.countUp();
         Assert.assertTrue(Wait.waitFor(() -> measure.checkExpiration(TimeUnit.MILLISECONDS.toNanos(100), false), 1_000, 1));

      } finally {
         load.set(true);
         running.set(false);
         latchOnMeasure.countDown();

         Assert.assertEquals(0, errors.get());
         executorService.shutdown();
         Wait.assertTrue(executorService::isShutdown);
         Wait.assertTrue(executorService::isTerminated, 5000, 1);
      }

   }
}
