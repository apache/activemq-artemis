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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.Wait;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

public class MultiThreadCriticalMeasureTest {

   private static final Logger logger = Logger.getLogger(MultiThreadCriticalMeasureTest.class);

   @Test
   public void testMultiThread() throws Throwable {
      int THREADS = 100;
      AtomicInteger errors = new AtomicInteger(0);
      Thread[] threads = new Thread[THREADS];
      AtomicBoolean running = new AtomicBoolean(true);
      ReusableLatch latch = new ReusableLatch(0);
      ReusableLatch latchOnMeasure = new ReusableLatch(0);
      try {
         CriticalMeasure measure = new CriticalMeasure(null, 0);

         CyclicBarrier barrier = new CyclicBarrier(THREADS + 1);

         Runnable runnable = () -> {
            try {
               logger.debug("Thread " + Thread.currentThread().getName() + " waiting to Star");
               barrier.await();
               logger.debug("Thread " + Thread.currentThread().getName() + " Started");
               while (running.get()) {
                  if (!latch.await(1, TimeUnit.NANOSECONDS)) {
                     latch.await();
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
            threads[i] = new Thread(runnable, "t=" + i);
            threads[i].start();
         }

         logger.debug("Going to release it now");
         barrier.await();

         for (int i = 0; i < 50; i++) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            logger.debug("Count up " + i);

            // simulating load down on the system... this will freeze load
            latch.countUp();
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
            Assert.assertFalse(measure.checkExpiration(TimeUnit.MILLISECONDS.toNanos(10), false));
            logger.debug("Count down");

            // this will resume load
            latch.countDown();
         }

         latchOnMeasure.countUp();

         Assert.assertTrue(Wait.waitFor(() -> measure.checkExpiration(TimeUnit.MILLISECONDS.toNanos(100), false), 1_000, 1));

      } finally {
         latch.countDown();
         latchOnMeasure.countDown();
         running.set(false);

         Assert.assertEquals(0, errors.get());

         for (Thread t : threads) {
            if (t != null) {
               t.join(100);
               if (t.isAlive()) {
                  t.interrupt();
               }
            }
         }

      }

   }
}
