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

package org.apache.activemq.artemis.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ActiveMQThreadPoolTest {

   private static final Logger logger = Logger.getLogger(ActiveMQThreadPoolTest.class);

   @Rule
   public ThreadLeakCheckRule checkRule = new ThreadLeakCheckRule();



   @Test
   public void testMaxedPool() throws Throwable {

      int blockedThreads = 5;

      ActiveMQThreadFactory factory = new ActiveMQThreadFactory("test", "test", true, ActiveMQThreadPoolExecutor.class.getClassLoader());
      ActiveMQThreadPoolExecutor executor = new ActiveMQThreadPoolExecutor(1, blockedThreads, 500l, TimeUnit.MILLISECONDS, factory);

      try {


         // These are beyond the limit
         int waitingThreads = 1000;

         CyclicBarrier cyclicBarrier = new CyclicBarrier(blockedThreads + 1);

         CountDownLatch complete = new CountDownLatch(blockedThreads + waitingThreads);
         CountDownLatch waiting = new CountDownLatch(1);
         AtomicInteger intvalue = new AtomicInteger(0);

         for (int i = 0; i < blockedThreads; i++) {
            executor.execute(() -> {
               try {
                  cyclicBarrier.await(10, TimeUnit.SECONDS);
                  logger.debug("Cycle is completed " + intvalue.incrementAndGet());
                  waiting.await(10, TimeUnit.SECONDS);
                  complete.countDown();

               } catch (Throwable e) {
                  e.printStackTrace();
               }
            });
         }

         cyclicBarrier.await(10, TimeUnit.SECONDS);

         Thread.sleep(1000);


         for (int i = 0; i < waitingThreads; i++) {
            executor.execute(() -> {
               try {
                  waiting.await(10, TimeUnit.SECONDS);
                  logger.debug("Thread " + Thread.currentThread().getName() + " work");
                  complete.countDown();

               } catch (Throwable e) {
                  e.printStackTrace();
               }
            });

         }


         Thread.sleep(1000);
         logger.debug("Cycle complete");

         waiting.countDown();

         Assert.assertTrue(complete.await(10, TimeUnit.SECONDS));
      } finally {
         executor.shutdownNow();
      }
   }

   @Test
   public void testMaxedPoolFromMultipleThreads() throws Throwable {


      int blocking = 10;
      ActiveMQThreadFactory factory = new ActiveMQThreadFactory("test", "test", true, ActiveMQThreadPoolExecutor.class.getClassLoader());
      ActiveMQThreadPoolExecutor executor = new ActiveMQThreadPoolExecutor(1, blocking, 500l, TimeUnit.MILLISECONDS, factory);

      AtomicInteger errors = new AtomicInteger(0);

      try {


         // These are beyond the limit
         int producers = 100;
         int runs = 1;

         // This will make 100 threads trying to call an executor.execute
         CyclicBarrier cyclicBarrierProducer = new CyclicBarrier(producers + 1);
         CountDownLatch blockingLatch = new CountDownLatch(blocking);
         CountDownLatch latchWaiting = new CountDownLatch(1);
         CountDownLatch done = new CountDownLatch(producers * runs);
         AtomicInteger countDone = new AtomicInteger(0);


         for (int i = 0; i < producers; i++) {
            Thread thread = new Thread() {
               public void run () {
                  try {
                     cyclicBarrierProducer.await(10, TimeUnit.SECONDS);

                     //System.out.println("I'm ready to execute");

                     for (int i = 0; i < runs; i++) {
                        //System.out.println("Submiting execution");
                        executor.execute(() -> {
                           blockingLatch.countDown();

                           try {
                              latchWaiting.await(10, TimeUnit.SECONDS);

                              //System.out.println("Awaited");

                           } catch (Throwable e) {
                              logger.warn(e.getMessage(), e);
                              errors.incrementAndGet();
                           }

                           countDone.incrementAndGet();
                           done.countDown();
                        });
                     }

                  } catch (Throwable e) {
                     logger.warn(e.getMessage(), e);
                     errors.incrementAndGet();
                  }
               }
            };
            thread.start();
         }

         cyclicBarrierProducer.await(10, TimeUnit.SECONDS);
         System.out.println("Waiting at executorBarrier");
         Assert.assertTrue(blockingLatch.await(10, TimeUnit.SECONDS));
         System.out.println("Arrived at executorBarrier");
         latchWaiting.countDown();

         Assert.assertTrue(done.await(10, TimeUnit.SECONDS));
         Assert.assertEquals(producers * runs, countDone.get());
         Assert.assertEquals(0, errors.get());
      } finally {
         executor.shutdownNow();
      }
   }

}
