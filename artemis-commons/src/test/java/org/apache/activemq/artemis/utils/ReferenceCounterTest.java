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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

public class ReferenceCounterTest extends Assert {

   class LatchRunner implements Runnable {

      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicInteger counts = new AtomicInteger(0);
      volatile Thread lastThreadUsed;

      @Override
      public void run() {
         counts.incrementAndGet();
         latch.countDown();
      }
   }

   @Test
   public void testReferenceNoExecutor() throws Exception {
      internalTestReferenceNoExecutor(null);
   }

   @Test
   public void testReferenceWithExecutor() throws Exception {
      ExecutorService executor = Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory());
      internalTestReferenceNoExecutor(executor);
      executor.shutdown();
   }

   @Test
   public void testReferenceValidExecutorUsed() throws Exception {
      ExecutorService executor = Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory());
      LatchRunner runner = new LatchRunner();
      ReferenceCounterUtil counter = new ReferenceCounterUtil(runner, executor);
      counter.increment();
      counter.decrement();

      runner.latch.await(5, TimeUnit.SECONDS);

      assertNotSame(runner.lastThreadUsed, Thread.currentThread());

      executor.shutdown();
   }

   public void internalTestReferenceNoExecutor(Executor executor) throws Exception {
      LatchRunner runner = new LatchRunner();

      final ReferenceCounterUtil ref;

      if (executor == null) {
         ref = new ReferenceCounterUtil(runner);
      } else {
         ref = new ReferenceCounterUtil(runner, executor);
      }

      Thread[] t = new Thread[100];

      for (int i = 0; i < t.length; i++) {
         t[i] = new Thread() {
            @Override
            public void run() {
               ref.increment();
            }
         };
         t[i].start();
      }

      for (Thread tx : t) {
         tx.join();
      }

      for (int i = 0; i < t.length; i++) {
         t[i] = new Thread() {
            @Override
            public void run() {
               ref.decrement();
            }
         };
         t[i].start();
      }

      for (Thread tx : t) {
         tx.join();
      }

      assertTrue(runner.latch.await(5, TimeUnit.SECONDS));

      assertEquals(1, runner.counts.get());

   }
}
