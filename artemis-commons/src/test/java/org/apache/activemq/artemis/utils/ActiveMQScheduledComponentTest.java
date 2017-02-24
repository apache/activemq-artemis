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

package org.apache.activemq.artemis.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ActiveMQScheduledComponentTest {

   @Rule
   public ThreadLeakCheckRule rule = new ThreadLeakCheckRule();

   ScheduledExecutorService scheduledExecutorService;
   ExecutorService executorService;

   @Before
   public void before() {
      scheduledExecutorService = new ScheduledThreadPoolExecutor(5);
      executorService = Executors.newSingleThreadExecutor();
   }

   @After
   public void after() {
      executorService.shutdown();
      scheduledExecutorService.shutdown();
   }

   @Test
   public void testAccumulation() throws Exception {
      final AtomicInteger count = new AtomicInteger(0);

      final ActiveMQScheduledComponent local = new ActiveMQScheduledComponent(scheduledExecutorService, executorService, 100, TimeUnit.MILLISECONDS, false) {
         @Override
         public void run() {
            if (count.get() == 0) {
               try {
                  Thread.sleep(800);
               } catch (Exception e) {
               }
            }
            count.incrementAndGet();
         }
      };

      local.start();

      Thread.sleep(1000);

      local.stop();

      Assert.assertTrue("just because one took a lot of time, it doesn't mean we can accumulate many, we got " + count + " executions", count.get() < 5);
   }

   @Test
   public void testAccumulationOwnPool() throws Exception {
      final AtomicInteger count = new AtomicInteger(0);

      final ActiveMQScheduledComponent local = new ActiveMQScheduledComponent(100, TimeUnit.MILLISECONDS, false) {
         @Override
         public void run() {
            if (count.get() == 0) {
               try {
                  Thread.sleep(500);
               } catch (Exception e) {
               }
            }
            count.incrementAndGet();
         }
      };

      local.start();

      Thread.sleep(1000);

      local.stop();

      Assert.assertTrue("just because one took a lot of time, it doesn't mean we can accumulate many, we got " + count + " executions", count.get() <= 5 && count.get() > 0);
   }

   @Test
   public void testUsingOwnExecutors() throws Exception {
      final CountDownLatch latch = new CountDownLatch(1);

      final ActiveMQScheduledComponent local = new ActiveMQScheduledComponent(10, TimeUnit.MILLISECONDS, false) {
         @Override
         public void run() {
            latch.countDown();
         }
      };

      local.start();
      local.start(); // should be ok to call start again

      try {
         Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

         // re-scheduling the executor at a big interval..
         // just to make sure it won't hung
         local.setTimeUnit(TimeUnit.HOURS);
         local.setPeriod(1);
      } finally {
         local.stop();
         local.stop(); // should be ok to call stop again
      }
   }

   @Test
   public void testUsingOwnExecutorsOnDemand() throws Throwable {
      final ReusableLatch latch = new ReusableLatch(1);

      final ActiveMQScheduledComponent local = new ActiveMQScheduledComponent(10, TimeUnit.MILLISECONDS, true) {
         @Override
         public void run() {
            latch.countDown();
         }
      };

      local.start();
      local.start(); // should be ok to call start again

      try {

         Assert.assertFalse(latch.await(20, TimeUnit.MILLISECONDS));

         local.delay();
         Assert.assertTrue(latch.await(20, TimeUnit.MILLISECONDS));
         latch.setCount(1);

         Assert.assertFalse(latch.await(20, TimeUnit.MILLISECONDS));

         // re-scheduling the executor at a big interval..
         // just to make sure it won't hung
         local.setTimeUnit(TimeUnit.HOURS);
         local.setPeriod(1);
      } finally {
         local.stop();
         local.stop(); // calling stop again should not be an issue.
      }
   }

}
