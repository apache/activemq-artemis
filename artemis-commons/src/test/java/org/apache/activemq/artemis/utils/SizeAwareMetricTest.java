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
package org.apache.activemq.artemis.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class SizeAwareMetricTest {

   ExecutorService executor;

   private void setupExecutor(int threads) throws Exception {
      if (executor == null) {
         executor = Executors.newFixedThreadPool(threads);
      }
   }

   @After
   public void shutdownExecutor() throws Exception {
      if (executor != null) {
         executor.shutdownNow();
         Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
         executor = null;
      }
   }

   @Test
   public void testWithParent() throws Exception {
      AtomicBoolean childBoolean = new AtomicBoolean(false);
      AtomicBoolean parentBoolean = new AtomicBoolean(false);

      SizeAwareMetric child = new SizeAwareMetric(5, 5, 2, 2);
      SizeAwareMetric parent = new SizeAwareMetric(10, 10, 15, 15);

      child.setOnSizeCallback(parent::addSize);
      child.setOverCallback(() -> childBoolean.set(true));
      child.setUnderCallback(() -> childBoolean.set(false));

      parent.setOverCallback(() -> parentBoolean.set(true));
      parent.setUnderCallback(() -> parentBoolean.set(false));

      for (int i = 0; i < 4; i++) {
         child.addSize(1, true);
      }

      Assert.assertEquals(4, child.getSize());
      Assert.assertEquals(4, parent.getSize());
      Assert.assertEquals(0, child.getElements());
      Assert.assertEquals(0, parent.getElements());
      Assert.assertFalse(childBoolean.get());
      Assert.assertFalse(parentBoolean.get());

      child.addSize(1, true);
      Assert.assertEquals(5, child.getSize());
      Assert.assertTrue(childBoolean.get());
      Assert.assertFalse(parentBoolean.get());
      Assert.assertEquals(0, child.getElements());
      Assert.assertEquals(0, parent.getElements());


      child.addSize(-5, true);

      Assert.assertEquals(0, child.getSize());
      Assert.assertEquals(0, parent.getSize());


      for (int i = 0; i < 5; i++) {
         child.addSize(1, false);
      }

      Assert.assertEquals(5, child.getSize());
      Assert.assertEquals(5, parent.getSize());
      Assert.assertEquals(5, child.getElements());
      Assert.assertEquals(5, parent.getElements());
      Assert.assertTrue(childBoolean.get());
      Assert.assertFalse(parentBoolean.get());
      Assert.assertTrue(child.isOverElements());

      for (int i = 0; i < 5; i++) {
         child.addSize(1, false);
      }

      Assert.assertEquals(10, child.getSize());
      Assert.assertEquals(10, parent.getSize());
      Assert.assertEquals(10, child.getElements());
      Assert.assertEquals(10, parent.getElements());

      Assert.assertTrue(childBoolean.get());
      Assert.assertTrue(parentBoolean.get());
      Assert.assertTrue(child.isOverElements());
      Assert.assertFalse(parent.isOverElements());
      Assert.assertTrue(parent.isOverSize());

   }

   @Test
   public void testMultipleSizeAdd() throws Exception {
      final int THREADS = 10;
      final int ELEMENTS = 1000;
      setupExecutor(THREADS);

      final AtomicInteger errors = new AtomicInteger(0);

      final AtomicInteger globalMetricOverCalls = new AtomicInteger(0);
      final AtomicInteger metricOverCalls = new AtomicInteger(0);
      final AtomicInteger globalMetricUnderCalls = new AtomicInteger(0);
      final AtomicInteger metricUnderCalls = new AtomicInteger(0);

      final AtomicBoolean globalMetricOver = new AtomicBoolean(false);
      final AtomicBoolean metricOver = new AtomicBoolean(false);

      SizeAwareMetric metric = new SizeAwareMetric(1000, 500, -1, -1);
      SizeAwareMetric globalMetric = new SizeAwareMetric(10000, 500, -1, -1);

      metric.setOnSizeCallback(globalMetric::addSize);
      metric.setOverCallback(() -> {
         metricOver.set(true);
         metricOverCalls.incrementAndGet();
      });
      metric.setUnderCallback(() -> {
         metricOver.set(false);
         metricUnderCalls.incrementAndGet();
      });

      globalMetric.setOverCallback(() -> {
         globalMetricOver.set(true);
         globalMetricOverCalls.incrementAndGet();
      });
      globalMetric.setUnderCallback(() -> {
         globalMetricOver.set(false);
         globalMetricOverCalls.incrementAndGet();
      });

      ReusableLatch latchDone = new ReusableLatch(THREADS);

      CyclicBarrier flagStart = new CyclicBarrier(THREADS + 1);
      for (int istart = 0; istart < THREADS; istart++) {
         executor.execute(() -> {
            try {
               flagStart.await(10, TimeUnit.SECONDS);

               for (int iadd = 0; iadd < ELEMENTS; iadd++) {
                  metric.addSize(1, false);
               }
               latchDone.countDown();
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }
         });
      }

      flagStart.await(10, TimeUnit.SECONDS);
      Assert.assertTrue(latchDone.await(10, TimeUnit.SECONDS));

      Assert.assertTrue(metricOver.get());
      Assert.assertTrue(metric.isOver());
      Assert.assertTrue(metric.isOverSize());
      Assert.assertFalse(metric.isOverElements());

      Assert.assertTrue(globalMetricOver.get());
      Assert.assertTrue(globalMetric.isOver());

      Assert.assertEquals(1, metricOverCalls.get());
      Assert.assertEquals(1, globalMetricOverCalls.get());
      Assert.assertEquals(0, metricUnderCalls.get());
      Assert.assertEquals(0, globalMetricUnderCalls.get());

      Assert.assertEquals(ELEMENTS * THREADS, metric.getSize());
      Assert.assertEquals(ELEMENTS * THREADS, metric.getElements());
      Assert.assertEquals(ELEMENTS * THREADS, globalMetric.getSize());
      Assert.assertEquals(ELEMENTS * THREADS, globalMetric.getElements());

      Assert.assertEquals(0, errors.get());

      latchDone.setCount(10);

      for (int istart = 0; istart < 10; istart++) {
         executor.execute(() -> {
            try {
               flagStart.await(10, TimeUnit.SECONDS);

               for (int iadd = 0; iadd < ELEMENTS; iadd++) {
                  metric.addSize(-1, false);
               }
               latchDone.countDown();
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }
         });
      }

      flagStart.await(10, TimeUnit.SECONDS);
      Assert.assertTrue(latchDone.await(10, TimeUnit.SECONDS));

      Assert.assertEquals(0, globalMetric.getSize());
      Assert.assertEquals(0, globalMetric.getElements());
      Assert.assertEquals(0, metric.getSize());
      Assert.assertEquals(0, metric.getElements());
      Assert.assertFalse(globalMetricOver.get());
      Assert.assertFalse(globalMetric.isOver());
   }


   @Test
   public void testMaxElements() throws Exception {
      SizeAwareMetric metric = new SizeAwareMetric(10000, 500, 10,10);

      AtomicBoolean over = new AtomicBoolean(false);
      metric.setOverCallback(() -> over.set(true));
      metric.setUnderCallback(() -> over.set(false));

      for (int i = 0; i < 11; i++) {
         metric.addSize(10, false);
      }

      Assert.assertTrue(over.get());
      Assert.assertEquals(110, metric.getSize());
      Assert.assertEquals(11, metric.getElements());
      metric.addSize(1000, false);

      for (int i = 0; i < 12; i++) {
         metric.addSize(-10, false);
      }

      Assert.assertFalse(over.get());

   }
   @Test
   public void testMaxElementsReleaseNonSizeParentMetric() throws Exception {
      SizeAwareMetric metricMain = new SizeAwareMetric(10000, 500, 10,10);
      SizeAwareMetric metric = new SizeAwareMetric(10000, 500, 1000,1000);

      metric.setOnSizeCallback(metricMain::addSize);

      AtomicBoolean over = new AtomicBoolean(false);
      metricMain.setOverCallback(() -> over.set(true));
      metricMain.setUnderCallback(() -> over.set(false));

      for (int i = 0; i < 11; i++) {
         metric.addSize(10);
      }
      metric.addSize(1000, true);

      Assert.assertEquals(1110L, metricMain.getSize());
      Assert.assertEquals(11, metricMain.getElements());
      Assert.assertEquals(1110L, metric.getSize());
      Assert.assertEquals(11, metric.getElements());
      Assert.assertTrue(metricMain.isOverElements());
      Assert.assertFalse(metricMain.isOverSize());
      Assert.assertFalse(metric.isOverElements());
      Assert.assertFalse(metric.isOverSize());
      Assert.assertTrue(over.get());

      metric.addSize(-1000, true);

      Assert.assertEquals(110L, metricMain.getSize());
      Assert.assertEquals(11, metricMain.getElements());
      Assert.assertTrue(metricMain.isOverElements());
      Assert.assertFalse(metricMain.isOverSize());
      Assert.assertTrue(over.get());

      for (int i = 0; i < 11; i++) {
         metric.addSize(-10);
      }

      Assert.assertEquals(0L, metricMain.getSize());
      Assert.assertEquals(0L, metricMain.getElements());
      Assert.assertFalse(metricMain.isOver());
      Assert.assertEquals(0L, metric.getSize());
      Assert.assertEquals(0L, metric.getElements());
      Assert.assertFalse(metric.isOver());
      Assert.assertFalse(over.get());
   }


   @Test
   public void testMaxElementsReleaseNonSize() throws Exception {
      SizeAwareMetric metric = new SizeAwareMetric(10000, 500, 10,10);

      AtomicBoolean over = new AtomicBoolean(false);
      metric.setOverCallback(() -> over.set(true));
      metric.setUnderCallback(() -> over.set(false));

      for (int i = 0; i < 11; i++) {
         metric.addSize(10);
      }
      metric.addSize(1000, true);

      Assert.assertEquals(1110L, metric.getSize());
      Assert.assertEquals(11, metric.getElements());
      Assert.assertTrue(metric.isOverElements());
      Assert.assertFalse(metric.isOverSize());
      Assert.assertTrue(over.get());

      metric.addSize(-1000, true);

      Assert.assertEquals(110L, metric.getSize());
      Assert.assertEquals(11, metric.getElements());
      Assert.assertTrue(metric.isOverElements());
      Assert.assertFalse(metric.isOverSize());
      Assert.assertTrue(over.get());

      for (int i = 0; i < 11; i++) {
         metric.addSize(-10);
      }

      Assert.assertEquals(0L, metric.getSize());
      Assert.assertEquals(0L, metric.getElements());
      Assert.assertFalse(metric.isOver());
      Assert.assertFalse(over.get());
   }

   @Test
   public void testMultipleSizeAddMultipleInstances() throws Exception {
      final int THREADS = 10, ELEMENTS = 1000;
      setupExecutor(THREADS);

      final AtomicInteger errors = new AtomicInteger(0);

      final AtomicInteger globalMetricOverCalls = new AtomicInteger(0);
      final AtomicInteger metricOverCalls = new AtomicInteger(0);
      final AtomicInteger globalMetricUnderCalls = new AtomicInteger(0);
      final AtomicInteger metricUnderCalls = new AtomicInteger(0);

      final AtomicBoolean globalMetricOver = new AtomicBoolean(false);
      final AtomicBoolean[] metricOverArray = new AtomicBoolean[THREADS];

      SizeAwareMetric globalMetric = new SizeAwareMetric(10000, 500, 0, 0);

      SizeAwareMetric[] metric = new SizeAwareMetric[THREADS];


      globalMetric.setOverCallback(() -> {
         globalMetricOver.set(true);
         globalMetricOverCalls.incrementAndGet();
      });
      globalMetric.setUnderCallback(() -> {
         globalMetricOver.set(false);
         globalMetricUnderCalls.incrementAndGet();
      });

      ReusableLatch latchDone = new ReusableLatch(THREADS);

      CyclicBarrier flagStart = new CyclicBarrier(THREADS + 1);
      for (int istart = 0; istart < THREADS; istart++) {
         final AtomicBoolean metricOver = new AtomicBoolean(false);
         final SizeAwareMetric themetric = new SizeAwareMetric(1000, 500, 0, 0);
         themetric.setOnSizeCallback(globalMetric::addSize);
         themetric.setOverCallback(() -> {
            metricOver.set(true);
            metricOverCalls.incrementAndGet();
         });
         themetric.setUnderCallback(() -> {
            metricOver.set(false);
            metricUnderCalls.incrementAndGet();
         });
         metric[istart] = themetric;
         metricOverArray[istart] = metricOver;
         executor.execute(() -> {
            try {
               flagStart.await(10, TimeUnit.SECONDS);

               for (int iadd = 0; iadd < ELEMENTS; iadd++) {
                  themetric.addSize(1);
               }
               latchDone.countDown();
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }
         });
      }

      flagStart.await(10, TimeUnit.SECONDS);
      Assert.assertTrue(latchDone.await(10, TimeUnit.SECONDS));

      for (SizeAwareMetric theMetric : metric) {
         Assert.assertTrue(theMetric.isOver());
         Assert.assertEquals(ELEMENTS, theMetric.getSize());
         Assert.assertEquals(ELEMENTS, theMetric.getElements());
      }

      for (AtomicBoolean theBool : metricOverArray) {
         Assert.assertTrue(theBool.get());
      }

      Assert.assertTrue(globalMetricOver.get());
      Assert.assertTrue(globalMetric.isOver());

      Assert.assertEquals(10, metricOverCalls.get());
      Assert.assertEquals(1, globalMetricOverCalls.get());
      Assert.assertEquals(0, metricUnderCalls.get());
      Assert.assertEquals(0, globalMetricUnderCalls.get());

      Assert.assertEquals(ELEMENTS * THREADS, globalMetric.getSize());
      Assert.assertEquals(ELEMENTS * THREADS, globalMetric.getElements());

      Assert.assertEquals(0, errors.get());

      latchDone.setCount(10);

      for (int istart = 0; istart < 10; istart++) {
         SizeAwareMetric theMetric = metric[istart];
         executor.execute(() -> {
            try {
               flagStart.await(10, TimeUnit.SECONDS);

               for (int iadd = 0; iadd < ELEMENTS; iadd++) {
                  theMetric.addSize(-1);
               }
               latchDone.countDown();
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }
         });
      }

      flagStart.await(10, TimeUnit.SECONDS);
      Assert.assertTrue(latchDone.await(10, TimeUnit.SECONDS));

      Assert.assertEquals(0, globalMetric.getSize());
      Assert.assertEquals(0, globalMetric.getElements());
      for (SizeAwareMetric theMetric : metric) {
         Assert.assertEquals(0, theMetric.getSize());
         Assert.assertEquals(0, theMetric.getElements());
      }

      Assert.assertEquals(10, metricOverCalls.get());
      Assert.assertEquals(1, globalMetricOverCalls.get());
      Assert.assertEquals(10, metricUnderCalls.get());
      Assert.assertEquals(1, globalMetricUnderCalls.get());
      Assert.assertFalse(globalMetricOver.get());
      Assert.assertFalse(globalMetric.isOver());
      for (AtomicBoolean theBool : metricOverArray) {
         Assert.assertFalse(theBool.get());
      }
   }

   @Test
   public void testUpdateMax() throws Exception {
      AtomicBoolean over = new AtomicBoolean(false);
      SizeAwareMetric metric = new SizeAwareMetric(1000, 500, -1, -1);
      metric.setOverCallback(() -> over.set(true));
      metric.setUnderCallback(() -> over.set(false));

      metric.addSize(900);
      Assert.assertFalse(over.get());

      metric.setMax(800, 700, 0, 0);
      Assert.assertTrue(over.get());

      metric.addSize(-200);
      Assert.assertFalse(over.get());
   }

   @Test
   public void testDisabled() throws Exception {
      AtomicBoolean over = new AtomicBoolean(false);
      SizeAwareMetric metric = new SizeAwareMetric(0, 0, -1, -1);
      metric.setSizeEnabled(false);
      metric.setOverCallback(() -> over.set(true));
      metric.addSize(100);
      Assert.assertEquals(100, metric.getSize());
      Assert.assertEquals(1, metric.getElements());
      Assert.assertFalse(over.get());
   }

   @Test
   public void testMultipleNonSized() throws Exception {
      AtomicBoolean over = new AtomicBoolean(false);
      final SizeAwareMetric metricMain = new SizeAwareMetric(0, 0, 1, 1);
      SizeAwareMetric metric = new SizeAwareMetric(0, 0, 1, 1);
      metric.setSizeEnabled(false);
      metric.setOverCallback(() -> over.set(true));
      metric.setOnSizeCallback(metricMain::addSize);
      for (int i = 0; i  < 10; i++) {
         metric.addSize(10, true);
      }

      Assert.assertEquals(100, metricMain.getSize());
      Assert.assertEquals(100, metric.getSize());
      Assert.assertEquals(0, metricMain.getElements());
      Assert.assertEquals(0, metric.getElements());

      for (int i = 0; i  < 10; i++) {
         metric.addSize(10, false);
      }

      Assert.assertEquals(200, metricMain.getSize());
      Assert.assertEquals(200, metric.getSize());
      Assert.assertEquals(10, metricMain.getElements());
      Assert.assertEquals(10, metric.getElements());
   }

   @Test
   public void testResetNeverUsed() throws Exception {
      SizeAwareMetric metric = new SizeAwareMetric(0, 0, 0, 0);
      AtomicBoolean over = new AtomicBoolean(false);

      metric.setOverCallback(() -> over.set(true));
      metric.setElementsEnabled(true);
      metric.setSizeEnabled(true);
      metric.setMax(0, 0, 0, 0);
      Assert.assertFalse(over.get());
      Assert.assertFalse(metric.isOver());
   }

   @Test
   public void testSwitchSides() throws Exception {
      SizeAwareMetric metric = new SizeAwareMetric(2000, 2000, 1, 1);
      AtomicBoolean over = new AtomicBoolean(false);

      metric.setOverCallback(() -> over.set(true));
      metric.setUnderCallback(() -> over.set(false));
      metric.setElementsEnabled(true);
      metric.setSizeEnabled(true);

      metric.addSize(2500, true);

      Assert.assertTrue(over.get());

      metric.addSize(1000);

      Assert.assertTrue(metric.isOverSize());

      metric.addSize(-2500, true);

      // Even though we are free from maxSize, we are still bound by maxElements, it should still be over
      Assert.assertTrue(over.get());
      Assert.assertTrue("Switch did not work", metric.isOverElements());

      Assert.assertEquals(1, metric.getElements());
      Assert.assertEquals(1000, metric.getSize());

      metric.addSize(5000, true);

      Assert.assertTrue(metric.isOverElements());
      Assert.assertEquals(6000, metric.getSize());

      metric.addSize(-1000);

      Assert.assertTrue(metric.isOverSize());
      Assert.assertEquals(0, metric.getElements());
      Assert.assertEquals(5000, metric.getSize());

      metric.addSize(-5000, true);
      Assert.assertFalse(metric.isOver());
      Assert.assertEquals(0, metric.getSize());
      Assert.assertEquals(0, metric.getElements());
   }


   @Test
   public void testMTOverAndUnder() throws Exception {
      final int THREADS = 10;
      final int ELEMENTS = 100;
      setupExecutor(THREADS * 2);

      SizeAwareMetric metric = new SizeAwareMetric(1, 1, -1, -1);
      AtomicInteger overCounter = new AtomicInteger(0);
      AtomicInteger errors = new AtomicInteger(0);
      metric.setOverCallback(() -> {
         int value = overCounter.incrementAndGet();
         if (value > 1) {
            new Exception("Value = " + value).printStackTrace();
            errors.incrementAndGet();
         }
      });
      metric.setUnderCallback(() -> {
         int value = overCounter.decrementAndGet();
         if (value < 0) {
            new Exception("Value = " + value).printStackTrace();
            errors.incrementAndGet();
         }
      });

      CyclicBarrier flagStart = new CyclicBarrier(THREADS * 2);

      CountDownLatch done = new CountDownLatch(THREADS * 2);

      for (int i = 0; i < THREADS; i++) {
         executor.execute(() -> {
            try {
               flagStart.await(10, TimeUnit.SECONDS);
               for (int repeat = 0; repeat < ELEMENTS; repeat++) {
                  metric.addSize(1);
               }
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }
            done.countDown();
         });
         executor.execute(() -> {
            try {
               flagStart.await(10, TimeUnit.SECONDS);
               for (int repeat = 0; repeat < ELEMENTS; repeat++) {
                  metric.addSize(-1);
               }
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }
            done.countDown();
         });
      }

      Assert.assertTrue(done.await(10, TimeUnit.SECONDS));


      Assert.assertEquals(0, metric.getSize());
      Assert.assertEquals(0, metric.getElements());
      Assert.assertEquals(0, errors.get());

   }

}
