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

import java.util.concurrent.CountDownLatch;

import org.apache.activemq.artemis.tests.unit.UnitTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.junit.Assert;
import org.junit.Test;

public class ReusableLatchTest extends ActiveMQTestBase {

   @Test
   public void testLatchWithParameterizedDown() throws Exception {
      ReusableLatch latch = new ReusableLatch(1000);

      latch.countDown(5000);

      assertTrue(latch.await(1000));

      assertEquals(0, latch.getCount());
   }

   @Test
   public void testLatchOnSingleThread() throws Exception {
      ReusableLatch latch = new ReusableLatch();

      for (int i = 1; i <= 100; i++) {
         latch.countUp();
         Assert.assertEquals(i, latch.getCount());
      }

      for (int i = 100; i > 0; i--) {
         Assert.assertEquals(i, latch.getCount());
         latch.countDown();
         Assert.assertEquals(i - 1, latch.getCount());
      }

      latch.await();
   }

   /**
    * This test will open numberOfThreads threads, and add numberOfAdds on the
    * VariableLatch After those addthreads are finished, the latch count should
    * be numberOfThreads * numberOfAdds Then it will open numberOfThreads
    * threads again releasing numberOfAdds on the VariableLatch After those
    * releaseThreads are finished, the latch count should be 0 And all the
    * waiting threads should be finished also
    *
    * @throws Exception
    */
   @Test
   public void testLatchOnMultiThread() throws Exception {
      final ReusableLatch latch = new ReusableLatch();

      latch.countUp(); // We hold at least one, so ThreadWaits won't go away

      final int numberOfThreads = 100;
      final int numberOfAdds = 100;

      class ThreadWait extends Thread {

         private volatile boolean waiting = true;

         @Override
         public void run() {
            try {
               if (!latch.await(5000)) {
                  UnitTestLogger.LOGGER.error("Latch timed out");
               }
            } catch (Exception e) {
               UnitTestLogger.LOGGER.error(e);
            }
            waiting = false;
         }
      }

      class ThreadAdd extends Thread {

         private final CountDownLatch latchReady;

         private final CountDownLatch latchStart;

         ThreadAdd(final CountDownLatch latchReady, final CountDownLatch latchStart) {
            this.latchReady = latchReady;
            this.latchStart = latchStart;
         }

         @Override
         public void run() {
            try {
               latchReady.countDown();
               // Everybody should start at the same time, to worse concurrency
               // effects
               latchStart.await();
               for (int i = 0; i < numberOfAdds; i++) {
                  latch.countUp();
               }
            } catch (Exception e) {
               UnitTestLogger.LOGGER.error(e.getMessage(), e);
            }
         }
      }

      CountDownLatch latchReady = new CountDownLatch(numberOfThreads);
      CountDownLatch latchStart = new CountDownLatch(1);

      ThreadAdd[] threadAdds = new ThreadAdd[numberOfThreads];
      ThreadWait[] waits = new ThreadWait[numberOfThreads];

      for (int i = 0; i < numberOfThreads; i++) {
         threadAdds[i] = new ThreadAdd(latchReady, latchStart);
         threadAdds[i].start();
         waits[i] = new ThreadWait();
         waits[i].start();
      }

      latchReady.await();
      latchStart.countDown();

      for (int i = 0; i < numberOfThreads; i++) {
         threadAdds[i].join();
      }

      for (int i = 0; i < numberOfThreads; i++) {
         Assert.assertTrue(waits[i].waiting);
      }

      Assert.assertEquals(numberOfThreads * numberOfAdds + 1, latch.getCount());

      class ThreadDown extends Thread {

         private final CountDownLatch latchReady;

         private final CountDownLatch latchStart;

         ThreadDown(final CountDownLatch latchReady, final CountDownLatch latchStart) {
            this.latchReady = latchReady;
            this.latchStart = latchStart;
         }

         @Override
         public void run() {
            try {
               latchReady.countDown();
               // Everybody should start at the same time, to worse concurrency
               // effects
               latchStart.await();
               for (int i = 0; i < numberOfAdds; i++) {
                  latch.countDown();
               }
            } catch (Exception e) {
               UnitTestLogger.LOGGER.error(e.getMessage(), e);
            }
         }
      }

      latchReady = new CountDownLatch(numberOfThreads);
      latchStart = new CountDownLatch(1);

      ThreadDown[] down = new ThreadDown[numberOfThreads];

      for (int i = 0; i < numberOfThreads; i++) {
         down[i] = new ThreadDown(latchReady, latchStart);
         down[i].start();
      }

      latchReady.await();
      latchStart.countDown();

      for (int i = 0; i < numberOfThreads; i++) {
         down[i].join();
      }

      Assert.assertEquals(1, latch.getCount());

      for (int i = 0; i < numberOfThreads; i++) {
         Assert.assertTrue(waits[i].waiting);
      }

      latch.countDown();

      for (int i = 0; i < numberOfThreads; i++) {
         waits[i].join();
      }

      Assert.assertEquals(0, latch.getCount());

      for (int i = 0; i < numberOfThreads; i++) {
         Assert.assertFalse(waits[i].waiting);
      }
   }

   @Test
   public void testReuseLatch() throws Exception {
      final ReusableLatch latch = new ReusableLatch(5);
      for (int i = 0; i < 5; i++) {
         latch.countDown();
      }

      latch.countUp();

      class ThreadWait extends Thread {

         private volatile boolean waiting = false;

         private volatile Exception e;

         private final CountDownLatch readyLatch = new CountDownLatch(1);

         @Override
         public void run() {
            waiting = true;
            readyLatch.countDown();
            try {
               if (!latch.await(1000)) {
                  UnitTestLogger.LOGGER.error("Latch timed out!", new Exception("trace"));
               }
            } catch (Exception e) {
               UnitTestLogger.LOGGER.error(e);
               this.e = e;
            }
            waiting = false;
         }
      }

      ThreadWait t = new ThreadWait();
      t.start();

      t.readyLatch.await();

      Assert.assertEquals(true, t.waiting);

      latch.countDown();

      t.join();

      Assert.assertEquals(false, t.waiting);

      Assert.assertNull(t.e);

      latch.countUp();

      t = new ThreadWait();
      t.start();

      t.readyLatch.await();

      Assert.assertEquals(true, t.waiting);

      latch.countDown();

      t.join();

      Assert.assertEquals(false, t.waiting);

      Assert.assertNull(t.e);

      Assert.assertTrue(latch.await(1000));

      Assert.assertEquals(0, latch.getCount());

      latch.countDown();

      Assert.assertEquals(0, latch.getCount());

   }

}
