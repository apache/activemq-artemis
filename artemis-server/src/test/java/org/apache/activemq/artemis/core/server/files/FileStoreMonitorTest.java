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

package org.apache.activemq.artemis.core.server.files;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileStoreMonitorTest extends ActiveMQTestBase {

   private static final Logger log = Logger.getLogger(FileStoreMonitorTest.class);

   private ScheduledExecutorService scheduledExecutorService;
   private ExecutorService executorService;

   @Before
   public void startScheduled() {
      scheduledExecutorService = new ScheduledThreadPoolExecutor(5);
      executorService = Executors.newSingleThreadExecutor();
   }

   @After
   public void stopScheduled() {
      scheduledExecutorService.shutdown();
      scheduledExecutorService = null;
      executorService.shutdown();
   }

   @Test
   public void testSimpleTick() throws Exception {
      File garbageFile = new File(getTestDirfile(), "garbage.bin");
      FileOutputStream garbage = new FileOutputStream(garbageFile);
      BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(garbage);
      PrintStream out = new PrintStream(bufferedOutputStream);

      // This is just to make sure there is at least something on the device.
      // If the testsuite is running with an empty tempFS, it would return 0 and the assertion would fail.
      for (int i = 0; i < 100; i++) {
         out.println("Garbage " + i);
      }

      bufferedOutputStream.close();

      final AtomicInteger over = new AtomicInteger(0);
      final AtomicInteger under = new AtomicInteger(0);
      final AtomicInteger tick = new AtomicInteger(0);

      FileStoreMonitor.Callback callback = new FileStoreMonitor.Callback() {
         @Override
         public void tick(long usableSpace, long totalSpace) {
            tick.incrementAndGet();
            log.debug("tick:: usableSpace: " + usableSpace + ", totalSpace:" + totalSpace);
         }

         @Override
         public void over(long usableSpace, long totalSpace) {
            over.incrementAndGet();
            log.debug("over:: usableSpace: " + usableSpace + ", totalSpace:" + totalSpace);
         }

         @Override
         public void under(long usableSpace, long totalSpace) {
            under.incrementAndGet();
            log.debug("under:: usableSpace: " + usableSpace + ", totalSpace:" + totalSpace);
         }
      };
      FileStoreMonitor storeMonitor = new FileStoreMonitor(scheduledExecutorService, executorService, 100, TimeUnit.MILLISECONDS, 0.999, null);
      storeMonitor.addCallback(callback);
      storeMonitor.addStore(getTestDirfile());

      storeMonitor.tick();

      Assert.assertEquals(0, over.get());
      Assert.assertEquals(1, tick.get());
      Assert.assertEquals(1, under.get());

      storeMonitor.setMaxUsage(0);

      storeMonitor.tick();

      Assert.assertEquals(1, over.get());
      Assert.assertEquals(2, tick.get());
      Assert.assertEquals(1, under.get());
   }

   @Test
   public void testScheduler() throws Exception {

      FileStoreMonitor storeMonitor = new FileStoreMonitor(scheduledExecutorService, executorService, 20, TimeUnit.MILLISECONDS, 0.9, null);

      final ReusableLatch latch = new ReusableLatch(5);
      storeMonitor.addStore(getTestDirfile());
      storeMonitor.addCallback(new FileStoreMonitor.Callback() {
         @Override
         public void tick(long usableSpace, long totalSpace) {
            log.debug("Tick");
            latch.countDown();
         }

         @Override
         public void over(long usableSpace, long totalSpace) {

         }

         @Override
         public void under(long usableSpace, long totalSpace) {

         }
      });
      storeMonitor.start();

      Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));

      storeMonitor.stop();

      latch.setCount(1);

      Assert.assertFalse(latch.await(100, TimeUnit.MILLISECONDS));

      //      FileStoreMonitor monitor = new FileStoreMonitor()

   }
}
