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
package org.apache.activemq.artemis;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClientThreadPoolsTest {

   private static Properties systemProperties;

   @BeforeClass
   public static void setup() {
      systemProperties = System.getProperties();
   }

   @AfterClass
   public static void tearDown() {
      System.clearProperty(ActiveMQClient.THREAD_POOL_MAX_SIZE_PROPERTY_KEY);
      System.clearProperty(ActiveMQClient.SCHEDULED_THREAD_POOL_SIZE_PROPERTY_KEY);
      ActiveMQClient.initializeGlobalThreadPoolProperties();
      ActiveMQClient.clearThreadPools();
      Assert.assertEquals(ActiveMQClient.DEFAULT_GLOBAL_THREAD_POOL_MAX_SIZE, ActiveMQClient.getGlobalThreadPoolSize());
   }

   @Test
   public void testSystemPropertyThreadPoolSettings() throws Exception {
      int threadPoolMaxSize = 100;
      int scheduledThreadPoolSize = 10;

      System.setProperty(ActiveMQClient.THREAD_POOL_MAX_SIZE_PROPERTY_KEY, "" + threadPoolMaxSize);
      System.setProperty(ActiveMQClient.SCHEDULED_THREAD_POOL_SIZE_PROPERTY_KEY, "" + scheduledThreadPoolSize);
      ActiveMQClient.initializeGlobalThreadPoolProperties();
      ActiveMQClient.clearThreadPools();

      testSystemPropertiesThreadPoolSettings(threadPoolMaxSize, scheduledThreadPoolSize);
   }

   @Test
   public void testShutdownPoolInUse() throws Exception {
      ActiveMQClient.setGlobalThreadPoolProperties(10, 1);
      ActiveMQClient.clearThreadPools();

      final CountDownLatch inUse = new CountDownLatch(1);
      final CountDownLatch neverLeave = new CountDownLatch(1);

      ActiveMQClient.getGlobalThreadPool().execute(new Runnable() {
         @Override
         public void run() {
            System.err.println("Hello!");
            try {
               inUse.countDown();
               neverLeave.await();
            } catch (Exception e) {
               e.printStackTrace();
               neverLeave.countDown();
            }
         }
      });

      Assert.assertTrue(inUse.await(10, TimeUnit.SECONDS));
      ActiveMQClient.clearThreadPools(100, TimeUnit.MILLISECONDS);
      Assert.assertTrue(neverLeave.await(10, TimeUnit.SECONDS));
   }

   @Test
   public void testInjectPools() throws Exception {
      ActiveMQClient.clearThreadPools();

      ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

      ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, ActiveMQThreadFactory.defaultThreadFactory());

      ActiveMQClient.injectPools(poolExecutor, scheduledThreadPoolExecutor);

      final CountDownLatch inUse = new CountDownLatch(1);
      final CountDownLatch neverLeave = new CountDownLatch(1);

      ActiveMQClient.getGlobalThreadPool().execute(new Runnable() {
         @Override
         public void run() {
            System.err.println("Hello!");
            try {
               inUse.countDown();
               neverLeave.await();
            } catch (Exception e) {
               e.printStackTrace();
               neverLeave.countDown();
            }
         }
      });

      Assert.assertTrue(inUse.await(10, TimeUnit.SECONDS));
      poolExecutor.shutdownNow();
      scheduledThreadPoolExecutor.shutdownNow();
      Assert.assertTrue(neverLeave.await(10, TimeUnit.SECONDS));

      Assert.assertTrue(inUse.await(10, TimeUnit.SECONDS));
      Assert.assertTrue(neverLeave.await(10, TimeUnit.SECONDS));

      ActiveMQClient.clearThreadPools(100, TimeUnit.MILLISECONDS);
   }

   @Test
   public void testStaticPropertiesThreadPoolSettings() throws Exception {

      int testMaxSize = 999;
      int testScheduleSize = 9;

      ActiveMQClient.setGlobalThreadPoolProperties(testMaxSize, testScheduleSize);
      ActiveMQClient.clearThreadPools();
      testSystemPropertiesThreadPoolSettings(testMaxSize, testScheduleSize);
   }

   @Test
   public void testSmallPool() throws Exception {

      int testMaxSize = 2;
      int testScheduleSize = 9;

      ActiveMQClient.setGlobalThreadPoolProperties(testMaxSize, testScheduleSize);
      ActiveMQClient.clearThreadPools();
      testSystemPropertiesThreadPoolSettings(testMaxSize, testScheduleSize);
   }

   private void testSystemPropertiesThreadPoolSettings(int expectedMax, int expectedScheduled) throws Exception {
      ServerLocatorImpl serverLocator = new ServerLocatorImpl(false);
      serverLocator.isUseGlobalPools();

      Method setThreadPools = ServerLocatorImpl.class.getDeclaredMethod("setThreadPools");
      setThreadPools.setAccessible(true);
      setThreadPools.invoke(serverLocator);

      // TODO: I would get this from the ActiveMQClient
      Field threadPoolField = ServerLocatorImpl.class.getDeclaredField("threadPool");
      Field scheduledThreadPoolField = ServerLocatorImpl.class.getDeclaredField("scheduledThreadPool");

      threadPoolField.setAccessible(true);
      scheduledThreadPoolField.setAccessible(true);

      ThreadPoolExecutor threadPool = (ThreadPoolExecutor) ActiveMQClient.getGlobalThreadPool();

      final CountDownLatch doneMax = new CountDownLatch(expectedMax);
      final CountDownLatch latch = new CountDownLatch(1);
      final CountDownLatch latchTotal = new CountDownLatch(expectedMax * 3); // we will schedule 3 * max, so all runnables should execute
      final AtomicInteger errors = new AtomicInteger(0);

      // Set this to true if you need to debug why executions are not being performed.
      final boolean debugExecutions = false;

      for (int i = 0; i < expectedMax * 3; i++) {
         final int localI = i;
         threadPool.execute(new Runnable() {
            @Override
            public void run() {
               try {

                  if (debugExecutions) {
                     System.out.println("runnable " + localI);
                  }
                  doneMax.countDown();
                  latch.await();
                  latchTotal.countDown();
               } catch (Exception e) {
                  errors.incrementAndGet();
               } finally {
                  if (debugExecutions) {
                     System.out.println("done " + localI);
                  }
               }
            }
         });
      }

      Assert.assertTrue(doneMax.await(5, TimeUnit.SECONDS));
      latch.countDown();
      Assert.assertTrue(latchTotal.await(5, TimeUnit.SECONDS));

      ScheduledThreadPoolExecutor scheduledThreadPool = (ScheduledThreadPoolExecutor) scheduledThreadPoolField.get(serverLocator);

      assertEquals(expectedMax, threadPool.getMaximumPoolSize());
      assertEquals(expectedScheduled, scheduledThreadPool.getCorePoolSize());
   }

   @Test
   public void testThreadPoolInjection() throws Exception {

      ServerLocator serverLocator = new ServerLocatorImpl(false);

      ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
      ScheduledThreadPoolExecutor scheduledThreadPool = new ScheduledThreadPoolExecutor(1);
      serverLocator.setThreadPools(threadPool, scheduledThreadPool);

      Field threadPoolField = ServerLocatorImpl.class.getDeclaredField("threadPool");
      Field scheduledThreadPoolField = ServerLocatorImpl.class.getDeclaredField("scheduledThreadPool");

      serverLocator.initialize();

      threadPoolField.setAccessible(true);
      scheduledThreadPoolField.setAccessible(true);

      ThreadPoolExecutor tpe = (ThreadPoolExecutor) threadPoolField.get(serverLocator);
      ScheduledThreadPoolExecutor stpe = (ScheduledThreadPoolExecutor) scheduledThreadPoolField.get(serverLocator);

      assertEquals(threadPool, tpe);
      assertEquals(scheduledThreadPool, stpe);
   }

   @After
   public void cleanup() {
      // Resets the global thread pool properties back to default.
      System.setProperties(systemProperties);
      ActiveMQClient.initializeGlobalThreadPoolProperties();
      ActiveMQClient.clearThreadPools();
   }

}
