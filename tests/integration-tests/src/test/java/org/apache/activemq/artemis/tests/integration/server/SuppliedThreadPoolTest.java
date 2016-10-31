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
package org.apache.activemq.artemis.tests.integration.server;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServiceRegistry;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ServiceRegistryImpl;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SuppliedThreadPoolTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private ServiceRegistry serviceRegistry;

   @Before
   public void setup() throws Exception {
      serviceRegistry = new ServiceRegistryImpl();
      serviceRegistry.setExecutorService(Executors.newFixedThreadPool(1, ActiveMQThreadFactory.defaultThreadFactory()));
      serviceRegistry.setIOExecutorService(Executors.newFixedThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory()));
      serviceRegistry.setScheduledExecutorService(Executors.newScheduledThreadPool(1, ActiveMQThreadFactory.defaultThreadFactory()));
      server = new ActiveMQServerImpl(null, null, null, null, serviceRegistry);
      server.start();
      server.waitForActivation(100, TimeUnit.MILLISECONDS);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (server.isActive()) {
         server.stop();
      }
      serviceRegistry.getExecutorService().shutdown();
      serviceRegistry.getScheduledExecutorService().shutdown();
      serviceRegistry.getIOExecutorService().shutdown();
      super.tearDown();
   }

   @Test
   public void testSuppliedThreadPoolsAreCorrectlySet() throws Exception {
      assertEquals(serviceRegistry.getScheduledExecutorService(), server.getScheduledPool());

      // To check the Executor is what we expect we must reflectively inspect the OrderedExecutorFactory.
      Field field = server.getExecutorFactory().getClass().getDeclaredField("parent");
      field.setAccessible(true);
      assertEquals(serviceRegistry.getExecutorService(), field.get(server.getExecutorFactory()));
   }

   @Test
   public void testServerDoesNotShutdownSuppliedThreadPoolsOnStop() throws Exception {
      server.stop();

      ScheduledExecutorService scheduledExecutorService = server.getScheduledPool();

      Field field = server.getExecutorFactory().getClass().getDeclaredField("parent");
      field.setAccessible(true);
      ExecutorService threadPool = (ExecutorService) field.get(server.getExecutorFactory());

      // Ensure that references to the supplied Thread Pools still exist after shutdown.
      assertNotNull(threadPool);
      assertNotNull(scheduledExecutorService);

      // Ensure that ActiveMQ Artemis does not shutdown supplied thread pools.
      assertFalse(threadPool.isShutdown());
      assertFalse(scheduledExecutorService.isShutdown());
   }

   @Test
   public void testCanRestartWithSuppliedThreadPool() throws Exception {
      server.stop();
      server.start();
      server.waitForActivation(100, TimeUnit.MILLISECONDS);
      testSuppliedThreadPoolsAreCorrectlySet();
   }

   @Test
   public void testJobsGetScheduledToSuppliedThreadPool() throws Exception {
      server.stop();

      ScheduledThreadPoolExecutor scheduledExecutorService = (ScheduledThreadPoolExecutor) server.getScheduledPool();

      Field field = server.getExecutorFactory().getClass().getDeclaredField("parent");
      field.setAccessible(true);
      ThreadPoolExecutor threadPool = (ThreadPoolExecutor) field.get(server.getExecutorFactory());

      // Check jobs are getting scheduled and executed.
      assertTrue(threadPool.getCompletedTaskCount() > 0);
      assertTrue(scheduledExecutorService.getTaskCount() > 0);
   }
}
