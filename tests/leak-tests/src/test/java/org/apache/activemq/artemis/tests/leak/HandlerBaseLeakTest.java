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

package org.apache.activemq.artemis.tests.leak;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.github.checkleak.core.CheckLeak;
import org.apache.activemq.artemis.utils.actors.OrderedExecutor;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandlerBaseLeakTest extends AbstractLeakTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   public void testHandlerLeak() throws Throwable {
      ExecutorService service = Executors.newFixedThreadPool(1);
      runAfter(service::shutdownNow);
      executeFactory(service);

      CheckLeak checkLeak = new CheckLeak();

      MemoryAssertions.assertNoInnerInstances(OrderedExecutor.class, checkLeak);
   }

   // this needs to be a sub-method, to make sure the VM will collect and discard certain objects, Otherwise HandlerBase would still show in the heap
   private static void executeFactory(ExecutorService service) throws InterruptedException {
      OrderedExecutorFactory factory = new OrderedExecutorFactory(service);
      CountDownLatch latch = new CountDownLatch(1);
      Executor executor = factory.getExecutor();
      executor.execute(latch::countDown);
      assertTrue(latch.await(1, TimeUnit.MINUTES));
   }

}
