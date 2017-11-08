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

package org.apache.activemq.artemis.utils.actors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class OrderedExecutorSanityTest {

   @Test
   public void shouldExecuteTasksInOrder() throws InterruptedException {
      final int threads = 3;
      final int tasks = 100;
      final long timeoutMillis = TimeUnit.SECONDS.toMillis(10);
      final ExecutorService executorService = Executors.newFixedThreadPool(threads);
      try {
         final ArtemisExecutor executor = new OrderedExecutor(executorService);
         //it can be not thread safe too
         final List<Integer> results = new ArrayList<>(tasks);
         final List<Integer> expectedResults = new ArrayList<>(tasks);
         final CountDownLatch executed = new CountDownLatch(tasks);
         for (int i = 0; i < tasks; i++) {
            final int value = i;
            executor.execute(() -> {
               results.add(value);
               executed.countDown();
            });
            expectedResults.add(value);
         }
         Assert.assertTrue("The tasks must be executed in " + timeoutMillis + " ms", executed.await(timeoutMillis, TimeUnit.MILLISECONDS));
         Assert.assertArrayEquals("The processing of tasks must be ordered", expectedResults.toArray(), results.toArray());
      } finally {
         executorService.shutdown();
      }
   }

   @Test
   public void shouldShutdownNowDoNotExecuteFurtherTasks() throws InterruptedException {
      final long timeoutMillis = TimeUnit.SECONDS.toMillis(10);
      final ExecutorService executorService = Executors.newSingleThreadExecutor();
      try {
         final OrderedExecutor executor = new OrderedExecutor(executorService);
         final CountDownLatch executed = new CountDownLatch(1);
         executor.execute(executed::countDown);
         Assert.assertTrue("The task must be executed in " + timeoutMillis + " ms", executed.await(timeoutMillis, TimeUnit.MILLISECONDS));
         executor.shutdownNow();
         Assert.assertEquals("There are no remaining tasks to be executed", 0, executor.remaining());
         //from now on new tasks won't be executed
         final CountDownLatch afterDeatchExecution = new CountDownLatch(1);
         executor.execute(afterDeatchExecution::countDown);
         Assert.assertFalse("After shutdownNow no new tasks can be executed", afterDeatchExecution.await(1, TimeUnit.SECONDS));
         //to avoid memory leaks the executor must take care of the new submitted tasks immediatly
         Assert.assertEquals("Any new task submitted after death must be collected", 0, executor.remaining());
      } finally {
         executorService.shutdown();
      }
   }

}
