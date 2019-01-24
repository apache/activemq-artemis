/*
 * Copyright The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.persistence.impl.journal;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.junit.Assert;
import org.junit.Test;

public class JournalStorageManagerTest {

   ScheduledExecutorService dumbScheduler = new ScheduledExecutorService() {
      @Override
      public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
         return null;
      }

      @Override
      public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
         return null;
      }

      @Override
      public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
         return null;
      }

      @Override
      public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
         return null;
      }

      @Override
      public void shutdown() {

      }

      @Override
      public List<Runnable> shutdownNow() {
         return null;
      }

      @Override
      public boolean isShutdown() {
         return false;
      }

      @Override
      public boolean isTerminated() {
         return false;
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
         return false;
      }

      @Override
      public <T> Future<T> submit(Callable<T> task) {
         return null;
      }

      @Override
      public <T> Future<T> submit(Runnable task, T result) {
         return null;
      }

      @Override
      public Future<?> submit(Runnable task) {
         return null;
      }

      @Override
      public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
         return null;
      }

      @Override
      public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                           long timeout,
                                           TimeUnit unit) throws InterruptedException {
         return null;
      }

      @Override
      public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
         return null;
      }

      @Override
      public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
                             long timeout,
                             TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
         return null;
      }

      @Override
      public void execute(Runnable command) {

      }
   };

   ExecutorFactory dumbExecutor = new ExecutorFactory() {
      @Override
      public ArtemisExecutor getExecutor() {
         return new ArtemisExecutor() {
            @Override
            public void execute(Runnable command) {
               command.run();
            }
         };
      }
   };

   /**
    * Test of fixJournalFileSize method, of class JournalStorageManager.
    */
   @Test
   public void testFixJournalFileSize() {
      JournalStorageManager manager = new JournalStorageManager(new ConfigurationImpl(), null, dumbExecutor, dumbScheduler, dumbExecutor);
      Assert.assertEquals(4096, manager.fixJournalFileSize(1024, 4096));
      Assert.assertEquals(4096, manager.fixJournalFileSize(4098, 4096));
      Assert.assertEquals(8192, manager.fixJournalFileSize(8192, 4096));
   }
}
