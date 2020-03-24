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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public interface ArtemisExecutor extends Executor {

   /**
    * Artemis is supposed to implement this properly, however in tests or tools
    * this can be used as a fake, doing a simple delegate and using the default methods implemented here.
    * @param executor
    * @return
    */
   static ArtemisExecutor delegate(Executor executor) {
      return new ArtemisExecutor() {
         @Override
         public void execute(Runnable command) {
            executor.execute(command);
         }
      };
   }

   /**
    * It will wait the current execution (if there is one) to finish
    * but will not complete any further executions.
    *
    * @param onPendingTask it will be called for each pending task found
    * @return the number of pending tasks that won't be executed
    */
   default int shutdownNow(Consumer<? super Runnable> onPendingTask, int timeout, TimeUnit unit) {
      return 0;
   }

   /** To be used to flush an executor from a different thread.
    *  WARNING: Do not call this within the executor. That would be stoopid ;)
    *
    * @param timeout
    * @param unit
    * @return
    */
   default boolean flush(long timeout, TimeUnit unit) {
      CountDownLatch latch = new CountDownLatch(1);
      execute(latch::countDown);
      try {
         return latch.await(timeout, unit);
      } catch (Exception e) {
         return false;
      }
   }

   /**
    * It will wait the current execution (if there is one) to finish
    * but will not complete any further executions
    */
   default int shutdownNow() {
      return shutdownNow(t -> {
      }, 1, TimeUnit.SECONDS);
   }


   default void shutdown() {
   }


   /**
    * This will verify if the executor is flushed with no wait (or very minimal wait if not the {@link org.apache.activemq.artemis.utils.actors.OrderedExecutor}
    * @return
    */
   default boolean isFlushed() {
      CountDownLatch latch = new CountDownLatch(1);
      Runnable runnable = new Runnable() {
         @Override
         public void run() {
            latch.countDown();
         }
      };
      execute(runnable);
      try {
         return latch.await(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
         return false;
      }
   }

}
