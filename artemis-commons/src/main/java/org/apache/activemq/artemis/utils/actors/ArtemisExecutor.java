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

public interface ArtemisExecutor extends Executor {

   /**
    * Artemis is supposed to implement this properly, however in tests or tools
    * this can be used as a fake, doing a sipmle delegate and using the default methods implemented here.
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

   default boolean flush() {
      return flush(30, TimeUnit.SECONDS);
   }

   default boolean flush(long timeout, TimeUnit unit) {
      CountDownLatch latch = new CountDownLatch(1);
      Runnable runnable = new Runnable() {
         @Override
         public void run() {
            latch.countDown();
         }
      };
      execute(runnable);
      try {
         return latch.await(timeout, unit);
      } catch (InterruptedException e) {
         return false;
      }
   }

   /**
    * This will verify if the executor is flushed with no wait (or very minimal wait if not the {@link org.apache.activemq.artemis.utils.actors.OrderedExecutor}
    * @return
    */
   default boolean isFlushed() {
      return flush(100, TimeUnit.MILLISECONDS);
   }

}
