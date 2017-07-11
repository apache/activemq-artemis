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

package org.apache.activemq.artemis.utils;

import java.util.concurrent.Executor;

public interface BoundedExecutor extends Executor {

   int pendingTasks();

   int capacity();

   default int remainingCapacity() {
      return capacity() - pendingTasks();
   }

   default boolean isEmpty() {
      return pendingTasks() == 0;
   }

   /**
    * Executes the given command at some time in the future.  The command
    * may execute in a new thread, in a pooled thread, or in the calling
    * thread, at the discretion of the {@code Executor} implementation.
    *
    * @param command the runnable task
    * @return true if the command is submitted to the executor, false otherwise
    * @throws NullPointerException if command is null
    */
   boolean tryExecute(Runnable command);
}
