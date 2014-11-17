/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.utils;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Clebert Suconic
 */

public class ReferenceCounterUtil implements ReferenceCounter
{
   private final Runnable runnable;

   /** If executor is null the runnable will be called within the same thread, otherwise the executor will be used */
   private final Executor executor;

   private final AtomicInteger uses = new AtomicInteger(0);


   public ReferenceCounterUtil(Runnable runnable)
   {
      this(runnable, null);
   }

   public ReferenceCounterUtil(Runnable runnable, Executor executor)
   {
      this.runnable = runnable;
      this.executor = executor;
   }

   @Override
   public int increment()
   {
      return uses.incrementAndGet();
   }

   @Override
   public int decrement()
   {
      int value = uses.decrementAndGet();
      if (value == 0)
      {
         if (executor != null)
         {
            executor.execute(runnable);
         }
         else
         {
            runnable.run();
         }
      }

      return value;
   }
}
