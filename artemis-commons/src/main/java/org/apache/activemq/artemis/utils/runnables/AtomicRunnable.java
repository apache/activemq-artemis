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

package org.apache.activemq.artemis.utils.runnables;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public abstract class AtomicRunnable implements Runnable {


   public static Runnable checkAtomic(Runnable run) {
      if (run instanceof AtomicRunnable) {
         return run;
      } else {
         return new AtomicRunnableWithDelegate(run);
      }
   }

   private volatile int ran;

   private static final AtomicIntegerFieldUpdater<AtomicRunnable> RAN_UPDATE =
      AtomicIntegerFieldUpdater.newUpdater(AtomicRunnable.class, "ran");

   @Override
   public void run() {
      if (RAN_UPDATE.compareAndSet(this, 0, 1)) {
         atomicRun();
      }
   }

   public abstract void atomicRun();
}

