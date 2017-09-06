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

package org.apache.activemq.artemis.utils.critical;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

final class CriticalMeasure {

   //uses updaters to avoid creates many AtomicLong instances
   private static final AtomicLongFieldUpdater<CriticalMeasure> TIME_ENTER_UPDATER = AtomicLongFieldUpdater.newUpdater(CriticalMeasure.class, "timeEnter");
   private static final AtomicLongFieldUpdater<CriticalMeasure> TIME_LEFT_UPDATER = AtomicLongFieldUpdater.newUpdater(CriticalMeasure.class, "timeLeft");

   //System::nanoTime can't reach this value so it's the best candidate to have a NULL semantic
   private static final long NIL = Long.MAX_VALUE;
   private volatile long timeEnter = NIL;
   private volatile long timeLeft = NIL;

   public void enterCritical() {
      //prefer lazySet in order to avoid heavy-weight full barriers
      TIME_ENTER_UPDATER.lazySet(this, System.nanoTime());
   }

   public void leaveCritical() {
      final long now = System.nanoTime();
      assert TIME_ENTER_UPDATER.get(this) <= now : "Must call enterCritical first";
      TIME_LEFT_UPDATER.lazySet(this, now);
   }

   public boolean isExpired(long timeout) {
      final long timeLeft = TIME_LEFT_UPDATER.get(this);
      final long timeEnter = TIME_ENTER_UPDATER.get(this);
      //timeLeft has never been called or is before the current timeEnter
      if (timeEnter != NIL && ((timeLeft != NIL && timeEnter > timeLeft) || timeLeft == NIL)) {
         final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeout);
         return System.nanoTime() - timeEnter > timeoutNanos;
      }
      return false;
   }

   public long enterTime() {
      return timeEnter;
   }

   public long leaveTime() {
      return timeLeft;
   }
}