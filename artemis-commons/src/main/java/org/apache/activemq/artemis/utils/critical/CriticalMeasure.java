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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class CriticalMeasure {

   //uses updaters to avoid creates many AtomicLong instances
   private static final AtomicLongFieldUpdater<CriticalMeasure> TIME_ENTER_UPDATER = AtomicLongFieldUpdater.newUpdater(CriticalMeasure.class, "timeEnter");
   private static final AtomicLongFieldUpdater<CriticalMeasure> TIME_LEFT_UPDATER = AtomicLongFieldUpdater.newUpdater(CriticalMeasure.class, "timeLeft");

   private volatile long timeEnter;
   private volatile long timeLeft;

   public CriticalMeasure() {
      //prefer this approach instead of using some fixed value because System::nanoTime could change sign
      //with long running processes
      enterCritical();
      leaveCritical();
   }

   public void enterCritical() {
      //prefer lazySet in order to avoid heavy-weight full barriers on x86
      TIME_ENTER_UPDATER.lazySet(this, System.nanoTime());
   }

   public void leaveCritical() {
      TIME_LEFT_UPDATER.lazySet(this, System.nanoTime());
   }

   public boolean isExpired(long timeout) {
      final long timeLeft = TIME_LEFT_UPDATER.get(this);
      final long timeEnter = TIME_ENTER_UPDATER.get(this);
      //due to how System::nanoTime works is better to use differences to prevent numerical overflow while comparing
      if (timeLeft - timeEnter < 0) {
         return System.nanoTime() - timeEnter > timeout;
      }
      return false;
   }

   public long enterTime() {
      return TIME_ENTER_UPDATER.get(this);
   }

   public long leaveTime() {
      return TIME_LEFT_UPDATER.get(this);
   }
}