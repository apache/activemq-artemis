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

import org.jboss.logging.Logger;

public class CriticalMeasure {

   private static final Logger logger = Logger.getLogger(CriticalMeasure.class);

   // this is used on enterCritical, if the logger is in trace mode
   private volatile Exception traceEnter;

   //uses updaters to avoid creates many AtomicLong instances
   static final AtomicLongFieldUpdater<CriticalMeasure> TIME_ENTER_UPDATER = AtomicLongFieldUpdater.newUpdater(CriticalMeasure.class, "timeEnter");
   static final AtomicLongFieldUpdater<CriticalMeasure> TIME_LEFT_UPDATER = AtomicLongFieldUpdater.newUpdater(CriticalMeasure.class, "timeLeft");

   private volatile long timeEnter;
   private volatile long timeLeft;

   private final int id;
   private final CriticalComponent component;

   public CriticalMeasure(CriticalComponent component, int id) {
      this.id = id;
      this.component = component;
      //prefer this approach instead of using some fixed value because System::nanoTime could change sign
      //with long running processes
      long time = System.nanoTime();
      TIME_LEFT_UPDATER.set(this, time);
      TIME_ENTER_UPDATER.set(this, time);
   }

   public void enterCritical() {
      //prefer lazySet in order to avoid heavy-weight full barriers on x86
      TIME_ENTER_UPDATER.lazySet(this, System.nanoTime());

      if (logger.isTraceEnabled()) {
         traceEnter = new Exception("entered");
      }
   }

   public void leaveCritical() {

      if (logger.isTraceEnabled()) {

         CriticalAnalyzer analyzer = component != null ? component.getCriticalAnalyzer() : null;
         if (analyzer != null) {
            long nanoTimeout = analyzer.getTimeoutNanoSeconds();
            if (checkExpiration(nanoTimeout, false)) {
               logger.trace("Path " + id + " on component " + getComponentName() + " is taking too long, leaving at", new Exception("left"));
               logger.trace("Path " + id + " on component " + getComponentName() + " is taking too long, entered at", traceEnter);
            }
         }
         traceEnter = null;
      }

      TIME_LEFT_UPDATER.lazySet(this, System.nanoTime());
   }

   protected String getComponentName() {
      if (component == null) {
         return "null";
      } else {
         return component.getClass().getName();
      }
   }

   public boolean checkExpiration(long timeout, boolean reset) {
      long time = System.nanoTime();
      final long timeLeft = TIME_LEFT_UPDATER.get(this);
      final long timeEnter = TIME_ENTER_UPDATER.get(this);
      //due to how System::nanoTime works is better to use differences to prevent numerical overflow while comparing
      if (timeLeft - timeEnter < 0) {
         boolean expired = System.nanoTime() - timeEnter > timeout;

         if (expired) {
            Exception lastTraceEnter = this.traceEnter;

            if (lastTraceEnter != null) {
               logger.warn("Component " + getComponentName() + " is expired on path " + id, lastTraceEnter);
            } else {
               logger.warn("Component " + getComponentName() + " is expired on path " + id);
            }

            if (reset) {
               TIME_LEFT_UPDATER.lazySet(this, time);
               TIME_ENTER_UPDATER.lazySet(this, time);
            }
         }
         return expired;
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