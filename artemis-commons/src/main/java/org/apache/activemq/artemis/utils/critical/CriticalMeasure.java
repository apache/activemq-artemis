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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.jboss.logging.Logger;

public class CriticalMeasure {

   private static final Logger logger = Logger.getLogger(CriticalMeasure.class);

   // this is used on enterCritical, if the logger is in trace mode
   private volatile Exception traceEnter;

   //uses updaters to avoid creates many AtomicLong instances
   static final AtomicLongFieldUpdater<CriticalMeasure> TIME_ENTER_UPDATER = AtomicLongFieldUpdater.newUpdater(CriticalMeasure.class, "timeEnter");

   static final AtomicReferenceFieldUpdater<CriticalMeasure, Thread> CURRENT_THREAD_UDPATER = AtomicReferenceFieldUpdater.newUpdater(CriticalMeasure.class, Thread.class, "currentThread");

   // While resetting the leaveMethod, I want to make sure no enter call would reset the value.
   // so I set the Current Thread to this Ghost Thread, to then set it back to null
   private static final Thread GHOST_THREAD = new Thread();

   private volatile Thread currentThread;
   private volatile long timeEnter;

   private final int id;
   private final CriticalComponent component;

   public CriticalMeasure(CriticalComponent component, int id) {
      this.id = id;
      this.component = component;
      TIME_ENTER_UPDATER.set(this, 0);
   }

   public void enterCritical() {

      // a sampling of a single thread at a time will be sufficient for the analyser,
      // typically what causes one thread to stall will repeat on another
      if (CURRENT_THREAD_UDPATER.compareAndSet(this, null, Thread.currentThread())) {
         //prefer lazySet in order to avoid heavy-weight full barriers on x86
         TIME_ENTER_UPDATER.lazySet(this, System.nanoTime());

         if (logger.isTraceEnabled()) {
            traceEnter = new Exception("entered");
         }
      }
   }

   public void leaveCritical() {

      if (CURRENT_THREAD_UDPATER.compareAndSet(this, Thread.currentThread(), GHOST_THREAD)) {
         // NULL_THREAD here represents a state where I would be ignoring any call to enterCritical or leaveCritical, while I reset the Time Enter Update
         // This is to avoid replacing time Enter by a new Value, right after current Thread is set to Null.
         // So we set to this ghost value while we are setting

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
         TIME_ENTER_UPDATER.set(this, 0);

         // I am pretty sure this is single threaded by now.. I don't need compareAndSet here
         CURRENT_THREAD_UDPATER.set(this, null);
      }
   }

   protected String getComponentName() {
      if (component == null) {
         return "null";
      } else {
         return component.getClass().getName();
      }
   }

   public boolean checkExpiration(long timeout, boolean reset) {
      final long timeEnter = TIME_ENTER_UPDATER.get(this);
      if (timeEnter != 0L) {
         long time = System.nanoTime();
         boolean expired = time - timeEnter > timeout;

         if (expired) {
            Exception lastTraceEnter = this.traceEnter;

            if (lastTraceEnter != null) {
               logger.warn("Component " + getComponentName() + " is expired on path " + id, lastTraceEnter);
            } else {
               logger.warn("Component " + getComponentName() + " is expired on path " + id);
            }

         }
         return expired;
      }
      return false;
   }

   public long enterTime() {
      return TIME_ENTER_UPDATER.get(this);
   }
}