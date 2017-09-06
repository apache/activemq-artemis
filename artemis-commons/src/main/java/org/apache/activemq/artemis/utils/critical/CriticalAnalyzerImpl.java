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

import java.util.ConcurrentModificationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.jboss.logging.Logger;

public class CriticalAnalyzerImpl implements CriticalAnalyzer {

   private final Logger logger = Logger.getLogger(CriticalAnalyzer.class);

   private volatile long timeoutNanoSeconds;

   private volatile long checkTimeNanoSeconds;

   @Override
   public void clear() {
      actions.clear();
      components.clear();
   }

   private CopyOnWriteArrayList<CriticalAction> actions = new CopyOnWriteArrayList<>();

   private Thread thread;

   private final Semaphore running = new Semaphore(1);

   private final ConcurrentHashSet<CriticalComponent> components = new ConcurrentHashSet<>();

   @Override
   public boolean isMeasuring() {
      return true;
   }

   @Override
   public void add(CriticalComponent component) {
      components.add(component);
   }

   @Override
   public void remove(CriticalComponent component) {
      components.remove(component);
   }

   @Override
   public CriticalAnalyzer setCheckTime(long timeout, TimeUnit unit) {
      this.checkTimeNanoSeconds = timeout;
      return this;
   }

   @Override
   public long getCheckTimeNanoSeconds() {
      if (checkTimeNanoSeconds == 0) {
         checkTimeNanoSeconds = getTimeout(TimeUnit.NANOSECONDS) / 2;
      }
      return checkTimeNanoSeconds;
   }

   @Override
   public CriticalAnalyzer setTimeout(long timeout, TimeUnit unit) {
      this.timeoutNanoSeconds = unit.toNanos(timeout);
      return this;
   }

   @Override
   public long getTimeout(TimeUnit unit) {
      if (timeoutNanoSeconds == 0) {
         timeoutNanoSeconds = TimeUnit.MINUTES.toNanos(2);
      }
      return unit.convert(timeoutNanoSeconds, TimeUnit.NANOSECONDS);
   }

   @Override
   public CriticalAnalyzer addAction(CriticalAction action) {
      this.actions.add(action);
      return this;
   }

   @Override
   public void check() {
      boolean retry = true;
      while (retry) {
         try {
            for (CriticalComponent component : components) {

               if (component.isExpired(timeoutNanoSeconds)) {
                  fireAction(component);
                  // no need to keep running if there's already a component failed
                  return;
               }
            }
            retry = false; // got to the end of the list, no need to retry
         } catch (ConcurrentModificationException dontCare) {
            // lets retry on the loop
         }
      }
   }

   private void fireAction(CriticalComponent component) {
      for (CriticalAction action: actions) {
         try {
            action.run(component);
         } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
         }
      }
   }

   @Override
   public void start() {

      if (!running.tryAcquire()) {
         // already running
         return;
      }

      // we are not using any Thread Pool or any Scheduled Executors from the ArtemisServer
      // as that would defeat the purpose,
      // as in any deadlocks the schedulers may be starving for something not responding fast enough
      thread = new Thread("Artemis Critical Analyzer") {
         @Override
         public void run() {
            try {
               while (true) {
                  if (running.tryAcquire(getCheckTimeNanoSeconds(), TimeUnit.NANOSECONDS)) {
                     running.release();
                     // this means that the server has been stopped as we could acquire the semaphore... returning now
                     break;
                  }
                  check();
               }
            } catch (InterruptedException interrupted) {
               // i will just leave on that case
            }
         }
      };

      thread.setDaemon(true);

      thread.start();
   }

   @Override
   public void stop() {
      if (!isStarted()) {
         // already stopped, leaving
         return;
      }

      running.release();

      try {
         if (thread != null) {
            thread.join();
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      }
   }

   @Override
   public boolean isStarted() {
      return running.availablePermits() == 0;
   }
}