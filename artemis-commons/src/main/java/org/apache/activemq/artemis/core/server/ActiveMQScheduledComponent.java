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

package org.apache.activemq.artemis.core.server;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.jboss.logging.Logger;

/**
 * This is for components with a scheduled at a fixed rate.
 */
public abstract class ActiveMQScheduledComponent implements ActiveMQComponent, Runnable {

   private static final Logger logger = Logger.getLogger(ActiveMQScheduledComponent.class);
   private ScheduledExecutorService scheduledExecutorService;
   private boolean startedOwnScheduler;
   private long period;
   private long millisecondsPeriod;
   private TimeUnit timeUnit;
   private final Executor executor;
   private ScheduledFuture future;
   private final boolean onDemand;

   long lastTime = 0;

   private final AtomicInteger delayed = new AtomicInteger(0);

   public ActiveMQScheduledComponent(ScheduledExecutorService scheduledExecutorService,
                                     Executor executor,
                                     long checkPeriod,
                                     TimeUnit timeUnit,
                                     boolean onDemand) {
      this.executor = executor;
      this.scheduledExecutorService = scheduledExecutorService;
      this.period = checkPeriod;
      this.timeUnit = timeUnit;
      this.onDemand = onDemand;
   }

   /**
    * This is useful for cases where we want our own scheduler executor.
    *
    * @param checkPeriod
    * @param timeUnit
    * @param onDemand
    */
   public ActiveMQScheduledComponent(long checkPeriod, TimeUnit timeUnit, boolean onDemand) {
      this(null, null, checkPeriod, timeUnit, onDemand);
   }

   @Override
   public synchronized void start() {
      if (future != null) {
         // already started
         return;
      }

      if (scheduledExecutorService == null) {
         scheduledExecutorService = new ScheduledThreadPoolExecutor(1, getThreadFactory());
         startedOwnScheduler = true;

      }

      if (onDemand) {
         return;
      }

      this.millisecondsPeriod = timeUnit.convert(period, TimeUnit.MILLISECONDS);

      if (period >= 0) {
         future = scheduledExecutorService.scheduleWithFixedDelay(runForScheduler, period, period, timeUnit);
      } else {
         logger.tracef("did not start scheduled executor on %s because period was configured as %d", this, period);
      }
   }

   protected ActiveMQThreadFactory getThreadFactory() {
      return new ActiveMQThreadFactory(this.getClass().getSimpleName() + "-scheduled-threads", false, getThisClassLoader());
   }

   private ClassLoader getThisClassLoader() {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
         @Override
         public ClassLoader run() {
            return ActiveMQScheduledComponent.this.getClass().getClassLoader();
         }
      });

   }

   public void delay() {
      int value = delayed.incrementAndGet();
      if (value > 10) {
         delayed.decrementAndGet();
      } else {
         // We only schedule up to 10 periods upfront.
         // this is to avoid a window where a current one would be running and a next one is coming.
         // in theory just 2 would be enough. I'm using 10 as a precaution here.
         scheduledExecutorService.schedule(runForScheduler, Math.min(period, period * value), timeUnit);
      }
   }

   public long getPeriod() {
      return period;
   }

   public synchronized ActiveMQScheduledComponent setPeriod(long period) {
      this.period = period;
      restartIfNeeded();
      return this;
   }

   public TimeUnit getTimeUnit() {
      return timeUnit;
   }

   public synchronized ActiveMQScheduledComponent setTimeUnit(TimeUnit timeUnit) {
      this.timeUnit = timeUnit;
      restartIfNeeded();
      return this;
   }

   @Override
   public synchronized void stop() {
      if (future != null) {
         future.cancel(false);
         future = null;
      }
      if (startedOwnScheduler) {
         this.scheduledExecutorService.shutdownNow();
         scheduledExecutorService = null;
         startedOwnScheduler = false;
      }

   }

   @Override
   public synchronized boolean isStarted() {
      return future != null;
   }

   // this will restart the schedulped component upon changes
   private void restartIfNeeded() {
      if (isStarted()) {
         stop();
         start();
      }
   }

   final Runnable runForExecutor = new Runnable() {
      @Override
      public void run() {
         if (onDemand && delayed.get() > 0) {
            delayed.decrementAndGet();
         }

         if (!onDemand && lastTime > 0) {
            if (System.currentTimeMillis() - lastTime < millisecondsPeriod) {
               logger.trace("Execution ignored due to too many simultaneous executions, probably a previous delayed execution");
               return;
            }
         }

         lastTime = System.currentTimeMillis();

         ActiveMQScheduledComponent.this.run();
      }
   };

   final Runnable runForScheduler = new Runnable() {
      @Override
      public void run() {
         if (executor != null) {
            executor.execute(runForExecutor);
         } else {
            runForExecutor.run();
         }
      }
   };

}
