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

   /** initialDelay < 0 would mean no initial delay, use the period instead */
   private long initialDelay;
   private long period;
   private long millisecondsPeriod;
   private TimeUnit timeUnit;
   private final Executor executor;
   private volatile ScheduledFuture future;
   private final boolean onDemand;

   long lastTime = 0;

   private final AtomicInteger delayed = new AtomicInteger(0);

   /**
    * It creates a scheduled component that can trigger {@link #run()} with a fixed {@code checkPeriod} on a configured {@code executor}.
    *
    * @param scheduledExecutorService the {@link ScheduledExecutorService} that periodically trigger {@link #run()} on the configured {@code executor}
    * @param executor                 the {@link Executor} that execute {@link #run()} when triggered
    * @param initialDelay             the time to delay first execution
    * @param checkPeriod              the delay between the termination of one execution and the start of the next
    * @param timeUnit                 the time unit of the {@code initialDelay} and {@code checkPeriod} parameters
    * @param onDemand                 if {@code true} the task won't be scheduled on {@link #start()}, {@code false} otherwise
    */
   public ActiveMQScheduledComponent(ScheduledExecutorService scheduledExecutorService,
                                     Executor executor,
                                     long initialDelay,
                                     long checkPeriod,
                                     TimeUnit timeUnit,
                                     boolean onDemand) {
      this.executor = executor;
      this.scheduledExecutorService = scheduledExecutorService;
      this.initialDelay = initialDelay;
      this.period = checkPeriod;
      this.timeUnit = timeUnit;
      this.onDemand = onDemand;
   }

   /**
    * It creates a scheduled component that can trigger {@link #run()} with a fixed {@code checkPeriod} on a configured {@code executor}.
    *
    * <p>
    * The component created will have {@code initialDelay} defaulted to {@code checkPeriod}.
    *
    * @param scheduledExecutorService the {@link ScheduledExecutorService} that periodically trigger {@link #run()} on the configured {@code executor}
    * @param executor                 the {@link Executor} that execute {@link #run()} when triggered
    * @param checkPeriod              the delay between the termination of one execution and the start of the next
    * @param timeUnit                 the time unit of the {@code initialDelay} and {@code checkPeriod} parameters
    * @param onDemand                 if {@code true} the task won't be scheduled on {@link #start()}, {@code false} otherwise
    */
   public ActiveMQScheduledComponent(ScheduledExecutorService scheduledExecutorService,
                                     Executor executor,
                                     long checkPeriod,
                                     TimeUnit timeUnit,
                                     boolean onDemand) {
      this(scheduledExecutorService, executor, -1, checkPeriod, timeUnit, onDemand);
   }

   /**
    * It creates a scheduled component that can trigger {@link #run()} with a fixed {@code checkPeriod} on a configured {@code executor}.
    *
    * <p>
    * This is useful for cases where we want our own scheduler executor: on {@link #start()} it will create a fresh new single-threaded {@link ScheduledExecutorService}
    * using {@link #getThreadFactory()} and {@link #getThisClassLoader()}, while on {@link #stop()} it will garbage it.
    *
    * @param initialDelay the time to delay first execution
    * @param checkPeriod  the delay between the termination of one execution and the start of the next
    * @param timeUnit     the time unit of the {@code initialDelay} and {@code checkPeriod} parameters
    * @param onDemand     if {@code true} the task won't be scheduled on {@link #start()}, {@code false} otherwise
    */
   public ActiveMQScheduledComponent(long initialDelay, long checkPeriod, TimeUnit timeUnit, boolean onDemand) {
      this(null, null, initialDelay, checkPeriod, timeUnit, onDemand);
   }

   /**
    * It creates a scheduled component that can trigger {@link #run()} with a fixed {@code checkPeriod} on a configured {@code executor}.
    *
    * <p>
    * This is useful for cases where we want our own scheduler executor.
    * The component created will have {@code initialDelay} defaulted to {@code checkPeriod}.
    *
    * @param checkPeriod the delay between the termination of one execution and the start of the next
    * @param timeUnit    the time unit of the {@code initialDelay} and {@code checkPeriod} parameters
    * @param onDemand    if {@code true} the task won't be scheduled on {@link #start()}, {@code false} otherwise
    */
   public ActiveMQScheduledComponent(long checkPeriod, TimeUnit timeUnit, boolean onDemand) {
      this(null, null, checkPeriod, checkPeriod, timeUnit, onDemand);
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
         future = scheduledExecutorService.scheduleWithFixedDelay(runForScheduler, initialDelay >= 0 ? initialDelay : period, period, timeUnit);
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

   public synchronized ActiveMQScheduledComponent setPeriod(long period, TimeUnit unit) {
      this.period = period;
      this.timeUnit = unit;
      restartIfNeeded();
      return this;
   }

   public long getInitialDelay() {
      return initialDelay;
   }

   public synchronized ActiveMQScheduledComponent setInitialDelay(long initialDelay) {
      this.initialDelay = initialDelay;
      restartIfNeeded();
      return this;
   }

   /**
    * Useful to change a running schedule and avoid multiple restarts.
    */
   public synchronized ActiveMQScheduledComponent setInitialDelayAndPeriod(long initialDelay, long period) {
      this.period = period;
      this.initialDelay = initialDelay;
      restartIfNeeded();
      return this;
   }

   /**
    * Useful to change a running schedule and avoid multiple restarts.
    */
   public synchronized ActiveMQScheduledComponent setInitialDelayAndPeriod(long initialDelay,
                                                                           long period,
                                                                           TimeUnit timeUnit) {
      this.period = period;
      this.initialDelay = initialDelay;
      this.timeUnit = timeUnit;
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
   public void stop() {
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

   // this will restart the scheduled component upon changes
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
