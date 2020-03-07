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

package org.apache.activemq.artemis.core.server.impl.jdbc;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.jboss.logging.Logger;

/**
 * Default implementation of a {@link ScheduledLeaseLock}: see {@link ScheduledLeaseLock#of(ScheduledExecutorService, ArtemisExecutor, String, LeaseLock, long, IOCriticalErrorListener)}.
 */
final class ActiveMQScheduledLeaseLock extends ActiveMQScheduledComponent implements ScheduledLeaseLock {

   private static final Logger LOGGER = Logger.getLogger(ActiveMQScheduledLeaseLock.class);

   private final String lockName;
   private final LeaseLock lock;
   private long lastLockRenewStart;
   private final long renewPeriodMillis;
   private final IOCriticalErrorListener ioCriticalErrorListener;

   ActiveMQScheduledLeaseLock(ScheduledExecutorService scheduledExecutorService,
                              ArtemisExecutor executor,
                              String lockName,
                              LeaseLock lock,
                              long renewPeriodMillis,
                              IOCriticalErrorListener ioCriticalErrorListener) {
      super(scheduledExecutorService, executor, 0, renewPeriodMillis, TimeUnit.MILLISECONDS, false);
      if (renewPeriodMillis >= lock.expirationMillis()) {
         throw new IllegalArgumentException("renewPeriodMillis must be < lock's expirationMillis");
      }
      this.lockName = lockName;
      this.lock = lock;
      this.renewPeriodMillis = renewPeriodMillis;
      //already expired start time
      this.lastLockRenewStart = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(lock.expirationMillis());
      this.ioCriticalErrorListener = ioCriticalErrorListener;
   }

   @Override
   public long renewPeriodMillis() {
      return renewPeriodMillis;
   }

   @Override
   public LeaseLock lock() {
      return lock;
   }

   @Override
   public synchronized void start() {
      if (isStarted()) {
         return;
      }
      this.lastLockRenewStart = System.nanoTime();
      super.start();
   }

   @Override
   public synchronized void stop() {
      if (!isStarted()) {
         return;
      }
      super.stop();
   }

   @Override
   public void run() {
      final long lastRenewStart = this.lastLockRenewStart;
      final long renewStart = System.nanoTime();
      try {
         if (!this.lock.renew()) {
            ioCriticalErrorListener.onIOException(new IllegalStateException(lockName + " lock can't be renewed"), "Critical error while on " + lockName + " renew", null);
         }
      } catch (Throwable t) {
         ioCriticalErrorListener.onIOException(t, "Critical error while on " + lockName + " renew", null);
         throw t;
      }
      //logic to detect slowness of DB and/or the scheduled executor service
      detectAndReportRenewSlowness(lockName, lastRenewStart, renewStart, renewPeriodMillis, lock.expirationMillis());
      this.lastLockRenewStart = renewStart;
   }

   private static void detectAndReportRenewSlowness(String lockName,
                                                    long lastRenewStart,
                                                    long renewStart,
                                                    long expectedRenewPeriodMillis,
                                                    long expirationMillis) {
      final long elapsedMillisToRenew = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - renewStart);
      if (elapsedMillisToRenew > expectedRenewPeriodMillis) {
         LOGGER.error(lockName + " lock renew tooks " + elapsedMillisToRenew + " ms, while is supposed to take <" + expectedRenewPeriodMillis + " ms");
      }
      final long measuredRenewPeriodNanos = renewStart - lastRenewStart;
      final long measuredRenewPeriodMillis = TimeUnit.NANOSECONDS.toMillis(measuredRenewPeriodNanos);
      if (measuredRenewPeriodMillis - expirationMillis > 100) {
         LOGGER.error(lockName + " lock renew period lasted " + measuredRenewPeriodMillis + " ms instead of " + expectedRenewPeriodMillis + " ms");
      } else if (measuredRenewPeriodMillis - expectedRenewPeriodMillis > 100) {
         LOGGER.warn(lockName + " lock renew period lasted " + measuredRenewPeriodMillis + " ms instead of " + expectedRenewPeriodMillis + " ms");
      }
   }
}
