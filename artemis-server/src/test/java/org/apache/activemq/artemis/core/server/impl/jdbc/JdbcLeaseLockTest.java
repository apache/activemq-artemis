/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.impl.jdbc;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

@RunWith(Parameterized.class)
public class JdbcLeaseLockTest extends ActiveMQTestBase {

   private JdbcSharedStateManager jdbcSharedStateManager;
   private DatabaseStorageConfiguration dbConf;
   private SQLProvider sqlProvider;

   @Parameterized.Parameters(name = "create_tables_prior_test={0}")
   public static List<Object[]> data() {
      return Arrays.asList(new Object[][] {
         {true},
         {false}
      });
   }

   @Parameter(0)
   public boolean withExistingTable;


   private LeaseLock lock() {
      return lock(dbConf.getJdbcLockExpirationMillis());
   }

   private LeaseLock lock(long acquireMillis) {
      try {
         return JdbcSharedStateManager
            .createLiveLock(
               UUID.randomUUID().toString(),
               jdbcSharedStateManager.getConnection(),
               sqlProvider,
               acquireMillis);
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      }
   }

   @Before
   public void createLockTable() throws Exception {
      dbConf = createDefaultDatabaseStorageConfiguration();
      sqlProvider = JDBCUtils.getSQLProvider(
         dbConf.getJdbcDriverClassName(),
         dbConf.getNodeManagerStoreTableName(),
         SQLProvider.DatabaseStoreType.NODE_MANAGER);

      if (withExistingTable) {
         TestJDBCDriver testDriver = TestJDBCDriver
            .usingConnectionUrl(
                dbConf.getJdbcConnectionUrl(),
                dbConf.getJdbcDriverClassName(),
                sqlProvider);
         testDriver.start();
         testDriver.stop();
      }

      jdbcSharedStateManager = JdbcSharedStateManager
         .usingConnectionUrl(
            UUID.randomUUID().toString(),
            dbConf.getJdbcLockExpirationMillis(),
            dbConf.getJdbcConnectionUrl(),
            dbConf.getJdbcDriverClassName(),
            sqlProvider);
   }

   @After
   public void dropLockTable() throws Exception {
      jdbcSharedStateManager.destroy();
      jdbcSharedStateManager.close();
   }

   @Test
   public void shouldAcquireLock() {
      final LeaseLock lock = lock();
      final boolean acquired = lock.tryAcquire();
      Assert.assertTrue("Must acquire the lock!", acquired);
      try {
         Assert.assertTrue("The lock is been held by the caller!", lock.isHeldByCaller());
      } finally {
         lock.release();
      }
   }

   @Test
   public void shouldNotAcquireLockWhenAlreadyHeldByOthers() {
      final LeaseLock lock = lock();
      Assert.assertTrue("Must acquire the lock", lock.tryAcquire());
      try {
         Assert.assertTrue("Lock held by the caller", lock.isHeldByCaller());
         final LeaseLock failingLock = lock();
         Assert.assertFalse("lock already held by other", failingLock.tryAcquire());
         Assert.assertFalse("lock already held by other", failingLock.isHeldByCaller());
         Assert.assertTrue("lock already held by other", failingLock.isHeld());
      } finally {
         lock.release();
      }
   }

   @Test
   public void shouldNotAcquireLockTwice() {
      final LeaseLock lock = lock();
      Assert.assertTrue("Must acquire the lock", lock.tryAcquire());
      try {
         Assert.assertFalse("lock already acquired", lock.tryAcquire());
      } finally {
         lock.release();
      }
   }

   @Test
   public void shouldNotCorruptGuardedState() throws InterruptedException {
      final AtomicLong sharedState = new AtomicLong(0);
      final int producers = 2;
      final int writesPerProducer = 10;
      final long idleMillis = 1000;
      final long millisToAcquireLock = writesPerProducer * (producers - 1) * idleMillis;
      final LeaseLock.Pauser pauser = LeaseLock.Pauser.sleep(idleMillis, TimeUnit.MILLISECONDS);
      final CountDownLatch finished = new CountDownLatch(producers);
      final LeaseLock[] locks = new LeaseLock[producers];
      final AtomicInteger lockIndex = new AtomicInteger(0);
      final Runnable producerTask = () -> {
         final LeaseLock lock = locks[lockIndex.getAndIncrement()];
         try {
            for (int i = 0; i < writesPerProducer; i++) {
               final LeaseLock.AcquireResult acquireResult = lock.tryAcquire(millisToAcquireLock, pauser, () -> true);
               if (acquireResult != LeaseLock.AcquireResult.Done) {
                  throw new IllegalStateException(acquireResult + " from " + Thread.currentThread());
               }
               //avoid the atomic getAndIncrement operation on purpose
               sharedState.lazySet(sharedState.get() + 1);
               lock.release();
            }
         } finally {
            finished.countDown();
         }
      };
      final Thread[] producerThreads = new Thread[producers];
      for (int i = 0; i < producers; i++) {
         locks[i] = lock();
         producerThreads[i] = new Thread(producerTask);
      }
      Stream.of(producerThreads).forEach(Thread::start);
      final long maxTestTime = millisToAcquireLock * writesPerProducer * producers;
      Assert.assertTrue("Each producers must complete the writes", finished.await(maxTestTime, TimeUnit.MILLISECONDS));
      Assert.assertEquals("locks hasn't mutual excluded producers", writesPerProducer * producers, sharedState.get());
   }

   @Test
   public void shouldAcquireExpiredLock() throws InterruptedException {
      final LeaseLock lock = lock(TimeUnit.SECONDS.toMillis(1));
      Assert.assertTrue("lock is not owned by anyone", lock.tryAcquire());
      try {
         Thread.sleep(lock.expirationMillis() * 2);
         Assert.assertFalse("lock is already expired", lock.isHeldByCaller());
         Assert.assertFalse("lock is already expired", lock.isHeld());
         Assert.assertTrue("lock is already expired", lock.tryAcquire());
      } finally {
         lock.release();
      }
   }

   @Test
   public void shouldOtherAcquireExpiredLock() throws InterruptedException {
      final LeaseLock lock = lock(TimeUnit.SECONDS.toMillis(1));
      Assert.assertTrue("lock is not owned by anyone", lock.tryAcquire());
      try {
         Thread.sleep(lock.expirationMillis() * 2);
         Assert.assertFalse("lock is already expired", lock.isHeldByCaller());
         Assert.assertFalse("lock is already expired", lock.isHeld());
         final LeaseLock otherLock = lock(TimeUnit.SECONDS.toMillis(10));
         try {
            Assert.assertTrue("lock is already expired", otherLock.tryAcquire());
         } finally {
            otherLock.release();
         }
      } finally {
         lock.release();
      }
   }

   @Test
   public void shouldRenewAcquiredLock() throws InterruptedException {
      final LeaseLock lock = lock(TimeUnit.SECONDS.toMillis(10));
      Assert.assertTrue("lock is not owned by anyone", lock.tryAcquire());
      try {
         Assert.assertTrue("lock is owned", lock.renew());
      } finally {
         lock.release();
      }
   }

   @Test
   public void shouldNotRenewReleasedLock() throws InterruptedException {
      final LeaseLock lock = lock(TimeUnit.SECONDS.toMillis(10));
      Assert.assertTrue("lock is not owned by anyone", lock.tryAcquire());
      lock.release();
      Assert.assertFalse("lock is already released", lock.isHeldByCaller());
      Assert.assertFalse("lock is already released", lock.isHeld());
      Assert.assertFalse("lock is already released", lock.renew());
   }

   @Test
   public void shouldRenewExpiredLockNotAcquiredByOthers() throws InterruptedException {
      final LeaseLock lock = lock(TimeUnit.SECONDS.toMillis(1));
      Assert.assertTrue("lock is not owned by anyone", lock.tryAcquire());
      try {
         Thread.sleep(lock.expirationMillis() * 2);
         Assert.assertFalse("lock is already expired", lock.isHeldByCaller());
         Assert.assertFalse("lock is already expired", lock.isHeld());
         Assert.assertTrue("lock is owned", lock.renew());
      } finally {
         lock.release();
      }
   }

   @Test
   public void shouldNotRenewLockAcquiredByOthers() throws InterruptedException {
      final LeaseLock lock = lock(TimeUnit.SECONDS.toMillis(1));
      Assert.assertTrue("lock is not owned by anyone", lock.tryAcquire());
      try {
         Thread.sleep(lock.expirationMillis() * 2);
         Assert.assertFalse("lock is already expired", lock.isHeldByCaller());
         Assert.assertFalse("lock is already expired", lock.isHeld());
         final LeaseLock otherLock = lock(TimeUnit.SECONDS.toMillis(10));
         Assert.assertTrue("lock is already expired", otherLock.tryAcquire());
         try {
            Assert.assertFalse("lock is owned by others", lock.renew());
         } finally {
            otherLock.release();
         }
      } finally {
         lock.release();
      }
   }
}

