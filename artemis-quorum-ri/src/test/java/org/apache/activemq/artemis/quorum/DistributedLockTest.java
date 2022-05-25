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
package org.apache.activemq.artemis.quorum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public abstract class DistributedLockTest {

   private final ArrayList<AutoCloseable> closeables = new ArrayList<>();

   @Before
   public void setupEnv() throws Throwable {
   }

   protected abstract void configureManager(Map<String, String> config);

   protected abstract String managerClassName();

   @After
   public void tearDownEnv() throws Throwable {
      closeables.forEach(closeables -> {
         try {
            closeables.close();
         } catch (Throwable t) {
            // silent here
         }
      });
   }

   protected DistributedPrimitiveManager createManagedDistributeManager() {
      return createManagedDistributeManager(stringStringMap -> {
      });
   }

   protected DistributedPrimitiveManager createManagedDistributeManager(Consumer<? super Map<String, String>> defaultConfiguration) {
      try {
         final HashMap<String, String> config = new HashMap<>();
         configureManager(config);
         defaultConfiguration.accept(config);
         final DistributedPrimitiveManager manager = DistributedPrimitiveManager.newInstanceOf(managerClassName(), config);
         closeables.add(manager);
         return manager;
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Test
   public void managerReturnsSameLockIfNotClosed() throws ExecutionException, InterruptedException, TimeoutException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      Assert.assertSame(manager.getDistributedLock("managerReturnsSameLockIfNotClosed"), manager.getDistributedLock("managerReturnsSameLockIfNotClosed"));
   }

   @Test
   public void managerReturnsDifferentLocksIfClosed() throws ExecutionException, InterruptedException, TimeoutException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock closedLock = manager.getDistributedLock("managerReturnsDifferentLocksIfClosed");
      closedLock.close();
      Assert.assertNotSame(closedLock, manager.getDistributedLock("managerReturnsDifferentLocksIfClosed"));
   }

   @Test
   public void managerReturnsDifferentLocksOnRestart() throws ExecutionException, InterruptedException, TimeoutException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock closedLock = manager.getDistributedLock("managerReturnsDifferentLocksOnRestart");
      manager.stop();
      manager.start();
      Assert.assertNotSame(closedLock, manager.getDistributedLock("managerReturnsDifferentLocksOnRestart"));
   }

   @Test(expected = IllegalStateException.class)
   public void managerCannotGetLockIfNotStarted() throws ExecutionException, InterruptedException, TimeoutException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.getDistributedLock("managerCannotGetLockIfNotStarted");
   }

   @Test(expected = NullPointerException.class)
   public void managerCannotGetLockWithNullLockId() throws ExecutionException, InterruptedException, TimeoutException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      manager.getDistributedLock(null);
   }

   @Test
   public void closingLockUnlockIt() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock closedLock = manager.getDistributedLock("closingLockUnlockIt");
      Assert.assertTrue(closedLock.tryLock());
      closedLock.close();
      Assert.assertTrue(manager.getDistributedLock("closingLockUnlockIt").tryLock());
   }

   @Test
   public void managerStopUnlockLocks() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      Assert.assertTrue(manager.getDistributedLock("managerStopUnlockLocks_a").tryLock());
      Assert.assertTrue(manager.getDistributedLock("managerStopUnlockLocks_b").tryLock());
      manager.stop();
      manager.start();
      //Wait.waitFor(() -> unavailableLock.get() == 1, Duration.ofSeconds(30L).toMillis());
      Assert.assertFalse(manager.getDistributedLock("managerStopUnlockLocks_a").isHeldByCaller());
      Assert.assertFalse(manager.getDistributedLock("managerStopUnlockLocks_b").isHeldByCaller());
   }

   @Test
   public void acquireAndReleaseLock() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("acquireAndReleaseLock");
      Assert.assertFalse(lock.isHeldByCaller());
      Assert.assertTrue(lock.tryLock());
      Assert.assertTrue(lock.isHeldByCaller());
      lock.unlock();
      Assert.assertFalse(lock.isHeldByCaller());
   }

   @Test(expected = IllegalStateException.class)
   public void cannotAcquireSameLockTwice() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("cannotAcquireSameLockTwice");
      Assert.assertTrue(lock.tryLock());
      lock.tryLock();
   }

   @Test
   public void heldLockIsVisibleByDifferentManagers() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager observerManager = createManagedDistributeManager();
      ownerManager.start();
      observerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("heldLockIsVisibleByDifferentManagers").tryLock());
      Assert.assertTrue(ownerManager.getDistributedLock("heldLockIsVisibleByDifferentManagers").isHeldByCaller());
      Assert.assertFalse(observerManager.getDistributedLock("heldLockIsVisibleByDifferentManagers").isHeldByCaller());
   }

   @Test
   public void unlockedLockIsVisibleByDifferentManagers() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager observerManager = createManagedDistributeManager();
      ownerManager.start();
      observerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("unlockedLockIsVisibleByDifferentManagers").tryLock());
      ownerManager.getDistributedLock("unlockedLockIsVisibleByDifferentManagers").unlock();
      Assert.assertFalse(observerManager.getDistributedLock("unlockedLockIsVisibleByDifferentManagers").isHeldByCaller());
      Assert.assertFalse(ownerManager.getDistributedLock("unlockedLockIsVisibleByDifferentManagers").isHeldByCaller());
      Assert.assertTrue(observerManager.getDistributedLock("unlockedLockIsVisibleByDifferentManagers").tryLock());
   }

   @Test
   public void cannotAcquireSameLockFromDifferentManagers() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager notOwnerManager = createManagedDistributeManager();
      ownerManager.start();
      notOwnerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("cannotAcquireSameLockFromDifferentManagers").tryLock());
      Assert.assertFalse(notOwnerManager.getDistributedLock("cannotAcquireSameLockFromDifferentManagers").tryLock());
   }

   @Test
   public void cannotUnlockFromNotOwnerManager() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager notOwnerManager = createManagedDistributeManager();
      ownerManager.start();
      notOwnerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("cannotUnlockFromNotOwnerManager").tryLock());
      notOwnerManager.getDistributedLock("cannotUnlockFromNotOwnerManager").unlock();
      Assert.assertFalse(notOwnerManager.getDistributedLock("cannotUnlockFromNotOwnerManager").isHeldByCaller());
      Assert.assertTrue(ownerManager.getDistributedLock("cannotUnlockFromNotOwnerManager").isHeldByCaller());
   }

   @Test
   public void timedTryLockSucceedWithShortTimeout() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock backgroundLock = manager.getDistributedLock("timedTryLockSucceedWithShortTimeout");
      Assert.assertTrue(backgroundLock.tryLock(1, TimeUnit.NANOSECONDS));
   }

   @Test
   public void timedTryLockFailAfterTimeout() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedPrimitiveManager otherManager = createManagedDistributeManager();
      otherManager.start();
      Assert.assertTrue(otherManager.getDistributedLock("timedTryLockFailAfterTimeout").tryLock());
      final long start = System.nanoTime();
      final long timeoutSec = 1;
      Assert.assertFalse(manager.getDistributedLock("timedTryLockFailAfterTimeout").tryLock(timeoutSec, TimeUnit.SECONDS));
      final long elapsed = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
      assertThat(elapsed, greaterThanOrEqualTo(timeoutSec));
   }

   @Test
   public void timedTryLockSuccess() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedPrimitiveManager otherManager = createManagedDistributeManager();
      otherManager.start();
      Assert.assertTrue(otherManager.getDistributedLock("timedTryLockSuccess").tryLock());
      DistributedLock backgroundLock = manager.getDistributedLock("timedTryLockSuccess");
      CompletableFuture<Boolean> acquired = new CompletableFuture<>();
      CountDownLatch startedTry = new CountDownLatch(1);
      Thread tryLockThread = new Thread(() -> {
         startedTry.countDown();
         try {
            if (!backgroundLock.tryLock(Long.MAX_VALUE, TimeUnit.DAYS)) {
               acquired.complete(false);
            } else {
               acquired.complete(true);
            }
         } catch (Throwable e) {
            acquired.complete(false);
         }
      });
      tryLockThread.start();
      Assert.assertTrue(startedTry.await(10, TimeUnit.SECONDS));
      otherManager.getDistributedLock("timedTryLockSuccess").unlock();
      Assert.assertTrue(acquired.get(4, TimeUnit.SECONDS));
   }

   @Test
   public void interruptStopTimedTryLock() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedPrimitiveManager otherManager = createManagedDistributeManager();
      otherManager.start();
      Assert.assertTrue(otherManager.getDistributedLock("interruptStopTimedTryLock").tryLock());
      DistributedLock backgroundLock = manager.getDistributedLock("interruptStopTimedTryLock");
      CompletableFuture<Boolean> interrupted = new CompletableFuture<>();
      CountDownLatch startedTry = new CountDownLatch(1);
      Thread tryLockThread = new Thread(() -> {
         startedTry.countDown();
         try {
            backgroundLock.tryLock(Long.MAX_VALUE, TimeUnit.DAYS);
            System.out.println("Case 1");
            interrupted.complete(false);
         } catch (UnavailableStateException e) {
            System.out.println("Case 2");
            e.printStackTrace();
            interrupted.complete(false);
         } catch (InterruptedException e) {
            System.out.println("Case 3");
            interrupted.complete(true);
         }
      });
      tryLockThread.start();
      Assert.assertTrue(startedTry.await(10, TimeUnit.SECONDS));
      // let background lock to perform some tries
      TimeUnit.SECONDS.sleep(1);
      System.out.println("Interrupting the thread: " + tryLockThread.getName());
      tryLockThread.interrupt();
      Assert.assertTrue(interrupted.get(40, TimeUnit.SECONDS));
   }

   @Test
   public void lockAndMutableLongWithSameIdCanExistsTogether() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final String id = "lockAndMutableLongWithSameIdCanExistsTogether";
      Assert.assertTrue(manager.getDistributedLock(id).tryLock());
      Assert.assertEquals(0, manager.getMutableLong(id).get());
      manager.getMutableLong(id).set(1);
      Assert.assertTrue(manager.getDistributedLock(id).isHeldByCaller());
      Assert.assertEquals(1, manager.getMutableLong(id).get());
   }
}

