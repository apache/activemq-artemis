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
package org.apache.activemq.artemis.quorum.etcd;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.activemq.artemis.quorum.etcd.EtcdDistributedPrimitiveManager.Config.ETCD_LEASE_HEARTBEAT_FHZ_PARAM;
import static org.apache.activemq.artemis.quorum.etcd.EtcdDistributedPrimitiveManager.Config.ETCD_LEASE_TTL_PARAM;
import static org.apache.activemq.artemis.quorum.etcd.EtcdDistributedPrimitiveManager.Config.ETCD_REQUEST_DEADLINE_PARAM;

public class EtcdQuorumTest {

   //static Process etcdProcess1, etcdProcess2, etcdProcess3;

   static final String etcdCommand;
   static {
      String etcd = System.getenv("ETCD_CMD");
      etcdCommand = etcd != null ? etcd : "etcd";
   }

   private static TemporaryFolder tmpFolder = new TemporaryFolder();

   private static final Map<Integer, Process> etcdProcesses = new HashMap<>();
   private static final Map<Integer, Path> etcdDatDir = new HashMap<>();
   private static final Map<Integer, Path> etcdWalDir = new HashMap<>();

   @BeforeClass
   public static void setUp() throws Exception {
      tmpFolder.create();
      etcdDatDir.putAll(
         Map.of(
            1, EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "dat-dir1"),
            2, EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "dat-dir2"),
            3, EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "dat-dir3")
         )
      );
      etcdWalDir.putAll(
         Map.of(
            1, EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "wal-dir1"),
            2, EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "wal-dir2"),
            3, EtcdProcessHelper.createEtcdTempFolder(tmpFolder, "wal-dir3")
         )
      );
   }

   private static void startEtcdNode(int nodeId) throws Exception {
      if (etcdProcesses.containsKey(nodeId)) {
         throw new IllegalStateException(nodeId + " already started!");
      }
      final int port1 = 2359 + 2*nodeId;
      final int port0 = port1 - 1 ;
      final Path etcdDataFolder = etcdDatDir.get(nodeId);
      final Path etcdWalFolder = etcdWalDir.get(nodeId);

      System.out.println(
         "\n----------------------------------------------------\n"+
         "Start Etcd node: " + nodeId +
         "\n----------------------------------------------------\n"
      );

      etcdProcesses.put(nodeId, EtcdProcessHelper.startProcess(
         "--listen-client-urls", "http://localhost:" + port0,
                   "--advertise-client-urls", "http://localhost:" + port0,
                   "--listen-peer-urls", "http://localhost:" + port1,
                   "--initial-advertise-peer-urls", "http://localhost:" + port1,
                   "--name", "node" + nodeId,
                   "--initial-cluster-token", "cluster-1",
                   "--data-dir", etcdDataFolder.toString(),
                   "--wal-dir", etcdWalFolder.toString(),
                   "--initial-cluster", "node1=http://localhost:2361,node2=http://localhost:2363,node3=http://localhost:2365",
                   "--initial-cluster-state", "new"
      ));
   }

   private static void stopNode(final int nodeId) {
      System.out.println(
         "\n----------------------------------------------------\n"+
         "Stop Etcd node: " + nodeId +
         "\n----------------------------------------------------\n"
      );
      EtcdProcessHelper.tearDown(etcdProcesses.remove(nodeId));
   }

   @AfterClass
   public static void tearDown() {
      stopAllNodes();
      EtcdProcessHelper.cleanup(tmpFolder);
      etcdDatDir.clear();
      etcdWalDir.clear();
   }

   @After
   public void afterTest() {
      stopAllNodes();
   }

   private static void startAllNodes() throws Exception {
      startEtcdNode(1);
      startEtcdNode(2);
      startEtcdNode(3);
   }

   private static void stopAllNodes() {
      etcdProcesses.values().forEach(EtcdProcessHelper::tearDown);
      etcdProcesses.clear();
   }

   private DistributedPrimitiveManager createManagedDistributeManager() {
      return new EtcdDistributedPrimitiveManager(Map.of(
         EtcdDistributedPrimitiveManager.Config.ETCD_ENDPOINT_PARAM, "localhost:2360,localhost:2362,localhost:2364",
         ETCD_LEASE_TTL_PARAM, "PT5S",
         ETCD_LEASE_HEARTBEAT_FHZ_PARAM, "PT4S",
         ETCD_REQUEST_DEADLINE_PARAM, "PT10S"
      ));
   }

   @Test
   public void canAcquireLockOnMajorityRestart() throws Exception {
      startAllNodes();
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertTrue(lock.tryLock());
      Assert.assertTrue(lock.isHeldByCaller());
      final CountDownLatch notAvailable = new CountDownLatch(1);
      final DistributedLock.UnavailableLockListener listener = notAvailable::countDown;
      lock.addListener(listener);
      Assert.assertEquals(1, notAvailable.getCount());
      stopNode(1);
      stopNode(2);
      notAvailable.await(30, TimeUnit.SECONDS);
      manager.stop();
      //restartMajorityNodes(true);
      startEtcdNode(2);
      final DistributedPrimitiveManager otherManager = createManagedDistributeManager();
      otherManager.start();
      // await more then the expected value, that depends by how curator session expiration is configured
      TimeUnit.SECONDS.sleep(2L);
      Assert.assertTrue(otherManager.getDistributedLock("a").tryLock());
   }

   @Test
   public void cannotStartManagerWithoutQuorum() throws Exception {
      startEtcdNode(1);
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      Assert.assertFalse(manager.start(5, TimeUnit.SECONDS));
      Assert.assertFalse(manager.isStarted());
   }

   @Test(expected = UnavailableStateException.class)
   public void cannotAcquireLockWithoutQuorum() throws Exception {
      startAllNodes();
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      stopNode(1);
      stopNode(2);
      TimeUnit.SECONDS.sleep(11L);
      System.out.println("------------------------------------------");
      System.out.println("try lock now");
      DistributedLock lock = manager.getDistributedLock("a");
      lock.tryLock();
   }

   @Test
   public void cannotCheckLockWithoutQuorum() throws Exception {
      startAllNodes();
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      stopNode(1);
      stopNode(2);
      DistributedLock lock = manager.getDistributedLock("a");
      final boolean held;
      try {
         held = lock.isHeldByCaller();
      } catch (UnavailableStateException expected) {
         return;
      }
      Assert.assertFalse(held);
   }

   @Test
   public void notifiedAsUnavailableWhileLoosingQuorum() throws Exception {
      startAllNodes();
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("a");
      CountDownLatch unavailable = new CountDownLatch(1);
      lock.addListener(unavailable::countDown);
      stopNode(2);
      stopNode(1);
      Assert.assertTrue(unavailable.await(20, TimeUnit.SECONDS));
   }

   @Test
   public void beNotifiedOnce() throws Exception {
      startAllNodes();
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("a");
      final AtomicInteger unavailableManager = new AtomicInteger(0);
      final AtomicInteger unavailableLock = new AtomicInteger(0);
      manager.addUnavailableManagerListener(unavailableManager::incrementAndGet);
      lock.addListener(unavailableLock::incrementAndGet);
      stopNode(3);
      stopNode(1);
      Wait.waitFor(() -> unavailableLock.get() == 1, Duration.ofSeconds(30L).toMillis());
      Wait.waitFor(() -> unavailableManager.get() == 1, Duration.ofSeconds(30L).toMillis());
      Assert.assertEquals(1, unavailableLock.get());
      Assert.assertEquals(1, unavailableManager.get());
   }

   @Test
   public void beNotifiedOfUnavailabilityWhileBlockedOnTimedLock() throws Exception {
      startAllNodes();
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("a");
      final AtomicInteger unavailableManager = new AtomicInteger(0);
      final AtomicInteger unavailableLock = new AtomicInteger(0);
      manager.addUnavailableManagerListener(unavailableManager::incrementAndGet);
      lock.addListener(unavailableLock::incrementAndGet);
      final DistributedPrimitiveManager otherManager = createManagedDistributeManager();
      otherManager.start();
      Wait.waitFor(() -> otherManager.getDistributedLock("a").tryLock(), Duration.ofSeconds(30L).toMillis());
      final CountDownLatch startedTimedLock = new CountDownLatch(1);
      final AtomicReference<Boolean> unavailableTimedLock = new AtomicReference<>(null);
      final Thread timedLock = new Thread(() -> {
         startedTimedLock.countDown();
         try {
            lock.tryLock(60L, TimeUnit.SECONDS);
            unavailableTimedLock.set(false);
         }
         catch (UnavailableStateException e) {
            unavailableTimedLock.set(true);
         }
         catch (InterruptedException e) {
            unavailableTimedLock.set(false);
         }
      });
      timedLock.start();
      Assert.assertTrue(startedTimedLock.await(5, TimeUnit.SECONDS));
      stopNode(3);
      stopNode(2);
      Wait.waitFor(() -> unavailableLock.get() > 0, Duration.ofSeconds(60L).toMillis());
      Assert.assertEquals(1, unavailableManager.get());
      Assert.assertEquals(true, unavailableTimedLock.get());
   }

   @Test
   public void beNotifiedOfAlreadyUnavailableManagerAfterAddingListener() throws Exception {
      startAllNodes();
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final AtomicBoolean unavailable = new AtomicBoolean(false);
      DistributedPrimitiveManager.UnavailableManagerListener managerListener = () -> unavailable.set(true);
      manager.addUnavailableManagerListener(managerListener);
      Assert.assertFalse(unavailable.get());
      stopNode(1);
      stopNode(3);
      Wait.waitFor(unavailable::get, Duration.ofSeconds(30L).toMillis());
      manager.removeUnavailableManagerListener(managerListener);
      final AtomicInteger unavailableOnRegister = new AtomicInteger();
      manager.addUnavailableManagerListener(unavailableOnRegister::incrementAndGet);
      Assert.assertEquals(1, unavailableOnRegister.get());
      unavailableOnRegister.set(0);
      try (DistributedLock lock = manager.getDistributedLock("a")) {
         lock.addListener(unavailableOnRegister::incrementAndGet);
         Assert.assertEquals(1, unavailableOnRegister.get());
      }
   }
}
