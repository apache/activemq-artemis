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
package org.apache.activemq.artemis.quorum.zookeeper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;

import org.apache.activemq.artemis.quorum.DistributedLockTest;
import org.apache.curator.test.TestingZooKeeperServer;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.Boolean.TRUE;
import static org.hamcrest.Matchers.greaterThan;

@RunWith(value = Parameterized.class)
public class CuratorDistributedLockTest extends DistributedLockTest {

   private static final int BASE_SERVER_PORT = 6666;
   private static final int CONNECTION_MS = 2000;
   // Beware: the server tick must be small enough that to let the session to be correctly expired
   private static final int SESSION_MS = 6000;
   private static final int SERVER_TICK_MS = 2000;
   private static final int RETRIES_MS = 100;
   private static final int RETRIES = 1;

   @Parameterized.Parameter
   public int nodes;
   @Rule
   public TemporaryFolder tmpFolder = new TemporaryFolder();
   private TestingCluster testingServer;
   private InstanceSpec[] clusterSpecs;
   private String connectString;

   @Parameterized.Parameters(name = "nodes={0}")
   public static Iterable<Object[]> getTestParameters() {
      return Arrays.asList(new Object[][]{{3}, {5}});
   }

   @Override
   public void setupEnv() throws Throwable {
      clusterSpecs = new InstanceSpec[nodes];
      for (int i = 0; i < nodes; i++) {
         clusterSpecs[i] = new InstanceSpec(tmpFolder.newFolder(), BASE_SERVER_PORT + i, -1, -1, true, -1, SERVER_TICK_MS, -1);
      }
      testingServer = new TestingCluster(clusterSpecs);
      testingServer.start();
      // start waits for quorumPeer!=null but not that it has started...
      Wait.waitFor(this::ensembleHasLeader);
      connectString = testingServer.getConnectString();
      super.setupEnv();
   }

   @Override
   public void tearDownEnv() throws Throwable {
      super.tearDownEnv();
      testingServer.close();
   }

   @Override
   protected void configureManager(Map<String, String> config) {
      config.put("connect-string", connectString);
      config.put("session-ms", Integer.toString(SESSION_MS));
      config.put("connection-ms", Integer.toString(CONNECTION_MS));
      config.put("retries", Integer.toString(RETRIES));
      config.put("retries-ms", Integer.toString(RETRIES_MS));
   }

   @Override
   protected String managerClassName() {
      return CuratorDistributedPrimitiveManager.class.getName();
   }

   @Test(expected = RuntimeException.class)
   public void cannotCreateManagerWithNotValidParameterNames() {
      final DistributedPrimitiveManager manager = createManagedDistributeManager(config -> config.put("_", "_"));
   }

   @Test
   public void canAcquireLocksFromDifferentNamespace() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      final DistributedPrimitiveManager manager1 = createManagedDistributeManager(config -> config.put("namespace", "1"));
      manager1.start();
      final DistributedPrimitiveManager manager2 = createManagedDistributeManager(config -> config.put("namespace", "2"));
      manager2.start();
      Assert.assertTrue(manager1.getDistributedLock("a").tryLock());
      Assert.assertTrue(manager2.getDistributedLock("a").tryLock());
   }

   @Test
   public void cannotStartManagerWithDisconnectedServer() throws IOException, ExecutionException, InterruptedException {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      testingServer.close();
      Assert.assertFalse(manager.start(1, TimeUnit.SECONDS));
   }

   @Test(expected = UnavailableStateException.class)
   public void cannotAcquireLockWithDisconnectedServer() throws IOException, ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final DistributedLock lock = manager.getDistributedLock("a");
      final CountDownLatch notAvailable = new CountDownLatch(1);
      final DistributedLock.UnavailableLockListener listener = notAvailable::countDown;
      lock.addListener(listener);
      testingServer.close();
      Assert.assertTrue(notAvailable.await(30, TimeUnit.SECONDS));
      lock.tryLock();
   }

   @Test(expected = UnavailableStateException.class)
   public void cannotTryLockWithDisconnectedServer() throws IOException, ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final DistributedLock lock = manager.getDistributedLock("a");
      testingServer.close();
      lock.tryLock();
   }

   @Test(expected = UnavailableStateException.class)
   public void cannotCheckLockStatusWithDisconnectedServer() throws IOException, ExecutionException, InterruptedException, TimeoutException, UnavailableStateException {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertFalse(lock.isHeldByCaller());
      Assert.assertTrue(lock.tryLock());
      testingServer.close();
      lock.isHeldByCaller();
   }

   @Test(expected = UnavailableStateException.class)
   public void looseLockAfterServerStop() throws ExecutionException, InterruptedException, TimeoutException, UnavailableStateException, IOException {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertTrue(lock.tryLock());
      Assert.assertTrue(lock.isHeldByCaller());
      final CountDownLatch notAvailable = new CountDownLatch(1);
      final DistributedLock.UnavailableLockListener listener = notAvailable::countDown;
      lock.addListener(listener);
      Assert.assertEquals(1, notAvailable.getCount());
      testingServer.close();
      Assert.assertTrue(notAvailable.await(30, TimeUnit.SECONDS));
      lock.isHeldByCaller();
   }

   @Test
   public void canAcquireLockOnMajorityRestart() throws Exception {
      final DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      final DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertTrue(lock.tryLock());
      Assert.assertTrue(lock.isHeldByCaller());
      final CountDownLatch notAvailable = new CountDownLatch(1);
      final DistributedLock.UnavailableLockListener listener = notAvailable::countDown;
      lock.addListener(listener);
      Assert.assertEquals(1, notAvailable.getCount());
      testingServer.stop();
      notAvailable.await();
      manager.stop();
      restartMajorityNodes(true);
      final DistributedPrimitiveManager otherManager = createManagedDistributeManager();
      otherManager.start();
      // await more then the expected value, that depends by how curator session expiration is configured
      TimeUnit.MILLISECONDS.sleep(SESSION_MS + SERVER_TICK_MS);
      Assert.assertTrue(otherManager.getDistributedLock("a").tryLock());
   }

   @Test
   public void cannotStartManagerWithoutQuorum() throws Exception {
      Assume.assumeThat(nodes, greaterThan(1));
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      stopMajorityNotLeaderNodes(true);
      Assert.assertFalse(manager.start(2, TimeUnit.SECONDS));
      Assert.assertFalse(manager.isStarted());
   }

   @Test(expected = UnavailableStateException.class)
   public void cannotAcquireLockWithoutQuorum() throws Exception {
      Assume.assumeThat(nodes, greaterThan(1));
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      stopMajorityNotLeaderNodes(true);
      DistributedLock lock = manager.getDistributedLock("a");
      lock.tryLock();
   }

   @Test
   public void cannotCheckLockWithoutQuorum() throws Exception {
      Assume.assumeThat(nodes, greaterThan(1));
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      stopMajorityNotLeaderNodes(true);
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
   public void canGetLockWithoutQuorum() throws Exception {
      Assume.assumeThat(nodes, greaterThan(1));
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      stopMajorityNotLeaderNodes(true);
      DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertNotNull(lock);
   }

   @Test
   public void notifiedAsUnavailableWhileLoosingQuorum() throws Exception {
      Assume.assumeThat(nodes, greaterThan(1));
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("a");
      CountDownLatch unavailable = new CountDownLatch(1);
      lock.addListener(unavailable::countDown);
      stopMajorityNotLeaderNodes(true);
      Assert.assertTrue(unavailable.await(SESSION_MS + SERVER_TICK_MS, TimeUnit.MILLISECONDS));
   }

   @Test
   public void beNotifiedOnce() throws Exception {
      Assume.assumeThat(nodes, greaterThan(1));
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("a");
      final AtomicInteger unavailableManager = new AtomicInteger(0);
      final AtomicInteger unavailableLock = new AtomicInteger(0);
      manager.addUnavailableManagerListener(unavailableManager::incrementAndGet);
      lock.addListener(unavailableLock::incrementAndGet);
      stopMajorityNotLeaderNodes(true);
      TimeUnit.MILLISECONDS.sleep(SESSION_MS + SERVER_TICK_MS + CONNECTION_MS);
      Assert.assertEquals(1, unavailableLock.get());
      Assert.assertEquals(1, unavailableManager.get());
   }

   @Test
   public void beNotifiedOfUnavailabilityWhileBlockedOnTimedLock() throws Exception {
      Assume.assumeThat(nodes, greaterThan(1));
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("a");
      final AtomicInteger unavailableManager = new AtomicInteger(0);
      final AtomicInteger unavailableLock = new AtomicInteger(0);
      manager.addUnavailableManagerListener(unavailableManager::incrementAndGet);
      lock.addListener(unavailableLock::incrementAndGet);
      final DistributedPrimitiveManager otherManager = createManagedDistributeManager();
      otherManager.start();
      Assert.assertTrue(otherManager.getDistributedLock("a").tryLock());
      final CountDownLatch startedTimedLock = new CountDownLatch(1);
      final AtomicReference<Boolean> unavailableTimedLock = new AtomicReference<>(null);
      Thread timedLock = new Thread(() -> {
         startedTimedLock.countDown();
         try {
            lock.tryLock(Long.MAX_VALUE, TimeUnit.DAYS);
            unavailableTimedLock.set(false);
         } catch (UnavailableStateException e) {
            unavailableTimedLock.set(true);
         } catch (InterruptedException e) {
            unavailableTimedLock.set(false);
         }
      });
      timedLock.start();
      Assert.assertTrue(startedTimedLock.await(10, TimeUnit.SECONDS));
      TimeUnit.SECONDS.sleep(1);
      stopMajorityNotLeaderNodes(true);
      TimeUnit.MILLISECONDS.sleep(SESSION_MS + CONNECTION_MS);
      Wait.waitFor(() -> unavailableLock.get() > 0, SERVER_TICK_MS);
      Assert.assertEquals(1, unavailableManager.get());
      Assert.assertEquals(TRUE, unavailableTimedLock.get());
   }

   private boolean ensembleHasLeader() {
      return testingServer.getServers().stream().filter(CuratorDistributedLockTest::isLeader).count() != 0;
   }

   private static boolean isLeader(TestingZooKeeperServer server) {
      long leaderId = server.getQuorumPeer().getLeaderId();
      long id = server.getQuorumPeer().getId();
      return id == leaderId;
   }

   private void stopMajorityNotLeaderNodes(boolean fromLast) throws Exception {
      List<TestingZooKeeperServer> followers = testingServer.getServers().stream().filter(Predicates.not(CuratorDistributedLockTest::isLeader)).collect(Collectors.toList());
      final int quorum = (nodes / 2) + 1;
      for (int i = 0; i < quorum; i++) {
         final int nodeIndex = fromLast ? (followers.size() - 1) - i : i;
         followers.get(nodeIndex).stop();
      }
   }

   private void restartMajorityNodes(boolean startFromLast) throws Exception {
      final int quorum = (nodes / 2) + 1;
      for (int i = 0; i < quorum; i++) {
         final int nodeIndex = startFromLast ? (nodes - 1) - i : i;
         if (!testingServer.restartServer(clusterSpecs[nodeIndex])) {
            throw new IllegalStateException("errored while restarting " + clusterSpecs[nodeIndex]);
         }
      }
   }
}
