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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CuratorDistributedPrimitiveManagerTest {

   private final ArrayList<AutoCloseable> autoCloseables = new ArrayList<>();

   private static final int BASE_SERVER_PORT = 6666;
   private static final int CONNECTION_MS = 2000;
   // Beware: the server tick must be small enough that to let the session to be correctly expired
   private static final int SESSION_MS = 6000;
   private static final int SERVER_TICK_MS = 2000;
   private static final int RETRIES_MS = 100;
   private static final int RETRIES = 1;

   public int nodes = 1;
   @Rule
   public TemporaryFolder tmpFolder = new TemporaryFolder();
   private TestingCluster testingServer;
   private String connectString;


   @Before
   public void setupEnv() throws Throwable {
      InstanceSpec[] clusterSpecs = new InstanceSpec[nodes];
      for (int i = 0; i < nodes; i++) {
         clusterSpecs[i] = new InstanceSpec(tmpFolder.newFolder(), BASE_SERVER_PORT + i, -1, -1, true, -1, SERVER_TICK_MS, -1);
      }
      testingServer = new TestingCluster(clusterSpecs);
      testingServer.start();
      connectString = testingServer.getConnectString();
   }

   @After
   public void tearDownEnv() throws Throwable {
      autoCloseables.forEach(closeables -> {
         try {
            closeables.close();
         } catch (Throwable t) {
            // silent here
         }
      });
      testingServer.close();
   }

   protected void configureManager(Map<String, String> config) {
      config.put("connect-string", connectString);
      config.put("session-ms", Integer.toString(SESSION_MS));
      config.put("connection-ms", Integer.toString(CONNECTION_MS));
      config.put("retries", Integer.toString(RETRIES));
      config.put("retries-ms", Integer.toString(RETRIES_MS));
   }

   protected DistributedPrimitiveManager createManagedDistributeManager(Consumer<? super Map<String, String>> defaultConfiguration) {
      try {
         final HashMap<String, String> config = new HashMap<>();
         configureManager(config);
         defaultConfiguration.accept(config);
         final DistributedPrimitiveManager manager = DistributedPrimitiveManager.newInstanceOf(managerClassName(), config);
         autoCloseables.add(manager);
         return manager;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   protected String managerClassName() {
      return CuratorDistributedPrimitiveManager.class.getName();
   }


   @Test
   public void verifyLayoutInZK() throws Exception {
      final DistributedPrimitiveManager manager = createManagedDistributeManager(config -> config.put("namespace", "activemq-artemis"));
      manager.start();
      Assert.assertTrue(manager.getDistributedLock("journal-identity-000-111").tryLock());

      Assert.assertTrue(manager.getMutableLong("journal-identity-000-111").compareAndSet(0, 1));

      CuratorFramework curatorFramework = ((CuratorDistributedPrimitiveManager)manager).getCurator();
      List<String> entries =  new LinkedList<>();
      dumpZK(curatorFramework.getZookeeperClient().getZooKeeper(), "/", entries);

      Assert.assertTrue(entries.get(2).contains("activation-sequence"));

      for (String entry: entries) {
         System.err.println("ZK: " + entry);
      }
   }

   private void dumpZK(ZooKeeper zooKeeper, String path, List<String> entries) throws InterruptedException, KeeperException {
      List<String> children = ZKPaths.getSortedChildren(zooKeeper,path);
      for (String s: children) {
         if (!s.equals("zookeeper")) {
            String qualifiedPath = (path.endsWith("/") ? path : path + "/") + s;
            Stat stat = new Stat();
            zooKeeper.getData(qualifiedPath, null, stat);
            entries.add(qualifiedPath + ", data-len:" + stat.getDataLength() + ", ephemeral: " + (stat.getEphemeralOwner() != 0));
            dumpZK(zooKeeper, qualifiedPath, entries);
         }
      }
   }
}
