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

package org.apache.activemq.artemis.tests.smoke.quorum;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class ZookeeperPluggableQuorumSinglePairTest extends PluggableQuorumSinglePairTest {

   private static final Logger LOGGER = Logger.getLogger(ZookeeperPluggableQuorumSinglePairTest.class);
   private static final int BASE_SERVER_PORT = 6666;
   // Beware: the server tick must be small enough that to let the session to be correctly expired
   private static final int SERVER_TICK_MS = 100;

   @Rule
   public TemporaryFolder tmpFolder = new TemporaryFolder();
   private TestingCluster testingServer;
   private InstanceSpec[] clusterSpecs;
   private int nodes;

   @Before
   @Override
   public void setup() throws Exception {
      super.setup();
      nodes = 3;
      clusterSpecs = new InstanceSpec[nodes];
      for (int i = 0; i < nodes; i++) {
         clusterSpecs[i] = new InstanceSpec(tmpFolder.newFolder(), BASE_SERVER_PORT + i, -1, -1, true, -1, SERVER_TICK_MS, -1);
      }
      testingServer = new TestingCluster(clusterSpecs);
      testingServer.start();
      Assert.assertEquals("127.0.0.1:6666,127.0.0.1:6667,127.0.0.1:6668", testingServer.getConnectString());
      LOGGER.infof("Cluster of %d nodes on: %s", 3, testingServer.getConnectString());
   }

   @Override
   @After
   public void after() throws Exception {
      try {
         super.after();
      } finally {
         testingServer.close();
      }
   }

   public ZookeeperPluggableQuorumSinglePairTest() {
      super("zk");
   }

   @Override
   protected boolean awaitAsyncSetupCompleted(long timeout, TimeUnit unit) {
      return true;
   }

   @Override
   protected void stopMajority() throws Exception {
      List<TestingZooKeeperServer> followers = testingServer.getServers();
      final int quorum = (nodes / 2) + 1;
      for (int i = 0; i < quorum; i++) {
         followers.get(i).stop();
      }
   }
}
