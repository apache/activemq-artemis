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

package org.apache.activemq.artemis.tests.smoke.quorum;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

public class ZookeeperPluggableQuorumSinglePairTest extends PluggableQuorumSinglePairTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
   private static final int BASE_SERVER_PORT = 6666;
   // Beware: the server tick must be small enough that to let the session to be correctly expired
   private static final int SERVER_TICK_MS = 100;

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
         clusterSpecs[i] = new InstanceSpec(temporaryFolder.newFolder(), BASE_SERVER_PORT + i, -1, -1, true, -1, SERVER_TICK_MS, -1);
      }
      testingServer = new TestingCluster(clusterSpecs);
      testingServer.start();
      Assert.assertEquals("127.0.0.1:6666,127.0.0.1:6667,127.0.0.1:6668", testingServer.getConnectString());
      logger.info("Cluster of {} nodes on: {}", 3, testingServer.getConnectString());
   }

   @Override
   @After
   public void after() throws Exception {
      // zk bits that leak from servers
      ThreadLeakCheckRule.addKownThread("ListenerHandler-");
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

   protected boolean ensembleHasLeader() {
      return testingServer.getServers().stream().filter(ZookeeperPluggableQuorumSinglePairTest::isLeader).count() != 0;
   }

   private static boolean isLeader(TestingZooKeeperServer server) {
      long leaderId = server.getQuorumPeer().getLeaderId();
      long id = server.getQuorumPeer().getId();
      return id == leaderId;
   }

   @Override
   protected int[] stopMajority() throws Exception {
      List<TestingZooKeeperServer> followers = testingServer.getServers();
      final int quorum = (nodes / 2) + 1;
      final int[] stopped = new int[quorum];
      for (int i = 0; i < quorum; i++) {
         followers.get(i).stop();
         stopped[i] = i;
      }
      return stopped;
   }

   @Override
   protected void restart(int[] nodes) throws Exception {
      List<TestingZooKeeperServer> servers = testingServer.getServers();
      for (int nodeIndex : nodes) {
         servers.get(nodeIndex).restart();
      }
   }
}
