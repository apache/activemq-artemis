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

package org.apache.activemq.artemis.tests.smoke.quorum;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.tests.util.Jmx.containsExactNodeIds;
import static org.apache.activemq.artemis.tests.util.Jmx.decodeNetworkTopologyJson;
import static org.apache.activemq.artemis.tests.util.Jmx.liveOf;
import static org.apache.activemq.artemis.tests.util.Jmx.validateNetworkTopology;
import static org.apache.activemq.artemis.tests.util.Jmx.withBackup;
import static org.apache.activemq.artemis.tests.util.Jmx.withLive;
import static org.apache.activemq.artemis.tests.util.Jmx.withMembers;
import static org.apache.activemq.artemis.tests.util.Jmx.withNodes;

public class ZookeeperPluggableQuorumPeerTest extends ZookeeperPluggableQuorumSinglePairTest {

   private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperPluggableQuorumPeerTest.class);

   public ZookeeperPluggableQuorumPeerTest() {
      super();
      // accepting the primary/backup vars to reuse the test, for peers, these are interchangeable as either can take
      // both roles as both wish to be primary but will revert to backup
      primary = new BrokerControl("primary-peer-a", JMX_PORT_PRIMARY, "zkReplicationPrimaryPeerA", PRIMARY_PORT_OFFSET);
      backup = new BrokerControl("primary-peer-b", JMX_PORT_BACKUP, "zkReplicationPrimaryPeerB", BACKUP_PORT_OFFSET);
      brokers = new LinkedList(Arrays.asList(primary, backup));
   }

   @Test
   @Override
   public void testBackupFailoverAndPrimaryFailback() throws Exception {
      // peers don't request fail back by default
      // just wait for setup to avoid partial stop of zk via fast tear down with async setup
      Wait.waitFor(this::ensembleHasLeader);
   }

   @Test
   public void testBackupCannotForgetPeerIdOnLostQuorum() throws Exception {
      // see FileLockTest::testCorrelationId to get more info why this is not peer-journal-001 as in broker.xml
      final String coordinationId = "peer.journal.001";
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);
      LOGGER.info("starting peer a");
      final Process live = primary.startServer(this, 0);
      LOGGER.info("waiting peer a to increase coordinated activation sequence to 1");
      Wait.assertEquals(1L, () -> primary.getActivationSequence().orElse(Long.MAX_VALUE).longValue(), timeout);
      Assert.assertEquals(coordinationId, primary.getNodeID().get());
      Wait.waitFor(() -> primary.listNetworkTopology().isPresent(), timeout);
      final String urlPeerA = liveOf(coordinationId, decodeNetworkTopologyJson(primary.listNetworkTopology().get()));
      Assert.assertNotNull(urlPeerA);
      LOGGER.info("peer a acceptor: {}", urlPeerA);
      LOGGER.info("killing peer a");
      ServerUtil.killServer(live, forceKill);
      LOGGER.info("starting peer b");
      Process emptyBackup = backup.startServer(this, 0);
      LOGGER.info("waiting until peer b act as empty backup");
      Wait.assertTrue(() -> backup.isBackup().orElse(false), timeout);
      LOGGER.info("Stop majority of quorum nodes");
      final int[] majority = stopMajority();
      LOGGER.info("Wait peer b to deactivate");
      Thread.sleep(2000);
      LOGGER.info("Restart majority of quorum nodes");
      restart(majority);
      LOGGER.info("Restart peer a as legit last live");
      final Process restartedLive = primary.startServer(this, 0);
      LOGGER.info("waiting peer a to increase coordinated activation sequence to 2");
      Wait.assertEquals(2L, () -> primary.getActivationSequence().orElse(Long.MAX_VALUE).longValue(), timeout);
      Assert.assertEquals(coordinationId, primary.getNodeID().get());
      LOGGER.info("waiting peer b to be a replica");
      Wait.waitFor(() -> backup.isReplicaSync().orElse(false));
      Wait.assertEquals(2L, () -> backup.getActivationSequence().get().longValue());
      final String expectedUrlPeerA = liveOf(coordinationId, decodeNetworkTopologyJson(primary.listNetworkTopology().get()));
      Assert.assertEquals(urlPeerA, expectedUrlPeerA);
   }

   @Test
   public void testMultiPrimary_Peer() throws Exception {

      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);
      LOGGER.info("starting peer b primary");

      Process backupInstance = backup.startServer(this, timeout);

      // alive as unreplicated, it has configured node id
      assertTrue(Wait.waitFor(() -> 1L == backup.getActivationSequence().orElse(Long.MAX_VALUE).longValue()));

      final String nodeID = backup.getNodeID().get();
      Assert.assertNotNull(nodeID);
      LOGGER.info("NodeID: {}", nodeID);

      LOGGER.info("starting peer a primary");
      primary.startServer(this, 0);
      Wait.assertTrue(() -> primary.isBackup().orElse(false), timeout);

      Wait.assertTrue(() -> !backup.isBackup().orElse(true), timeout);

      for (BrokerControl broker : brokers) {
         Wait.assertTrue(() -> validateNetworkTopology(broker.listNetworkTopology().orElse(""),
                                                       containsExactNodeIds(nodeID)
                                                          .and(withLive(nodeID, Objects::nonNull))
                                                          .and(withBackup(nodeID, Objects::nonNull))
                                                          .and(withMembers(1))
                                                          .and(withNodes(2))), timeout);
      }

      LOGGER.info("primary topology is: {}", primary.listNetworkTopology().get());
      LOGGER.info("backup topology is: {}", backup.listNetworkTopology().get());
      Assert.assertTrue(backup.isReplicaSync().get());
      Assert.assertTrue(primary.isReplicaSync().get());


      LOGGER.info("killing peer-b");
      ServerUtil.killServer(backupInstance, forceKill);

      // peer-a now UNREPLICATED
      Wait.assertTrue(() -> 2L == primary.getActivationSequence().get().longValue());

      LOGGER.info("restarting peer-b");
      backup.startServer(this, 0);

      assertTrue(Wait.waitFor(() -> nodeID.equals(backup.getNodeID().orElse("not set yet"))));
      // peer-b now a REPLICA
      Wait.waitFor(() -> backup.isReplicaSync().get());
      Wait.assertTrue(() -> 2L == backup.getActivationSequence().get().longValue());
   }
}
