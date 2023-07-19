/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.smoke.quorum;

import javax.management.remote.JMXServiceURL;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.Jmx;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

import static org.apache.activemq.artemis.tests.util.Jmx.backupOf;
import static org.apache.activemq.artemis.tests.util.Jmx.containsExactNodeIds;
import static org.apache.activemq.artemis.tests.util.Jmx.decodeNetworkTopologyJson;
import static org.apache.activemq.artemis.tests.util.Jmx.liveOf;
import static org.apache.activemq.artemis.tests.util.Jmx.validateNetworkTopology;
import static org.apache.activemq.artemis.tests.util.Jmx.withBackup;
import static org.apache.activemq.artemis.tests.util.Jmx.withLive;
import static org.apache.activemq.artemis.tests.util.Jmx.withMembers;
import static org.apache.activemq.artemis.tests.util.Jmx.withNodes;

@RunWith(Parameterized.class)
public abstract class PluggableQuorumSinglePairTest extends SmokeTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   static final String JMX_SERVER_HOSTNAME = "localhost";
   static final int JMX_PORT_PRIMARY = 10099;
   static final int JMX_PORT_BACKUP = 10199;

   static final String PRIMARY_DATA_FOLDER = "ReplicationPrimary";
   static final String BACKUP_DATA_FOLDER = "ReplicationBackup";

   static final int PRIMARY_PORT_OFFSET = 0;
   static final int BACKUP_PORT_OFFSET = PRIMARY_PORT_OFFSET + 100;

   public static class BrokerControl {

      final String name;
      final ObjectNameBuilder objectNameBuilder;
      final String dataFolder;
      final JMXServiceURL jmxServiceURL;
      final int portID;

      BrokerControl(final String name, int jmxPort, String dataFolder, int portID) {
         this.portID = portID;
         this.dataFolder = dataFolder;
         try {
            jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + jmxPort + "/jmxrmi");
         } catch (MalformedURLException e) {
            throw new RuntimeException(e);
         }
         this.objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), name, true);
         this.name = name;
      }

      public Process startServer(SmokeTestBase env, int millisTimeout) throws Exception {
         return env.startServer(dataFolder, portID, millisTimeout);
      }

      public void cleanupData() {
         SmokeTestBase.cleanupData(dataFolder);
      }

      public Optional<Boolean> isReplicaSync() throws Exception {
         return Jmx.isReplicaSync(jmxServiceURL, objectNameBuilder);
      }

      public Optional<Boolean> isBackup() throws Exception {
         return Jmx.isBackup(jmxServiceURL, objectNameBuilder);
      }

      public Optional<String> getNodeID() throws Exception {
         return Jmx.getNodeID(jmxServiceURL, objectNameBuilder);
      }

      public Optional<String> listNetworkTopology() throws Exception {
         return Jmx.listNetworkTopology(jmxServiceURL, objectNameBuilder);
      }

      public Optional<Long> getActivationSequence() throws Exception {
         return Jmx.getActivationSequence(jmxServiceURL, objectNameBuilder);
      }

      public Optional<Boolean> isActive() throws Exception {
         return Jmx.isActive(jmxServiceURL, objectNameBuilder);
      }
   }

   @Parameterized.Parameter
   public boolean forceKill;

   @Parameterized.Parameters(name = "forceKill={0}")
   public static Iterable<Object[]> getParams() {
      return Arrays.asList(new Object[][]{{false}, {true}});
   }

   protected BrokerControl primary;
   protected BrokerControl backup;
   protected List<BrokerControl> brokers;

   public PluggableQuorumSinglePairTest(String brokerFolderPrefix) {
      primary = new BrokerControl("primary", JMX_PORT_PRIMARY, brokerFolderPrefix + PRIMARY_DATA_FOLDER, PRIMARY_PORT_OFFSET);
      backup = new BrokerControl("backup", JMX_PORT_BACKUP, brokerFolderPrefix + BACKUP_DATA_FOLDER, BACKUP_PORT_OFFSET);
      brokers = Arrays.asList(primary, backup);
   }

   protected abstract boolean awaitAsyncSetupCompleted(long timeout, TimeUnit unit) throws InterruptedException;

   protected abstract int[] stopMajority() throws Exception;

   protected abstract void restart(int[] nodes) throws Exception;

   @Before
   public void setup() throws Exception {
      brokers.forEach(BrokerControl::cleanupData);
   }

   @Override
   @After
   public void after() throws Exception {
      super.after();
   }

   @Test
   public void testCanQueryEmptyBackup() throws Exception {
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);
      logger.info("starting primary");
      Process live = primary.startServer(this, timeout);
      Assert.assertTrue(awaitAsyncSetupCompleted(timeout, TimeUnit.MILLISECONDS));
      Wait.assertTrue(() -> !primary.isBackup().orElse(true), timeout);
      logger.info("killing primary");
      ServerUtil.killServer(live, forceKill);
      logger.info("starting backup");
      backup.startServer(this, 0);
      Wait.assertTrue(() -> backup.isBackup().orElse(false), timeout);
      logger.info("Stopping majority of consensus nodes");
      final int[] stopped = stopMajority();
      logger.info("Waiting until isolated");
      Thread.sleep(2000);
      logger.info("Restarting majority of consensus nodes");
      restart(stopped);
      Wait.assertTrue(() -> backup.isBackup().orElse(false), timeout);
   }

   @Test
   public void testBackupFailoverAndPrimaryFailback() throws Exception {
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);
      logger.info("starting primary");
      Process primaryInstance = primary.startServer(this, timeout);
      Assert.assertTrue(awaitAsyncSetupCompleted(timeout, TimeUnit.MILLISECONDS));
      // primary UN REPLICATED
      Assert.assertEquals(1L, primary.getActivationSequence().get().longValue());

      logger.info("started primary");
      logger.info("starting backup");
      Process backupInstance = backup.startServer(this, 0);
      Wait.assertTrue(() -> backup.isBackup().orElse(false), timeout);
      final String nodeID = primary.getNodeID().get();
      Assert.assertNotNull(nodeID);
      logger.info("NodeID: {}", nodeID);
      for (BrokerControl broker : brokers) {
         Wait.assertTrue(() -> validateNetworkTopology(broker.listNetworkTopology().orElse(""),
                                                           containsExactNodeIds(nodeID)
                                                          .and(withLive(nodeID, Objects::nonNull))
                                                          .and(withBackup(nodeID, Objects::nonNull))
                                                          .and(withMembers(1))
                                                          .and(withNodes(2))), timeout);
      }
      logger.info("primary topology is: {}", primary.listNetworkTopology().get());
      logger.info("backup topology is: {}", backup.listNetworkTopology().get());
      Assert.assertTrue(backup.isReplicaSync().get());
      logger.info("backup is synchronized with live");
      final String urlBackup = backupOf(nodeID, decodeNetworkTopologyJson(backup.listNetworkTopology().get()));
      Assert.assertNotNull(urlBackup);
      logger.info("backup: {}", urlBackup);
      final String urlPrimary = liveOf(nodeID, decodeNetworkTopologyJson(primary.listNetworkTopology().get()));
      Assert.assertNotNull(urlPrimary);
      logger.info("primary: {}", urlPrimary);
      Assert.assertNotEquals(urlPrimary, urlBackup);

      // primary REPLICATED, backup matches (has replicated) activation sequence
      Assert.assertEquals(1L, primary.getActivationSequence().get().longValue());
      Assert.assertEquals(1L, backup.getActivationSequence().get().longValue());

      logger.info("killing primary");
      ServerUtil.killServer(primaryInstance, forceKill);
      logger.info("killed primary");
      Wait.assertTrue(() -> !backup.isBackup().orElse(true), timeout);
      Wait.assertTrue(() -> validateNetworkTopology(backup.listNetworkTopology().orElse(""),
                                                    containsExactNodeIds(nodeID)
                                                       .and(withLive(nodeID, urlBackup::equals))
                                                       .and(withBackup(nodeID, Objects::isNull))
                                                       .and(withMembers(1))
                                                       .and(withNodes(1))), timeout);
      logger.info("backup topology is: {}", backup.listNetworkTopology().get());
      Assert.assertEquals(nodeID, backup.getNodeID().get());

      // backup UN REPLICATED (new version)
      Assert.assertEquals(2L, backup.getActivationSequence().get().longValue());

      // wait a bit before restarting primary
      logger.info("waiting before starting primary");
      TimeUnit.SECONDS.sleep(4);
      logger.info("starting primary");
      primaryInstance = primary.startServer(this, 0);
      logger.info("started primary");
      Wait.assertTrue(() -> backup.isBackup().orElse(false), timeout);
      Assert.assertTrue(!primary.isBackup().get());
      for (BrokerControl broker : brokers) {
         Wait.assertTrue(() -> validateNetworkTopology(broker.listNetworkTopology().orElse(""),
                                                       containsExactNodeIds(nodeID)
                                                          .and(withLive(nodeID, urlPrimary::equals))
                                                          .and(withBackup(nodeID, urlBackup::equals))
                                                          .and(withMembers(1))
                                                          .and(withNodes(2))), timeout);
      }
      logger.info("primary topology is: {}", primary.listNetworkTopology().get());
      logger.info("backup topology is: {}", backup.listNetworkTopology().get());
      Assert.assertTrue(backup.isReplicaSync().get());
      logger.info("backup is synchronized with live");
      Assert.assertEquals(nodeID, primary.getNodeID().get());

      // primary ran un replicated for a short while after failback, before backup was in sync
      Assert.assertEquals(3L, primary.getActivationSequence().get().longValue());
      Assert.assertEquals(3L, backup.getActivationSequence().get().longValue());

      logger.info("Done, killing both");
      ServerUtil.killServer(primaryInstance);
      ServerUtil.killServer(backupInstance);
   }

   @Test
   public void testLivePrimarySuicideOnLostQuorum() throws Exception {
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);
      Process primaryInstance = primary.startServer(this, timeout);
      Assert.assertTrue(awaitAsyncSetupCompleted(timeout, TimeUnit.MILLISECONDS));
      Wait.assertTrue(() -> !primary.isBackup().orElse(true), timeout);
      final String nodeID = primary.getNodeID().get();
      Wait.assertTrue(() -> validateNetworkTopology(primary.listNetworkTopology().orElse(""),
                                                    containsExactNodeIds(nodeID)
                                                       .and(withLive(nodeID, Objects::nonNull))
                                                       .and(withBackup(nodeID, Objects::isNull))
                                                       .and(withMembers(1))
                                                       .and(withNodes(1))), timeout);
      final String urlLive = liveOf(nodeID, decodeNetworkTopologyJson(primary.listNetworkTopology().get()));
      Assert.assertTrue(validateNetworkTopology(primary.listNetworkTopology().orElse(""),
                                                containsExactNodeIds(nodeID)
                                                   .and(withLive(nodeID, urlLive::equals))
                                                   .and(withBackup(nodeID, Objects::isNull))
                                                   .and(withMembers(1))
                                                   .and(withNodes(1))));
      stopMajority();
      Wait.waitFor(()-> !primaryInstance.isAlive(), timeout);
   }

   @Test
   public void testLiveBackupSuicideOnLostQuorum() throws Exception {
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);
      Process primaryInstance = primary.startServer(this, timeout);
      Assert.assertTrue(awaitAsyncSetupCompleted(timeout, TimeUnit.MILLISECONDS));
      Wait.assertTrue(() -> !primary.isBackup().orElse(true), timeout);
      Process backupInstance = backup.startServer(this, 0);
      Wait.assertTrue(() -> backup.isBackup().orElse(false), timeout);
      final String nodeID = primary.getNodeID().get();
      Assert.assertNotNull(nodeID);
      for (BrokerControl broker : brokers) {
         Wait.assertTrue(() -> validateNetworkTopology(broker.listNetworkTopology().orElse(""),
                                                       containsExactNodeIds(nodeID)
                                                          .and(withLive(nodeID, Objects::nonNull))
                                                          .and(withBackup(nodeID, Objects::nonNull))
                                                          .and(withMembers(1))
                                                          .and(withNodes(2))), timeout);
      }
      Assert.assertTrue(backup.isReplicaSync().get());
      final String urlBackup = backupOf(nodeID, decodeNetworkTopologyJson(backup.listNetworkTopology().get()));
      Assert.assertNotNull(urlBackup);
      final String urlPrimary = liveOf(nodeID, decodeNetworkTopologyJson(primary.listNetworkTopology().get()));
      Assert.assertNotNull(urlPrimary);
      Assert.assertNotEquals(urlPrimary, urlBackup);
      ServerUtil.killServer(primaryInstance, forceKill);
      Wait.assertTrue(() -> !backup.isBackup().orElse(true), timeout);
      Wait.assertTrue(() -> validateNetworkTopology(backup.listNetworkTopology().orElse(""),
                                                    containsExactNodeIds(nodeID)
                                                       .and(withLive(nodeID, urlBackup::equals))
                                                       .and(withBackup(nodeID, Objects::isNull))
                                                       .and(withMembers(1))
                                                       .and(withNodes(1))), timeout);
      Assert.assertEquals(nodeID, backup.getNodeID().get());
      stopMajority();
      Wait.waitFor(()-> !backupInstance.isAlive(), timeout);
   }


   @Test
   public void testOnlyLastUnreplicatedCanStart() throws Exception {
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);
      logger.info("starting primary");
      Process primaryInstance = primary.startServer(this, timeout);
      Assert.assertTrue(awaitAsyncSetupCompleted(timeout, TimeUnit.MILLISECONDS));
      Wait.assertTrue(() -> !primary.isBackup().orElse(true), timeout);
      logger.info("started primary");
      logger.info("starting backup");
      Process backupInstance = backup.startServer(this, 0);
      Wait.assertTrue(() -> backup.isBackup().orElse(false), timeout);
      final String nodeID = primary.getNodeID().get();
      Assert.assertNotNull(nodeID);
      logger.info("NodeID: {}", nodeID);
      for (BrokerControl broker : brokers) {
         Wait.assertTrue(() -> validateNetworkTopology(broker.listNetworkTopology().orElse(""),
                                                       containsExactNodeIds(nodeID)
                                                          .and(withLive(nodeID, Objects::nonNull))
                                                          .and(withBackup(nodeID, Objects::nonNull))
                                                          .and(withMembers(1))
                                                          .and(withNodes(2))), timeout);
      }
      logger.info("primary topology is: {}", primary.listNetworkTopology().get());
      logger.info("backup topology is: {}", backup.listNetworkTopology().get());
      Assert.assertTrue(backup.isReplicaSync().get());
      logger.info("backup is synchronized with live");
      final String urlBackup = backupOf(nodeID, decodeNetworkTopologyJson(backup.listNetworkTopology().get()));
      Assert.assertNotNull(urlBackup);
      logger.info("backup: {}", urlBackup);
      final String urlPrimary = liveOf(nodeID, decodeNetworkTopologyJson(primary.listNetworkTopology().get()));
      Assert.assertNotNull(urlPrimary);
      logger.info("primary: {}", urlPrimary);
      Assert.assertNotEquals(urlPrimary, urlBackup);


      // verify sequence id's in sync
      Assert.assertEquals(1L, primary.getActivationSequence().get().longValue());
      Assert.assertEquals(1L, backup.getActivationSequence().get().longValue());

      logger.info("killing primary");
      ServerUtil.killServer(primaryInstance, forceKill);
      logger.info("killed primary");
      Wait.assertTrue(() -> !backup.isBackup().orElse(true), timeout);
      Wait.assertTrue(() -> validateNetworkTopology(backup.listNetworkTopology().orElse(""),
                                                    containsExactNodeIds(nodeID)
                                                       .and(withLive(nodeID, urlBackup::equals))
                                                       .and(withBackup(nodeID, Objects::isNull))
                                                       .and(withMembers(1))
                                                       .and(withNodes(1))), timeout);
      logger.info("backup topology is: {}", backup.listNetworkTopology().get());
      Assert.assertEquals(nodeID, backup.getNodeID().get());


      // backup now UNREPLICATED, it is the only node that can continue
      Assert.assertEquals(2L, backup.getActivationSequence().get().longValue());

      logger.info("killing backup");
      ServerUtil.killServer(backupInstance, forceKill);

      // wait a bit before restarting primary
      logger.info("waiting before starting primary");
      TimeUnit.SECONDS.sleep(4);
      logger.info("restarting primary");

      Process restartedPrimary = primary.startServer(this, 0);
      logger.info("restarted primary, {}", restartedPrimary);

      Wait.assertFalse("Primary shouldn't activate", () -> primary.isActive().orElse(false), 5000);

      ServerUtil.killServer(restartedPrimary);

      logger.info("restarting backup");

      // backup can resume with data seq 3
      final Process restartedBackupInstance = backup.startServer(this, 5000);
      Wait.waitFor(() -> backup.isActive().orElse(false), 5000);
      assertTrue(Wait.waitFor(() -> nodeID.equals(backup.getNodeID().orElse("not set yet"))));
      logger.info("restarted backup");

      Assert.assertEquals(3L, backup.getActivationSequence().get().longValue());
   }
}
