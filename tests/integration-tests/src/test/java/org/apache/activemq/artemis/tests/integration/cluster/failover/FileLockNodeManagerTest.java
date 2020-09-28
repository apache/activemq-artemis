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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.core.server.impl.jdbc.JdbcNodeManager;
import org.apache.activemq.artemis.tests.integration.cluster.util.SameProcessActiveMQServer;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class FileLockNodeManagerTest extends FailoverTestBase {

   public enum NodeManagerType {
      InVM, Jdbc, File
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         int ackBatchSize) throws Exception {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks, ackBatchSize));
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks));
   }

   protected ClientSession createSession(ClientSessionFactory sf1) throws Exception {
      return addClientSession(sf1.createSession());
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf1.createSession(xa, autoCommitSends, autoCommitAcks));
   }

   @Parameterized.Parameters(name = "{0} Node Manager, Use Separate Lock Folder = {1}")
   public static Iterable<? extends Object> nodeManagerTypes() {
      return Arrays.asList(new Object[][]{
         {NodeManagerType.File, false},
         {NodeManagerType.File, true}});
   }

   @Parameterized.Parameter(0)
   public NodeManagerType nodeManagerType;
   @Parameterized.Parameter(1)
   public boolean useSeparateLockFolder;

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return getNettyAcceptorTransportConfiguration(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return getNettyConnectorTransportConfiguration(live);
   }

   private List<ScheduledExecutorService> scheduledExecutorServices = new ArrayList<>();
   private List<ExecutorService> executors = new ArrayList<>();

   @Override
   protected NodeManager createReplicatedBackupNodeManager(Configuration backupConfig) {
      Assume.assumeThat("Replicated backup is supported only by " + NodeManagerType.InVM + " Node Manager", nodeManagerType, Is.is(NodeManagerType.InVM));
      return super.createReplicatedBackupNodeManager(backupConfig);
   }

   @Override
   protected Configuration createDefaultInVMConfig() throws Exception {
      final Configuration config = super.createDefaultInVMConfig();
      if (useSeparateLockFolder) {
         config.setNodeManagerLockDirectory(getTestDir() + "/nm_lock");
      }
      return config;
   }

   @Override
   protected NodeManager createNodeManager() throws Exception {

      switch (nodeManagerType) {

         case InVM:
            return new InVMNodeManager(false);
         case Jdbc:
            final ThreadFactory daemonThreadFactory = t -> {
               final Thread th = new Thread(t);
               th.setDaemon(true);
               return th;
            };
            final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory);
            scheduledExecutorServices.add(scheduledExecutorService);
            final ExecutorService executor = Executors.newFixedThreadPool(2, daemonThreadFactory);
            executors.add(executor);
            final DatabaseStorageConfiguration dbConf = createDefaultDatabaseStorageConfiguration();
            final ExecutorFactory executorFactory = new OrderedExecutorFactory(executor);
            return JdbcNodeManager.with(dbConf, scheduledExecutorService, executorFactory);
         case File:
            final Configuration config = createDefaultInVMConfig();
            if (useSeparateLockFolder) {
               config.getNodeManagerLockLocation().mkdirs();
            }
            return new FileLockNodeManager(config.getNodeManagerLockLocation(), false);

         default:
            throw new AssertionError("enum type not supported!");
      }
   }


   @Override
   protected TestableServer createTestableServer(Configuration config) throws Exception {
      final boolean isBackup = config.getHAPolicyConfiguration() instanceof ReplicaPolicyConfiguration || config.getHAPolicyConfiguration() instanceof SharedStoreSlavePolicyConfiguration;
      NodeManager nodeManager = this.nodeManager;
      //create a separate NodeManager for the backup
      if (isBackup && (nodeManagerType == NodeManagerType.Jdbc || nodeManagerType == NodeManagerType.File)) {
         nodeManager = createNodeManager();
      }
      return new SameProcessActiveMQServer(createInVMFailoverServer(true, config, nodeManager, isBackup ? 2 : 1));
   }


   @After
   public void shutDownExecutors() {
      if (!scheduledExecutorServices.isEmpty()) {
         ThreadLeakCheckRule.addKownThread("oracle.jdbc.driver.BlockSource.ThreadedCachingBlockSource.BlockReleaser");
         executors.forEach(ExecutorService::shutdown);
         scheduledExecutorServices.forEach(ExecutorService::shutdown);
         executors.clear();
         scheduledExecutorServices.clear();
      }
   }

   @Test(timeout = 120000)
   public void testSimpleFailover() throws Exception {
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.HOST_PROP_NAME, "127.0.0.1");
      TransportConfiguration tc = createTransportConfiguration(true, false, params);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithHA(tc)).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setReconnectAttempts(150).setRetryInterval(10);

      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = createSession(sf, true, true, 0);

      session.createQueue(new QueueConfiguration(ADDRESS));

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 10;

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      crash(session);

      sendMessages(session, producer, numMessages);
      receiveMessages(consumer, 0, numMessages, true);

      session.close();

      sf.close();

      Assert.assertEquals(0, sf.numSessions());

      Assert.assertEquals(0, sf.numConnections());
   }

}
