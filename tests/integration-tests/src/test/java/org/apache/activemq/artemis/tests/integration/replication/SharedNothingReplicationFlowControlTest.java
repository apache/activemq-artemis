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

package org.apache.activemq.artemis.tests.integration.replication;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.LoaderCallback;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SharedNothingReplicationFlowControlTest extends ActiveMQTestBase {

   ExecutorService sendMessageExecutor;


   @Before
   public void setupExecutor() {
      sendMessageExecutor = Executors.newCachedThreadPool();
   }

   @After
   public void teardownExecutor() {
      sendMessageExecutor.shutdownNow();
   }

   private static final Logger logger = Logger.getLogger(SharedNothingReplicationFlowControlTest.class);

   @Rule
   public TemporaryFolder brokersFolder = new TemporaryFolder();

   @Test
   public void testReplicationIfFlowControlled() throws Exception {
      // start live
      Configuration liveConfiguration = createLiveConfiguration();
      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.start();

      Wait.waitFor(() -> liveServer.isStarted());

      ServerLocator locator = ServerLocatorImpl.newLocator("tcp://localhost:61616");
      locator.setCallTimeout(60_000L);
      locator.setConnectionTTL(60_000L);

      final ClientSessionFactory csf = locator.createSessionFactory();
      ClientSession sess = csf.createSession();
      sess.createQueue("flowcontrol", RoutingType.ANYCAST, "flowcontrol", true);
      sess.close();


      int i = 0;
      final int j = 100;
      final CountDownLatch allMessageSent = new CountDownLatch(j);

      // start backup
      Configuration backupConfiguration = createBackupConfiguration();
      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.start();

      Wait.waitFor(() -> backupServer.isStarted());

      Wait.waitFor(backupServer::isReplicaSync, 30000);

      TestInterceptor interceptor = new TestInterceptor(30000);
      // Increase latency of handling packet on backup side and flow control would work
      backupServer.getClusterManager().getClusterController().addIncomingInterceptorForReplication(interceptor);

      byte[] body = new byte[32 * 1024];
      while (i < j) {
         sendMessageExecutor.execute(() -> {
            try {
               ClientSession session = csf.createSession(true, true);
               ClientProducer producer = session.createProducer("flowcontrol");
               ClientMessage message = session.createMessage(true);
               message.writeBodyBufferBytes(body);
               logger.infof("try to send a message after replicated");
               producer.send(message);
               logger.info("send message done");
               producer.close();
               session.close();

               allMessageSent.countDown();
            } catch (ActiveMQException e) {
               logger.error("send message", e);
            }
         });
         i++;
      }

      Assert.assertTrue("all message sent", allMessageSent.await(30, TimeUnit.SECONDS));
      interceptor.setSleepTime(0);

      csf.close();
      locator.close();
      Assert.assertTrue("Waiting for replica sync timeout", Wait.waitFor(liveServer::isReplicaSync, 30000));
      backupServer.stop(true);
      liveServer.stop(true);

      SequentialFileFactory fileFactory;

      File liveJournalDir = brokersFolder.getRoot().toPath().resolve("live").resolve("data").resolve("journal").toFile();
      fileFactory = new MappedSequentialFileFactory(liveConfiguration.getJournalLocation(), liveConfiguration.getJournalFileSize(), false, liveConfiguration.getJournalBufferSize_NIO(), liveConfiguration.getJournalBufferTimeout_NIO(), null);

      JournalImpl liveMessageJournal = new JournalImpl(liveConfiguration.getJournalFileSize(), liveConfiguration.getJournalMinFiles(), liveConfiguration.getJournalPoolFiles(), liveConfiguration.getJournalCompactMinFiles(), liveConfiguration.getJournalCompactPercentage(), fileFactory, "activemq-data", "amq", fileFactory.getMaxIO());

      liveMessageJournal.start();
      final AtomicInteger liveJournalCounter = new AtomicInteger();
      liveMessageJournal.load(new SharedNothingReplicationFlowControlTest.AddRecordLoaderCallback() {
         @Override
         public void addRecord(RecordInfo info) {
            if (!(info.userRecordType == JournalRecordIds.ADD_MESSAGE_PROTOCOL)) {
               // ignore
            }
            logger.infof("got live message %d %d", info.id, info.userRecordType);
            liveJournalCounter.incrementAndGet();
         }
      });

      // read backup's journal
      File backupJournalDir = brokersFolder.getRoot().toPath().resolve("backup").resolve("data").resolve("journal").toFile();
      fileFactory = new MappedSequentialFileFactory(backupConfiguration.getJournalLocation(), backupConfiguration.getJournalFileSize(), false, backupConfiguration.getJournalBufferSize_NIO(), backupConfiguration.getJournalBufferTimeout_NIO(), null);

      JournalImpl backupMessageJournal = new JournalImpl(backupConfiguration.getJournalFileSize(), backupConfiguration.getJournalMinFiles(), backupConfiguration.getJournalPoolFiles(), backupConfiguration.getJournalCompactMinFiles(), backupConfiguration.getJournalCompactPercentage(), fileFactory, "activemq-data", "amq", fileFactory.getMaxIO());

      backupMessageJournal.start();

      final AtomicInteger replicationCounter = new AtomicInteger();
      backupMessageJournal.load(new SharedNothingReplicationFlowControlTest.AddRecordLoaderCallback() {
         @Override
         public void addRecord(RecordInfo info) {
            if (!(info.userRecordType == JournalRecordIds.ADD_MESSAGE_PROTOCOL)) {
               // ignore
            }
            logger.infof("replicated message %d", info.id);
            replicationCounter.incrementAndGet();
         }
      });

      logger.infof("expected %d messages, live=%d, backup=%d", j, liveJournalCounter.get(), replicationCounter.get());
      Assert.assertEquals("Live lost journal record", j, liveJournalCounter.get());
      Assert.assertEquals("Backup did not replicated all journal", j, replicationCounter.get());
   }

   // Set a small call timeout and write buffer high water mark value to trigger replication flow control
   private Configuration createLiveConfiguration() throws Exception {
      Configuration conf = new ConfigurationImpl();
      conf.setName("localhost::live");

      File liveDir = brokersFolder.newFolder("live");
      conf.setBrokerInstance(liveDir);

      conf.addAcceptorConfiguration("live", "tcp://localhost:61616?writeBufferHighWaterMark=2048&writeBufferLowWaterMark=2048");
      conf.addConnectorConfiguration("backup", "tcp://localhost:61617");
      conf.addConnectorConfiguration("live", "tcp://localhost:61616");

      conf.setClusterUser("mycluster");
      conf.setClusterPassword("mypassword");

      ReplicatedPolicyConfiguration haPolicy = new ReplicatedPolicyConfiguration();
      haPolicy.setVoteOnReplicationFailure(false);
      haPolicy.setCheckForLiveServer(false);
      conf.setHAPolicyConfiguration(haPolicy);

      ClusterConnectionConfiguration ccconf = new ClusterConnectionConfiguration();
      ccconf.setStaticConnectors(new ArrayList<>()).getStaticConnectors().add("backup");
      ccconf.setName("cluster");
      ccconf.setConnectorName("live");
      ccconf.setCallTimeout(4000);
      conf.addClusterConfiguration(ccconf);

      conf.setSecurityEnabled(false).setJMXManagementEnabled(false).setJournalType(JournalType.MAPPED).setJournalFileSize(1024 * 512).setConnectionTTLOverride(60_000L);

      return conf;
   }

   private Configuration createBackupConfiguration() throws Exception {
      Configuration conf = new ConfigurationImpl();
      conf.setName("localhost::backup");

      File backupDir = brokersFolder.newFolder("backup");
      conf.setBrokerInstance(backupDir);

      ReplicaPolicyConfiguration haPolicy = new ReplicaPolicyConfiguration();
      haPolicy.setClusterName("cluster");
      conf.setHAPolicyConfiguration(haPolicy);

      conf.addAcceptorConfiguration("backup", "tcp://localhost:61617");
      conf.addConnectorConfiguration("live", "tcp://localhost:61616");
      conf.addConnectorConfiguration("backup", "tcp://localhost:61617");

      conf.setClusterUser("mycluster");
      conf.setClusterPassword("mypassword");

      ClusterConnectionConfiguration ccconf = new ClusterConnectionConfiguration();
      ccconf.setStaticConnectors(new ArrayList<>()).getStaticConnectors().add("live");
      ccconf.setName("cluster");
      ccconf.setConnectorName("backup");
      conf.addClusterConfiguration(ccconf);

      /**
       * Set a bad url then, as a result the backup node would make a decision
       * of replicating from live node in the case of connection failure.
       * Set big check period to not schedule checking which would stop server.
       */
      conf.setNetworkCheckPeriod(1000000).setNetworkCheckURLList("http://localhost:28787").setSecurityEnabled(false).setJMXManagementEnabled(false).setJournalType(JournalType.MAPPED).setJournalFileSize(1024 * 512).setConnectionTTLOverride(60_000L);

      return conf;
   }

   abstract class AddRecordLoaderCallback implements LoaderCallback {

      @Override
      public void addPreparedTransaction(PreparedTransactionInfo preparedTransaction) {

      }

      @Override
      public void deleteRecord(long id) {

      }

      @Override
      public void updateRecord(RecordInfo info) {

      }

      @Override
      public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> recordsToDelete) {

      }
   }

   public static final class TestInterceptor implements Interceptor {

      private long sleepTime;

      public TestInterceptor(long sleepTime) {
         this.sleepTime = sleepTime;
      }

      public void setSleepTime(long sleepTime) {
         this.sleepTime = sleepTime;
      }

      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) throws ActiveMQException {
         try {
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() < startTime + sleepTime) {
               Thread.sleep(100);
            }
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         return true;
      }
   }
}
