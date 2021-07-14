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
package org.apache.activemq.artemis.tests.integration.replication;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.LoaderCallback;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.message.impl.CoreMessagePersister;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SharedNothingReplicationTest extends ActiveMQTestBase {

   private static final Logger logger = Logger.getLogger(SharedNothingReplicationTest.class);

   @Rule
   public TemporaryFolder brokersFolder = new TemporaryFolder();

   private SlowMessagePersister slowMessagePersister;
   ExecutorService sendMessageExecutor;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      sendMessageExecutor = Executors.newSingleThreadExecutor();
      CoreMessagePersister.registerPersister(SlowMessagePersister._getInstance());
   }

   @After
   @Override
   public void tearDown() throws Exception {
      CoreMessagePersister.resetPersister();
      sendMessageExecutor.shutdownNow();
      super.tearDown();
   }

   @Test
   public void testReplicateFromSlowLive() throws Exception {
      // start live
      Configuration liveConfiguration = createLiveConfiguration();
      ActiveMQServer liveServer = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      liveServer.start();

      Wait.waitFor(liveServer::isStarted);

      final CountDownLatch replicated = new CountDownLatch(1);

      ServerLocator locator = ServerLocatorImpl.newLocator("tcp://localhost:61616");
      locator.setCallTimeout(60_000L);
      locator.setConnectionTTL(60_000L);
      locator.addClusterTopologyListener(new ClusterTopologyListener() {
         @Override
         public void nodeUP(TopologyMember member, boolean last) {
            logger.debugf("nodeUP fired last=%s, live=%s, backup=%s", last, member.getLive(), member.getBackup());
            if (member.getBackup() != null) {
               replicated.countDown();
            }
         }

         @Override
         public void nodeDown(long eventUID, String nodeID) {

         }
      });

      final ClientSessionFactory csf = locator.createSessionFactory();
      ClientSession sess = csf.createSession();
      sess.createQueue(new QueueConfiguration("slow").setRoutingType(RoutingType.ANYCAST));
      sess.close();

      // let's write some messages
      int i = 0;
      final int j = 50;
      final CountDownLatch allMessageSent = new CountDownLatch(j);
      while (i < 5) {
         sendMessageExecutor.execute(() -> {
            try {
               ClientSession session = csf.createSession(true, true);
               ClientProducer producer = session.createProducer("slow");
               ClientMessage message = session.createMessage(true);
               // this will make journal's append executor busy
               message.putLongProperty("delay", 500L);
               logger.debugf("try to send a message before replicated");
               producer.send(message);
               logger.debugf("send message done");
               producer.close();
               session.close();

               allMessageSent.countDown();
            } catch (ActiveMQException e) {
               logger.error("send message", e);
            }
         });
         i++;
      }

      // start backup
      Configuration backupConfiguration = createBackupConfiguration();
      ActiveMQServer backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.start();

      Wait.waitFor(() -> backupServer.isStarted());

      Assert.assertTrue("can not replicate in 30 seconds", replicated.await(30, TimeUnit.SECONDS));

      while (i < j) {
         sendMessageExecutor.execute(() -> {
            try {
               ClientSession session = csf.createSession(true, true);
               ClientProducer producer = session.createProducer("slow");
               ClientMessage message = session.createMessage(true);
               message.putLongProperty("delay", 0L);
               logger.debugf("try to send a message after replicated");
               producer.send(message);
               logger.debugf("send message done");
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

      csf.close();
      locator.close();
      backupServer.stop(true);
      liveServer.stop(true);

      SequentialFileFactory fileFactory;

      File liveJournalDir = brokersFolder.getRoot().toPath().resolve("live").resolve("data").resolve("journal").toFile();
      fileFactory = new MappedSequentialFileFactory(liveConfiguration.getJournalLocation(), liveConfiguration.getJournalFileSize(), false, liveConfiguration.getJournalBufferSize_NIO(), liveConfiguration.getJournalBufferTimeout_NIO(), null);

      JournalImpl liveMessageJournal = new JournalImpl(liveConfiguration.getJournalFileSize(), liveConfiguration.getJournalMinFiles(), liveConfiguration.getJournalPoolFiles(), liveConfiguration.getJournalCompactMinFiles(), liveConfiguration.getJournalCompactPercentage(), fileFactory, "activemq-data", "amq", fileFactory.getMaxIO());

      liveMessageJournal.start();
      final AtomicInteger liveJournalCounter = new AtomicInteger();
      liveMessageJournal.load(new AddRecordLoaderCallback() {
         @Override
         public void addRecord(RecordInfo info) {
            if (!(info.userRecordType == JournalRecordIds.ADD_MESSAGE_PROTOCOL)) {
               // ignore
            }
            logger.debugf("got live message %d", info.id);
            liveJournalCounter.incrementAndGet();
         }
      });

      // read backup's journal
      File backupJournalDir = brokersFolder.getRoot().toPath().resolve("backup").resolve("data").resolve("journal").toFile();
      fileFactory = new MappedSequentialFileFactory(backupConfiguration.getJournalLocation(), backupConfiguration.getJournalFileSize(), false, backupConfiguration.getJournalBufferSize_NIO(), backupConfiguration.getJournalBufferTimeout_NIO(), null);

      JournalImpl backupMessageJournal = new JournalImpl(backupConfiguration.getJournalFileSize(), backupConfiguration.getJournalMinFiles(), backupConfiguration.getJournalPoolFiles(), backupConfiguration.getJournalCompactMinFiles(), backupConfiguration.getJournalCompactPercentage(), fileFactory, "activemq-data", "amq", fileFactory.getMaxIO());

      backupMessageJournal.start();

      final AtomicInteger replicationCounter = new AtomicInteger();
      backupMessageJournal.load(new AddRecordLoaderCallback() {
         @Override
         public void addRecord(RecordInfo info) {
            if (!(info.userRecordType == JournalRecordIds.ADD_MESSAGE_PROTOCOL)) {
               // ignore
            }
            logger.debugf("replicated message %d", info.id);
            replicationCounter.incrementAndGet();
         }
      });

      logger.debugf("expected %d messages, live=%d, backup=%d", j, liveJournalCounter.get(), replicationCounter.get());
      Assert.assertEquals("Live lost journal record", j, liveJournalCounter.get());
      Assert.assertEquals("Backup did not replicated all journal", j, replicationCounter.get());

      // if this ever happens.. you need to make sure this persister is registered instead of the CoreMessagePersister
      Assert.assertTrue("The test is not valid, slow persister stopped being used", SlowMessagePersister._getInstance().used);
   }

   protected HAPolicyConfiguration createReplicationLiveConfiguration() {
      return new ReplicatedPolicyConfiguration()
         .setVoteOnReplicationFailure(false)
         .setCheckForLiveServer(false);
   }

   private Configuration createLiveConfiguration() throws Exception {
      Configuration conf = new ConfigurationImpl();
      conf.setName("localhost::live");

      File liveDir = brokersFolder.newFolder("live");
      conf.setBrokerInstance(liveDir);

      conf.addAcceptorConfiguration("live", "tcp://localhost:61616");
      conf.addConnectorConfiguration("backup", "tcp://localhost:61617");
      conf.addConnectorConfiguration("live", "tcp://localhost:61616");

      conf.setClusterUser("mycluster");
      conf.setClusterPassword("mypassword");

      conf.setHAPolicyConfiguration(createReplicationLiveConfiguration());

      ClusterConnectionConfiguration ccconf = new ClusterConnectionConfiguration();
      ccconf.setStaticConnectors(new ArrayList<>()).getStaticConnectors().add("backup");
      ccconf.setName("cluster");
      ccconf.setConnectorName("live");
      conf.addClusterConfiguration(ccconf);

      conf.setSecurityEnabled(false).setJMXManagementEnabled(false).setJournalType(JournalType.MAPPED).setJournalFileSize(1024 * 512).setConnectionTTLOverride(60_000L);

      return conf;
   }

   protected HAPolicyConfiguration createReplicationBackupConfiguration() {
      return new ReplicaPolicyConfiguration().setClusterName("cluster");
   }

   private Configuration createBackupConfiguration() throws Exception {
      Configuration conf = new ConfigurationImpl();
      conf.setName("localhost::backup");

      File backupDir = brokersFolder.newFolder("backup");
      conf.setBrokerInstance(backupDir);

      conf.setHAPolicyConfiguration(createReplicationBackupConfiguration());

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

      conf.setSecurityEnabled(false).setJMXManagementEnabled(false).setJournalType(JournalType.MAPPED).setJournalFileSize(1024 * 512).setConnectionTTLOverride(60_000L);

      return conf;
   }

   static class SlowMessagePersister extends CoreMessagePersister implements Persister<Message> {

      boolean used = false;

      private static final Logger logger = Logger.getLogger(SlowMessagePersister.class);

      static SlowMessagePersister theInstance;

      private final CoreMessagePersister persister;

      private SlowMessagePersister() {
         persister = CoreMessagePersister.getInstance();
      }

      static SlowMessagePersister _getInstance() {
         if (theInstance == null) {
            theInstance = new SlowMessagePersister();
         }
         return theInstance;
      }

      @Override
      public byte getID() {
         return persister.getID();
      }

      @Override
      public int getEncodeSize(Message record) {
         return persister.getEncodeSize(record);
      }

      @Override
      public void encode(ActiveMQBuffer buffer, Message record) {
         used = true;
         try {
            Long delay = record.getLongProperty("delay");
            if (delay == null || delay.longValue() <= 0) {
               logger.debugf("encode message %d, caller=%s", record.getMessageID(), Thread.currentThread().getName());
            } else {
               logger.debugf("sleep %d ms before encode message %d, caller=%s", delay.longValue(), record.getMessageID(), Thread.currentThread().getName());
               Thread.sleep(delay.longValue());
            }
         } catch (InterruptedException e) {
            // it's ok
         }
         persister.encode(buffer, record);
      }

      @Override
      public Message decode(ActiveMQBuffer buffer, Message message, CoreMessageObjectPools pool) {
         return persister.decode(buffer, message, pool);
      }
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

}