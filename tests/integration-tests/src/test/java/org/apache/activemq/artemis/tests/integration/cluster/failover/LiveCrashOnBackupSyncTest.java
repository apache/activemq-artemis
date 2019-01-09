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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
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
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreFactoryNIO;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreImpl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.activemq.artemis.tests.util.SpawnedVMSupport;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;

public class LiveCrashOnBackupSyncTest {

   static int OK = 2;
   public static String instancePth = System.getProperty("java.io.tmpdir");

   @Before
   public void setUp() throws Exception {
      deleteFile(new File(instancePth + "live"));
      deleteFile(new File(instancePth + "backup"));
   }

   @Test
   public void liveCrashOnBackupSyncLargeMessageTest() throws Exception {
      Process process = SpawnedVMSupport.spawnVM(LiveCrashOnBackupSyncTest.class.getCanonicalName());
      Assert.assertEquals(OK, process.waitFor());

      Configuration liveConfiguration = createLiveConfiguration();
      ActiveMQServer liveServer = ActiveMQServers.newActiveMQServer(liveConfiguration);
      liveServer.start();
      Wait.waitFor(() -> liveServer.isStarted());

      File liveLMDir = liveServer.getConfiguration().getLargeMessagesLocation();
      Set<Long> liveLM = getAllMessageFileIds(liveLMDir);
      Assert.assertEquals("we really ought to delete these after delivery", 0, liveLM.size());
      liveServer.stop();
   }

   @After
   public void tearDown() throws FileNotFoundException {
      deleteFile(new File(instancePth + "live"));
      deleteFile(new File(instancePth + "backup"));
   }

   private Configuration createLiveConfiguration() throws Exception {
      Configuration conf = new ConfigurationImpl();
      conf.setName("localhost::live");
      File liveDir = newFolder("live");
      conf.setBrokerInstance(liveDir);
      conf.addAcceptorConfiguration("live", "tcp://localhost:61616");
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
      conf.addClusterConfiguration(ccconf);

      conf.setSecurityEnabled(false).setJMXManagementEnabled(false).setJournalType(JournalType.MAPPED).setJournalFileSize(1024 * 512).setConnectionTTLOverride(60_000L);
      return conf;
   }

   private Configuration createBackupConfiguration() throws Exception {
      Configuration conf = new ConfigurationImpl();
      conf.setName("localhost::backup");
      File backupDir = newFolder("backup");
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

      conf.setSecurityEnabled(false).setJMXManagementEnabled(false).setJournalType(JournalType.MAPPED).setJournalFileSize(1024 * 512).setConnectionTTLOverride(60_000L);
      return conf;
   }

   private File newFolder(String live) throws IOException {
      File file = new File(instancePth + live);
      if (!file.exists()) {
         file.mkdirs();
      }
      return file;
   }

   private void deleteFile(File path) throws FileNotFoundException {
      if (!path.exists())
         return;
      if (path.isDirectory()) {
         for (File f : path.listFiles()) {
            deleteFile(f);
         }
      }
      path.delete();
   }

   private void createProducerSendSomeLargeMessages(int msgCount) throws Exception {
      byte[] buffer = new byte[100 * 1024];
      ServerLocator locator = ServerLocatorImpl.newLocator("tcp://localhost:61616");
      locator.setCallTimeout(60_000L);
      locator.setConnectionTTL(60_000L);
      final ClientSessionFactory csf = locator.createSessionFactory();
      ClientSession session = csf.createSession();
      session.createQueue("LiveCrashTestQueue", RoutingType.ANYCAST, "LiveCrashTestQueue", true);
      ClientProducer producer = session.createProducer("LiveCrashTestQueue");
      ClientMessage msgs = session.createMessage(true);
      msgs.getBodyBuffer().writeBytes(buffer);
      for (int i = 0; i < msgCount; i++) {
         producer.send(msgs);
      }
      producer.close();
      session.close();
   }

   private void receiveMsgs(int msgCount) throws Exception {
      ServerLocator locator = ServerLocatorImpl.newLocator("tcp://localhost:61616");
      locator.setCallTimeout(60_000L);
      locator.setConnectionTTL(60_000L);
      final ClientSessionFactory csf = locator.createSessionFactory();
      ClientSession session = csf.createSession();
      session.start();
      ClientConsumer consumer = session.createConsumer("LiveCrashTestQueue");
      for (int i = 0; i < msgCount; i++) {
         ClientMessage message = consumer.receive(1000);
         Assert.assertNotNull("Expecting a message " + i, message);
         message.acknowledge();
      }
      session.commit();
      consumer.close();
      session.close();
   }

   private Set<Long> getAllMessageFileIds(File dir) {
      Set<Long> idsOnBkp = new TreeSet<>();
      String[] fileList = dir.list();
      if (fileList != null) {
         for (String filename : fileList) {
            if (filename.endsWith(".msg")) {
               idsOnBkp.add(Long.valueOf(filename.split("\\.")[0]));
            }
         }
      }
      return idsOnBkp;
   }

   public static void main(String[] arg) {
      try {
         LiveCrashOnBackupSyncTest stop = new LiveCrashOnBackupSyncTest();
         Configuration liveConfiguration = stop.createLiveConfiguration();
         ActiveMQServer liveServer = new ActiveMQServerImpl(liveConfiguration, ManagementFactory.getPlatformMBeanServer(), new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), new SecurityConfiguration())) {
            @Override
            protected PagingStoreFactoryNIO getPagingStoreFactory() {
               return new PagingStoreFactoryNIO(this.getStorageManager(), this.getConfiguration().getPagingLocation(), this.getConfiguration().getJournalBufferTimeout_NIO(), this.getScheduledPool(), this.getExecutorFactory(), this.getConfiguration().isJournalSyncNonTransactional(), null) {
                  @Override
                  public synchronized PagingStore newStore(SimpleString address, AddressSettings settings) {
                     return new DelayPagingStoreImpl(address, this.getScheduledExecutor(), liveConfiguration.getJournalBufferTimeout_NIO(), getPagingManager(), getStorageManager(), null, this, address, settings, getExecutorFactory().getExecutor(), this.isSyncNonTransactional());
                  }
               };
            }
         };
         liveServer.start();
         Wait.waitFor(() -> liveServer.isStarted());

         Configuration backupConfiguration = stop.createBackupConfiguration();
         ActiveMQServer backupServer = ActiveMQServers.newActiveMQServer(backupConfiguration);
         backupServer.start();
         Wait.waitFor(() -> backupServer.isStarted());

         stop.createProducerSendSomeLargeMessages(100);
         stop.receiveMsgs(100);

         //flush
         liveServer.getStorageManager().getMessageJournal().stop();

         System.exit(OK);
      } catch (Exception e) {
         e.printStackTrace();
      } catch (Throwable throwable) {
         throwable.printStackTrace();
      }
   }
}

class DelayPagingStoreImpl extends PagingStoreImpl {

   DelayPagingStoreImpl(SimpleString address,
                               ScheduledExecutorService scheduledExecutor,
                               long syncTimeout,
                               PagingManager pagingManager,
                               StorageManager storageManager,
                               SequentialFileFactory fileFactory,
                               PagingStoreFactory storeFactory,
                               SimpleString storeName,
                               AddressSettings addressSettings,
                               ArtemisExecutor executor,
                               boolean syncNonTransactional) {
      super(address, scheduledExecutor, syncTimeout, pagingManager, storageManager, fileFactory, storeFactory, storeName, addressSettings, executor, syncNonTransactional);
   }

   @Override
   public void sendPages(ReplicationManager replicator, Collection<Integer> pageIds) throws Exception {
      //in order to extend the synchronization time
      Thread.sleep(20 * 1000);
      super.sendPages(replicator, pageIds);
   }
}