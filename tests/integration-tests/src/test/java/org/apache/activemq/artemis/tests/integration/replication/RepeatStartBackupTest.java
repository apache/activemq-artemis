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
package org.apache.activemq.artemis.tests.integration.replication;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.DistributedLockManagerConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationBackupPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicationPrimaryPolicyConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.lockmanager.file.FileBasedLockManager;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepeatStartBackupTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private DistributedLockManagerConfiguration managerConfiguration;
   ActiveMQServer backupServer;
   ActiveMQServer server;

   File newTemporaryFolder(String name) {
      File newFolder = new File(temporaryFolder, name);
      newFolder.mkdirs();
      return newFolder;
   }

   @BeforeEach
   @Override
   public void setUp() throws Exception {
      managerConfiguration = new DistributedLockManagerConfiguration(FileBasedLockManager.class.getName(), Collections.singletonMap("locks-folder", newTemporaryFolder("manager").toString()));
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);

      // start live
      Configuration liveConfiguration = createLiveConfiguration();

      server = addServer(ActiveMQServers.newActiveMQServer(liveConfiguration));
      server.setIdentity("PRIMARY");
      server.getConfiguration().setJournalFileSize(100 * 1024);

      server.start();

      server.addAddressInfo(new AddressInfo("t1").addRoutingType(RoutingType.ANYCAST));
      server.createQueue(QueueConfiguration.of("t1").setAddress("t1").setRoutingType(RoutingType.ANYCAST));

      // start backup
      Configuration backupConfiguration = createBackupConfiguration();
      ((ReplicationBackupPolicyConfiguration) backupConfiguration.getHAPolicyConfiguration()).setAllowFailBack(true);
      backupServer = addServer(ActiveMQServers.newActiveMQServer(backupConfiguration));
      backupServer.setIdentity("BACKUP");
      backupServer.start();

      Wait.waitFor(backupServer::isStarted);

      Wait.assertTrue(() -> backupServer.isReplicaSync(), timeout);
   }

   @Test
   public void testLoopStart() throws Exception {

      try (AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler()) {

         ExecutorService executorService = Executors.newFixedThreadPool(1);
         runAfter(executorService::shutdownNow);

         AtomicInteger errors = new AtomicInteger(0);
         AtomicBoolean running = new AtomicBoolean(true);

         runAfter(() -> running.set(false));
         CountDownLatch latch = new CountDownLatch(1);

         executorService.execute(() -> {
            try {
               ConnectionFactory factory = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
               try (Connection connection = factory.createConnection()) {
                  Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                  MessageConsumer consumer = session.createConsumer(session.createQueue("t1"));
                  MessageProducer producer = session.createProducer(session.createQueue("t1"));
                  connection.start();
                  while (running.get()) {
                     producer.send(session.createTextMessage("hello"));
                     Assertions.assertNotNull(consumer.receive(1000));
                  }
               }
            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
               errors.incrementAndGet();
            } finally {
               latch.countDown();
            }
         });

         for (int i = 0; i < 5; i++) {
            logger.info("\n*******************************************************************************************************************************\ntest {}\n*******************************************************************************************************************************", i);
            backupServer.stop();
            Wait.assertFalse(backupServer::isStarted);
            backupServer.start();
            Wait.assertTrue(backupServer::isStarted);
            if (i % 2 == 1) {
               Wait.assertTrue(backupServer::isReplicaSync);
            }

            Assertions.assertFalse(loggerHandler.findText("AMQ229254"));
            Assertions.assertFalse(loggerHandler.findText("AMQ229006"));
            loggerHandler.clear();
         }

         running.set(false);

         Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));

         Assertions.assertEquals(0, errors.get());
      }


   }

   protected HAPolicyConfiguration createReplicationLiveConfiguration() {
      ReplicationPrimaryPolicyConfiguration haPolicy = ReplicationPrimaryPolicyConfiguration.withDefault();
      haPolicy.setDistributedManagerConfiguration(managerConfiguration);
      return haPolicy;
   }

   protected HAPolicyConfiguration createReplicationBackupConfiguration() {
      ReplicationBackupPolicyConfiguration haPolicy = ReplicationBackupPolicyConfiguration.withDefault();
      haPolicy.setDistributedManagerConfiguration(managerConfiguration);
      haPolicy.setClusterName("cluster");
      return haPolicy;
   }

   protected Configuration createLiveConfiguration() throws Exception {
      Configuration conf = new ConfigurationImpl();
      conf.setJournalType(JournalType.NIO);
      conf.setName("localhost::live");

      File liveDir = newTemporaryFolder("live");
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

      conf.setSecurityEnabled(false).setJMXManagementEnabled(false).setJournalType(JournalType.NIO).setJournalFileSize(1024 * 512).setConnectionTTLOverride(60_000L);

      return conf;
   }

   protected Configuration createBackupConfiguration() throws Exception {
      Configuration conf = new ConfigurationImpl();
      conf.setName("localhost::backup");

      File backupDir = newTemporaryFolder("backup");
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

      conf.setSecurityEnabled(false).setJMXManagementEnabled(false).setJournalType(JournalType.NIO).setJournalFileSize(1024 * 512).setConnectionTTLOverride(60_000L);

      return conf;
   }



}
