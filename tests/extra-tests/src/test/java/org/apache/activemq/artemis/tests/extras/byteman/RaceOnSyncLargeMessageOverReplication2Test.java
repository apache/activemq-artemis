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
package org.apache.activemq.artemis.tests.extras.byteman;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.FailoverEventListener;
import org.apache.activemq.artemis.api.core.client.FailoverEventType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.SharedNothingBackupActivation;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.ReplicatedBackupUtils;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * This test will add more bytes to the large message while still syncing.
 * At the time of writing I couldn't replicate any issues, but I'm keeping it here to validate the impl
 */
@RunWith(BMUnitRunner.class)
public class RaceOnSyncLargeMessageOverReplication2Test extends ActiveMQTestBase {
   private static final Logger log = Logger.getLogger(RaceOnSyncLargeMessageOverReplication2Test.class);

   public static int messageChunkCount = 0;

   private static ActiveMQServer backupServer;
   private static ActiveMQServer liveServer;

   ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616?minLargeMessageSize=10000&HA=true&retryInterval=100&reconnectAttempts=-1&producerWindowSize=10000");
   ActiveMQConnection connection;
   Session session;
   Queue queue;
   MessageProducer producer;

   Configuration backupConfig;

   Configuration liveConfig;

   // To inform the main thread the condition is met
   static final ReusableLatch flagChunkEntered = new ReusableLatch(1);
   // To wait while the condition is worked out
   static final ReusableLatch flagChunkWait = new ReusableLatch(1);

   // To inform the main thread the condition is met
   static final ReusableLatch flagSyncEntered = new ReusableLatch(1);
   // To wait while the condition is worked out
   static final ReusableLatch flagSyncWait = new ReusableLatch(1);

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      log.debug("Tmp::" + getTemporaryDir());

      flagChunkEntered.setCount(1);
      flagChunkWait.setCount(1);

      flagSyncEntered.setCount(1);
      flagSyncWait.setCount(1);

      messageChunkCount = 0;

      TransportConfiguration liveConnector = TransportConfigurationUtils.getNettyConnector(true, 0);
      TransportConfiguration liveAcceptor = TransportConfigurationUtils.getNettyAcceptor(true, 0);
      TransportConfiguration backupConnector = TransportConfigurationUtils.getNettyConnector(false, 0);
      TransportConfiguration backupAcceptor = TransportConfigurationUtils.getNettyAcceptor(false, 0);

      backupConfig = createDefaultInVMConfig().setBindingsDirectory(getBindingsDir(0, true)).
         setJournalDirectory(getJournalDir(0, true)).setPagingDirectory(getPageDir(0, true)).
         setLargeMessagesDirectory(getLargeMessagesDir(0, true));

      liveConfig = createDefaultInVMConfig();

      ReplicatedBackupUtils.configureReplicationPair(backupConfig, backupConnector, backupAcceptor, liveConfig, liveConnector, liveAcceptor);

      liveServer = createServer(liveConfig);
      liveServer.getConfiguration().addQueueConfiguration(new CoreQueueConfiguration().setName("Queue").setAddress("Queue"));
      liveServer.start();

      waitForServerToStart(liveServer);

      // Just to make sure the expression worked
      Assert.assertEquals(10000, factory.getMinLargeMessageSize());
      Assert.assertEquals(10000, factory.getProducerWindowSize());
      Assert.assertEquals(100, factory.getRetryInterval());
      Assert.assertEquals(-1, factory.getReconnectAttempts());
      Assert.assertTrue(factory.isHA());

      connection = (ActiveMQConnection) factory.createConnection();
      session = connection.createSession(true, Session.SESSION_TRANSACTED);
      queue = session.createQueue("Queue");
      producer = session.createProducer(queue);

   }

   private void startBackup() throws Exception {
      backupServer = createServer(backupConfig);
      backupServer.start();

      waitForServerToStart(backupServer);

   }

   @After
   public void stopServers() throws Exception {
      if (connection != null) {
         try {
            connection.close();
         } catch (Exception e) {
         }
      }
      if (backupServer != null) {
         backupServer.stop();
         backupServer = null;
      }

      if (liveServer != null) {
         liveServer.stop();
         liveServer = null;
      }

      backupServer = liveServer = null;
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "InterruptSending",
         targetClass = "org.apache.activemq.artemis.core.protocol.core.impl.ActiveMQSessionContext",
         targetMethod = "sendLargeMessageChunk",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.RaceOnSyncLargeMessageOverReplication2Test.messageChunkSent();"), @BMRule(
         name = "InterruptSync",
         targetClass = "org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager",
         targetMethod = "sendLargeMessageFiles",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.RaceOnSyncLargeMessageOverReplication2Test.syncLargeMessage();")})
   public void testSendLargeMessage() throws Exception {

      final CountDownLatch failedOver = new CountDownLatch(1);
      connection.setFailoverListener(new FailoverEventListener() {
         @Override
         public void failoverEvent(FailoverEventType eventType) {
            failedOver.countDown();
         }
      });
      Thread t;

      {
         final MapMessage message = createLargeMessage();

         t = new Thread() {
            @Override
            public void run() {
               try {
                  producer.send(message);
                  session.commit();
               } catch (JMSException expected) {
                  expected.printStackTrace();
               }
            }
         };
      }

      t.start();

      // I'm trying to simulate the following race here:
      // The message is syncing while the client is already sending the body of the message

      Assert.assertTrue(flagChunkEntered.await(10, TimeUnit.SECONDS));

      startBackup();

      Assert.assertTrue(flagSyncEntered.await(10, TimeUnit.SECONDS));

      flagChunkWait.countDown();

      t.join(5000);

      log.debug("Thread joined");

      Assert.assertFalse(t.isAlive());

      flagSyncWait.countDown();

      Assert.assertTrue(((SharedNothingBackupActivation) backupServer.getActivation()).waitForBackupSync(10, TimeUnit.SECONDS));

      waitForRemoteBackup(connection.getSessionFactory(), 30);

      liveServer.fail(true);

      Assert.assertTrue(failedOver.await(10, TimeUnit.SECONDS));

      {
         MessageConsumer consumer = session.createConsumer(queue);

         connection.start();

         MapMessage message = (MapMessage) consumer.receive(5000);

         Assert.assertNotNull(message);

         for (int i = 0; i < 10; i++) {
            Assert.assertEquals(1024 * 1024, message.getBytes("test" + i).length);
         }

         session.commit();
      }
   }

   public static void syncLargeMessage() {

      try {
         flagSyncEntered.countDown();
         flagSyncWait.await(100, TimeUnit.SECONDS);
      } catch (Exception e) {
         e.printStackTrace();
      }

   }

   public static void messageChunkSent() {
      messageChunkCount++;

      try {
         if (messageChunkCount == 1) {
            flagChunkEntered.countDown();
            flagChunkWait.await(10, TimeUnit.SECONDS);
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private MapMessage createLargeMessage() throws JMSException {
      MapMessage message = session.createMapMessage();

      for (int i = 0; i < 10; i++) {
         message.setBytes("test" + i, new byte[1024 * 1024]);
      }
      return message;
   }

}
