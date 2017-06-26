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
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
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

@RunWith(BMUnitRunner.class)
public class LargeMessageOverReplicationTest extends ActiveMQTestBase {

   public static int messageChunkCount = 0;

   private static final ReusableLatch ruleFired = new ReusableLatch(1);
   private static ActiveMQServer backupServer;
   private static ActiveMQServer liveServer;

   ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616?minLargeMessageSize=10000&HA=true&retryInterval=100&reconnectAttempts=-1&producerWindowSize=10000");
   ActiveMQConnection connection;
   Session session;
   Queue queue;
   MessageProducer producer;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      ruleFired.setCount(1);
      messageChunkCount = 0;

      TransportConfiguration liveConnector = TransportConfigurationUtils.getNettyConnector(true, 0);
      TransportConfiguration liveAcceptor = TransportConfigurationUtils.getNettyAcceptor(true, 0);
      TransportConfiguration backupConnector = TransportConfigurationUtils.getNettyConnector(false, 0);
      TransportConfiguration backupAcceptor = TransportConfigurationUtils.getNettyAcceptor(false, 0);

      Configuration backupConfig = createDefaultInVMConfig().setBindingsDirectory(getBindingsDir(0, true)).setJournalDirectory(getJournalDir(0, true)).setPagingDirectory(getPageDir(0, true)).setLargeMessagesDirectory(getLargeMessagesDir(0, true));

      Configuration liveConfig = createDefaultInVMConfig();

      ReplicatedBackupUtils.configureReplicationPair(backupConfig, backupConnector, backupAcceptor, liveConfig, liveConnector, liveAcceptor);

      liveServer = createServer(liveConfig);
      liveServer.getConfiguration().addQueueConfiguration(new CoreQueueConfiguration().setName("Queue").setAddress("Queue"));
      liveServer.start();

      waitForServerToStart(liveServer);

      backupServer = createServer(backupConfig);
      backupServer.start();

      waitForServerToStart(backupServer);

      // Just to make sure the expression worked
      Assert.assertEquals(10000, factory.getMinLargeMessageSize());
      Assert.assertEquals(10000, factory.getProducerWindowSize());
      Assert.assertEquals(100, factory.getRetryInterval());
      Assert.assertEquals(-1, factory.getReconnectAttempts());
      Assert.assertTrue(factory.isHA());

      connection = (ActiveMQConnection) factory.createConnection();

      waitForRemoteBackup(connection.getSessionFactory(), 30);

      session = connection.createSession(true, Session.SESSION_TRANSACTED);
      queue = session.createQueue("Queue");
      producer = session.createProducer(queue);

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

   /*
  * simple test to induce a potential race condition where the server's acceptors are active, but the server's
  * state != STARTED
  */
   @Test
   @BMRules(
      rules = {@BMRule(
         name = "InterruptSending",
         targetClass = "org.apache.activemq.artemis.core.protocol.core.impl.ActiveMQSessionContext",
         targetMethod = "sendLargeMessageChunk",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.LargeMessageOverReplicationTest.messageChunkSent();")})
   public void testSendLargeMessage() throws Exception {

      MapMessage message = createLargeMessage();

      try {
         producer.send(message);
         Assert.fail("expected an exception");
         //      session.commit();
      } catch (JMSException expected) {
      }

      session.rollback();

      producer.send(message);
      session.commit();

      MessageConsumer consumer = session.createConsumer(queue);
      connection.start();

      MapMessage messageRec = (MapMessage) consumer.receive(5000);
      Assert.assertNotNull(messageRec);

      for (int i = 0; i < 10; i++) {
         Assert.assertEquals(1024 * 1024, message.getBytes("test" + i).length);
      }
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "InterruptReceive",
         targetClass = "org.apache.activemq.artemis.core.protocol.core.impl.CoreSessionCallback",
         targetMethod = "sendLargeMessageContinuation",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.LargeMessageOverReplicationTest.messageChunkReceived();")})
   public void testReceiveLargeMessage() throws Exception {

      MapMessage message = createLargeMessage();

      producer.send(message);
      session.commit();

      MessageConsumer consumer = session.createConsumer(queue);
      connection.start();

      MapMessage messageRec = null;

      try {
         consumer.receive(5000);
         Assert.fail("Expected a failure here");
      } catch (JMSException expected) {
      }

      session.rollback();

      messageRec = (MapMessage) consumer.receive(5000);
      Assert.assertNotNull(messageRec);
      session.commit();

      for (int i = 0; i < 10; i++) {
         Assert.assertEquals(1024 * 1024, message.getBytes("test" + i).length);
      }
   }

   public static void messageChunkReceived() {
      messageChunkCount++;

      if (messageChunkCount == 100) {
         final CountDownLatch latch = new CountDownLatch(1);
         new Thread() {
            @Override
            public void run() {
               try {
                  latch.countDown();
                  liveServer.stop();
               } catch (Exception e) {
                  e.printStackTrace();
               }
            }
         }.start();
         try {
            // just to make sure it's about to be stopped
            // avoiding bootstrapping the thread as a delay
            latch.await(1, TimeUnit.MINUTES);
         } catch (Throwable ignored) {
         }
      }
   }

   public static void messageChunkSent() {
      messageChunkCount++;

      try {
         if (messageChunkCount == 10) {
            liveServer.fail(true);

            System.err.println("activating");
            if (!backupServer.waitForActivation(1, TimeUnit.MINUTES)) {
               Logger.getLogger(LargeMessageOverReplicationTest.class).warn("Can't failover server");
            }
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
