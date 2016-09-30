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
package org.apache.activemq.artemis.tests.integration.divert;

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
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.ReplicatedBackupUtils;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReplicationWithDivertTest extends ActiveMQTestBase {

   public static final String JMS_SOURCE_QUEUE = "Queue";
   public static final String SOURCE_QUEUE = "jms.queue." + JMS_SOURCE_QUEUE;
   public static final String JMS_TARGET_QUEUE = "DestQueue";
   public static final String TARGET_QUEUE = "jms.queue." + JMS_TARGET_QUEUE;
   public static int messageChunkCount = 0;

   private static ActiveMQServer backupServer;
   private static ActiveMQServer liveServer;

   ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616?minLargeMessageSize=10000&HA=true&retryInterval=100&reconnectAttempts=-1&producerWindowSize=10000");
   ActiveMQConnection connection;
   Session session;
   Queue queue;
   Queue targetQueue;
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

      System.out.println("Tmp::" + getTemporaryDir());

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
      backupConfig.addQueueConfiguration(new CoreQueueConfiguration().setAddress(SOURCE_QUEUE).setName(SOURCE_QUEUE));
      backupConfig.addQueueConfiguration(new CoreQueueConfiguration().setAddress(TARGET_QUEUE).setName(TARGET_QUEUE));

      DivertConfiguration divertConfiguration = new DivertConfiguration().setName("Test").setAddress(SOURCE_QUEUE).setForwardingAddress(TARGET_QUEUE).setRoutingName("Test");

      liveConfig = createDefaultInVMConfig();
      liveConfig.addQueueConfiguration(new CoreQueueConfiguration().setAddress(SOURCE_QUEUE).setName(SOURCE_QUEUE).setDurable(true));
      liveConfig.addQueueConfiguration(new CoreQueueConfiguration().setAddress(TARGET_QUEUE).setName(TARGET_QUEUE).setDurable(true));
      liveConfig.addDivertConfiguration(divertConfiguration);

      backupConfig.addDivertConfiguration(divertConfiguration);

      ReplicatedBackupUtils.configureReplicationPair(backupConfig, backupConnector, backupAcceptor, liveConfig, liveConnector, liveAcceptor);

      liveServer = createServer(liveConfig);
      liveServer.start();

      startBackup();

      waitForServerToStart(liveServer);

      // Just to make sure the expression worked
      Assert.assertEquals(10000, factory.getMinLargeMessageSize());
      Assert.assertEquals(10000, factory.getProducerWindowSize());
      Assert.assertEquals(100, factory.getRetryInterval());
      Assert.assertEquals(-1, factory.getReconnectAttempts());
      Assert.assertTrue(factory.isHA());

      connection = (ActiveMQConnection) factory.createConnection();
      session = connection.createSession(true, Session.SESSION_TRANSACTED);
      queue = session.createQueue(JMS_SOURCE_QUEUE);
      targetQueue = session.createQueue(JMS_TARGET_QUEUE);

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
   public void testSendLargeMessage() throws Exception {

      final CountDownLatch failedOver = new CountDownLatch(1);
      connection.setFailoverListener(new FailoverEventListener() {
         @Override
         public void failoverEvent(FailoverEventType eventType) {
            failedOver.countDown();
         }
      });
      Thread t;

      final int numberOfMessage = 5;
      {
         final MapMessage message = createLargeMessage();

         t = new Thread() {
            @Override
            public void run() {
               try {
                  for (int i = 0; i < numberOfMessage; i++) {
                     producer.send(message);
                     session.commit();
                  }
               } catch (JMSException expected) {
                  expected.printStackTrace();
               }
            }
         };
      }

      t.start();

      t.join(10000);

      {
         MessageConsumer consumer = session.createConsumer(queue);

         connection.start();

         for (int msgi = 0; msgi < numberOfMessage; msgi++) {
            MapMessage message = (MapMessage) consumer.receive(5000);

            Assert.assertNotNull(message);

            for (int i = 0; i < 10; i++) {
               Assert.assertEquals(200 * 1024, message.getBytes("test" + i).length);
            }

            session.commit();
         }
         consumer.close();
      }

      Assert.assertFalse(t.isAlive());
      liveServer.stop(true);
      Assert.assertTrue(failedOver.await(10, TimeUnit.SECONDS));

      {
         MessageConsumer consumer = session.createConsumer(targetQueue);

         connection.start();

         for (int msgi = 0; msgi < numberOfMessage; msgi++) {
            MapMessage message = (MapMessage) consumer.receive(5000);

            Assert.assertNotNull(message);

            for (int i = 0; i < 10; i++) {
               Assert.assertEquals(200 * 1024, message.getBytes("test" + i).length);
            }

            session.commit();
         }

         consumer.close();
      }
   }

   private MapMessage createLargeMessage() throws JMSException {
      MapMessage message = session.createMapMessage();

      for (int i = 0; i < 10; i++) {
         message.setBytes("test" + i, new byte[200 * 1024]);
      }
      return message;
   }

}
