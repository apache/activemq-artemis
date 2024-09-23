/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.amqp.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.collection.LongObjectHashMap;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.journal.collections.JournalHashMap;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.AckRetry;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerTarget;
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.AckManager;
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.AckManagerProvider;
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.ReferenceIDSupplier;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonAbstractReceiver;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AckManagerTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   ActiveMQServer server1;

   private static final String SNF_NAME = "$ACTIVEMQ_ARTEMIS_MIRROR_other";

   @BeforeEach
   @Override
   public void setUp() throws Exception {
      super.setUp();

      server1 = createServer(true, createDefaultConfig(0, true), 100024, -1, -1, -1);
      server1.getConfiguration().addAddressSetting(SNF_NAME, new AddressSettings().setMaxSizeBytes(-1).setMaxSizeMessages(-1).setMaxReadPageMessages(20));
      server1.getConfiguration().getAcceptorConfigurations().clear();
      server1.getConfiguration().addAcceptorConfiguration("server", "tcp://localhost:61616");
      server1.start();
   }

   @Test
   public void testDirectACK() throws Throwable {

      String protocol = "AMQP";

      SimpleString TOPIC_NAME = SimpleString.of("tp" + RandomUtil.randomString());

      server1.addAddressInfo(new AddressInfo(TOPIC_NAME).addRoutingType(RoutingType.MULTICAST));

      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      // creating 5 subscriptions
      for (int i = 0; i < 5; i++) {
         try (Connection connection = connectionFactory.createConnection()) {
            connection.setClientID("c" + i);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(TOPIC_NAME.toString());
            session.createDurableSubscriber(topic, "s" + i);
         }
      }

      int numberOfMessages = 500;
      int numberOfAcksC1 = 100;
      int numberOfAcksC2 = 200;

      Queue c1s1 = server1.locateQueue("c1.s1");
      assertNotNull(c1s1);
      Queue c2s2 = server1.locateQueue("c2.s2");
      assertNotNull(c2s2);

      PagingStore store = server1.getPagingManager().getPageStore(TOPIC_NAME);
      store.startPaging();

      try (Connection connection = connectionFactory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic(TOPIC_NAME.toString());
         MessageProducer producer = session.createProducer(topic);

         for (int i = 0; i < numberOfMessages; i++) {
            Message m = session.createTextMessage("hello " + i);
            m.setIntProperty("i", i);
            producer.send(m);
            if ((i + 1) % 100 == 0) {
               c1s1.pause();
               c2s2.pause();
               session.commit();
            }
         }
         session.commit();
      }

      ReferenceIDSupplier referenceIDSupplier = new ReferenceIDSupplier(server1);

      {
         AckManager ackManager = AckManagerProvider.getManager(server1);

         AtomicInteger counter = new AtomicInteger(0);

         for (long pageID = store.getFirstPage(); pageID <= store.getCurrentWritingPage(); pageID++) {
            Page page = store.usePage(pageID);
            try {
               page.getMessages().forEach(pagedMessage -> {
                  int increment = counter.incrementAndGet();
                  if (increment <= numberOfAcksC1) {
                     ackManager.addRetry(referenceIDSupplier.getServerID(pagedMessage.getMessage()), c1s1, referenceIDSupplier.getID(pagedMessage.getMessage()), AckReason.NORMAL);
                  }
                  if (increment <= numberOfAcksC2) {
                     ackManager.addRetry(referenceIDSupplier.getServerID(pagedMessage.getMessage()), c2s2, referenceIDSupplier.getID(pagedMessage.getMessage()), AckReason.NORMAL);
                  }
               });
            } finally {
               page.usageDown();
            }
         }
      }

      // in this following loop we will get the ackManager, compare the stored retries. stop the server and validate if they were reloaded correctly
      for (int repeat = 0; repeat < 2; repeat++) {
         logger.info("Repeating {}", repeat);
         AckManager ackManager = AckManagerProvider.getManager(server1);
         ackManager.start();

         HashMap<SimpleString, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>>> sortedRetries = ackManager.sortRetries();

         assertEquals(1, sortedRetries.size());

         LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>> acksOnAddress = sortedRetries.get(c1s1.getAddress());

         assertEquals(2, acksOnAddress.size());

         JournalHashMap<AckRetry, AckRetry, Queue> acksOnc1s1 = acksOnAddress.get(c1s1.getID());
         JournalHashMap<AckRetry, AckRetry, Queue> acksOnc2s2 = acksOnAddress.get(c2s2.getID());
         assertEquals(numberOfAcksC1, acksOnc1s1.size());
         assertEquals(numberOfAcksC2, acksOnc2s2.size());

         Wait.assertEquals(numberOfMessages, c1s1::getMessageCount);
         Wait.assertEquals(numberOfMessages, c2s2::getMessageCount);

         AckManager originalManager = AckManagerProvider.getManager(server1);
         server1.stop();
         assertEquals(0, AckManagerProvider.getSize());
         server1.start();
         AckManager newManager = AckManagerProvider.getManager(server1);
         assertEquals(1, AckManagerProvider.getSize());
         assertNotSame(originalManager, AckManagerProvider.getManager(server1));
         AckManager manager = AckManagerProvider.getManager(server1);
         Wait.assertTrue(manager::isStarted, 5_000);

         assertEquals(1, AckManagerProvider.getSize());
         assertNotSame(newManager, ackManager);
      }

      AckManager ackManager = AckManagerProvider.getManager(server1);
      ackManager.start();
      HashMap<SimpleString, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>>> sortedRetries = ackManager.sortRetries();
      assertEquals(1, sortedRetries.size());
      LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>> acksOnAddress = sortedRetries.get(c1s1.getAddress());
      JournalHashMap<AckRetry, AckRetry, Queue> acksOnc1s1 = acksOnAddress.get(c1s1.getID());
      JournalHashMap<AckRetry, AckRetry, Queue> acksOnc2s2 = acksOnAddress.get(c2s2.getID());

      Wait.assertEquals(0, () -> acksOnc1s1.size());
      Wait.assertEquals(0, () -> acksOnc2s2.size());

      for (int i = 0; i < 5; i++) {
         AtomicInteger counter = new AtomicInteger(0);

         for (long pageID = store.getFirstPage(); pageID <= store.getCurrentWritingPage(); pageID++) {
            Page page = store.usePage(pageID);
            try {
               page.getMessages().forEach(pagedMessage -> {
                  int increment = counter.incrementAndGet();
                  if (increment <= numberOfAcksC1) {
                     ackManager.addRetry(referenceIDSupplier.getServerID(pagedMessage.getMessage()), c1s1, referenceIDSupplier.getID(pagedMessage.getMessage()), AckReason.NORMAL);
                  }
                  if (increment <= numberOfAcksC2) {
                     ackManager.addRetry(referenceIDSupplier.getServerID(pagedMessage.getMessage()), c2s2, referenceIDSupplier.getID(pagedMessage.getMessage()), AckReason.NORMAL);
                  }
               });
            } finally {
               page.usageDown();
            }
         }
         Wait.assertEquals(0, () -> acksOnc1s1.size());
         Wait.assertEquals(0, () -> acksOnc2s2.size());
      }

      c1s1.resume();
      c2s2.resume();

      // creating 5 subscriptions
      for (int i = 0; i < 5; i++) {
         try (Connection connection = connectionFactory.createConnection()) {
            connection.setClientID("c" + i);
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(TOPIC_NAME.toString());
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, "s" + i);

            int start;
            switch (i) {
               case 1:
                  start = numberOfAcksC1;
                  break;
               case 2:
                  start = numberOfAcksC2;
                  break;
               default:
                  start = 0;
            }

            logger.debug("receiving messages for {}", i);

            for (int m = start; m < numberOfMessages; m++) {
               logger.debug("Receiving i={}, m={}", i, m);
               TextMessage message = (TextMessage) subscriber.receive(5000);
               assertNotNull(message);
               assertEquals("hello " + m, message.getText());
               assertEquals(m, message.getIntProperty("i"));
            }
            assertNull(subscriber.receiveNoWait());

         }
      }

      server1.getStorageManager().getMessageJournal().scheduleCompactAndBlock(10_000);
      assertEquals(0, getCounter(JournalRecordIds.ACK_RETRY, countJournal(server1.getConfiguration())));

      assertEquals(1, AckManagerProvider.getSize());

      // the server was restarted at least once, locating it again
      Queue c1s1AfterRestart = server1.locateQueue("c1.s1");
      assertNotNull(c1s1AfterRestart);

      ackManager.addRetry(referenceIDSupplier.getDefaultNodeID(), c1s1, 10_000_000L,AckReason.NORMAL);
      ackManager.addRetry(referenceIDSupplier.getDefaultNodeID(), c1s1, 10_000_001L,AckReason.NORMAL);

      Wait.assertTrue(() -> ackManager.sortRetries().isEmpty(), 5000);

      server1.getStorageManager().getMessageJournal().scheduleCompactAndBlock(10_000);
      assertEquals(0, getCounter(JournalRecordIds.ACK_RETRY, countJournal(server1.getConfiguration())));

      server1.stop();

      assertEquals(0, AckManagerProvider.getSize());
   }


   @Test
   public void testLogUnack() throws Throwable {
      String protocol = "AMQP";

      SimpleString TOPIC_NAME = SimpleString.of("tp" + RandomUtil.randomString());

      server1.addAddressInfo(new AddressInfo(TOPIC_NAME).addRoutingType(RoutingType.MULTICAST));

      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      // creating 5 subscriptions
      for (int i = 0; i < 5; i++) {
         try (Connection connection = connectionFactory.createConnection()) {
            connection.setClientID("c" + i);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(TOPIC_NAME.toString());
            session.createDurableSubscriber(topic, "s" + i);
         }
      }

      Queue c1s1 = server1.locateQueue("c1.s1");
      assertNotNull(c1s1);
      Queue c2s2 = server1.locateQueue("c2.s2");
      assertNotNull(c2s2);

      try (AssertionLoggerHandler assertionLoggerHandler = new AssertionLoggerHandler()) {
         server1.getConfiguration().setMirrorAckManagerWarnUnacked(true);
         c1s1.addConsumer(Mockito.mock(Consumer.class));
         AckManager ackManager = AckManagerProvider.getManager(server1);
         ackManager.ack("neverFound", c1s1, 1000, AckReason.NORMAL, true);

         // ID for there are consumers
         Wait.assertTrue(() -> assertionLoggerHandler.findText("AMQ111011"), 5000, 100);
         // ID for give up retry
         Wait.assertTrue(() -> assertionLoggerHandler.findText("AMQ111012"), 5000, 100);

         server1.getConfiguration().setMirrorAckManagerWarnUnacked(false);
         assertionLoggerHandler.clear();

         ackManager.ack("neverFound", c1s1, 1000, AckReason.NORMAL, true);

         // ID for there are consumers
         assertFalse(assertionLoggerHandler.findText("AMQ111011"));
         // ID for give up retry
         assertFalse(assertionLoggerHandler.findText("AMQ111012"));
      }
   }

   @Test
   public void testRetryFromPaging() throws Throwable {

      String protocol = "AMQP";

      SimpleString TOPIC_NAME = SimpleString.of("tp" + RandomUtil.randomString());

      server1.addAddressInfo(new AddressInfo(TOPIC_NAME).addRoutingType(RoutingType.MULTICAST));

      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      // creating 5 subscriptions
      for (int i = 0; i < 2; i++) {
         try (Connection connection = connectionFactory.createConnection()) {
            connection.setClientID("c" + i);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(TOPIC_NAME.toString());
            session.createDurableSubscriber(topic, "s" + i);
         }
      }

      int numberOfMessages = 15000;
      int numberOfAcksC0 = 100;
      int numberOfAcksC1 = 14999;

      String c0s0Name = "c0.s0";
      String c1s1Name = "c1.s1";

      final Queue c0s0 = server1.locateQueue(c0s0Name);
      assertNotNull(c0s0);
      final Queue c1s1 = server1.locateQueue(c1s1Name);
      assertNotNull(c1s1);

      PagingStore store = server1.getPagingManager().getPageStore(TOPIC_NAME);
      store.startPaging();

      try (Connection connection = connectionFactory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic(TOPIC_NAME.toString());
         MessageProducer producer = session.createProducer(topic);

         for (int i = 0; i < numberOfMessages; i++) {
            Message m = session.createTextMessage("hello " + i);
            m.setIntProperty("i", i);
            producer.send(m);
            if ((i + 1) % 100 == 0) {
               c1s1.pause();
               c0s0.pause();
               session.commit();
            }
         }
         session.commit();
      }

      ReferenceIDSupplier referenceIDSupplier = new ReferenceIDSupplier(server1);

      {
         AckManager ackManager = AckManagerProvider.getManager(server1);
         ackManager.stop();

         AtomicInteger counter = new AtomicInteger(0);

         for (long pageID = store.getFirstPage(); pageID <= store.getCurrentWritingPage(); pageID++) {
            Page page = store.usePage(pageID);
            try {
               page.getMessages().forEach(pagedMessage -> {
                  int increment = counter.incrementAndGet();
                  if (increment <= numberOfAcksC0) {
                     ackManager.addRetry(referenceIDSupplier.getServerID(pagedMessage.getMessage()), c0s0, referenceIDSupplier.getID(pagedMessage.getMessage()), AckReason.NORMAL);
                  }
                  if (increment <= numberOfAcksC1) {
                     ackManager.addRetry(referenceIDSupplier.getServerID(pagedMessage.getMessage()), c1s1, referenceIDSupplier.getID(pagedMessage.getMessage()), AckReason.NORMAL);
                  }
               });
            } finally {
               page.usageDown();
            }
         }
      }

      server1.stop();

      server1.start();


      Queue c0s0AfterRestart = server1.locateQueue(c0s0Name);
      assertNotNull(c0s0AfterRestart);
      Queue c1s1AfterRestart = server1.locateQueue(c1s1Name);
      assertNotNull(c1s1AfterRestart);

      Wait.assertEquals(numberOfMessages - numberOfAcksC1, c1s1AfterRestart::getMessageCount, 10_000);
      Wait.assertEquals(numberOfAcksC1, c1s1AfterRestart::getMessagesAcknowledged, 10_000);
      Wait.assertEquals(numberOfMessages - numberOfAcksC0, c0s0AfterRestart::getMessageCount, 10_000);
      Wait.assertEquals(numberOfAcksC0, c0s0AfterRestart::getMessagesAcknowledged, 10_000);

      server1.stop();

      assertEquals(0, AckManagerProvider.getSize());
   }




   private int getCounter(byte typeRecord, HashMap<Integer, AtomicInteger> values) {
      AtomicInteger value = values.get((int) typeRecord);
      if (value == null) {
         return 0;
      } else {
         return value.get();
      }
   }

   protected static AMQPMirrorControllerTarget locateMirrorTarget(ActiveMQServer server) {
      ActiveMQServerImpl theServer = (ActiveMQServerImpl) server;

      for (RemotingConnection connection : theServer.getRemotingService().getConnections()) {
         if (connection instanceof ActiveMQProtonRemotingConnection) {
            ActiveMQProtonRemotingConnection protonRC = (ActiveMQProtonRemotingConnection) connection;
            for (AMQPSessionContext sessionContext : protonRC.getAmqpConnection().getSessions().values()) {
               for (ProtonAbstractReceiver receiver : sessionContext.getReceivers().values()) {
                  if (receiver instanceof AMQPMirrorControllerTarget) {
                     return (AMQPMirrorControllerTarget) receiver;
                  }
               }
            }
         }
      }

      return null;
   }

   private int acksCount(File countJournalLocation) throws Exception {
      HashMap<Integer, AtomicInteger> countJournal = countJournal(countJournalLocation, 10485760, 2, 2);
      AtomicInteger acksCount = countJournal.get((int)JournalRecordIds.ACKNOWLEDGE_CURSOR);
      return acksCount != null ? acksCount.get() : 0;
   }

}
