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
package org.apache.activemq.artemis.tests.integration.paging;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientConsumerInternal;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PageCursorProvider;
import org.apache.activemq.artemis.core.paging.cursor.impl.PageCursorProviderImpl;
import org.apache.activemq.artemis.core.paging.cursor.impl.PagePositionImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreFactoryNIO;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.AckDescribe;
import org.apache.activemq.artemis.core.persistence.impl.journal.DescribeJournal;
import org.apache.activemq.artemis.core.persistence.impl.journal.DescribeJournal.ReferenceDescribe;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PagingTest extends ActiveMQTestBase {

   private static final Logger logger = Logger.getLogger(PagingTest.class);

   protected ServerLocator locator;
   protected ActiveMQServer server;
   protected ClientSessionFactory sf;
   static final int MESSAGE_SIZE = 1024; // 1k

   protected static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected static final int RECEIVE_TIMEOUT = 5000;

   protected static final int PAGE_MAX = 100 * 1024;

   protected static final int PAGE_SIZE = 10 * 1024;

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      locator = createInVMNonHALocator();
   }

   @Test
   public void testPageOnLargeMessageMultipleQueues() throws Exception {
      Configuration config = createDefaultInVMConfig();

      final int PAGE_MAX = 20 * 1024;

      final int PAGE_SIZE = 10 * 1024;

      ActiveMQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX);
      server.start();

      final int numberOfBytes = 1024;

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));

      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

      session.createQueue(ADDRESS, ADDRESS.concat("-0"), null, true);
      session.createQueue(ADDRESS, ADDRESS.concat("-1"), null, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      ClientMessage message = null;

      for (int i = 0; i < 201; i++) {
         message = session.createMessage(true);

         message.getBodyBuffer().writerIndex(0);

         message.getBodyBuffer().writeBytes(new byte[numberOfBytes]);

         for (int j = 1; j <= numberOfBytes; j++) {
            message.getBodyBuffer().writeInt(j);
         }

         producer.send(message);
      }

      session.close();

      server.stop();

      server = createServer(true, config, PAGE_SIZE, PAGE_MAX);
      server.start();

      sf = createSessionFactory(locator);

      for (int ad = 0; ad < 2; ad++) {
         session = sf.createSession(false, false, false);

         ClientConsumer consumer = session.createConsumer(ADDRESS.concat("-" + ad));

         session.start();

         for (int i = 0; i < 201; i++) {
            ClientMessage message2 = consumer.receive(10000);

            Assert.assertNotNull(message2);

            message2.acknowledge();

            Assert.assertNotNull(message2);
         }

         if (ad > -1) {
            session.commit();
         } else {
            session.rollback();
            for (int i = 0; i < 100; i++) {
               ClientMessage message2 = consumer.receive(10000);

               Assert.assertNotNull(message2);

               message2.acknowledge();

               Assert.assertNotNull(message2);
            }
            session.commit();

         }

         consumer.close();

         session.close();
      }
   }

   @Test
   public void testPageCleanup() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 5000;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }
      session.commit();
      producer.close();
      session.close();

      session = sf.createSession(false, false, false);
      producer = session.createProducer(PagingTest.ADDRESS);
      producer.send(session.createMessage(true));
      session.rollback();
      producer.close();
      session.close();

      session = sf.createSession(false, false, false);
      producer = session.createProducer(PagingTest.ADDRESS);

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }
      session.commit();
      producer.close();
      session.close();
      //System.out.println("Just sent " + numberOfMessages + " messages.");

      Queue queue = server.locateQueue(PagingTest.ADDRESS);

      session = sf.createSession(false, false, false);

      session.start();

      assertEquals(numberOfMessages * 2, getMessageCount(queue));

      // The consumer has to be created after the getMessageCount(queue) assertion
      // otherwise delivery could alter the messagecount and give us a false failure
      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);
      ClientMessage msg = null;

      for (int i = 0; i < numberOfMessages * 2; i++) {
         msg = consumer.receive(1000);
         assertNotNull(msg);
         msg.acknowledge();
         if (i % 500 == 0) {
            session.commit();
         }
      }
      session.commit();
      consumer.close();
      session.close();

      sf.close();

      locator.close();

      assertEquals(0, getMessageCount(queue));

      waitForNotPaging(queue);

      server.stop();

      HashMap<Integer, AtomicInteger> counts = countJournalLivingRecords(server.getConfiguration());

      AtomicInteger pgComplete = counts.get((int) JournalRecordIds.PAGE_CURSOR_COMPLETE);

      assertTrue(pgComplete == null || pgComplete.get() == 0);

      System.out.println("pgComplete = " + pgComplete);
   }

   // First page is complete but it wasn't deleted
   @Test
   public void testFirstPageCompleteNotDeleted() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 20;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      Queue queue = server.createQueue(ADDRESS, ADDRESS, null, true, false);

      queue.getPageSubscription().getPagingStore().startPaging();

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty("count", i);

         producer.send(message);

         if ((i + 1) % 5 == 0) {
            session.commit();
            queue.getPageSubscription().getPagingStore().forceAnotherPage();
         }
      }

      session.commit();
      producer.close();
      session.close();

      // This will make the cursor to set the page complete and not actually delete it
      queue.getPageSubscription().getPagingStore().disableCleanup();

      session = sf.createSession(false, false, false);

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      session.start();

      for (int i = 0; i < 5; i++) {
         ClientMessage msg = consumer.receive(2000);
         assertNotNull(msg);
         assertEquals(i, msg.getIntProperty("count").intValue());

         msg.individualAcknowledge();

         System.out.println(msg);
      }

      session.commit();

      session.close();

      server.stop();

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, false, false);

      consumer = session.createConsumer(ADDRESS);
      session.start();

      for (int i = 5; i < numberOfMessages; i++) {
         ClientMessage msg = consumer.receive(2000);
         assertNotNull(msg);
         assertEquals(i, msg.getIntProperty("count").intValue());
         msg.acknowledge();
         System.out.println(msg);
      }

      assertNull(consumer.receiveImmediate());
      session.commit();

      session.close();
      sf.close();
      locator.close();

   }

   @Test
   public void testPreparedACKAndRestart() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 50;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setAckBatchSize(0);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      Queue queue = server.locateQueue(PagingTest.ADDRESS);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      queue.getPageSubscription().getPagingStore().startPaging();

      forcePage(queue);

      // Send many messages, 5 on each page
      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message = session.createMessage(true);

         message.putIntProperty("count", i);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         producer.send(message);

         if ((i + 1) % 5 == 0) {
            System.out.println("Forcing at " + i);
            session.commit();
            queue.getPageSubscription().getPagingStore().forceAnotherPage();
         }
      }

      session.close();

      session = sf.createSession(true, false, false);

      Xid xidConsumeNoCommit = newXID();
      session.start(xidConsumeNoCommit, XAResource.TMNOFLAGS);

      ClientConsumer cons = session.createConsumer(ADDRESS);

      session.start();

      // First message is consumed, prepared, will be rolled back later
      ClientMessage firstMessageConsumed = cons.receive(5000);
      assertNotNull(firstMessageConsumed);
      firstMessageConsumed.acknowledge();

      session.end(xidConsumeNoCommit, XAResource.TMSUCCESS);

      session.prepare(xidConsumeNoCommit);

      Xid xidConsumeCommit = newXID();
      session.start(xidConsumeCommit, XAResource.TMNOFLAGS);

      Xid neverCommittedXID = newXID();

      for (int i = 1; i < numberOfMessages; i++) {
         if (i == 20) {
            // I elected a single message to be in prepared state, it won't ever be committed
            session.end(xidConsumeCommit, XAResource.TMSUCCESS);
            session.commit(xidConsumeCommit, true);
            session.start(neverCommittedXID, XAResource.TMNOFLAGS);
         }
         ClientMessage message = cons.receive(5000);
         assertNotNull(message);
         System.out.println("ACK " + i);
         message.acknowledge();
         assertEquals(i, message.getIntProperty("count").intValue());
         if (i == 20) {
            session.end(neverCommittedXID, XAResource.TMSUCCESS);
            session.prepare(neverCommittedXID);
            xidConsumeCommit = newXID();
            session.start(xidConsumeCommit, XAResource.TMNOFLAGS);
         }
      }

      session.end(xidConsumeCommit, XAResource.TMSUCCESS);

      session.commit(xidConsumeCommit, true);

      session.close();
      sf.close();

      // Restart the server, and we expect cleanup to not destroy any page with prepared data
      server.stop();

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      queue = server.locateQueue(ADDRESS);

      assertTrue(queue.getPageSubscription().getPagingStore().isPaging());

      producer = session.createProducer(ADDRESS);

      for (int i = numberOfMessages; i < numberOfMessages * 2; i++) {
         ClientMessage message = session.createMessage(true);

         message.putIntProperty("count", i);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         producer.send(message);

         if ((i + 1) % 5 == 0) {
            session.commit();
            queue.getPageSubscription().getPagingStore().forceAnotherPage();
         }
      }

      cons = session.createConsumer(ADDRESS);

      session.start();

      for (int i = numberOfMessages; i < numberOfMessages * 2; i++) {
         ClientMessage message = cons.receive(5000);
         assertNotNull(message);
         assertEquals(i, message.getIntProperty("count").intValue());
         message.acknowledge();
      }
      assertNull(cons.receiveImmediate());
      session.commit();

      System.out.println("count = " + getMessageCount(queue));

      session.commit();

      session.close();

      session = sf.createSession(true, false, false);

      session.rollback(xidConsumeNoCommit);

      session.start();

      xidConsumeCommit = newXID();

      session.start(xidConsumeCommit, XAResource.TMNOFLAGS);
      cons = session.createConsumer(ADDRESS);

      session.start();

      ClientMessage message = cons.receive(5000);
      assertNotNull(message);
      message.acknowledge();

      session.end(xidConsumeCommit, XAResource.TMSUCCESS);

      session.commit(xidConsumeCommit, true);

      session.close();
   }

   /**
    * @param queue
    * @throws InterruptedException
    */
   private void forcePage(Queue queue) throws InterruptedException {
      for (long timeout = System.currentTimeMillis() + 5000; timeout > System.currentTimeMillis() && !queue.getPageSubscription().getPagingStore().isPaging(); ) {
         Thread.sleep(10);
      }
      assertTrue(queue.getPageSubscription().getPagingStore().isPaging());
   }

   @Test
   public void testMoveExpire() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalDirectory(getJournalDir()).setJournalSyncNonTransactional(false).setJournalCompactMinFiles(0) // disable compact
         .setMessageExpiryScanPeriod(500);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(PAGE_SIZE).setMaxSizeBytes(PAGE_MAX).setExpiryAddress(new SimpleString("EXP")).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      server.getAddressSettingsRepository().clear();

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      server.start();

      final int numberOfMessages = 5000;

      locator = createInVMNonHALocator().setConsumerWindowSize(10 * 1024 * 1024).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, false, false);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      session.createQueue("EXP", "EXP", null, true);

      Queue queue1 = server.locateQueue(ADDRESS);
      Queue qEXP = server.locateQueue(new SimpleString("EXP"));

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      final int MESSAGE_SIZE = 1024;

      byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message = session.createMessage(true);

         if (i < 1000) {
            message.setExpiration(System.currentTimeMillis() + 1000);
         }

         message.putIntProperty("tst-count", i);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }
      session.commit();
      producer.close();

      for (long timeout = System.currentTimeMillis() + 60000; timeout > System.currentTimeMillis() && getMessageCount(qEXP) < 1000; ) {
         System.out.println("count = " + getMessageCount(qEXP));
         Thread.sleep(100);
      }

      assertEquals(1000, getMessageCount(qEXP));

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      for (int i = 0; i < numberOfMessages - 1000; i++) {
         ClientMessage message = consumer.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         assertTrue(message.getIntProperty("tst-count") >= 1000);
      }

      session.commit();

      assertNull(consumer.receiveImmediate());

      for (long timeout = System.currentTimeMillis() + 5000; timeout > System.currentTimeMillis() && getMessageCount(queue1) != 0; ) {
         Thread.sleep(100);
      }
      assertEquals(0, getMessageCount(queue1));

      consumer.close();

      consumer = session.createConsumer("EXP");

      for (int i = 0; i < 1000; i++) {
         ClientMessage message = consumer.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         assertTrue(message.getIntProperty("tst-count") < 1000);
      }

      assertNull(consumer.receiveImmediate());

      // This is just to hold some messages as being delivered
      ClientConsumerInternal cons = (ClientConsumerInternal) session.createConsumer(ADDRESS);

      session.commit();
      producer.close();
      session.close();

      server.stop();
   }

   @Test
   public void testDeleteQueueRestart() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalDirectory(getJournalDir()).setJournalSyncNonTransactional(false).setJournalCompactMinFiles(0); // disable compact

      ActiveMQServer server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 5000;

      locator = createInVMNonHALocator().setConsumerWindowSize(10 * 1024 * 1024).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      SimpleString QUEUE2 = ADDRESS.concat("-2");

      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, false, false);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      session.createQueue(PagingTest.ADDRESS, QUEUE2, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      // This is just to hold some messages as being delivered
      ClientConsumerInternal cons = (ClientConsumerInternal) session.createConsumer(ADDRESS);
      ClientConsumerInternal cons2 = (ClientConsumerInternal) session.createConsumer(QUEUE2);

      ClientMessage message = null;

      byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }
      session.commit();
      producer.close();
      session.start();

      long timeout = System.currentTimeMillis() + 30000;

      // I want the buffer full to make sure there are pending messages on the server's side
      while (System.currentTimeMillis() < timeout && (cons.getBufferSize() < 1000 || cons2.getBufferSize() < 1000)) {
         System.out.println("cons1 buffer = " + cons.getBufferSize() + ", cons2 buffer = " + cons2.getBufferSize());
         Thread.sleep(100);
      }

      assertTrue(cons.getBufferSize() >= 1000);
      assertTrue(cons2.getBufferSize() >= 1000);

      session.close();

      Queue queue = server.locateQueue(QUEUE2);

      long deletedQueueID = queue.getID();

      server.destroyQueue(QUEUE2);

      sf.close();
      locator.close();
      locator = null;
      sf = null;

      server.stop();

      final HashMap<Integer, AtomicInteger> recordsType = countJournal(config);

      for (Map.Entry<Integer, AtomicInteger> entry : recordsType.entrySet()) {
         System.out.println(entry.getKey() + "=" + entry.getValue());
      }

      assertNull("The system is acking page records instead of just delete data", recordsType.get(new Integer(JournalRecordIds.ACKNOWLEDGE_CURSOR)));

      Pair<List<RecordInfo>, List<PreparedTransactionInfo>> journalData = loadMessageJournal(config);

      HashSet<Long> deletedQueueReferences = new HashSet<>();

      for (RecordInfo info : journalData.getA()) {
         if (info.getUserRecordType() == JournalRecordIds.ADD_REF) {
            DescribeJournal.ReferenceDescribe ref = (ReferenceDescribe) DescribeJournal.newObjectEncoding(info);

            if (ref.refEncoding.queueID == deletedQueueID) {
               deletedQueueReferences.add(new Long(info.id));
            }
         } else if (info.getUserRecordType() == JournalRecordIds.ACKNOWLEDGE_REF) {
            AckDescribe ref = (AckDescribe) DescribeJournal.newObjectEncoding(info);

            if (ref.refEncoding.queueID == deletedQueueID) {
               deletedQueueReferences.remove(new Long(info.id));
            }
         }
      }

      if (!deletedQueueReferences.isEmpty()) {
         for (Long value : deletedQueueReferences) {
            System.out.println("Deleted Queue still has a reference:" + value);
         }

         fail("Deleted queue still have references");
      }

      server.start();

      locator = createInVMNonHALocator();
      locator.setConsumerWindowSize(10 * 1024 * 1024);
      sf = locator.createSessionFactory();
      session = sf.createSession(false, false, false);
      cons = (ClientConsumerInternal) session.createConsumer(ADDRESS);
      session.start();

      for (int i = 0; i < numberOfMessages; i++) {
         message = cons.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         if (i % 1000 == 0) {
            session.commit();
         }
      }
      session.commit();
      producer.close();
      session.close();

      queue = server.locateQueue(PagingTest.ADDRESS);

      assertEquals(0, getMessageCount(queue));

      timeout = System.currentTimeMillis() + 10000;
      while (timeout > System.currentTimeMillis() && queue.getPageSubscription().getPagingStore().isPaging()) {
         Thread.sleep(100);
      }
      assertFalse(queue.getPageSubscription().getPagingStore().isPaging());

      server.stop();
   }

   @Test
   public void testPreparePersistent() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 5000;

      final int numberOfTX = 10;

      final int messagesPerTX = numberOfMessages / numberOfTX;

      locator = createInVMNonHALocator();

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }
      session.commit();
      session.close();
      session = null;

      sf.close();
      locator.close();

      server.stop();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.start();

      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);

      Queue queue = server.locateQueue(ADDRESS);

      assertEquals(numberOfMessages, getMessageCount(queue));

      LinkedList<Xid> xids = new LinkedList<>();

      int msgReceived = 0;
      for (int i = 0; i < numberOfTX; i++) {
         ClientSession sessionConsumer = sf.createSession(true, false, false);
         Xid xid = newXID();
         xids.add(xid);
         sessionConsumer.start(xid, XAResource.TMNOFLAGS);
         sessionConsumer.start();
         ClientConsumer consumer = sessionConsumer.createConsumer(PagingTest.ADDRESS);
         for (int msgCount = 0; msgCount < messagesPerTX; msgCount++) {
            if (msgReceived == numberOfMessages) {
               break;
            }
            msgReceived++;
            ClientMessage msg = consumer.receive(10000);
            assertNotNull(msg);
            msg.acknowledge();
         }
         sessionConsumer.end(xid, XAResource.TMSUCCESS);
         sessionConsumer.prepare(xid);
         sessionConsumer.close();
      }

      ClientSession sessionCheck = sf.createSession(true, true);

      ClientConsumer consumer = sessionCheck.createConsumer(PagingTest.ADDRESS);

      assertNull(consumer.receiveImmediate());

      sessionCheck.close();

      assertEquals(numberOfMessages, getMessageCount(queue));

      sf.close();
      locator.close();

      server.stop();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.start();

      waitForServerToStart(server);

      queue = server.locateQueue(ADDRESS);

      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);

      session = sf.createSession(true, false, false);

      consumer = session.createConsumer(PagingTest.ADDRESS);

      session.start();

      assertEquals(numberOfMessages, getMessageCount(queue));

      ClientMessage msg = consumer.receive(5000);
      if (msg != null) {
         while (true) {
            ClientMessage msg2 = consumer.receive(1000);
            if (msg2 == null) {
               break;
            }
         }
      }
      assertNull(msg);

      for (int i = xids.size() - 1; i >= 0; i--) {
         Xid xid = xids.get(i);
         session.rollback(xid);
      }

      xids.clear();

      session.close();

      session = sf.createSession(false, false, false);

      session.start();

      consumer = session.createConsumer(PagingTest.ADDRESS);

      for (int i = 0; i < numberOfMessages; i++) {
         msg = consumer.receive(1000);
         assertNotNull(msg);
         msg.acknowledge();

         assertEquals(i, msg.getIntProperty("id").intValue());

         if (i % 500 == 0) {
            session.commit();
         }
      }

      session.commit();

      session.close();

      sf.close();

      locator.close();

      assertEquals(0, getMessageCount(queue));

      waitForNotPaging(queue);
   }

   @Test
   public void testSendOverBlockingNoFlowControl() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.getAddressSettingsRepository().getMatch("#").setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      server.start();

      final int biggerMessageSize = 10 * 1024;

      final int numberOfMessages = 500;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setProducerWindowSize(-1).setMinLargeMessageSize(1024 * 1024);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[biggerMessageSize];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= biggerMessageSize; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);

         if (i % 10 == 0) {
            session.commit();
         }
      }
      session.commit();

      session.start();

      ClientConsumer cons = session.createConsumer(ADDRESS);

      for (int i = 0; i < numberOfMessages; i++) {
         message = cons.receive(5000);
         assertNotNull(message);
         message.acknowledge();

         if (i % 10 == 0) {
            session.commit();
         }
      }

      session.commit();

   }

   @Test
   public void testReceiveImmediate() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 1000;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }
      session.commit();
      session.close();

      session = null;

      sf.close();
      locator.close();

      server.stop();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.start();

      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);

      Queue queue = server.locateQueue(ADDRESS);

      assertEquals(numberOfMessages, getMessageCount(queue));

      int msgReceived = 0;
      ClientSession sessionConsumer = sf.createSession(false, false, false);
      sessionConsumer.start();
      ClientConsumer consumer = sessionConsumer.createConsumer(PagingTest.ADDRESS);
      for (int msgCount = 0; msgCount < numberOfMessages; msgCount++) {
         log.info("Received " + msgCount);
         msgReceived++;
         ClientMessage msg = consumer.receiveImmediate();
         if (msg == null) {
            log.info("It's null. leaving now");
            sessionConsumer.commit();
            fail("Didn't receive a message");
         }
         msg.acknowledge();

         if (msgCount % 5 == 0) {
            log.info("commit");
            sessionConsumer.commit();
         }
      }

      sessionConsumer.commit();

      sessionConsumer.close();

      sf.close();

      locator.close();

      assertEquals(0, getMessageCount(queue));

      long timeout = System.currentTimeMillis() + 5000;
      while (timeout > System.currentTimeMillis() && queue.getPageSubscription().getPagingStore().isPaging()) {
         Thread.sleep(100);
      }
      assertFalse(queue.getPageSubscription().getPagingStore().isPaging());

   }

   /**
    * This test will remove all the page directories during a restart, simulating a crash scenario. The server should still start after this
    */
   @Test
   public void testDeletePhysicalPages() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setPersistDeliveryCountBeforeDelivery(true);

      config.setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 1000;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }
      session.commit();
      session.close();

      session = null;

      sf.close();
      locator.close();

      server.stop();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.start();

      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);

      Queue queue = server.locateQueue(ADDRESS);

      assertEquals(numberOfMessages, getMessageCount(queue));

      int msgReceived = 0;
      ClientSession sessionConsumer = sf.createSession(false, false, false);
      sessionConsumer.start();
      ClientConsumer consumer = sessionConsumer.createConsumer(PagingTest.ADDRESS);
      for (int msgCount = 0; msgCount < numberOfMessages; msgCount++) {
         log.info("Received " + msgCount);
         msgReceived++;
         ClientMessage msg = consumer.receiveImmediate();
         if (msg == null) {
            log.info("It's null. leaving now");
            sessionConsumer.commit();
            fail("Didn't receive a message");
         }
         msg.acknowledge();

         if (msgCount % 5 == 0) {
            log.info("commit");
            sessionConsumer.commit();
         }
      }

      sessionConsumer.commit();

      sessionConsumer.close();

      sf.close();

      locator.close();

      assertEquals(0, getMessageCount(queue));

      long timeout = System.currentTimeMillis() + 5000;
      while (timeout > System.currentTimeMillis() && queue.getPageSubscription().getPagingStore().isPaging()) {
         Thread.sleep(100);
      }
      assertFalse(queue.getPageSubscription().getPagingStore().isPaging());

      server.stop();

      // Deleting the paging data. Simulating a failure
      // a dumb user, or anything that will remove the data
      deleteDirectory(new File(getPageDir()));

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.start();

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      queue = server.locateQueue(ADDRESS);

      sf = createSessionFactory(locator);
      session = sf.createSession(false, false, false);

      producer = session.createProducer(PagingTest.ADDRESS);

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }

      session.commit();

      server.stop();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.start();

      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);

      queue = server.locateQueue(ADDRESS);

      // assertEquals(numberOfMessages, getMessageCount(queue));

      msgReceived = 0;
      sessionConsumer = sf.createSession(false, false, false);
      sessionConsumer.start();
      consumer = sessionConsumer.createConsumer(PagingTest.ADDRESS);
      for (int msgCount = 0; msgCount < numberOfMessages; msgCount++) {
         log.info("Received " + msgCount);
         msgReceived++;
         ClientMessage msg = consumer.receiveImmediate();
         if (msg == null) {
            log.info("It's null. leaving now");
            sessionConsumer.commit();
            fail("Didn't receive a message");
         }
         msg.acknowledge();

         if (msgCount % 5 == 0) {
            log.info("commit");
            sessionConsumer.commit();
         }
      }

      sessionConsumer.commit();

      sessionConsumer.close();

   }

   @Test
   public void testMissingTXEverythingAcked() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 5000;

      final int numberOfTX = 10;

      final int messagesPerTX = numberOfMessages / numberOfTX;

      try {
         locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

         sf = createSessionFactory(locator);

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(ADDRESS.toString(), "q1", true);

         session.createQueue(ADDRESS.toString(), "q2", true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[MESSAGE_SIZE];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= MESSAGE_SIZE; j++) {
            bb.put(getSamplebyte(j));
         }

         for (int i = 0; i < numberOfMessages; i++) {
            message = session.createMessage(true);

            ActiveMQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
            if (i % messagesPerTX == 0) {
               session.commit();
            }
         }
         session.commit();
         session.close();
      } finally {
         try {
            server.stop();
         } catch (Throwable ignored) {
         }
      }

      ArrayList<RecordInfo> records = new ArrayList<>();

      List<PreparedTransactionInfo> list = new ArrayList<>();

      JournalImpl jrn = new JournalImpl(config.getJournalFileSize(), 2, 2, 0, 0, new NIOSequentialFileFactory(server.getConfiguration().getJournalLocation(), 1), "activemq-data", "amq", 1);
      jrn.start();
      jrn.load(records, list, null);

      // Delete everything from the journal
      for (RecordInfo info : records) {
         if (!info.isUpdate && info.getUserRecordType() != JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE &&
            info.getUserRecordType() != JournalRecordIds.PAGE_CURSOR_COUNTER_INC &&
            info.getUserRecordType() != JournalRecordIds.PAGE_CURSOR_COMPLETE) {
            jrn.appendDeleteRecord(info.id, false);
         }
      }

      jrn.stop();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      ClientSessionFactory csf = createSessionFactory(locator);

      ClientSession sess = csf.createSession();

      sess.start();

      ClientConsumer cons = sess.createConsumer("q1");

      assertNull(cons.receiveImmediate());

      ClientConsumer cons2 = sess.createConsumer("q2");
      assertNull(cons2.receiveImmediate());

      Queue q1 = server.locateQueue(new SimpleString("q1"));
      Queue q2 = server.locateQueue(new SimpleString("q2"));

      q1.getPageSubscription().cleanupEntries(false);
      q2.getPageSubscription().cleanupEntries(false);

      PageCursorProvider provider = q1.getPageSubscription().getPagingStore().getCursorProvider();
      provider.cleanup();

      waitForNotPaging(q1);

      sess.close();
   }

   @Test
   public void testMissingTXEverythingAcked2() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 6;

      final int numberOfTX = 2;

      final int messagesPerTX = numberOfMessages / numberOfTX;

      try {
         locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

         sf = createSessionFactory(locator);

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(ADDRESS.toString(), "q1", true);

         session.createQueue(ADDRESS.toString(), "q2", true);

         server.getPagingManager().getPageStore(ADDRESS).startPaging();

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[MESSAGE_SIZE];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= MESSAGE_SIZE; j++) {
            bb.put(getSamplebyte(j));
         }

         for (int i = 0; i < numberOfMessages; i++) {
            message = session.createMessage(true);

            ActiveMQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putStringProperty("id", "str-" + i);

            producer.send(message);
            if ((i + 1) % messagesPerTX == 0) {
               session.commit();
            }
         }
         session.commit();

         session.start();

         for (int i = 1; i <= 2; i++) {
            ClientConsumer cons = session.createConsumer("q" + i);

            for (int j = 0; j < 3; j++) {
               ClientMessage msg = cons.receive(5000);

               assertNotNull(msg);

               assertEquals("str-" + j, msg.getStringProperty("id"));

               msg.acknowledge();
            }

            session.commit();

         }

         session.close();
      } finally {
         locator.close();
         try {
            server.stop();
         } catch (Throwable ignored) {
         }
      }

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      locator = createInVMNonHALocator();

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      ClientSessionFactory csf = createSessionFactory(locator);

      ClientSession session = csf.createSession();

      session.start();

      for (int i = 1; i <= 2; i++) {
         ClientConsumer cons = session.createConsumer("q" + i);

         for (int j = 3; j < 6; j++) {
            ClientMessage msg = cons.receive(5000);

            assertNotNull(msg);

            assertEquals("str-" + j, msg.getStringProperty("id"));

            msg.acknowledge();
         }

         session.commit();
         assertNull(cons.receive(500));

      }

      session.close();

      long timeout = System.currentTimeMillis() + 5000;

      while (System.currentTimeMillis() < timeout && server.getPagingManager().getPageStore(ADDRESS).isPaging()) {
         Thread.sleep(100);
      }
   }

   @Test
   public void testTwoQueuesOneNoRouting() throws Exception {
      boolean persistentMessages = true;

      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 1000;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);
      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS.concat("-invalid"), new SimpleString(ActiveMQServerImpl.GENERIC_IGNORED_FILTER), true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[MESSAGE_SIZE];

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(persistentMessages);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }

      session.commit();

      session.start();

      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

      for (int i = 0; i < numberOfMessages; i++) {
         message = consumer.receive(5000);
         assertNotNull(message);
         message.acknowledge();

         assertEquals(i, message.getIntProperty("id").intValue());
         if (i % 1000 == 0) {
            session.commit();
         }
      }

      session.commit();

      session.commit();

      session.commit();

      PagingStore store = server.getPagingManager().getPageStore(ADDRESS);
      store.getCursorProvider().cleanup();

      long timeout = System.currentTimeMillis() + 5000;
      while (store.isPaging() && timeout > System.currentTimeMillis()) {
         Thread.sleep(100);
      }

      // It's async, so need to wait a bit for it happening
      assertFalse(server.getPagingManager().getPageStore(ADDRESS).isPaging());
   }

   @Test
   public void testSendReceivePagingPersistent() throws Exception {
      internaltestSendReceivePaging(true);
   }

   @Test
   public void testSendReceivePagingNonPersistent() throws Exception {
      internaltestSendReceivePaging(false);
   }

   @Test
   public void testWithDiverts() throws Exception {
      internalMultiQueuesTest(true);
   }

   @Test
   public void testWithMultiQueues() throws Exception {
      internalMultiQueuesTest(false);
   }

   public void internalMultiQueuesTest(final boolean divert) throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      if (divert) {
         DivertConfiguration divert1 = new DivertConfiguration().setName("dv1").setRoutingName("nm1").setAddress(PagingTest.ADDRESS.toString()).setForwardingAddress(PagingTest.ADDRESS.toString() + "-1").setExclusive(true);

         config.addDivertConfiguration(divert1);

         DivertConfiguration divert2 = new DivertConfiguration().setName("dv2").setRoutingName("nm2").setAddress(PagingTest.ADDRESS.toString()).setForwardingAddress(PagingTest.ADDRESS.toString() + "-2").setExclusive(true);

         config.addDivertConfiguration(divert2);
      }

      server.start();

      final int numberOfMessages = 3000;

      final byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      final AtomicBoolean running = new AtomicBoolean(true);

      class TCount extends Thread {

         Queue queue;

         TCount(Queue queue) {
            this.queue = queue;
         }

         @Override
         public void run() {
            try {
               while (running.get()) {
                  // this will be overusing what some users do. flush / getCount
                  getMessagesAdded(queue);
                  getMessageCount(queue);
                  Thread.sleep(10);
               }
            } catch (InterruptedException e) {
               log.info("Thread interrupted");
            }
         }
      }

      TCount tcount1 = null;
      TCount tcount2 = null;

      try {
         {
            locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

            sf = createSessionFactory(locator);

            ClientSession session = sf.createSession(false, false, false);

            if (divert) {
               session.createQueue(PagingTest.ADDRESS + "-1", PagingTest.ADDRESS + "-1", null, true);

               session.createQueue(PagingTest.ADDRESS + "-2", PagingTest.ADDRESS + "-2", null, true);
            } else {
               session.createQueue(PagingTest.ADDRESS.toString(), PagingTest.ADDRESS + "-1", null, true);

               session.createQueue(PagingTest.ADDRESS.toString(), PagingTest.ADDRESS + "-2", null, true);
            }

            ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

            ClientMessage message = null;

            for (int i = 0; i < numberOfMessages; i++) {
               if (i % 500 == 0) {
                  log.info("Sent " + i + " messages");
                  session.commit();
               }
               message = session.createMessage(true);

               ActiveMQBuffer bodyLocal = message.getBodyBuffer();

               bodyLocal.writeBytes(body);

               message.putIntProperty(new SimpleString("id"), i);

               producer.send(message);
            }

            session.commit();

            session.close();

            server.stop();

            sf.close();
            locator.close();
         }

         server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
         server.start();

         Queue queue1 = server.locateQueue(PagingTest.ADDRESS.concat("-1"));

         Queue queue2 = server.locateQueue(PagingTest.ADDRESS.concat("-2"));

         assertNotNull(queue1);

         assertNotNull(queue2);

         assertNotSame(queue1, queue2);

         tcount1 = new TCount(queue1);

         tcount2 = new TCount(queue2);

         tcount1.start();
         tcount2.start();

         locator = createInVMNonHALocator();
         final ClientSessionFactory sf2 = createSessionFactory(locator);

         final AtomicInteger errors = new AtomicInteger(0);

         Thread[] threads = new Thread[2];

         for (int start = 1; start <= 2; start++) {

            final String addressToSubscribe = PagingTest.ADDRESS + "-" + start;

            threads[start - 1] = new Thread() {
               @Override
               public void run() {
                  try {
                     ClientSession session = sf2.createSession(null, null, false, true, true, false, 0);

                     ClientConsumer consumer = session.createConsumer(addressToSubscribe);

                     session.start();

                     for (int i = 0; i < numberOfMessages; i++) {
                        ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

                        Assert.assertNotNull(message2);

                        Assert.assertEquals(i, message2.getIntProperty("id").intValue());

                        message2.acknowledge();

                        Assert.assertNotNull(message2);

                        if (i % 100 == 0) {
                           if (i % 5000 == 0) {
                              log.info(addressToSubscribe + " consumed " + i + " messages");
                           }
                           session.commit();
                        }

                        try {
                           assertBodiesEqual(body, message2.getBodyBuffer());
                        } catch (AssertionError e) {
                           PagingTest.log.info("Expected buffer:" + ActiveMQTestBase.dumpBytesHex(body, 40));
                           PagingTest.log.info("Arriving buffer:" + ActiveMQTestBase.dumpBytesHex(message2.getBodyBuffer().toByteBuffer().array(), 40));
                           throw e;
                        }
                     }

                     session.commit();

                     consumer.close();

                     session.close();
                  } catch (Throwable e) {
                     e.printStackTrace();
                     errors.incrementAndGet();
                  }

               }
            };
         }

         for (int i = 0; i < 2; i++) {
            threads[i].start();
         }

         for (int i = 0; i < 2; i++) {
            threads[i].join();
         }

         sf2.close();
         locator.close();

         assertEquals(0, errors.get());

         for (int i = 0; i < 20 && server.getPagingManager().getTransactions().size() != 0; i++) {
            if (server.getPagingManager().getTransactions().size() != 0) {
               // The delete may be asynchronous, giving some time case it eventually happen asynchronously
               Thread.sleep(500);
            }
         }

         assertEquals(0, server.getPagingManager().getTransactions().size());

      } finally {
         running.set(false);

         if (tcount1 != null) {
            tcount1.interrupt();
            tcount1.join();
         }

         if (tcount2 != null) {
            tcount2.interrupt();
            tcount2.join();
         }

         try {
            server.stop();
         } catch (Throwable ignored) {
         }
      }

   }

   @Test
   public void testMultiQueuesNonPersistentAndPersistent() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 3000;

      final byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      {
         locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

         sf = createSessionFactory(locator);

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(PagingTest.ADDRESS.toString(), PagingTest.ADDRESS + "-1", null, true);

         session.createQueue(PagingTest.ADDRESS.toString(), PagingTest.ADDRESS + "-2", null, false);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++) {
            if (i % 500 == 0) {
               session.commit();
            }
            message = session.createMessage(true);

            ActiveMQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.commit();

         session.close();

         server.stop();

         sf.close();
         locator.close();
      }

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.start();

      ServerLocator locator1 = createInVMNonHALocator();
      final ClientSessionFactory sf2 = locator1.createSessionFactory();

      final AtomicInteger errors = new AtomicInteger(0);

      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               ClientSession session = sf2.createSession(null, null, false, true, true, false, 0);

               ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS + "-1");

               session.start();

               for (int i = 0; i < numberOfMessages; i++) {
                  ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

                  Assert.assertNotNull(message2);

                  Assert.assertEquals(i, message2.getIntProperty("id").intValue());

                  message2.acknowledge();

                  Assert.assertNotNull(message2);

                  if (i % 1000 == 0) {
                     session.commit();
                  }

                  try {
                     assertBodiesEqual(body, message2.getBodyBuffer());
                  } catch (AssertionError e) {
                     PagingTest.log.info("Expected buffer:" + ActiveMQTestBase.dumpBytesHex(body, 40));
                     PagingTest.log.info("Arriving buffer:" + ActiveMQTestBase.dumpBytesHex(message2.getBodyBuffer().toByteBuffer().array(), 40));
                     throw e;
                  }
               }

               session.commit();

               consumer.close();

               session.close();
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }

         }
      };

      t.start();
      t.join();

      assertEquals(0, errors.get());

      for (int i = 0; i < 20 && server.getPagingManager().getPageStore(ADDRESS).isPaging(); i++) {
         // The delete may be asynchronous, giving some time case it eventually happen asynchronously
         Thread.sleep(500);
      }

      assertFalse(server.getPagingManager().getPageStore(ADDRESS).isPaging());

      for (int i = 0; i < 20 && server.getPagingManager().getTransactions().size() != 0; i++) {
         // The delete may be asynchronous, giving some time case it eventually happen asynchronously
         Thread.sleep(500);
      }

      assertEquals(0, server.getPagingManager().getTransactions().size());

   }

   private void internaltestSendReceivePaging(final boolean persistentMessages) throws Exception {

      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 1000;
      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      Queue queue = server.locateQueue(ADDRESS);
      queue.getPageSubscription().getPagingStore().startPaging();

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[numberOfIntegers * 4];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= numberOfIntegers; j++) {
         bb.putInt(j);
      }

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(persistentMessages);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
      }

      session.close();
      sf.close();
      locator.close();

      server.stop();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.start();

      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);

      session = sf.createSession(null, null, false, true, true, false, 0);

      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

      session.start();

      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

         Assert.assertNotNull(message2);

         Assert.assertEquals(i, message2.getIntProperty("id").intValue());

         assertEquals(body.length, message2.getBodySize());

         message2.acknowledge();

         Assert.assertNotNull(message2);

         if (i % 1000 == 0) {
            session.commit();
         }

         try {
            assertBodiesEqual(body, message2.getBodyBuffer());
         } catch (AssertionError e) {
            PagingTest.log.info("Expected buffer:" + ActiveMQTestBase.dumpBytesHex(body, 40));
            PagingTest.log.info("Arriving buffer:" + ActiveMQTestBase.dumpBytesHex(message2.getBodyBuffer().toByteBuffer().array(), 40));
            throw e;
         }
      }

      consumer.close();

      session.close();
   }

   private void assertBodiesEqual(final byte[] body, final ActiveMQBuffer buffer) {
      byte[] other = new byte[body.length];

      buffer.readBytes(other);

      ActiveMQTestBase.assertEqualsByteArrays(body, other);
   }

   /**
    * - Make a destination in page mode
    * - Add stuff to a transaction
    * - Consume the entire destination (not in page mode any more)
    * - Add stuff to a transaction again
    * - Check order
    */
   @Test
   public void testDepageDuringTransaction() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      byte[] body = new byte[MESSAGE_SIZE];
      // ActiveMQBuffer bodyLocal = ActiveMQChannelBuffers.buffer(DataConstants.SIZE_INT * numberOfIntegers);

      ClientMessage message = null;

      int numberOfMessages = 0;
      while (true) {
         message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(body);

         // Stop sending message as soon as we start paging
         if (server.getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging()) {
            break;
         }
         numberOfMessages++;

         producer.send(message);
      }

      Assert.assertTrue(server.getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging());

      session.start();

      ClientSession sessionTransacted = sf.createSession(null, null, false, false, false, false, 0);

      ClientProducer producerTransacted = sessionTransacted.createProducer(PagingTest.ADDRESS);

      for (int i = 0; i < 10; i++) {
         message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(body);
         message.putIntProperty(new SimpleString("id"), i);

         // Consume messages to force an eventual out of order delivery
         if (i == 5) {
            ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);
            for (int j = 0; j < numberOfMessages; j++) {
               ClientMessage msg = consumer.receive(PagingTest.RECEIVE_TIMEOUT);
               msg.acknowledge();
               Assert.assertNotNull(msg);
            }

            Assert.assertNull(consumer.receiveImmediate());
            consumer.close();
         }

         Integer messageID = (Integer) message.getObjectProperty(new SimpleString("id"));
         Assert.assertNotNull(messageID);
         Assert.assertEquals(messageID.intValue(), i);

         producerTransacted.send(message);
      }

      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

      Assert.assertNull(consumer.receiveImmediate());

      sessionTransacted.commit();

      sessionTransacted.close();

      for (int i = 0; i < 10; i++) {
         message = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

         Assert.assertNotNull(message);

         Integer messageID = (Integer) message.getObjectProperty(new SimpleString("id"));

         Assert.assertNotNull(messageID);
         Assert.assertEquals("message received out of order", messageID.intValue(), i);

         message.acknowledge();
      }

      Assert.assertNull(consumer.receiveImmediate());

      consumer.close();

      session.close();
   }

   /**
    * - Make a destination in page mode
    * - Add stuff to a transaction
    * - Consume the entire destination (not in page mode any more)
    * - Add stuff to a transaction again
    * - Check order
    * <br>
    * Test under discussion at : http://community.jboss.org/thread/154061?tstart=0
    */
   @Test
   public void testDepageDuringTransaction2() throws Exception {
      boolean IS_DURABLE_MESSAGE = true;
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      byte[] body = new byte[MESSAGE_SIZE];

      ClientSession sessionTransacted = sf.createSession(null, null, false, false, false, false, 0);
      ClientProducer producerTransacted = sessionTransacted.createProducer(PagingTest.ADDRESS);

      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);
      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientMessage firstMessage = sessionTransacted.createMessage(IS_DURABLE_MESSAGE);
      firstMessage.getBodyBuffer().writeBytes(body);
      firstMessage.putIntProperty(new SimpleString("id"), 0);

      producerTransacted.send(firstMessage);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      int numberOfMessages = 0;
      while (true) {
         System.out.println("Sending message " + numberOfMessages);
         message = session.createMessage(IS_DURABLE_MESSAGE);
         message.getBodyBuffer().writeBytes(body);
         message.putIntProperty("id", numberOfMessages);
         message.putBooleanProperty("new", false);

         // Stop sending message as soon as we start paging
         if (server.getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging()) {
            break;
         }
         numberOfMessages++;

         producer.send(message);
      }

      Assert.assertTrue(server.getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging());

      session.start();

      for (int i = 1; i < 10; i++) {
         message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(body);
         message.putIntProperty(new SimpleString("id"), i);

         // Consume messages to force an eventual out of order delivery
         if (i == 5) {
            ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);
            for (int j = 0; j < numberOfMessages; j++) {
               ClientMessage msg = consumer.receive(PagingTest.RECEIVE_TIMEOUT);
               msg.acknowledge();
               assertEquals(j, msg.getIntProperty("id").intValue());
               assertFalse(msg.getBooleanProperty("new"));
               Assert.assertNotNull(msg);
            }

            ClientMessage msgReceived = consumer.receiveImmediate();

            Assert.assertNull(msgReceived);
            consumer.close();
         }

         Integer messageID = (Integer) message.getObjectProperty(new SimpleString("id"));
         Assert.assertNotNull(messageID);
         Assert.assertEquals(messageID.intValue(), i);

         producerTransacted.send(message);
      }

      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

      Assert.assertNull(consumer.receiveImmediate());

      sessionTransacted.commit();

      sessionTransacted.close();

      for (int i = 0; i < 10; i++) {
         message = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

         Assert.assertNotNull(message);

         Integer messageID = (Integer) message.getObjectProperty(new SimpleString("id"));

         // System.out.println(messageID);
         Assert.assertNotNull(messageID);
         Assert.assertEquals("message received out of order", i, messageID.intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer.receiveImmediate());

      consumer.close();

      session.close();

   }

   @Test
   public void testDepageDuringTransaction3() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      byte[] body = new byte[MESSAGE_SIZE];

      ClientSession sessionTransacted = sf.createSession(null, null, false, false, false, false, 0);
      ClientProducer producerTransacted = sessionTransacted.createProducer(PagingTest.ADDRESS);

      ClientSession sessionNonTX = sf.createSession(true, true, 0);
      sessionNonTX.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producerNonTransacted = sessionNonTX.createProducer(PagingTest.ADDRESS);

      sessionNonTX.start();

      for (int i = 0; i < 50; i++) {
         ClientMessage message = sessionNonTX.createMessage(true);
         message.getBodyBuffer().writeBytes(body);
         message.putIntProperty(new SimpleString("id"), i);
         message.putStringProperty(new SimpleString("tst"), new SimpleString("i=" + i));

         producerTransacted.send(message);

         if (i % 2 == 0) {
            for (int j = 0; j < 20; j++) {
               ClientMessage msgSend = sessionNonTX.createMessage(true);
               msgSend.putStringProperty(new SimpleString("tst"), new SimpleString("i=" + i + ", j=" + j));
               msgSend.getBodyBuffer().writeBytes(new byte[10 * 1024]);
               producerNonTransacted.send(msgSend);
            }
            assertTrue(server.getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging());
         } else {
            ClientConsumer consumer = sessionNonTX.createConsumer(PagingTest.ADDRESS);
            for (int j = 0; j < 20; j++) {
               ClientMessage msgReceived = consumer.receive(10000);
               assertNotNull(msgReceived);
               msgReceived.acknowledge();
            }
            consumer.close();
         }
      }

      ClientConsumer consumerNonTX = sessionNonTX.createConsumer(PagingTest.ADDRESS);
      while (true) {
         ClientMessage msgReceived = consumerNonTX.receive(1000);
         if (msgReceived == null) {
            break;
         }
         msgReceived.acknowledge();
      }
      consumerNonTX.close();

      ClientConsumer consumer = sessionNonTX.createConsumer(PagingTest.ADDRESS);

      Assert.assertNull(consumer.receiveImmediate());

      sessionTransacted.commit();

      sessionTransacted.close();

      for (int i = 0; i < 50; i++) {
         ClientMessage message = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

         Assert.assertNotNull(message);

         Integer messageID = (Integer) message.getObjectProperty(new SimpleString("id"));

         Assert.assertNotNull(messageID);
         Assert.assertEquals("message received out of order", i, messageID.intValue());

         message.acknowledge();
      }

      Assert.assertNull(consumer.receiveImmediate());

      consumer.close();

      sessionNonTX.close();
   }

   @Test
   public void testDepageDuringTransaction4() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false).setJournalSyncTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final AtomicInteger errors = new AtomicInteger(0);

      final int numberOfMessages = 10000;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(false);

      sf = createSessionFactory(locator);

      final byte[] body = new byte[MESSAGE_SIZE];

      Thread producerThread = new Thread() {
         @Override
         public void run() {
            ClientSession sessionProducer = null;
            try {
               sessionProducer = sf.createSession(false, false);
               ClientProducer producer = sessionProducer.createProducer(ADDRESS);

               for (int i = 0; i < numberOfMessages; i++) {
                  ClientMessage msg = sessionProducer.createMessage(true);
                  msg.getBodyBuffer().writeBytes(body);
                  msg.putIntProperty("count", i);
                  producer.send(msg);

                  if (i % 100 == 0 && i != 0) {
                     sessionProducer.commit();
                     // Thread.sleep(500);
                  }
               }

               sessionProducer.commit();

            } catch (Throwable e) {
               e.printStackTrace(); // >> junit report
               errors.incrementAndGet();
            } finally {
               try {
                  if (sessionProducer != null) {
                     sessionProducer.close();
                  }
               } catch (Throwable e) {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }
            }
         }
      };

      ClientSession session = sf.createSession(true, true, 0);
      session.start();
      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      producerThread.start();

      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         assertEquals(i, msg.getIntProperty("count").intValue());
         msg.acknowledge();
         if (i > 0 && i % 10 == 0) {
            session.commit();
         }
      }
      session.commit();

      session.close();

      producerThread.join();

      locator.close();

      sf.close();

      assertEquals(0, errors.get());
   }

   @Test
   public void testOrderingNonTX() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false).setJournalSyncTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_SIZE * 2);

      server.getConfiguration();
      server.getConfiguration();

      server.start();

      final AtomicInteger errors = new AtomicInteger(0);

      final int numberOfMessages = 2000;

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      final CountDownLatch ready = new CountDownLatch(1);

      final byte[] body = new byte[MESSAGE_SIZE];

      Thread producerThread = new Thread() {
         @Override
         public void run() {
            ClientSession sessionProducer = null;
            try {
               sessionProducer = sf.createSession(true, true);
               ClientProducer producer = sessionProducer.createProducer(ADDRESS);

               for (int i = 0; i < numberOfMessages; i++) {
                  ClientMessage msg = sessionProducer.createMessage(true);
                  msg.getBodyBuffer().writeBytes(body);
                  msg.putIntProperty("count", i);
                  producer.send(msg);

                  if (i == 1000) {
                     // The session is not TX, but we do this just to perform a round trip to the server
                     // and make sure there are no pending messages
                     sessionProducer.commit();

                     assertTrue(server.getPagingManager().getPageStore(ADDRESS).isPaging());
                     ready.countDown();
                  }
               }

               sessionProducer.commit();

               log.info("Producer gone");

            } catch (Throwable e) {
               e.printStackTrace(); // >> junit report
               errors.incrementAndGet();
            } finally {
               try {
                  if (sessionProducer != null) {
                     sessionProducer.close();
                  }
               } catch (Throwable e) {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }
            }
         }
      };

      ClientSession session = sf.createSession(true, true, 0);
      session.start();
      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      producerThread.start();

      assertTrue(ready.await(100, TimeUnit.SECONDS));

      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         if (i != msg.getIntProperty("count").intValue()) {
            log.info("Received " + i + " with property = " + msg.getIntProperty("count"));
            log.info("###### different");
         }
         // assertEquals(i, msg.getIntProperty("count").intValue());
         msg.acknowledge();
      }

      session.close();

      producerThread.join();

      assertEquals(0, errors.get());
   }

   @Test
   public void testPageOnSchedulingNoRestart() throws Exception {
      internalTestPageOnScheduling(false);
   }

   @Test
   public void testPageOnSchedulingRestart() throws Exception {
      internalTestPageOnScheduling(true);
   }

   public void internalTestPageOnScheduling(final boolean restart) throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 1000;

      final int numberOfBytes = 1024;

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[numberOfBytes];

      for (int j = 0; j < numberOfBytes; j++) {
         body[j] = ActiveMQTestBase.getSamplebyte(j);
      }

      long scheduledTime = System.currentTimeMillis() + 5000;

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         message.getBodyBuffer().writeBytes(body);
         message.putIntProperty(new SimpleString("id"), i);

         PagingStore store = server.getPagingManager().getPageStore(PagingTest.ADDRESS);

         // Worse scenario possible... only schedule what's on pages
         if (store.getCurrentPage() != null) {
            message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, scheduledTime);
         }

         producer.send(message);
      }

      if (restart) {
         session.close();

         server.stop();

         server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
         server.start();

         sf = createSessionFactory(locator);

         session = sf.createSession(null, null, false, true, true, false, 0);
      }

      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

      session.start();

      for (int i = 0; i < numberOfMessages; i++) {

         ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

         Assert.assertNotNull(message2);

         message2.acknowledge();

         Assert.assertNotNull(message2);

         Long scheduled = (Long) message2.getObjectProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
         if (scheduled != null) {
            Assert.assertTrue("Scheduling didn't work", System.currentTimeMillis() >= scheduledTime);
         }

         try {
            assertBodiesEqual(body, message2.getBodyBuffer());
         } catch (AssertionError e) {
            PagingTest.log.info("Expected buffer:" + ActiveMQTestBase.dumpBytesHex(body, 40));
            PagingTest.log.info("Arriving buffer:" + ActiveMQTestBase.dumpBytesHex(message2.getBodyBuffer().toByteBuffer().array(), 40));
            throw e;
         }
      }

      consumer.close();

      session.close();
   }

   @Test
   public void testRollbackOnSend() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 10;

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, false, true, false, 0);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         for (int j = 1; j <= numberOfIntegers; j++) {
            bodyLocal.writeInt(j);
         }

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
      }

      session.rollback();

      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

      session.start();

      Assert.assertNull(consumer.receiveImmediate());

      session.close();
   }

   @Test
   public void testRollbackOnSendThenSendMore() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, false, true, false, 0);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      Queue queue = server.locateQueue(ADDRESS);

      queue.getPageSubscription().getPagingStore().startPaging();

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message;

      for (int i = 0; i < 20; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(new byte[100 * 4]);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
         session.commit();
         queue.getPageSubscription().getPagingStore().forceAnotherPage();

      }

      for (int i = 20; i < 24; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(new byte[100 * 4]);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
      }

      session.rollback();

      ClientSession consumerSession = sf.createSession(false, false);

      queue.getPageSubscription().getPagingStore().disableCleanup();

      queue.getPageSubscription().getPagingStore().getCursorProvider().cleanup();

      consumerSession.start();
      ClientConsumer consumer = consumerSession.createConsumer(ADDRESS, SimpleString.toSimpleString("id > 0"));
      for (int i = 0; i < 19; i++) {
         ClientMessage messageRec = consumer.receive(5000);
         System.err.println("msg::" + messageRec);
         Assert.assertNotNull(messageRec);
         messageRec.acknowledge();
         consumerSession.commit();

         // The only reason I'm calling cleanup directly is that it would be easy to debug in case of bugs
         // if you see an issue with cleanup here, enjoy debugging this method
         queue.getPageSubscription().getPagingStore().getCursorProvider().cleanup();
      }
      queue.getPageSubscription().getPagingStore().enableCleanup();

      consumerSession.close();

      session.close();
      sf.close();

      server.stop();
   }

   // The pages are complete, and this is simulating a scenario where the server crashed before deleting the pages.
   @Test
   public void testRestartWithComplete() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig();

      final AtomicBoolean mainCleanup = new AtomicBoolean(true);

      class InterruptedCursorProvider extends PageCursorProviderImpl {

         InterruptedCursorProvider(PagingStore pagingStore,
                                   StorageManager storageManager,
                                   Executor executor,
                                   int maxCacheSize) {
            super(pagingStore, storageManager, executor, maxCacheSize);
         }

         @Override
         public void cleanup() {
            if (mainCleanup.get()) {
               super.cleanup();
            } else {
               try {
                  pagingStore.unlock();
               } catch (Throwable ignored) {
               }
            }
         }
      }

      server = new ActiveMQServerImpl(config, ManagementFactory.getPlatformMBeanServer(), new ActiveMQSecurityManagerImpl()) {
         @Override
         protected PagingStoreFactoryNIO getPagingStoreFactory() {
            return new PagingStoreFactoryNIO(this.getStorageManager(), this.getConfiguration().getPagingLocation(), this.getConfiguration().getJournalBufferTimeout_NIO(), this.getScheduledPool(), this.getExecutorFactory(), this.getConfiguration().isJournalSyncNonTransactional(), null) {
               @Override
               public PageCursorProvider newCursorProvider(PagingStore store,
                                                           StorageManager storageManager,
                                                           AddressSettings addressSettings,
                                                           Executor executor) {
                  return new InterruptedCursorProvider(store, storageManager, executor, addressSettings.getPageCacheMaxSize());
               }
            };
         }

      };

      addServer(server);

      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(PagingTest.PAGE_SIZE).setMaxSizeBytes(PagingTest.PAGE_MAX).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      server.start();

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(true, true, 0);
      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      Queue queue = server.locateQueue(ADDRESS);

      queue.getPageSubscription().getPagingStore().startPaging();

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message;

      for (int i = 0; i < 20; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(new byte[100 * 4]);

         message.putIntProperty(new SimpleString("idi"), i);

         producer.send(message);
         session.commit();
         if (i < 19) {
            queue.getPageSubscription().getPagingStore().forceAnotherPage();
         }

      }

      Assert.assertEquals(20, queue.getPageSubscription().getPagingStore().getCurrentWritingPage());

      // This will force a scenario where the pages are cleaned up. When restarting we need to check if the current page is complete
      // if it is complete we must move to another page avoiding races on cleanup
      // which could happen during a crash / restart
      long tx = server.getStorageManager().generateID();
      for (int i = 1; i <= 20; i++) {
         server.getStorageManager().storePageCompleteTransactional(tx, queue.getID(), new PagePositionImpl(i, 1));
      }

      server.getStorageManager().commit(tx);

      session.close();
      sf.close();

      server.stop();
      mainCleanup.set(false);

      logger.trace("Server restart");

      server.start();

      queue = server.locateQueue(ADDRESS);

      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = sf.createSession(null, null, false, false, true, false, 0);
      producer = session.createProducer(PagingTest.ADDRESS);

      for (int i = 0; i < 10; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(new byte[100 * 4]);

         message.putIntProperty(new SimpleString("newid"), i);

         producer.send(message);
         session.commit();

         if (i == 5) {
            queue.getPageSubscription().getPagingStore().forceAnotherPage();
         }
      }

      mainCleanup.set(true);

      queue = server.locateQueue(ADDRESS);
      queue.getPageSubscription().cleanupEntries(false);
      queue.getPageSubscription().getPagingStore().getCursorProvider().cleanup();

      ClientConsumer consumer = session.createConsumer(ADDRESS);
      session.start();

      for (int i = 0; i < 10; i++) {
         message = consumer.receive(5000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getIntProperty("newid").intValue());
         message.acknowledge();
      }

      server.stop();

      //      Thread.sleep(5000);

   }

   @Test
   public void testCommitOnSend() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfIntegers = 10;

      final int numberOfMessages = 500;

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, false, false, false, 0);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         for (int j = 1; j <= numberOfIntegers; j++) {
            bodyLocal.writeInt(j);
         }

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
      }

      session.commit();

      session.close();

      locator.close();

      locator = createInVMNonHALocator();

      server.stop();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(null, null, false, false, false, false, 0);

      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

      session.start();
      for (int i = 0; i < numberOfMessages; i++) {
         if (i == 55) {
            System.out.println("i = 55");
         }
         ClientMessage msg = consumer.receive(5000);
         Assert.assertNotNull(msg);
         msg.acknowledge();
         session.commit();
      }

      session.close();
   }

   @Test
   public void testParialConsume() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 1000;

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, false, false, false, 0);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(new byte[1024]);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
      }

      session.commit();

      session.close();

      locator.close();

      server.stop();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      locator = createInVMNonHALocator();

      sf = createSessionFactory(locator);

      session = sf.createSession(null, null, false, false, false, false, 0);

      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

      session.start();
      // 347 = I just picked any odd number, not rounded, to make sure it's not at the beginning of any page
      for (int i = 0; i < 347; i++) {
         ClientMessage msg = consumer.receive(5000);
         assertEquals(i, msg.getIntProperty("id").intValue());
         Assert.assertNotNull(msg);
         msg.acknowledge();
         session.commit();
      }

      session.close();

      locator.close();

      server.stop();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      locator = createInVMNonHALocator();

      sf = createSessionFactory(locator);

      session = sf.createSession(null, null, false, false, false, false, 0);

      consumer = session.createConsumer(PagingTest.ADDRESS);

      session.start();
      for (int i = 347; i < numberOfMessages; i++) {
         ClientMessage msg = consumer.receive(5000);
         assertEquals(i, msg.getIntProperty("id").intValue());
         Assert.assertNotNull(msg);
         msg.acknowledge();
         session.commit();
      }

      session.close();
   }

   @Test
   public void testPageMultipleDestinations() throws Exception {
      internalTestPageMultipleDestinations(false);
   }

   @Test
   public void testPageMultipleDestinationsTransacted() throws Exception {
      internalTestPageMultipleDestinations(true);
   }

   @Test
   public void testDropMessages() throws Exception {
      clearDataRecreateServerDirs();

      HashMap<String, AddressSettings> settings = new HashMap<>();

      AddressSettings set = new AddressSettings();
      set.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);

      settings.put(PagingTest.ADDRESS.toString(), set);

      server = createServer(true, createDefaultInVMConfig(), 1024, 10 * 1024, settings);

      server.start();

      final int numberOfMessages = 1000;

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      for (int i = 0; i < numberOfMessages; i++) {
         byte[] body = new byte[1024];

         message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(body);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

      session.start();

      for (int i = 0; i < 5; i++) {
         ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

         Assert.assertNotNull(message2);

         message2.acknowledge();
      }

      Assert.assertNull(consumer.receiveImmediate());

      Assert.assertEquals(0, server.getPagingManager().getPageStore(PagingTest.ADDRESS).getAddressSize());

      for (int i = 0; i < numberOfMessages; i++) {
         byte[] body = new byte[1024];

         message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(body);

         producer.send(message);
      }

      for (int i = 0; i < 5; i++) {
         ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

         Assert.assertNotNull(message2);

         message2.acknowledge();
      }

      Assert.assertNull(consumer.receiveImmediate());

      session.close();

      session = sf.createSession(false, true, true);

      producer = session.createProducer(PagingTest.ADDRESS);

      for (int i = 0; i < numberOfMessages; i++) {
         byte[] body = new byte[1024];

         message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(body);

         producer.send(message);
      }

      session.commit();

      consumer = session.createConsumer(PagingTest.ADDRESS);

      session.start();

      for (int i = 0; i < 5; i++) {
         ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

         Assert.assertNotNull(message2);

         message2.acknowledge();
      }

      session.commit();

      Assert.assertNull(consumer.receiveImmediate());

      session.close();

      Assert.assertEquals(0, server.getPagingManager().getPageStore(PagingTest.ADDRESS).getAddressSize());
   }

   @Test
   public void testDropMessagesExpiring() throws Exception {
      clearDataRecreateServerDirs();

      HashMap<String, AddressSettings> settings = new HashMap<>();

      AddressSettings set = new AddressSettings();
      set.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);

      settings.put(PagingTest.ADDRESS.toString(), set);

      server = createServer(true, createDefaultInVMConfig(), 1024, 1024 * 1024, settings);

      server.start();

      final int numberOfMessages = 30000;

      locator.setAckBatchSize(0);

      sf = createSessionFactory(locator);
      ClientSession sessionProducer = sf.createSession();

      sessionProducer.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = sessionProducer.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      ClientSession sessionConsumer = sf.createSession();

      class MyHandler implements MessageHandler {

         int count;

         @Override
         public void onMessage(ClientMessage message1) {
            try {
               Thread.sleep(1);
            } catch (Exception e) {

            }

            count++;

            if (count % 1000 == 0) {
               log.info("received " + count);
            }

            try {
               message1.acknowledge();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }

      ClientConsumer consumer = sessionConsumer.createConsumer(PagingTest.ADDRESS);

      sessionConsumer.start();

      consumer.setMessageHandler(new MyHandler());

      for (int i = 0; i < numberOfMessages; i++) {
         byte[] body = new byte[1024];

         message = sessionProducer.createMessage(false);
         message.getBodyBuffer().writeBytes(body);

         message.setExpiration(System.currentTimeMillis() + 100);

         producer.send(message);
      }

      sessionProducer.close();
      sessionConsumer.close();
   }

   private void internalTestPageMultipleDestinations(final boolean transacted) throws Exception {
      Configuration config = createDefaultInVMConfig();

      final int NUMBER_OF_BINDINGS = 100;

      int NUMBER_OF_MESSAGES = 2;

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, !transacted, true, false, 0);

      for (int i = 0; i < NUMBER_OF_BINDINGS; i++) {
         session.createQueue(PagingTest.ADDRESS, new SimpleString("someQueue" + i), null, true);
      }

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[1024];

      message = session.createMessage(true);
      message.getBodyBuffer().writeBytes(body);

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         producer.send(message);

         if (transacted) {
            session.commit();
         }
      }

      session.close();

      server.stop();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(null, null, false, true, true, false, 0);

      session.start();

      for (int msg = 0; msg < NUMBER_OF_MESSAGES; msg++) {

         for (int i = 0; i < NUMBER_OF_BINDINGS; i++) {
            ClientConsumer consumer = session.createConsumer(new SimpleString("someQueue" + i));

            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();

            Assert.assertNotNull(message2);

            consumer.close();

         }
      }

      session.close();

      for (int i = 0; i < NUMBER_OF_BINDINGS; i++) {
         Queue queue = (Queue) server.getPostOffice().getBinding(new SimpleString("someQueue" + i)).getBindable();

         Assert.assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, getMessageCount(queue));
         Assert.assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, queue.getDeliveringCount());
      }
   }

   @Test
   public void testSyncPage() throws Exception {
      Configuration config = createDefaultInVMConfig();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      try {
         server.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true, false);

         final CountDownLatch pageUp = new CountDownLatch(0);
         final CountDownLatch pageDone = new CountDownLatch(1);

         OperationContext ctx = new DummyOperationContext(pageUp, pageDone);

         OperationContextImpl.setContext(ctx);

         PagingManager paging = server.getPagingManager();

         PagingStore store = paging.getPageStore(ADDRESS);

         store.sync();

         assertTrue(pageUp.await(10, TimeUnit.SECONDS));

         assertTrue(pageDone.await(10, TimeUnit.SECONDS));

         server.stop();

      } finally {
         try {
            server.stop();
         } catch (Throwable ignored) {
         }

         OperationContextImpl.clearContext();
      }

   }

   @Test
   public void testSyncPageTX() throws Exception {
      Configuration config = createDefaultInVMConfig();

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      server.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true, false);

      final CountDownLatch pageUp = new CountDownLatch(0);
      final CountDownLatch pageDone = new CountDownLatch(1);

      OperationContext ctx = new DummyOperationContext(pageUp, pageDone);
      OperationContextImpl.setContext(ctx);

      PagingManager paging = server.getPagingManager();

      PagingStore store = paging.getPageStore(ADDRESS);

      store.sync();

      assertTrue(pageUp.await(10, TimeUnit.SECONDS));

      assertTrue(pageDone.await(10, TimeUnit.SECONDS));
   }

   @Test
   public void testPagingOneDestinationOnly() throws Exception {
      SimpleString PAGED_ADDRESS = new SimpleString("paged");
      SimpleString NON_PAGED_ADDRESS = new SimpleString("non-paged");

      Configuration configuration = createDefaultInVMConfig();

      Map<String, AddressSettings> addresses = new HashMap<>();

      addresses.put("#", new AddressSettings());

      AddressSettings pagedDestination = new AddressSettings().setPageSizeBytes(1024).setMaxSizeBytes(10 * 1024);

      addresses.put(PAGED_ADDRESS.toString(), pagedDestination);

      server = createServer(true, configuration, -1, -1, addresses);

      server.start();

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, false);

      session.createQueue(PAGED_ADDRESS, PAGED_ADDRESS, true);

      session.createQueue(NON_PAGED_ADDRESS, NON_PAGED_ADDRESS, true);

      ClientProducer producerPaged = session.createProducer(PAGED_ADDRESS);
      ClientProducer producerNonPaged = session.createProducer(NON_PAGED_ADDRESS);

      int NUMBER_OF_MESSAGES = 100;

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[512]);

         producerPaged.send(msg);
         producerNonPaged.send(msg);
      }

      session.close();

      Assert.assertTrue(server.getPagingManager().getPageStore(PAGED_ADDRESS).isPaging());
      Assert.assertFalse(server.getPagingManager().getPageStore(NON_PAGED_ADDRESS).isPaging());

      session = sf.createSession(false, true, false);

      session.start();

      ClientConsumer consumerNonPaged = session.createConsumer(NON_PAGED_ADDRESS);
      ClientConsumer consumerPaged = session.createConsumer(PAGED_ADDRESS);

      ClientMessage[] ackList = new ClientMessage[NUMBER_OF_MESSAGES];

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         ClientMessage msg = consumerNonPaged.receive(5000);
         Assert.assertNotNull(msg);
         ackList[i] = msg;
      }

      Assert.assertNull(consumerNonPaged.receiveImmediate());

      for (ClientMessage ack : ackList) {
         ack.acknowledge();
      }

      consumerNonPaged.close();

      session.commit();

      ackList = null;

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         ClientMessage msg = consumerPaged.receive(5000);
         Assert.assertNotNull(msg);
         msg.acknowledge();
         session.commit();
      }

      Assert.assertNull(consumerPaged.receiveImmediate());

      session.close();
   }

   @Test
   public void testPagingDifferentSizes() throws Exception {
      SimpleString PAGED_ADDRESS_A = new SimpleString("paged-a");
      SimpleString PAGED_ADDRESS_B = new SimpleString("paged-b");

      Configuration configuration = createDefaultInVMConfig();

      Map<String, AddressSettings> addresses = new HashMap<>();

      addresses.put("#", new AddressSettings());

      AddressSettings pagedDestinationA = new AddressSettings().setPageSizeBytes(1024).setMaxSizeBytes(10 * 1024);

      int NUMBER_MESSAGES_BEFORE_PAGING = 11;

      addresses.put(PAGED_ADDRESS_A.toString(), pagedDestinationA);

      AddressSettings pagedDestinationB = new AddressSettings().setPageSizeBytes(2024).setMaxSizeBytes(20 * 1024);

      addresses.put(PAGED_ADDRESS_B.toString(), pagedDestinationB);

      server = createServer(true, configuration, -1, -1, addresses);
      server.start();

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, false);

      session.createQueue(PAGED_ADDRESS_A, PAGED_ADDRESS_A, true);

      session.createQueue(PAGED_ADDRESS_B, PAGED_ADDRESS_B, true);

      ClientProducer producerA = session.createProducer(PAGED_ADDRESS_A);
      ClientProducer producerB = session.createProducer(PAGED_ADDRESS_B);

      int NUMBER_OF_MESSAGES = 100;

      for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++) {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[896]);

         producerA.send(msg);
         producerB.send(msg);
      }

      session.commit(); // commit was called to clean the buffer only (making sure everything is on the server side)

      Assert.assertTrue(server.getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
      Assert.assertFalse(server.getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

      for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++) {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[896]);

         producerA.send(msg);
         producerB.send(msg);
      }

      session.commit(); // commit was called to clean the buffer only (making sure everything is on the server side)

      Assert.assertTrue(server.getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
      Assert.assertTrue(server.getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

      for (int i = NUMBER_MESSAGES_BEFORE_PAGING * 2; i < NUMBER_OF_MESSAGES; i++) {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[896]);

         producerA.send(msg);
         producerB.send(msg);
      }

      session.close();

      Assert.assertTrue(server.getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
      Assert.assertTrue(server.getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

      session = sf.createSession(null, null, false, true, true, false, 0);

      session.start();

      ClientConsumer consumerA = session.createConsumer(PAGED_ADDRESS_A);

      ClientConsumer consumerB = session.createConsumer(PAGED_ADDRESS_B);

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         ClientMessage msg = consumerA.receive(5000);
         Assert.assertNotNull("Couldn't receive a message on consumerA, iteration = " + i, msg);
         msg.acknowledge();
      }

      Assert.assertNull(consumerA.receiveImmediate());

      consumerA.close();

      Assert.assertTrue(server.getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         ClientMessage msg = consumerB.receive(5000);
         Assert.assertNotNull(msg);
         msg.acknowledge();
         session.commit();
      }

      Assert.assertNull(consumerB.receiveImmediate());

      consumerB.close();

      session.close();
   }

   @Test
   public void testPageAndDepageRapidly() throws Exception {
      boolean persistentMessages = true;

      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false).setJournalFileSize(10 * 1024 * 1024);

      server = createServer(true, config, 512 * 1024, 1024 * 1024);

      server.start();

      final int messageSize = 51527;

      final int numberOfMessages = 200;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      final AtomicInteger errors = new AtomicInteger(0);

      Thread consumeThread = new Thread() {
         @Override
         public void run() {
            ClientSession sessionConsumer = null;
            try {
               sessionConsumer = sf.createSession(false, false);
               sessionConsumer.start();

               ClientConsumer cons = sessionConsumer.createConsumer(ADDRESS);

               for (int i = 0; i < numberOfMessages; i++) {
                  ClientMessage msg = cons.receive(PagingTest.RECEIVE_TIMEOUT);
                  assertNotNull(msg);
                  msg.acknowledge();

                  if (i % 20 == 0) {
                     sessionConsumer.commit();
                  }
               }
               sessionConsumer.commit();
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            } finally {
               try {
                  sessionConsumer.close();
               } catch (ActiveMQException e) {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }
            }

         }
      };

      consumeThread.start();

      ClientMessage message = null;

      byte[] body = new byte[messageSize];

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(persistentMessages);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);

         Thread.sleep(50);
      }

      consumeThread.join();

      assertEquals(0, errors.get());

      long timeout = System.currentTimeMillis() + 5000;

      while (System.currentTimeMillis() < timeout && (server.getPagingManager().getPageStore(ADDRESS).isPaging() || server.getPagingManager().getPageStore(ADDRESS).getNumberOfPages() != 1)) {
         Thread.sleep(1);
      }

      // It's async, so need to wait a bit for it happening
      assertFalse(server.getPagingManager().getPageStore(ADDRESS).isPaging());

      assertEquals(1, server.getPagingManager().getPageStore(ADDRESS).getNumberOfPages());
   }

   @Test
   public void testTwoQueuesDifferentFilters() throws Exception {
      boolean persistentMessages = true;

      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int numberOfMessages = 200;
      locator = createInVMNonHALocator().setClientFailureCheckPeriod(120000).setConnectionTTL(5000000).setCallTimeout(120000).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      // note: if you want to change this, numberOfMessages has to be a multiple of NQUEUES
      int NQUEUES = 2;

      for (int i = 0; i < NQUEUES; i++) {
         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS.concat("=" + i), new SimpleString("propTest=" + i), true);
      }

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      byte[] body = new byte[MESSAGE_SIZE];

      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(persistentMessages);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty("propTest", i % NQUEUES);
         message.putIntProperty("id", i);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }

      session.commit();

      session.start();

      for (int nqueue = 0; nqueue < NQUEUES; nqueue++) {
         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS.concat("=" + nqueue));

         for (int i = 0; i < (numberOfMessages / NQUEUES); i++) {
            message = consumer.receive(500000);
            assertNotNull(message);
            message.acknowledge();

            assertEquals(nqueue, message.getIntProperty("propTest").intValue());
         }

         assertNull(consumer.receiveImmediate());

         consumer.close();

         session.commit();
      }

      PagingStore store = server.getPagingManager().getPageStore(ADDRESS);
      store.getCursorProvider().cleanup();

      long timeout = System.currentTimeMillis() + 5000;
      while (store.isPaging() && timeout > System.currentTimeMillis()) {
         Thread.sleep(100);
      }

      // It's async, so need to wait a bit for it happening
      assertFalse(server.getPagingManager().getPageStore(ADDRESS).isPaging());
   }

   @Test
   public void testTwoQueues() throws Exception {
      boolean persistentMessages = true;

      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 1000;

      try {
         ServerLocator locator = createInVMNonHALocator().setClientFailureCheckPeriod(120000).setConnectionTTL(5000000).setCallTimeout(120000).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS.concat("=1"), null, true);
         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS.concat("=2"), null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[messageSize];

         for (int i = 0; i < numberOfMessages; i++) {
            message = session.createMessage(persistentMessages);

            ActiveMQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty("propTest", i % 2 == 0 ? 1 : 2);

            producer.send(message);
            if (i % 1000 == 0) {
               session.commit();
            }
         }

         session.commit();

         session.start();

         for (int msg = 1; msg <= 2; msg++) {
            ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS.concat("=" + msg));

            for (int i = 0; i < numberOfMessages; i++) {
               message = consumer.receive(5000);
               assertNotNull(message);
               message.acknowledge();

               // assertEquals(msg, message.getIntProperty("propTest").intValue());

               System.out.println("i = " + i + " msg = " + message.getIntProperty("propTest"));
            }

            session.commit();

            assertNull(consumer.receiveImmediate());

            consumer.close();
         }

         PagingStore store = server.getPagingManager().getPageStore(ADDRESS);
         store.getCursorProvider().cleanup();

         long timeout = System.currentTimeMillis() + 5000;
         while (store.isPaging() && timeout > System.currentTimeMillis()) {
            Thread.sleep(100);
         }

         store.getCursorProvider().cleanup();

         waitForNotPaging(server.locateQueue(PagingTest.ADDRESS.concat("=1")));

         sf.close();

         locator.close();
      } finally {
         try {
            server.stop();
         } catch (Throwable ignored) {
         }
      }
   }

   @Test
   public void testTwoQueuesAndOneInativeQueue() throws Exception {
      boolean persistentMessages = true;

      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator().setClientFailureCheckPeriod(120000).setConnectionTTL(5000000).setCallTimeout(120000).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS.concat("=1"), null, true);
         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS.concat("=2"), null, true);

         // A queue with an impossible filter
         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS.concat("-3"), new SimpleString("nothing='something'"), true);

         PagingStore store = server.getPagingManager().getPageStore(ADDRESS);

         Queue queue = server.locateQueue(PagingTest.ADDRESS.concat("=1"));

         queue.getPageSubscription().getPagingStore().startPaging();

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = session.createMessage(persistentMessages);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(new byte[1024]);

         producer.send(message);

         session.commit();

         session.start();

         for (int msg = 1; msg <= 2; msg++) {
            ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS.concat("=" + msg));

            message = consumer.receive(5000);
            assertNotNull(message);
            message.acknowledge();

            assertNull(consumer.receiveImmediate());

            consumer.close();
         }

         session.commit();
         session.close();

         store.getCursorProvider().cleanup();

         waitForNotPaging(server.locateQueue(PagingTest.ADDRESS.concat("=1")));

         sf.close();

         locator.close();
      } finally {
         try {
            server.stop();
         } catch (Throwable ignored) {
         }
      }
   }

   @Test
   public void testTwoQueuesConsumeOneRestart() throws Exception {
      boolean persistentMessages = true;

      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 1000;

      try {
         ServerLocator locator = createInVMNonHALocator().setClientFailureCheckPeriod(120000).setConnectionTTL(5000000).setCallTimeout(120000).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS.concat("=1"), null, true);
         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS.concat("=2"), null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[messageSize];

         for (int i = 0; i < numberOfMessages; i++) {
            message = session.createMessage(persistentMessages);

            ActiveMQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty("propTest", i % 2 == 0 ? 1 : 2);

            producer.send(message);
            if (i % 1000 == 0) {
               session.commit();
            }
         }

         session.commit();

         session.start();

         session.deleteQueue(PagingTest.ADDRESS.concat("=1"));

         sf = locator.createSessionFactory();

         session = sf.createSession(false, false, false);

         session.start();

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS.concat("=2"));

         for (int i = 0; i < numberOfMessages; i++) {
            message = consumer.receive(5000);
            assertNotNull(message);
            message.acknowledge();
         }

         session.commit();

         assertNull(consumer.receiveImmediate());

         consumer.close();

         long timeout = System.currentTimeMillis() + 10000;

         PagingStore store = server.getPagingManager().getPageStore(ADDRESS);

         // It's async, so need to wait a bit for it happening
         while (timeout > System.currentTimeMillis() && store.isPaging()) {
            Thread.sleep(100);
         }

         assertFalse(server.getPagingManager().getPageStore(ADDRESS).isPaging());

         server.stop();

         server.start();

         server.stop();
         server.start();

         sf.close();

         locator.close();
      } finally {
         try {
            server.stop();
         } catch (Throwable ignored) {
         }
      }
   }

   @Test
   public void testDLAOnLargeMessageAndPaging() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setThreadPoolMaxSize(5).setJournalSyncNonTransactional(false);

      Map<String, AddressSettings> settings = new HashMap<>();
      AddressSettings dla = new AddressSettings().setMaxDeliveryAttempts(5).setDeadLetterAddress(new SimpleString("DLA")).setRedeliveryDelay(0);
      settings.put(ADDRESS.toString(), dla);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX, settings);

      server.start();

      final int messageSize = 1024;

      ServerLocator locator = null;
      ClientSessionFactory sf = null;
      ClientSession session = null;
      try {
         locator = createInVMNonHALocator();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);

         sf = locator.createSessionFactory();

         session = sf.createSession(false, false, false);

         session.createQueue(ADDRESS, ADDRESS, true);
         session.createQueue("DLA", "DLA", true);

         Queue serverQueue = server.locateQueue(ADDRESS);
         Queue serverQueueDLA = server.locateQueue(SimpleString.toSimpleString("DLA"));

         PagingStore pgStoreAddress = server.getPagingManager().getPageStore(ADDRESS);
         pgStoreAddress.startPaging();
         PagingStore pgStoreDLA = server.getPagingManager().getPageStore(new SimpleString("DLA"));

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         for (int i = 0; i < 100; i++) {
            log.debug("send message #" + i);
            ClientMessage message = session.createMessage(true);

            message.putStringProperty("id", "str" + i);

            message.setBodyInputStream(createFakeLargeStream(messageSize));

            producer.send(message);

            if ((i + 1) % 2 == 0) {
               session.commit();
            }
         }

         session.commit();

         session.start();

         ClientConsumer cons = session.createConsumer(ADDRESS);

         for (int msgNr = 0; msgNr < 2; msgNr++) {
            for (int i = 0; i < 5; i++) {
               ClientMessage msg = cons.receive(5000);

               assertNotNull(msg);

               msg.acknowledge();

               for (int j = 0; j < messageSize; j++) {
                  assertEquals(getSamplebyte(j), msg.getBodyBuffer().readByte());
               }

               session.rollback();
            }

            pgStoreDLA.startPaging();
         }

         for (int i = 2; i < 100; i++) {
            log.debug("Received message " + i);
            ClientMessage message = cons.receive(5000);
            assertNotNull("Message " + i + " wasn't received", message);
            message.acknowledge();

            final AtomicInteger bytesOutput = new AtomicInteger(0);

            message.setOutputStream(new OutputStream() {
               @Override
               public void write(int b) throws IOException {
                  bytesOutput.incrementAndGet();
               }
            });

            try {
               if (!message.waitOutputStreamCompletion(10000)) {
                  log.info(threadDump("dump"));
                  fail("Couldn't finish large message receiving");
               }
            } catch (Throwable e) {
               log.info("output bytes = " + bytesOutput);
               log.info(threadDump("dump"));
               fail("Couldn't finish large message receiving for id=" + message.getStringProperty("id") +
                       " with messageID=" +
                       message.getMessageID());
            }

         }

         assertNull(cons.receiveImmediate());

         cons.close();

         cons = session.createConsumer("DLA");

         for (int i = 0; i < 2; i++) {
            assertNotNull(cons.receive(5000));
         }

         sf.close();

         session.close();

         locator.close();

         server.stop();

         server.start();

         locator = createInVMNonHALocator();

         sf = locator.createSessionFactory();

         session = sf.createSession(false, false);

         session.start();

         cons = session.createConsumer(ADDRESS);

         for (int i = 2; i < 100; i++) {
            log.debug("Received message " + i);
            ClientMessage message = cons.receive(5000);
            assertNotNull(message);

            assertEquals("str" + i, message.getStringProperty("id"));

            message.acknowledge();

            message.setOutputStream(new OutputStream() {
               @Override
               public void write(int b) throws IOException {

               }
            });

            assertTrue(message.waitOutputStreamCompletion(5000));
         }

         assertNull(cons.receiveImmediate());

         cons.close();

         cons = session.createConsumer("DLA");

         for (int msgNr = 0; msgNr < 2; msgNr++) {
            ClientMessage msg = cons.receive(10000);

            assertNotNull(msg);

            assertEquals("str" + msgNr, msg.getStringProperty("id"));

            for (int i = 0; i < messageSize; i++) {
               assertEquals(getSamplebyte(i), msg.getBodyBuffer().readByte());
            }

            msg.acknowledge();
         }

         cons.close();

         cons = session.createConsumer(ADDRESS);

         session.commit();

         assertNull(cons.receiveImmediate());

         long timeout = System.currentTimeMillis() + 5000;

         pgStoreAddress = server.getPagingManager().getPageStore(ADDRESS);

         pgStoreAddress.getCursorProvider().getSubscription(serverQueue.getID()).cleanupEntries(false);

         pgStoreAddress.getCursorProvider().cleanup();

         while (timeout > System.currentTimeMillis() && pgStoreAddress.isPaging()) {
            Thread.sleep(50);
         }

         assertFalse(pgStoreAddress.isPaging());

         session.commit();
      } finally {
         session.close();
         sf.close();
         locator.close();
         try {
            server.stop();
         } catch (Throwable ignored) {
         }
      }
   }

   @Test
   public void testExpireLargeMessageOnPaging() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setMessageExpiryScanPeriod(500).setJournalSyncNonTransactional(false);

      Map<String, AddressSettings> settings = new HashMap<>();
      AddressSettings dla = new AddressSettings().setMaxDeliveryAttempts(5).setDeadLetterAddress(new SimpleString("DLA")).setExpiryAddress(new SimpleString("DLA"));
      settings.put(ADDRESS.toString(), dla);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX, settings);

      server.start();

      final int messageSize = 20;

      try {
         ServerLocator locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(false, false, false);

         session.createQueue(ADDRESS, ADDRESS, true);

         session.createQueue("DLA", "DLA", true);

         PagingStore pgStoreAddress = server.getPagingManager().getPageStore(ADDRESS);
         pgStoreAddress.startPaging();
         PagingStore pgStoreDLA = server.getPagingManager().getPageStore(new SimpleString("DLA"));

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < 500; i++) {
            if (i % 100 == 0)
               log.info("send message #" + i);
            message = session.createMessage(true);

            message.putStringProperty("id", "str" + i);

            message.setExpiration(System.currentTimeMillis() + 2000);

            if (i % 2 == 0) {
               message.setBodyInputStream(createFakeLargeStream(messageSize));
            } else {
               byte[] bytes = new byte[messageSize];
               for (int s = 0; s < bytes.length; s++) {
                  bytes[s] = getSamplebyte(s);
               }
               message.getBodyBuffer().writeBytes(bytes);
            }

            producer.send(message);

            if ((i + 1) % 2 == 0) {
               session.commit();
               if (i < 400) {
                  pgStoreAddress.forceAnotherPage();
               }
            }
         }

         session.commit();

         sf.close();

         locator.close();

         server.stop();

         Thread.sleep(3000);

         server.start();

         locator = createInVMNonHALocator();

         sf = locator.createSessionFactory();

         session = sf.createSession(false, false);

         session.start();

         ClientConsumer consAddr = session.createConsumer(ADDRESS);

         assertNull(consAddr.receive(1000));

         ClientConsumer cons = session.createConsumer("DLA");

         for (int i = 0; i < 500; i++) {
            log.info("Received message " + i);
            message = cons.receive(10000);
            assertNotNull(message);
            message.acknowledge();

            message.saveToOutputStream(new OutputStream() {
               @Override
               public void write(int b) throws IOException {

               }
            });
         }

         assertNull(cons.receiveImmediate());

         session.commit();

         cons.close();

         long timeout = System.currentTimeMillis() + 5000;

         pgStoreAddress = server.getPagingManager().getPageStore(ADDRESS);

         while (timeout > System.currentTimeMillis() && pgStoreAddress.isPaging()) {
            Thread.sleep(50);
         }

         assertFalse(pgStoreAddress.isPaging());

         session.close();
      } finally {
         locator.close();
         try {
            server.stop();
         } catch (Throwable ignored) {
         }
      }
   }

   @Test
   /**
    * When running this test from an IDE add this to the test command line so that the AssertionLoggerHandler works properly:
    *
    *   -Djava.util.logging.manager=org.jboss.logmanager.LogManager  -Dlogging.configuration=file:<path_to_source>/tests/config/logging.properties
    *
    *   Note: Idea should get these from the pom and you shouldn't need to do this.
    */ public void testFailMessagesNonDurable() throws Exception {
      AssertionLoggerHandler.startCapture();

      try {
         clearDataRecreateServerDirs();

         Configuration config = createDefaultInVMConfig();

         HashMap<String, AddressSettings> settings = new HashMap<>();

         AddressSettings set = new AddressSettings();
         set.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);

         settings.put(PagingTest.ADDRESS.toString(), set);

         server = createServer(true, config, 1024, 5 * 1024, settings);

         server.start();

         locator.setBlockOnNonDurableSend(false).setBlockOnDurableSend(false).setBlockOnAcknowledge(true);

         sf = createSessionFactory(locator);
         ClientSession session = sf.createSession(true, true, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = session.createMessage(false);

         int biggerMessageSize = 1024;
         byte[] body = new byte[biggerMessageSize];
         ByteBuffer bb = ByteBuffer.wrap(body);
         for (int j = 1; j <= biggerMessageSize; j++) {
            bb.put(getSamplebyte(j));
         }

         message.getBodyBuffer().writeBytes(body);

         // Send enough messages to fill up the address, but don't test for an immediate exception because we do not block
         // on non-durable send. Instead of receiving an exception immediately the exception will be logged on the server.
         for (int i = 0; i < 32; i++) {
            producer.send(message);
         }

         // allow time for the logging to actually happen on the server
         Thread.sleep(100);

         Assert.assertTrue("Expected to find AMQ224016", AssertionLoggerHandler.findText("AMQ224016"));

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();

         // Once the destination is full and the client has run out of credits then it will receive an exception
         for (int i = 0; i < 10; i++) {
            validateExceptionOnSending(producer, message);
         }

         // Receive a message.. this should release credits
         ClientMessage msgReceived = consumer.receive(5000);
         assertNotNull(msgReceived);
         msgReceived.acknowledge();
         session.commit(); // to make sure it's on the server (roundtrip)

         boolean exception = false;

         try {
            for (int i = 0; i < 1000; i++) {
               // this send will succeed on the server
               producer.send(message);
            }
         } catch (Exception e) {
            exception = true;
         }

         assertTrue("Expected to throw an exception", exception);
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   @Test
   public void testFailMessagesDurable() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig();

      HashMap<String, AddressSettings> settings = new HashMap<>();

      AddressSettings set = new AddressSettings();
      set.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);

      settings.put(PagingTest.ADDRESS.toString(), set);

      server = createServer(true, config, 1024, 5 * 1024, settings);

      server.start();

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(true, true, 0);

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = session.createMessage(true);

      int biggerMessageSize = 1024;
      byte[] body = new byte[biggerMessageSize];
      ByteBuffer bb = ByteBuffer.wrap(body);
      for (int j = 1; j <= biggerMessageSize; j++) {
         bb.put(getSamplebyte(j));
      }

      message.getBodyBuffer().writeBytes(body);

      // Send enough messages to fill up the address and test for an exception.
      // The address will actually fill up after 3 messages. Also, it takes 32 messages for the client's
      // credits to run out.
      for (int i = 0; i < 50; i++) {
         if (i > 2) {
            validateExceptionOnSending(producer, message);
         } else {
            producer.send(message);
         }
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      // Receive a message.. this should release credits
      ClientMessage msgReceived = consumer.receive(5000);
      assertNotNull(msgReceived);
      msgReceived.acknowledge();
      session.commit(); // to make sure it's on the server (roundtrip)

      boolean exception = false;

      try {
         for (int i = 0; i < 1000; i++) {
            // this send will succeed on the server
            producer.send(message);
         }
      } catch (Exception e) {
         exception = true;
      }

      assertTrue("Expected to throw an exception", exception);
   }

   @Test
   public void testFailMessagesDuplicates() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig();

      HashMap<String, AddressSettings> settings = new HashMap<>();

      AddressSettings set = new AddressSettings();
      set.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);

      settings.put(PagingTest.ADDRESS.toString(), set);

      server = createServer(true, config, 1024, 5 * 1024, settings);

      server.start();

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);
      ClientSession session = addClientSession(sf.createSession(true, true, 0));

      session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = session.createMessage(true);

      int biggerMessageSize = 1024;
      byte[] body = new byte[biggerMessageSize];
      ByteBuffer bb = ByteBuffer.wrap(body);
      for (int j = 1; j <= biggerMessageSize; j++) {
         bb.put(getSamplebyte(j));
      }

      message.getBodyBuffer().writeBytes(body);

      // Send enough messages to fill up the address.
      producer.send(message);
      producer.send(message);
      producer.send(message);

      Queue q = (Queue) server.getPostOffice().getBinding(ADDRESS).getBindable();
      Assert.assertEquals(3, getMessageCount(q));

      // send a message with a dup ID that should fail b/c the address is full
      SimpleString dupID1 = new SimpleString("abcdefg");
      message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID1.getData());
      message.putStringProperty("key", dupID1.toString());

      validateExceptionOnSending(producer, message);

      Assert.assertEquals(3, getMessageCount(q));

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      // Receive a message...this should open space for another message
      ClientMessage msgReceived = consumer.receive(5000);
      assertNotNull(msgReceived);
      msgReceived.acknowledge();
      session.commit(); // to make sure it's on the server (roundtrip)
      consumer.close();

      Assert.assertEquals(2, getMessageCount(q));

      producer.send(message);

      Assert.assertEquals(3, getMessageCount(q));

      consumer = session.createConsumer(ADDRESS);

      for (int i = 0; i < 3; i++) {
         msgReceived = consumer.receive(5000);
         assertNotNull(msgReceived);
         msgReceived.acknowledge();
         session.commit();
      }
   }

   /**
    * This method validates if sending a message will throw an exception
    */
   private void validateExceptionOnSending(ClientProducer producer, ClientMessage message) {
      ActiveMQException expected = null;

      try {
         // after the address is full this send should fail (since the address full policy is FAIL)
         producer.send(message);
      } catch (ActiveMQException e) {
         expected = e;
      }

      assertNotNull(expected);
      assertEquals(ActiveMQExceptionType.ADDRESS_FULL, expected.getType());
   }

   @Test
   public void testSpreadMessagesWithFilterWithDeadConsumer() throws Exception {
      testSpreadMessagesWithFilter(true);
   }

   @Test
   public void testSpreadMessagesWithFilterWithoutDeadConsumer() throws Exception {
      testSpreadMessagesWithFilter(false);
   }

   @Test
   public void testRouteOnTopWithMultipleQueues() throws Exception {

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      ServerLocator locator = createInVMNonHALocator().setBlockOnDurableSend(false);
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, 0);

      session.createQueue("Q", "Q1", "dest=1", true);
      session.createQueue("Q", "Q2", "dest=2", true);
      session.createQueue("Q", "Q3", "dest=3", true);

      Queue queue = server.locateQueue(new SimpleString("Q1"));
      queue.getPageSubscription().getPagingStore().startPaging();

      ClientProducer prod = session.createProducer("Q");
      ClientMessage msg = session.createMessage(true);
      msg.putIntProperty("dest", 1);
      prod.send(msg);
      session.commit();

      msg = session.createMessage(true);
      msg.putIntProperty("dest", 2);
      prod.send(msg);
      session.commit();

      session.start();
      ClientConsumer cons1 = session.createConsumer("Q1");
      msg = cons1.receive(5000);
      assertNotNull(msg);
      msg.acknowledge();

      ClientConsumer cons2 = session.createConsumer("Q2");
      msg = cons2.receive(5000);
      assertNotNull(msg);

      queue.getPageSubscription().getPagingStore().forceAnotherPage();

      msg = session.createMessage(true);
      msg.putIntProperty("dest", 1);
      prod.send(msg);
      session.commit();

      msg = cons1.receive(5000);
      assertNotNull(msg);
      msg.acknowledge();

      queue.getPageSubscription().cleanupEntries(false);

      System.out.println("Waiting there");

      server.stop();
   }

   // https://issues.jboss.org/browse/HORNETQ-1042 - spread messages because of filters
   public void testSpreadMessagesWithFilter(boolean deadConsumer) throws Exception {

      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         locator.setBlockOnDurableSend(false);
         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(true, false);

         session.createQueue(ADDRESS.toString(), "Q1", "destQ=1 or both=true", true);
         session.createQueue(ADDRESS.toString(), "Q2", "destQ=2 or both=true", true);

         if (deadConsumer) {
            // This queue won't receive any messages
            session.createQueue(ADDRESS.toString(), "Q3", "destQ=3", true);
         }

         session.createQueue(ADDRESS.toString(), "Q_initial", "initialBurst=true", true);

         ClientSession sessionConsumerQ3 = null;

         final AtomicInteger consumerQ3Msgs = new AtomicInteger(0);

         if (deadConsumer) {
            sessionConsumerQ3 = sf.createSession(true, true);
            ClientConsumer consumerQ3 = sessionConsumerQ3.createConsumer("Q3");

            consumerQ3.setMessageHandler(new MessageHandler() {

               @Override
               public void onMessage(ClientMessage message) {
                  System.out.println("Received an unexpected message");
                  consumerQ3Msgs.incrementAndGet();
               }
            });

            sessionConsumerQ3.start();

         }

         final int initialBurst = 100;
         final int messagesSentAfterBurst = 100;
         final int MESSAGE_SIZE = 300;
         final byte[] bodyWrite = new byte[MESSAGE_SIZE];

         Queue serverQueue = server.locateQueue(new SimpleString("Q1"));
         PagingStore pageStore = serverQueue.getPageSubscription().getPagingStore();

         ClientProducer producer = session.createProducer(ADDRESS);

         // send an initial burst that will put the system into page mode. The initial burst will be towards Q1 only
         for (int i = 0; i < initialBurst; i++) {
            ClientMessage m = session.createMessage(true);
            m.getBodyBuffer().writeBytes(bodyWrite);
            m.putIntProperty("destQ", 1);
            m.putBooleanProperty("both", false);
            m.putBooleanProperty("initialBurst", true);
            producer.send(m);
            if (i % 100 == 0) {
               session.commit();
            }
         }

         session.commit();

         pageStore.forceAnotherPage();

         for (int i = 0; i < messagesSentAfterBurst; i++) {
            {
               ClientMessage m = session.createMessage(true);
               m.getBodyBuffer().writeBytes(bodyWrite);
               m.putIntProperty("destQ", 1);
               m.putBooleanProperty("initialBurst", false);
               m.putIntProperty("i", i);
               m.putBooleanProperty("both", i % 10 == 0);
               producer.send(m);
            }

            if (i % 10 != 0) {
               ClientMessage m = session.createMessage(true);
               m.getBodyBuffer().writeBytes(bodyWrite);
               m.putIntProperty("destQ", 2);
               m.putIntProperty("i", i);
               m.putBooleanProperty("both", false);
               m.putBooleanProperty("initialBurst", false);
               producer.send(m);
            }

            if (i > 0 && i % 10 == 0) {
               session.commit();
               if (i + 10 < messagesSentAfterBurst) {
                  pageStore.forceAnotherPage();
               }
            }
         }

         session.commit();

         ClientConsumer consumerQ1 = session.createConsumer("Q1");
         ClientConsumer consumerQ2 = session.createConsumer("Q2");
         session.start();

         // consuming now

         // initial burst

         for (int i = 0; i < initialBurst; i++) {
            ClientMessage m = consumerQ1.receive(5000);
            assertNotNull(m);
            assertEquals(1, m.getIntProperty("destQ").intValue());
            m.acknowledge();
            session.commit();
         }

         // This will consume messages from the beginning of the queue only
         ClientConsumer consumerInitial = session.createConsumer("Q_initial");
         for (int i = 0; i < initialBurst; i++) {
            ClientMessage m = consumerInitial.receive(5000);
            assertNotNull(m);
            assertEquals(1, m.getIntProperty("destQ").intValue());
            m.acknowledge();
         }

         assertNull(consumerInitial.receiveImmediate());
         session.commit();

         // messages from Q1

         for (int i = 0; i < messagesSentAfterBurst; i++) {
            ClientMessage m = consumerQ1.receive(5000);
            assertNotNull(m);
            if (!m.getBooleanProperty("both")) {
               assertEquals(1, m.getIntProperty("destQ").intValue());
            }
            assertEquals(i, m.getIntProperty("i").intValue());
            m.acknowledge();

            session.commit();
         }

         for (int i = 0; i < messagesSentAfterBurst; i++) {
            ClientMessage m = consumerQ2.receive(5000);
            assertNotNull(m);

            if (!m.getBooleanProperty("both")) {
               assertEquals(2, m.getIntProperty("destQ").intValue());
            }
            assertEquals(i, m.getIntProperty("i").intValue());
            m.acknowledge();

            session.commit();
         }

         waitForNotPaging(serverQueue);

         if (sessionConsumerQ3 != null) {
            sessionConsumerQ3.close();
         }
         assertEquals(0, consumerQ3Msgs.intValue());

         session.close();
         locator.close();

      } finally {
         server.stop();
      }
   }

   // We send messages to pages, create a big hole (a few pages without any messages), ack everything
   // and expect it to move to the next page
   @Test
   public void testPageHole() throws Throwable {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator().setBlockOnDurableSend(true);
         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(true, true, 0);

         session.createQueue(ADDRESS.toString(), "Q1", "dest=1", true);
         session.createQueue(ADDRESS.toString(), "Q2", "dest=2", true);

         PagingStore store = server.getPagingManager().getPageStore(ADDRESS);

         store.startPaging();

         ClientProducer prod = session.createProducer(ADDRESS);

         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("dest", 1);
         prod.send(msg);

         for (int i = 0; i < 100; i++) {
            msg = session.createMessage(true);
            msg.putIntProperty("dest", 2);
            prod.send(msg);

            if (i > 0 && i % 10 == 0) {
               store.forceAnotherPage();
            }
         }

         session.start();

         ClientConsumer cons1 = session.createConsumer("Q1");

         ClientMessage msgReceivedCons1 = cons1.receive(5000);
         assertNotNull(msgReceivedCons1);
         msgReceivedCons1.acknowledge();

         ClientConsumer cons2 = session.createConsumer("Q2");
         for (int i = 0; i < 100; i++) {
            ClientMessage msgReceivedCons2 = cons2.receive(1000);
            assertNotNull(msgReceivedCons2);
            msgReceivedCons2.acknowledge();

            session.commit();

            // It will send another message when it's mid consumed
            if (i == 20) {
               // wait at least one page to be deleted before sending a new one
               for (long timeout = System.currentTimeMillis() + 5000; timeout > System.currentTimeMillis() && store.checkPageFileExists(2); ) {
                  Thread.sleep(10);
               }
               msg = session.createMessage(true);
               msg.putIntProperty("dest", 1);
               prod.send(msg);
            }
         }

         msgReceivedCons1 = cons1.receive(5000);
         assertNotNull(msgReceivedCons1);
         msgReceivedCons1.acknowledge();

         assertNull(cons1.receiveImmediate());
         assertNull(cons2.receiveImmediate());

         session.commit();

         session.close();

         waitForNotPaging(store);
      } finally {
         server.stop();
      }

   }

   @Test
   public void testMultiFiltersBrowsing() throws Throwable {
      internalTestMultiFilters(true);
   }

   @Test
   public void testMultiFiltersRegularConsumer() throws Throwable {
      internalTestMultiFilters(false);
   }

   public void internalTestMultiFilters(boolean browsing) throws Throwable {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator().setBlockOnDurableSend(true);
         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(true, true, 0);

         session.createQueue(ADDRESS.toString(), "Q1", null, true);

         PagingStore store = server.getPagingManager().getPageStore(ADDRESS);

         ClientProducer prod = session.createProducer(ADDRESS);

         ClientMessage msg = null;
         store.startPaging();

         for (int i = 0; i < 100; i++) {
            msg = session.createMessage(true);
            msg.putStringProperty("color", "red");
            msg.putIntProperty("count", i);
            prod.send(msg);

            if (i > 0 && i % 10 == 0) {
               store.startPaging();
               store.forceAnotherPage();
            }
         }

         for (int i = 0; i < 100; i++) {
            msg = session.createMessage(true);
            msg.putStringProperty("color", "green");
            msg.putIntProperty("count", i);
            prod.send(msg);

            if (i > 0 && i % 10 == 0) {
               store.startPaging();
               store.forceAnotherPage();
            }
         }

         session.commit();

         session.close();

         session = sf.createSession(false, false, 0);
         session.start();

         ClientConsumer cons1;

         if (browsing) {
            cons1 = session.createConsumer("Q1", "color='green'", true);
         } else {
            cons1 = session.createConsumer("Q1", "color='red'", false);
         }

         for (int i = 0; i < 100; i++) {
            msg = cons1.receive(5000);

            System.out.println("Received " + msg);
            assertNotNull(msg);
            if (!browsing) {
               msg.acknowledge();
            }
         }

         session.commit();

         session.close();
      } finally {
         server.stop();
      }

   }

   @Test
   public void testPendingACKOutOfOrder() throws Throwable {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         locator.setBlockOnDurableSend(false);
         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(true, true, 0);

         session.createQueue(ADDRESS.toString(), "Q1", true);

         PagingStore store = server.getPagingManager().getPageStore(ADDRESS);

         store.startPaging();

         ClientProducer prod = session.createProducer(ADDRESS);

         for (int i = 0; i < 100; i++) {
            ClientMessage msg = session.createMessage(true);
            msg.putIntProperty("count", i);
            prod.send(msg);
            session.commit();
            if ((i + 1) % 5 == 0 && i < 50) {
               store.forceAnotherPage();
            }
         }

         session.start();

         ClientConsumer cons1 = session.createConsumer("Q1");

         for (int i = 0; i < 100; i++) {
            ClientMessage msg = cons1.receive(5000);
            assertNotNull(msg);

            if (i == 13) {
               msg.individualAcknowledge();
            }
         }

         session.close();

         locator.close();

         server.stop();

         server.start();

         store = server.getPagingManager().getPageStore(ADDRESS);

         locator = createInVMNonHALocator();

         sf = locator.createSessionFactory();

         session = sf.createSession(true, true, 0);
         cons1 = session.createConsumer("Q1");
         session.start();

         for (int i = 0; i < 99; i++) {
            ClientMessage msg = cons1.receive(5000);
            assertNotNull(msg);
            System.out.println("count = " + msg.getIntProperty("count"));
            msg.acknowledge();
         }

         assertNull(cons1.receiveImmediate());

         session.close();
         waitForNotPaging(store);
      } finally {
         server.stop();
      }

   }

   // Test a scenario where a page was complete and now needs to be cleared
   @Test
   public void testPageCompleteWasLive() throws Throwable {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         locator.setBlockOnDurableSend(false);
         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(true, true, 0);

         session.createQueue(ADDRESS.toString(), "Q1", "dest=1", true);
         session.createQueue(ADDRESS.toString(), "Q2", "dest=2", true);

         PagingStore store = server.getPagingManager().getPageStore(ADDRESS);

         store.startPaging();

         ClientProducer prod = session.createProducer(ADDRESS);

         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("dest", 1);
         prod.send(msg);

         msg = session.createMessage(true);
         msg.putIntProperty("dest", 2);
         prod.send(msg);

         session.start();

         ClientConsumer cons1 = session.createConsumer("Q1");

         ClientMessage msgReceivedCons1 = cons1.receive(1000);

         assertNotNull(msgReceivedCons1);

         ClientConsumer cons2 = session.createConsumer("Q2");
         ClientMessage msgReceivedCons2 = cons2.receive(1000);
         assertNotNull(msgReceivedCons2);

         store.forceAnotherPage();

         msg = session.createMessage(true);

         msg.putIntProperty("dest", 1);

         prod.send(msg);

         msgReceivedCons1.acknowledge();

         msgReceivedCons1 = cons1.receive(1000);
         assertNotNull(msgReceivedCons1);
         msgReceivedCons1.acknowledge();
         msgReceivedCons2.acknowledge();

         assertNull(cons1.receiveImmediate());
         assertNull(cons2.receiveImmediate());

         session.commit();

         session.close();

         waitForNotPaging(store);
      } finally {
         server.stop();
      }

   }

   @Test
   public void testNoCursors() throws Exception {
      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = sf.createSession();

      session.createQueue(ADDRESS, ADDRESS, true);
      ClientProducer prod = session.createProducer(ADDRESS);

      for (int i = 0; i < 100; i++) {
         Message msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[1024]);
         prod.send(msg);
      }

      session.commit();

      session.deleteQueue(ADDRESS);
      session.close();
      sf.close();
      locator.close();
      server.stop();
      server.start();
      waitForNotPaging(server.getPagingManager().getPageStore(ADDRESS));
      server.stop();

   }

   // Test a scenario where a page was complete and now needs to be cleared
   @Test
   public void testMoveMessages() throws Throwable {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);

      server.start();

      final int LARGE_MESSAGE_SIZE = 1024 * 1024;

      try {
         ServerLocator locator = createInVMNonHALocator();
         locator.setBlockOnDurableSend(false);
         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(true, true, 0);

         session.createQueue("Q1", "Q1", true);
         session.createQueue("Q2", "Q2", true);

         PagingStore store = server.getPagingManager().getPageStore(new SimpleString("Q1"));

         ClientProducer prod = session.createProducer("Q1");

         for (int i = 0; i < 50; i++) {
            ClientMessage msg = session.createMessage(true);
            msg.putIntProperty("count", i);
            if (i > 0 && i % 10 == 0) {
               msg.setBodyInputStream(createFakeLargeStream(LARGE_MESSAGE_SIZE));
            }
            prod.send(msg);
         }
         session.commit();

         store.startPaging();
         for (int i = 50; i < 100; i++) {
            ClientMessage msg = session.createMessage(true);
            msg.putIntProperty("count", i);
            if (i % 10 == 0) {
               msg.setBodyInputStream(createFakeLargeStream(LARGE_MESSAGE_SIZE));
            }
            prod.send(msg);
            if (i % 10 == 0) {
               session.commit();
               store.forceAnotherPage();
            }
         }
         session.commit();

         Queue queue = server.locateQueue(new SimpleString("Q1"));

         queue.moveReferences(10, (Filter) null, new SimpleString("Q2"), false);

         waitForNotPaging(store);

         session.close();
         locator.close();

         server.stop();
         server.start();

         locator = createInVMNonHALocator();
         locator.setBlockOnDurableSend(false);
         sf = locator.createSessionFactory();
         session = sf.createSession(true, true, 0);

         session.start();

         ClientConsumer cons = session.createConsumer("Q2");

         for (int i = 0; i < 100; i++) {
            ClientMessage msg = cons.receive(10000);
            assertNotNull(msg);
            if (i > 0 && i % 10 == 0) {
               byte[] largeMessageRead = new byte[LARGE_MESSAGE_SIZE];
               msg.getBodyBuffer().readBytes(largeMessageRead);
               for (int j = 0; j < LARGE_MESSAGE_SIZE; j++) {
                  assertEquals(largeMessageRead[j], getSamplebyte(j));
               }
            }
            msg.acknowledge();
            assertEquals(i, msg.getIntProperty("count").intValue());
         }

         assertNull(cons.receiveImmediate());

         waitForNotPaging(server.locateQueue(new SimpleString("Q2")));

         session.close();
         sf.close();
         locator.close();

      } finally {
         server.stop();
      }

   }

   @Override
   protected Configuration createDefaultInVMConfig() throws Exception {
      return super.createDefaultInVMConfig().setJournalSyncNonTransactional(false);
   }

   private static final class DummyOperationContext implements OperationContext {

      private final CountDownLatch pageUp;
      private final CountDownLatch pageDone;

      private DummyOperationContext(CountDownLatch pageUp, CountDownLatch pageDone) {
         this.pageDone = pageDone;
         this.pageUp = pageUp;
      }

      @Override
      public void onError(int errorCode, String errorMessage) {
      }

      @Override
      public void done() {
      }

      @Override
      public void storeLineUp() {
      }

      @Override
      public boolean waitCompletion(long timeout) throws Exception {
         return false;
      }

      @Override
      public void waitCompletion() throws Exception {

      }

      @Override
      public void replicationLineUp() {

      }

      @Override
      public void replicationDone() {

      }

      @Override
      public void pageSyncLineUp() {
         pageUp.countDown();
      }

      @Override
      public void pageSyncDone() {
         pageDone.countDown();
      }

      @Override
      public void executeOnCompletion(IOCallback runnable) {
         runnable.done();
      }

      @Override
      public void executeOnCompletion(IOCallback runnable, boolean storeOnly) {
         runnable.done();
      }
   }
}
