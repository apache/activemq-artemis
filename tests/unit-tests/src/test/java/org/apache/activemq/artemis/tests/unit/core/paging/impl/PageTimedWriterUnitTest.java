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

package org.apache.activemq.artemis.tests.unit.core.paging.impl;

import javax.transaction.xa.Xid;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.OperationConsistencyLevel;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.paging.impl.PageTimedWriter;
import org.apache.activemq.artemis.core.paging.impl.PagedMessageImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreImpl;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManagerAccessor;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.apache.activemq.artemis.tests.util.ArtemisTestCase;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PageTimedWriterUnitTest extends ArtemisTestCase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final SimpleString ADDRESS = SimpleString.of("someAddress");

   ScheduledExecutorService scheduledExecutorService;
   ExecutorService executorService;
   OrderedExecutorFactory executorFactory;
   OperationContext context;

   RouteContextList routeContextList;

   // almost real as we use an extension that can allow mocking internal objects such as journals and replicationManager
   JournalStorageManager realJournalStorageManager;

   PagingStoreImpl pageStore;

   CountDownLatch allowRunning;

   final ReusableLatch enteredSync = new ReusableLatch(1);
   final ReusableLatch allowSync = new ReusableLatch(0);
   final AtomicInteger doneSync = new AtomicInteger(0);

   final AtomicInteger pageWrites = new AtomicInteger(0);

   PageTimedWriter timer;

   Configuration configuration;

   SequentialFileFactory inMemoryFileFactory;

   Journal mockBindingsJournal;
   Journal mockMessageJournal;

   AtomicInteger numberOfCommitsMessageJournal = new AtomicInteger(0);
   AtomicInteger numberOfPreparesMessageJournal = new AtomicInteger(0);

   AtomicBoolean useReplication = new AtomicBoolean(false);
   AtomicBoolean returnSynchronizing = new AtomicBoolean(false);
   ReplicationManager mockReplicationManager;


   private class MockableJournalStorageManager extends JournalStorageManager {

      MockableJournalStorageManager(Configuration config,
                                           Journal bindingsJournal,
                                           Journal messagesJournal,
                                           ExecutorFactory executorFactory,
                                           ExecutorFactory ioExecutors) {
         super(config, Mockito.mock(CriticalAnalyzer.class), executorFactory, ioExecutors);
         this.bindingsJournal = bindingsJournal;
         this.messageJournal = messagesJournal;
      }

      @Override
      public void start() throws Exception {
         super.start();
         idGenerator.forceNextID(1);
      }

      @Override
      protected void createDirectories() {
         // not creating any folders
      }

      @Override
      public void pageWrite(final SimpleString address, final PagedMessage message, final long pageNumber, boolean storageUp, boolean originallyReplicated) {
         super.pageWrite(address, message, pageNumber, storageUp, originallyReplicated);
         pageWrites.incrementAndGet();
      }
   }

   private CoreMessage createMessage() {
      long id = realJournalStorageManager.generateID();
      ActiveMQBuffer buffer = RandomUtil.randomBuffer(10);
      CoreMessage msg = new CoreMessage(id, 50 + buffer.capacity());

      msg.setAddress(ADDRESS);

      msg.getBodyBuffer().resetReaderIndex();
      msg.getBodyBuffer().resetWriterIndex();

      msg.getBodyBuffer().writeBytes(buffer, buffer.capacity());

      return msg;
   }

   private int commitCall() {
      return numberOfCommitsMessageJournal.incrementAndGet();
   }

   private int prepareCall() {
      return numberOfPreparesMessageJournal.incrementAndGet();
   }


   @BeforeEach
   public void setupMocks() throws Exception {
      configuration = new ConfigurationImpl();
      configuration.setJournalType(JournalType.NIO);
      scheduledExecutorService = Executors.newScheduledThreadPool(10);
      executorService = Executors.newFixedThreadPool(10);
      runAfter(scheduledExecutorService::shutdownNow);
      runAfter(executorService::shutdownNow);
      runAfter(() -> OperationContextImpl.clearContext());
      executorFactory = new OrderedExecutorFactory(executorService);
      context = OperationContextImpl.getContext(executorFactory);
      assertNotNull(context);

      mockBindingsJournal = Mockito.mock(Journal.class);
      mockMessageJournal = Mockito.mock(Journal.class);

      Mockito.doAnswer(a -> commitCall()).when(mockMessageJournal).appendCommitRecord(Mockito.anyLong(), Mockito.anyBoolean());
      Mockito.doAnswer(a -> commitCall()).when(mockMessageJournal).appendCommitRecord(Mockito.anyLong(), Mockito.anyBoolean(), Mockito.any());
      Mockito.doAnswer(a -> commitCall()).when(mockMessageJournal).appendCommitRecord(Mockito.anyLong(), Mockito.anyBoolean(), Mockito.any(), Mockito.anyBoolean());

      Mockito.doAnswer(a -> prepareCall()).when(mockMessageJournal).appendPrepareRecord(Mockito.anyLong(), (byte[]) Mockito.any(), Mockito.anyBoolean());
      Mockito.doAnswer(a -> prepareCall()).when(mockMessageJournal).appendPrepareRecord(Mockito.anyLong(), Mockito.any(EncodingSupport.class), Mockito.anyBoolean(), Mockito.any());
      Mockito.doAnswer(a -> prepareCall()).when(mockMessageJournal).appendPrepareRecord(Mockito.anyLong(), Mockito.any(EncodingSupport.class), Mockito.anyBoolean());

      mockReplicationManager = Mockito.mock(ReplicationManager.class);
      Mockito.when(mockReplicationManager.isStarted()).thenAnswer(a -> useReplication.get());
      Mockito.when(mockReplicationManager.isSynchronizing()).thenAnswer(a -> returnSynchronizing.get());
      Mockito.doAnswer(a -> {
         if (useReplication.get()) {
            OperationContext ctx = OperationContextImpl.getContext();
            if (ctx != null) {
               ctx.replicationDone();
            }
         }
         return null;
      }).when(mockReplicationManager).pageWrite(Mockito.any(SimpleString.class), Mockito.any(PagedMessage.class), Mockito.anyLong(), Mockito.anyBoolean());

      realJournalStorageManager = new MockableJournalStorageManager(configuration, mockBindingsJournal, mockMessageJournal, executorFactory, executorFactory);
      realJournalStorageManager.start();

      JournalStorageManagerAccessor.setReplicationManager(realJournalStorageManager, mockReplicationManager);

      allowRunning = new CountDownLatch(1);

      inMemoryFileFactory = new FakeSequentialFileFactory();

      PagingStoreFactory mockPageStoreFactory = Mockito.mock(PagingStoreFactory.class);
      PagingManager mockPagingManager = Mockito.mock(PagingManager.class);

      final int MAX_SIZE = 1024 * 10;

      AddressSettings settings = new AddressSettings().setPageSizeBytes(MAX_SIZE).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      pageStore = new PagingStoreImpl(ADDRESS, scheduledExecutorService, 100, mockPagingManager, realJournalStorageManager, inMemoryFileFactory,
                                      mockPageStoreFactory, ADDRESS, settings, executorFactory.getExecutor(), false) {
         @Override
         protected PageTimedWriter createPageTimedWriter(ScheduledExecutorService scheduledExecutor, long syncTimeout) {

            PageTimedWriter timer = new PageTimedWriter(100_000, realJournalStorageManager, this, scheduledExecutorService, executorFactory.getExecutor(), true, 100) {
               @Override
               public void run() {
                  try {
                     allowRunning.await();
                  } catch (InterruptedException e) {
                     logger.warn(e.getMessage(), e);
                     Thread.currentThread().interrupt();

                  }
                  super.run();
               }

               @Override
               protected void performSync() throws Exception {
                  enteredSync.countDown();
                  super.performSync();
                  try {
                     allowSync.await();
                  } catch (InterruptedException e) {
                     logger.warn(e.getMessage(), e);
                  }
                  doneSync.incrementAndGet();
               }
            };

            timer.start();
            return timer;

         }
      };

      timer = pageStore.getPageTimedWriter();

      pageStore.start();
      pageStore.startPaging();

      routeContextList = new RoutingContextImpl.ContextListing();
      Queue mockQueue = Mockito.mock(Queue.class);
      Mockito.when(mockQueue.getID()).thenReturn(1L);
      routeContextList.addAckedQueue(mockQueue);
   }

   // a test to validate if the Mocks are correctly setup
   @Test
   public void testValidateMocks() throws Exception {
      TransactionImpl tx = new TransactionImpl(realJournalStorageManager);
      tx.setContainsPersistent();
      AtomicInteger count = new AtomicInteger(0);
      tx.addOperation(new TransactionOperationAbstract() {
         @Override
         public void afterCommit(Transaction tx) {
            count.incrementAndGet();
         }
      });
      assertEquals(0, count.get());
      tx.commit();
      assertEquals(1, count.get(), "tx.commit is not correctly wired on mocking");


      realJournalStorageManager.afterCompleteOperations(new IOCallback() {
         @Override
         public void done() {
            count.incrementAndGet();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      });

      realJournalStorageManager.afterCompleteOperations(new IOCallback() {
         @Override
         public void done() {
            count.incrementAndGet();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      }, OperationConsistencyLevel.FULL);

      assertEquals(3, count.get(), "afterCompletion is not correctly wired on mocking");

      long id = realJournalStorageManager.generateID();
      long newID = realJournalStorageManager.generateID();
      assertEquals(1L, newID - id);

   }

   PagedMessage createPagedMessage() {
      return new PagedMessageImpl(createMessage(), new long[] {1}, -1);
   }

   @Test
   public void testIOCompletion() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      timer.addTask(context, createPagedMessage(), null, Mockito.mock(RouteContextList.class));

      context.executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
         }
      });

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.SECONDS));

   }

   @Test
   public void testIOCompletionWhileReplica() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      useReplication.set(true);

      timer.addTask(context, createPagedMessage(), null, Mockito.mock(RouteContextList.class));

      context.executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
         }
      });

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   @Test
   public void testTXCompletionWhileReplica() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      useReplication.set(true);

      TransactionImpl transaction = new TransactionImpl(realJournalStorageManager);
      transaction.setContainsPersistent();
      transaction.addOperation(new TransactionOperationAbstract() {
         @Override
         public void afterCommit(Transaction tx) {
            super.afterCommit(tx);
            latch.countDown();
         }
      });

      timer.addTask(context, createPagedMessage(), transaction, Mockito.mock(RouteContextList.class));

      transaction.commit();

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   @Test
   public void testAllowWriteWhileSyncPending() throws Exception {
      int numberOfMessages = 100;
      CountDownLatch latch = new CountDownLatch(numberOfMessages);

      allowRunning.countDown();
      useReplication.set(false);

      assertEquals(0, doneSync.get());
      enteredSync.setCount(1);
      allowSync.setCount(1);

      for (int i = 0; i < numberOfMessages; i++) {
         TransactionImpl newTX = new TransactionImpl(realJournalStorageManager);
         newTX.setContainsPersistent();
         newTX.addOperation(new TransactionOperationAbstract() {
            @Override
            public void afterCommit(Transaction tx) {
               super.afterCommit(tx);
               latch.countDown();
            }
         });
         pageStore.page(createMessage(), newTX, routeContextList);
         newTX.commit();

         if (i == 0) {
            assertTrue(enteredSync.await(10, TimeUnit.SECONDS));
            assertEquals(0, doneSync.get()); // it should not complete until allowSync is released
         }
      }

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      allowSync.countDown();
      assertTrue(latch.await(10, TimeUnit.SECONDS));

      assertEquals(numberOfMessages, pageWrites.get());
   }

   @Test
   public void testTXCompletionWhileDisableReplica() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      useReplication.set(true);

      TransactionImpl transaction = new TransactionImpl(realJournalStorageManager);
      transaction.setContainsPersistent();
      transaction.addOperation(new TransactionOperationAbstract() {
         @Override
         public void afterCommit(Transaction tx) {
            super.afterCommit(tx);
            latch.countDown();
         }
      });

      timer.addTask(context, createPagedMessage(), transaction, Mockito.mock(RouteContextList.class));

      numberOfCommitsMessageJournal.set(0); // it should been 0 before anyway but since I have no real reason to require it to be zero before, I'm doing this just in case it ever changes
      transaction.commit();
      Assertions.assertEquals(0, numberOfCommitsMessageJournal.get());
      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      useReplication.set(false);
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      Wait.assertEquals(1, numberOfCommitsMessageJournal::get, 5000, 100);
   }


   @Test
   public void testTXCompletionPrepare() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      useReplication.set(true);

      TransactionImpl transaction = new TransactionImpl(Mockito.mock(Xid.class), realJournalStorageManager, -1);
      transaction.setContainsPersistent();
      transaction.addOperation(new TransactionOperationAbstract() {
         @Override
         public void afterPrepare(Transaction tx) {
            super.afterCommit(tx);
            latch.countDown();
         }
      });

      timer.addTask(context, createPagedMessage(), transaction, Mockito.mock(RouteContextList.class));

      numberOfCommitsMessageJournal.set(0); // it should been 0 before anyway but since I have no real reason to require it to be zero before, I'm doing this just in case it ever changes
      numberOfPreparesMessageJournal.set(0);
      transaction.prepare();
      Assertions.assertEquals(0, numberOfCommitsMessageJournal.get());
      Assertions.assertEquals(0, numberOfPreparesMessageJournal.get());
      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assertions.assertEquals(0, numberOfCommitsMessageJournal.get());
      Wait.assertEquals(1, numberOfPreparesMessageJournal::get, 5000, 100);
   }


   // add a task while replicating, process it when no longer replicating (disconnect a node scenario)
   @Test
   public void testDisableReplica() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      useReplication.set(true);

      timer.addTask(context, createPagedMessage(), null, Mockito.mock(RouteContextList.class));

      context.executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
         }
      });

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      useReplication.set(false);
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   // add a task while not replicating, process it when is now replicating (reconnect a node scenario)
   // this is the oppostie from testDisableReplica
   @Test
   public void testEnableReplica() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      useReplication.set(false);

      timer.addTask(context, createPagedMessage(), null, Mockito.mock(RouteContextList.class));

      context.executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
         }
      });

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      useReplication.set(true);
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   @Test
   public void testTXCompletion() throws Exception {
      ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
      ExecutorService executorService = Executors.newFixedThreadPool(10);
      runAfter(scheduledExecutorService::shutdownNow);
      runAfter(executorService::shutdownNow);
      runAfter(() -> OperationContextImpl.clearContext());

      OrderedExecutorFactory executorFactory = new OrderedExecutorFactory(executorService);

      OperationContextImpl.clearContext();

      OperationContext context = OperationContextImpl.getContext(executorFactory);

      CountDownLatch latch = new CountDownLatch(1);

      Transaction tx = new TransactionImpl(realJournalStorageManager, Integer.MAX_VALUE);
      tx.setContainsPersistent();

      timer.addTask(context, createPagedMessage(), tx, Mockito.mock(RouteContextList.class));
      tx.addOperation(new TransactionOperationAbstract() {
         @Override
         public void afterCommit(Transaction tx) {
            latch.countDown();
         }
      });
      tx.commit();

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));

      allowRunning.countDown();

      assertTrue(latch.await(10, TimeUnit.SECONDS));

   }

}
