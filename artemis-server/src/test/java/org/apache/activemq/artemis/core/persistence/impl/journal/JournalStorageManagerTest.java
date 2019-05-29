/*
 * Copyright The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.persistence.impl.journal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.impl.JournalLoader;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.stream.Collectors.toList;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class JournalStorageManagerTest {

   @Parameterized.Parameter
   public JournalType journalType;

   @Parameterized.Parameters(name = "journal type={0}")
   public static Collection<Object[]> getParams() {
      final JournalType[] values = JournalType.values();
      return Stream.of(JournalType.values())
         .map(journalType -> new Object[]{journalType})
         .collect(toList());
   }

   private static ExecutorService executor;
   private static ExecutorService ioExecutor;
   private static ExecutorService testExecutor;

   @BeforeClass
   public static void initExecutors() {
      executor = Executors.newSingleThreadExecutor();
      //to allow concurrent compaction and I/O operations
      ioExecutor = Executors.newFixedThreadPool(2);
      testExecutor = Executors.newSingleThreadExecutor();
   }

   @AfterClass
   public static void destroyExecutors() {
      ioExecutor.shutdownNow();
      executor.shutdownNow();
      testExecutor.shutdownNow();
   }

   /**
    * Test of fixJournalFileSize method, of class JournalStorageManager.
    */
   @Test
   public void testFixJournalFileSize() {
      if (journalType == JournalType.ASYNCIO) {
         assumeTrue("AIO is not supported on this platform", AIOSequentialFileFactory.isSupported());
      }
      final ConfigurationImpl configuration = new ConfigurationImpl().setJournalType(journalType);
      final ExecutorFactory executorFactory = new OrderedExecutorFactory(executor);
      final ExecutorFactory ioExecutorFactory = new OrderedExecutorFactory(ioExecutor);
      final JournalStorageManager manager = spy(new JournalStorageManager(configuration, null, executorFactory, null, ioExecutorFactory));
      Assert.assertEquals(4096, manager.fixJournalFileSize(1024, 4096));
      Assert.assertEquals(4096, manager.fixJournalFileSize(4098, 4096));
      Assert.assertEquals(8192, manager.fixJournalFileSize(8192, 4096));
   }

   @Test(timeout = 20_000)
   public void testStopReplicationDoesNotDeadlockWhileStopping() throws Exception {
      if (journalType == JournalType.ASYNCIO) {
         assumeTrue("AIO is not supported on this platform", AIOSequentialFileFactory.isSupported());
      }
      final ConfigurationImpl configuration = new ConfigurationImpl().setJournalType(journalType);
      final ExecutorFactory executorFactory = new OrderedExecutorFactory(executor);
      final ExecutorFactory ioExecutorFactory = new OrderedExecutorFactory(ioExecutor);
      final JournalStorageManager manager = spy(new JournalStorageManager(configuration, null, executorFactory, null, ioExecutorFactory));
      manager.start();
      manager.loadBindingJournal(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
      final PostOffice postOffice = mock(PostOffice.class);
      final JournalLoader journalLoader = mock(JournalLoader.class);
      manager.loadMessageJournal(postOffice, null, null, null, null, null, null, journalLoader);
      final ReplicationManager replicationManager = mock(ReplicationManager.class);
      final PagingManager pagingManager = mock(PagingManager.class);
      when(pagingManager.getStoreNames()).thenReturn(new SimpleString[0]);
      manager.startReplication(replicationManager, pagingManager, UUID.randomUUID().toString(), false, 0);
      final LargeServerMessage largeMessage = manager.createLargeMessage(manager.generateID() + 1, new CoreMessage());
      largeMessage.setDurable(true);
      when(replicationManager.isSynchronizing()).thenReturn(true);
      largeMessage.deleteFile();
      final long pendingRecordID = largeMessage.getPendingRecordID();
      final AtomicReference<CompletableFuture<Void>> stopReplication = new AtomicReference<>();
      doAnswer(invocation -> {
         final CompletableFuture<Void> finished = new CompletableFuture<>();
         final CountDownLatch beforeStopReplication;
         if (stopReplication.compareAndSet(null, finished)) {
            beforeStopReplication = new CountDownLatch(1);
            //before the deadlock fix:
            //manager::stop is already owning the large message lock here
            //but not yet the manager read lock
            testExecutor.execute(() -> {
               beforeStopReplication.countDown();
               try {
                  //it needs to acquire the manager write lock
                  //and large message lock next
                  manager.stopReplication();
                  finished.complete(null);
               } catch (Exception e) {
                  finished.completeExceptionally(e);
               }
            });
         } else {
            beforeStopReplication = null;
         }
         if (beforeStopReplication != null) {
            beforeStopReplication.await();
            //do not remove this sleep: before the deadlock fix
            //it was needed to give manager::stopReplication the chance to acquire
            //the manager write lock before manager::stop
            TimeUnit.MILLISECONDS.sleep(500);
         }
         //confirmPendingLargeMessage will acquire manager read lock
         return invocation.callRealMethod();
      }).when(manager).confirmPendingLargeMessage(pendingRecordID);
      manager.stop();
      final CompletableFuture<Void> stoppedReplication = stopReplication.get();
      Assert.assertNotNull(stoppedReplication);
      stoppedReplication.get();
   }

}
