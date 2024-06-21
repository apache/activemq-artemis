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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.message.impl.CoreMessagePersister;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.paging.cursor.PageCursorProvider;
import org.apache.activemq.artemis.core.paging.cursor.PageIterator;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.PagedReference;
import org.apache.activemq.artemis.core.paging.cursor.PagedReferenceImpl;
import org.apache.activemq.artemis.core.paging.cursor.impl.PageCursorProviderImpl;
import org.apache.activemq.artemis.core.paging.cursor.impl.PageCursorProviderTestAccessor;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.paging.impl.PageReadWriter;
import org.apache.activemq.artemis.core.paging.impl.PageTransactionInfoImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreTestAccessor;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessagePersister;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.apache.activemq.artemis.tests.unit.core.postoffice.impl.fakes.FakeQueue;
import org.apache.activemq.artemis.tests.unit.util.FakePagingManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.activemq.artemis.utils.collections.LinkedList;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

public class PagingStoreImplTest extends ActiveMQTestBase {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   static {
      MessagePersister.registerPersister(CoreMessagePersister.getInstance());
      MessagePersister.registerPersister(AMQPMessagePersister.getInstance());
   }

   private static final SimpleString destinationTestName = SimpleString.of("test");

   protected ExecutorService executor;

   @Test
   public void testAddAndRemoveMessages() {
      long id1 = RandomUtil.randomLong();
      long id2 = RandomUtil.randomLong();
      PageTransactionInfo trans = new PageTransactionInfoImpl(id2);

      trans.setRecordID(id1);

      // anything between 2 and 100
      int nr1 = RandomUtil.randomPositiveInt() % 98 + 2;

      for (int i = 0; i < nr1; i++) {
         trans.increment(1, 0);
      }

      assertEquals(nr1, trans.getNumberOfMessages());

      ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(trans.getEncodeSize());

      trans.encode(buffer);

      PageTransactionInfo trans2 = new PageTransactionInfoImpl(id1);
      trans2.decode(buffer);

      assertEquals(id2, trans2.getTransactionID());

      assertEquals(nr1, trans2.getNumberOfMessages());

   }

   @Test
   public void testDoubleStart() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, new FakeStoreFactory(factory), PagingStoreImplTest.destinationTestName, new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE), getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      // this is not supposed to throw an exception.
      // As you could have start being called twice as Stores are dynamically
      // created, on a multi-thread environment
      storeImpl.start();

      storeImpl.stop();

   }

   @Test
   public void testPageWithNIO() throws Exception {
      ActiveMQTestBase.recreateDirectory(getTestDir());
      testConcurrentPaging(new NIOSequentialFileFactory(new File(getTestDir()), 1).setDatasync(false), 1);
   }

   @Test
   public void testStore() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      AddressSettings addressSettings = new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      PagingStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, PagingStoreImplTest.destinationTestName, addressSettings, getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      assertEquals(1, storeImpl.getNumberOfPages());

      List<ActiveMQBuffer> buffers = new ArrayList<>();

      ActiveMQBuffer buffer = createRandomBuffer(0, 10);

      buffers.add(buffer);
      SimpleString destination = SimpleString.of("test");

      Message msg = createMessage(1, storeImpl, destination, buffer);

      assertTrue(storeImpl.isPaging());

      final RoutingContextImpl ctx = new RoutingContextImpl(null);
      assertTrue(storeImpl.page(msg, ctx.getTransaction(), ctx.getContextListing(storeImpl.getStoreName())));

      assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.addSyncPoint(OperationContextImpl.getContext());

      storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, PagingStoreImplTest.destinationTestName, addressSettings, getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.stop();
   }

   @Test
   public void testDepageOnCurrentPage() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      SimpleString destination = SimpleString.of("test");

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingStoreImpl storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, PagingStoreImplTest.destinationTestName, new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE), getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      List<ActiveMQBuffer> buffers = new ArrayList<>();

      int numMessages = 10;

      for (int i = 0; i < numMessages; i++) {

         ActiveMQBuffer buffer = createRandomBuffer(i + 1L, 10);

         buffers.add(buffer);

         Message msg = createMessage(i, storeImpl, destination, buffer);
         final RoutingContextImpl ctx = new RoutingContextImpl(null);
         assertTrue(storeImpl.page(msg, ctx.getTransaction(), ctx.getContextListing(storeImpl.getStoreName())));

      }

      assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.addSyncPoint(OperationContextImpl.getContext());

      Page page = storeImpl.depage();

      page.open(true);

      LinkedList<PagedMessage> msg = page.read(new NullStorageManager());

      assertEquals(numMessages, msg.size());
      assertEquals(1, storeImpl.getNumberOfPages());

      page.close(false);
      page = storeImpl.depage();

      assertNull(page);

      assertEquals(0, storeImpl.getNumberOfPages());

      for (int i = 0; i < numMessages; i++) {
         ActiveMQBuffer horn1 = buffers.get(i);
         ActiveMQBuffer horn2 = msg.get(i).getMessage().toCore().getBodyBuffer();
         horn1.resetReaderIndex();
         horn2.resetReaderIndex();
         for (int j = 0; j < horn1.writerIndex(); j++) {
            assertEquals(horn1.readByte(), horn2.readByte());
         }
      }

   }

   @Test
   public void testRemoveInTheMiddle() throws Exception {
      SequentialFileFactory factory = new NIOSequentialFileFactory(getTestDirfile(), 1);

      SimpleString destination = SimpleString.of("test");

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingStoreImpl storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, PagingStoreImplTest.destinationTestName, new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE), getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);
      PageSubscription subscription = storeImpl.getCursorProvider().createSubscription(1, null, true);
      FakeQueue fakeQueue = new FakeQueue(destination, 1).setDurable(true).setPageSubscription(subscription);

      storeImpl.start();

      for (int repeat = 0; repeat < 5; repeat++) {
         logger.debug("###############################################################################################################################");
         logger.debug("#repeat {}", repeat);

         storeImpl.startPaging();
         assertEquals(1, storeImpl.getNumberOfPages());
         storeImpl.getCursorProvider().disableCleanup();

         int numMessages = 100;
         {
            int page = 1;

            for (int i = 0; i < numMessages; i++) {
               ActiveMQBuffer buffer = createRandomBuffer(i + 1L, 10);

               Message msg = createMessage(i, storeImpl, destination, buffer);
               msg.putIntProperty("i", i);
               msg.putIntProperty("page", page);
               final RoutingContextImpl ctx = new RoutingContextImpl(null);
               ctx.addQueue(fakeQueue.getName(), fakeQueue);
               assertTrue(storeImpl.page(msg, ctx.getTransaction(), ctx.getContextListing(storeImpl.getStoreName())));
               if (i > 0 && i % 10 == 0) {
                  storeImpl.forceAnotherPage();
                  page++;
                  assertEquals(page, storeImpl.getNumberOfPages());
               }
            }
         }

         assertEquals(numMessages / 10, storeImpl.getNumberOfPages());

         PageIterator iterator = subscription.iterator();
         for (int i = 0; i < numMessages; i++) {
            assertTrue(iterator.hasNext());
            PagedReference reference = iterator.next();
            assertNotNull(reference);
            assertEquals(i, reference.getPagedMessage().getMessage().getIntProperty("i").intValue());
            int pageOnMsg = reference.getMessage().getIntProperty("page").intValue();
            if (pageOnMsg > 2 && pageOnMsg < 10) {
               subscription.ack(reference);
            }
         }
         iterator.close();

         if (logger.isDebugEnabled()) {
            debugPage(storeImpl, subscription, storeImpl.getFirstPage(), storeImpl.getCurrentWritingPage());
         }

         PageCursorProviderTestAccessor.cleanup(storeImpl.getCursorProvider());

         assertTrue(storeImpl.isPaging());

         int messagesRead = 0;
         iterator = subscription.iterator();
         while (iterator.hasNext()) {
            PagedReference reference = iterator.next();
            if (reference == null) {
               break;
            }

            assertTrue(subscription.contains(reference));

            logger.debug("#received message {}, {}", messagesRead, reference);
            messagesRead++;
            int pageOnMsg = reference.getMessage().getIntProperty("page");
            assertTrue(pageOnMsg <= 2 || pageOnMsg >= 10, "received " + reference);

            subscription.ack(reference);
         }
         iterator.close();

         assertEquals(30, messagesRead);

         assertEquals(3, storeImpl.getNumberOfPages());

         PageCursorProviderTestAccessor.cleanup(storeImpl.getCursorProvider());

         assertFalse(storeImpl.isPaging());

         assertEquals(1, PagingStoreTestAccessor.getUsedPagesSize(storeImpl));

         assertEquals(1, storeImpl.getNumberOfPages());
      }
   }

   private void debugPage(PagingStoreImpl storeImpl, PageSubscription subscription, long startPage, long endPage) throws Exception {
      for (long pgID = startPage; pgID <= endPage; pgID++) {
         Page page = storeImpl.newPageObject(pgID);
         page.open(false);
         logger.debug("# Page {}", pgID);
         page.getMessages().forEach(p -> {
            String acked;
            try {
               acked = subscription.contains(new PagedReferenceImpl(p, subscription)) + "...";
            } catch (Exception e) {
               e.printStackTrace();
               acked = "";
            }
            logger.debug("{}{}", acked, p);
         });
         page.close(false);
      }
   }

   @Test
   public void testRemoveCurrentPage() throws Exception {
      SequentialFileFactory factory = new NIOSequentialFileFactory(getTestDirfile(), 1);

      SimpleString destination = SimpleString.of("test");

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingStoreImpl storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, PagingStoreImplTest.destinationTestName, new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE), getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);
      PageSubscription subscription = storeImpl.getCursorProvider().createSubscription(1, null, true);
      FakeQueue fakeQueue = new FakeQueue(destination, 1).setDurable(true).setPageSubscription(subscription);

      storeImpl.getCursorProvider().disableCleanup();
      storeImpl.start();

      for (int repeat = 0; repeat < 5; repeat++) {

         logger.debug("#repeat {}", repeat);

         storeImpl.startPaging();

         int numMessages = 97;
         {
            int page = 1;

            for (int i = 1; i <= numMessages; i++) {
               ActiveMQBuffer buffer = createRandomBuffer(i + 1L, 10);

               Message msg = createMessage(i, storeImpl, destination, buffer);
               msg.putIntProperty("i", i);
               msg.putIntProperty("page", page);
               final RoutingContextImpl ctx = new RoutingContextImpl(null);
               ctx.addQueue(fakeQueue.getName(), fakeQueue);
               assertTrue(storeImpl.page(msg, ctx.getTransaction(), ctx.getContextListing(storeImpl.getStoreName())));
               if (i > 0 && i % 10 == 0) {
                  storeImpl.forceAnotherPage();
                  page++;
               }
            }
         }

         assertEquals(10, storeImpl.getNumberOfPages());

         assertEquals(10, factory.listFiles("page").size());

         int messagesRead = 0;
         PageIterator iterator = subscription.iterator();
         for (int i = 1; i <= numMessages; i++) {
            assertTrue(iterator.hasNext());
            PagedReference reference = iterator.next();
            assertNotNull(reference);
            assertEquals(i, reference.getPagedMessage().getMessage().getIntProperty("i").intValue());
            int pageOnMsg = reference.getMessage().getIntProperty("page").intValue();
            if (pageOnMsg == 10) {
               messagesRead++;
               subscription.ack(reference);
            }
         }
         iterator.close();

         assertEquals(7, messagesRead);

         PageCursorProviderTestAccessor.cleanup(storeImpl.getCursorProvider());

         assertEquals(10, factory.listFiles("page").size());

         assertTrue(storeImpl.isPaging());

         storeImpl.forceAnotherPage();

         assertEquals(11, factory.listFiles("page").size());

         PageCursorProviderTestAccessor.cleanup(storeImpl.getCursorProvider());

         assertEquals(10, factory.listFiles("page").size());

         assertEquals(10, storeImpl.getNumberOfPages());

         assertEquals(11 + 10 * repeat, storeImpl.getCurrentWritingPage());

         messagesRead = 0;
         iterator = subscription.iterator();
         while (iterator.hasNext()) {
            PagedReference reference = iterator.next();
            if (reference == null) {
               break;
            }
            messagesRead++;
            int pageOnMsg = reference.getMessage().getIntProperty("page");
            assertTrue(pageOnMsg != 10);

            subscription.ack(reference);
         }
         iterator.close();

         assertEquals(90, messagesRead);

         PageCursorProviderTestAccessor.cleanup(storeImpl.getCursorProvider());

         assertFalse(storeImpl.isPaging());
      }
   }


   @Test
   public void testReadNumberOfMessages() throws Exception {
      SequentialFileFactory factory = new NIOSequentialFileFactory(getTestDirfile(), 1);

      SimpleString destination = SimpleString.of("test");

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      StorageManager storageManager = createStorageManagerMock();

      PagingStoreImpl storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), storageManager, factory, storeFactory, PagingStoreImplTest.destinationTestName, new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE), getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);
      PageSubscription subscription = storeImpl.getCursorProvider().createSubscription(1, null, true);
      FakeQueue fakeQueue = new FakeQueue(destination, 1).setDurable(true).setPageSubscription(subscription);

      storeImpl.getCursorProvider().disableCleanup();
      storeImpl.start();
      storeImpl.startPaging();
      for (int i = 1; i <= 10; i++) {
         ActiveMQBuffer buffer = createRandomBuffer(i + 1L, 10);

         Message msg = createMessage(i, storeImpl, destination, buffer);
         msg.putIntProperty("i", i);
         msg.putIntProperty("page", 1);
         final RoutingContextImpl ctx = new RoutingContextImpl(null);
         ctx.addQueue(fakeQueue.getName(), fakeQueue);
         assertTrue(storeImpl.page(msg, ctx.getTransaction(), ctx.getContextListing(storeImpl.getStoreName())));
      }

      assertEquals(1, storeImpl.getNumberOfPages());

      assertEquals(1, factory.listFiles("page").size());

      String fileName = storeImpl.createFileName(1);

      ArrayList<PagedMessage> messages = new ArrayList<>();

      SequentialFile file = factory.createSequentialFile(storeImpl.createFileName(1));

      file.open();
      int size = PageReadWriter.readFromSequentialFile(storageManager, storeImpl.getStoreName(), factory, file, 1, messages::add, PageReadWriter.SKIP_ALL, null, null);
      file.close();

      assertEquals(0, messages.size());

      assertEquals(10, size);

   }

   @Test
   public void testDepageMultiplePages() throws Exception {
      SequentialFileFactory factory = new NIOSequentialFileFactory(new File(getPageDir()), 1).setDatasync(false);
      SimpleString destination = SimpleString.of("test");

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingStoreImpl store = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, PagingStoreImplTest.destinationTestName, new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE), getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);

      store.start();

      assertEquals(0, store.getNumberOfPages());

      store.startPaging();

      assertEquals(1, store.getNumberOfPages());

      List<ActiveMQBuffer> buffers = new ArrayList<>();

      for (int i = 0; i < 10; i++) {

         ActiveMQBuffer buffer = createRandomBuffer(i + 1L, 10);

         buffers.add(buffer);

         if (i == 5) {
            store.forceAnotherPage();
         }

         Message msg = createMessage(i, store, destination, buffer);

         final RoutingContextImpl ctx = new RoutingContextImpl(null);
         assertTrue(store.page(msg, ctx.getTransaction(), ctx.getContextListing(store.getStoreName())));
      }

      assertEquals(2, store.getNumberOfPages());

      store.addSyncPoint(OperationContextImpl.getContext());

      int sequence = 0;

      for (int pageNr = 0; pageNr < 2; pageNr++) {
         Page page = store.depage();

         logger.debug("numberOfPages = {}", store.getNumberOfPages());

         page.open(true);

         LinkedList<PagedMessage> msg = page.read(new NullStorageManager());

         page.close(false, false);

         assertEquals(5, msg.size());

         for (int i = 0; i < 5; i++) {
            assertEquals(sequence++, msg.get(i).getMessage().getMessageID());
            ActiveMQTestBase.assertEqualsBuffers(18, buffers.get(pageNr * 5 + i), msg.get(i).getMessage().toCore().getBodyBuffer());
         }
      }

      assertEquals(1, store.getNumberOfPages());

      assertTrue(store.isPaging());

      Message msg = createMessage(1, store, destination, buffers.get(0));

      final RoutingContextImpl ctx = new RoutingContextImpl(null);
      assertTrue(store.page(msg, ctx.getTransaction(), ctx.getContextListing(store.getStoreName())));

      Page newPage = store.depage();

      newPage.open(true);

      assertEquals(1, newPage.read(new NullStorageManager()).size());

      newPage.delete(null);

      assertEquals(1, store.getNumberOfPages());

      assertTrue(store.isPaging());

      assertNull(store.depage());

      assertFalse(store.isPaging());

      {
         final RoutingContextImpl ctx2 = new RoutingContextImpl(null);
         assertFalse(store.page(msg, ctx2.getTransaction(), ctx2.getContextListing(store.getStoreName())));
      }

      store.startPaging();

      {
         final RoutingContextImpl ctx2 = new RoutingContextImpl(null);
         assertTrue(store.page(msg, ctx2.getTransaction(), ctx2.getContextListing(store.getStoreName())));
      }

      Page page = store.depage();

      assertNotNull(page);

      page.open(true);

      LinkedList<PagedMessage> msgs = page.read(new NullStorageManager());

      assertEquals(1, msgs.size());

      assertEquals(1L, msgs.get(0).getMessage().getMessageID());

      ActiveMQTestBase.assertEqualsBuffers(18, buffers.get(0), msgs.get(0).getMessage().toCore().getBodyBuffer());

      assertEquals(1, store.getNumberOfPages());

      assertTrue(store.isPaging());

      assertNull(store.depage());

      assertEquals(0, store.getNumberOfPages());

      page.open(true);
      page.close(false);

   }

   @Test
   public void testConcurrentDepage() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory(1, false);

      testConcurrentPaging(factory, 10);
   }

   protected void testConcurrentPaging(final SequentialFileFactory factory,
                                       final int numberOfThreads) throws Exception {
      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final int MAX_SIZE = 1024 * 10;

      final AtomicLong messageIdGenerator = new AtomicLong(0);

      final AtomicInteger aliveProducers = new AtomicInteger(numberOfThreads);

      final CountDownLatch latchStart = new CountDownLatch(numberOfThreads);

      final ConcurrentHashMap<Long, Message> buffers = new ConcurrentHashMap<>();

      final ArrayList<Page> readPages = new ArrayList<>();

      AddressSettings settings = new AddressSettings().setPageSizeBytes(MAX_SIZE).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      final PagingStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, SimpleString.of("test"), settings, getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      // Marked the store to be paged
      storeImpl.startPaging();

      assertEquals(1, storeImpl.getNumberOfPages());

      final SimpleString destination = SimpleString.of("test");

      class WriterThread extends Thread {

         Exception e;

         @Override
         public void run() {

            try {
               boolean firstTime = true;
               while (true) {
                  long id = messageIdGenerator.incrementAndGet();

                  // Each thread will Keep paging until all the messages are depaged.
                  // This is possible because the depage thread is not actually reading the pages.
                  // Just using the internal API to remove it from the page file system
                  Message msg = createMessage(id, storeImpl, destination, createRandomBuffer(id, 5));
                  final RoutingContextImpl ctx2 = new RoutingContextImpl(null);
                  if (storeImpl.page(msg, ctx2.getTransaction(), ctx2.getContextListing(storeImpl.getStoreName()))) {
                     buffers.put(id, msg);
                  } else {
                     break;
                  }

                  if (firstTime) {
                     // We have at least one data paged. So, we can start depaging now
                     latchStart.countDown();
                     firstTime = false;
                  }
               }
            } catch (Exception e1) {
               e1.printStackTrace();
               this.e = e1;
            } finally {
               aliveProducers.decrementAndGet();
            }
         }
      }

      final class ReaderThread extends Thread {

         Exception e;

         @Override
         public void run() {
            try {
               // Wait every producer to produce at least one message
               ActiveMQTestBase.waitForLatch(latchStart);

               while (aliveProducers.get() > 0) {
                  Page page = storeImpl.depage();
                  if (page != null) {
                     readPages.add(page);
                  }
               }
            } catch (Exception e1) {
               e1.printStackTrace();
               this.e = e1;
            }
         }
      }

      WriterThread[] producerThread = new WriterThread[numberOfThreads];

      for (int i = 0; i < numberOfThreads; i++) {
         producerThread[i] = new WriterThread();
         producerThread[i].start();
      }

      ReaderThread consumer = new ReaderThread();
      consumer.start();

      for (int i = 0; i < numberOfThreads; i++) {
         producerThread[i].join();
         if (producerThread[i].e != null) {
            throw producerThread[i].e;
         }
      }

      consumer.join();

      if (consumer.e != null) {
         throw consumer.e;
      }

      final ConcurrentMap<Long, Message> buffers2 = new ConcurrentHashMap<>();

      for (Page page : readPages) {
         page.open(true);
         LinkedList<PagedMessage> msgs = page.read(new NullStorageManager());
         page.close(false, false);


         try (LinkedListIterator<PagedMessage> iter = msgs.iterator()) {
            while (iter.hasNext()) {
               PagedMessage msg = iter.next();
               long id = msg.getMessage().toCore().getBodyBuffer().readLong();
               msg.getMessage().toCore().getBodyBuffer().resetReaderIndex();

               Message msgWritten = buffers.remove(id);
               buffers2.put(id, msg.getMessage());
               assertNotNull(msgWritten);
               assertEquals(msg.getMessage().getAddress(), msgWritten.getAddress());
               ActiveMQTestBase.assertEqualsBuffers(10, msgWritten.toCore().getBodyBuffer(), msg.getMessage().toCore().getBodyBuffer());
            }
         }
      }

      assertEquals(0, buffers.size());

      List<String> files = factory.listFiles("page");

      assertTrue(files.size() != 0);

      for (String file : files) {
         SequentialFile fileTmp = factory.createSequentialFile(file);
         fileTmp.open();
         assertTrue(fileTmp.size() <= MAX_SIZE, "The page file size (" + fileTmp.size() + ") shouldn't be > " + MAX_SIZE);
         fileTmp.close();
      }

      PagingStore storeImpl2 = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, SimpleString.of("test"), settings, getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);
      storeImpl2.start();

      long numberOfPages = storeImpl2.getNumberOfPages();
      assertTrue(numberOfPages != 0);

      storeImpl2.startPaging();

      storeImpl2.startPaging();

      assertEquals(numberOfPages, storeImpl2.getNumberOfPages());

      long lastMessageId = messageIdGenerator.incrementAndGet();
      Message lastMsg = createMessage(lastMessageId, storeImpl, destination, createRandomBuffer(lastMessageId, 5));

      storeImpl2.forceAnotherPage();

      final RoutingContextImpl ctx = new RoutingContextImpl(null);
      storeImpl2.page(lastMsg, ctx.getTransaction(), ctx.getContextListing(storeImpl2.getStoreName()));
      buffers2.put(lastMessageId, lastMsg);

      Page lastPage = null;
      while (true) {
         Page page = storeImpl2.depage();
         if (page == null) {
            break;
         }

         lastPage = page;

         page.open(true);

         LinkedList<PagedMessage> msgs = page.read(new NullStorageManager());

         page.close(false, false);

         msgs.forEach((msg) -> {
            long id = msg.getMessage().toCore().getBodyBuffer().readLong();
            Message msgWritten = buffers2.remove(id);
            assertNotNull(msgWritten);
            assertEquals(msg.getMessage().getAddress(), msgWritten.getAddress());
            ActiveMQTestBase.assertEqualsByteArrays(msgWritten.toCore().getBodyBuffer().writerIndex(), msgWritten.toCore().getBodyBuffer().toByteBuffer().array(), msg.getMessage().toCore().getBodyBuffer().toByteBuffer().array());
         });
      }

      lastPage.open(true);
      LinkedList<PagedMessage> lastMessages = lastPage.read(new NullStorageManager());
      lastPage.close(false, false);
      assertEquals(1, lastMessages.size());

      lastMessages.get(0).getMessage().toCore().getBodyBuffer().resetReaderIndex();
      assertEquals(lastMessages.get(0).getMessage().toCore().getBodyBuffer().readLong(), lastMessageId);

      assertEquals(0, buffers2.size());

      assertEquals(0, storeImpl.getAddressSize());

      storeImpl.stop();
   }

   @Test
   public void testRestartPage() throws Throwable {
      clearDataRecreateServerDirs();
      SequentialFileFactory factory = new NIOSequentialFileFactory(new File(getPageDir()), 1).setDatasync(false);

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final int MAX_SIZE = 1024 * 10;

      AddressSettings settings = new AddressSettings().setPageSizeBytes(MAX_SIZE).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      final PagingStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, SimpleString.of("test"), settings, getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      // Marked the store to be paged
      storeImpl.startPaging();

      storeImpl.depage();

      assertNull(storeImpl.getCurrentPage());

      storeImpl.startPaging();

      assertNotNull(storeImpl.getCurrentPage());

      storeImpl.stop();
   }

   @Test
   public void testOrderOnPaging() throws Throwable {
      ExecutorService executorService = Executors.newFixedThreadPool(2);
      try {
         clearDataRecreateServerDirs();
         SequentialFileFactory factory = new FakeSequentialFileFactory();

         PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

         final int MAX_SIZE = 1024 * 10;

         AddressSettings settings = new AddressSettings().setPageSizeBytes(MAX_SIZE).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

         final PagingStore store = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, SimpleString.of("test"), settings, getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), false);

         store.start();

         assertEquals(0, store.getNumberOfPages());

         // Marked the store to be paged
         store.startPaging();

         assertEquals(1, store.getNumberOfPages());

         final SimpleString destination = SimpleString.of("test");

         final long NUMBER_OF_MESSAGES = 10000;

         final List<Throwable> errors = new ArrayList<>();

         ReusableLatch done = new ReusableLatch(0);
         done.countUp();

         executorService.execute(() -> {
            try {
               for (long i = 0; i < NUMBER_OF_MESSAGES; i++) {
                  // Each thread will Keep paging until all the messages are depaged.
                  // This is possible because the depage thread is not actually reading the pages.
                  // Just using the internal API to remove it from the page file system
                  Message msg = createMessage(i, store, destination, createRandomBuffer(i, 1024));
                  msg.putLongProperty("count", i);

                  final RoutingContextImpl ctx2 = new RoutingContextImpl(null);
                  store.page(msg, ctx2.getTransaction(), ctx2.getContextListing(store.getStoreName()));
               }
            } catch (Throwable e) {
               e.printStackTrace();
               errors.add(e);
            } finally {
               done.countDown();
            }
         });

         assertTrue(done.await(10, TimeUnit.SECONDS));
         done.countUp();

         executorService.execute(() -> {
            try {
               AtomicInteger msgsRead = new AtomicInteger(0);

               while (msgsRead.get() < NUMBER_OF_MESSAGES) {
                  Page page = store.depage();
                  AtomicInteger countOnPage = new AtomicInteger(0);
                  if (page != null) {
                     page.open(true);
                     LinkedList<PagedMessage> messages = page.read(new NullStorageManager());
                     messages.forEach(pgmsg -> {
                        assertEquals(countOnPage.getAndIncrement(), pgmsg.getMessageNumber());
                        Message msg = pgmsg.getMessage();
                        assertEquals(msgsRead.getAndIncrement(), msg.getMessageID());
                        assertEquals(msg.getMessageID(), msg.getLongProperty("count").longValue());
                     });

                     page.close(false, false);
                  }
               }

            } catch (Throwable e) {
               e.printStackTrace();
               errors.add(e);
            } finally {
               done.countDown();
            }

         });

         assertTrue(done.await(10, TimeUnit.SECONDS));

         store.stop();

         for (Throwable e : errors) {
            throw e;
         }
      } finally {
         executorService.shutdownNow();
      }
   }


   @Test
   public void testWriteIncompletePage() throws Exception {
      clearDataRecreateServerDirs();
      SequentialFileFactory factory = new NIOSequentialFileFactory(new File(getPageDir()), 1).setDatasync(false);

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final int MAX_SIZE = 1024 * 1024;

      AddressSettings settings = new AddressSettings().setPageSizeBytes(MAX_SIZE).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      final PagingStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, SimpleString.of("test"), settings, getExecutorFactory().getExecutor(), getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      // Marked the store to be paged
      storeImpl.startPaging();

      Page page = storeImpl.getCurrentPage();

      int num1 = 20;
      for (int i = 0; i < num1; i++) {
         writePageMessage(storeImpl, i);
      }
      // simulate uncompleted page
      long position = page.getFile().position();
      writePageMessage(storeImpl, 30);
      page.getFile().position(position);
      ByteBuffer buffer = ByteBuffer.allocate(10);
      for (int i = 0; i < buffer.capacity(); i++) {
         buffer.put((byte) 'Z');
      }
      buffer.rewind();
      page.getFile().writeDirect(buffer, true);
      storeImpl.stop();

      // write uncompleted page
      storeImpl.start();
      int num2 = 10;
      for (int i = 0; i < num2; i++) {
         writePageMessage(storeImpl, i + num1);
      }

      // simulate broker restart
      storeImpl.stop();
      storeImpl.start();

      AtomicLong msgsRead = new AtomicLong(0);

      while (msgsRead.get() < num1 + num2) {
         page = storeImpl.depage();
         assertNotNull(page, "no page after read " + msgsRead + " msg");
         page.open(true);
         LinkedList<PagedMessage> messages = page.read(new NullStorageManager());

         messages.forEach(pgmsg -> {
            Message msg = pgmsg.getMessage();
            assertEquals(msgsRead.longValue(), msg.getMessageID());
            assertEquals(msg.getMessageID(), msg.getLongProperty("count").longValue());
            msgsRead.incrementAndGet();
         });
         page.close(false);
      }

      storeImpl.stop();
   }

   @Test
   public void testLogStartPaging() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingStoreImpl store = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100,
                                                  createMockManager(), createStorageManagerMock(), factory, storeFactory,
                                                  PagingStoreImplTest.destinationTestName,
                                                  new AddressSettings()
                                                     .setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE),
                                                  getExecutorFactory().getExecutor(),  getExecutorFactory().getExecutor(), true);

      store.start();
      try (AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler()) {
         store.startPaging();
         store.stopPaging();
         assertTrue(loggerHandler.findText("AMQ222038"));
      }
   }

   @Test
   public void testLogStopPaging() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingStoreImpl store = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100,
                                                  createMockManager(), createStorageManagerMock(), factory, storeFactory,
                                                  PagingStoreImplTest.destinationTestName,
                                                  new AddressSettings()
                                                     .setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE),
                                                  getExecutorFactory().getExecutor(),  getExecutorFactory().getExecutor(), true);
      store.start();
      try (AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler()) {
         store.startPaging();
         store.stopPaging();
         assertTrue(loggerHandler.findText("AMQ224108"));
      }
   }

   @Test
   public void testGetAddressLimitPercent() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingStoreImpl store = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100,
                                                  createMockManager(), createStorageManagerMock(), factory, storeFactory,
                                                  PagingStoreImplTest.destinationTestName,
                                                  new AddressSettings()
                                                     .setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK),
                                                  getExecutorFactory().getExecutor(),  getExecutorFactory().getExecutor(), true);

      store.start();
      try {
         assertEquals(0, store.getAddressLimitPercent());
         store.addSize(100);

         // no limit set
         assertEquals(0, store.getAddressLimitPercent());

         store.applySetting(new AddressSettings().setMaxSizeBytes(1000));
         assertEquals(10, store.getAddressLimitPercent());

         store.addSize(900);
         assertEquals(100, store.getAddressLimitPercent());

         store.addSize(900);
         assertEquals(190, store.getAddressLimitPercent());

         store.addSize(-900);
         assertEquals(100, store.getAddressLimitPercent());

         store.addSize(-1);
         assertEquals(99, store.getAddressLimitPercent());

      } finally {
         store.stop();
      }
   }

   @Test
   public void testGetAddressLimitPercentGlobalSize() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final AtomicLong limit = new AtomicLong();
      AtomicLong globalSize = new AtomicLong();
      PagingManager pagingManager = new FakePagingManager() {
         @Override
         public boolean isUsingGlobalSize() {
            return limit.get() > 0;
         }

         @Override
         public FakePagingManager addSize(int s, boolean sizeOnly) {
            globalSize.addAndGet(s);
            return this;
         }

         @Override
         public long getGlobalSize() {
            return globalSize.get();
         }

         @Override
         public boolean isGlobalFull() {
            return globalSize.get() >= limit.get();
         }

         @Override
         public long getMaxSize() {
            return limit.get();
         }
      };

      PagingStoreImpl store = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100,
                                                  pagingManager, createStorageManagerMock(), factory, storeFactory,
                                                  PagingStoreImplTest.destinationTestName,
                                                  new AddressSettings()
                                                     .setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK),
                                                  getExecutorFactory().getExecutor(),  getExecutorFactory().getExecutor(), true);

      store.start();
      try {
         // no usage yet
         assertEquals(0, store.getAddressLimitPercent());

         store.addSize(100);

         // no global limit set
         assertEquals(0, store.getAddressLimitPercent());

         // set a global limit
         limit.set(1000);
         assertEquals(10, store.getAddressLimitPercent());

         store.addSize(900);
         assertEquals(100, store.getAddressLimitPercent());

         store.addSize(900);
         assertEquals(190, store.getAddressLimitPercent());

         store.addSize(-900);
         assertEquals(100, store.getAddressLimitPercent());

         store.addSize(-1);
         assertEquals(99, store.getAddressLimitPercent());

      } finally {
         store.stop();
      }
   }

   @Test
   public void testBlockUnblock() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingStoreImpl store = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100,
                                                  createMockManager(), createStorageManagerMock(), factory, storeFactory,
                                                  PagingStoreImplTest.destinationTestName,
                                                  new AddressSettings()
                                                     .setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK),
                                                  getExecutorFactory().getExecutor(),  getExecutorFactory().getExecutor(), true);

      store.start();
      try {
         final  AtomicInteger calls = new AtomicInteger();
         final Runnable trackMemoryChecks = calls::incrementAndGet;
         store.applySetting(new AddressSettings().setMaxSizeBytes(1000).setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK));
         store.addSize(100);
         store.flushExecutors();
         store.checkMemory(trackMemoryChecks, null);
         assertEquals(1, calls.get());

         store.block();
         store.checkMemory(trackMemoryChecks, null);
         assertEquals(1, calls.get());

         store.unblock();

         assertTrue(Wait.waitFor(() -> 2 == calls.get(), 1000, 50));

         store.addSize(900);
         assertEquals(100, store.getAddressLimitPercent());
         store.flushExecutors();

         // address full blocks
         store.checkMemory(trackMemoryChecks, null);
         assertEquals(2, calls.get());

         store.block();

         // release memory
         store.addSize(-900);
         assertEquals(10, store.getAddressLimitPercent());

         // not yet released memory checks b/c of blocked
         assertEquals(2, calls.get());

         store.unblock();
         store.flushExecutors();

         // now released
         assertTrue(Wait.waitFor(() -> 3 == calls.get(), 1000, 50));

         // reverse - unblock while full does not release
         store.block();

         store.addSize(900);
         assertEquals(100, store.getAddressLimitPercent());

         store.checkMemory(trackMemoryChecks, null);
         assertEquals(3, calls.get(), "no change");
         assertEquals(3, calls.get(), "no change to be sure to be sure!");

         store.unblock();
         assertEquals(3, calls.get(), "no change after unblock");

         store.addSize(-900);
         assertEquals(10, store.getAddressLimitPercent());

         assertTrue(Wait.waitFor(() -> 4 == calls.get(), 1000, 50), "change");


      } finally {
         store.stop();
      }
   }

   /**
    * @return
    */
   protected PagingManager createMockManager() {
      return new FakePagingManager();
   }

   private StorageManager createStorageManagerMock() {
      return new NullStorageManager();
   }

   private ExecutorFactory getExecutorFactory() {
      return () -> ArtemisExecutor.delegate(executor);
   }

   protected void writePageMessage(final PagingStore storeImpl,
                                  final long id) throws Exception {
      Message msg = createMessage(id, storeImpl, PagingStoreImplTest.destinationTestName, createRandomBuffer(id, 10));
      msg.putLongProperty("count", id);

      final RoutingContextImpl ctx2 = new RoutingContextImpl(null);
      storeImpl.page(msg, ctx2.getTransaction(), ctx2.getContextListing(storeImpl.getStoreName()));
   }

   private CoreMessage createMessage(final long id,
                                       final PagingStore store,
                                       final SimpleString destination,
                                       final ActiveMQBuffer buffer) {
      CoreMessage msg = new CoreMessage(id, 50 + buffer.capacity());

      msg.setAddress(destination);

      msg.getBodyBuffer().resetReaderIndex();
      msg.getBodyBuffer().resetWriterIndex();

      msg.getBodyBuffer().writeBytes(buffer, buffer.capacity());

      return msg;
   }

   protected ActiveMQBuffer createRandomBuffer(final long id, final int size) {
      return RandomUtil.randomBuffer(size, id);
   }

   // Protected ----------------------------------------------------

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();
      executor = Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory(getClass().getName()));
   }

   @Override
   @AfterEach
   public void tearDown() throws Exception {
      executor.shutdown();
      super.tearDown();
   }

   static final class FakeStoreFactory implements PagingStoreFactory {

      final SequentialFileFactory factory;

      FakeStoreFactory() {
         factory = new FakeSequentialFileFactory();
      }

      FakeStoreFactory(final SequentialFileFactory factory) {
         this.factory = factory;
      }

      @Override
      public SequentialFileFactory newFileFactory(final SimpleString destinationName) throws Exception {
         return factory;
      }

      @Override
      public void removeFileFactory(SequentialFileFactory fileFactory) throws Exception {
      }

      @Override
      public PagingStore newStore(final SimpleString destinationName, final AddressSettings addressSettings) {
         return null;
      }

      @Override
      public List<PagingStore> reloadStores(final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception {
         return null;
      }

      @Override
      public PageCursorProvider newCursorProvider(PagingStore store,
                                                  StorageManager storageManager,
                                                  AddressSettings addressSettings,
                                                  ArtemisExecutor executor) {
         return new PageCursorProviderImpl(store, storageManager);
      }

      @Override
      public void setPagingManager(final PagingManager manager) {
      }

      @Override
      public void stop() throws InterruptedException {
      }

      @Override
      public void injectMonitor(FileStoreMonitor monitor) throws Exception {

      }

      public void beforePageRead() throws Exception {
      }

      public void afterPageRead() throws Exception {
      }

      public ByteBuffer allocateDirectBuffer(final int size) {
         return ByteBuffer.allocateDirect(size);
      }

      public void freeDirectuffer(final ByteBuffer buffer) {
      }
   }

   private static final class CountingRunnable implements Runnable {
      final AtomicInteger calls = new AtomicInteger();

      @Override
      public void run() {
         calls.incrementAndGet();
      }

      public int getCount() {
         return calls.get();
      }
   }

   @Test
   @Timeout(10)
   public void testCheckExecutionIsNotRepeated() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingManager mockManager = Mockito.mock(PagingManager.class);

      ArtemisExecutor sameThreadExecutor = Runnable::run;
      PagingStoreImpl store = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100,
                                                  mockManager, createStorageManagerMock(), factory, storeFactory,
                                                  PagingStoreImplTest.destinationTestName,
                                                  new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK),
                                                  sameThreadExecutor, sameThreadExecutor, true);

      store.start();
      try {
         store.applySetting(new AddressSettings().setMaxSizeBytes(1000).setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK));

         Mockito.when(mockManager.addSize(Mockito.anyInt())).thenReturn(mockManager);
         store.addSize(100);

         // Do an initial check
         final CountingRunnable trackMemoryCheck1 = new CountingRunnable();
         assertEquals(0, trackMemoryCheck1.getCount());
         store.checkMemory(trackMemoryCheck1, null);
         assertEquals(1, trackMemoryCheck1.getCount());

         // Do another check, this time indicate the disk is full during the first couple
         // requests, making the task initially be retained for later but then executed.
         final CountingRunnable trackMemoryCheck2 = new CountingRunnable();
         Mockito.when(mockManager.isDiskFull()).thenReturn(true, true, false);
         assertEquals(0, trackMemoryCheck2.getCount());
         store.checkMemory(trackMemoryCheck2, null);
         assertEquals(1, trackMemoryCheck2.getCount());

         // Now run the released memory checks. The task should NOT execute again, verify it doesnt.
         store.checkReleasedMemory();
         assertEquals(1, trackMemoryCheck2.getCount());
      } finally {
         store.stop();
      }
   }
}
