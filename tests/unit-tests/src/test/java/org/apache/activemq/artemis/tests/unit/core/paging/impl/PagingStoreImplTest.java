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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

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
import org.apache.activemq.artemis.core.paging.cursor.impl.PageCursorProviderImpl;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.paging.impl.PageTransactionInfoImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreImpl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessagePersister;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.apache.activemq.artemis.tests.unit.util.FakePagingManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PagingStoreImplTest extends ActiveMQTestBase {
   private static final Logger log = Logger.getLogger(PagingStoreImplTest.class);

   static {
      MessagePersister.registerPersister(CoreMessagePersister.getInstance());
      MessagePersister.registerPersister(AMQPMessagePersister.getInstance());
   }

   private static final SimpleString destinationTestName = new SimpleString("test");
   private final ReadLock lock = new ReentrantReadWriteLock().readLock();

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

      Assert.assertEquals(nr1, trans.getNumberOfMessages());

      ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(trans.getEncodeSize());

      trans.encode(buffer);

      PageTransactionInfo trans2 = new PageTransactionInfoImpl(id1);
      trans2.decode(buffer);

      Assert.assertEquals(id2, trans2.getTransactionID());

      Assert.assertEquals(nr1, trans2.getNumberOfMessages());

   }

   @Test
   public void testDoubleStart() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, new FakeStoreFactory(factory), PagingStoreImplTest.destinationTestName, new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE), getExecutorFactory().getExecutor(), true);

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
      testConcurrentPaging(new NIOSequentialFileFactory(new File(getTestDir()), 1), 1);
   }

   @Test
   public void testStore() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      AddressSettings addressSettings = new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      PagingStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, PagingStoreImplTest.destinationTestName, addressSettings, getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      List<ActiveMQBuffer> buffers = new ArrayList<>();

      ActiveMQBuffer buffer = createRandomBuffer(0, 10);

      buffers.add(buffer);
      SimpleString destination = new SimpleString("test");

      Message msg = createMessage(1, storeImpl, destination, buffer);

      Assert.assertTrue(storeImpl.isPaging());

      final RoutingContextImpl ctx = new RoutingContextImpl(null);
      Assert.assertTrue(storeImpl.page(msg, ctx.getTransaction(), ctx.getContextListing(storeImpl.getStoreName()), lock));

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.sync();

      storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, PagingStoreImplTest.destinationTestName, addressSettings, getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.stop();
   }

   @Test
   public void testDepageOnCurrentPage() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      SimpleString destination = new SimpleString("test");

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingStoreImpl storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, PagingStoreImplTest.destinationTestName, new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE), getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      List<ActiveMQBuffer> buffers = new ArrayList<>();

      int numMessages = 10;

      for (int i = 0; i < numMessages; i++) {

         ActiveMQBuffer buffer = createRandomBuffer(i + 1L, 10);

         buffers.add(buffer);

         Message msg = createMessage(i, storeImpl, destination, buffer);
         final RoutingContextImpl ctx = new RoutingContextImpl(null);
         Assert.assertTrue(storeImpl.page(msg, ctx.getTransaction(), ctx.getContextListing(storeImpl.getStoreName()), lock));

      }

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.sync();

      Page page = storeImpl.depage();

      page.open();

      List<PagedMessage> msg = page.read(new NullStorageManager());

      Assert.assertEquals(numMessages, msg.size());
      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      page.close(false);
      page = storeImpl.depage();

      Assert.assertNull(page);

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      for (int i = 0; i < numMessages; i++) {
         ActiveMQBuffer horn1 = buffers.get(i);
         ActiveMQBuffer horn2 = msg.get(i).getMessage().toCore().getBodyBuffer();
         horn1.resetReaderIndex();
         horn2.resetReaderIndex();
         for (int j = 0; j < horn1.writerIndex(); j++) {
            Assert.assertEquals(horn1.readByte(), horn2.readByte());
         }
      }

   }

   @Test
   public void testDepageMultiplePages() throws Exception {
      SequentialFileFactory factory = new FakeSequentialFileFactory();
      SimpleString destination = new SimpleString("test");

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      PagingStoreImpl store = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, PagingStoreImplTest.destinationTestName, new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE), getExecutorFactory().getExecutor(), true);

      store.start();

      Assert.assertEquals(0, store.getNumberOfPages());

      store.startPaging();

      Assert.assertEquals(1, store.getNumberOfPages());

      List<ActiveMQBuffer> buffers = new ArrayList<>();

      for (int i = 0; i < 10; i++) {

         ActiveMQBuffer buffer = createRandomBuffer(i + 1L, 10);

         buffers.add(buffer);

         if (i == 5) {
            store.forceAnotherPage();
         }

         Message msg = createMessage(i, store, destination, buffer);

         final RoutingContextImpl ctx = new RoutingContextImpl(null);
         Assert.assertTrue(store.page(msg, ctx.getTransaction(), ctx.getContextListing(store.getStoreName()), lock));
      }

      Assert.assertEquals(2, store.getNumberOfPages());

      store.sync();

      int sequence = 0;

      for (int pageNr = 0; pageNr < 2; pageNr++) {
         Page page = store.depage();

         log.debug("numberOfPages = " + store.getNumberOfPages());

         page.open();

         List<PagedMessage> msg = page.read(new NullStorageManager());

         page.close(false, false);

         Assert.assertEquals(5, msg.size());

         for (int i = 0; i < 5; i++) {
            Assert.assertEquals(sequence++, msg.get(i).getMessage().getMessageID());
            ActiveMQTestBase.assertEqualsBuffers(18, buffers.get(pageNr * 5 + i), msg.get(i).getMessage().toCore().getBodyBuffer());
         }
      }

      Assert.assertEquals(1, store.getNumberOfPages());

      Assert.assertTrue(store.isPaging());

      Message msg = createMessage(1, store, destination, buffers.get(0));

      final RoutingContextImpl ctx = new RoutingContextImpl(null);
      Assert.assertTrue(store.page(msg, ctx.getTransaction(), ctx.getContextListing(store.getStoreName()), lock));

      Page newPage = store.depage();

      newPage.open();

      Assert.assertEquals(1, newPage.read(new NullStorageManager()).size());

      newPage.delete(null);

      Assert.assertEquals(1, store.getNumberOfPages());

      Assert.assertTrue(store.isPaging());

      Assert.assertNull(store.depage());

      Assert.assertFalse(store.isPaging());

      {
         final RoutingContextImpl ctx2 = new RoutingContextImpl(null);
         Assert.assertFalse(store.page(msg, ctx2.getTransaction(), ctx2.getContextListing(store.getStoreName()), lock));
      }

      store.startPaging();

      {
         final RoutingContextImpl ctx2 = new RoutingContextImpl(null);
         Assert.assertTrue(store.page(msg, ctx2.getTransaction(), ctx2.getContextListing(store.getStoreName()), lock));
      }

      Page page = store.depage();

      page.open();

      List<PagedMessage> msgs = page.read(new NullStorageManager());

      Assert.assertEquals(1, msgs.size());

      Assert.assertEquals(1L, msgs.get(0).getMessage().getMessageID());

      ActiveMQTestBase.assertEqualsBuffers(18, buffers.get(0), msgs.get(0).getMessage().toCore().getBodyBuffer());

      Assert.assertEquals(1, store.getNumberOfPages());

      Assert.assertTrue(store.isPaging());

      Assert.assertNull(store.depage());

      Assert.assertEquals(0, store.getNumberOfPages());

      page.open();
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

      final PagingStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, new SimpleString("test"), settings, getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      // Marked the store to be paged
      storeImpl.startPaging();

      Assert.assertEquals(1, storeImpl.getNumberOfPages());

      final SimpleString destination = new SimpleString("test");

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
                  if (storeImpl.page(msg, ctx2.getTransaction(), ctx2.getContextListing(storeImpl.getStoreName()), lock)) {
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
         page.open();
         List<PagedMessage> msgs = page.read(new NullStorageManager());
         page.close(false, false);

         for (PagedMessage msg : msgs) {
            long id = msg.getMessage().toCore().getBodyBuffer().readLong();
            msg.getMessage().toCore().getBodyBuffer().resetReaderIndex();

            Message msgWritten = buffers.remove(id);
            buffers2.put(id, msg.getMessage());
            Assert.assertNotNull(msgWritten);
            Assert.assertEquals(msg.getMessage().getAddress(), msgWritten.getAddress());
            ActiveMQTestBase.assertEqualsBuffers(10, msgWritten.toCore().getBodyBuffer(), msg.getMessage().toCore().getBodyBuffer());
         }
      }

      Assert.assertEquals(0, buffers.size());

      List<String> files = factory.listFiles("page");

      Assert.assertTrue(files.size() != 0);

      for (String file : files) {
         SequentialFile fileTmp = factory.createSequentialFile(file);
         fileTmp.open();
         Assert.assertTrue("The page file size (" + fileTmp.size() + ") shouldn't be > " + MAX_SIZE, fileTmp.size() <= MAX_SIZE);
         fileTmp.close();
      }

      PagingStore storeImpl2 = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, new SimpleString("test"), settings, getExecutorFactory().getExecutor(), true);
      storeImpl2.start();

      int numberOfPages = storeImpl2.getNumberOfPages();
      Assert.assertTrue(numberOfPages != 0);

      storeImpl2.startPaging();

      storeImpl2.startPaging();

      Assert.assertEquals(numberOfPages, storeImpl2.getNumberOfPages());

      long lastMessageId = messageIdGenerator.incrementAndGet();
      Message lastMsg = createMessage(lastMessageId, storeImpl, destination, createRandomBuffer(lastMessageId, 5));

      storeImpl2.forceAnotherPage();

      final RoutingContextImpl ctx = new RoutingContextImpl(null);
      storeImpl2.page(lastMsg, ctx.getTransaction(), ctx.getContextListing(storeImpl2.getStoreName()), lock);
      buffers2.put(lastMessageId, lastMsg);

      Page lastPage = null;
      while (true) {
         Page page = storeImpl2.depage();
         if (page == null) {
            break;
         }

         lastPage = page;

         page.open();

         List<PagedMessage> msgs = page.read(new NullStorageManager());

         page.close(false, false);

         for (PagedMessage msg : msgs) {

            long id = msg.getMessage().toCore().getBodyBuffer().readLong();
            Message msgWritten = buffers2.remove(id);
            Assert.assertNotNull(msgWritten);
            Assert.assertEquals(msg.getMessage().getAddress(), msgWritten.getAddress());
            ActiveMQTestBase.assertEqualsByteArrays(msgWritten.toCore().getBodyBuffer().writerIndex(), msgWritten.toCore().getBodyBuffer().toByteBuffer().array(), msg.getMessage().toCore().getBodyBuffer().toByteBuffer().array());
         }
      }

      lastPage.open();
      List<PagedMessage> lastMessages = lastPage.read(new NullStorageManager());
      lastPage.close(false, false);
      Assert.assertEquals(1, lastMessages.size());

      lastMessages.get(0).getMessage().toCore().getBodyBuffer().resetReaderIndex();
      Assert.assertEquals(lastMessages.get(0).getMessage().toCore().getBodyBuffer().readLong(), lastMessageId);

      Assert.assertEquals(0, buffers2.size());

      Assert.assertEquals(0, storeImpl.getAddressSize());

      storeImpl.stop();
   }

   @Test
   public void testRestartPage() throws Throwable {
      clearDataRecreateServerDirs();
      SequentialFileFactory factory = new NIOSequentialFileFactory(new File(getPageDir()), 1);

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final int MAX_SIZE = 1024 * 10;

      AddressSettings settings = new AddressSettings().setPageSizeBytes(MAX_SIZE).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      final PagingStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, new SimpleString("test"), settings, getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

      // Marked the store to be paged
      storeImpl.startPaging();

      storeImpl.depage();

      Assert.assertNull(storeImpl.getCurrentPage());

      storeImpl.startPaging();

      Assert.assertNotNull(storeImpl.getCurrentPage());

      storeImpl.stop();
   }

   @Test
   public void testOrderOnPaging() throws Throwable {
      clearDataRecreateServerDirs();
      SequentialFileFactory factory = new NIOSequentialFileFactory(new File(getPageDir()), 1);

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final int MAX_SIZE = 1024 * 10;

      AddressSettings settings = new AddressSettings().setPageSizeBytes(MAX_SIZE).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      final PagingStore store = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, new SimpleString("test"), settings, getExecutorFactory().getExecutor(), false);

      store.start();

      Assert.assertEquals(0, store.getNumberOfPages());

      // Marked the store to be paged
      store.startPaging();

      final CountDownLatch producedLatch = new CountDownLatch(1);

      Assert.assertEquals(1, store.getNumberOfPages());

      final SimpleString destination = new SimpleString("test");

      final long NUMBER_OF_MESSAGES = 100000;

      final List<Throwable> errors = new ArrayList<>();

      class WriterThread extends Thread {

         WriterThread() {
            super("PageWriter");
         }

         @Override
         public void run() {

            try {
               for (long i = 0; i < NUMBER_OF_MESSAGES; i++) {
                  // Each thread will Keep paging until all the messages are depaged.
                  // This is possible because the depage thread is not actually reading the pages.
                  // Just using the internal API to remove it from the page file system
                  Message msg = createMessage(i, store, destination, createRandomBuffer(i, 1024));
                  msg.putLongProperty("count", i);

                  final RoutingContextImpl ctx2 = new RoutingContextImpl(null);
                  while (!store.page(msg, ctx2.getTransaction(), ctx2.getContextListing(store.getStoreName()), lock)) {
                     store.startPaging();
                  }

                  if (i == 0) {
                     producedLatch.countDown();
                  }
               }
            } catch (Throwable e) {
               e.printStackTrace();
               errors.add(e);
            }
         }
      }

      class ReaderThread extends Thread {

         ReaderThread() {
            super("PageReader");
         }

         @Override
         public void run() {
            try {

               long msgsRead = 0;

               while (msgsRead < NUMBER_OF_MESSAGES) {
                  Page page = store.depage();
                  if (page != null) {
                     page.open();
                     List<PagedMessage> messages = page.read(new NullStorageManager());

                     for (PagedMessage pgmsg : messages) {
                        Message msg = pgmsg.getMessage();

                        Assert.assertEquals(msgsRead++, msg.getMessageID());

                        Assert.assertEquals(msg.getMessageID(), msg.getLongProperty("count").longValue());
                     }

                     page.close(false, false);
                     page.delete(null);
                  } else {
                     log.debug("Depaged!!!! numerOfMessages = " + msgsRead + " of " + NUMBER_OF_MESSAGES);
                     Thread.sleep(500);
                  }
               }

            } catch (Throwable e) {
               e.printStackTrace();
               errors.add(e);
            }
         }
      }

      WriterThread producerThread = new WriterThread();
      producerThread.start();
      ReaderThread consumer = new ReaderThread();
      consumer.start();

      producerThread.join();
      consumer.join();

      store.stop();

      for (Throwable e : errors) {
         throw e;
      }
   }

   @Test
   public void testWriteIncompletePage() throws Exception {
      clearDataRecreateServerDirs();
      SequentialFileFactory factory = new NIOSequentialFileFactory(new File(getPageDir()), 1);

      PagingStoreFactory storeFactory = new FakeStoreFactory(factory);

      final int MAX_SIZE = 1024 * 1024;

      AddressSettings settings = new AddressSettings().setPageSizeBytes(MAX_SIZE).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      final PagingStore storeImpl = new PagingStoreImpl(PagingStoreImplTest.destinationTestName, null, 100, createMockManager(), createStorageManagerMock(), factory, storeFactory, new SimpleString("test"), settings, getExecutorFactory().getExecutor(), true);

      storeImpl.start();

      Assert.assertEquals(0, storeImpl.getNumberOfPages());

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

      long msgsRead = 0;

      while (msgsRead < num1 + num2) {
         page = storeImpl.depage();
         assertNotNull("no page after read " + msgsRead + " msg", page);
         page.open();
         List<PagedMessage> messages = page.read(new NullStorageManager());

         for (PagedMessage pgmsg : messages) {
            Message msg = pgmsg.getMessage();
            Assert.assertEquals(msgsRead, msg.getMessageID());
            Assert.assertEquals(msg.getMessageID(), msg.getLongProperty("count").longValue());
            msgsRead++;
         }
         page.close(false);
      }

      storeImpl.stop();
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
      return new ExecutorFactory() {

         @Override
         public ArtemisExecutor getExecutor() {
            return ArtemisExecutor.delegate(executor);
         }
      };
   }

   protected void writePageMessage(final PagingStore storeImpl,
                                  final long id) throws Exception {
      Message msg = createMessage(id, storeImpl, PagingStoreImplTest.destinationTestName, createRandomBuffer(id, 10));
      msg.putLongProperty("count", id);

      final RoutingContextImpl ctx2 = new RoutingContextImpl(null);
      storeImpl.page(msg, ctx2.getTransaction(), ctx2.getContextListing(storeImpl.getStoreName()), lock);
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
   @Before
   public void setUp() throws Exception {
      super.setUp();
      executor = Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory());
   }

   @Override
   @After
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
         return new PageCursorProviderImpl(store, storageManager, executor, addressSettings.getPageCacheMaxSize());
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
}
