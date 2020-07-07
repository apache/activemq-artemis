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
package org.apache.activemq.artemis.tests.integration.largemessage;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.io.AbstractSequentialFile;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.buffer.TimedBuffer;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerMessagePlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.integration.security.SecurityTest;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.critical.EmptyCriticalAnalyzer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ServerLargeMessageTest extends ActiveMQTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   String originalPath;

   @Before
   public void setupProperty() {
      originalPath = System.getProperty("java.security.auth.login.config");
      if (originalPath == null) {
         URL resource = SecurityTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            originalPath = resource.getFile();
            System.setProperty("java.security.auth.login.config", originalPath);
         }
      }
   }

   @After
   public void clearProperty() {
      if (originalPath == null) {
         System.clearProperty("java.security.auth.login.config");
      } else {
         System.setProperty("java.security.auth.login.config", originalPath);
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // The ClientConsumer should be able to also send ServerLargeMessages as that's done by the CoreBridge
   @Test
   public void testSendServerMessage() throws Exception {
      ActiveMQServer server = createServer(true);

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false);

      try {
         LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

         fileMessage.setMessageID(1005);

         for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
            fileMessage.addBytes(new byte[]{ActiveMQTestBase.getSamplebyte(i)});
         }
         // The server would be doing this
         fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         fileMessage.releaseResources(false);

         session.createQueue(new QueueConfiguration("A").setRoutingType(RoutingType.ANYCAST));

         ClientProducer prod = session.createProducer("A");

         prod.send(fileMessage);

         fileMessage.deleteFile();

         session.commit();

         session.start();

         ClientConsumer cons = session.createConsumer("A");

         ClientMessage msg = cons.receive(5000);

         Assert.assertNotNull(msg);

         Assert.assertEquals(msg.getBodySize(), 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
            Assert.assertEquals(ActiveMQTestBase.getSamplebyte(i), msg.getBodyBuffer().readByte());
         }

         msg.acknowledge();

         session.commit();

      } finally {
         sf.close();
         locator.close();
         server.stop();
      }
   }

   @Test
   public void testSendServerMessageWithValidatedUser() throws Exception {
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("PropertiesLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      server.getConfiguration().setPopulateValidatedUser(true);

      Role role = new Role("programmers", true, true, true, true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      server.getSecurityRepository().addMatch("#", roles);

      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);

      try {
         ClientSession session = sf.createSession("first", "secret", false, true, true, false, 0);
         ClientMessage clientMessage = session.createMessage(false);
         clientMessage.setBodyInputStream(ActiveMQTestBase.createFakeLargeStream(ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE));

         session.createQueue(new QueueConfiguration("A").setRoutingType(RoutingType.ANYCAST));

         ClientProducer prod = session.createProducer("A");
         prod.send(clientMessage);
         session.commit();
         session.start();

         ClientConsumer cons = session.createConsumer("A");
         ClientMessage msg = cons.receive(5000);

         assertEquals("first", msg.getValidatedUserID());
      } finally {
         sf.close();
         locator.close();
         server.stop();
      }
   }

   @Test
   public void testLargeServerMessageSync() throws Exception {
      final AtomicBoolean open = new AtomicBoolean(false);
      final AtomicBoolean sync = new AtomicBoolean(false);

      JournalStorageManager storageManager = new JournalStorageManager(createDefaultInVMConfig(), EmptyCriticalAnalyzer.getInstance(), getOrderedExecutor(), getOrderedExecutor()) {
         @Override
         public SequentialFile createFileForLargeMessage(long messageID, LargeMessageExtension extension) {
            return new SequentialFile() {
               @Override
               public boolean isOpen() {
                  return open.get();
               }

               @Override
               public boolean exists() {
                  return true;
               }

               @Override
               public void open() throws Exception {
                  open.set(true);
               }

               @Override
               public void open(int maxIO, boolean useExecutor) throws Exception {
                  open.set(true);
               }

               @Override
               public boolean fits(int size) {
                  return false;
               }

               @Override
               public int calculateBlockStart(int position) throws Exception {
                  return 0;
               }

               @Override
               public ByteBuffer map(int position, long size) throws IOException {
                  return null;
               }

               @Override
               public String getFileName() {
                  return null;
               }

               @Override
               public void fill(int size) throws Exception {
               }

               @Override
               public void delete() throws IOException, InterruptedException, ActiveMQException {
               }

               @Override
               public void write(ActiveMQBuffer bytes, boolean sync, IOCallback callback) throws Exception {
               }

               @Override
               public void write(ActiveMQBuffer bytes, boolean sync) throws Exception {
               }

               @Override
               public void write(EncodingSupport bytes, boolean sync, IOCallback callback) throws Exception {
               }

               @Override
               public void write(EncodingSupport bytes, boolean sync) throws Exception {
               }

               @Override
               public void writeDirect(ByteBuffer bytes, boolean sync, IOCallback callback) {
               }

               @Override
               public void writeDirect(ByteBuffer bytes, boolean sync) throws Exception {
               }

               @Override
               public void blockingWriteDirect(ByteBuffer bytes, boolean sync, boolean releaseBuffer) throws Exception {
               }

               @Override
               public int read(ByteBuffer bytes, IOCallback callback) throws Exception {
                  return 0;
               }

               @Override
               public int read(ByteBuffer bytes) throws Exception {
                  return 0;
               }

               @Override
               public void position(long pos) throws IOException {
               }

               @Override
               public long position() {
                  return 0;
               }

               @Override
               public void close() throws Exception {
                  open.set(false);
               }

               @Override
               public void sync() throws IOException {
                  sync.set(true);
               }

               @Override
               public long size() throws Exception {
                  return 0;
               }

               @Override
               public void renameTo(String newFileName) throws Exception {
               }

               @Override
               public SequentialFile cloneFile() {
                  return null;
               }

               @Override
               public void copyTo(SequentialFile newFileName) throws Exception {
               }

               @Override
               public void setTimedBuffer(TimedBuffer buffer) {
               }

               @Override
               public File getJavaFile() {
                  return null;
               }
            };
         }
      };

      LargeServerMessageImpl largeServerMessage = new LargeServerMessageImpl(storageManager);
      largeServerMessage.setMessageID(1234);
      largeServerMessage.addBytes(new byte[0]);
      assertTrue(open.get());
      largeServerMessage.releaseResources(true);
      assertTrue(sync.get());
   }

   @Test
   public void testLargeServerMessageCopyIsolation() throws Exception {
      ActiveMQServer server = createServer(true);
      server.start();

      try {
         LargeServerMessageImpl largeMessage = new LargeServerMessageImpl((JournalStorageManager)server.getStorageManager());
         largeMessage.setMessageID(23456);

         for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
            largeMessage.addBytes(new byte[]{ActiveMQTestBase.getSamplebyte(i)});
         }

         //now replace the underlying file with a fake
         replaceFile(largeMessage);

         Message copied = largeMessage.copy(99999);
         assertEquals(99999, copied.getMessageID());

      } finally {
         server.stop();
      }
   }

   @Test
   public void testMessagePluginForLargeMessage() throws Exception {
      ActiveMQServer server = createServer(true);

      LargeMessagePlugin plugin = new LargeMessagePlugin();
      server.registerBrokerPlugin(plugin);

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false);

      try {
         LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

         fileMessage.setMessageID(1005);

         for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
            fileMessage.addBytes(new byte[]{ActiveMQTestBase.getSamplebyte(i)});
         }
         // The server would be doing this
         fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         fileMessage.releaseResources(false);

         session.createQueue("A", RoutingType.ANYCAST, "A");

         ClientProducer prod = session.createProducer("A");

         prod.send(fileMessage);

         fileMessage.deleteFile();

         session.commit();

         session.start();

         ClientConsumer cons = session.createConsumer("A");

         ClientMessage msg = cons.receive(5000);

         Assert.assertNotNull(msg);

         Assert.assertEquals(msg.getBodySize(), 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
            Assert.assertEquals(ActiveMQTestBase.getSamplebyte(i), msg.getBodyBuffer().readByte());
         }

         msg.acknowledge();

         session.commit();

         plugin.validateSuccessfulSendAndReceive();
      } finally {
         sf.close();
         locator.close();
         server.stop();
      }
   }

   @Test
   public void testMessagePluginForLargeMessageOnSendException() throws Exception {
      ActiveMQServer server = createServer(true);

      LargeMessagePlugin plugin = new LargeMessagePlugin();
      plugin.setOnSendException(true);

      server.registerBrokerPlugin(plugin);

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false);

      try {
         LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

         fileMessage.setMessageID(1005);

         for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
            fileMessage.addBytes(new byte[]{ActiveMQTestBase.getSamplebyte(i)});
         }
         // The server would be doing this
         fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         fileMessage.releaseResources(false);

         session.createQueue("A", RoutingType.ANYCAST, "A");

         ClientProducer prod = session.createProducer("A");

         prod.send(fileMessage);

         fileMessage.deleteFile();

         session.commit();

      } finally {
         sf.close();
         locator.close();
         server.stop();
      }
      assertTrue(plugin.onSendExceptionCalled.get());
   }

   //the test verifies that a corrupted (zero-sized)
   //large message will be detected by server and won't be
   //delivered to clients.
   @Test
   public void testCorruptedLargeMessage() throws Exception {
      ActiveMQServer server = createServer(true);

      final CountDownLatch latch = new CountDownLatch(1);

      server.registerBrokerPlugin(new ActiveMQServerMessagePlugin() {
         @Override
         public void afterSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue, RoutingStatus result) throws ActiveMQException {
            latch.countDown();
         }
      });

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false);

      try {
         LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

         fileMessage.setMessageID(1005);

         for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
            fileMessage.addBytes(new byte[]{ActiveMQTestBase.getSamplebyte(i)});
         }
         // The server would be doing this
         fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         fileMessage.releaseResources(false);

         session.createQueue("A", RoutingType.ANYCAST, "A");

         ClientProducer prod = session.createProducer("A");

         prod.send(fileMessage);

         fileMessage.deleteFile();

         session.commit();

         latch.await();
         //corrupt the large file
         String lmLoc = server.getConfiguration().getLargeMessagesDirectory();
         File lmDir = new File(lmLoc);

         Wait.assertEquals(1, () -> {
            File[] fileList = lmDir.listFiles();
            for (File f : fileList) {
               System.out.println("large file: " + f.getAbsolutePath() + " size: " + f.length());
            }
            return fileList.length;
         }, 2000);

         File[] fileList = lmDir.listFiles();
         File lmFile = fileList[0];

         lmFile.delete();
         lmFile.createNewFile();

         long size = lmFile.length();

         assertEquals(0, size);

         session.start();

         ClientConsumer cons = session.createConsumer("A");
         ClientMessage msg = cons.receive(1000);

         Assert.assertNull(msg);

         session.commit();

      } finally {
         sf.close();
         locator.close();
         server.stop();
      }
   }

   private void replaceFile(LargeServerMessageImpl largeMessage) throws Exception {
      SequentialFile originalFile = largeMessage.getAppendFile();
      MockSequentialFile mockFile = new MockSequentialFile(originalFile);

      largeMessage.getLargeBody().replaceFile(mockFile);
      mockFile.close();
   }

      // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   private class MockSequentialFile extends AbstractSequentialFile {

      private SequentialFile originalFile;

      MockSequentialFile(SequentialFile originalFile) throws Exception {
         super(originalFile.getJavaFile().getParentFile(), originalFile.getFileName(), new FakeSequentialFileFactory(), null);
         this.originalFile = originalFile;
         this.originalFile.close();
      }

      @Override
      public void open() throws Exception {
         //open and close it right away to simulate failure condition
         originalFile.open();
         originalFile.close();
      }

      @Override
      public void open(int maxIO, boolean useExecutor) throws Exception {
      }

      @Override
      public ByteBuffer map(int position, long size) throws IOException {
         return null;
      }

      @Override
      public boolean isOpen() {
         return originalFile.isOpen();
      }

      @Override
      public int calculateBlockStart(int position) throws Exception {
         return originalFile.calculateBlockStart(position);
      }

      @Override
      public void fill(int size) throws Exception {
         originalFile.fill(size);
      }

      @Override
      public void writeDirect(ByteBuffer bytes, boolean sync, IOCallback callback) {
         originalFile.writeDirect(bytes, sync, callback);
      }

      @Override
      public void writeDirect(ByteBuffer bytes, boolean sync) throws Exception {
         originalFile.writeDirect(bytes, sync);
      }

      @Override
      public void blockingWriteDirect(ByteBuffer bytes, boolean sync, boolean releaseBuffer) throws Exception {
         originalFile.blockingWriteDirect(bytes, sync, releaseBuffer);
      }

      @Override
      public int read(ByteBuffer bytes, IOCallback callback) throws Exception {
         return originalFile.read(bytes, callback);
      }

      @Override
      public int read(ByteBuffer bytes) throws Exception {
         return originalFile.read(bytes);
      }

      @Override
      public void sync() throws IOException {
         originalFile.sync();
      }

      @Override
      public long size() throws Exception {
         return originalFile.size();
      }

      @Override
      public SequentialFile cloneFile() {
         return originalFile.cloneFile();
      }
   }

   private static class LargeMessagePlugin implements ActiveMQServerMessagePlugin {

      private AtomicBoolean beforeSendCalled = new AtomicBoolean(false);
      private AtomicBoolean afterSendCalled = new AtomicBoolean(false);
      private AtomicBoolean onSendExceptionCalled = new AtomicBoolean(false);
      private AtomicBoolean beforeMessageRouteCalled = new AtomicBoolean(false);
      private AtomicBoolean afterMessageRouteCalled = new AtomicBoolean(false);
      private AtomicBoolean onMessageRouteExceptionCalled = new AtomicBoolean(false);
      private AtomicBoolean beforeDeliverCalled = new AtomicBoolean(false);
      private AtomicBoolean afterDeliverCalled = new AtomicBoolean(false);
      private AtomicBoolean messageExpiredCalled = new AtomicBoolean(false);
      private AtomicBoolean messageAcknowledgedCalled = new AtomicBoolean(false);

      private AtomicBoolean throwOnSend = new AtomicBoolean(false);


      @Override
      public void beforeSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue) throws ActiveMQException {
         this.beforeSendCalled.set(true);
         if (throwOnSend.get()) {
            throw new ActiveMQException("for test!");
         }
      }

      @Override
      public void afterSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue, RoutingStatus result) throws ActiveMQException {
         this.afterSendCalled.set(true);
      }

      @Override
      public void onSendException(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue,
                                   Exception e) throws ActiveMQException {
         this.onSendExceptionCalled.set(true);
      }

      @Override
      public void beforeMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates) throws ActiveMQException {
         this.beforeMessageRouteCalled.set(true);
      }

      @Override
      public void afterMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates,
                                    RoutingStatus result) throws ActiveMQException {
         this.afterMessageRouteCalled.set(true);
      }

      @Override
      public void onMessageRouteException(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates,
                                           Exception e) throws ActiveMQException {
         this.onMessageRouteExceptionCalled.set(true);
      }

      @Override
      public void beforeDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
         this.beforeDeliverCalled.set(true);
      }

      @Override
      public void afterDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
         this.afterDeliverCalled.set(true);
      }

      @Override
      public void messageExpired(MessageReference message, SimpleString messageExpiryAddress, ServerConsumer consumer) throws ActiveMQException {
         this.messageExpiredCalled.set(true);
      }

      @Override
      public void messageAcknowledged(MessageReference ref, AckReason reason, ServerConsumer consumer) throws ActiveMQException {
         this.messageAcknowledgedCalled.set(true);
      }

      public void validateSuccessfulSendAndReceive() {
         assertTrue(beforeSendCalled.get());
         assertTrue(afterSendCalled.get());
         assertFalse(onSendExceptionCalled.get());
         assertTrue(beforeMessageRouteCalled.get());
         assertTrue(afterMessageRouteCalled.get());
         assertFalse(onMessageRouteExceptionCalled.get());
         assertTrue(beforeDeliverCalled.get());
         assertTrue(afterDeliverCalled.get());
         assertFalse(messageExpiredCalled.get());
         assertTrue(messageAcknowledgedCalled.get());
      }

      public void setOnSendException(boolean flag) {
         this.throwOnSend.set(flag);
      }
   }
}
