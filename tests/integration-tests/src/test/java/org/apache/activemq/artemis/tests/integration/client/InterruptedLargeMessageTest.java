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
package org.apache.activemq.artemis.tests.integration.client;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionContinuationMessage;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueConfig;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.largemessage.LargeMessageTestBase;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InterruptedLargeMessageTest extends LargeMessageTestBase {
   // Constants -----------------------------------------------------

   static final int RECEIVE_WAIT_TIME = 60000;

   private final int LARGE_MESSAGE_SIZE = ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE * 3;

   private final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected ServerLocator locator;

   public InterruptedLargeMessageTest(StoreConfiguration.StoreType storeType) {
      super(storeType);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      LargeMessageTestInterceptorIgnoreLastPacket.clearInterrupt();
      locator = createFactory(isNetty());
   }

   protected boolean isNetty() {
      return false;
   }

   @Test
   public void testInterruptLargeMessageSend() throws Exception {

      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.clearInterrupt();
      ActiveMQServer server = createServer(true, isNetty());

      server.getConfiguration().getIncomingInterceptorClassNames().add(LargeMessageTestInterceptorIgnoreLastPacket.class.getName());

      server.start();

      locator.setBlockOnNonDurableSend(false).setBlockOnDurableSend(false);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      Message clientFile = createLargeClientMessageStreaming(session, LARGE_MESSAGE_SIZE, true);

      clientFile.setExpiration(System.currentTimeMillis());

      producer.send(clientFile);

      Thread.sleep(500);

      for (ServerSession srvSession : server.getSessions()) {
         ((ServerSessionImpl) srvSession).clearLargeMessage();
      }

      server.stop(false);

      ActiveMQTestBase.forceGC();

      server.start();

      server.stop();

      validateNoFilesOnLargeDir();
   }

   @Test
   public void testCloseConsumerDuringTransmission() throws Exception {
      ActiveMQServer server = createServer(true, isNetty());

      LargeMessageTestInterceptorIgnoreLastPacket.disableInterrupt();

      server.start();

      locator.setBlockOnNonDurableSend(false).setBlockOnDurableSend(false).addIncomingInterceptor(new LargeMessageTestInterceptorIgnoreLastPacket());

      ClientSessionFactory sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      Message clientFile = createLargeClientMessageStreaming(session, LARGE_MESSAGE_SIZE, true);

      producer.send(clientFile);

      session.commit();

      LargeMessageTestInterceptorIgnoreLastPacket.clearInterrupt();

      final AtomicInteger unexpectedErrors = new AtomicInteger(0);
      final AtomicInteger expectedErrors = new AtomicInteger(0);
      final ClientConsumer cons = session.createConsumer(ADDRESS);

      session.start();

      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               System.out.println("Receiving message");
               ClientMessage msg = cons.receive(5000);
               if (msg == null) {
                  System.err.println("Message not received");
                  unexpectedErrors.incrementAndGet();
                  return;
               }

               msg.checkCompletion();
            } catch (ActiveMQException e) {
               e.printStackTrace();
               expectedErrors.incrementAndGet();
            }
         }
      };

      t.start();

      LargeMessageTestInterceptorIgnoreLastPacket.awaitInterrupt();

      cons.close();

      t.join();

      Assert.assertEquals(0, unexpectedErrors.get());
      Assert.assertEquals(1, expectedErrors.get());

      session.close();

      server.stop();
   }

   @Test
   public void testForcedInterruptUsingJMS() throws Exception {
      ActiveMQServer server = createServer(true, isNetty());

      server.start();

      SimpleString jmsAddress = new SimpleString("jms.queue.Test");

      server.createQueue(jmsAddress, jmsAddress, null, true, false);

      final AtomicInteger unexpectedErrors = new AtomicInteger(0);
      final AtomicInteger expectedErrors = new AtomicInteger(0);
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://0");
      Connection connection = cf.createConnection();
      Session session = connection.createSession(Session.SESSION_TRANSACTED);
      connection.start();
      final MessageConsumer consumer = session.createConsumer(session.createQueue(jmsAddress.toString()));

      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               System.out.println("Receiving message");
               javax.jms.Message msg = consumer.receive(5000);
               if (msg == null) {
                  System.err.println("Message not received");
                  unexpectedErrors.incrementAndGet();
                  return;
               }
            } catch (JMSException e) {
               log.debug("This exception was ok as it was expected", e);
               expectedErrors.incrementAndGet();
            } catch (Throwable e) {
               log.warn("Captured unexpected exception", e);
               unexpectedErrors.incrementAndGet();
            }
         }
      };

      t.start();
      t.interrupt();

      t.join();

      Assert.assertEquals(0, unexpectedErrors.get());
      Assert.assertEquals(1, expectedErrors.get());

      session.close();

      server.stop();
   }

   @Test
   public void testSendNonPersistentQueue() throws Exception {

      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.disableInterrupt();
      ActiveMQServer server = createServer(true, isNetty());

      server.start();

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < 10; i++) {
         Message clientFile = createLargeClientMessageStreaming(session, LARGE_MESSAGE_SIZE, true);

         producer.send(clientFile);
      }
      session.commit();

      session.close();

      session = sf.createSession(false, false);

      ClientConsumer cons = session.createConsumer(ADDRESS);

      session.start();

      for (int h = 0; h < 5; h++) {
         for (int i = 0; i < 10; i++) {
            ClientMessage clientMessage = cons.receive(5000);
            Assert.assertNotNull(clientMessage);
            for (int countByte = 0; countByte < LARGE_MESSAGE_SIZE; countByte++) {
               Assert.assertEquals(ActiveMQTestBase.getSamplebyte(countByte), clientMessage.getBodyBuffer().readByte());
            }
            clientMessage.acknowledge();
         }
         session.rollback();
      }

      server.stop(false);
      server.start();

      server.stop();

      validateNoFilesOnLargeDir();
   }

   @Test
   public void testSendPaging() throws Exception {

      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.disableInterrupt();
      ActiveMQServer server = createServer(true, createDefaultConfig(isNetty()), 10000, 20000, new HashMap<String, AddressSettings>());

      // server.getConfiguration()
      // .getIncomingInterceptorClassNames()
      // .add(LargeMessageTestInterceptorIgnoreLastPacket.class.getName());

      server.start();

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      server.getPagingManager().getPageStore(ADDRESS).startPaging();

      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < 10; i++) {
         Message clientFile = createLargeClientMessageStreaming(session, LARGE_MESSAGE_SIZE, true);

         producer.send(clientFile);
      }
      session.commit();

      validateNoFilesOnLargeDir(server.getConfiguration().getLargeMessagesDirectory(), 10);

      for (int h = 0; h < 5; h++) {
         session.close();

         sf.close();

         server.stop();

         server.start();

         sf = createSessionFactory(locator);

         session = sf.createSession(false, false);

         ClientConsumer cons = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < 10; i++) {
            ClientMessage clientMessage = cons.receive(5000);
            Assert.assertNotNull(clientMessage);
            for (int countByte = 0; countByte < LARGE_MESSAGE_SIZE; countByte++) {
               Assert.assertEquals(ActiveMQTestBase.getSamplebyte(countByte), clientMessage.getBodyBuffer().readByte());
            }
            clientMessage.acknowledge();
         }
         if (h == 4) {
            session.commit();
         } else {
            session.rollback();
         }

         session.close();
         sf.close();
      }

      server.stop(false);
      server.start();

      validateNoFilesOnLargeDir();

   }

   @Test
   public void testSendPreparedXA() throws Exception {
      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.disableInterrupt();

      ActiveMQServer server = createServer(true, createDefaultConfig(isNetty()), 10000, 20000, new HashMap<String, AddressSettings>());

      server.getConfiguration().getIncomingInterceptorClassNames().add(LargeMessageTestInterceptorIgnoreLastPacket.class.getName());

      server.start();

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(true, false, false);

      Xid xid1 = newXID();
      Xid xid2 = newXID();

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      session.start(xid1, XAResource.TMNOFLAGS);

      for (int i = 0; i < 10; i++) {
         Message clientFile = createLargeClientMessageStreaming(session, LARGE_MESSAGE_SIZE, true);
         clientFile.putIntProperty("txid", 1);
         producer.send(clientFile);
      }
      session.end(xid1, XAResource.TMSUCCESS);

      session.prepare(xid1);

      session.start(xid2, XAResource.TMNOFLAGS);

      for (int i = 0; i < 10; i++) {
         Message clientFile = createLargeClientMessageStreaming(session, LARGE_MESSAGE_SIZE, true);
         clientFile.putIntProperty("txid", 2);
         clientFile.putIntProperty("i", i);
         producer.send(clientFile);
      }
      session.end(xid2, XAResource.TMSUCCESS);

      session.prepare(xid2);

      session.close();
      sf.close();

      server.stop(false);
      server.start();

      for (int start = 0; start < 2; start++) {
         System.out.println("Start " + start);

         sf = createSessionFactory(locator);

         if (start == 0) {
            session = sf.createSession(true, false, false);
            session.commit(xid1, false);
            session.close();
         }

         session = sf.createSession(false, false, false);
         ClientConsumer cons1 = session.createConsumer(ADDRESS);
         session.start();
         for (int i = 0; i < 10; i++) {
            log.info("I = " + i);
            ClientMessage msg = cons1.receive(5000);
            Assert.assertNotNull(msg);
            Assert.assertEquals(1, msg.getIntProperty("txid").intValue());
            msg.acknowledge();
         }

         if (start == 1) {
            session.commit();
         } else {
            session.rollback();
         }

         session.close();
         sf.close();

         server.stop();
         server.start();
      }
      server.stop();

      validateNoFilesOnLargeDir(server.getConfiguration().getLargeMessagesDirectory(), 10);

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(true, false, false);
      session.rollback(xid2);

      sf.close();

      server.stop();
      server.start();
      server.stop();

      validateNoFilesOnLargeDir();
   }

   @Test
   public void testRestartBeforeDelete() throws Exception {

      class NoPostACKQueue extends QueueImpl {

         NoPostACKQueue(long id,
                        SimpleString address,
                        SimpleString name,
                        Filter filter,
                        SimpleString user,
                        PageSubscription pageSubscription,
                        boolean durable,
                        boolean temporary,
                        boolean autoCreated,
                        ScheduledExecutorService scheduledExecutor,
                        PostOffice postOffice,
                        StorageManager storageManager,
                        HierarchicalRepository<AddressSettings> addressSettingsRepository,
                        Executor executor) {
            super(id, address, name, filter, pageSubscription, user, durable, temporary, autoCreated, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executor);
         }

         @Override
         public void postAcknowledge(final MessageReference ref) {
            System.out.println("Ignoring postACK on message " + ref);
         }

         @Override
         public void deliverScheduledMessages() {
         }
      }

      final class NoPostACKQueueFactory implements QueueFactory {

         final StorageManager storageManager;

         final PostOffice postOffice;

         final ScheduledExecutorService scheduledExecutor;

         final HierarchicalRepository<AddressSettings> addressSettingsRepository;

         final ExecutorFactory execFactory;

         NoPostACKQueueFactory(StorageManager storageManager,
                               PostOffice postOffice,
                               ScheduledExecutorService scheduledExecutor,
                               HierarchicalRepository<AddressSettings> addressSettingsRepository,
                               final ExecutorFactory execFactory) {
            this.storageManager = storageManager;
            this.postOffice = postOffice;
            this.scheduledExecutor = scheduledExecutor;
            this.addressSettingsRepository = addressSettingsRepository;
            this.execFactory = execFactory;
         }

         @Override
         public Queue createQueueWith(final QueueConfig config) {
            return new NoPostACKQueue(config.id(), config.address(), config.name(), config.filter(), config.user(), config.pageSubscription(), config.isDurable(), config.isTemporary(), config.isAutoCreated(), scheduledExecutor, postOffice, storageManager, addressSettingsRepository, execFactory.getExecutor());
         }

         @Deprecated
         @Override
         public Queue createQueue(long persistenceID,
                                  SimpleString address,
                                  SimpleString name,
                                  Filter filter,
                                  PageSubscription pageSubscription,
                                  SimpleString user,
                                  boolean durable,
                                  boolean temporary,
                                  boolean autoCreated) {

            return new NoPostACKQueue(persistenceID, address, name, filter, user, pageSubscription, durable, temporary, autoCreated, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, execFactory.getExecutor());
         }

         /* (non-Javadoc)
          * @see org.apache.activemq.artemis.core.server.QueueFactory#setPostOffice(org.apache.activemq.artemis.core.postoffice.PostOffice)
          */
         @Override
         public void setPostOffice(PostOffice postOffice) {
         }

      }

      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.disableInterrupt();

      ActiveMQServer server = createServer(true, isNetty());
      server.start();

      QueueFactory original = server.getQueueFactory();

      ((ActiveMQServerImpl) server).replaceQueueFactory(new NoPostACKQueueFactory(server.getStorageManager(), server.getPostOffice(), server.getScheduledPool(), server.getAddressSettingsRepository(), server.getExecutorFactory()));

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < 10; i++) {
         Message clientFile = createLargeClientMessageStreaming(session, LARGE_MESSAGE_SIZE, true);

         producer.send(clientFile);
      }
      session.commit();

      session.close();

      session = sf.createSession(false, false);

      ClientConsumer cons = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < 10; i++) {
         ClientMessage msg = cons.receive(5000);
         Assert.assertNotNull(msg);
         msg.saveToOutputStream(new java.io.OutputStream() {
            @Override
            public void write(int b) throws IOException {
            }
         });
         msg.acknowledge();
         session.commit();
      }

      ((ActiveMQServerImpl) server).replaceQueueFactory(original);
      server.stop(false);
      server.start();

      server.stop();

      validateNoFilesOnLargeDir();
   }

   @Test
   public void testConsumeAfterRestart() throws Exception {
      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.clearInterrupt();

      ActiveMQServer server = createServer(true, isNetty());
      server.start();

      QueueFactory original = server.getQueueFactory();

      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < 10; i++) {
         Message clientFile = createLargeClientMessageStreaming(session, LARGE_MESSAGE_SIZE, true);

         producer.send(clientFile);
      }
      session.commit();

      session.close();
      sf.close();

      server.stop();
      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, false);

      ClientConsumer cons = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < 10; i++) {
         ClientMessage msg = cons.receive(5000);
         Assert.assertNotNull(msg);
         msg.saveToOutputStream(new java.io.OutputStream() {
            @Override
            public void write(int b) throws IOException {
            }
         });
         msg.acknowledge();
         session.commit();
      }

      ((ActiveMQServerImpl) server).replaceQueueFactory(original);
      server.stop(false);
      server.start();

      server.stop();

      validateNoFilesOnLargeDir();
   }

   public static class LargeMessageTestInterceptorIgnoreLastPacket implements Interceptor {

      public static void clearInterrupt() {
         intMessages = true;
         latch = new CountDownLatch(1);
      }

      public static void disableInterrupt() {
         intMessages = false;
      }

      public static void awaitInterrupt() throws Exception {
         latch.await();
      }

      private static boolean intMessages = false;

      private static CountDownLatch latch = new CountDownLatch(1);

      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) throws ActiveMQException {
         if (packet instanceof SessionContinuationMessage) {
            SessionContinuationMessage msg = (SessionContinuationMessage) packet;
            if (!msg.isContinues() && intMessages) {
               System.out.println("Ignored a message");
               latch.countDown();
               return false;
            }
         }
         return true;
      }
   }
}
