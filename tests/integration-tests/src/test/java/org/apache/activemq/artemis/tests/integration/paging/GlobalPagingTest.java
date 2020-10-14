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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientRequestor;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GlobalPagingTest extends PagingTest {

   public GlobalPagingTest(StoreConfiguration.StoreType storeType, boolean mapped) {
      super(storeType, mapped);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }

   @Override
   protected ActiveMQServer createServer(final boolean realFiles,
                                         final Configuration configuration,
                                         final int pageSize,
                                         final long maxAddressSize,
                                         final Map<String, AddressSettings> settings) {
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(configuration, realFiles));

      if (settings != null) {
         for (Map.Entry<String, AddressSettings> setting : settings.entrySet()) {
            server.getAddressSettingsRepository().addMatch(setting.getKey(), setting.getValue());
         }
      }

      server.getConfiguration().setGlobalMaxSize(maxAddressSize);
      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(pageSize).setMaxSizeBytes(-1).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   // test doesn't make sense on GlobalPaging due to configuration issues
   @Test @Ignore @Override
   public void testPurge() throws Exception {
   }

   @Test
   public void testPagingOverFullDisk() throws Exception {
      if (storeType == StoreConfiguration.StoreType.DATABASE) return;

      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      server = createServer(true, config, PagingTest.PAGE_SIZE, PagingTest.PAGE_MAX);
      server.getConfiguration().setGlobalMaxSize(-1);

      server.start();

      ActiveMQServerImpl serverImpl = (ActiveMQServerImpl) server;
      serverImpl.getMonitor().stop(); // stop the scheduled executor, we will do it manually only
      serverImpl.getMonitor().tick();

      final int numberOfMessages = 500;

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true);

      sf = createSessionFactory(locator);

      final ClientSession session = sf.createSession(false, false, false);

      session.createQueue(new QueueConfiguration(PagingTest.ADDRESS));

      final ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      ClientMessage message = null;

      final byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      Queue queue = server.locateQueue(ADDRESS);
      queue.getPageSubscription().getPagingStore().forceAnotherPage();

      sendFewMessages(numberOfMessages, session, producer, body);

      serverImpl.getMonitor().setMaxUsage(0); // forcing disk full (faking it)

      serverImpl.getMonitor().tick();

      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               sendFewMessages(numberOfMessages, session, producer, body);
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      };

      t.start();

      t.join(1000);
      Assert.assertTrue(t.isAlive());

      // releasing the disk
      serverImpl.getMonitor().setMaxUsage(1).tick();
      t.join(5000);
      Assert.assertFalse(t.isAlive());

      session.start();

      Wait.assertEquals(numberOfMessages * 2, queue::getMessageCount);

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

      Wait.assertEquals(0, queue::getMessageCount);
   }

   protected void sendFewMessages(int numberOfMessages,
                                  ClientSession session,
                                  ClientProducer producer,
                                  byte[] body) throws ActiveMQException {
      ClientMessage message;
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
   }

   @Test
   public void testManagementAddressCannotPageOrChangeGlobalSize() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      final ActiveMQServer server = createServer(true, config, PagingTest.PAGE_SIZE, -1);

      try {
         final SimpleString managementAddress = server.getConfiguration().getManagementAddress();
         server.getConfiguration().setGlobalMaxSize(1);
         server.start();

         final ServerLocator locator = createInVMNonHALocator()
            .setBlockOnNonDurableSend(true)
            .setBlockOnDurableSend(true)
            .setBlockOnAcknowledge(true);

         try (ClientSessionFactory sf = createSessionFactory(locator);

              ClientSession session = sf.createSession(false, true, true)) {

            session.start();

            if (server.locateQueue(managementAddress) == null) {

               session.createQueue(new QueueConfiguration(managementAddress));
            }

            final Queue managementQueue = server.locateQueue(managementAddress);

            Assert.assertNull(managementQueue.getPageSubscription());

            Assert.assertNull(server.getPagingManager().getPageStore(managementAddress));

            final SimpleString address = SimpleString.toSimpleString("queue");

            if (server.locateQueue(address) == null) {

               session.createQueue(new QueueConfiguration(address));
            }

            final CountDownLatch startSendMessages = new CountDownLatch(1);

            final PagingManager pagingManager = server.getPagingManager();

            final long globalSize = pagingManager.getGlobalSize();

            final Thread globalSizeChecker = new Thread(() -> {
               startSendMessages.countDown();
               while (!Thread.currentThread().isInterrupted()) {
                  Assert.assertEquals(globalSize, pagingManager.getGlobalSize());
               }
            });

            globalSizeChecker.start();

            try (ClientRequestor requestor = new ClientRequestor(session, managementAddress)) {

               ClientMessage message = session.createMessage(false);

               ManagementHelper.putAttribute(message, "queue." + address.toString(), "messageCount");

               Assert.assertTrue("bodySize = " + message.getBodySize() + " must be > of globalMaxSize = " + server.getConfiguration().getGlobalMaxSize(), message.getBodySize() > server.getConfiguration().getGlobalMaxSize());

               startSendMessages.await();

               for (int i = 0; i < 100; i++) {
                  try {
                     ClientMessage reply = requestor.request(message);
                     Assert.assertEquals(0L, ManagementHelper.getResult(reply));
                  } catch (ActiveMQAddressFullException e) {
                     Assert.fail(e.getMessage());
                     return;
                  }
               }

            } finally {
               globalSizeChecker.interrupt();
            }
         }

      } finally {
         server.stop(true);
      }
   }

   @Test
   public void testManagementMessageRequestCannotFailAfterFailedDirectDeliver() throws Exception {
      clearDataRecreateServerDirs();

      Configuration config = createDefaultNettyConfig().setJournalSyncNonTransactional(false);

      final ActiveMQServer server = createServer(true, config, PagingTest.PAGE_SIZE, -1);

      try {
         final SimpleString managementAddress = server.getConfiguration().getManagementAddress();
         server.start();
         //need to use Netty in order to have direct delivery available
         final ServerLocator locator = createNettyNonHALocator()
            .setBlockOnNonDurableSend(true)
            .setBlockOnDurableSend(true)
            .setBlockOnAcknowledge(true);

         try (ClientSessionFactory sf = createSessionFactory(locator);

              ClientSession session = sf.createSession(false, true, true)) {

            session.start();

            if (server.locateQueue(managementAddress) == null) {

               session.createQueue(new QueueConfiguration(managementAddress));
            }

            final SimpleString address = SimpleString.toSimpleString("queue");

            if (server.locateQueue(address) == null) {

               session.createQueue(new QueueConfiguration(address));
            }

            try (ClientProducer requestProducer = session.createProducer(managementAddress)) {
               final SimpleString replyQueue = new SimpleString(managementAddress + "." + UUID.randomUUID().toString());
               session.createQueue(new QueueConfiguration(replyQueue).setRoutingType(ActiveMQDefaultConfiguration.getDefaultRoutingType()).setDurable(false).setTemporary(true));
               int id = 1000;
               try (ClientConsumer consumer = session.createConsumer(replyQueue)) {
                  final Queue queue = server.locateQueue(replyQueue);
                  final MessageReference reference = MessageReference.Factory.createReference(session.createMessage(false), queue, null);
                  reference.getMessage().setMessageID(id++);
                  //it will cause QueueImpl::directDeliver -> false
                  queue.addHead(reference, false);
                  Wait.assertFalse(queue::isDirectDeliver);
                  queue.removeReferenceWithID(reference.getMessageID());
                  ClientMessage message = session.createMessage(false);
                  message.putStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME, replyQueue);
                  ManagementHelper.putAttribute(message, "queue." + address.toString(), "messageCount");
                  requestProducer.send(message);
                  Assert.assertNotNull(consumer.receive());
               }
            }
         }

      } finally {
         server.stop(true);
      }
   }

}
