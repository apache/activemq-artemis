/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.client;

import javax.transaction.xa.Xid;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.QueueStatus;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.config.PersistedAddressSetting;
import org.apache.activemq.artemis.core.persistence.config.PersistedRoles;
import org.apache.activemq.artemis.core.persistence.impl.PageCountPending;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.server.group.impl.GroupBinding;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.JournalLoader;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.SpawnedVMSupport;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SendAckFailTest extends ActiveMQTestBase {

   @Before
   @After
   public void deleteDirectory() throws Exception {
      deleteDirectory(new File("./target/send-ack"));
   }

   @Override
   public String getJournalDir(final int index, final boolean backup) {
      return "./target/send-ack/journal";
   }

   @Override
   protected String getBindingsDir(final int index, final boolean backup) {
      return "./target/send-ack/binding";
   }

   @Override
   protected String getPageDir(final int index, final boolean backup) {
      return "./target/send-ack/page";
   }

   @Override
   protected String getLargeMessagesDir(final int index, final boolean backup) {
      return "./target/send-ack/large-message";
   }

   @Test
   public void testSend() throws Exception {
      Process process = SpawnedVMSupport.spawnVM(SendAckFailTest.class.getName());
      ActiveMQServer server = null;

      try {

         HashSet<Integer> listSent = new HashSet<>();

         Thread t = null;
         {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
            ServerLocator locator = factory.getServerLocator();

            locator.setConfirmationWindowSize(0).setInitialConnectAttempts(100).setRetryInterval(100).setBlockOnDurableSend(false).setReconnectAttempts(0);

            ClientSessionFactory sf = locator.createSessionFactory();

            ClientSession session = sf.createSession();
            session.createAddress(SimpleString.toSimpleString("T1"), RoutingType.ANYCAST, true);
            session.createQueue(SimpleString.toSimpleString("T1"), RoutingType.ANYCAST, SimpleString.toSimpleString("T1"), true);

            ClientProducer producer = session.createProducer("T1");

            session.setSendAcknowledgementHandler(new SendAcknowledgementHandler() {
               @Override
               public void sendAcknowledged(Message message) {
                  listSent.add(message.getIntProperty("myid"));
               }
            });

            t = new Thread() {
               @Override
               public void run() {
                  for (int i = 0; i < 5000; i++) {
                     try {
                        producer.send(session.createMessage(true).putIntProperty("myid", i));
                     } catch (Exception e) {
                        e.printStackTrace();
                        break;
                     }
                  }
               }
            };
            t.start();
         }

         Wait.waitFor(() -> listSent.size() > 100, 5000, 10);

         Assert.assertTrue(process.waitFor(1, TimeUnit.MINUTES));

         server = startServer(false);

         {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
            ServerLocator locator = factory.getServerLocator();

            ClientSessionFactory sf = locator.createSessionFactory();

            ClientSession session = sf.createSession();

            ClientConsumer consumer = session.createConsumer("T1");

            session.start();

            for (int i = 0; i < listSent.size(); i++) {
               ClientMessage message = consumer.receive(1000);
               if (message == null) {
                  for (Integer msgi : listSent) {
                     System.out.println("Message " + msgi + " was lost");
                  }
                  fail("missed messages!");
               }
               message.acknowledge();

               if (!listSent.remove(message.getIntProperty("myid"))) {
                  System.out.println("Message " + message + " with id " + message.getIntProperty("myid") + " received in duplicate");
                  fail("Message " + message + " with id " + message.getIntProperty("myid") + " received in duplicate");
               }
            }
         }

      } finally {
         if (process != null) {
            process.destroy();
         }
         if (server != null) {
            server.stop();
         }
      }
   }

   public static void main(String[] arg) {
      SendAckFailTest test = new SendAckFailTest();
      test.startServer(true);
   }

   public ActiveMQServer startServer(boolean fail) {
      try {
         //ActiveMQServerImpl server = (ActiveMQServerImpl) createServer(true, true);

         AtomicInteger count = new AtomicInteger(0);

         ActiveMQSecurityManager securityManager = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), new SecurityConfiguration());

         Configuration configuration = createDefaultConfig(true);

         ActiveMQServer server = new ActiveMQServerImpl(configuration, ManagementFactory.getPlatformMBeanServer(), securityManager) {
            @Override
            public StorageManager createStorageManager() {
               StorageManager original = super.createStorageManager();

               return new StorageManagerDelegate(original) {
                  @Override
                  public void storeMessage(Message message) throws Exception {

                     if (fail) {
                        if (count.incrementAndGet() == 110) {
                           System.out.println("Failing " + message);
                           System.out.flush();
                           Thread.sleep(100);
                           Runtime.getRuntime().halt(-1);
                        }
                     }
                     super.storeMessage(message);

                  }
               };

            }
         };



         System.out.println("Location::" + server.getConfiguration().getJournalLocation().getAbsolutePath());
         server.start();
         return server;
      } catch (Exception e) {
         e.printStackTrace();
         return null;
      }
   }


   private class StorageManagerDelegate implements StorageManager {

      @Override
      public void start() throws Exception {
         manager.start();
      }

      @Override
      public void stop() throws Exception {
         manager.stop();
      }

      @Override
      public void updateQueueBinding(long tx, Binding binding) throws Exception {
         manager.updateQueueBinding(tx, binding);
      }

      @Override
      public boolean isStarted() {
         return manager.isStarted();
      }

      @Override
      public long generateID() {
         return manager.generateID();
      }

      @Override
      public long getCurrentID() {
         return manager.getCurrentID();
      }

      @Override
      public void criticalError(Throwable error) {
         manager.criticalError(error);
      }

      @Override
      public OperationContext getContext() {
         return manager.getContext();
      }

      @Override
      public void lineUpContext() {
         manager.lineUpContext();
      }

      @Override
      public OperationContext newContext(Executor executor) {
         return manager.newContext(executor);
      }

      @Override
      public OperationContext newSingleThreadContext() {
         return manager.newSingleThreadContext();
      }

      @Override
      public void setContext(OperationContext context) {
         manager.setContext(context);
      }

      @Override
      public void stop(boolean ioCriticalError, boolean sendFailover) throws Exception {
         manager.stop(ioCriticalError, sendFailover);
      }

      @Override
      public void pageClosed(SimpleString storeName, int pageNumber) {
         manager.pageClosed(storeName, pageNumber);
      }

      @Override
      public void pageDeleted(SimpleString storeName, int pageNumber) {
         manager.pageDeleted(storeName, pageNumber);
      }

      @Override
      public void pageWrite(PagedMessage message, int pageNumber) {
         manager.pageWrite(message, pageNumber);
      }

      @Override
      public void afterCompleteOperations(IOCallback run) {
         manager.afterCompleteOperations(run);
      }

      @Override
      public void afterStoreOperations(IOCallback run) {
         manager.afterStoreOperations(run);
      }

      @Override
      public boolean waitOnOperations(long timeout) throws Exception {
         return manager.waitOnOperations(timeout);
      }

      @Override
      public void waitOnOperations() throws Exception {
         manager.waitOnOperations();
      }

      @Override
      public void beforePageRead() throws Exception {
         manager.beforePageRead();
      }

      @Override
      public void afterPageRead() throws Exception {
         manager.afterPageRead();
      }

      @Override
      public ByteBuffer allocateDirectBuffer(int size) {
         return manager.allocateDirectBuffer(size);
      }

      @Override
      public void freeDirectBuffer(ByteBuffer buffer) {
         manager.freeDirectBuffer(buffer);
      }

      @Override
      public void clearContext() {
         manager.clearContext();
      }

      @Override
      public void confirmPendingLargeMessageTX(Transaction transaction,
                                               long messageID,
                                               long recordID) throws Exception {
         manager.confirmPendingLargeMessageTX(transaction, messageID, recordID);
      }

      @Override
      public void confirmPendingLargeMessage(long recordID) throws Exception {
         manager.confirmPendingLargeMessage(recordID);
      }

      @Override
      public void storeMessage(Message message) throws Exception {
         manager.storeMessage(message);
      }

      @Override
      public void storeReference(long queueID, long messageID, boolean last) throws Exception {
         manager.storeReference(queueID, messageID, last);
      }

      @Override
      public void deleteMessage(long messageID) throws Exception {
         manager.deleteMessage(messageID);
      }

      @Override
      public void storeAcknowledge(long queueID, long messageID) throws Exception {
         manager.storeAcknowledge(queueID, messageID);
      }

      @Override
      public void storeCursorAcknowledge(long queueID, PagePosition position) throws Exception {
         manager.storeCursorAcknowledge(queueID, position);
      }

      @Override
      public void updateDeliveryCount(MessageReference ref) throws Exception {
         manager.updateDeliveryCount(ref);
      }

      @Override
      public void updateScheduledDeliveryTime(MessageReference ref) throws Exception {
         manager.updateScheduledDeliveryTime(ref);
      }

      @Override
      public void storeDuplicateID(SimpleString address, byte[] duplID, long recordID) throws Exception {
         manager.storeDuplicateID(address, duplID, recordID);
      }

      @Override
      public void deleteDuplicateID(long recordID) throws Exception {
         manager.deleteDuplicateID(recordID);
      }

      @Override
      public void storeMessageTransactional(long txID, Message message) throws Exception {
         manager.storeMessageTransactional(txID, message);
      }

      @Override
      public void storeReferenceTransactional(long txID, long queueID, long messageID) throws Exception {
         manager.storeReferenceTransactional(txID, queueID, messageID);
      }

      @Override
      public void storeAcknowledgeTransactional(long txID, long queueID, long messageID) throws Exception {
         manager.storeAcknowledgeTransactional(txID, queueID, messageID);
      }

      @Override
      public void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position) throws Exception {
         manager.storeCursorAcknowledgeTransactional(txID, queueID, position);
      }

      @Override
      public void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception {
         manager.deleteCursorAcknowledgeTransactional(txID, ackID);
      }

      @Override
      public void deleteCursorAcknowledge(long ackID) throws Exception {
         manager.deleteCursorAcknowledge(ackID);
      }

      @Override
      public void storePageCompleteTransactional(long txID, long queueID, PagePosition position) throws Exception {
         manager.storePageCompleteTransactional(txID, queueID, position);
      }

      @Override
      public void deletePageComplete(long ackID) throws Exception {
         manager.deletePageComplete(ackID);
      }

      @Override
      public void updateScheduledDeliveryTimeTransactional(long txID, MessageReference ref) throws Exception {
         manager.updateScheduledDeliveryTimeTransactional(txID, ref);
      }

      @Override
      public void storeDuplicateIDTransactional(long txID,
                                                SimpleString address,
                                                byte[] duplID,
                                                long recordID) throws Exception {
         manager.storeDuplicateIDTransactional(txID, address, duplID, recordID);
      }

      @Override
      public void updateDuplicateIDTransactional(long txID,
                                                 SimpleString address,
                                                 byte[] duplID,
                                                 long recordID) throws Exception {
         manager.updateDuplicateIDTransactional(txID, address, duplID, recordID);
      }

      @Override
      public void deleteDuplicateIDTransactional(long txID, long recordID) throws Exception {
         manager.deleteDuplicateIDTransactional(txID, recordID);
      }

      @Override
      public LargeServerMessage createLargeMessage() {
         return manager.createLargeMessage();
      }

      @Override
      public LargeServerMessage createLargeMessage(long id, Message message) throws Exception {
         return manager.createLargeMessage(id, message);
      }

      @Override
      public SequentialFile createFileForLargeMessage(long messageID, LargeMessageExtension extension) {
         return manager.createFileForLargeMessage(messageID, extension);
      }

      @Override
      public void prepare(long txID, Xid xid) throws Exception {
         manager.prepare(txID, xid);
      }

      @Override
      public void commit(long txID) throws Exception {
         manager.commit(txID);
      }

      @Override
      public void commit(long txID, boolean lineUpContext) throws Exception {
         manager.commit(txID, lineUpContext);
      }

      @Override
      public void rollback(long txID) throws Exception {
         manager.rollback(txID);
      }

      @Override
      public void rollbackBindings(long txID) throws Exception {
         manager.rollbackBindings(txID);
      }

      @Override
      public void commitBindings(long txID) throws Exception {
         manager.commitBindings(txID);
      }

      @Override
      public void storePageTransaction(long txID, PageTransactionInfo pageTransaction) throws Exception {
         manager.storePageTransaction(txID, pageTransaction);
      }

      @Override
      public void updatePageTransaction(long txID, PageTransactionInfo pageTransaction, int depage) throws Exception {
         manager.updatePageTransaction(txID, pageTransaction, depage);
      }

      @Override
      public void deletePageTransactional(long recordID) throws Exception {
         manager.deletePageTransactional(recordID);
      }

      @Override
      public JournalLoadInformation loadMessageJournal(PostOffice postOffice,
                                                       PagingManager pagingManager,
                                                       ResourceManager resourceManager,
                                                       Map<Long, QueueBindingInfo> queueInfos,
                                                       Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                                       Set<Pair<Long, Long>> pendingLargeMessages,
                                                       List<PageCountPending> pendingNonTXPageCounter,
                                                       JournalLoader journalLoader) throws Exception {
         return manager.loadMessageJournal(postOffice, pagingManager, resourceManager, queueInfos, duplicateIDMap, pendingLargeMessages, pendingNonTXPageCounter, journalLoader);
      }

      @Override
      public long storeHeuristicCompletion(Xid xid, boolean isCommit) throws Exception {
         return manager.storeHeuristicCompletion(xid, isCommit);
      }

      @Override
      public void deleteHeuristicCompletion(long id) throws Exception {
         manager.deleteHeuristicCompletion(id);
      }

      @Override
      public void addQueueBinding(long tx, Binding binding) throws Exception {
         manager.addQueueBinding(tx, binding);
      }

      @Override
      public void deleteQueueBinding(long tx, long queueBindingID) throws Exception {
         manager.deleteQueueBinding(tx, queueBindingID);
      }

      @Override
      public long storeQueueStatus(long queueID, QueueStatus status) throws Exception {
         return manager.storeQueueStatus(queueID, status);
      }

      @Override
      public void deleteQueueStatus(long recordID) throws Exception {
         manager.deleteQueueStatus(recordID);
      }

      @Override
      public void addAddressBinding(long tx, AddressInfo addressInfo) throws Exception {
         manager.addAddressBinding(tx, addressInfo);
      }

      @Override
      public void deleteAddressBinding(long tx, long addressBindingID) throws Exception {
         manager.deleteAddressBinding(tx, addressBindingID);
      }

      @Override
      public JournalLoadInformation loadBindingJournal(List<QueueBindingInfo> queueBindingInfos,
                                                       List<GroupingInfo> groupingInfos,
                                                       List<AddressBindingInfo> addressBindingInfos) throws Exception {
         return manager.loadBindingJournal(queueBindingInfos, groupingInfos, addressBindingInfos);
      }

      @Override
      public void addGrouping(GroupBinding groupBinding) throws Exception {
         manager.addGrouping(groupBinding);
      }

      @Override
      public void deleteGrouping(long tx, GroupBinding groupBinding) throws Exception {
         manager.deleteGrouping(tx, groupBinding);
      }

      @Override
      public void storeAddressSetting(PersistedAddressSetting addressSetting) throws Exception {
         manager.storeAddressSetting(addressSetting);
      }

      @Override
      public void deleteAddressSetting(SimpleString addressMatch) throws Exception {
         manager.deleteAddressSetting(addressMatch);
      }

      @Override
      public List<PersistedAddressSetting> recoverAddressSettings() throws Exception {
         return manager.recoverAddressSettings();
      }

      @Override
      public void storeSecurityRoles(PersistedRoles persistedRoles) throws Exception {
         manager.storeSecurityRoles(persistedRoles);
      }

      @Override
      public void deleteSecurityRoles(SimpleString addressMatch) throws Exception {
         manager.deleteSecurityRoles(addressMatch);
      }

      @Override
      public List<PersistedRoles> recoverPersistedRoles() throws Exception {
         return manager.recoverPersistedRoles();
      }

      @Override
      public long storePageCounter(long txID, long queueID, long value, long size) throws Exception {
         return manager.storePageCounter(txID, queueID, value, size);
      }

      @Override
      public long storePendingCounter(long queueID, long pageID) throws Exception {
         return manager.storePendingCounter(queueID, pageID);
      }

      @Override
      public void deleteIncrementRecord(long txID, long recordID) throws Exception {
         manager.deleteIncrementRecord(txID, recordID);
      }

      @Override
      public void deletePageCounter(long txID, long recordID) throws Exception {
         manager.deletePageCounter(txID, recordID);
      }

      @Override
      public void deletePendingPageCounter(long txID, long recordID) throws Exception {
         manager.deletePendingPageCounter(txID, recordID);
      }

      @Override
      public long storePageCounterInc(long txID, long queueID, int add, long size) throws Exception {
         return manager.storePageCounterInc(txID, queueID, add, size);
      }

      @Override
      public long storePageCounterInc(long queueID, int add, long size) throws Exception {
         return manager.storePageCounterInc(queueID, add, size);
      }

      @Override
      public Journal getBindingsJournal() {
         return manager.getBindingsJournal();
      }

      @Override
      public Journal getMessageJournal() {
         return manager.getMessageJournal();
      }

      @Override
      public void startReplication(ReplicationManager replicationManager,
                                   PagingManager pagingManager,
                                   String nodeID,
                                   boolean autoFailBack,
                                   long initialReplicationSyncTimeout) throws Exception {
         manager.startReplication(replicationManager, pagingManager, nodeID, autoFailBack, initialReplicationSyncTimeout);
      }

      @Override
      public boolean addToPage(PagingStore store,
                               Message msg,
                               Transaction tx,
                               RouteContextList listCtx) throws Exception {
         return manager.addToPage(store, msg, tx, listCtx);
      }

      @Override
      public void stopReplication() {
         manager.stopReplication();
      }

      @Override
      public void addBytesToLargeMessage(SequentialFile appendFile, long messageID, byte[] bytes) throws Exception {
         manager.addBytesToLargeMessage(appendFile, messageID, bytes);
      }

      @Override
      public void storeID(long journalID, long id) throws Exception {
         manager.storeID(journalID, id);
      }

      @Override
      public void deleteID(long journalD) throws Exception {
         manager.deleteID(journalD);
      }

      @Override
      public void readLock() {
         manager.readLock();
      }

      @Override
      public void readUnLock() {
         manager.readUnLock();
      }

      @Override
      public void persistIdGenerator() {
         manager.persistIdGenerator();
      }

      @Override
      public void injectMonitor(FileStoreMonitor monitor) throws Exception {
         manager.injectMonitor(monitor);
      }

      private final StorageManager manager;

      StorageManagerDelegate(StorageManager manager) {
         this.manager = manager;
      }
   }

}