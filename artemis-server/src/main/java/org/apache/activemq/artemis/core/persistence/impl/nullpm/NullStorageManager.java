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
package org.apache.activemq.artemis.core.persistence.impl.nullpm;

import javax.transaction.xa.Xid;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
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
import org.apache.activemq.artemis.core.persistence.AddressQueueStatus;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.config.PersistedAddressSetting;
import org.apache.activemq.artemis.core.persistence.config.PersistedDivertConfiguration;
import org.apache.activemq.artemis.core.persistence.config.PersistedRole;
import org.apache.activemq.artemis.core.persistence.config.PersistedSecuritySetting;
import org.apache.activemq.artemis.core.persistence.config.PersistedUser;
import org.apache.activemq.artemis.core.persistence.impl.PageCountPending;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.server.group.impl.GroupBinding;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.JournalLoader;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.Transaction;

public class NullStorageManager implements StorageManager {

   private final AtomicLong idSequence = new AtomicLong(0);

   private volatile boolean started;

   private final IOCriticalErrorListener ioCriticalErrorListener;

   public NullStorageManager(IOCriticalErrorListener ioCriticalErrorListener) {
      this.ioCriticalErrorListener = ioCriticalErrorListener;
   }

   public NullStorageManager() {
      this(new IOCriticalErrorListener() {
         @Override
         public void onIOException(Throwable code, String message, SequentialFile file) {
            code.printStackTrace();
         }
      });
   }

   @Override
   public void criticalError(Throwable error) {

   }

   @Override
   public long storeQueueStatus(long queueID, AddressQueueStatus status) throws Exception {
      return 0;
   }

   @Override
   public void updateQueueBinding(long tx, Binding binding) throws Exception {
   }

   @Override
   public void deleteQueueStatus(long recordID) throws Exception {

   }

   @Override
   public long storeAddressStatus(long addressID, AddressQueueStatus status) throws Exception {
      return 0;
   }

   @Override
   public void deleteAddressStatus(long recordID) throws Exception {

   }

   @Override
   public void injectMonitor(FileStoreMonitor monitor) throws Exception {

   }

   private static final OperationContext dummyContext = new OperationContext() {

      @Override
      public void onError(final int errorCode, final String errorMessage) {
      }

      @Override
      public void done() {
      }

      @Override
      public void executeOnCompletion(IOCallback runnable, boolean storeOnly) {
         runnable.done();
      }

      @Override
      public void storeLineUp() {
      }

      @Override
      public boolean waitCompletion(final long timeout) throws Exception {
         return true;
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
      }

      @Override
      public void pageSyncDone() {
      }

      @Override
      public void executeOnCompletion(final IOCallback runnable) {
         runnable.done();
      }
   };

   @Override
   public void deleteQueueBinding(long tx, final long queueBindingID) throws Exception {
   }

   @Override
   public void addAddressBinding(long tx, AddressInfo addressInfo) throws Exception {
   }

   @Override
   public void deleteAddressBinding(long tx, long addressBindingID) throws Exception {
   }

   @Override
   public void commit(final long txID) throws Exception {
   }

   @Override
   public JournalLoadInformation loadBindingJournal(final List<QueueBindingInfo> queueBindingInfos,
                                                    final List<GroupingInfo> groupingInfos,
                                                    final List<AddressBindingInfo> addressBindingInfos) throws Exception {
      return new JournalLoadInformation();
   }

   @Override
   public void prepare(final long txID, final Xid xid) throws Exception {
   }

   @Override
   public void rollback(final long txID) throws Exception {
   }

   @Override
   public void rollbackBindings(final long txID) throws Exception {
   }

   @Override
   public void commitBindings(final long txID) throws Exception {
   }

   @Override
   public void storeReference(final long queueID, final long messageID, final boolean last) throws Exception {
   }

   @Override
   public void storeReferenceTransactional(final long txID, final long queueID, final long messageID) throws Exception {
   }

   @Override
   public void storeAcknowledge(final long queueID, final long messageID) throws Exception {
   }

   @Override
   public void storeAcknowledgeTransactional(final long txID,
                                             final long queueID,
                                             final long messageiD) throws Exception {
   }

   @Override
   public boolean deleteMessage(final long messageID) throws Exception {
      return true;
   }

   @Override
   public void storeMessage(final Message message) throws Exception {
   }

   @Override
   public void storeMessageTransactional(final long txID, final Message message) throws Exception {
   }

   @Override
   public boolean updateScheduledDeliveryTime(final MessageReference ref) throws Exception {
      return true;
   }

   @Override
   public void updateScheduledDeliveryTimeTransactional(final long txID, final MessageReference ref) throws Exception {
   }

   @Override
   public void storePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception {
   }

   @Override
   public boolean updateDeliveryCount(final MessageReference ref) throws Exception {
      return true;
   }

   @Override
   public void storeDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception {
   }

   @Override
   public void storeDuplicateIDTransactional(final long txID,
                                             final SimpleString address,
                                             final byte[] duplID,
                                             final long recordID) throws Exception {
   }

   @Override
   public void updateDuplicateIDTransactional(final long txID,
                                              final SimpleString address,
                                              final byte[] duplID,
                                              final long recordID) throws Exception {
   }

   @Override
   public long storeHeuristicCompletion(final Xid xid, final boolean isCommit) throws Exception {
      return generateID();
   }

   @Override
   public void deleteHeuristicCompletion(final long txID) throws Exception {
   }

   @Override
   public void addQueueBinding(final long tx, final Binding binding) throws Exception {
   }

   @Override
   public LargeServerMessage createLargeMessage() {
      return new NullStorageLargeServerMessage();
   }

   @Override
   public LargeServerMessage createLargeMessage(final long id, final Message message) {
      NullStorageLargeServerMessage largeMessage = new NullStorageLargeServerMessage();

      largeMessage.moveHeadersAndProperties(message);

      largeMessage.setMessageID(id);

      return largeMessage;
   }

   @Override
   public LargeServerMessage largeMessageCreated(long id, LargeServerMessage largeMessage) throws Exception {
      return null;
   }

   @Override
   public long generateID() {
      long id = idSequence.getAndIncrement();

      return id;
   }

   @Override
   public long getCurrentID() {
      return idSequence.get();
   }

   @Override
   public synchronized void start() throws Exception {
      if (started) {
         throw new IllegalStateException("Already started");
      }

      started = true;
   }

   @Override
   public synchronized void stop() throws Exception {
      if (!started) {
         throw new IllegalStateException("Not started");
      }

      idSequence.set(0);

      started = false;
   }

   @Override
   public synchronized boolean isStarted() {
      return started;
   }

   @Override
   public JournalLoadInformation loadMessageJournal(final PostOffice postOffice,
                                                    final PagingManager pagingManager,
                                                    final ResourceManager resourceManager,
                                                    final Map<Long, QueueBindingInfo> queueInfos,
                                                    final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                                    final Set<Pair<Long, Long>> pendingLargeMessages,
                                                    List<PageCountPending> pendingNonTXPageCounter,
                                                    final JournalLoader journalLoader) throws Exception {
      return new JournalLoadInformation();
   }

   @Override
   public void deleteDuplicateIDTransactional(final long txID, final long recordID) throws Exception {
   }

   @Override
   public void deleteDuplicateID(final long recordID) throws Exception {
   }

   @Override
   public void pageClosed(final SimpleString storeName, final int pageNumber) {
   }

   @Override
   public void pageDeleted(final SimpleString storeName, final int pageNumber) {
   }

   @Override
   public void pageWrite(final PagedMessage message, final int pageNumber) {
   }

   @Override
   public void addGrouping(final GroupBinding groupBinding) throws Exception {
   }

   @Override
   public void deleteGrouping(final long tx, final GroupBinding groupBinding) throws Exception {
   }

   @Override
   public boolean waitOnOperations(final long timeout) throws Exception {
      return true;
   }

   @Override
   public void afterCompleteOperations(final IOCallback run) {
      run.done();
   }

   @Override
   public void afterStoreOperations(IOCallback run) {
      run.done();
   }

   @Override
   public void waitOnOperations() throws Exception {
   }

   @Override
   public OperationContext getContext() {
      return NullStorageManager.dummyContext;
   }

   @Override
   public OperationContext newContext(final Executor executor) {
      return NullStorageManager.dummyContext;
   }

   @Override
   public OperationContext newSingleThreadContext() {
      return NullStorageManager.dummyContext;
   }

   @Override
   public void setContext(final OperationContext context) {
   }

   @Override
   public void clearContext() {
   }

   @Override
   public List<PersistedAddressSetting> recoverAddressSettings() throws Exception {
      return Collections.emptyList();
   }

   @Override
   public void storeAddressSetting(final PersistedAddressSetting addressSetting) throws Exception {
   }

   @Override
   public List<PersistedSecuritySetting> recoverSecuritySettings() throws Exception {
      return Collections.emptyList();
   }

   @Override
   public void storeDivertConfiguration(PersistedDivertConfiguration persistedDivertConfiguration) throws Exception {
   }

   @Override
   public void deleteDivertConfiguration(String divertName) throws Exception {
   }

   @Override
   public List<PersistedDivertConfiguration> recoverDivertConfigurations() {
      return null;
   }

   @Override
   public void storeUser(PersistedUser persistedUser) throws Exception {
   }

   @Override
   public void deleteUser(String username) throws Exception {
   }

   @Override
   public Map<String, PersistedUser> getPersistedUsers() {
      return null;
   }

   @Override
   public void storeRole(PersistedRole persistedRole) throws Exception {
   }

   @Override
   public void deleteRole(String role) throws Exception {
   }

   @Override
   public Map<String, PersistedRole> getPersistedRoles() {
      return null;
   }

   @Override
   public void storeSecuritySetting(final PersistedSecuritySetting persistedRoles) throws Exception {
   }

   @Override
   public void deleteAddressSetting(final SimpleString addressMatch) throws Exception {
   }

   @Override
   public void deleteSecuritySetting(final SimpleString addressMatch) throws Exception {
   }

   @Override
   public void deletePageTransactional(final long recordID) throws Exception {
   }

   @Override
   public void updatePageTransaction(final long txID,
                                     final PageTransactionInfo pageTransaction,
                                     final int depage) throws Exception {
   }

   @Override
   public void storeCursorAcknowledge(final long queueID, final PagePosition position) {
   }

   @Override
   public void storeCursorAcknowledgeTransactional(final long txID, final long queueID, final PagePosition position) {
   }

   @Override
   public void deleteCursorAcknowledgeTransactional(final long txID, final long ackID) throws Exception {
   }

   @Override
   public void deleteCursorAcknowledge(long ackID) throws Exception {
   }

   @Override
   public void storePageCompleteTransactional(long txID, long queueID, PagePosition position) throws Exception {
   }

   @Override
   public void deletePageComplete(long ackID) throws Exception {
   }

   @Override
   public long storePageCounter(final long txID, final long queueID, final long value, final long size) throws Exception {
      return 0;
   }

   @Override
   public long storePendingCounter(long queueID, long pageID) throws Exception {
      return -1;
   }

   @Override
   public void deleteIncrementRecord(final long txID, final long recordID) throws Exception {
   }

   @Override
   public void deletePageCounter(final long txID, final long recordID) throws Exception {
   }

   @Override
   public void deletePendingPageCounter(long txID, long recordID) throws Exception {
   }

   @Override
   public long storePageCounterInc(final long txID, final long queueID, final int add, final long size) throws Exception {
      return 0;
   }

   @Override
   public long storePageCounterInc(final long queueID, final int add, final long size) throws Exception {
      return 0;
   }

   @Override
   public void commit(final long txID, final boolean lineUpContext) throws Exception {
   }

   @Override
   public void lineUpContext() {
   }

   @Override
   public void confirmPendingLargeMessageTX(final Transaction transaction,
                                            final long messageID,
                                            final long recordID) throws Exception {
   }

   @Override
   public void confirmPendingLargeMessage(final long recordID) throws Exception {
   }

   @Override
   public void stop(final boolean ioCriticalError, boolean sendFailover) throws Exception {
   }

   @Override
   public Journal getBindingsJournal() {
      return null;
   }

   @Override
   public Journal getMessageJournal() {
      return null;
   }

   @Override
   public void startReplication(final ReplicationManager replicationManager,
                                final PagingManager pagingManager,
                                final String nodeID,
                                final boolean autoFailBack,
                                long initialReplicationSyncTimeout) throws Exception {
      // no-op
   }

   @Override
   public void deleteLargeMessageBody(LargeServerMessage largeServerMessage) throws ActiveMQException {

   }

   @Override
   public void largeMessageClosed(LargeServerMessage largeServerMessage) throws ActiveMQException {

   }

   @Override
   public boolean addToPage(PagingStore store,
                            Message msg,
                            Transaction tx,
                            RouteContextList listCtx) throws Exception {
      /**
       * Exposing the read-lock here is an encapsulation violation done in order to keep the code
       * simpler. The alternative would be to add a second method, say 'verifyPaging', to
       * PagingStore.
       * <p>
       * Adding this second method would also be more surprise prone as it would require a certain
       * calling order.
       * <p>
       * The reasoning is that exposing the lock is more explicit and therefore `less bad`.
       */
      if (store != null) {
         return store.page(msg, tx, listCtx, null);
      } else {
         return false;
      }
   }

   @Override
   public void stopReplication() {
      // no-op
   }

   @Override
   public SequentialFile createFileForLargeMessage(final long messageID, final LargeMessageExtension extension) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void addBytesToLargeMessage(SequentialFile appendFile, long messageID, byte[] bytes) throws Exception {
      // no-op
   }

   @Override
   public void addBytesToLargeMessage(SequentialFile file, long messageId, ActiveMQBuffer bytes) throws Exception {

   }

   @Override
   public void beforePageRead() throws Exception {
   }

   @Override
   public boolean beforePageRead(long timeout, TimeUnit unit) throws InterruptedException {
      return true;
   }

   @Override
   public void afterPageRead() throws Exception {
   }

   @Override
   public ByteBuffer allocateDirectBuffer(final int size) {
      return ByteBuffer.allocateDirect(size);
   }

   @Override
   public void freeDirectBuffer(final ByteBuffer buffer) {
      // We can just have hope on GC here :-)
   }

   @Override
   public void storeID(final long journalID, final long id) throws Exception {
      // no-op
   }

   @Override
   public void readLock() {
      // no-op
   }

   @Override
   public void readUnLock() {
      // no-op
   }

   @Override
   public void persistIdGenerator() {
      // no-op
   }

   @Override
   public void deleteID(long journalD) throws Exception {

   }
}
