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
package org.apache.activemq.artemis.core.persistence;

import javax.transaction.xa.Xid;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.persistence.config.PersistedAddressSetting;
import org.apache.activemq.artemis.core.persistence.config.PersistedRoles;
import org.apache.activemq.artemis.core.persistence.impl.PageCountPending;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.server.group.impl.GroupBinding;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.JournalLoader;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.IDGenerator;

/**
 * A StorageManager
 *
 * Note about IDGEnerator
 *
 * I've changed StorageManager to extend IDGenerator, because in some places
 * all we needed from the StorageManager was the idGeneration.
 * I couldn't just get the IDGenerator from the inner part because the NullPersistent has its own sequence.
 * So the best was to add the interface and adjust the callers for the method
 */
public interface StorageManager extends IDGenerator, ActiveMQComponent {

   void criticalError(Throwable error);

   /**
    * Get the context associated with the thread for later reuse
    */
   OperationContext getContext();

   void lineUpContext();

   /**
    * It just creates an OperationContext without associating it
    */
   OperationContext newContext(Executor executor);

   OperationContext newSingleThreadContext();

   /**
    * Set the context back to the thread
    */
   void setContext(OperationContext context);

   /**
    *
    * @param ioCriticalError is the server being stopped due to an IO critical error.
    * @param sendFailover this is to send the replication stopping in case of replication.
    * @throws Exception
    */
   void stop(boolean ioCriticalError, boolean sendFailover) throws Exception;

   // Message related operations

   void pageClosed(SimpleString storeName, int pageNumber);

   void pageDeleted(SimpleString storeName, int pageNumber);

   void pageWrite(PagedMessage message, int pageNumber);

   void afterCompleteOperations(IOCallback run);

   /**
    * This is similar to afterComplete, however this only cares about the journal part.
    */
   void afterStoreOperations(IOCallback run);

   /**
    * Block until the operations are done.
    * Warning: Don't use it inside an ordered executor, otherwise the system may lock up
    * in case of the pools are full
    *
    * @throws Exception
    */
   boolean waitOnOperations(long timeout) throws Exception;

   /**
    * Block until the operations are done.
    * Warning: Don't use it inside an ordered executor, otherwise the system may lock up
    * in case of the pools are full
    *
    * @throws Exception
    */
   void waitOnOperations() throws Exception;

   /**
    * We need a safeguard in place to avoid too much concurrent IO happening on Paging, otherwise
    * the system may become unresponsive if too many destinations are reading all the same time.
    * This is called before we read, so we can limit concurrent reads
    *
    * @throws Exception
    */
   void beforePageRead() throws Exception;

   /**
    * We need a safeguard in place to avoid too much concurrent IO happening on Paging, otherwise
    * the system may become unresponsive if too many destinations are reading all the same time.
    * This is called after we read, so we can limit concurrent reads
    *
    * @throws Exception
    */
   void afterPageRead() throws Exception;

   /**
    * AIO has an optimized buffer which has a method to release it
    * instead of the way NIO will release data based on GC.
    * These methods will use that buffer if the inner method supports it
    */
   ByteBuffer allocateDirectBuffer(int size);

   /**
    * AIO has an optimized buffer which has a method to release it
    * instead of the way NIO will release data based on GC.
    * These methods will use that buffer if the inner method supports it
    */
   void freeDirectBuffer(ByteBuffer buffer);

   void clearContext();

   /**
    * Confirms that a large message was finished
    */
   void confirmPendingLargeMessageTX(Transaction transaction, long messageID, long recordID) throws Exception;

   /**
    * Confirms that a large message was finished
    */
   void confirmPendingLargeMessage(long recordID) throws Exception;

   void storeMessage(Message message) throws Exception;

   void storeReference(long queueID, long messageID, boolean last) throws Exception;

   void deleteMessage(long messageID) throws Exception;

   void storeAcknowledge(long queueID, long messageID) throws Exception;

   void storeCursorAcknowledge(long queueID, PagePosition position) throws Exception;

   void updateDeliveryCount(MessageReference ref) throws Exception;

   void updateScheduledDeliveryTime(MessageReference ref) throws Exception;

   void storeDuplicateID(SimpleString address, byte[] duplID, long recordID) throws Exception;

   void deleteDuplicateID(long recordID) throws Exception;

   void storeMessageTransactional(long txID, Message message) throws Exception;

   void storeReferenceTransactional(long txID, long queueID, long messageID) throws Exception;

   void storeAcknowledgeTransactional(long txID, long queueID, long messageID) throws Exception;

   void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position) throws Exception;

   void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception;

   void deleteCursorAcknowledge(long ackID) throws Exception;

   void storePageCompleteTransactional(long txID, long queueID, PagePosition position) throws Exception;

   void deletePageComplete(long ackID) throws Exception;

   void updateScheduledDeliveryTimeTransactional(long txID, MessageReference ref) throws Exception;

   void storeDuplicateIDTransactional(long txID, SimpleString address, byte[] duplID, long recordID) throws Exception;

   void updateDuplicateIDTransactional(long txID, SimpleString address, byte[] duplID, long recordID) throws Exception;

   void deleteDuplicateIDTransactional(long txID, long recordID) throws Exception;

   LargeServerMessage createLargeMessage();

   /**
    * Creates a new LargeMessage with the given id.
    *
    * @param id
    * @param message This is a temporary message that holds the parsed properties. The remoting
    *                layer can't create a ServerMessage directly, then this will be replaced.
    * @return a large message object
    * @throws Exception
    */
   LargeServerMessage createLargeMessage(long id, Message message) throws Exception;

   enum LargeMessageExtension {
      DURABLE(".msg"), TEMPORARY(".tmp"), SYNC(".sync");
      final String extension;

      LargeMessageExtension(String extension) {
         this.extension = extension;
      }

      public String getExtension() {
         return extension;
      }
   }

   /**
    * Instantiates a SequentialFile to be used for storing a {@link LargeServerMessage}.
    *
    * @param messageID the id of the message
    * @param extension the extension to add to the file
    * @return
    */
   SequentialFile createFileForLargeMessage(final long messageID, LargeMessageExtension extension);

   void prepare(long txID, Xid xid) throws Exception;

   void commit(long txID) throws Exception;

   void commit(long txID, boolean lineUpContext) throws Exception;

   void rollback(long txID) throws Exception;

   void rollbackBindings(long txID) throws Exception;

   void commitBindings(long txID) throws Exception;

   void storePageTransaction(long txID, PageTransactionInfo pageTransaction) throws Exception;

   void updatePageTransaction(long txID, PageTransactionInfo pageTransaction, int depage) throws Exception;

   void deletePageTransactional(long recordID) throws Exception;

   JournalLoadInformation loadMessageJournal(final PostOffice postOffice,
                                             final PagingManager pagingManager,
                                             final ResourceManager resourceManager,
                                             Map<Long, QueueBindingInfo> queueInfos,
                                             final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                             final Set<Pair<Long, Long>> pendingLargeMessages,
                                             List<PageCountPending> pendingNonTXPageCounter,
                                             final JournalLoader journalLoader) throws Exception;

   long storeHeuristicCompletion(Xid xid, boolean isCommit) throws Exception;

   void deleteHeuristicCompletion(long id) throws Exception;

   // BindingsImpl related operations

   void addQueueBinding(long tx, Binding binding) throws Exception;

   void deleteQueueBinding(long tx, long queueBindingID) throws Exception;

   /**
    *
    * @param queueID The id of the queue
    * @param status The current status of the queue. (Reserved for future use, ATM we only use this record for PAUSED)
    * @return the id of the journal
    * @throws Exception
    */
   long storeQueueStatus(long queueID, QueueStatus status) throws Exception;

   void deleteQueueStatus(long recordID) throws Exception;

   void addAddressBinding(long tx, AddressInfo addressInfo) throws Exception;

   void deleteAddressBinding(long tx, long addressBindingID) throws Exception;

   JournalLoadInformation loadBindingJournal(List<QueueBindingInfo> queueBindingInfos,
                                             List<GroupingInfo> groupingInfos,
                                             List<AddressBindingInfo> addressBindingInfos) throws Exception;

   // grouping related operations
   void addGrouping(GroupBinding groupBinding) throws Exception;

   void deleteGrouping(long tx, GroupBinding groupBinding) throws Exception;

   void storeAddressSetting(PersistedAddressSetting addressSetting) throws Exception;

   void deleteAddressSetting(SimpleString addressMatch) throws Exception;

   List<PersistedAddressSetting> recoverAddressSettings() throws Exception;

   void storeSecurityRoles(PersistedRoles persistedRoles) throws Exception;

   void deleteSecurityRoles(SimpleString addressMatch) throws Exception;

   List<PersistedRoles> recoverPersistedRoles() throws Exception;

   /**
    * @return The ID with the stored counter
    */
   long storePageCounter(long txID, long queueID, long value) throws Exception;

   long storePendingCounter(long queueID, long pageID, int inc) throws Exception;

   void deleteIncrementRecord(long txID, long recordID) throws Exception;

   void deletePageCounter(long txID, long recordID) throws Exception;

   void deletePendingPageCounter(long txID, long recordID) throws Exception;

   /**
    * @return the ID with the increment record
    * @throws Exception
    */
   long storePageCounterInc(long txID, long queueID, int add) throws Exception;

   /**
    * @return the ID with the increment record
    * @throws Exception
    */
   long storePageCounterInc(long queueID, int add) throws Exception;

   /**
    * @return the bindings journal
    */
   Journal getBindingsJournal();

   /**
    * @return the message journal
    */
   Journal getMessageJournal();

   /**
    * @see org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager#startReplication(org.apache.activemq.artemis.core.replication.ReplicationManager, org.apache.activemq.artemis.core.paging.PagingManager, String, boolean, long)
    */
   void startReplication(ReplicationManager replicationManager,
                         PagingManager pagingManager,
                         String nodeID,
                         boolean autoFailBack,
                         long initialReplicationSyncTimeout) throws Exception;

   /**
    * Write message to page if we are paging.
    * <p>
    * This is primarily a {@link PagingStore} call, but as with any other call writing persistent
    * data, it must go through here. Both for the sake of replication, and also to ensure that it
    * takes the locks (storage manager and pagingStore) in the right order. Avoiding thus the
    * creation of dead-locks.
    *
    * @return {@code true} if we are paging and have handled the data, {@code false} if the data
    * needs to be sent to the journal
    * @throws Exception
    */
   boolean addToPage(PagingStore store, Message msg, Transaction tx, RouteContextList listCtx) throws Exception;

   /**
    * Stops the replication of data from the live to the backup.
    * <p>
    * Typical scenario is a broken connection.
    */
   void stopReplication();

   /**
    * @param appendFile
    * @param messageID
    * @param bytes
    */
   void addBytesToLargeMessage(SequentialFile appendFile, long messageID, byte[] bytes) throws Exception;

   /**
    * Stores the id from IDManager.
    *
    * @param journalID
    * @param id
    * @throws Exception
    */
   void storeID(long journalID, long id) throws Exception;

   /*
       Deletes the ID from IDManager.
    */
   void deleteID(long journalD) throws Exception;

   /**
    * Read lock the StorageManager. USE WITH CARE!
    * <p>
    * The main lock is used to write lock the whole manager when starting replication. Sub-systems,
    * say Paging classes, that use locks of their own AND also write through the StorageManager MUST
    * first read lock the storageManager before taking their own locks. Otherwise, we may dead-lock
    * when starting replication sync.
    */
   void readLock();

   /**
    * Unlock the manager.
    *
    * @see StorageManager#readLock()
    */
   void readUnLock();

   /**
    * Closes the {@link IDGenerator} persisting the current record ID.
    * <p>
    * Effectively a "pre-stop" method. Necessary due to the "stop"-order at
    * {@link org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl}
    */
   void persistIdGenerator();

   void injectMonitor(FileStoreMonitor monitor) throws Exception;
}
