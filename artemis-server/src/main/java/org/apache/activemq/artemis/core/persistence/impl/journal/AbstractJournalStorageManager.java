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
package org.apache.activemq.artemis.core.persistence.impl.journal;

import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ACKNOWLEDGE_CURSOR;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ADD_LARGE_MESSAGE_PENDING;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.DUPLICATE_ID;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.PAGE_CURSOR_COUNTER_INC;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME;

import java.io.File;
import java.io.FileInputStream;
import java.security.DigestInputStream;
import java.security.InvalidParameterException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.PagedReferenceImpl;
import org.apache.activemq.artemis.core.paging.impl.PageTransactionInfoImpl;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.QueueStatus;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.config.PersistedAddressSetting;
import org.apache.activemq.artemis.core.persistence.config.PersistedRoles;
import org.apache.activemq.artemis.core.persistence.impl.PageCountPending;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.CursorAckRecordEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.DeleteEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.DeliveryCountUpdateEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.DuplicateIDEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.FinishPageMessageOperation;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.GroupingEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.HeuristicCompletionEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.LargeMessagePersister;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PageCountPendingImpl;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PageCountRecord;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PageCountRecordInc;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PageUpdateTXEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PendingLargeMessageEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PersistentAddressBindingEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PersistentQueueBindingEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.QueueStatusEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.RefEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.ScheduledDeliveryEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.XidEncoding;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.group.impl.GroupBinding;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.JournalLoader;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.apache.activemq.artemis.utils.critical.CriticalComponentImpl;
import org.jboss.logging.Logger;

/**
 * Controls access to the journals and other storage files such as the ones used to store pages and
 * large messages.  This class must control writing of any non-transient data, as it is the key point
 * for synchronizing any replicating backup server.
 * <p>
 * Using this class also ensures that locks are acquired in the right order, avoiding dead-locks.
 */
public abstract class AbstractJournalStorageManager extends CriticalComponentImpl implements StorageManager {

   private static final int CRITICAL_PATHS = 1;
   private static final int CRITICAL_STORE = 0;

   private static final Logger logger = Logger.getLogger(AbstractJournalStorageManager.class);

   public enum JournalContent {
      BINDINGS((byte) 0), MESSAGES((byte) 1);

      public final byte typeByte;

      JournalContent(byte b) {
         typeByte = b;
      }

      public static JournalContent getType(byte type) {
         if (MESSAGES.typeByte == type)
            return MESSAGES;
         if (BINDINGS.typeByte == type)
            return BINDINGS;
         throw new InvalidParameterException("invalid byte: " + type);
      }
   }

   private static final long CHECKPOINT_BATCH_SIZE = Integer.MAX_VALUE;

   protected Semaphore pageMaxConcurrentIO;

   protected BatchingIDGenerator idGenerator;

   protected final ExecutorFactory ioExecutorFactory;

   protected final ScheduledExecutorService scheduledExecutorService;

   protected final ReentrantReadWriteLock storageManagerLock = new ReentrantReadWriteLock(true);

   protected Journal messageJournal;

   protected Journal bindingsJournal;

   protected volatile boolean started;

   /**
    * Used to create Operation Contexts
    */
   protected final ExecutorFactory executorFactory;

   final Executor executor;

   Executor singleThreadExecutor;

   private final boolean syncTransactional;

   private final boolean syncNonTransactional;

   protected boolean journalLoaded = false;

   private final IOCriticalErrorListener ioCriticalErrorListener;

   protected final Configuration config;

   // Persisted core configuration
   protected final Map<SimpleString, PersistedRoles> mapPersistedRoles = new ConcurrentHashMap<>();

   protected final Map<SimpleString, PersistedAddressSetting> mapPersistedAddressSettings = new ConcurrentHashMap<>();

   protected final Set<Long> largeMessagesToDelete = new HashSet<>();

   public AbstractJournalStorageManager(final Configuration config,
                                        final CriticalAnalyzer analyzer,
                                        final ExecutorFactory executorFactory,
                                        final ScheduledExecutorService scheduledExecutorService,
                                        final ExecutorFactory ioExecutorFactory) {
      this(config, analyzer, executorFactory, scheduledExecutorService, ioExecutorFactory, null);
   }

   public AbstractJournalStorageManager(Configuration config,
                                        CriticalAnalyzer analyzer,
                                        ExecutorFactory executorFactory,
                                        ScheduledExecutorService scheduledExecutorService,
                                        ExecutorFactory ioExecutorFactory,
                                        IOCriticalErrorListener criticalErrorListener) {
      super(analyzer, CRITICAL_PATHS);

      this.executorFactory = executorFactory;

      this.ioCriticalErrorListener = criticalErrorListener;

      this.ioExecutorFactory = ioExecutorFactory;

      this.scheduledExecutorService = scheduledExecutorService;

      this.config = config;

      executor = executorFactory.getExecutor();

      syncNonTransactional = config.isJournalSyncNonTransactional();
      syncTransactional = config.isJournalSyncTransactional();

      init(config, criticalErrorListener);

      idGenerator = new BatchingIDGenerator(0, CHECKPOINT_BATCH_SIZE, this);
   }


   @Override
   public long getMaxRecordSize() {
      return messageJournal.getMaxRecordSize();
   }


   /**
    * Called during initialization.  Used by implementations to setup Journals, Stores etc...
    *
    * @param config
    * @param criticalErrorListener
    */
   protected abstract void init(Configuration config, IOCriticalErrorListener criticalErrorListener);

   @Override
   public void criticalError(Throwable error) {
      ioCriticalErrorListener.onIOException(error, error.getMessage(), null);
   }

   @Override
   public void clearContext() {
      OperationContextImpl.clearContext();
   }

   public static String md5(File file) {
      try {
         byte[] buffer = new byte[1 << 4];
         MessageDigest md = MessageDigest.getInstance("MD5");

         byte[] digest;
         try (FileInputStream is = new FileInputStream(file);
              DigestInputStream is2 = new DigestInputStream(is, md)) {
            while (is2.read(buffer) > 0) {
               continue;
            }
            digest = md.digest();
         }
         return Base64.encodeBytes(digest);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public IDGenerator getIDGenerator() {
      return idGenerator;
   }

   @Override
   public final void waitOnOperations() throws Exception {
      if (!started) {
         ActiveMQServerLogger.LOGGER.serverIsStopped();
         throw new IllegalStateException("Server is stopped");
      }
      waitOnOperations(0);
   }

   @Override
   public final boolean waitOnOperations(final long timeout) throws Exception {
      if (!started) {
         ActiveMQServerLogger.LOGGER.serverIsStopped();
         throw new IllegalStateException("Server is stopped");
      }
      return getContext().waitCompletion(timeout);
   }

   @Override
   public OperationContext getContext() {
      return OperationContextImpl.getContext(executorFactory);
   }

   @Override
   public void setContext(final OperationContext context) {
      OperationContextImpl.setContext(context);
   }

   @Override
   public OperationContext newSingleThreadContext() {
      return newContext(singleThreadExecutor);
   }

   @Override
   public OperationContext newContext(final Executor executor1) {
      return new OperationContextImpl(executor1);
   }

   @Override
   public void afterCompleteOperations(final IOCallback run) {
      getContext().executeOnCompletion(run);
   }

   @Override
   public void afterStoreOperations(IOCallback run) {
      getContext().executeOnCompletion(run, true);
   }

   @Override
   public long generateID() {
      return idGenerator.generateID();
   }

   @Override
   public long getCurrentID() {
      return idGenerator.getCurrentID();
   }

   // Non transactional operations

   @Override
   public void confirmPendingLargeMessageTX(final Transaction tx, long messageID, long recordID) throws Exception {
      readLock();
      try {
         installLargeMessageConfirmationOnTX(tx, recordID);
         messageJournal.appendDeleteRecordTransactional(tx.getID(), recordID, new DeleteEncoding(JournalRecordIds.ADD_LARGE_MESSAGE_PENDING, messageID));
      } finally {
         readUnLock();
      }
   }

   /**
    * We don't need messageID now but we are likely to need it we ever decide to support a database
    */
   @Override
   public void confirmPendingLargeMessage(long recordID) throws Exception {
      readLock();
      try {
         messageJournal.appendDeleteRecord(recordID, true, getContext());
      } finally {
         readUnLock();
      }
   }

   @Override
   public void storeMessage(final Message message) throws Exception {
      if (message.getMessageID() <= 0) {
         // Sanity check only... this shouldn't happen unless there is a bug
         throw ActiveMQMessageBundle.BUNDLE.messageIdNotAssigned();
      }

      readLock();
      try {
         // Note that we don't sync, the add reference that comes immediately after will sync if
         // appropriate

         if (message.isLargeMessage()) {
            messageJournal.appendAddRecord(message.getMessageID(), JournalRecordIds.ADD_LARGE_MESSAGE, LargeMessagePersister.getInstance(), message, false, getContext(false));
         } else {
            messageJournal.appendAddRecord(message.getMessageID(), JournalRecordIds.ADD_MESSAGE_PROTOCOL, message.getPersister(), message, false, getContext(false));
         }
      } finally {
         readUnLock();
      }
   }

   @Override
   public void storeReference(final long queueID, final long messageID, final boolean last) throws Exception {
      readLock();
      try {
         messageJournal.appendUpdateRecord(messageID, JournalRecordIds.ADD_REF, new RefEncoding(queueID), last && syncNonTransactional, getContext(last && syncNonTransactional));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void readLock() {
      enterCritical(CRITICAL_STORE);
      storageManagerLock.readLock().lock();
   }

   @Override
   public void readUnLock() {
      storageManagerLock.readLock().unlock();
      leaveCritical(CRITICAL_STORE);
   }

   @Override
   public void storeAcknowledge(final long queueID, final long messageID) throws Exception {
      readLock();
      try {
         messageJournal.appendUpdateRecord(messageID, JournalRecordIds.ACKNOWLEDGE_REF, new RefEncoding(queueID), syncNonTransactional, getContext(syncNonTransactional));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void storeCursorAcknowledge(long queueID, PagePosition position) throws Exception {
      readLock();
      try {
         long ackID = idGenerator.generateID();
         position.setRecordID(ackID);
         messageJournal.appendAddRecord(ackID, JournalRecordIds.ACKNOWLEDGE_CURSOR, new CursorAckRecordEncoding(queueID, position), syncNonTransactional, getContext(syncNonTransactional));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deleteMessage(final long messageID) throws Exception {
      readLock();
      try {
         // Messages are deleted on postACK, one after another.
         // If these deletes are synchronized, we would build up messages on the Executor
         // increasing chances of losing deletes.
         // The StorageManager should verify messages without references
         messageJournal.appendDeleteRecord(messageID, false, getContext(false));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void updateScheduledDeliveryTime(final MessageReference ref) throws Exception {
      ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding(ref.getScheduledDeliveryTime(), ref.getQueue().getID());
      readLock();
      try {
         messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(), JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME, encoding, syncNonTransactional, getContext(syncNonTransactional));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void storeDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception {
      readLock();
      try {
         DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

         messageJournal.appendAddRecord(recordID, JournalRecordIds.DUPLICATE_ID, encoding, syncNonTransactional, getContext(syncNonTransactional));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deleteDuplicateID(final long recordID) throws Exception {
      readLock();
      try {
         messageJournal.appendDeleteRecord(recordID, syncNonTransactional, getContext(syncNonTransactional));
      } finally {
         readUnLock();
      }
   }

   // Transactional operations

   @Override
   public void storeMessageTransactional(final long txID, final Message message) throws Exception {
      if (message.getMessageID() <= 0) {
         throw ActiveMQMessageBundle.BUNDLE.messageIdNotAssigned();
      }

      readLock();
      try {
         if (message.isLargeMessage()) {
            messageJournal.appendAddRecordTransactional(txID, message.getMessageID(), JournalRecordIds.ADD_LARGE_MESSAGE, LargeMessagePersister.getInstance(), message);
         } else {
            messageJournal.appendAddRecordTransactional(txID, message.getMessageID(), JournalRecordIds.ADD_MESSAGE_PROTOCOL, message.getPersister(), message);
         }

      } finally {
         readUnLock();
      }
   }

   @Override
   public void storePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception {
      readLock();
      try {
         pageTransaction.setRecordID(generateID());
         messageJournal.appendAddRecordTransactional(txID, pageTransaction.getRecordID(), JournalRecordIds.PAGE_TRANSACTION, pageTransaction);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void updatePageTransaction(final long txID,
                                     final PageTransactionInfo pageTransaction,
                                     final int depages) throws Exception {
      readLock();
      try {
         messageJournal.appendUpdateRecordTransactional(txID, pageTransaction.getRecordID(), JournalRecordIds.PAGE_TRANSACTION, new PageUpdateTXEncoding(pageTransaction.getTransactionID(), depages));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void storeReferenceTransactional(final long txID, final long queueID, final long messageID) throws Exception {
      readLock();
      try {
         messageJournal.appendUpdateRecordTransactional(txID, messageID, JournalRecordIds.ADD_REF, new RefEncoding(queueID));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void storeAcknowledgeTransactional(final long txID,
                                             final long queueID,
                                             final long messageID) throws Exception {
      readLock();
      try {
         messageJournal.appendUpdateRecordTransactional(txID, messageID, JournalRecordIds.ACKNOWLEDGE_REF, new RefEncoding(queueID));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position) throws Exception {
      readLock();
      try {
         long ackID = idGenerator.generateID();
         position.setRecordID(ackID);
         messageJournal.appendAddRecordTransactional(txID, ackID, JournalRecordIds.ACKNOWLEDGE_CURSOR, new CursorAckRecordEncoding(queueID, position));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void storePageCompleteTransactional(long txID, long queueID, PagePosition position) throws Exception {
      long recordID = idGenerator.generateID();
      position.setRecordID(recordID);
      messageJournal.appendAddRecordTransactional(txID, recordID, JournalRecordIds.PAGE_CURSOR_COMPLETE, new CursorAckRecordEncoding(queueID, position));
   }

   @Override
   public void deletePageComplete(long ackID) throws Exception {
      messageJournal.appendDeleteRecord(ackID, false);
   }

   @Override
   public void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception {
      readLock();
      try {
         messageJournal.appendDeleteRecordTransactional(txID, ackID);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deleteCursorAcknowledge(long ackID) throws Exception {
      messageJournal.appendDeleteRecord(ackID, false);
   }

   @Override
   public long storeHeuristicCompletion(final Xid xid, final boolean isCommit) throws Exception {
      readLock();
      try {
         long id = generateID();

         messageJournal.appendAddRecord(id, JournalRecordIds.HEURISTIC_COMPLETION, new HeuristicCompletionEncoding(xid, isCommit), true, getContext(true));
         return id;
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deleteHeuristicCompletion(final long id) throws Exception {
      readLock();
      try {

         messageJournal.appendDeleteRecord(id, true, getContext(true));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deletePageTransactional(final long recordID) throws Exception {
      readLock();
      try {
         messageJournal.appendDeleteRecord(recordID, false);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void updateScheduledDeliveryTimeTransactional(final long txID, final MessageReference ref) throws Exception {
      ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding(ref.getScheduledDeliveryTime(), ref.getQueue().getID());
      readLock();
      try {

         messageJournal.appendUpdateRecordTransactional(txID, ref.getMessage().getMessageID(), JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME, encoding);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void prepare(final long txID, final Xid xid) throws Exception {
      readLock();
      try {
         messageJournal.appendPrepareRecord(txID, new XidEncoding(xid), syncTransactional, getContext(syncTransactional));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void commit(final long txID) throws Exception {
      commit(txID, true);
   }

   @Override
   public void commitBindings(final long txID) throws Exception {
      bindingsJournal.appendCommitRecord(txID, true);
   }

   @Override
   public void rollbackBindings(final long txID) throws Exception {
      // no need to sync, it's going away anyways
      bindingsJournal.appendRollbackRecord(txID, false);
   }

   @Override
   public void commit(final long txID, final boolean lineUpContext) throws Exception {
      readLock();
      try {
         messageJournal.appendCommitRecord(txID, syncTransactional, getContext(syncTransactional), lineUpContext);
         if (!lineUpContext && !syncTransactional) {
            /**
             * If {@code lineUpContext == false}, it means that we have previously lined up a
             * context somewhere else (specifically see @{link TransactionImpl#asyncAppendCommit}),
             * hence we need to mark it as done even if {@code syncTransactional = false} as in this
             * case {@code getContext(syncTransactional=false)} would pass a dummy context to the
             * {@code messageJournal.appendCommitRecord(...)} call above.
             */
            getContext(true).done();
         }
      } finally {
         readUnLock();
      }
   }

   @Override
   public void rollback(final long txID) throws Exception {
      readLock();
      try {
         messageJournal.appendRollbackRecord(txID, syncTransactional, getContext(syncTransactional));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void storeDuplicateIDTransactional(final long txID,
                                             final SimpleString address,
                                             final byte[] duplID,
                                             final long recordID) throws Exception {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      readLock();
      try {
         messageJournal.appendAddRecordTransactional(txID, recordID, JournalRecordIds.DUPLICATE_ID, encoding);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void updateDuplicateIDTransactional(final long txID,
                                              final SimpleString address,
                                              final byte[] duplID,
                                              final long recordID) throws Exception {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      readLock();
      try {
         messageJournal.appendUpdateRecordTransactional(txID, recordID, JournalRecordIds.DUPLICATE_ID, encoding);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deleteDuplicateIDTransactional(final long txID, final long recordID) throws Exception {
      readLock();
      try {
         messageJournal.appendDeleteRecordTransactional(txID, recordID);
      } finally {
         readUnLock();
      }
   }

   // Other operations

   @Override
   public void updateDeliveryCount(final MessageReference ref) throws Exception {
      // no need to store if it's the same value
      // otherwise the journal will get OME in case of lots of redeliveries
      if (ref.getDeliveryCount() == ref.getPersistedCount()) {
         return;
      }

      ref.setPersistedCount(ref.getDeliveryCount());
      DeliveryCountUpdateEncoding updateInfo = new DeliveryCountUpdateEncoding(ref.getQueue().getID(), ref.getDeliveryCount());

      readLock();
      try {
         messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(), JournalRecordIds.UPDATE_DELIVERY_COUNT, updateInfo, syncNonTransactional, getContext(syncNonTransactional));
      } finally {
         readUnLock();
      }
   }

   @Override
   public void storeAddressSetting(PersistedAddressSetting addressSetting) throws Exception {
      deleteAddressSetting(addressSetting.getAddressMatch());
      readLock();
      try {
         long id = idGenerator.generateID();
         addressSetting.setStoreId(id);
         bindingsJournal.appendAddRecord(id, JournalRecordIds.ADDRESS_SETTING_RECORD, addressSetting, true);
         mapPersistedAddressSettings.put(addressSetting.getAddressMatch(), addressSetting);
      } finally {
         readUnLock();
      }
   }

   @Override
   public List<PersistedAddressSetting> recoverAddressSettings() throws Exception {
      return new ArrayList<>(mapPersistedAddressSettings.values());
   }

   @Override
   public List<PersistedRoles> recoverPersistedRoles() throws Exception {
      return new ArrayList<>(mapPersistedRoles.values());
   }

   @Override
   public void storeSecurityRoles(PersistedRoles persistedRoles) throws Exception {

      deleteSecurityRoles(persistedRoles.getAddressMatch());
      readLock();
      try {
         final long id = idGenerator.generateID();
         persistedRoles.setStoreId(id);
         bindingsJournal.appendAddRecord(id, JournalRecordIds.SECURITY_RECORD, persistedRoles, true);
         mapPersistedRoles.put(persistedRoles.getAddressMatch(), persistedRoles);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void storeID(final long journalID, final long id) throws Exception {
      readLock();
      try {
         bindingsJournal.appendAddRecord(journalID, JournalRecordIds.ID_COUNTER_RECORD, BatchingIDGenerator.createIDEncodingSupport(id), true);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deleteID(long journalD) throws Exception {
      readLock();
      try {
         bindingsJournal.appendDeleteRecord(journalD, false);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deleteAddressSetting(SimpleString addressMatch) throws Exception {
      PersistedAddressSetting oldSetting = mapPersistedAddressSettings.remove(addressMatch);
      if (oldSetting != null) {
         readLock();
         try {
            bindingsJournal.appendDeleteRecord(oldSetting.getStoreId(), false);
         } finally {
            readUnLock();
         }
      }
   }

   @Override
   public void deleteSecurityRoles(SimpleString addressMatch) throws Exception {
      PersistedRoles oldRoles = mapPersistedRoles.remove(addressMatch);
      if (oldRoles != null) {
         readLock();
         try {
            bindingsJournal.appendDeleteRecord(oldRoles.getStoreId(), false);
         } finally {
            readUnLock();
         }
      }
   }

   @Override
   public JournalLoadInformation loadMessageJournal(final PostOffice postOffice,
                                                    final PagingManager pagingManager,
                                                    final ResourceManager resourceManager,
                                                    Map<Long, QueueBindingInfo> queueInfos,
                                                    final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                                    final Set<Pair<Long, Long>> pendingLargeMessages,
                                                    List<PageCountPending> pendingNonTXPageCounter,
                                                    final JournalLoader journalLoader) throws Exception {
      List<RecordInfo> records = new ArrayList<>();

      List<PreparedTransactionInfo> preparedTransactions = new ArrayList<>();

      Set<PageTransactionInfo> invalidPageTransactions = null;

      Map<Long, Message> messages = new HashMap<>();
      readLock();
      try {

         JournalLoadInformation info = messageJournal.load(records, preparedTransactions, new LargeMessageTXFailureCallback(this, messages));

         ArrayList<LargeServerMessage> largeMessages = new ArrayList<>();

         Map<Long, Map<Long, AddMessageRecord>> queueMap = new HashMap<>();

         Map<Long, PageSubscription> pageSubscriptions = new HashMap<>();

         final int totalSize = records.size();

         for (int reccount = 0; reccount < totalSize; reccount++) {
            // It will show log.info only with large journals (more than 1 million records)
            if (reccount > 0 && reccount % 1000000 == 0) {
               long percent = (long) ((((double) reccount) / ((double) totalSize)) * 100f);

               ActiveMQServerLogger.LOGGER.percentLoaded(percent);
            }

            RecordInfo record = records.get(reccount);
            byte[] data = record.data;

            ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(data);

            byte recordType = record.getUserRecordType();

            switch (recordType) {
               case JournalRecordIds.ADD_LARGE_MESSAGE_PENDING: {
                  PendingLargeMessageEncoding pending = new PendingLargeMessageEncoding();

                  pending.decode(buff);

                  if (pendingLargeMessages != null) {
                     // it could be null on tests, and we don't need anything on that case
                     pendingLargeMessages.add(new Pair<>(record.id, pending.largeMessageID));
                  }
                  break;
               }
               case JournalRecordIds.ADD_LARGE_MESSAGE: {
                  LargeServerMessage largeMessage = parseLargeMessage(messages, buff);

                  messages.put(record.id, largeMessage);

                  largeMessages.add(largeMessage);

                  break;
               }
               case JournalRecordIds.ADD_MESSAGE: {
                  throw new IllegalStateException("This is using old journal data, export your data and import at the correct version");
               }

               case JournalRecordIds.ADD_MESSAGE_PROTOCOL: {

                  Message message = MessagePersister.getInstance().decode(buff, null);

                  messages.put(record.id, message);

                  break;
               }
               case JournalRecordIds.ADD_REF: {
                  long messageID = record.id;

                  RefEncoding encoding = new RefEncoding();

                  encoding.decode(buff);

                  Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

                  if (queueMessages == null) {
                     queueMessages = new LinkedHashMap<>();

                     queueMap.put(encoding.queueID, queueMessages);
                  }

                  Message message = messages.get(messageID);

                  if (message == null) {
                     ActiveMQServerLogger.LOGGER.cannotFindMessage(record.id);
                  } else {
                     queueMessages.put(messageID, new AddMessageRecord(message));
                  }

                  break;
               }
               case JournalRecordIds.ACKNOWLEDGE_REF: {
                  long messageID = record.id;

                  RefEncoding encoding = new RefEncoding();

                  encoding.decode(buff);

                  Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

                  if (queueMessages == null) {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueue(encoding.queueID, messageID);
                  } else {
                     AddMessageRecord rec = queueMessages.remove(messageID);

                     if (rec == null) {
                        ActiveMQServerLogger.LOGGER.cannotFindMessage(messageID);
                     }
                  }

                  break;
               }
               case JournalRecordIds.UPDATE_DELIVERY_COUNT: {
                  long messageID = record.id;

                  DeliveryCountUpdateEncoding encoding = new DeliveryCountUpdateEncoding();

                  encoding.decode(buff);

                  Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

                  if (queueMessages == null) {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueDelCount(encoding.queueID);
                  } else {
                     AddMessageRecord rec = queueMessages.get(messageID);

                     if (rec == null) {
                        ActiveMQServerLogger.LOGGER.journalCannotFindMessageDelCount(messageID);
                     } else {
                        rec.setDeliveryCount(encoding.count);
                     }
                  }

                  break;
               }
               case JournalRecordIds.PAGE_TRANSACTION: {
                  PageTransactionInfo invalidPGTx = null;
                  if (record.isUpdate) {
                     PageUpdateTXEncoding pageUpdate = new PageUpdateTXEncoding();

                     pageUpdate.decode(buff);

                     PageTransactionInfo pageTX = pagingManager.getTransaction(pageUpdate.pageTX);

                     if (pageTX == null) {
                        ActiveMQServerLogger.LOGGER.journalCannotFindPageTX(pageUpdate.pageTX);
                     } else {
                        if (!pageTX.onUpdate(pageUpdate.recods, null, null)) {
                           invalidPGTx = pageTX;
                        }
                     }
                  } else {
                     PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

                     pageTransactionInfo.decode(buff);

                     pageTransactionInfo.setRecordID(record.id);

                     pagingManager.addTransaction(pageTransactionInfo);

                     if (!pageTransactionInfo.checkSize(null, null)) {
                        invalidPGTx = pageTransactionInfo;
                     }
                  }

                  if (invalidPGTx != null) {
                     if (invalidPageTransactions == null) {
                        invalidPageTransactions = new HashSet<>();
                     }
                     invalidPageTransactions.add(invalidPGTx);
                  }

                  break;
               }
               case JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME: {
                  long messageID = record.id;

                  ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding();

                  encoding.decode(buff);

                  Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

                  if (queueMessages == null) {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueScheduled(encoding.queueID, messageID);
                  } else {

                     AddMessageRecord rec = queueMessages.get(messageID);

                     if (rec == null) {
                        ActiveMQServerLogger.LOGGER.cannotFindMessage(messageID);
                     } else {
                        rec.setScheduledDeliveryTime(encoding.scheduledDeliveryTime);
                     }
                  }

                  break;
               }
               case JournalRecordIds.DUPLICATE_ID: {
                  DuplicateIDEncoding encoding = new DuplicateIDEncoding();

                  encoding.decode(buff);

                  List<Pair<byte[], Long>> ids = duplicateIDMap.get(encoding.address);

                  if (ids == null) {
                     ids = new ArrayList<>();

                     duplicateIDMap.put(encoding.address, ids);
                  }

                  ids.add(new Pair<>(encoding.duplID, record.id));

                  break;
               }
               case JournalRecordIds.HEURISTIC_COMPLETION: {
                  HeuristicCompletionEncoding encoding = new HeuristicCompletionEncoding();
                  encoding.decode(buff);
                  resourceManager.putHeuristicCompletion(record.id, encoding.xid, encoding.isCommit);
                  break;
               }
               case JournalRecordIds.ACKNOWLEDGE_CURSOR: {
                  CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
                  encoding.decode(buff);

                  encoding.position.setRecordID(record.id);

                  PageSubscription sub = locateSubscription(encoding.queueID, pageSubscriptions, queueInfos, pagingManager);

                  if (sub != null) {
                     sub.reloadACK(encoding.position);
                  } else {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueReloading(encoding.queueID);
                     messageJournal.appendDeleteRecord(record.id, false);

                  }

                  break;
               }
               case JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE: {
                  PageCountRecord encoding = new PageCountRecord();

                  encoding.decode(buff);

                  PageSubscription sub = locateSubscription(encoding.getQueueID(), pageSubscriptions, queueInfos, pagingManager);

                  if (sub != null) {
                     sub.getCounter().loadValue(record.id, encoding.getValue(), encoding.getPersistentSize());
                  } else {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueReloadingPage(encoding.getQueueID());
                     messageJournal.appendDeleteRecord(record.id, false);
                  }

                  break;
               }

               case JournalRecordIds.PAGE_CURSOR_COUNTER_INC: {
                  PageCountRecordInc encoding = new PageCountRecordInc();

                  encoding.decode(buff);

                  PageSubscription sub = locateSubscription(encoding.getQueueID(), pageSubscriptions, queueInfos, pagingManager);

                  if (sub != null) {
                     sub.getCounter().loadInc(record.id, encoding.getValue(), encoding.getPersistentSize());
                  } else {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueReloadingPageCursor(encoding.getQueueID());
                     messageJournal.appendDeleteRecord(record.id, false);
                  }

                  break;
               }

               case JournalRecordIds.PAGE_CURSOR_COMPLETE: {
                  CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
                  encoding.decode(buff);

                  encoding.position.setRecordID(record.id);

                  PageSubscription sub = locateSubscription(encoding.queueID, pageSubscriptions, queueInfos, pagingManager);

                  if (sub != null) {
                     if (!sub.reloadPageCompletion(encoding.position)) {
                        if (logger.isDebugEnabled()) {
                           logger.debug("Complete page " + encoding.position.getPageNr() + " doesn't exist on page manager " + sub.getPagingStore().getAddress());
                        }
                        messageJournal.appendDeleteRecord(record.id, false);
                     }
                  } else {
                     ActiveMQServerLogger.LOGGER.cantFindQueueOnPageComplete(encoding.queueID);
                     messageJournal.appendDeleteRecord(record.id, false);
                  }

                  break;
               }

               case JournalRecordIds.PAGE_CURSOR_PENDING_COUNTER: {

                  PageCountPendingImpl pendingCountEncoding = new PageCountPendingImpl();

                  pendingCountEncoding.decode(buff);
                  pendingCountEncoding.setID(record.id);

                  // This can be null on testcases not interested on this outcome
                  if (pendingNonTXPageCounter != null) {
                     pendingNonTXPageCounter.add(pendingCountEncoding);
                  }

                  break;
               }

               default: {
                  throw new IllegalStateException("Invalid record type " + recordType);
               }
            }

            // This will free up memory sooner. The record is not needed any more
            // and its byte array would consume memory during the load process even though it's not necessary any longer
            // what would delay processing time during load
            records.set(reccount, null);
         }

         // Release the memory as soon as not needed any longer
         records.clear();
         records = null;

         journalLoader.handleAddMessage(queueMap);

         loadPreparedTransactions(postOffice, pagingManager, resourceManager, queueInfos, preparedTransactions, duplicateIDMap, pageSubscriptions, pendingLargeMessages, journalLoader);

         for (PageSubscription sub : pageSubscriptions.values()) {
            sub.getCounter().processReload();
         }

         for (LargeServerMessage msg : largeMessages) {
            if (msg.getRefCount() == 0) {
               ActiveMQServerLogger.LOGGER.largeMessageWithNoRef(msg.getMessageID());
               msg.decrementDelayDeletionCount();
            }
         }

         journalLoader.handleNoMessageReferences(messages);

         // To recover positions on Iterators
         if (pagingManager != null) {
            // it could be null on certain tests that are not dealing with paging
            // This could also be the case in certain embedded conditions
            pagingManager.processReload();
         }

         journalLoader.postLoad(messageJournal, resourceManager, duplicateIDMap);

         checkInvalidPageTransactions(pagingManager, invalidPageTransactions);

         journalLoaded = true;
         return info;
      } finally {
         readUnLock();
      }
   }

   public void checkInvalidPageTransactions(PagingManager pagingManager,
                                            Set<PageTransactionInfo> invalidPageTransactions) {
      if (invalidPageTransactions != null) {
         for (PageTransactionInfo pginfo : invalidPageTransactions) {
            pginfo.checkSize(this, pagingManager);
         }
      }
   }

   /**
    * @param queueID
    * @param pageSubscriptions
    * @param queueInfos
    * @return
    */
   private static PageSubscription locateSubscription(final long queueID,
                                                      final Map<Long, PageSubscription> pageSubscriptions,
                                                      final Map<Long, QueueBindingInfo> queueInfos,
                                                      final PagingManager pagingManager) throws Exception {

      PageSubscription subs = pageSubscriptions.get(queueID);
      if (subs == null) {
         QueueBindingInfo queueInfo = queueInfos.get(queueID);

         if (queueInfo != null) {
            SimpleString address = queueInfo.getAddress();
            PagingStore store = pagingManager.getPageStore(address);
            subs = store.getCursorProvider().getSubscription(queueID);
            pageSubscriptions.put(queueID, subs);
         }
      }

      return subs;
   }

   // grouping handler operations
   @Override
   public void addGrouping(final GroupBinding groupBinding) throws Exception {
      GroupingEncoding groupingEncoding = new GroupingEncoding(groupBinding.getId(), groupBinding.getGroupId(), groupBinding.getClusterName());
      readLock();
      try {
         bindingsJournal.appendAddRecord(groupBinding.getId(), JournalRecordIds.GROUP_RECORD, groupingEncoding, true);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deleteGrouping(long tx, final GroupBinding groupBinding) throws Exception {
      readLock();
      try {
         bindingsJournal.appendDeleteRecordTransactional(tx, groupBinding.getId());
      } finally {
         readUnLock();
      }
   }

   // BindingsImpl operations

   @Override
   public void updateQueueBinding(long tx, Binding binding) throws Exception {
      internalQueueBinding(true, tx, binding);
   }

   @Override
   public void addQueueBinding(final long tx, final Binding binding) throws Exception {
      internalQueueBinding(false, tx, binding);
   }

   private void internalQueueBinding(boolean update, final long tx, final Binding binding) throws Exception {
      Queue queue = (Queue) binding.getBindable();

      Filter filter = queue.getFilter();

      SimpleString filterString = filter == null ? null : filter.getFilterString();

      PersistentQueueBindingEncoding bindingEncoding = new PersistentQueueBindingEncoding(queue.getName(), binding.getAddress(), filterString, queue.getUser(), queue.isAutoCreated(), queue.getMaxConsumers(), queue.isPurgeOnNoConsumers(), queue.isExclusive(), queue.getRoutingType().getType());

      readLock();
      try {
         if (update) {
            bindingsJournal.appendUpdateRecordTransactional(tx, binding.getID(), JournalRecordIds.QUEUE_BINDING_RECORD, bindingEncoding);
         } else {
            bindingsJournal.appendAddRecordTransactional(tx, binding.getID(), JournalRecordIds.QUEUE_BINDING_RECORD, bindingEncoding);
         }
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deleteQueueBinding(long tx, final long queueBindingID) throws Exception {
      readLock();
      try {
         bindingsJournal.appendDeleteRecordTransactional(tx, queueBindingID);
      } finally {
         readUnLock();
      }
   }

   @Override
   public long storeQueueStatus(long queueID, QueueStatus status) throws Exception {
      long recordID = idGenerator.generateID();

      readLock();
      try {
         bindingsJournal.appendAddRecord(recordID, JournalRecordIds.QUEUE_STATUS_RECORD, new QueueStatusEncoding(queueID, status), true);
      } finally {
         readUnLock();
      }


      return recordID;
   }

   @Override
   public void deleteQueueStatus(long recordID) throws Exception {
      readLock();
      try {
         bindingsJournal.appendDeleteRecord(recordID, true);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void addAddressBinding(final long tx, final AddressInfo addressInfo) throws Exception {
      PersistentAddressBindingEncoding bindingEncoding = new PersistentAddressBindingEncoding(addressInfo.getName(),
                                                                                              addressInfo.getRoutingTypes(),
                                                                                              addressInfo.isAutoCreated());

      readLock();
      try {
         long recordID = idGenerator.generateID();
         bindingEncoding.setId(recordID);
         addressInfo.setId(recordID);
         bindingsJournal.appendAddRecordTransactional(tx, recordID, JournalRecordIds.ADDRESS_BINDING_RECORD, bindingEncoding);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deleteAddressBinding(long tx, final long addressBindingID) throws Exception {
      readLock();
      try {
         bindingsJournal.appendDeleteRecordTransactional(tx, addressBindingID);
      } finally {
         readUnLock();
      }
   }

   @Override
   public long storePageCounterInc(long txID, long queueID, int value, long persistentSize) throws Exception {
      readLock();
      try {
         long recordID = idGenerator.generateID();
         messageJournal.appendAddRecordTransactional(txID, recordID, JournalRecordIds.PAGE_CURSOR_COUNTER_INC, new PageCountRecordInc(queueID, value, persistentSize));
         return recordID;
      } finally {
         readUnLock();
      }
   }

   @Override
   public long storePageCounterInc(long queueID, int value, long persistentSize) throws Exception {
      readLock();
      try {
         final long recordID = idGenerator.generateID();
         messageJournal.appendAddRecord(recordID, JournalRecordIds.PAGE_CURSOR_COUNTER_INC, new PageCountRecordInc(queueID, value, persistentSize), true, getContext());
         return recordID;
      } finally {
         readUnLock();
      }
   }

   @Override
   public long storePageCounter(long txID, long queueID, long value, long persistentSize) throws Exception {
      readLock();
      try {
         final long recordID = idGenerator.generateID();
         messageJournal.appendAddRecordTransactional(txID, recordID, JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE, new PageCountRecord(queueID, value, persistentSize));
         return recordID;
      } finally {
         readUnLock();
      }
   }

   @Override
   public long storePendingCounter(final long queueID, final long pageID) throws Exception {
      readLock();
      try {
         final long recordID = idGenerator.generateID();
         PageCountPendingImpl pendingInc = new PageCountPendingImpl(queueID, pageID);
         // We must guarantee the record sync before we actually write on the page otherwise we may get out of sync
         // on the counter
         messageJournal.appendAddRecord(recordID, JournalRecordIds.PAGE_CURSOR_PENDING_COUNTER, pendingInc, true);
         return recordID;
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deleteIncrementRecord(long txID, long recordID) throws Exception {
      readLock();
      try {
         messageJournal.appendDeleteRecordTransactional(txID, recordID);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deletePageCounter(long txID, long recordID) throws Exception {
      readLock();
      try {
         messageJournal.appendDeleteRecordTransactional(txID, recordID);
      } finally {
         readUnLock();
      }
   }

   @Override
   public void deletePendingPageCounter(long txID, long recordID) throws Exception {
      readLock();
      try {
         messageJournal.appendDeleteRecordTransactional(txID, recordID);
      } finally {
         readUnLock();
      }
   }

   @Override
   public JournalLoadInformation loadBindingJournal(final List<QueueBindingInfo> queueBindingInfos,
                                                    final List<GroupingInfo> groupingInfos,
                                                    final List<AddressBindingInfo> addressBindingInfos) throws Exception {
      List<RecordInfo> records = new ArrayList<>();

      List<PreparedTransactionInfo> preparedTransactions = new ArrayList<>();

      JournalLoadInformation bindingsInfo = bindingsJournal.load(records, preparedTransactions, null);

      HashMap<Long, PersistentQueueBindingEncoding> mapBindings = new HashMap<>();

      for (RecordInfo record : records) {
         long id = record.id;

         ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(record.data);

         byte rec = record.getUserRecordType();

         if (rec == JournalRecordIds.QUEUE_BINDING_RECORD) {
            PersistentQueueBindingEncoding bindingEncoding = newQueueBindingEncoding(id, buffer);
            mapBindings.put(bindingEncoding.getId(), bindingEncoding);
         } else if (rec == JournalRecordIds.ID_COUNTER_RECORD) {
            idGenerator.loadState(record.id, buffer);
         } else if (rec == JournalRecordIds.ADDRESS_BINDING_RECORD) {
            PersistentAddressBindingEncoding bindingEncoding = newAddressBindingEncoding(id, buffer);
            addressBindingInfos.add(bindingEncoding);
         } else if (rec == JournalRecordIds.GROUP_RECORD) {
            GroupingEncoding encoding = newGroupEncoding(id, buffer);
            groupingInfos.add(encoding);
         } else if (rec == JournalRecordIds.ADDRESS_SETTING_RECORD) {
            PersistedAddressSetting setting = newAddressEncoding(id, buffer);
            mapPersistedAddressSettings.put(setting.getAddressMatch(), setting);
         } else if (rec == JournalRecordIds.SECURITY_RECORD) {
            PersistedRoles roles = newSecurityRecord(id, buffer);
            mapPersistedRoles.put(roles.getAddressMatch(), roles);
         } else if (rec == JournalRecordIds.QUEUE_STATUS_RECORD) {
            QueueStatusEncoding statusEncoding = newQueueStatusEncoding(id, buffer);
            PersistentQueueBindingEncoding queueBindingEncoding = mapBindings.get(statusEncoding.queueID);
            if (queueBindingEncoding != null) {
               queueBindingEncoding.addQueueStatusEncoding(statusEncoding);
            } else {
               // unlikely to happen, so I didn't bother about the Logger method
               ActiveMQServerLogger.LOGGER.infoNoQueueWithID(statusEncoding.queueID, statusEncoding.getId());
               this.deleteQueueStatus(statusEncoding.getId());
            }
         } else {
            // unlikely to happen
            ActiveMQServerLogger.LOGGER.invalidRecordType(rec, new Exception("invalid record type " + rec));
         }
      }

      for (PersistentQueueBindingEncoding queue : mapBindings.values()) {
         queueBindingInfos.add(queue);
      }

      mapBindings.clear(); // just to give a hand to GC

      // This will instruct the IDGenerator to beforeStop old records
      idGenerator.cleanup();

      return bindingsInfo;
   }

   @Override
   public void lineUpContext() {
      readLock();
      try {
         messageJournal.lineUpContext(getContext());
      } finally {
         readUnLock();
      }
   }

   // ActiveMQComponent implementation
   // ------------------------------------------------------

   protected abstract void beforeStart() throws Exception;

   @Override
   public synchronized void start() throws Exception {
      if (started) {
         return;
      }

      beforeStart();

      singleThreadExecutor = ioExecutorFactory.getExecutor();

      bindingsJournal.start();

      messageJournal.start();

      started = true;
   }

   @Override
   public void stop() throws Exception {
      stop(false, true);
   }

   @Override
   public synchronized void persistIdGenerator() {
      if (journalLoaded && idGenerator != null) {
         // Must call close to make sure last id is persisted
         idGenerator.persistCurrentID();
      }
   }

   /**
    * Assumption is that this is only called with a writeLock on the StorageManager.
    */
   protected abstract void performCachedLargeMessageDeletes();

   @Override
   public synchronized void stop(boolean ioCriticalError, boolean sendFailover) throws Exception {
      if (!started) {
         return;
      }

      if (!ioCriticalError) {
         performCachedLargeMessageDeletes();
         // Must call close to make sure last id is persisted
         if (journalLoaded && idGenerator != null)
            idGenerator.persistCurrentID();
      }

      final CountDownLatch latch = new CountDownLatch(1);
      executor.execute(new Runnable() {
         @Override
         public void run() {
            latch.countDown();
         }
      });

      latch.await(30, TimeUnit.SECONDS);

      beforeStop();

      bindingsJournal.stop();

      messageJournal.stop();

      journalLoaded = false;

      started = false;
   }

   protected abstract void beforeStop() throws Exception;

   @Override
   public synchronized boolean isStarted() {
      return started;
   }

   /**
    * TODO: Is this still being used ?
    */
   public JournalLoadInformation[] loadInternalOnly() throws Exception {
      readLock();
      try {
         JournalLoadInformation[] info = new JournalLoadInformation[2];
         info[0] = bindingsJournal.loadInternalOnly();
         info[1] = messageJournal.loadInternalOnly();

         return info;
      } finally {
         readUnLock();
      }
   }

   @Override
   public void beforePageRead() throws Exception {
      if (pageMaxConcurrentIO != null) {
         pageMaxConcurrentIO.acquire();
      }
   }

   @Override
   public void afterPageRead() throws Exception {
      if (pageMaxConcurrentIO != null) {
         pageMaxConcurrentIO.release();
      }
   }

   // Public -----------------------------------------------------------------------------------

   @Override
   public Journal getMessageJournal() {
      return messageJournal;
   }

   @Override
   public Journal getBindingsJournal() {
      return bindingsJournal;
   }

   // Package protected ---------------------------------------------

   protected void confirmLargeMessage(final LargeServerMessage largeServerMessage) {
      if (largeServerMessage.getPendingRecordID() >= 0) {
         try {
            confirmPendingLargeMessage(largeServerMessage.getPendingRecordID());
            largeServerMessage.setPendingRecordID(LargeServerMessage.NO_PENDING_ID);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
         }
      }
   }

   protected abstract LargeServerMessage parseLargeMessage(Map<Long, Message> messages,
                                                           ActiveMQBuffer buff) throws Exception;

   private void loadPreparedTransactions(final PostOffice postOffice,
                                         final PagingManager pagingManager,
                                         final ResourceManager resourceManager,
                                         final Map<Long, QueueBindingInfo> queueInfos,
                                         final List<PreparedTransactionInfo> preparedTransactions,
                                         final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                         final Map<Long, PageSubscription> pageSubscriptions,
                                         final Set<Pair<Long, Long>> pendingLargeMessages,
                                         JournalLoader journalLoader) throws Exception {
      // recover prepared transactions
      for (PreparedTransactionInfo preparedTransaction : preparedTransactions) {
         XidEncoding encodingXid = new XidEncoding(preparedTransaction.getExtraData());

         Xid xid = encodingXid.xid;

         Transaction tx = new TransactionImpl(preparedTransaction.getId(), xid, this);

         List<MessageReference> referencesToAck = new ArrayList<>();

         Map<Long, Message> messages = new HashMap<>();

         // Use same method as load message journal to prune out acks, so they don't get added.
         // Then have reacknowledge(tx) methods on queue, which needs to add the page size

         // first get any sent messages for this tx and recreate
         for (RecordInfo record : preparedTransaction.getRecords()) {
            byte[] data = record.data;

            ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(data);

            byte recordType = record.getUserRecordType();

            switch (recordType) {
               case JournalRecordIds.ADD_LARGE_MESSAGE: {
                  messages.put(record.id, parseLargeMessage(messages, buff));

                  break;
               }
               case JournalRecordIds.ADD_MESSAGE: {

                  break;
               }
               case JournalRecordIds.ADD_MESSAGE_PROTOCOL: {
                  Message message = MessagePersister.getInstance().decode(buff, null);

                  messages.put(record.id, message);

                  break;
               }
               case JournalRecordIds.ADD_REF: {
                  long messageID = record.id;

                  RefEncoding encoding = new RefEncoding();

                  encoding.decode(buff);

                  Message message = messages.get(messageID);

                  if (message == null) {
                     throw new IllegalStateException("Cannot find message with id " + messageID);
                  }

                  journalLoader.handlePreparedSendMessage(message, tx, encoding.queueID);

                  break;
               }
               case JournalRecordIds.ACKNOWLEDGE_REF: {
                  long messageID = record.id;

                  RefEncoding encoding = new RefEncoding();

                  encoding.decode(buff);

                  journalLoader.handlePreparedAcknowledge(messageID, referencesToAck, encoding.queueID);

                  break;
               }
               case JournalRecordIds.PAGE_TRANSACTION: {

                  PageTransactionInfo pageTransactionInfo = new PageTransactionInfoImpl();

                  pageTransactionInfo.decode(buff);

                  if (record.isUpdate) {
                     PageTransactionInfo pgTX = pagingManager.getTransaction(pageTransactionInfo.getTransactionID());
                     pgTX.reloadUpdate(this, pagingManager, tx, pageTransactionInfo.getNumberOfMessages());
                  } else {
                     pageTransactionInfo.setCommitted(false);

                     tx.putProperty(TransactionPropertyIndexes.PAGE_TRANSACTION, pageTransactionInfo);

                     pagingManager.addTransaction(pageTransactionInfo);

                     tx.addOperation(new FinishPageMessageOperation());
                  }

                  break;
               }
               case SET_SCHEDULED_DELIVERY_TIME: {
                  // Do nothing - for prepared txs, the set scheduled delivery time will only occur in a send in which
                  // case the message will already have the header for the scheduled delivery time, so no need to do
                  // anything.

                  break;
               }
               case DUPLICATE_ID: {
                  // We need load the duplicate ids at prepare time too
                  DuplicateIDEncoding encoding = new DuplicateIDEncoding();

                  encoding.decode(buff);

                  DuplicateIDCache cache = postOffice.getDuplicateIDCache(encoding.address);

                  cache.load(tx, encoding.duplID);

                  break;
               }
               case ACKNOWLEDGE_CURSOR: {
                  CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
                  encoding.decode(buff);

                  encoding.position.setRecordID(record.id);

                  PageSubscription sub = locateSubscription(encoding.queueID, pageSubscriptions, queueInfos, pagingManager);

                  if (sub != null) {
                     sub.reloadPreparedACK(tx, encoding.position);
                     referencesToAck.add(new PagedReferenceImpl(encoding.position, null, sub));
                  } else {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueReloadingACK(encoding.queueID);
                  }
                  break;
               }
               case PAGE_CURSOR_COUNTER_VALUE: {
                  ActiveMQServerLogger.LOGGER.journalPAGEOnPrepared();

                  break;
               }

               case PAGE_CURSOR_COUNTER_INC: {
                  PageCountRecordInc encoding = new PageCountRecordInc();

                  encoding.decode(buff);

                  PageSubscription sub = locateSubscription(encoding.getQueueID(), pageSubscriptions, queueInfos, pagingManager);

                  if (sub != null) {
                     sub.getCounter().applyIncrementOnTX(tx, record.id, encoding.getValue(), encoding.getPersistentSize());
                     sub.notEmpty();
                  } else {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueReloadingACK(encoding.getQueueID());
                  }

                  break;
               }

               default: {
                  ActiveMQServerLogger.LOGGER.journalInvalidRecordType(recordType);
               }
            }
         }

         for (RecordInfo recordDeleted : preparedTransaction.getRecordsToDelete()) {
            byte[] data = recordDeleted.data;

            if (data.length > 0) {
               ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(data);
               byte b = buff.readByte();

               switch (b) {
                  case ADD_LARGE_MESSAGE_PENDING: {
                     long messageID = buff.readLong();
                     if (!pendingLargeMessages.remove(new Pair<>(recordDeleted.id, messageID))) {
                        ActiveMQServerLogger.LOGGER.largeMessageNotFound(recordDeleted.id);
                     }
                     installLargeMessageConfirmationOnTX(tx, recordDeleted.id);
                     break;
                  }
                  default:
                     ActiveMQServerLogger.LOGGER.journalInvalidRecordTypeOnPreparedTX(b);
               }
            }

         }

         journalLoader.handlePreparedTransaction(tx, referencesToAck, xid, resourceManager);
      }
   }

   OperationContext getContext(final boolean sync) {
      if (sync) {
         return getContext();
      } else {
         return DummyOperationContext.getInstance();
      }
   }

   // Inner Classes
   // ----------------------------------------------------------------------------

   private static final class DummyOperationContext implements OperationContext {

      private static DummyOperationContext instance = new DummyOperationContext();

      public static OperationContext getInstance() {
         return DummyOperationContext.instance;
      }

      @Override
      public void executeOnCompletion(final IOCallback runnable) {
         // There are no executeOnCompletion calls while using the DummyOperationContext
         // However we keep the code here for correctness
         runnable.done();
      }

      @Override
      public void executeOnCompletion(IOCallback runnable, boolean storeOnly) {
         executeOnCompletion(runnable);
      }

      @Override
      public void replicationDone() {
      }

      @Override
      public void replicationLineUp() {
      }

      @Override
      public void storeLineUp() {
      }

      @Override
      public void done() {
      }

      @Override
      public void onError(final int errorCode, final String errorMessage) {
      }

      @Override
      public void waitCompletion() {
      }

      @Override
      public boolean waitCompletion(final long timeout) {
         return true;
      }

      @Override
      public void pageSyncLineUp() {
      }

      @Override
      public void pageSyncDone() {
      }
   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   protected static PersistedRoles newSecurityRecord(long id, ActiveMQBuffer buffer) {
      PersistedRoles roles = new PersistedRoles();
      roles.decode(buffer);
      roles.setStoreId(id);
      return roles;
   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   static PersistedAddressSetting newAddressEncoding(long id, ActiveMQBuffer buffer) {
      PersistedAddressSetting setting = new PersistedAddressSetting();
      setting.decode(buffer);
      setting.setStoreId(id);
      return setting;
   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   static GroupingEncoding newGroupEncoding(long id, ActiveMQBuffer buffer) {
      GroupingEncoding encoding = new GroupingEncoding();
      encoding.decode(buffer);
      encoding.setId(id);
      return encoding;
   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   protected static PersistentQueueBindingEncoding newQueueBindingEncoding(long id, ActiveMQBuffer buffer) {
      PersistentQueueBindingEncoding bindingEncoding = new PersistentQueueBindingEncoding();

      bindingEncoding.decode(buffer);

      bindingEncoding.setId(id);
      return bindingEncoding;
   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   protected static QueueStatusEncoding newQueueStatusEncoding(long id, ActiveMQBuffer buffer) {
      QueueStatusEncoding statusEncoding = new QueueStatusEncoding();

      statusEncoding.decode(buffer);
      statusEncoding.setId(id);

      return statusEncoding;
   }

   protected static PersistentAddressBindingEncoding newAddressBindingEncoding(long id, ActiveMQBuffer buffer) {
      PersistentAddressBindingEncoding bindingEncoding = new PersistentAddressBindingEncoding();

      bindingEncoding.decode(buffer);

      bindingEncoding.setId(id);
      return bindingEncoding;
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
      return store.page(msg, tx, listCtx, storageManagerLock.readLock());
   }

   private void installLargeMessageConfirmationOnTX(Transaction tx, long recordID) {
      TXLargeMessageConfirmationOperation txoper = (TXLargeMessageConfirmationOperation) tx.getProperty(TransactionPropertyIndexes.LARGE_MESSAGE_CONFIRMATIONS);
      if (txoper == null) {
         txoper = new TXLargeMessageConfirmationOperation(this);
         tx.putProperty(TransactionPropertyIndexes.LARGE_MESSAGE_CONFIRMATIONS, txoper);
      }
      txoper.confirmedMessages.add(recordID);
   }

}
