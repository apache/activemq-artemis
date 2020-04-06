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
package org.apache.activemq.artemis.core.journal;

import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.utils.collections.SparseArrayLinkedList;

/**
 * Most methods on the journal provide a blocking version where you select the sync mode and a non
 * blocking mode where you pass a completion callback as a parameter.
 * <p>
 * Notice also that even on the callback methods it's possible to pass the sync mode. That will only
 * make sense on the NIO operations.
 */
public interface Journal extends ActiveMQComponent {

   enum JournalState {
      STOPPED,
      /**
       * The journal has some fields initialized and services running. But it is not fully
       * operational. See {@link JournalState#LOADED}.
       */
      STARTED,
      /**
       * When a replicating server is still not synchronized with its live. So if the live stops,
       * the backup may not fail-over and will stop as well.
       */
      SYNCING,
      /**
       * Journal is being used by a replicating server which is up-to-date with its live. That means
       * that if the live stops, the backup can fail-over.
       */
      SYNCING_UP_TO_DATE,
      /**
       * The journal is fully operational. This is the state the journal should be when its server
       * is live.
       */
      LOADED;
   }

   // Non transactional operations

   void appendAddRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception;

   default void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception {
      appendAddRecord(id, recordType, EncoderPersister.getInstance(), record, sync);
   }

   void appendAddRecord(long id, byte recordType, Persister persister, Object record, boolean sync) throws Exception;

   void appendAddRecord(long id,
                        byte recordType,
                        Persister persister,
                        Object record,
                        boolean sync,
                        IOCompletion completionCallback) throws Exception;

   default void appendAddRecord(long id,
                        byte recordType,
                        EncodingSupport record,
                        boolean sync,
                        IOCompletion completionCallback) throws Exception {
      appendAddRecord(id, recordType, EncoderPersister.getInstance(), record, sync, completionCallback);
   }

   void appendUpdateRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception;

   boolean tryAppendUpdateRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception;

   default void appendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception {
      appendUpdateRecord(id, recordType, EncoderPersister.getInstance(), record, sync);
   }

   default boolean tryAppendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception {
      return tryAppendUpdateRecord(id, recordType, EncoderPersister.getInstance(), record, sync);
   }

   void appendUpdateRecord(long id, byte recordType, Persister persister, Object record, boolean sync) throws Exception;

   boolean tryAppendUpdateRecord(long id, byte recordType, Persister persister, Object record, boolean sync) throws Exception;

   default void appendUpdateRecord(long id,
                                   byte recordType,
                                   EncodingSupport record,
                                   boolean sync,
                                   IOCompletion completionCallback) throws Exception {
      appendUpdateRecord(id, recordType, EncoderPersister.getInstance(), record, sync, completionCallback);
   }

   default boolean tryAppendUpdateRecord(long id,
                                   byte recordType,
                                   EncodingSupport record,
                                   boolean sync,
                                   IOCompletion completionCallback) throws Exception {
      return tryAppendUpdateRecord(id, recordType, EncoderPersister.getInstance(), record, sync, completionCallback);
   }

   void appendUpdateRecord(long id,
                           byte recordType,
                           Persister persister,
                           Object record,
                           boolean sync,
                           IOCompletion callback) throws Exception;

   boolean tryAppendUpdateRecord(long id,
                           byte recordType,
                           Persister persister,
                           Object record,
                           boolean sync,
                           IOCompletion callback) throws Exception;

   void appendDeleteRecord(long id, boolean sync) throws Exception;

   boolean tryAppendDeleteRecord(long id, boolean sync) throws Exception;

   void appendDeleteRecord(long id, boolean sync, IOCompletion completionCallback) throws Exception;

   boolean tryAppendDeleteRecord(long id, boolean sync, IOCompletion completionCallback) throws Exception;

   // Transactional operations

   void appendAddRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception;

   default void appendAddRecordTransactional(long txID, long id, byte recordType, EncodingSupport record) throws Exception {
      appendAddRecordTransactional(txID, id, recordType, EncoderPersister.getInstance(), record);
   }

   void appendAddRecordTransactional(long txID,
                                     long id,
                                     byte recordType,
                                     Persister persister,
                                     Object record) throws Exception;

   void appendUpdateRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception;

   default void appendUpdateRecordTransactional(long txID, long id, byte recordType, EncodingSupport record) throws Exception {
      appendUpdateRecordTransactional(txID, id, recordType, EncoderPersister.getInstance(), record);
   }

   void appendUpdateRecordTransactional(long txID, long id, byte recordType, Persister persister, Object record) throws Exception;

   void appendDeleteRecordTransactional(long txID, long id, byte[] record) throws Exception;

   void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record) throws Exception;

   void appendDeleteRecordTransactional(long txID, long id) throws Exception;

   void appendCommitRecord(long txID, boolean sync) throws Exception;

   void appendCommitRecord(long txID, boolean sync, IOCompletion callback) throws Exception;

   /**
    * @param txID
    * @param sync
    * @param callback
    * @param lineUpContext if appendCommitRecord should call a storeLineUp. This is because the
    *                      caller may have already taken into account
    * @throws Exception
    */
   void appendCommitRecord(long txID, boolean sync, IOCompletion callback, boolean lineUpContext) throws Exception;

   /**
    * <p>If the system crashed after a prepare was called, it should store information that is required to bring the transaction
    * back to a state it could be committed. </p>
    *
    * <p> transactionData allows you to store any other supporting user-data related to the transaction</p>
    *
    * @param txID
    * @param transactionData - extra user data for the prepare
    * @throws Exception
    */
   void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync) throws Exception;

   void appendPrepareRecord(long txID,
                            EncodingSupport transactionData,
                            boolean sync,
                            IOCompletion callback) throws Exception;

   void appendPrepareRecord(long txID, byte[] transactionData, boolean sync) throws Exception;

   void appendRollbackRecord(long txID, boolean sync) throws Exception;

   void appendRollbackRecord(long txID, boolean sync, IOCompletion callback) throws Exception;

   // Load

   JournalLoadInformation load(LoaderCallback reloadManager) throws Exception;

   /**
    * Load internal data structures and not expose any data. This is only useful if you're using the
    * journal but not interested on the current data. Useful in situations where the journal is
    * being replicated, copied... etc.
    */
   JournalLoadInformation loadInternalOnly() throws Exception;

   /**
    * Load internal data structures, and remain waiting for synchronization to complete.
    *
    * @param state the current state of the journal, this parameter ensures consistency.
    */
   JournalLoadInformation loadSyncOnly(JournalState state) throws Exception;

   void lineUpContext(IOCompletion callback);

   default JournalLoadInformation load(List<RecordInfo> committedRecords,
                               List<PreparedTransactionInfo> preparedTransactions,
                               TransactionFailureCallback transactionFailure) throws Exception {
      return load(committedRecords, preparedTransactions, transactionFailure, true);
   }

   JournalLoadInformation load(List<RecordInfo> committedRecords,
                               List<PreparedTransactionInfo> preparedTransactions,
                               TransactionFailureCallback transactionFailure,
                               boolean fixBadTx) throws Exception;

   default JournalLoadInformation load(SparseArrayLinkedList<RecordInfo> committedRecords,
                                       List<PreparedTransactionInfo> preparedTransactions,
                                       TransactionFailureCallback transactionFailure) throws Exception {
      return load(committedRecords, preparedTransactions, transactionFailure, true);
   }

   JournalLoadInformation load(SparseArrayLinkedList<RecordInfo> committedRecords,
                               List<PreparedTransactionInfo> preparedTransactions,
                               TransactionFailureCallback transactionFailure,
                               boolean fixBadTx) throws Exception;

   int getAlignment() throws Exception;

   int getNumberOfRecords();

   int getUserVersion();

   void runDirectJournalBlast() throws Exception;

   /**
    * Reserves journal file IDs, creates the necessary files for synchronization, and places
    * references to these (reserved for sync) files in the map.
    * <p>
    * During the synchronization between a live server and backup, we reserve in the backup the
    * journal file IDs used in the live server. This call also makes sure the files are created
    * empty without any kind of headers added.
    *
    * @param fileIds IDs to reserve for synchronization
    * @return map to be filled with id and journal file pairs for <b>synchronization</b>.
    * @throws Exception
    */
   Map<Long, JournalFile> createFilesForBackupSync(long[] fileIds) throws Exception;

   /**
    * Write lock the Journal and write lock the compacting process. Necessary only during
    * replication for backup synchronization.
    */
   void synchronizationLock();

   /**
    * Unlock the Journal and the compacting process.
    *
    * @see Journal#synchronizationLock()
    */
   void synchronizationUnlock();

   /**
    * Force the usage of a new {@link JournalFile}.
    *
    * @throws Exception
    */
   void forceMoveNextFile() throws Exception;

   /**
    * Returns the {@link JournalFile}s in use.
    *
    * @return array with all {@link JournalFile}s in use
    */
   JournalFile[] getDataFiles();

   SequentialFileFactory getFileFactory();

   int getFileSize();

   /**
    * This method will start compact using the compactorExecutor and block up to timeout seconds
    *
    * @param timeout the timeout in seconds or block forever if {@code <= 0}
    * @throws Exception
    */
   void scheduleCompactAndBlock(int timeout) throws Exception;

   /**
    * Stops any operation that may delete or modify old (stale) data.
    * <p>
    * Meant to be used during synchronization of data between a live server and its replicating
    * (remote) backup. Old files must not be compacted or deleted during synchronization.
    */
   void replicationSyncPreserveOldFiles();

   /**
    * Restarts file reclaim and compacting on the journal.
    * <p>
    * Meant to be used to revert the effect of {@link #replicationSyncPreserveOldFiles()}. it should
    * only be called once the synchronization of the backup and live servers is completed.
    */
   void replicationSyncFinished();

   /**
    * It will make sure there are no more pending operations on the Executors.
    * */
   void flush() throws Exception;

   /**
    * The max size record that can be stored in the journal
    * @return
    */
   long getMaxRecordSize();
}
