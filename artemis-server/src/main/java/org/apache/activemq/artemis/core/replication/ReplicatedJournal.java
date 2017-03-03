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
package org.apache.activemq.artemis.core.replication;

import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.LoaderCallback;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.dataformat.ByteArrayEncoding;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.replication.ReplicationManager.ADD_OPERATION_TYPE;

/**
 * Used by the {@link org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager} to replicate journal calls.
 * <p>
 * This class wraps a {@link ReplicationManager} and the local {@link Journal}. Every call will be
 * relayed to both instances.
 *
 * @see org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager
 */
public class ReplicatedJournal implements Journal {

   private static final boolean trace = false;

   private static void trace(final String message) {
      System.out.println("ReplicatedJournal::" + message);
   }

   private final ReplicationManager replicationManager;

   private final Journal localJournal;

   private final byte journalID;

   public ReplicatedJournal(final byte journalID,
                            final Journal localJournal,
                            final ReplicationManager replicationManager) {
      super();
      this.journalID = journalID;
      this.localJournal = localJournal;
      this.replicationManager = replicationManager;
   }

   @Override
   public void flush() throws Exception {

   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendAddRecord(long, byte, byte[], boolean)
    */
   @Override
   public void appendAddRecord(final long id,
                               final byte recordType,
                               final byte[] record,
                               final boolean sync) throws Exception {
      this.appendAddRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   @Override
   public void appendAddRecord(final long id,
                               final byte recordType,
                               Persister persister,
                               final Object record,
                               final boolean sync) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("Append record id = " + id + " recordType = " + recordType);
      }
      replicationManager.appendUpdateRecord(journalID, ADD_OPERATION_TYPE.ADD, id, recordType, persister, record);
      localJournal.appendAddRecord(id, recordType, persister, record, sync);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendAddRecord(long, byte, org.apache.activemq.artemis.core.journal.EncodingSupport, boolean)
    */
   @Override
   public void appendAddRecord(final long id,
                               final byte recordType,
                               Persister persister,
                               final Object record,
                               final boolean sync,
                               final IOCompletion completionCallback) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("Append record id = " + id + " recordType = " + recordType);
      }
      replicationManager.appendUpdateRecord(journalID, ADD_OPERATION_TYPE.ADD, id, recordType, persister, record);
      localJournal.appendAddRecord(id, recordType, persister, record, sync, completionCallback);
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendAddRecordTransactional(long, long, byte, byte[])
    */
   @Override
   public void appendAddRecordTransactional(final long txID,
                                            final long id,
                                            final byte recordType,
                                            final byte[] record) throws Exception {
      this.appendAddRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendAddRecordTransactional(long, long, byte, org.apache.activemq.artemis.core.journal.EncodingSupport)
    */
   @Override
   public void appendAddRecordTransactional(final long txID,
                                            final long id,
                                            final byte recordType,
                                            final Persister persister,
                                            final Object record) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("Append record TXid = " + id + " recordType = " + recordType);
      }
      replicationManager.appendAddRecordTransactional(journalID, ADD_OPERATION_TYPE.ADD, txID, id, recordType, persister, record);
      localJournal.appendAddRecordTransactional(txID, id, recordType, persister, record);
   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendCommitRecord(long, boolean)
    */
   @Override
   public void appendCommitRecord(final long txID, final boolean sync) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendCommit " + txID);
      }
      replicationManager.appendCommitRecord(journalID, txID, sync, true);
      localJournal.appendCommitRecord(txID, sync);
   }

   @Override
   public void appendCommitRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendCommit " + txID);
      }
      replicationManager.appendCommitRecord(journalID, txID, sync, true);
      localJournal.appendCommitRecord(txID, sync, callback);
   }

   @Override
   public void appendCommitRecord(long txID,
                                  boolean sync,
                                  IOCompletion callback,
                                  boolean lineUpContext) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendCommit " + txID);
      }
      replicationManager.appendCommitRecord(journalID, txID, sync, lineUpContext);
      localJournal.appendCommitRecord(txID, sync, callback, lineUpContext);
   }

   /**
    * @param id
    * @param sync
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendDeleteRecord(long, boolean)
    */
   @Override
   public void appendDeleteRecord(final long id, final boolean sync) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendDelete " + id);
      }
      replicationManager.appendDeleteRecord(journalID, id);
      localJournal.appendDeleteRecord(id, sync);
   }

   @Override
   public void appendDeleteRecord(final long id,
                                  final boolean sync,
                                  final IOCompletion completionCallback) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendDelete " + id);
      }
      replicationManager.appendDeleteRecord(journalID, id);
      localJournal.appendDeleteRecord(id, sync, completionCallback);
   }

   /**
    * @param txID
    * @param id
    * @param record
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendDeleteRecordTransactional(long, long, byte[])
    */
   @Override
   public void appendDeleteRecordTransactional(final long txID, final long id, final byte[] record) throws Exception {
      this.appendDeleteRecordTransactional(txID, id, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param record
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendDeleteRecordTransactional(long, long, org.apache.activemq.artemis.core.journal.EncodingSupport)
    */
   @Override
   public void appendDeleteRecordTransactional(final long txID,
                                               final long id,
                                               final EncodingSupport record) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendDelete txID=" + txID + " id=" + id);
      }
      replicationManager.appendDeleteRecordTransactional(journalID, txID, id, record);
      localJournal.appendDeleteRecordTransactional(txID, id, record);
   }

   /**
    * @param txID
    * @param id
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendDeleteRecordTransactional(long, long)
    */
   @Override
   public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendDelete (noencoding) txID=" + txID + " id=" + id);
      }
      replicationManager.appendDeleteRecordTransactional(journalID, txID, id);
      localJournal.appendDeleteRecordTransactional(txID, id);
   }

   /**
    * @param txID
    * @param transactionData
    * @param sync
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendPrepareRecord(long, byte[], boolean)
    */
   @Override
   public void appendPrepareRecord(final long txID, final byte[] transactionData, final boolean sync) throws Exception {
      this.appendPrepareRecord(txID, new ByteArrayEncoding(transactionData), sync);
   }

   /**
    * @param txID
    * @param transactionData
    * @param sync
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendPrepareRecord(long, org.apache.activemq.artemis.core.journal.EncodingSupport, boolean)
    */
   @Override
   public void appendPrepareRecord(final long txID,
                                   final EncodingSupport transactionData,
                                   final boolean sync) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendPrepare txID=" + txID);
      }
      replicationManager.appendPrepareRecord(journalID, txID, transactionData);
      localJournal.appendPrepareRecord(txID, transactionData, sync);
   }

   @Override
   public void appendPrepareRecord(final long txID,
                                   final EncodingSupport transactionData,
                                   final boolean sync,
                                   final IOCompletion callback) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendPrepare txID=" + txID);
      }
      replicationManager.appendPrepareRecord(journalID, txID, transactionData);
      localJournal.appendPrepareRecord(txID, transactionData, sync, callback);
   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendRollbackRecord(long, boolean)
    */
   @Override
   public void appendRollbackRecord(final long txID, final boolean sync) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendRollback " + txID);
      }
      replicationManager.appendRollbackRecord(journalID, txID);
      localJournal.appendRollbackRecord(txID, sync);
   }

   @Override
   public void appendRollbackRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendRollback " + txID);
      }
      replicationManager.appendRollbackRecord(journalID, txID);
      localJournal.appendRollbackRecord(txID, sync, callback);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendUpdateRecord(long, byte, byte[], boolean)
    */
   @Override
   public void appendUpdateRecord(final long id,
                                  final byte recordType,
                                  final byte[] record,
                                  final boolean sync) throws Exception {
      this.appendUpdateRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendUpdateRecord(long, byte, org.apache.activemq.artemis.core.journal.EncodingSupport, boolean)
    */
   @Override
   public void appendUpdateRecord(final long id,
                                  final byte recordType,
                                  final Persister persister,
                                  final Object record,
                                  final boolean sync) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendUpdateRecord id = " + id + " , recordType = " + recordType);
      }
      replicationManager.appendUpdateRecord(journalID, ADD_OPERATION_TYPE.UPDATE, id, recordType, persister, record);
      localJournal.appendUpdateRecord(id, recordType, persister, record, sync);
   }

   @Override
   public void appendUpdateRecord(final long id,
                                  final byte journalRecordType,
                                  final Persister persister,
                                  final Object record,
                                  final boolean sync,
                                  final IOCompletion completionCallback) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendUpdateRecord id = " + id + " , recordType = " + journalRecordType);
      }
      replicationManager.appendUpdateRecord(journalID, ADD_OPERATION_TYPE.UPDATE, id, journalRecordType, persister, record);
      localJournal.appendUpdateRecord(id, journalRecordType, persister, record, sync, completionCallback);
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, byte[])
    */
   @Override
   public void appendUpdateRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final byte[] record) throws Exception {
      this.appendUpdateRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, org.apache.activemq.artemis.core.journal.EncodingSupport)
    */
   @Override
   public void appendUpdateRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final Persister persister,
                                               final Object record) throws Exception {
      if (ReplicatedJournal.trace) {
         ReplicatedJournal.trace("AppendUpdateRecord txid=" + txID + " id = " + id + " , recordType = " + recordType);
      }
      replicationManager.appendAddRecordTransactional(journalID, ADD_OPERATION_TYPE.UPDATE, txID, id, recordType, persister, record);
      localJournal.appendUpdateRecordTransactional(txID, id, recordType, persister, record);
   }

   /**
    * @param committedRecords
    * @param preparedTransactions
    * @param transactionFailure
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#load(java.util.List, java.util.List, org.apache.activemq.artemis.core.journal.TransactionFailureCallback)
    */
   @Override
   public JournalLoadInformation load(final List<RecordInfo> committedRecords,
                                      final List<PreparedTransactionInfo> preparedTransactions,
                                      final TransactionFailureCallback transactionFailure) throws Exception {
      return localJournal.load(committedRecords, preparedTransactions, transactionFailure);
   }

   /**
    * @param reloadManager
    * @throws Exception
    * @see org.apache.activemq.artemis.core.journal.Journal#load(org.apache.activemq.artemis.core.journal.LoaderCallback)
    */
   @Override
   public JournalLoadInformation load(final LoaderCallback reloadManager) throws Exception {
      return localJournal.load(reloadManager);
   }

   /**
    * @throws Exception
    * @see org.apache.activemq.artemis.core.server.ActiveMQComponent#start()
    */
   @Override
   public void start() throws Exception {
      localJournal.start();
   }

   /**
    * @throws Exception
    * @see org.apache.activemq.artemis.core.server.ActiveMQComponent#stop()
    */
   @Override
   public void stop() throws Exception {
      localJournal.stop();
   }

   @Override
   public int getAlignment() throws Exception {
      return localJournal.getAlignment();
   }

   @Override
   public boolean isStarted() {
      return localJournal.isStarted();
   }

   @Override
   public JournalLoadInformation loadInternalOnly() throws Exception {
      return localJournal.loadInternalOnly();
   }

   @Override
   public int getNumberOfRecords() {
      return localJournal.getNumberOfRecords();
   }

   @Override
   public void runDirectJournalBlast() throws Exception {
      localJournal.runDirectJournalBlast();
   }

   @Override
   public int getUserVersion() {
      return localJournal.getUserVersion();
   }

   @Override
   public void lineUpContext(IOCompletion callback) {
      ((OperationContext) callback).replicationLineUp();
      localJournal.lineUpContext(callback);
   }

   @Override
   public JournalLoadInformation loadSyncOnly(JournalState state) throws Exception {
      return localJournal.loadSyncOnly(state);
   }

   @Override
   public Map<Long, JournalFile> createFilesForBackupSync(long[] fileIds) throws Exception {
      throw new UnsupportedOperationException("This method should only be called at a replicating backup");
   }

   @Override
   public void synchronizationLock() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void synchronizationUnlock() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void forceMoveNextFile() {
      throw new UnsupportedOperationException();
   }

   @Override
   public JournalFile[] getDataFiles() {
      throw new UnsupportedOperationException();
   }

   @Override
   public SequentialFileFactory getFileFactory() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getFileSize() {
      return localJournal.getFileSize();
   }

   @Override
   public void scheduleCompactAndBlock(int timeout) throws Exception {
      localJournal.scheduleCompactAndBlock(timeout);
   }

   @Override
   public void replicationSyncPreserveOldFiles() {
      throw new UnsupportedOperationException("should never get called");
   }

   @Override
   public void replicationSyncFinished() {
      throw new UnsupportedOperationException("should never get called");
   }
}
