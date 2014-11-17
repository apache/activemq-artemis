/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.core.replication;

import java.util.List;
import java.util.Map;

import org.apache.activemq6.core.journal.EncodingSupport;
import org.apache.activemq6.core.journal.IOCompletion;
import org.apache.activemq6.core.journal.Journal;
import org.apache.activemq6.core.journal.JournalLoadInformation;
import org.apache.activemq6.core.journal.LoaderCallback;
import org.apache.activemq6.core.journal.PreparedTransactionInfo;
import org.apache.activemq6.core.journal.RecordInfo;
import org.apache.activemq6.core.journal.SequentialFileFactory;
import org.apache.activemq6.core.journal.TransactionFailureCallback;
import org.apache.activemq6.core.journal.impl.JournalFile;
import org.apache.activemq6.core.journal.impl.dataformat.ByteArrayEncoding;
import org.apache.activemq6.core.persistence.OperationContext;
import org.apache.activemq6.core.replication.ReplicationManager.ADD_OPERATION_TYPE;

/**
 * Used by the {@link org.apache.activemq6.core.persistence.impl.journal.JournalStorageManager} to replicate journal calls.
 * <p>
 * This class wraps a {@link ReplicationManager} and the local {@link Journal}. Every call will be
 * relayed to both instances.
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @see org.apache.activemq6.core.persistence.impl.journal.JournalStorageManager
 */
public class ReplicatedJournal implements Journal
{

   private static final boolean trace = false;

   private static void trace(final String message)
   {
      System.out.println("ReplicatedJournal::" + message);
   }

   private final ReplicationManager replicationManager;

   private final Journal localJournal;

   private final byte journalID;

   public ReplicatedJournal(final byte journalID, final Journal localJournal,
                            final ReplicationManager replicationManager)
   {
      super();
      this.journalID = journalID;
      this.localJournal = localJournal;
      this.replicationManager = replicationManager;
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendAddRecord(long, byte, byte[], boolean)
    */
   public void appendAddRecord(final long id, final byte recordType, final byte[] record, final boolean sync) throws Exception
   {
      this.appendAddRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   public void appendAddRecord(final long id, final byte recordType, final EncodingSupport record, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("Append record id = " + id + " recordType = " + recordType);
      }
      replicationManager.appendUpdateRecord(journalID, ADD_OPERATION_TYPE.ADD, id, recordType, record);
      localJournal.appendAddRecord(id, recordType, record, sync);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendAddRecord(long, byte, org.apache.activemq6.core.journal.EncodingSupport, boolean)
    */
   public void appendAddRecord(final long id,
                               final byte recordType,
                               final EncodingSupport record,
                               final boolean sync,
                               final IOCompletion completionCallback) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("Append record id = " + id + " recordType = " + recordType);
      }
      replicationManager.appendUpdateRecord(journalID, ADD_OPERATION_TYPE.ADD, id, recordType, record);
      localJournal.appendAddRecord(id, recordType, record, sync, completionCallback);
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendAddRecordTransactional(long, long, byte, byte[])
    */
   public void appendAddRecordTransactional(final long txID, final long id, final byte recordType, final byte[] record) throws Exception
   {
      this.appendAddRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendAddRecordTransactional(long, long, byte, org.apache.activemq6.core.journal.EncodingSupport)
    */
   public void appendAddRecordTransactional(final long txID,
                                            final long id,
                                            final byte recordType,
                                            final EncodingSupport record) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("Append record TXid = " + id + " recordType = " + recordType);
      }
      replicationManager.appendAddRecordTransactional(journalID, ADD_OPERATION_TYPE.ADD, txID, id, recordType, record);
      localJournal.appendAddRecordTransactional(txID, id, recordType, record);
   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendCommitRecord(long, boolean)
    */
   public void appendCommitRecord(final long txID, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendCommit " + txID);
      }
      replicationManager.appendCommitRecord(journalID, txID, sync, true);
      localJournal.appendCommitRecord(txID, sync);
   }

   public void appendCommitRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendCommit " + txID);
      }
      replicationManager.appendCommitRecord(journalID, txID, sync, true);
      localJournal.appendCommitRecord(txID, sync, callback);
   }

   public void appendCommitRecord(long txID, boolean sync, IOCompletion callback, boolean lineUpContext) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendCommit " + txID);
      }
      replicationManager.appendCommitRecord(journalID, txID, sync, lineUpContext);
      localJournal.appendCommitRecord(txID, sync, callback, lineUpContext);
   }


   /**
    * @param id
    * @param sync
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendDeleteRecord(long, boolean)
    */
   @Override
   public void appendDeleteRecord(final long id, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendDelete " + id);
      }
      replicationManager.appendDeleteRecord(journalID, id);
      localJournal.appendDeleteRecord(id, sync);
   }

   @Override
   public void appendDeleteRecord(final long id, final boolean sync, final IOCompletion completionCallback) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
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
    * @see org.apache.activemq6.core.journal.Journal#appendDeleteRecordTransactional(long, long, byte[])
    */
   public void appendDeleteRecordTransactional(final long txID, final long id, final byte[] record) throws Exception
   {
      this.appendDeleteRecordTransactional(txID, id, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param record
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendDeleteRecordTransactional(long, long, org.apache.activemq6.core.journal.EncodingSupport)
    */
   public void appendDeleteRecordTransactional(final long txID, final long id, final EncodingSupport record) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendDelete txID=" + txID + " id=" + id);
      }
      replicationManager.appendDeleteRecordTransactional(journalID, txID, id, record);
      localJournal.appendDeleteRecordTransactional(txID, id, record);
   }

   /**
    * @param txID
    * @param id
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendDeleteRecordTransactional(long, long)
    */
   public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
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
    * @see org.apache.activemq6.core.journal.Journal#appendPrepareRecord(long, byte[], boolean)
    */
   public void appendPrepareRecord(final long txID, final byte[] transactionData, final boolean sync) throws Exception
   {
      this.appendPrepareRecord(txID, new ByteArrayEncoding(transactionData), sync);
   }

   /**
    * @param txID
    * @param transactionData
    * @param sync
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendPrepareRecord(long, org.apache.activemq6.core.journal.EncodingSupport, boolean)
    */
   public void appendPrepareRecord(final long txID, final EncodingSupport transactionData, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendPrepare txID=" + txID);
      }
      replicationManager.appendPrepareRecord(journalID, txID, transactionData);
      localJournal.appendPrepareRecord(txID, transactionData, sync);
   }

   @Override
   public void appendPrepareRecord(final long txID,
                                   final EncodingSupport transactionData,
                                   final boolean sync,
                                   final IOCompletion callback) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendPrepare txID=" + txID);
      }
      replicationManager.appendPrepareRecord(journalID, txID, transactionData);
      localJournal.appendPrepareRecord(txID, transactionData, sync, callback);
   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendRollbackRecord(long, boolean)
    */
   public void appendRollbackRecord(final long txID, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendRollback " + txID);
      }
      replicationManager.appendRollbackRecord(journalID, txID);
      localJournal.appendRollbackRecord(txID, sync);
   }

   public void appendRollbackRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
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
    * @see org.apache.activemq6.core.journal.Journal#appendUpdateRecord(long, byte, byte[], boolean)
    */
   public void appendUpdateRecord(final long id, final byte recordType, final byte[] record, final boolean sync) throws Exception
   {
      this.appendUpdateRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendUpdateRecord(long, byte, org.apache.activemq6.core.journal.EncodingSupport, boolean)
    */
   @Override
   public void appendUpdateRecord(final long id, final byte recordType, final EncodingSupport record, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendUpdateRecord id = " + id + " , recordType = " + recordType);
      }
      replicationManager.appendUpdateRecord(journalID, ADD_OPERATION_TYPE.UPDATE, id, recordType, record);
      localJournal.appendUpdateRecord(id, recordType, record, sync);
   }

   @Override
   public void appendUpdateRecord(final long id,
                                  final byte journalRecordType,
                                  final EncodingSupport record,
                                  final boolean sync,
                                  final IOCompletion completionCallback) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendUpdateRecord id = " + id + " , recordType = " + journalRecordType);
      }
      replicationManager.appendUpdateRecord(journalID, ADD_OPERATION_TYPE.UPDATE, id, journalRecordType, record);
      localJournal.appendUpdateRecord(id, journalRecordType, record, sync, completionCallback);
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, byte[])
    */
   public void appendUpdateRecordTransactional(final long txID, final long id, final byte recordType,
                                               final byte[] record) throws Exception
   {
      this.appendUpdateRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, org.apache.activemq6.core.journal.EncodingSupport)
    */
   public void appendUpdateRecordTransactional(final long txID, final long id, final byte recordType,
                                               final EncodingSupport record) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendUpdateRecord txid=" + txID + " id = " + id + " , recordType = " + recordType);
      }
      replicationManager.appendAddRecordTransactional(journalID, ADD_OPERATION_TYPE.UPDATE, txID, id, recordType,
                                                      record);
      localJournal.appendUpdateRecordTransactional(txID, id, recordType, record);
   }

   /**
    * @param committedRecords
    * @param preparedTransactions
    * @param transactionFailure
    *
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#load(java.util.List, java.util.List, org.apache.activemq6.core.journal.TransactionFailureCallback)
    */
   public JournalLoadInformation load(final List<RecordInfo> committedRecords,
                                      final List<PreparedTransactionInfo> preparedTransactions,
                                      final TransactionFailureCallback transactionFailure) throws Exception
   {
      return localJournal.load(committedRecords, preparedTransactions, transactionFailure);
   }

   /**
    * @param reloadManager
    *
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#load(org.apache.activemq6.core.journal.LoaderCallback)
    */
   public JournalLoadInformation load(final LoaderCallback reloadManager) throws Exception
   {
      return localJournal.load(reloadManager);
   }

   /**
    * @param pages
    * @throws Exception
    * @see org.apache.activemq6.core.journal.Journal#perfBlast(int)
    */
   public void perfBlast(final int pages)
   {
      localJournal.perfBlast(pages);
   }

   /**
    * @throws Exception
    * @see org.apache.activemq6.core.server.HornetQComponent#start()
    */
   public void start() throws Exception
   {
      localJournal.start();
   }

   /**
    * @throws Exception
    * @see org.apache.activemq6.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      localJournal.stop();
   }

   public int getAlignment() throws Exception
   {
      return localJournal.getAlignment();
   }

   public boolean isStarted()
   {
      return localJournal.isStarted();
   }

   @Override
   public JournalLoadInformation loadInternalOnly() throws Exception
   {
      return localJournal.loadInternalOnly();
   }

   public int getNumberOfRecords()
   {
      return localJournal.getNumberOfRecords();
   }

   public void runDirectJournalBlast() throws Exception
   {
      localJournal.runDirectJournalBlast();
   }

   public int getUserVersion()
   {
      return localJournal.getUserVersion();
   }

   public void lineUpContext(IOCompletion callback)
   {
      ((OperationContext)callback).replicationLineUp();
      localJournal.lineUpContext(callback);
   }

   @Override
   public JournalLoadInformation loadSyncOnly(JournalState state) throws Exception
   {
      return localJournal.loadSyncOnly(state);
   }

   @Override
   public Map<Long, JournalFile> createFilesForBackupSync(long[] fileIds) throws Exception
   {
      throw new UnsupportedOperationException("This method should only be called at a replicating backup");
   }

   @Override
   public void synchronizationLock()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void synchronizationUnlock()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void forceMoveNextFile()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public JournalFile[] getDataFiles()
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public SequentialFileFactory getFileFactory()
   {
      throw new UnsupportedOperationException();
   }

   public int getFileSize()
   {
      return localJournal.getFileSize();
   }

   @Override
   public void scheduleCompactAndBlock(int timeout) throws Exception
   {
      localJournal.scheduleCompactAndBlock(timeout);
   }

   @Override
   public void replicationSyncPreserveOldFiles()
   {
      throw new UnsupportedOperationException("should never get called");
   }

   @Override
   public void replicationSyncFinished()
   {
      throw new UnsupportedOperationException("should never get called");
   }
}
