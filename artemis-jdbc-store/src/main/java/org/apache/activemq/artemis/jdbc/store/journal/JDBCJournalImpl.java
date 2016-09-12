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

package org.apache.activemq.artemis.jdbc.store.journal;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.LoaderCallback;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.SimpleWaitIOCallback;
import org.apache.activemq.artemis.jdbc.store.drivers.AbstractJDBCDriver;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.jboss.logging.Logger;

public class JDBCJournalImpl extends AbstractJDBCDriver implements Journal {


   private static final Logger logger = Logger.getLogger(JDBCJournalImpl.class);

   // Sync Delay in ms
   public static final int SYNC_DELAY = 5;

   private static int USER_VERSION = 1;

   private final List<JDBCJournalRecord> records;

   private PreparedStatement insertJournalRecords;

   private PreparedStatement selectJournalRecords;

   private PreparedStatement countJournalRecords;

   private PreparedStatement deleteJournalRecords;

   private PreparedStatement deleteJournalTxRecords;

   private boolean started;

   private JDBCJournalSync syncTimer;

   private final Executor completeExecutor;

   private final Object journalLock = new Object();

   private final ScheduledExecutorService scheduledExecutorService;

   // Track Tx Records
   private Map<Long, TransactionHolder> transactions = new ConcurrentHashMap<>();

   // Sequence ID for journal records
   private AtomicLong seq = new AtomicLong(0);

   public JDBCJournalImpl(String jdbcUrl, String tableName, String jdbcDriverClass, ScheduledExecutorService scheduledExecutorService, Executor completeExecutor) {
      super(tableName, jdbcUrl, jdbcDriverClass);
      records = new ArrayList<>();
      this.scheduledExecutorService = scheduledExecutorService;
      this.completeExecutor = completeExecutor;
   }

   @Override
   public void start() throws Exception {
      super.start();
      syncTimer = new JDBCJournalSync(scheduledExecutorService, completeExecutor, SYNC_DELAY, TimeUnit.MILLISECONDS, this);
      started = true;
   }

   @Override
   protected void createSchema() throws SQLException {
      createTable(sqlProvider.getCreateJournalTableSQL());
   }

   @Override
   protected void prepareStatements() throws SQLException {
      logger.tracef("preparing statements");
      insertJournalRecords = connection.prepareStatement(sqlProvider.getInsertJournalRecordsSQL());
      selectJournalRecords = connection.prepareStatement(sqlProvider.getSelectJournalRecordsSQL());
      countJournalRecords = connection.prepareStatement(sqlProvider.getCountJournalRecordsSQL());
      deleteJournalRecords = connection.prepareStatement(sqlProvider.getDeleteJournalRecordsSQL());
      deleteJournalTxRecords = connection.prepareStatement(sqlProvider.getDeleteJournalTxRecordsSQL());
   }

   @Override
   public synchronized void stop() throws SQLException {
      if (started) {
         synchronized (journalLock) {
            sync();
            started = false;
            super.stop();
         }
      }
   }

   @Override
   public synchronized void destroy() throws Exception {
      super.destroy();
      stop();
   }

   public synchronized int sync() {
      if (!started)
         return 0;

      List<JDBCJournalRecord> recordRef;
      synchronized (records) {
         if (records.isEmpty()) {
            return 0;
         }
         recordRef = new ArrayList<>(records);
         records.clear();
      }

      // We keep a list of deleted records and committed tx (used for cleaning up old transaction data).
      List<Long> deletedRecords = new ArrayList<>();
      List<Long> committedTransactions = new ArrayList<>();

      TransactionHolder holder;

      boolean success = false;
      try {
         for (JDBCJournalRecord record : recordRef) {
            record.storeLineUp();

            switch (record.getRecordType()) {
               case JDBCJournalRecord.DELETE_RECORD:
                  // Standard SQL Delete Record, Non transactional delete
                  deletedRecords.add(record.getId());
                  record.writeDeleteRecord(deleteJournalRecords);
                  break;
               case JDBCJournalRecord.ROLLBACK_RECORD:
                  // Roll back we remove all records associated with this TX ID.  This query is always performed last.
                  deleteJournalTxRecords.setLong(1, record.getTxId());
                  deleteJournalTxRecords.addBatch();
                  break;
               case JDBCJournalRecord.COMMIT_RECORD:
                  // We perform all the deletes and add the commit record in the same Database TX
                  holder = transactions.get(record.getTxId());
                  for (RecordInfo info : holder.recordsToDelete) {
                     deletedRecords.add(record.getId());
                     deleteJournalRecords.setLong(1, info.id);
                     deleteJournalRecords.addBatch();
                  }
                  record.writeRecord(insertJournalRecords);
                  committedTransactions.add(record.getTxId());
                  break;
               default:
                  // Default we add a new record to the DB
                  record.writeRecord(insertJournalRecords);
                  break;
            }
         }
      }
      catch (SQLException e) {
         executeCallbacks(recordRef, success);
         return 0;
      }

      try {
         connection.setAutoCommit(false);

         insertJournalRecords.executeBatch();
         deleteJournalRecords.executeBatch();
         deleteJournalTxRecords.executeBatch();

         connection.commit();
         success = true;
      }
      catch (SQLException e) {
         performRollback(recordRef);
      }

      try {
         if (success)
            cleanupTxRecords(deletedRecords, committedTransactions);
      }
      catch (SQLException e) {
         e.printStackTrace();
      }

      executeCallbacks(recordRef, success);
      return recordRef.size();
   }

   /* We store Transaction reference in memory (once all records associated with a Tranascation are Deleted,
      we remove the Tx Records (i.e. PREPARE, COMMIT). */
   private synchronized void cleanupTxRecords(List<Long> deletedRecords, List<Long> committedTx) throws SQLException {
      connection.rollback();
      List<RecordInfo> iterableCopy;
      List<TransactionHolder> iterableCopyTx = new ArrayList<>();
      iterableCopyTx.addAll(transactions.values());

      for (Long txId : committedTx) {
         transactions.get(txId).committed = true;
      }

      // TODO (mtaylor) perhaps we could store a reverse mapping of IDs to prevent this O(n) loop
      for (TransactionHolder h : iterableCopyTx) {

         iterableCopy = new ArrayList<>();
         iterableCopy.addAll(h.recordInfos);

         for (RecordInfo info : iterableCopy) {
            if (deletedRecords.contains(info.id)) {
               h.recordInfos.remove(info);
            }
         }

         if (h.recordInfos.isEmpty() && h.committed) {
            deleteJournalTxRecords.setLong(1, h.transactionID);
            deleteJournalTxRecords.addBatch();
            transactions.remove(h.transactionID);
         }
      }
   }

   private void performRollback(List<JDBCJournalRecord> records) {
      try {
         for (JDBCJournalRecord record : records) {
            if (record.isTransactional() || record.getRecordType() == JDBCJournalRecord.PREPARE_RECORD) {
               removeTxRecord(record);
            }
         }

         List<TransactionHolder> txHolders = new ArrayList<>();
         txHolders.addAll(transactions.values());

         // On rollback we must update the tx map to remove all the tx entries
         for (TransactionHolder txH : txHolders) {
            if (!txH.prepared && txH.recordInfos.isEmpty() && txH.recordsToDelete.isEmpty()) {
               transactions.remove(txH.transactionID);
            }
         }
      }
      catch (Exception sqlE) {
         ActiveMQJournalLogger.LOGGER.error("Error performing rollback", sqlE);
      }
   }

   // TODO Use an executor.
   private void executeCallbacks(final List<JDBCJournalRecord> records, final boolean result) {
      Runnable r = new Runnable() {
         @Override
         public void run() {
            for (JDBCJournalRecord record : records) {
               record.complete(result);
            }
         }
      };
      completeExecutor.execute(r);
   }

   private void appendRecord(JDBCJournalRecord record) throws Exception {

      SimpleWaitIOCallback callback = null;
      if (record.isSync() && record.getIoCompletion() == null && !record.isTransactional()) {
         callback = new SimpleWaitIOCallback();
         record.setIoCompletion(callback);
      }

      synchronized (journalLock) {
         if (record.isTransactional() || record.getRecordType() == JDBCJournalRecord.PREPARE_RECORD) {
            addTxRecord(record);
         }

         synchronized (records) {
            records.add(record);
         }
      }

      syncTimer.delay();

      if (callback != null)
         callback.waitCompletion();
   }

   private synchronized void addTxRecord(JDBCJournalRecord record) {
      TransactionHolder txHolder = transactions.get(record.getTxId());
      if (txHolder == null) {
         txHolder = new TransactionHolder(record.getTxId());
         transactions.put(record.getTxId(), txHolder);
      }

      // We actually only need the record ID in this instance.
      if (record.isTransactional()) {
         RecordInfo info = new RecordInfo(record.getId(), record.getRecordType(), new byte[0], record.isUpdate(), record.getCompactCount());
         if (record.getRecordType() == JDBCJournalRecord.DELETE_RECORD_TX) {
            txHolder.recordsToDelete.add(info);
         }
         else {
            txHolder.recordInfos.add(info);
         }
      }
      else {
         txHolder.prepared = true;
      }
   }

   private synchronized void removeTxRecord(JDBCJournalRecord record) {
      TransactionHolder txHolder = transactions.get(record.getTxId());

      // We actually only need the record ID in this instance.
      if (record.isTransactional()) {
         RecordInfo info = new RecordInfo(record.getTxId(), record.getRecordType(), new byte[0], record.isUpdate(), record.getCompactCount());
         if (record.getRecordType() == JDBCJournalRecord.DELETE_RECORD_TX) {
            txHolder.recordsToDelete.remove(info);
         }
         else {
            txHolder.recordInfos.remove(info);
         }
      }
      else {
         txHolder.prepared = false;
      }
   }

   @Override
   public void appendAddRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD, seq.incrementAndGet());
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD, seq.incrementAndGet());
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendAddRecord(long id,
                               byte recordType,
                               EncodingSupport record,
                               boolean sync,
                               IOCompletion completionCallback) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD, seq.incrementAndGet());
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setSync(sync);
      r.setIoCompletion(completionCallback);
      appendRecord(r);
   }

   @Override
   public void appendUpdateRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.UPDATE_RECORD, seq.incrementAndGet());
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.UPDATE_RECORD, seq.incrementAndGet());
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendUpdateRecord(long id,
                                  byte recordType,
                                  EncodingSupport record,
                                  boolean sync,
                                  IOCompletion completionCallback) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD, seq.incrementAndGet());
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setSync(sync);
      r.setIoCompletion(completionCallback);
      appendRecord(r);
   }

   @Override
   public void appendDeleteRecord(long id, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.DELETE_RECORD, seq.incrementAndGet());
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendDeleteRecord(long id, boolean sync, IOCompletion completionCallback) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.DELETE_RECORD, seq.incrementAndGet());
      r.setSync(sync);
      r.setIoCompletion(completionCallback);
      appendRecord(r);
   }

   @Override
   public void appendAddRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD_TX, seq.incrementAndGet());
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendAddRecordTransactional(long txID,
                                            long id,
                                            byte recordType,
                                            EncodingSupport record) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD_TX, seq.incrementAndGet());
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendUpdateRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.UPDATE_RECORD_TX, seq.incrementAndGet());
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendUpdateRecordTransactional(long txID,
                                               long id,
                                               byte recordType,
                                               EncodingSupport record) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.UPDATE_RECORD_TX, seq.incrementAndGet());
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id, byte[] record) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.DELETE_RECORD_TX, seq.incrementAndGet());
      r.setRecord(record);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.DELETE_RECORD_TX, seq.incrementAndGet());
      r.setRecord(record);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.DELETE_RECORD_TX, seq.incrementAndGet());
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendCommitRecord(long txID, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(-1, JDBCJournalRecord.COMMIT_RECORD, seq.incrementAndGet());
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendCommitRecord(long txID, boolean sync, IOCompletion callback) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(-1, JDBCJournalRecord.COMMIT_RECORD, seq.incrementAndGet());
      r.setTxId(txID);
      r.setIoCompletion(callback);
      appendRecord(r);
   }

   @Override
   public void appendCommitRecord(long txID,
                                  boolean sync,
                                  IOCompletion callback,
                                  boolean lineUpContext) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(-1, JDBCJournalRecord.COMMIT_RECORD, seq.incrementAndGet());
      r.setTxId(txID);
      r.setStoreLineUp(lineUpContext);
      r.setIoCompletion(callback);
      appendRecord(r);
   }

   @Override
   public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(-1, JDBCJournalRecord.PREPARE_RECORD, seq.incrementAndGet());
      r.setTxId(txID);
      r.setTxData(transactionData);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendPrepareRecord(long txID,
                                   EncodingSupport transactionData,
                                   boolean sync,
                                   IOCompletion callback) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.PREPARE_RECORD, seq.incrementAndGet());
      r.setTxId(txID);
      r.setTxData(transactionData);
      r.setTxData(transactionData);
      r.setSync(sync);
      r.setIoCompletion(callback);
      appendRecord(r);
   }

   @Override
   public void appendPrepareRecord(long txID, byte[] transactionData, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.PREPARE_RECORD, seq.incrementAndGet());
      r.setTxId(txID);
      r.setTxData(transactionData);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendRollbackRecord(long txID, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.ROLLBACK_RECORD, seq.incrementAndGet());
      r.setTxId(txID);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendRollbackRecord(long txID, boolean sync, IOCompletion callback) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.ROLLBACK_RECORD, seq.incrementAndGet());
      r.setTxId(txID);
      r.setSync(sync);
      r.setIoCompletion(callback);
      appendRecord(r);
   }

   @Override
   public synchronized JournalLoadInformation load(LoaderCallback reloadManager) throws Exception {
      JournalLoadInformation jli = new JournalLoadInformation();
      JDBCJournalReaderCallback jrc = new JDBCJournalReaderCallback(reloadManager);
      JDBCJournalRecord r;

      try (ResultSet rs = selectJournalRecords.executeQuery()) {
         int noRecords = 0;
         while (rs.next()) {
            r = JDBCJournalRecord.readRecord(rs);
            switch (r.getRecordType()) {
               case JDBCJournalRecord.ADD_RECORD:
                  jrc.onReadAddRecord(r.toRecordInfo());
                  break;
               case JDBCJournalRecord.UPDATE_RECORD:
                  jrc.onReadUpdateRecord(r.toRecordInfo());
                  break;
               case JDBCJournalRecord.DELETE_RECORD:
                  jrc.onReadDeleteRecord(r.getId());
                  break;
               case JDBCJournalRecord.ADD_RECORD_TX:
                  jrc.onReadAddRecordTX(r.getTxId(), r.toRecordInfo());
                  break;
               case JDBCJournalRecord.UPDATE_RECORD_TX:
                  jrc.onReadUpdateRecordTX(r.getTxId(), r.toRecordInfo());
                  break;
               case JDBCJournalRecord.DELETE_RECORD_TX:
                  jrc.onReadDeleteRecordTX(r.getTxId(), r.toRecordInfo());
                  break;
               case JDBCJournalRecord.PREPARE_RECORD:
                  jrc.onReadPrepareRecord(r.getTxId(), r.getTxDataAsByteArray(), r.getTxCheckNoRecords());
                  break;
               case JDBCJournalRecord.COMMIT_RECORD:
                  jrc.onReadCommitRecord(r.getTxId(), r.getTxCheckNoRecords());
                  break;
               case JDBCJournalRecord.ROLLBACK_RECORD:
                  jrc.onReadRollbackRecord(r.getTxId());
                  break;
               default:
                  throw new Exception("Error Reading Journal, Unknown Record Type: " + r.getRecordType());
            }
            noRecords++;
            if (r.getSeq() > seq.longValue()) {
               seq.set(r.getSeq());
            }
         }
         jrc.checkPreparedTx();

         jli.setMaxID(((JDBCJournalLoaderCallback) reloadManager).getMaxId());
         jli.setNumberOfRecords(noRecords);
         transactions = jrc.getTransactions();
      }
      return jli;
   }

   @Override
   public JournalLoadInformation loadInternalOnly() throws Exception {
      return null;
   }

   @Override
   public JournalLoadInformation loadSyncOnly(JournalState state) throws Exception {
      return null;
   }

   @Override
   public void lineUpContext(IOCompletion callback) {
      callback.storeLineUp();
   }

   @Override
   public JournalLoadInformation load(List<RecordInfo> committedRecords,
                                      List<PreparedTransactionInfo> preparedTransactions,
                                      TransactionFailureCallback transactionFailure) throws Exception {
      return load(committedRecords, preparedTransactions, transactionFailure, true);
   }

   public synchronized JournalLoadInformation load(final List<RecordInfo> committedRecords,
                                                   final List<PreparedTransactionInfo> preparedTransactions,
                                                   final TransactionFailureCallback failureCallback,
                                                   final boolean fixBadTX) throws Exception {
      JDBCJournalLoaderCallback lc = new JDBCJournalLoaderCallback(committedRecords, preparedTransactions, failureCallback, fixBadTX);
      return load(lc);
   }

   @Override
   public int getAlignment() throws Exception {
      return 0;
   }

   @Override
   public int getNumberOfRecords() {
      int count = 0;
      try (ResultSet rs = countJournalRecords.executeQuery()) {
         rs.next();
         count = rs.getInt(1);
      }
      catch (SQLException e) {
         return -1;
      }
      return count;
   }

   @Override
   public int getUserVersion() {
      return USER_VERSION;
   }

   @Override
   public void perfBlast(int pages) {
   }

   @Override
   public void runDirectJournalBlast() throws Exception {
   }

   @Override
   public Map<Long, JournalFile> createFilesForBackupSync(long[] fileIds) throws Exception {
      return null;
   }

   @Override
   public final void synchronizationLock() {
      logger.error("Replication is not supported with JDBC Store", new Exception("trace"));
   }

   @Override
   public final void synchronizationUnlock() {
      logger.error("Replication is not supported with JDBC Store", new Exception("trace"));
   }

   @Override
   public void forceMoveNextFile() throws Exception {
   }

   @Override
   public JournalFile[] getDataFiles() {
      return new JournalFile[0];
   }

   @Override
   public SequentialFileFactory getFileFactory() {
      return null;
   }

   @Override
   public int getFileSize() {
      return 0;
   }

   @Override
   public void scheduleCompactAndBlock(int timeout) throws Exception {

   }

   @Override
   public void replicationSyncPreserveOldFiles() {

   }

   @Override
   public void replicationSyncFinished() {

   }

   @Override
   public boolean isStarted() {
      return started;
   }

}
