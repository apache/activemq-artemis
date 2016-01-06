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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.LoaderCallback;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.derby.jdbc.AutoloadedDriver;

public class JDBCJournalImpl implements Journal {

   // Sync Delay in ms
   public static final int SYNC_DELAY = 500;

   private static int USER_VERSION = 1;

   private final String tableName;

   private Connection connection;

   private List<JDBCJournalRecord> records;

   private PreparedStatement insertJournalRecords;

   private PreparedStatement selectJournalRecords;

   private PreparedStatement countJournalRecords;

   private PreparedStatement deleteJournalRecords;

   private PreparedStatement deleteTxJournalRecords;

   private boolean started;

   private String jdbcUrl;

   private Timer syncTimer;

   private Driver dbDriver;

   private final ReadWriteLock journalLock = new ReentrantReadWriteLock();

   private boolean isDerby = false;

   public JDBCJournalImpl(String jdbcUrl, String tableName) {
      this.tableName = tableName;
      this.jdbcUrl = jdbcUrl;

      records = new ArrayList<JDBCJournalRecord>();
   }

   @Override
   public void start() throws Exception {
      // Load Database driver, sets Derby Autoloaded Driver as lowest priority.
      List<Driver> drivers = Collections.list(DriverManager.getDrivers());
      if (drivers.size() <= 2 && drivers.size() > 0) {
         dbDriver = drivers.get(0);
         isDerby = dbDriver instanceof AutoloadedDriver;

         if (drivers.size() > 1 && isDerby) {
            dbDriver = drivers.get(1);
         }

         if (isDerby) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
               @Override
               public void run() {
                  try {
                     DriverManager.getConnection("jdbc:derby:;shutdown=true");
                  }
                  catch (Exception e) {
                  }
               }
            });
         }
      }
      else {
         String error = drivers.isEmpty() ? "No DB driver found on class path" : "Too many DB drivers on class path, not sure which to use";
         throw new RuntimeException(error);
      }

      connection = dbDriver.connect(jdbcUrl, new Properties());

      // If JOURNAL table doesn't exist then create it
      ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null);
      if (!rs.next()) {
         Statement statement = connection.createStatement();
         statement.executeUpdate(JDBCJournalRecord.createTableSQL(tableName));
      }

      insertJournalRecords = connection.prepareStatement(JDBCJournalRecord.insertRecordsSQL(tableName));
      selectJournalRecords = connection.prepareStatement(JDBCJournalRecord.selectRecordsSQL(tableName));
      countJournalRecords = connection.prepareStatement("SELECT COUNT(*) FROM " + tableName);
      deleteJournalRecords = connection.prepareStatement(JDBCJournalRecord.deleteRecordsSQL(tableName));
      deleteTxJournalRecords = connection.prepareStatement(JDBCJournalRecord.deleteTxRecordsSQL(tableName));

      syncTimer = new Timer();
      syncTimer.scheduleAtFixedRate(new JDBCJournalSync(this), SYNC_DELAY * 2, SYNC_DELAY);

      started = true;
   }

   @Override
   public void stop() throws Exception {
      stop(true);
   }

   public synchronized void stop(boolean shutdownConnection) throws Exception {
      if (started) {
         syncTimer.cancel();
         sync();
         if (shutdownConnection) {
            connection.close();
         }
         started = false;
      }
   }

   public synchronized void destroy() throws Exception {
      connection.setAutoCommit(false);
      Statement statement = connection.createStatement();
      statement.executeUpdate("DROP TABLE " + tableName);
      connection.commit();
      stop();
   }

   public int sync() throws SQLException {
      List<JDBCJournalRecord> recordRef = records;
      records = new ArrayList<JDBCJournalRecord>();

      for (JDBCJournalRecord record : recordRef) {
         record.storeLineUp();

         switch (record.getRecordType()) {
            case JDBCJournalRecord.DELETE_RECORD:
               record.writeDeleteRecord(deleteJournalRecords);
               break;
            case JDBCJournalRecord.DELETE_RECORD_TX:
               record.writeDeleteTxRecord(deleteTxJournalRecords);
               break;
            default:
               record.writeRecord(insertJournalRecords);
               break;
         }
      }

      boolean success = false;
      try {
         connection.setAutoCommit(false);
         insertJournalRecords.executeBatch();
         deleteJournalRecords.executeBatch();
         deleteTxJournalRecords.executeBatch();
         connection.commit();
         success = true;
      }
      catch (SQLException e) {
         connection.rollback();
         e.printStackTrace();
      }
      executeCallbacks(recordRef, success);
      return recordRef.size();
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
      Thread t = new Thread(r);
      t.start();
   }

   private void appendRecord(JDBCJournalRecord record) throws SQLException {
      try {
         journalLock.writeLock().lock();
         records.add(record);
      }
      finally {
         journalLock.writeLock().unlock();
      }
   }

   @Override
   public void appendAddRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD);
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD);
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
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD);
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setSync(sync);
      r.setIoCompletion(completionCallback);
      appendRecord(r);
   }

   @Override
   public void appendUpdateRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.UPDATE_RECORD);
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.UPDATE_RECORD);
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
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD);
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setSync(sync);
      r.setIoCompletion(completionCallback);
      appendRecord(r);
   }

   @Override
   public void appendDeleteRecord(long id, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.DELETE_RECORD);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendDeleteRecord(long id, boolean sync, IOCompletion completionCallback) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.DELETE_RECORD);
      r.setSync(sync);
      r.setIoCompletion(completionCallback);
      appendRecord(r);
   }

   @Override
   public void appendAddRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD_TX);
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
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.ADD_RECORD_TX);
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendUpdateRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.UPDATE_RECORD_TX);
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
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.UPDATE_RECORD_TX);
      r.setUserRecordType(recordType);
      r.setRecord(record);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id, byte[] record) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.DELETE_RECORD_TX);
      r.setRecord(record);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.DELETE_RECORD_TX);
      r.setRecord(record);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendDeleteRecordTransactional(long txID, long id) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(id, JDBCJournalRecord.DELETE_RECORD_TX);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendCommitRecord(long txID, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.COMMIT_RECORD);
      r.setTxId(txID);
      appendRecord(r);
   }

   @Override
   public void appendCommitRecord(long txID, boolean sync, IOCompletion callback) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.COMMIT_RECORD);
      r.setTxId(txID);
      r.setIoCompletion(callback);
      appendRecord(r);
   }

   @Override
   public void appendCommitRecord(long txID,
                                  boolean sync,
                                  IOCompletion callback,
                                  boolean lineUpContext) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.COMMIT_RECORD);
      r.setTxId(txID);
      r.setStoreLineUp(lineUpContext);
      r.setIoCompletion(callback);
      appendRecord(r);
   }

   @Override
   public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.PREPARE_RECORD);
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
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.PREPARE_RECORD);
      r.setTxId(txID);
      r.setTxData(transactionData);
      r.setSync(sync);
      r.setIoCompletion(callback);
      appendRecord(r);
   }

   @Override
   public void appendPrepareRecord(long txID, byte[] transactionData, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.PREPARE_RECORD);
      r.setTxId(txID);
      r.setTxData(transactionData);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendRollbackRecord(long txID, boolean sync) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.ROLLBACK_RECORD);
      r.setTxId(txID);
      r.setSync(sync);
      appendRecord(r);
   }

   @Override
   public void appendRollbackRecord(long txID, boolean sync, IOCompletion callback) throws Exception {
      JDBCJournalRecord r = new JDBCJournalRecord(0, JDBCJournalRecord.PREPARE_RECORD);
      r.setTxId(txID);
      r.setSync(sync);
      r.setIoCompletion(callback);
      appendRecord(r);
   }

   @Override
   public JournalLoadInformation load(LoaderCallback reloadManager) throws Exception {
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
         }
         jli.setMaxID(((JDBCJournalLoaderCallback) reloadManager).getMaxId());
         jli.setNumberOfRecords(noRecords);
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

   public final void synchronizationLock() {
      journalLock.writeLock().lock();
   }

   public final void synchronizationUnlock() {
      journalLock.writeLock().unlock();
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
