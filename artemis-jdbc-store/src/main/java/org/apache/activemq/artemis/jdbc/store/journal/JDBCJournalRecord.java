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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.apache.activemq.artemis.utils.ActiveMQBufferInputStream;

public class JDBCJournalRecord {
   /*
   Database Table Schema:

   id BIGINT (long)
   recordType SMALLINT (byte)
   compactCount SMALLINT (byte)
   txId BIGINT (long)
   userRecordType SMALLINT (byte)
   variableSize INT (int)
   record BLOB (InputStream)
   txDataSize INT (int)
   txData BLOB (InputStream)
   txCheckNoRecords INT (int)
   */

   // Record types taken from Journal Impl
   public static final byte ADD_RECORD = 11;
   public static final byte UPDATE_RECORD = 12;
   public static final byte ADD_RECORD_TX = 13;
   public static final byte UPDATE_RECORD_TX = 14;

   public static final byte DELETE_RECORD_TX = 15;
   public static final byte DELETE_RECORD = 16;

   public static final byte PREPARE_RECORD = 17;
   public static final byte COMMIT_RECORD = 18;
   public static final byte ROLLBACK_RECORD = 19;

   // Callback and sync operations
   private IOCompletion ioCompletion = null;
   private boolean storeLineUp = true;
   private boolean sync = false;

   // DB Fields for all records
   private Long id;
   private byte recordType;
   private byte compactCount;
   private long txId;

   // DB fields for ADD_RECORD(TX), UPDATE_RECORD(TX),
   private int variableSize;
   protected byte userRecordType;
   private InputStream record;

   // DB Fields for PREPARE_RECORD
   private int txDataSize;
   private InputStream txData;

   // DB Fields for COMMIT_RECORD and PREPARE_RECORD
   private int txCheckNoRecords;

   private boolean isUpdate;

   private boolean isTransactional;

   public JDBCJournalRecord(long id, byte recordType) {
      this.id = id;
      this.recordType = recordType;

      isUpdate = recordType == UPDATE_RECORD || recordType == UPDATE_RECORD_TX;
      isTransactional = recordType == UPDATE_RECORD_TX || recordType == ADD_RECORD_TX || recordType == DELETE_RECORD_TX;

      // set defaults
      compactCount = 0;
      txId = 0;
      variableSize = 0;
      userRecordType = -1;
      record = new ByteArrayInputStream(new byte[0]);
      txDataSize = 0;
      txData = new ByteArrayInputStream(new byte[0]);
      txCheckNoRecords = 0;
   }

   public static String createTableSQL(String tableName) {
      return "CREATE TABLE " + tableName + "(id BIGINT,recordType SMALLINT,compactCount SMALLINT,txId BIGINT,userRecordType SMALLINT,variableSize INTEGER,record BLOB,txDataSize INTEGER,txData BLOB,txCheckNoRecords INTEGER,timestamp BIGINT)";
   }

   public static String insertRecordsSQL(String tableName) {
      return "INSERT INTO " + tableName + "(id,recordType,compactCount,txId,userRecordType,variableSize,record,txDataSize,txData,txCheckNoRecords,timestamp) "
         + "VALUES (?,?,?,?,?,?,?,?,?,?,?)";
   }

   public static String selectRecordsSQL(String tableName) {
      return "SELECT id," + "recordType," + "compactCount," + "txId," + "userRecordType," + "variableSize," + "record," + "txDataSize," + "txData," + "txCheckNoRecords " + "FROM " + tableName;
   }

   public static String deleteRecordsSQL(String tableName) {
      return "DELETE FROM " + tableName + " WHERE id = ?";
   }

   public static String deleteCommittedDeleteRecordsForTxSQL(String tableName) {
      return "DELETE FROM " + tableName + " WHERE id IN (SELECT id FROM " + tableName + " WHERE txID=?)";
   }

   public static String deleteCommittedTxRecordsSQL(String tableName) {
      return "DELETE FROM " + tableName + " WHERE txId=? AND (recordType=" + PREPARE_RECORD + " OR recordType=" + COMMIT_RECORD + ")";
   }

   public static String deleteJournalTxRecordsSQL(String tableName) {
      return "DELETE FROM " + tableName + " WHERE txId=?";
   }

   public static String deleteRolledBackTxSQL(String tableName) {
      return "DELETE FROM " + tableName + " WHERE txId=?";
   }

   public void complete(boolean success) {
      if (ioCompletion != null) {
         if (success) {
            ioCompletion.done();
         }
         else {
            ioCompletion.onError(1, "DATABASE TRANSACTION FAILED");
         }
      }
   }

   public void storeLineUp() {
      if (storeLineUp && ioCompletion != null) {
         ioCompletion.storeLineUp();
      }
   }

   protected void writeRecord(PreparedStatement statement) throws SQLException {

      byte[] recordBytes = new byte[variableSize];
      byte[] txDataBytes = new byte[txDataSize];

      try {
         record.read(recordBytes);
         txData.read(txDataBytes);
      }
      catch (IOException e) {
         ActiveMQJournalLogger.LOGGER.error("Error occurred whilst reading Journal Record", e);
      }

      statement.setLong(1, id);
      statement.setByte(2, recordType);
      statement.setByte(3, compactCount);
      statement.setLong(4, txId);
      statement.setByte(5, userRecordType);
      statement.setInt(6, variableSize);
      statement.setBytes(7, recordBytes);
      statement.setInt(8, txDataSize);
      statement.setBytes(9, txDataBytes);
      statement.setInt(10, txCheckNoRecords);
      statement.setLong(11, System.currentTimeMillis());
      statement.addBatch();
   }

   protected void writeDeleteTxRecord(PreparedStatement deleteTxStatement) throws SQLException {
      deleteTxStatement.setLong(1, txId);
      deleteTxStatement.addBatch();
   }

   protected void writeDeleteRecord(PreparedStatement deleteStatement) throws SQLException {
      deleteStatement.setLong(1, id);
      deleteStatement.addBatch();
   }

   public static JDBCJournalRecord readRecord(ResultSet rs) throws SQLException {
      JDBCJournalRecord record = new JDBCJournalRecord(rs.getLong(1), (byte) rs.getShort(2));
      record.setCompactCount((byte) rs.getShort(3));
      record.setTxId(rs.getLong(4));
      record.setUserRecordType((byte) rs.getShort(5));
      record.setVariableSize(rs.getInt(6));
      record.setRecord(rs.getBytes(7));
      record.setTxDataSize(rs.getInt(8));
      record.setTxData(rs.getBytes(9));
      record.setTxCheckNoRecords(rs.getInt(10));
      return record;
   }

   public IOCompletion getIoCompletion() {
      return ioCompletion;
   }

   public void setIoCompletion(IOCompletion ioCompletion) {
      this.ioCompletion = ioCompletion;
   }

   public boolean isStoreLineUp() {
      return storeLineUp;
   }

   public void setStoreLineUp(boolean storeLineUp) {
      this.storeLineUp = storeLineUp;
   }

   public boolean isSync() {
      return sync;
   }

   public void setSync(boolean sync) {
      this.sync = sync;
   }

   public Long getId() {
      return id;
   }

   public byte getRecordType() {
      return recordType;
   }

   public byte getCompactCount() {
      return compactCount;
   }

   public void setCompactCount(byte compactCount) {
      this.compactCount = compactCount;
   }

   public long getTxId() {
      return txId;
   }

   public void setTxId(long txId) {
      this.txId = txId;
   }

   public int getVariableSize() {
      return variableSize;
   }

   public void setVariableSize(int variableSize) {
      this.variableSize = variableSize;
   }

   public byte getUserRecordType() {
      return userRecordType;
   }

   public void setUserRecordType(byte userRecordType) {
      this.userRecordType = userRecordType;
   }

   public void setRecord(byte[] record) {
      if (record != null) {
         this.variableSize = record.length;
         this.record = new ByteArrayInputStream(record);
      }
   }

   public void setRecord(InputStream record) {
      this.record = record;
   }

   public void setRecord(EncodingSupport record) {
      this.variableSize = record.getEncodeSize();

      ActiveMQBuffer encodedBuffer = ActiveMQBuffers.fixedBuffer(variableSize);
      record.encode(encodedBuffer);
      this.record = new ActiveMQBufferInputStream(encodedBuffer);
   }

   public InputStream getRecord() {
      return record;
   }

   public int getTxCheckNoRecords() {
      return txCheckNoRecords;
   }

   public void setTxCheckNoRecords(int txCheckNoRecords) {
      this.txCheckNoRecords = txCheckNoRecords;
   }

   public void setTxDataSize(int txDataSize) {
      this.txDataSize = txDataSize;
   }

   public int getTxDataSize() {
      return txDataSize;
   }

   public InputStream getTxData() {
      return txData;
   }

   public void setTxData(InputStream record) {
      this.record = record;
   }

   public void setTxData(EncodingSupport txData) {
      this.txDataSize = txData.getEncodeSize();

      ActiveMQBuffer encodedBuffer = ActiveMQBuffers.fixedBuffer(txDataSize);
      txData.encode(encodedBuffer);
      this.txData = new ActiveMQBufferInputStream(encodedBuffer);
   }

   public void setTxData(byte[] txData) {
      if (txData != null) {
         this.txDataSize = txData.length;
         this.txData = new ByteArrayInputStream(txData);
      }
   }

   public boolean isUpdate() {
      return isUpdate;
   }

   public byte[] getRecordData() throws IOException {
      byte[] data = new byte[variableSize];
      record.read(data);
      return data;
   }

   public byte[] getTxDataAsByteArray() throws IOException {
      byte[] data = new byte[txDataSize];
      txData.read(data);
      return data;
   }

   public RecordInfo toRecordInfo() throws IOException {
      return new RecordInfo(getId(), getUserRecordType(), getRecordData(), isUpdate(), getCompactCount());
   }

   public boolean isTransactional() {
      return isTransactional;
   }
}