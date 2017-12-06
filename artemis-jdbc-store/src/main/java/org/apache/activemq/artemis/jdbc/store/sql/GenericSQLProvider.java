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
package org.apache.activemq.artemis.jdbc.store.sql;

public class GenericSQLProvider implements SQLProvider {

   /**
    * The JDBC Node Manager shared state is contained in these 4 rows: each one is used exclusively for a specific purpose.
    */
   private static final int STATE_ROW_ID = 0;
   private static final int LIVE_LOCK_ROW_ID = 1;
   private static final int BACKUP_LOCK_ROW_ID = 2;
   private static final int NODE_ID_ROW_ID = 3;

   // Default to lowest (MYSQL = 64k)
   private static final long MAX_BLOB_SIZE = 64512;

   protected final String tableName;

   private final String createFileTableSQL;

   private final String insertFileSQL;

   private final String selectFileNamesByExtensionSQL;

   private final String selectIdByFileNameSQL;

   private final String appendToFileSQL;

   private final String readLargeObjectSQL;

   private final String deleteFileSQL;

   private final String updateFileNameByIdSQL;

   private final String copyFileRecordByIdSQL;

   private final String cloneFileRecordSQL;

   private final String dropFileTableSQL;

   private final String[] createJournalTableSQL;

   private final String insertJournalRecordsSQL;

   private final String selectJournalRecordsSQL;

   private final String deleteJournalRecordsSQL;

   private final String deleteJournalTxRecordsSQL;

   private final String countJournalRecordsSQL;

   private final String createNodeManagerStoreTableSQL;

   private final String createStateSQL;

   private final String createNodeIdSQL;

   private final String createLiveLockSQL;

   private final String createBackupLockSQL;

   private final String tryAcquireLiveLockSQL;

   private final String tryAcquireBackupLockSQL;

   private final String tryReleaseLiveLockSQL;

   private final String tryReleaseBackupLockSQL;

   private final String isLiveLockedSQL;

   private final String isBackupLockedSQL;

   private final String renewLiveLockSQL;

   private final String renewBackupLockSQL;

   private final String currentTimestampSQL;

   private final String writeStateSQL;

   private final String readStateSQL;

   private final String writeNodeIdSQL;

   private final String initializeNodeIdSQL;

   private final String readNodeIdSQL;

   protected final DatabaseStoreType databaseStoreType;

   protected GenericSQLProvider(String tableName, DatabaseStoreType databaseStoreType) {
      this.tableName = tableName;

      this.databaseStoreType = databaseStoreType;

      createFileTableSQL = "CREATE TABLE " + tableName + "(ID BIGINT AUTO_INCREMENT, FILENAME VARCHAR(255), EXTENSION VARCHAR(10), DATA BLOB, PRIMARY KEY(ID))";

      insertFileSQL = "INSERT INTO " + tableName + " (FILENAME, EXTENSION, DATA) VALUES (?,?,?)";

      selectFileNamesByExtensionSQL = "SELECT FILENAME, ID FROM " + tableName + " WHERE EXTENSION=?";

      selectIdByFileNameSQL = "SELECT ID, FILENAME, EXTENSION, DATA FROM " + tableName + " WHERE fileName=?";

      appendToFileSQL = "SELECT DATA FROM " + tableName + " WHERE ID=? FOR UPDATE";

      readLargeObjectSQL = "SELECT DATA FROM " + tableName + " WHERE ID=?";

      deleteFileSQL = "DELETE FROM " + tableName + " WHERE ID=?";

      updateFileNameByIdSQL = "UPDATE " + tableName + " SET FILENAME=? WHERE ID=?";

      cloneFileRecordSQL = "INSERT INTO " + tableName + "(FILENAME, EXTENSION, DATA) " + "(SELECT FILENAME, EXTENSION, DATA FROM " + tableName + " WHERE ID=?)";

      copyFileRecordByIdSQL = "UPDATE " + tableName + " SET DATA = (SELECT DATA FROM " + tableName + " WHERE ID=?) WHERE ID=?";

      dropFileTableSQL = "DROP TABLE " + tableName;

      createJournalTableSQL = new String[]{"CREATE TABLE " + tableName + "(id BIGINT,recordType SMALLINT,compactCount SMALLINT,txId BIGINT,userRecordType SMALLINT,variableSize INTEGER,record BLOB,txDataSize INTEGER,txData BLOB,txCheckNoRecords INTEGER,seq BIGINT NOT NULL, PRIMARY KEY(seq))", "CREATE INDEX " + tableName + "_IDX ON " + tableName + " (id)"};

      insertJournalRecordsSQL = "INSERT INTO " + tableName + "(id,recordType,compactCount,txId,userRecordType,variableSize,record,txDataSize,txData,txCheckNoRecords,seq) " + "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

      selectJournalRecordsSQL = "SELECT id,recordType,compactCount,txId,userRecordType,variableSize,record,txDataSize,txData,txCheckNoRecords,seq " + "FROM " + tableName + " ORDER BY seq ASC";

      deleteJournalRecordsSQL = "DELETE FROM " + tableName + " WHERE id = ?";

      deleteJournalTxRecordsSQL = "DELETE FROM " + tableName + " WHERE txId=?";

      countJournalRecordsSQL = "SELECT COUNT(*) FROM " + tableName;

      createNodeManagerStoreTableSQL = "CREATE TABLE " + tableName + " ( ID INT NOT NULL, HOLDER_ID VARCHAR(128), HOLDER_EXPIRATION_TIME TIMESTAMP, NODE_ID CHAR(36),STATE CHAR(1), PRIMARY KEY(ID))";

      createStateSQL = "INSERT INTO " + tableName + " (ID) VALUES (" + STATE_ROW_ID + ")";

      createNodeIdSQL = "INSERT INTO " + tableName + " (ID) VALUES (" + NODE_ID_ROW_ID + ")";

      createLiveLockSQL = "INSERT INTO " + tableName + " (ID) VALUES (" + LIVE_LOCK_ROW_ID + ")";

      createBackupLockSQL = "INSERT INTO " + tableName + " (ID) VALUES (" + BACKUP_LOCK_ROW_ID + ")";

      tryAcquireLiveLockSQL = "UPDATE " + tableName + " SET HOLDER_ID = ?, HOLDER_EXPIRATION_TIME = ? WHERE (HOLDER_EXPIRATION_TIME IS NULL OR HOLDER_EXPIRATION_TIME < CURRENT_TIMESTAMP) AND ID = " + LIVE_LOCK_ROW_ID;

      tryAcquireBackupLockSQL = "UPDATE " + tableName + " SET HOLDER_ID = ?, HOLDER_EXPIRATION_TIME = ? WHERE (HOLDER_EXPIRATION_TIME IS NULL OR HOLDER_EXPIRATION_TIME < CURRENT_TIMESTAMP) AND ID = " + BACKUP_LOCK_ROW_ID;

      tryReleaseLiveLockSQL = "UPDATE " + tableName + " SET HOLDER_ID = NULL, HOLDER_EXPIRATION_TIME = NULL WHERE HOLDER_ID = ? AND ID = " + LIVE_LOCK_ROW_ID;

      tryReleaseBackupLockSQL = "UPDATE " + tableName + " SET HOLDER_ID = NULL, HOLDER_EXPIRATION_TIME = NULL WHERE HOLDER_ID = ? AND ID = " + BACKUP_LOCK_ROW_ID;

      isLiveLockedSQL = "SELECT HOLDER_ID, HOLDER_EXPIRATION_TIME FROM " + tableName + " WHERE ID = " + LIVE_LOCK_ROW_ID;

      isBackupLockedSQL = "SELECT HOLDER_ID, HOLDER_EXPIRATION_TIME FROM " + tableName + " WHERE ID = " + BACKUP_LOCK_ROW_ID;

      renewLiveLockSQL = "UPDATE " + tableName + " SET HOLDER_EXPIRATION_TIME = ? WHERE HOLDER_ID = ? AND ID = " + LIVE_LOCK_ROW_ID;

      renewBackupLockSQL = "UPDATE " + tableName + " SET HOLDER_EXPIRATION_TIME = ? WHERE HOLDER_ID = ? AND ID = " + BACKUP_LOCK_ROW_ID;

      currentTimestampSQL = "SELECT CURRENT_TIMESTAMP FROM " + tableName;

      writeStateSQL = "UPDATE " + tableName + " SET STATE = ? WHERE ID = " + STATE_ROW_ID;

      readStateSQL = "SELECT STATE FROM " + tableName + " WHERE ID = " + STATE_ROW_ID;

      writeNodeIdSQL = "UPDATE " + tableName + " SET NODE_ID = ? WHERE ID = " + NODE_ID_ROW_ID;

      initializeNodeIdSQL = "UPDATE " + tableName + " SET NODE_ID = ? WHERE NODE_ID IS NULL AND ID = " + NODE_ID_ROW_ID;

      readNodeIdSQL = "SELECT NODE_ID FROM " + tableName + " WHERE ID = " + NODE_ID_ROW_ID;

   }

   @Override
   public long getMaxBlobSize() {
      return MAX_BLOB_SIZE;
   }

   @Override
   public String getTableName() {
      return tableName;
   }

   // Journal SQL Statements
   @Override
   public String[] getCreateJournalTableSQL() {
      return createJournalTableSQL;
   }

   @Override
   public String getInsertJournalRecordsSQL() {
      return insertJournalRecordsSQL;
   }

   @Override
   public String getSelectJournalRecordsSQL() {
      return selectJournalRecordsSQL;
   }

   @Override
   public String getDeleteJournalRecordsSQL() {
      return deleteJournalRecordsSQL;
   }

   @Override
   public String getDeleteJournalTxRecordsSQL() {
      return deleteJournalTxRecordsSQL;
   }

   @Override
   public String getCountJournalRecordsSQL() {
      return countJournalRecordsSQL;
   }

   // Large Message Statements
   @Override
   public String getCreateFileTableSQL() {
      return createFileTableSQL;
   }

   @Override
   public String getInsertFileSQL() {
      return insertFileSQL;
   }

   @Override
   public String getSelectFileByFileName() {
      return selectIdByFileNameSQL;
   }

   @Override
   public String getSelectFileNamesByExtensionSQL() {
      return selectFileNamesByExtensionSQL;
   }

   @Override
   public String getAppendToLargeObjectSQL() {
      return appendToFileSQL;
   }

   @Override
   public String getReadLargeObjectSQL() {
      return readLargeObjectSQL;
   }

   @Override
   public String getDeleteFileSQL() {
      return deleteFileSQL;
   }

   @Override
   public String getUpdateFileNameByIdSQL() {
      return updateFileNameByIdSQL;
   }

   @Override
   public String getCopyFileRecordByIdSQL() {
      return copyFileRecordByIdSQL;
   }

   @Override
   public String getCloneFileRecordByIdSQL() {
      return cloneFileRecordSQL;
   }

   @Override
   public String getDropFileTableSQL() {
      return dropFileTableSQL;
   }

   @Override
   public String createNodeManagerStoreTableSQL() {
      return createNodeManagerStoreTableSQL;
   }

   @Override
   public String createStateSQL() {
      return createStateSQL;
   }

   @Override
   public String createNodeIdSQL() {
      return createNodeIdSQL;
   }

   @Override
   public String createLiveLockSQL() {
      return createLiveLockSQL;
   }

   @Override
   public String createBackupLockSQL() {
      return createBackupLockSQL;
   }

   @Override
   public String tryAcquireLiveLockSQL() {
      return tryAcquireLiveLockSQL;
   }

   @Override
   public String tryAcquireBackupLockSQL() {
      return tryAcquireBackupLockSQL;
   }

   @Override
   public String tryReleaseLiveLockSQL() {
      return tryReleaseLiveLockSQL;
   }

   @Override
   public String tryReleaseBackupLockSQL() {
      return tryReleaseBackupLockSQL;
   }

   @Override
   public String isLiveLockedSQL() {
      return isLiveLockedSQL;
   }

   @Override
   public String isBackupLockedSQL() {
      return isBackupLockedSQL;
   }

   @Override
   public String renewLiveLockSQL() {
      return renewLiveLockSQL;
   }

   @Override
   public String renewBackupLockSQL() {
      return renewBackupLockSQL;
   }

   @Override
   public String currentTimestampSQL() {
      return currentTimestampSQL;
   }

   @Override
   public String writeStateSQL() {
      return writeStateSQL;
   }

   @Override
   public String readStateSQL() {
      return readStateSQL;
   }

   @Override
   public String writeNodeIdSQL() {
      return writeNodeIdSQL;
   }

   @Override
   public String readNodeIdSQL() {
      return readNodeIdSQL;
   }

   @Override
   public String initializeNodeIdSQL() {
      return initializeNodeIdSQL;
   }

   @Override
   public boolean closeConnectionOnShutdown() {
      return true;
   }

   public static class Factory implements SQLProvider.Factory {

      @Override
      public SQLProvider create(String tableName, DatabaseStoreType storeType) {
         return new GenericSQLProvider(tableName, storeType);
      }
   }
}
