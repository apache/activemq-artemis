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

   // Default to lowest (MYSQL = 64k)
   private static final int MAX_BLOB_SIZE = 64512;

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

   protected GenericSQLProvider(String tableName) {
      this.tableName = tableName;

      createFileTableSQL = "CREATE TABLE " + tableName +
         "(ID BIGINT AUTO_INCREMENT, FILENAME VARCHAR(255), EXTENSION VARCHAR(10), DATA BLOB, PRIMARY KEY(ID))";

      insertFileSQL = "INSERT INTO " + tableName + " (FILENAME, EXTENSION, DATA) VALUES (?,?,?)";

      selectFileNamesByExtensionSQL = "SELECT FILENAME, ID FROM " + tableName + " WHERE EXTENSION=?";

      selectIdByFileNameSQL = "SELECT ID, FILENAME, EXTENSION, DATA FROM " + tableName + " WHERE fileName=?";

      appendToFileSQL = "UPDATE " + tableName + " SET DATA = CONCAT(DATA, ?) WHERE ID=?";

      readLargeObjectSQL = "SELECT DATA FROM " + tableName + " WHERE ID=?";

      deleteFileSQL = "DELETE FROM " + tableName + " WHERE ID=?";

      updateFileNameByIdSQL = "UPDATE " + tableName + " SET FILENAME=? WHERE ID=?";

      cloneFileRecordSQL = "INSERT INTO " + tableName + "(FILENAME, EXTENSION, DATA) " +
         "(SELECT FILENAME, EXTENSION, DATA FROM " + tableName + " WHERE ID=?)";

      copyFileRecordByIdSQL = "UPDATE " + tableName + " SET DATA = (SELECT DATA FROM " + tableName + " WHERE ID=?) WHERE ID=?";

      dropFileTableSQL = "DROP TABLE " + tableName;

      createJournalTableSQL = new String[] {
         "CREATE TABLE " + tableName + "(id BIGINT,recordType SMALLINT,compactCount SMALLINT,txId BIGINT,userRecordType SMALLINT,variableSize INTEGER,record BLOB,txDataSize INTEGER,txData BLOB,txCheckNoRecords INTEGER,seq BIGINT NOT NULL, PRIMARY KEY(seq))",
         "CREATE INDEX " + tableName + "_IDX ON " + tableName + " (id)"
      };

      insertJournalRecordsSQL = "INSERT INTO " + tableName + "(id,recordType,compactCount,txId,userRecordType,variableSize,record,txDataSize,txData,txCheckNoRecords,seq) " + "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

      selectJournalRecordsSQL = "SELECT id,recordType,compactCount,txId,userRecordType,variableSize,record,txDataSize,txData,txCheckNoRecords,seq " + "FROM " + tableName + " ORDER BY seq ASC";

      deleteJournalRecordsSQL = "DELETE FROM " + tableName + " WHERE id = ?";

      deleteJournalTxRecordsSQL = "DELETE FROM " + tableName + " WHERE txId=?";

      countJournalRecordsSQL = "SELECT COUNT(*) FROM " + tableName;
   }

   @Override
   public int getMaxBlobSize() {
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
   public boolean closeConnectionOnShutdown() {
      return true;
   }

   public static class Factory implements SQLProvider.Factory {

      @Override
      public SQLProvider create(String tableName) {
         return new GenericSQLProvider(tableName);
      }
   }
}
