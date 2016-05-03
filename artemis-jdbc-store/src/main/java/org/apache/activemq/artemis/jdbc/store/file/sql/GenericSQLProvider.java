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
package org.apache.activemq.artemis.jdbc.store.file.sql;

public class GenericSQLProvider implements SQLProvider {

   // Default to lowest (MYSQL = 64k)
   private static final int MAX_BLOB_SIZE = 64512;

   private final String tableName;

   private final String createFileTableSQL;

   private final String insertFileSQL;

   private final String selectFileNamesByExtensionSQL;

   private final String selectIdByFileNameSQL;

   private final String appendToFileSQL;

   private final String readFileSQL;

   private final String deleteFileSQL;

   private final String updateFileNameByIdSQL;

   private final String copyFileRecordByIdSQL;

   private final String cloneFileRecordSQL;

   private final String dropFileTableSQL;

   public GenericSQLProvider(String tableName) {
      this.tableName = tableName;

      createFileTableSQL = "CREATE TABLE " + tableName +
         "(ID INT AUTO_INCREMENT, FILENAME VARCHAR(255), EXTENSION VARCHAR(10), DATA BLOB, PRIMARY KEY(ID))";

      insertFileSQL = "INSERT INTO " + tableName +
         " (FILENAME, EXTENSION, DATA) VALUES (?,?,?)";

      selectFileNamesByExtensionSQL = "SELECT FILENAME, ID FROM " + tableName + " WHERE EXTENSION=?";

      selectIdByFileNameSQL = "SELECT ID, FILENAME, EXTENSION, DATA FROM " + tableName + " WHERE fileName=?";

      appendToFileSQL = "UPDATE " + tableName + " SET DATA = CONCAT(DATA, ?) WHERE ID=?";

      readFileSQL = "SELECT DATA FROM " + tableName + " WHERE ID=?";

      deleteFileSQL = "DELETE FROM " + tableName + " WHERE ID=?";

      updateFileNameByIdSQL = "UPDATE " + tableName + " SET FILENAME=? WHERE ID=?";

      cloneFileRecordSQL = "INSERT INTO " + tableName + "(FILENAME, EXTENSION, DATA) " +
         "(SELECT FILENAME, EXTENSION, DATA FROM " + tableName + " WHERE ID=?)";

      copyFileRecordByIdSQL = "UPDATE " + tableName + " SET DATA = (SELECT DATA FROM " + tableName + " WHERE ID=?) WHERE ID=?";

      dropFileTableSQL = "DROP TABLE " + tableName;
   }

   @Override
   public int getMaxBlobSize() {
      return MAX_BLOB_SIZE;
   }

   @Override
   public String getTableName() {
      return tableName;
   }

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
   public String getAppendToFileSQL() {
      return appendToFileSQL;
   }

   @Override
   public String getReadFileSQL() {
      return readFileSQL;
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


}
