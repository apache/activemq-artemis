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
package org.apache.activemq.artemis.jdbc.store.drivers.mysql;

import org.apache.activemq.artemis.jdbc.store.sql.GenericSQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

public class MySQLSQLProvider extends GenericSQLProvider {

   private static final int MAX_BLOB_SIZE = 4 * 1024 * 1024 * 1024; // 4GB

   private final String createFileTableSQL;

   private final String[] createJournalTableSQL;

   private final String copyFileRecordByIdSQL;

   private MySQLSQLProvider(String tName) {
      super(tName.toLowerCase());

      createFileTableSQL = "CREATE TABLE " + tableName +
         "(ID BIGINT NOT NULL AUTO_INCREMENT," +
         "FILENAME VARCHAR(255), EXTENSION VARCHAR(10), DATA LONGBLOB, PRIMARY KEY(ID)) ENGINE=InnoDB;";

      createJournalTableSQL = new String[] {
         "CREATE TABLE " + tableName + "(id BIGINT,recordType SMALLINT,compactCount SMALLINT,txId BIGINT,userRecordType SMALLINT,variableSize INTEGER,record LONGBLOB,txDataSize INTEGER,txData LONGBLOB,txCheckNoRecords INTEGER,seq BIGINT) ENGINE=InnoDB;",
         "CREATE INDEX " + tableName + "_IDX ON " + tableName + " (id)"
      };

      copyFileRecordByIdSQL = " UPDATE " + tableName + ", (SELECT DATA AS FROM_DATA FROM " + tableName +
         " WHERE id=?) SELECT_COPY SET DATA=FROM_DATA WHERE id=?;";
   }

   @Override
   public int getMaxBlobSize() {
      return MAX_BLOB_SIZE;
   }

   @Override
   public String getCreateFileTableSQL() {
      return createFileTableSQL;
   }

   @Override
   public String[] getCreateJournalTableSQL() {
      return createJournalTableSQL;
   }

   @Override
   public String getCopyFileRecordByIdSQL() {
      return copyFileRecordByIdSQL;
   }

   public static class Factory implements SQLProvider.Factory {

      @Override
      public SQLProvider create(String tableName) {
         return new MySQLSQLProvider(tableName);
      }
   }
}
