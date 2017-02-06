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
package org.apache.activemq.artemis.jdbc.store.drivers.postgres;

import org.apache.activemq.artemis.jdbc.store.sql.GenericSQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

public class PostgresSQLProvider extends GenericSQLProvider {

   // BYTEA Size used in Journal
   private static final int MAX_BLOB_SIZE = 1024 * 1024 * 1024; // 1GB

   private final String createFileTableSQL;

   private final String[] createJournalTableSQL;

   private PostgresSQLProvider(String tName) {
      super(tName.toLowerCase());
      createFileTableSQL = "CREATE TABLE " + tableName +
         "(ID BIGSERIAL, FILENAME VARCHAR(255), EXTENSION VARCHAR(10), DATA OID, PRIMARY KEY(ID))";

      createJournalTableSQL = new String[] {
         "CREATE TABLE " + tableName + "(id BIGINT,recordType SMALLINT,compactCount SMALLINT,txId BIGINT,userRecordType SMALLINT,variableSize INTEGER,record BYTEA,txDataSize INTEGER,txData BYTEA,txCheckNoRecords INTEGER,seq BIGINT)",
         "CREATE INDEX " + tableName + "_IDX ON " + tableName + " (id)"
      };
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
   public int getMaxBlobSize() {
      return MAX_BLOB_SIZE;
   }

   public static class Factory implements SQLProvider.Factory {

      @Override
      public SQLProvider create(String tableName) {
         return new PostgresSQLProvider(tableName);
      }
   }
}

