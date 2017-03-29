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
package org.apache.activemq.artemis.jdbc.store.drivers.derby;

import org.apache.activemq.artemis.jdbc.store.sql.GenericSQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

public class DerbySQLProvider extends GenericSQLProvider {

   // Derby max blob size = 2G
   private static final int MAX_BLOB_SIZE = 2147483647;

   private final String createFileTableSQL;

   private DerbySQLProvider(String tableName, DatabaseStoreType databaseStoreType) {
      super(tableName.toUpperCase(), databaseStoreType);

      createFileTableSQL = "CREATE TABLE " + tableName +
         "(ID BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
         "FILENAME VARCHAR(255), EXTENSION VARCHAR(10), DATA BLOB, PRIMARY KEY(ID))";
   }

   @Override
   public long getMaxBlobSize() {
      return MAX_BLOB_SIZE;
   }

   @Override
   public String getCreateFileTableSQL() {
      return createFileTableSQL;
   }

   @Override
   public boolean closeConnectionOnShutdown() {
      return false;
   }

   public static class Factory implements SQLProvider.Factory {

      @Override
      public SQLProvider create(String tableName, DatabaseStoreType databaseStoreType) {
         return new DerbySQLProvider(tableName, databaseStoreType);
      }
   }
}
