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
package org.apache.activemq.artemis.quorum.db;

import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.lang.invoke.MethodHandles;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

public class MySQLDatabaseAdapter extends BaseDatabaseAdapter {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public MySQLDatabaseAdapter(Map<String, String> properties) throws Exception {
      super(properties);
      verifyInitialization();
   }

   @Override
   boolean tryLock(DatabaseDistributedLock lock) throws UnavailableStateException, InterruptedException {
      try {
         return mysqlTryLock(lock.getAssociatedConnection(), lock.getLockId());
      } catch (SQLException e) {
         Throwable cause = e;
         do {
            if (cause instanceof InterruptedIOException)
               throw new InterruptedException("Interrupt during tryLock");
            cause = e.getCause();
         }
         while (cause != null);
         logger.debug("Exception occurred in MSSQLDatabaseAdapter.tryLock()", e);
         throw new UnavailableStateException(e.getMessage(), e);
      }
   }

   private boolean mysqlTryLock(Connection c, String lockId) throws SQLException {
      try (CallableStatement getLock = c.prepareCall("{? = call GET_LOCK(?, 0)}")) {
         getLock.setString(2, lockId);
         getLock.registerOutParameter(1, Types.INTEGER);
         getLock.execute();
         Object result = getLock.getObject(1);
         if (result == null)
            throw new SQLException("Error occurred acquiring lock");
         int ret = getLock.getInt(1);  // 1 = successful, 0 = timeout, null on error
         return ret == 1;
      }
   }

   @Override
   void releaseLock(DatabaseDistributedLock lock) {
      try {
         mysqlReleaseLock(lock.getAssociatedConnection(), lock.getLockId());
      } catch (SQLException e) {
         logger.debug("Exception occurred in MySQLDatabaseAdapter.releaseLock()", e);
      }
   }

   private void mysqlReleaseLock(Connection c, String lockId) throws SQLException {
      try (CallableStatement releaseLock = c.prepareCall("{? = call RELEASE_LOCK(?)}")) {
         releaseLock.setString(2, lockId);
         releaseLock.registerOutParameter(1, Types.INTEGER);
         releaseLock.execute();
         Object result = releaseLock.getObject(1); // 1 = successful, 0 = this session doesn't own the lock, NULL = lock name doesn't exist
         if (result != null) {
            int ret = releaseLock.getInt(1);
            if (ret == 0) {
               throw new SQLException("Received request in to unlock " + lockId + " but ");
            }
         }
      }
   }

   @Override
   protected String findOldLocksQuery() {
      return "select LOCKID from ARTEMIS_LOCKS where LAST_ACCESS < DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 HOUR)";  // Oracle and MSSQL will interpret this as 2 hours ago
   }

   private static final String TABLE_CREATION = "create table ARTEMIS_LOCKS(LOCKID varchar(64),LONG_VALUE bigint,LAST_ACCESS timestamp(3), primary key(LOCKID))";

   private void verifyInitialization() {
      try (Connection c = getConnection(); Statement st = c.createStatement()) {
         Long locksTableExists = getDatabaseLong(st, "select count(*) from INFORMATION_SCHEMA.TABLES where TABLE_NAME='ARTEMIS_LOCKS'");
         if (locksTableExists == 0) {
            logger.warn("MySQLDatabaseAdapter artemis locks table does not exist, attempting to create with: {}", TABLE_CREATION);
            st.execute(TABLE_CREATION); // attempt to automatically create
         }
         if (mysqlTryLock(c, "testlock")) {
            mysqlReleaseLock(c, "testlock");
         }
      } catch (SQLException e) {  // we come here if the user doesn't have rights to create the table
         logger.error("Error initializing MySQLDatabaseAdapter", e);
         logger.error("Please ensure that the ARTEMIS_LOCKS table is created and that your user has execute rights to the DBMS_LOCK package (GRANT EXECUTE on DBMS_LOCK to [youruser])");
      }
   }

}
