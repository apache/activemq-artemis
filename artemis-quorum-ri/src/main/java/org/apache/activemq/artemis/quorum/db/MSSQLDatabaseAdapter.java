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

public class MSSQLDatabaseAdapter extends BaseDatabaseAdapter {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public MSSQLDatabaseAdapter(Map<String, String> properties) throws Exception {
      super(properties);
      verifyInitialization();
   }

   @Override
   boolean tryLock(DatabaseDistributedLock lock) throws UnavailableStateException, InterruptedException {
      return mssqlTryLock(lock.getAssociatedConnection(), lock.getLockId());
   }

   private boolean mssqlTryLock(Connection c, String lockId) throws UnavailableStateException, InterruptedException {
      try (CallableStatement getLock = c.prepareCall(
         "{? = call sp_getapplock(@Resource=?, @LockMode='Exclusive', @LockOwner='Session', @LockTimeout=0)}")) {
         getLock.setString(2, lockId);
         getLock.registerOutParameter(1, Types.INTEGER);
         getLock.execute();
         int statusCode = getLock.getInt(1);
         // 0 = lock granted synchronously, 1 = lock granted after waiting for other locks to be released
         // -1 = timeout, -2 = cancelled, -3 = chosen as deadlock victim, -999 = illegal parameter or call error
         if (statusCode >= 0) {
            return true;
         } else if (statusCode == -1 || statusCode == -2 || statusCode == -3)  // couldn't get it
            return false;
         else {
            throw new UnavailableStateException("MSSQL returned code " + statusCode + " from sp_getapplock() request");
         }
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


   @Override
   void releaseLock(DatabaseDistributedLock lock) {
      try {
         mssqlReleaseLock(lock.getAssociatedConnection(), lock.getLockId());
      } catch (SQLException e) {
         logger.debug("Exception occurred in MSSQLDatabaseAdapter.releaseLock()", e);
      }
   }

   void mssqlReleaseLock(Connection c, String lockid) throws SQLException {
      try (CallableStatement releaseLock = c.prepareCall("{? = call sp_releaseapplock(@Resource=?, @LockOwner='Session')}")) {
         releaseLock.setString(2, lockid);
         releaseLock.registerOutParameter(1, Types.INTEGER);
         releaseLock.execute();
         int statusCode = releaseLock.getInt(1);
         if (statusCode != 0) {
            throw new IllegalStateException("Unable to properly release lock, received status code " + statusCode);
         }
      }
   }

   @Override
   boolean isConnectionHealthy(DatabaseDistributedLock lock) {
      try (Statement st = lock.getAssociatedConnection().createStatement()) {
         st.execute("select 1");
         return true;
      } catch (SQLException ex) {
         return false;
      }
   }

   private static final String TABLE_CREATION = "create table ARTEMIS_LOCKS(LOCKID varchar(64),LONG_VALUE bigint,LAST_ACCESS datetime)";

   private void verifyInitialization() {
      try (Connection c = getConnection(); Statement st = c.createStatement()) {
         Long locksTableExists = getDatabaseLong(st, "select count(*) from INFORMATION_SCHEMA.TABLES where TABLE_NAME='ARTEMIS_LOCKS'");
         if (locksTableExists == 0) {
            logger.warn("MSSQLDatabaseAdapter artemis locks table does not exist, attempting to create with: {}", TABLE_CREATION);
            st.execute(TABLE_CREATION); // attempt to automatically create
            if (mssqlTryLock(c, "locktest")) {  // if we have an execute rights issue, it should blow up here and we give the user an error
               mssqlReleaseLock(c, "locktest");
            }
         }
      } catch (SQLException | UnavailableStateException |
               InterruptedException e) {  // we come here if the user doesn't have rights to create the table
         logger.error("Error initializing MSSQLDatabaseAdapter", e);
         logger.error("Please ensure that the ARTEMIS_LOCKS table is created and that your user has execute rights to sp_getapplock() and sp_releaseapplock()");
      }
   }

}
