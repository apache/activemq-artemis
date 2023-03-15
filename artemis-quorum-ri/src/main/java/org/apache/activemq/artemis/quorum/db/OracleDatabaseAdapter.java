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

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

/*  You need to do this:
 GRANT EXECUTE on dbms_lock to <dbuser>;


create table ARTEMIS_LOCKS(lockid VARCHAR2(64),VAL NUMBER(16),LAST_ACCESS TIMESTAMP);
create index ARTEMIS_LOCKS_IX(lockid)

*/
public class OracleDatabaseAdapter extends BaseDatabaseAdapter {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public OracleDatabaseAdapter(Map<String, String> properties) throws Exception {
      super(properties);
      verifyInitialization();
   }

   @Override
   public boolean tryLock(DatabaseDistributedLock lock) throws UnavailableStateException, InterruptedException {
      try {
         Pair<Boolean, String> ret = oracleTryLock(lock.getAssociatedConnection(), lock.getLockId());
         if (ret.getA()) {
            lock.setAdapterLockContext(ret.getB());
         }
         return ret.getA();
      } catch (SQLException e) {
         Throwable cause = e;
         do {
            if (cause instanceof InterruptedIOException) {
               throw new InterruptedException("Interrupt during tryLock");
            }
            cause = e.getCause();
         }
         while (cause != null);
         throw new UnavailableStateException(e.getMessage(), e);
      }
   }

   private Pair<Boolean, String> oracleTryLock(Connection c, String lockid) throws SQLException {
      try (CallableStatement getLockHandle = c.prepareCall("{call dbms_lock.allocate_unique(?, ?)}");
           CallableStatement getLock = c.prepareCall("{ ? = call dbms_lock.request( lockhandle => ?, lockmode => DBMS_LOCK.X_MODE, timeout => 0)}")) {
         getLockHandle.setString(1, lockid);
         getLockHandle.registerOutParameter(2, Types.NUMERIC);
         getLockHandle.execute();
         String lockHandle = getLockHandle.getString(2);
         getLock.setString(2, lockHandle);
         getLock.registerOutParameter(1, Types.INTEGER);
         getLock.execute();
         int statusCode = getLock.getInt(1);
         // 0 = success, 4 = already own lock,  1 = timeout, 2 = deadlock, 3 = parameter error, 5 = illegal lock handle
         if (statusCode == 0) {
            return new Pair<>(true, lockHandle);
         } else if (statusCode == 1) // couldn't get it
            return new Pair<>(false, null);
         else if (statusCode == 4) // spec doesn't want us allowing tryLock() twice
            throw new IllegalStateException("Already have the lock");
         else
            throw new SQLException("Oracle returned code " + statusCode + " from DBMS_LOCK.REQUEST");
      }
   }

   @Override
   public void releaseLock(DatabaseDistributedLock lock) {
      if (lock.getAdapterLockContext() == null) // we can unlock anything if we don't actually have the handle (we shouldn't have the lock)
         return;
      String lockHandle = (String) lock.getAdapterLockContext();
      try {
         oracleReleaseLock(lock.getAssociatedConnection(), lockHandle);
      } catch (SQLException e) {
         logger.error("Exception occurred in OracleDatabaseAdapter.releaseLock()", e);
      }
   }

   private void oracleReleaseLock(Connection c, String lockHandle) throws SQLException {
      try (CallableStatement releaseLock = c.prepareCall("{? = call dbms_lock.release(lockhandle => ?)}")) {
         releaseLock.setString(2, lockHandle);
         releaseLock.registerOutParameter(1, Types.INTEGER);
         releaseLock.execute();
         int statusCode = releaseLock.getInt(1);
         if (statusCode != 0) {
            throw new SQLException("Unable to properly release lock, received status code " + statusCode);
         }
      }
   }

   @Override
   boolean isConnectionHealthy(DatabaseDistributedLock lock) {
      try (Statement st = lock.getAssociatedConnection().createStatement()) {
         st.execute("select 1 from DUAL");
         return true;
      } catch (SQLException ex) {
         return false;
      }
   }

   private static final String TABLE_CREATION = "create table ARTEMIS_LOCKS(LOCKID varchar2(64),LONG_VALUE number(16),LAST_ACCESS timestamp, primary key(LOCKID))";

   private void verifyInitialization() {
      try (Connection c = getConnection(); Statement st = c.createStatement()) {
         Long locksTableExists = getDatabaseLong(st, "select count(*) from USER_TABLES where TABLE_NAME='ARTEMIS_LOCKS'");
         if (locksTableExists == 0) {
            logger.warn("OracleDatabaseAdapter artemis locks table does not exist, attempting to create with: {}", TABLE_CREATION);
            st.execute(TABLE_CREATION); // attempt to automatically create
            Pair<Boolean, String> test = oracleTryLock(c, "testlock");
            if (test.getA()) {
               oracleReleaseLock(c, test.getB());
            }
         }
      } catch (SQLException e) {  // we come here if the user doesn't have rights to create the table
         logger.error("Error initializing OracleDatabaseAdapter", e);
         logger.error("Please ensure that the ARTEMIS_LOCKS table is created and that your user has execute rights to the DBMS_LOCK package (GRANT EXECUTE on DBMS_LOCK to [youruser])");
      }
   }
}
