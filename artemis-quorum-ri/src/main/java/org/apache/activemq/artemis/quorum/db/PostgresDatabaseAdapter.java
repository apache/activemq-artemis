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

import com.sangupta.murmur.Murmur2;
import org.apache.activemq.artemis.api.core.Pair;
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

public class PostgresDatabaseAdapter extends BaseDatabaseAdapter {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public PostgresDatabaseAdapter(Map<String, String> properties) throws Exception {
      super(properties);
      verifyInitialization();
   }

   @Override
   boolean tryLock(DatabaseDistributedLock lock) throws UnavailableStateException, InterruptedException {
      try {
         Pair<Boolean, Long> ret = postgresTryLock(lock.getAssociatedConnection(), lock.getLockId());
         if (ret.getA()) {
            lock.setAdapterLockContext(ret.getB());
         }
         return ret.getA();
      } catch (SQLException e) {
         Throwable cause = e;
         do {
            if (cause instanceof InterruptedIOException)
               throw new InterruptedException("Interrupt during tryLock");
            cause = e.getCause();
         }
         while (cause != null);
         throw new UnavailableStateException(e.getMessage(), e);
      }
   }

   private Pair<Boolean, Long> postgresTryLock(Connection c, String lockId) throws SQLException {
      try (CallableStatement getLock = c.prepareCall(
         "{? = call pg_try_advisory_lock(?) }")) {
         // postgres takes a 64-bit integer to represent the lockId. I am hashing our lock id.
         // is there a better way than this? Is it worth pulling in the Murmur hashing library?
         // open to ideas on this.
         long numericLockId = Murmur2.hash64(lockId.getBytes(), lockId.length(), 509803945L);
         getLock.setLong(2, numericLockId);
         getLock.registerOutParameter(1, Types.BOOLEAN);
         getLock.execute();
         return new Pair<>(getLock.getBoolean(1), numericLockId);
      }
   }

   @Override
   void releaseLock(DatabaseDistributedLock lock) {
      if (lock.getAdapterLockContext() == null)
         return;
      try {
         postgresReleaseLock(lock.getAssociatedConnection(), lock.getLockId(), (Long) lock.getAdapterLockContext());
      } catch (SQLException e) {
         logger.error("Exception occurred in PostgresDatabaseAdapter.releaseLock()", e);
      }
   }

   private void postgresReleaseLock(Connection c, String lockId, long lockNumber) throws SQLException {
      try (CallableStatement releaseLock = c.prepareCall("{? = call pg_advisory_unlock(?) }")) {
         releaseLock.setLong(2, lockNumber);
         releaseLock.registerOutParameter(1, Types.BOOLEAN);
         releaseLock.execute();
         boolean ret = releaseLock.getBoolean(1);
         if (!ret) {
            throw new IllegalStateException("Attempted to release lock " + lockId + " but postgres says we don't hold it!");
         }
      }

   }

   @Override
   protected String findOldLocksQuery() {
      return "select LOCKID from ARTEMIS_LOCKS where LAST_ACCESS + interval '2 hours' < CURRENT_TIMESTAMP";
   }

   /* We call this when the adapter is initialized to verify that the database meets the criteria for running.
      We will attempt to use current access rights to simply fix the situation and get running
    */
   private static final String TABLE_CREATION = "create table ARTEMIS_LOCKS(LOCKID varchar(64),LONG_VALUE bigint,LAST_ACCESS timestamp, primary key(LOCKID))";

   private void verifyInitialization() {
      try (Connection c = getConnection(); Statement st = c.createStatement()) {
         Long locksTableExists = getDatabaseLong(st, "select count(*) from INFORMATION_SCHEMA.TABLES where TABLE_NAME='artemis_locks'"); // postgres lowers case
         if (locksTableExists == 0) {
            logger.warn("PostgresDatabaseAdapter artemis locks table does not exist, attempting to create with: {}", TABLE_CREATION);
            st.execute(TABLE_CREATION); // attempt to automatically create
         }
         Pair<Boolean, Long> test = postgresTryLock(c, "testlock");
         if (test.getA()) {
            postgresReleaseLock(c, "testlock", test.getB());
         }
      } catch (SQLException e) {  // we come here if the user doesn't have rights to create the table
         logger.error("Error initializing OracleDatabaseAdapter", e);
         logger.error("Please ensure that the 'artemis_locks' table is created and that your user has execute rights to pg_try_advistory_lock and pg_advisory_unlock");
      }
   }
}
