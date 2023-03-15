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
import java.util.HashMap;
import java.util.Map;

public class H2DatabaseAdapter extends BaseDatabaseAdapter {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public H2DatabaseAdapter(Map<String, String> properties) throws Exception {
      super(properties);
      verifyInitialization();
   }

   @Override
   boolean tryLock(DatabaseDistributedLock lock) throws UnavailableStateException, InterruptedException {
      try (CallableStatement getLock = lock.getAssociatedConnection().prepareCall(
         "{? = call lock_trylock(?)}")) {
         getLock.setString(2, lock.getLockId());
         getLock.registerOutParameter(1, Types.INTEGER);
         getLock.execute();
         int statusCode = getLock.getInt(1);
         // 0 = lock ok, 1 = no lock
         return statusCode == 0;
      } catch (SQLException e) {
         Throwable cause = e;
         do {
            if (cause instanceof InterruptedIOException)
               throw new InterruptedException("Interrupt during tryLock");
            cause = e.getCause();
         }
         while (cause != null);
         logger.debug("Exception occurred in H2DatabaseAdapter.tryLock()", e);
         throw new UnavailableStateException(e.getMessage(), e);
      }
   }

   @Override
   void releaseLock(DatabaseDistributedLock lock) {
      try (CallableStatement releaseLock = lock.getAssociatedConnection().prepareCall("{? = call lock_unlock(?)}")) {
         releaseLock.setString(2, lock.getLockId());
         releaseLock.registerOutParameter(1, Types.INTEGER);
         releaseLock.execute();
         int statusCode = releaseLock.getInt(1);
         if (statusCode != 0) {
            throw new IllegalStateException("Unable to properly release lock, received status code " + statusCode);
         }
      } catch (SQLException e) {
         logger.debug("Exception occurred in H2DatabaseAdapter.releaseLock()", e);
      }
   }

   @Override
   boolean isConnectionHealthy(DatabaseDistributedLock lock) {
      return true;
   }

   void verifyInitialization() {
      try (Connection c = getConnection(); Statement st = c.createStatement()) {
         st.execute("create table if not exists ARTEMIS_LOCKS(LOCKID varchar(64),LONG_VALUE bigint,LAST_ACCESS timestamp,primary key(LOCKID))");
         st.execute("create alias if not exists lock_unlock for \"org.apache.activemq.artemis.quorum.db.H2DatabaseAdapter.h2ReleaseLock\"");
         st.execute("create alias if not exists lock_trylock for \"org.apache.activemq.artemis.quorum.db.H2DatabaseAdapter.h2TryLock\"");
      } catch (SQLException e) {
         logger.error("Error verifying setup", e);
      }
   }

   private static final Map<String, Integer> databaseLocks = new HashMap<>();

   public static Integer h2TryLock(Connection conn, String lockid) {
      Integer currentSessionId = ((org.h2.engine.SessionLocal) ((org.h2.jdbc.JdbcConnection) conn).getSession()).getId();
      synchronized (databaseLocks) {
         Integer owningSession = databaseLocks.get(lockid);
         if (owningSession != null) {
            if (owningSession.equals(currentSessionId)) { // we already have the lock
               return 0;
            } else {
               return 1; // someone else has the lock
            }
         } else {
            databaseLocks.put(lockid, currentSessionId); // we just acquired the lock
            return 0;
         }
      }
   }

   public static Integer h2ReleaseLock(Connection conn, String lockid) {
      Integer currentSessionId = ((org.h2.engine.SessionLocal) ((org.h2.jdbc.JdbcConnection) conn).getSession()).getId();
      synchronized (databaseLocks) {
         Integer owningSession = databaseLocks.get(lockid);
         if (owningSession != null) {
            if (owningSession.equals(currentSessionId)) { // we had the lock and it was ours, release
               databaseLocks.remove(lockid);
               return 0;
            } else {
               return 2;  // we don't own this lock! we can't unlock it
            }
         } else {
            return 1; // we never had the lock to begin with
         }
      }
   }

   protected String findOldLocksQuery() {
      return "select LOCKID from ARTEMIS_LOCKS where LAST_ACCESS < DATEADD(HOUR,-2,CURRENT_TIMESTAMP)";
   }
}
