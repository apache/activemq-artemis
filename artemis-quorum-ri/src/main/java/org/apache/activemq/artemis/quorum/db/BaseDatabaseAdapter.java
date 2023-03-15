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

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public abstract class BaseDatabaseAdapter {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
   protected DatabaseConnectionProvider provider;

   static DatabaseConnectionProvider getProvider(Map<String, String> properties) throws Exception {
      // a DatabaseConnectionProvider implementation class can be implemented so that the mechanism to get
      // database connections can be injected by an implementation where artemis is embedded or other circumstances
      if (properties.containsKey("database-connection-provider-class")) {
         return (DatabaseConnectionProvider) Class.forName(properties.get("database-connection-provider-class"))
            .getDeclaredConstructor(Map.class).newInstance(properties);
      } else {
         return new DefaultDatabaseConnectionProvider(properties);
      }
   }

   public BaseDatabaseAdapter(Map<String, String> properties) throws Exception {
      this.provider = getProvider(properties);
   }

   /**
    * this shouldn't generally be called by the adapter implementation as it gets a new connection.
    * Connections are retrieved when the DatabaseDistributedLock is created and live for the life of that lock.
    * From within the adapter call  DatabaseDistributedLock.getAssociatedConnection() instead to get the connection
    * associated with the lock context.
    */
   Connection getConnection() throws SQLException {
      return provider.getConnection();
   }

   /**
    * Implementations must return true if a lock is successfully acquired for the given connection,
    * false if the lock is unable to be acquired but the attempt was otherwise functional,
    * exceptions in case things didn't work
    */
   abstract boolean tryLock(DatabaseDistributedLock lock) throws UnavailableStateException, InterruptedException;

   /**
    * release the database lock associated with the given connector
    */
   abstract void releaseLock(DatabaseDistributedLock lock);

   public void updateLastAccessTime(DatabaseDistributedLock lock) throws SQLException {
      try (PreparedStatement update = lock.getAssociatedConnection().prepareStatement("update ARTEMIS_LOCKS set LAST_ACCESS=CURRENT_TIMESTAMP where LOCKID=?")) {
         update.setString(1, lock.getLockId());
         update.executeUpdate();
      }
   }


   /**
    * purge the record associated with this lock
    * records with longs are the ones that ultimately get a record in the ARTEMIS_LOCKS.  If a record has been around for a long time and no one has a lock on it
    * it is time to get rid of it
    */
   public void cleanup(DatabaseDistributedLock lock) {
      try (PreparedStatement delete = lock.getAssociatedConnection().prepareStatement("delete from ARTEMIS_LOCKS where LOCKID=?")) {
         delete.setString(1, lock.getLockId());
         delete.executeUpdate();
      } catch (SQLException ex) {
         logger.warn("exception thrown deleting lock ", ex);
      }
   }

   /**
    * Database lock is held
    */
   protected Long readCurrentValue(DatabaseDistributedLock lock) throws SQLException {
      Long ret = null;
      try (PreparedStatement read = lock.getAssociatedConnection().prepareStatement("select LONG_VALUE from ARTEMIS_LOCKS where LOCKID=?")) {
         read.setString(1, lock.getLockId());
         ResultSet rs = read.executeQuery();
         if (rs.next()) {
            ret = rs.getLong("LONG_VALUE");
         }
      }
      updateLastAccessTime(lock);
      return ret;
   }


   /* must return the lock ids that are considered old and might be defunct at this point.  the maintenance
   routine in DatabasePrimitiveManager will get these ids, verify that it can lock them and then delete them */
   public List<String> getOldLockIds(Connection c) {
      return findOldLocks(c);
   }

   /* common implementation that should kind of work for everyone, override getLockIds() if this doesn't work
   for your adapter implementation
   */
   protected List<String> findOldLocks(Connection c) {
      List<String> ret = new ArrayList<>();
      try (Statement st = c.createStatement()) {
         ResultSet rs = st.executeQuery(findOldLocksQuery());
         while (rs.next()) {
            ret.add(rs.getString(1));
         }
      } catch (SQLException ex) {
         logger.error("exception getting old lockIds", ex);
      }
      return ret;
   }

   // this wasn't declared abstract to leave it open for an implementer to getOldLockIds() directly instead and
   // do something different than this query approach
   protected String findOldLocksQuery() {
      return "Implement findOldLocksQuery for your database"; // intentional syntax error
   }

   /**
    * get the current value of the long associated with the lockId
    */
   public long getLong(DatabaseDistributedLock lock) throws SQLException {
      Long val = readCurrentValue(lock);
      return val == null ? 0L : val;
   }

   /**
    * set the value associated with the lockId
    */
   public void setLong(DatabaseDistributedLock lock, long val) throws SQLException {
      Long curr = readCurrentValue(lock);
      if (curr == null) {
         try (PreparedStatement insert = lock.getAssociatedConnection().prepareStatement("insert into ARTEMIS_LOCKS(LOCKID,LONG_VALUE,LAST_ACCESS) " +
            "values (?,?,CURRENT_TIMESTAMP)")) {
            insert.setString(1, lock.getLockId());
            insert.setLong(2, val);
            insert.executeUpdate();
         }
      } else {
         try (PreparedStatement insert = lock.getAssociatedConnection().prepareStatement("update ARTEMIS_LOCKS set LONG_VALUE=?, LAST_ACCESS=CURRENT_TIMESTAMP where LOCKID=?")) {
            insert.setLong(1, val);
            insert.setString(2, lock.getLockId());
            insert.executeUpdate();
         }
      }
   }

   boolean isConnectionHealthy(DatabaseDistributedLock lock) {
      try (Statement st = lock.getAssociatedConnection().createStatement()) {
         st.execute("select 1");
         return true;
      } catch (SQLException ex) {
         return false;
      }
   }

   public void close(DatabaseDistributedLock lock) {
      Connection c = lock.getAssociatedConnection();
      try {
         c.close();
      } catch (SQLException ex) {
         // should never happy, really
         logger.warn("Exception during connection close ", ex);
      }
   }

   protected Long getDatabaseLong(Statement st, String query) throws SQLException {
      ResultSet rs = st.executeQuery(query);
      rs.next();
      return rs.getLong(1);
   }

}
