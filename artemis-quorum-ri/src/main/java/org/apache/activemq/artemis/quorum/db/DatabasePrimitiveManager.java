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

import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.MutableLong;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/* Configuration examples:  Add the adapter-class in below, 3 are currently implemented
 * -- OracleDatabaseAdapter, MSSQLDatabaseAdapter and PostgresDatabaseAdapter
 * The url, user and password values are utilized by the DefaultDatabaseConnectionProvider and will provide a simple
 * connection via the JDBC DriverManager. If you wish to provide your own mechanism for getting a database connection,
 * you can implement the DatabaseConnectionProvider interface and then in the properties for the DatabasePrimitiveManager
 * add the property database-connection-provider-class and set it to your implementations classname. This should work
 * well for embedded systems. The
 *
 *       <ha-policy>
 *          <replication>
 *             <primary>
 *                <manager>
 *                   <class-name>org.apache.activemq.artemis.quorum.db.DatabasePrimitiveManager</class-name>
 *                   <properties>
 *                      <property key="adapter-class" value="org.apache.activemq.artemis.quorum.db.MSSQLDatabaseAdapter"/>
 *                      <property key="url" value="jdbc:sqlserver://localhost:1433;databaseName=TestDB;trustServerCertificate=true"/>
 *                      <property key="user" value="testuser"/>
 *                      <property key="password" value="testpassword"/>
 *                   </properties>
 *                </manager>
 *             </primary>
 *          </replication>
 *       </ha-policy>
 *
 */


public class DatabasePrimitiveManager implements DistributedPrimitiveManager {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
   private final Map<String, DatabaseDistributedLock> locks = new HashMap<>();
   private final BaseDatabaseAdapter adapter;
   private boolean started;
   private ScheduledExecutorService maintenance;

   public DatabasePrimitiveManager(Map<String, String> args) throws ClassNotFoundException, NoSuchMethodException,
      InvocationTargetException, InstantiationException, IllegalAccessException {
      if (!args.containsKey("adapter-class"))
         throw new IllegalStateException("adapter-class property must be configured for DatabasePrimitiveManager");
      // pass along the manager properties map to the adapter class to allow it to be used for initialization if needed
      adapter = (BaseDatabaseAdapter) Class.forName(args.get("adapter-class")).
         getDeclaredConstructor(Map.class).newInstance(args);
   }

   // The locking model is such that only one method is going to be executed at a time.
   // if we have enough locks that this periodic maintenance becomes problematic, then we'll need to adjust the
   // locking model.
   static final String MAINTENANCE_LOCK = "artemis_lockid_maintenance";

   synchronized void cleanupOldLocks() {
      try {
         DistributedLock maintenanceLock = getDistributedLock(MAINTENANCE_LOCK);
         if (maintenanceLock.tryLock()) { // if someone already has the maintenance lock, just let it go
            lockAndRemoveOldLocks();
            maintenanceLock.unlock();
         }
      } catch (Exception ex) {
         logger.debug("cleaning up old locks failed for now, will try again later", ex);
      }
   }

   private void lockAndRemoveOldLocks() {
      try (Connection c = adapter.getConnection()) {
         List<String> lockIds = adapter.getOldLockIds(c); // this should filter and old return IDs that are actually "old"
         for (String lockId : lockIds) {
            boolean alreadyHadLock = locks.containsKey(lockId);
            DatabaseDistributedLock lock = (DatabaseDistributedLock) getDistributedLock(lockId);
            if (lock.tryLock()) { // the lock is old and we are able to lock it, we are assuming it is safe to get rid of
               logger.debug("Removing stale lock {} record as part of maintenance", lockId);
               adapter.cleanup(lock);  // because no one has accessed it within the maintenance window
               lock.close();
            } else {
               // the lock is still in use because someone still has it, update its LAST_ACCESS
               // so we'll leave it alone for another maintenance cycle
               logger.debug("Lock {} has old time but is still in use (locked), updating last access time", lockId);
               adapter.updateLastAccessTime(lock);
               if (!alreadyHadLock)  // this lock was not already instantiated outside of this maintenance cycle,
                  lock.close();    // close it to avoid unnecessarily holding a database connection
            }
         }
      } catch (Exception ex) {
         logger.warn("exception getting database connection for periodic maintenance, " + ex.getMessage());
      }
   }

   private synchronized void verifyLiveness() {
      // check connections
      for (DatabaseDistributedLock lock : locks.values()) {
         if (!adapter.isConnectionHealthy(lock)) {
            lock.handleLost();
            lock.close();
         }
      }
   }


   @Override
   public synchronized boolean isStarted() {
      return started;
   }

   @Override
   public synchronized void addUnavailableManagerListener(UnavailableManagerListener listener) {
      // todo
   }

   @Override
   public synchronized void removeUnavailableManagerListener(UnavailableManagerListener listener) {
      // todo
   }

   @Override
   public synchronized boolean start(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
      logger.debug("DatabasePrimitiveManager[{}] starting", adapter.getClass().getName());
      if (timeout >= 0) {
         Objects.requireNonNull(unit);
      }
      if (started) {
         return true;
      }
      started = true;
      // is there another accessible thread pool we should just use for this maintenance operation?
      maintenance = Executors.newSingleThreadScheduledExecutor();
      maintenance.scheduleWithFixedDelay(this::cleanupOldLocks, 0, 2, TimeUnit.HOURS); // first run right away on startup
      maintenance.scheduleWithFixedDelay(this::verifyLiveness, 1, 1, TimeUnit.MINUTES);
      return true;
   }

   @Override
   public void start() throws InterruptedException, ExecutionException {
      start(-1, null);
   }

   @Override
   public synchronized void stop() {
      if (!started) {
         return;
      }
      logger.debug("DatabasePrimitiveManager[{}] stopping", adapter.getClass().getName());
      try {
         maintenance.shutdownNow();
         locks.forEach((lockId, lock) -> {
            try {
               lock.close(false);
            } catch (Throwable t) {
               logger.warn("exception closing lock", t);
            }
         });
         locks.clear();
         try {
            maintenance.awaitTermination(10, TimeUnit.SECONDS);
         } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
         }
      } finally {
         started = false;
      }
   }

   @Override
   public synchronized DistributedLock getDistributedLock(String lockId) throws ExecutionException {
      Objects.requireNonNull(lockId);
      if (!started) {
         throw new IllegalStateException("manager should be started first");
      }
      final DatabaseDistributedLock lock = locks.get(lockId);
      if (lock != null && !lock.isClosed()) {
         return lock;
      }
      try {
         final DatabaseDistributedLock newLock = new DatabaseDistributedLock(locks::remove, this, adapter, lockId);
         locks.put(lockId, newLock);
         return newLock;
      } catch (SQLException ioEx) {
         throw new ExecutionException(ioEx);
      }
   }

   @Override
   public MutableLong getMutableLong(final String mutableLongId) throws ExecutionException {
      final DatabaseDistributedLock lock = (DatabaseDistributedLock) getDistributedLock(mutableLongId);
      return new MutableLong() {
         @Override
         public String getMutableLongId() {
            return mutableLongId;
         }

         @Override
         public long get() throws UnavailableStateException {
            try {
               return lock.getLong();
            } catch (SQLException e) {
               throw new UnavailableStateException(e);
            }
         }

         @Override
         public void set(long value) throws UnavailableStateException {
            try {
               lock.setLong(value);
            } catch (SQLException e) {
               throw new UnavailableStateException(e);
            }
         }

         @Override
         public void close() {
            //lock.close();
         }
      };
   }
}
