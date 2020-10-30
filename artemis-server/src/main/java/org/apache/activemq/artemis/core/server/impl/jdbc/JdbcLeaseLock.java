/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.impl.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.jboss.logging.Logger;

/**
 * JDBC implementation of a {@link LeaseLock} with a {@code String} defined {@link #holderId()}.
 */
final class JdbcLeaseLock implements LeaseLock {

   private static final Logger LOGGER = Logger.getLogger(JdbcLeaseLock.class);
   private static final int MAX_HOLDER_ID_LENGTH = 128;
   private final JDBCConnectionProvider connectionProvider;
   private final String holderId;
   private final String tryAcquireLock;
   private final String tryReleaseLock;
   private final String renewLock;
   private final String isLocked;
   private final String currentDateTime;
   private final long expirationMillis;
   private final int queryTimeout;
   private boolean maybeAcquired;
   private final String lockName;
   private long localExpirationTime;

   /**
    * The lock will be responsible (ie {@link #close()}) of all the {@link PreparedStatement}s used by it, but not of the {@link Connection},
    * whose life cycle will be managed externally.
    */
   JdbcLeaseLock(String holderId,
                 JDBCConnectionProvider connectionProvider,
                 String tryAcquireLock,
                 String tryReleaseLock,
                 String renewLock,
                 String isLocked,
                 String currentDateTime,
                 long expirationMIllis,
                 long queryTimeoutMillis,
                 String lockName) {
      if (holderId.length() > MAX_HOLDER_ID_LENGTH) {
         throw new IllegalArgumentException("holderId length must be <=" + MAX_HOLDER_ID_LENGTH);
      }
      this.holderId = holderId;
      this.tryAcquireLock = tryAcquireLock;
      this.tryReleaseLock = tryReleaseLock;
      this.renewLock = renewLock;
      this.isLocked = isLocked;
      this.currentDateTime = currentDateTime;
      this.expirationMillis = expirationMIllis;
      this.maybeAcquired = false;
      this.connectionProvider = connectionProvider;
      this.lockName = lockName;
      this.localExpirationTime = -1;
      int expectedTimeout = -1;
      if (queryTimeoutMillis >= 0) {
         expectedTimeout = (int) TimeUnit.MILLISECONDS.toSeconds(queryTimeoutMillis);
         if (expectedTimeout <= 0) {
            LOGGER.warn("queryTimeoutMillis is too low: it's suggested to configure a multi-seconds value. Disabling it because too low.");
            expectedTimeout = -1;
         }
      }
      this.queryTimeout = expectedTimeout;

   }

   public String holderId() {
      return holderId;
   }

   /**
    * Given that many DBMS won't support standard SQL queries to collect CURRENT_TIMESTAMP at milliseconds granularity,
    * this value is stripped of the milliseconds part, making it less optimistic then the reality, if >= 0.<p>
    * It's commonly used as an hard deadline for JDBC operations, hence is fine to not have a high precision.
    */
   @Override
   public long localExpirationTime() {
      return localExpirationTime;
   }

   @Override
   public long expirationMillis() {
      return expirationMillis;
   }

   private String readableLockStatus() {
      try (Connection connection = connectionProvider.getConnection()) {
         connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
         final boolean autoCommit = connection.getAutoCommit();
         connection.setAutoCommit(false);
         try (PreparedStatement preparedStatement = connection.prepareStatement(this.isLocked)) {
            final String lockStatus;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
               if (!resultSet.next()) {
                  lockStatus = null;
               } else {
                  final String currentHolderId = resultSet.getString(1);
                  final Timestamp expirationTime = resultSet.getTimestamp(2);
                  final Timestamp currentTimestamp = resultSet.getTimestamp(3);
                  lockStatus = "holderId = " + currentHolderId + " expirationTime = " + expirationTime + " currentTimestamp = " + currentTimestamp;
               }
            }
            connection.commit();
            return lockStatus;
         } catch (SQLException ie) {
            connection.rollback();
            return ie.getMessage();
         } finally {
            connection.setAutoCommit(autoCommit);
         }
      } catch (SQLException e) {
         return e.getMessage();
      }
   }

   private long dbCurrentTimeMillis(Connection connection) throws SQLException {
      try (PreparedStatement currentDateTime = connection.prepareStatement(this.currentDateTime)) {
         if (queryTimeout >= 0) {
            currentDateTime.setQueryTimeout(queryTimeout);
         }
         final long startTime = stripMilliseconds(System.currentTimeMillis());
         try (ResultSet resultSet = currentDateTime.executeQuery()) {
            resultSet.next();
            final long endTime = stripMilliseconds(System.currentTimeMillis());
            final Timestamp currentTimestamp = resultSet.getTimestamp(1);
            final long currentTime = currentTimestamp.getTime();
            final long currentTimeMillis = stripMilliseconds(currentTime);
            if (currentTimeMillis < startTime) {
               LOGGER.warnf("[%s] %s query currentTimestamp = %s on database should happen AFTER %s on broker", lockName, holderId, currentTimestamp, new Timestamp(startTime));
            }
            if (currentTimeMillis > endTime) {
               LOGGER.warnf("[%s] %s query currentTimestamp = %s on database should happen BEFORE %s on broker", lockName, holderId, currentTimestamp, new Timestamp(endTime));
            }
            return currentTime;
         }
      }
   }

   @Override
   public boolean renew() {
      try (Connection connection = connectionProvider.getConnection()) {
         connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
         final boolean autoCommit = connection.getAutoCommit();
         connection.setAutoCommit(false);
         try (PreparedStatement preparedStatement = connection.prepareStatement(this.renewLock)) {
            final long now = dbCurrentTimeMillis(connection);
            final long localExpirationTime = now + expirationMillis;
            final Timestamp expirationTime = new Timestamp(localExpirationTime);
            if (LOGGER.isDebugEnabled()) {
               LOGGER.debugf("[%s] %s is renewing lock with expirationTime = %s",
                             lockName, holderId, expirationTime);
            }
            preparedStatement.setTimestamp(1, expirationTime);
            preparedStatement.setString(2, holderId);
            preparedStatement.setTimestamp(3, expirationTime);
            preparedStatement.setTimestamp(4, expirationTime);
            final int updatedRows = preparedStatement.executeUpdate();
            final boolean renewed = updatedRows == 1;
            connection.commit();
            if (!renewed) {
               this.localExpirationTime = -1;
               if (LOGGER.isDebugEnabled()) {
                  LOGGER.debugf("[%s] %s has failed to renew lock: lock status = { %s }",
                                lockName, holderId, readableLockStatus());
               }
            } else {
               this.localExpirationTime = stripMilliseconds(localExpirationTime);
               LOGGER.debugf("[%s] %s has renewed lock", lockName, holderId);
            }
            return renewed;
         } catch (SQLException ie) {
            connection.rollback();
            throw new IllegalStateException(ie);
         } finally {
            connection.setAutoCommit(autoCommit);
         }
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      }
   }

   private static long stripMilliseconds(long time) {
      return (time / 1000) * 1000;
   }

   @Override
   public boolean tryAcquire() {
      try (Connection connection = connectionProvider.getConnection()) {
         connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
         final boolean autoCommit = connection.getAutoCommit();
         connection.setAutoCommit(false);
         try (PreparedStatement preparedStatement = connection.prepareStatement(this.tryAcquireLock)) {
            final long now = dbCurrentTimeMillis(connection);
            preparedStatement.setString(1, holderId);
            final long localExpirationTime = now + expirationMillis;
            final Timestamp expirationTime = new Timestamp(localExpirationTime);
            preparedStatement.setTimestamp(2, expirationTime);
            preparedStatement.setTimestamp(3, expirationTime);
            LOGGER.debugf("[%s] %s is trying to acquire lock with expirationTime %s",
                          lockName, holderId, expirationTime);
            final boolean acquired = preparedStatement.executeUpdate() == 1;
            connection.commit();
            if (acquired) {
               this.maybeAcquired = true;
               this.localExpirationTime = stripMilliseconds(localExpirationTime);
               LOGGER.debugf("[%s] %s has acquired lock", lockName, holderId);
            } else {
               if (LOGGER.isDebugEnabled()) {
                  LOGGER.debugf("[%s] %s has failed to acquire lock: lock status = { %s }",
                                lockName, holderId, readableLockStatus());
               }
            }
            return acquired;
         } catch (SQLException ie) {
            connection.rollback();
            throw new IllegalStateException(ie);
         } finally {
            connection.setAutoCommit(autoCommit);
         }
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public boolean isHeld() {
      return checkValidHolderId(Objects::nonNull);
   }

   @Override
   public boolean isHeldByCaller() {
      return checkValidHolderId(this.holderId::equals);
   }

   private boolean checkValidHolderId(Predicate<? super String> holderIdFilter) {
      try (Connection connection = connectionProvider.getConnection()) {
         connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
         final boolean autoCommit = connection.getAutoCommit();
         connection.setAutoCommit(false);
         try (PreparedStatement preparedStatement = connection.prepareStatement(this.isLocked)) {
            boolean result;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
               if (!resultSet.next()) {
                  result = false;
               } else {
                  final String currentHolderId = resultSet.getString(1);
                  result = holderIdFilter.test(currentHolderId);
                  final Timestamp expirationTime = resultSet.getTimestamp(2);
                  final Timestamp currentTimestamp = resultSet.getTimestamp(3);
                  final long currentTimestampMillis = currentTimestamp.getTime();
                  boolean zombie = false;
                  if (expirationTime != null) {
                     final long lockExpirationTime = expirationTime.getTime();
                     final long expiredBy = currentTimestampMillis - lockExpirationTime;
                     if (expiredBy > 0) {
                        result = false;
                        zombie = true;
                     }
                  }
                  if (LOGGER.isDebugEnabled()) {
                     LOGGER.debugf("[%s] %s has found %s with holderId = %s expirationTime = %s currentTimestamp = %s",
                                   lockName, holderId, zombie ? "zombie lock" : "lock",
                                   currentHolderId, expirationTime, currentTimestamp);
                  }
               }
            }
            connection.commit();
            return result;
         } catch (SQLException ie) {
            connection.rollback();
            throw new IllegalStateException(ie);
         } finally {
            connection.setAutoCommit(autoCommit);
         }
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public void release() {
      try (Connection connection = connectionProvider.getConnection()) {
         connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
         final boolean autoCommit = connection.getAutoCommit();
         connection.setAutoCommit(false);
         try (PreparedStatement preparedStatement = connection.prepareStatement(this.tryReleaseLock)) {
            preparedStatement.setString(1, holderId);
            final boolean released = preparedStatement.executeUpdate() == 1;
            //consider it as released to avoid on finalize to be reclaimed
            this.localExpirationTime = -1;
            this.maybeAcquired = false;
            connection.commit();
            if (!released) {
               if (LOGGER.isDebugEnabled()) {
                  LOGGER.debugf("[%s] %s has failed to release lock: lock status = { %s }",
                                lockName, holderId, readableLockStatus());
               }
            } else {
               LOGGER.debugf("[%s] %s has released lock", lockName, holderId);
            }
         } catch (SQLException ie) {
            connection.rollback();
            throw new IllegalStateException(ie);
         } finally {
            connection.setAutoCommit(autoCommit);
         }
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public void close() throws SQLException {
      if (this.maybeAcquired) {
         release();
      }
   }

   @Override
   protected void finalize() throws Throwable {
      close();
   }

}
