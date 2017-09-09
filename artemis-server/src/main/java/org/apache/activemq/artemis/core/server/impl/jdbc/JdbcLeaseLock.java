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
import java.util.function.Predicate;

import org.jboss.logging.Logger;

/**
 * JDBC implementation of a {@link LeaseLock} with a {@code String} defined {@link #holderId()}.
 */
final class JdbcLeaseLock implements LeaseLock {

   private static final Logger LOGGER = Logger.getLogger(JdbcLeaseLock.class);
   private static final int MAX_HOLDER_ID_LENGTH = 128;
   private final Connection connection;
   private final long maxAllowableMillisDiffFromDBTime;
   private long millisDiffFromCurrentTime;
   private final String holderId;
   private final PreparedStatement tryAcquireLock;
   private final PreparedStatement tryReleaseLock;
   private final PreparedStatement renewLock;
   private final PreparedStatement isLocked;
   private final PreparedStatement currentDateTime;
   private final long expirationMillis;
   private boolean maybeAcquired;

   /**
    * The lock will be responsible (ie {@link #close()}) of all the {@link PreparedStatement}s used by it, but not of the {@link Connection},
    * whose life cycle will be managed externally.
    */
   JdbcLeaseLock(String holderId,
                 Connection connection,
                 PreparedStatement tryAcquireLock,
                 PreparedStatement tryReleaseLock,
                 PreparedStatement renewLock,
                 PreparedStatement isLocked,
                 PreparedStatement currentDateTime,
                 long expirationMIllis,
                 long maxAllowableMillisDiffFromDBTime) {
      if (holderId.length() > MAX_HOLDER_ID_LENGTH) {
         throw new IllegalArgumentException("holderId length must be <=" + MAX_HOLDER_ID_LENGTH);
      }
      this.holderId = holderId;
      this.maxAllowableMillisDiffFromDBTime = maxAllowableMillisDiffFromDBTime;
      this.millisDiffFromCurrentTime = Long.MAX_VALUE;
      this.tryAcquireLock = tryAcquireLock;
      this.tryReleaseLock = tryReleaseLock;
      this.renewLock = renewLock;
      this.isLocked = isLocked;
      this.currentDateTime = currentDateTime;
      this.expirationMillis = expirationMIllis;
      this.maybeAcquired = false;
      this.connection = connection;
   }

   public String holderId() {
      return holderId;
   }

   @Override
   public long expirationMillis() {
      return expirationMillis;
   }

   private long timeDifference() throws SQLException {
      if (Long.MAX_VALUE == millisDiffFromCurrentTime) {
         if (maxAllowableMillisDiffFromDBTime > 0) {
            millisDiffFromCurrentTime = determineTimeDifference();
         } else {
            millisDiffFromCurrentTime = 0L;
         }
      }
      return millisDiffFromCurrentTime;
   }

   private long determineTimeDifference() throws SQLException {
      try (ResultSet resultSet = currentDateTime.executeQuery()) {
         long result = 0L;
         if (resultSet.next()) {
            final Timestamp timestamp = resultSet.getTimestamp(1);
            final long diff = System.currentTimeMillis() - timestamp.getTime();
            if (Math.abs(diff) > maxAllowableMillisDiffFromDBTime) {
               // off by more than maxAllowableMillisDiffFromDBTime so lets adjust
               result = (-diff);
            }
            LOGGER.info(holderId() + " diff adjust from db: " + result + ", db time: " + timestamp);
         }
         return result;
      }
   }

   @Override
   public boolean renew() {
      synchronized (connection) {
         try {
            final boolean result;
            connection.setAutoCommit(false);
            try {
               final long timeDifference = timeDifference();
               final PreparedStatement preparedStatement = this.renewLock;
               final long now = System.currentTimeMillis() + timeDifference;
               final Timestamp timestamp = new Timestamp(now + expirationMillis);
               preparedStatement.setTimestamp(1, timestamp);
               preparedStatement.setString(2, holderId);
               result = preparedStatement.executeUpdate() == 1;
            } catch (SQLException ie) {
               connection.rollback();
               connection.setAutoCommit(true);
               throw new IllegalStateException(ie);
            }
            connection.commit();
            connection.setAutoCommit(true);
            return result;
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   @Override
   public boolean tryAcquire() {
      synchronized (connection) {
         try {
            final boolean acquired;
            connection.setAutoCommit(false);
            try {
               final long timeDifference = timeDifference();
               final PreparedStatement preparedStatement = tryAcquireLock;
               final long now = System.currentTimeMillis() + timeDifference;
               preparedStatement.setString(1, holderId);
               final Timestamp timestamp = new Timestamp(now + expirationMillis);
               preparedStatement.setTimestamp(2, timestamp);
               acquired = preparedStatement.executeUpdate() == 1;
            } catch (SQLException ie) {
               connection.rollback();
               connection.setAutoCommit(true);
               throw new IllegalStateException(ie);
            }
            connection.commit();
            connection.setAutoCommit(true);
            if (acquired) {
               this.maybeAcquired = true;
            }
            return acquired;
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
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
      synchronized (connection) {
         try {
            boolean result;
            connection.setAutoCommit(false);
            try {
               final long timeDifference = timeDifference();
               final PreparedStatement preparedStatement = this.isLocked;
               try (ResultSet resultSet = preparedStatement.executeQuery()) {
                  if (!resultSet.next()) {
                     result = false;
                  } else {
                     final String currentHolderId = resultSet.getString(1);
                     result = holderIdFilter.test(currentHolderId);
                     //warn about any zombie lock
                     final Timestamp timestamp = resultSet.getTimestamp(2);
                     if (timestamp != null) {
                        final long lockExpirationTime = timestamp.getTime();
                        final long now = System.currentTimeMillis() + timeDifference;
                        final long expiredBy = now - lockExpirationTime;
                        if (expiredBy > 0) {
                           result = false;
                           LOGGER.warn("found zombie lock with holderId: " + currentHolderId + " expired by: " + expiredBy + " ms");
                        }
                     }
                  }
               }
            } catch (SQLException ie) {
               connection.rollback();
               connection.setAutoCommit(true);
               throw new IllegalStateException(ie);
            }
            connection.commit();
            connection.setAutoCommit(true);
            return result;
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   @Override
   public void release() {
      synchronized (connection) {
         try {
            connection.setAutoCommit(false);
            try {
               final PreparedStatement preparedStatement = this.tryReleaseLock;
               preparedStatement.setString(1, holderId);
               if (preparedStatement.executeUpdate() != 1) {
                  LOGGER.warn(holderId + " has failed to release a lock");
               } else {
                  LOGGER.info(holderId + " has released a lock");
               }
               //consider it as released to avoid on finalize to be reclaimed
               this.maybeAcquired = false;
            } catch (SQLException ie) {
               connection.rollback();
               connection.setAutoCommit(true);
               throw new IllegalStateException(ie);
            }
            connection.commit();
            connection.setAutoCommit(true);
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   @Override
   public void close() throws SQLException {
      synchronized (connection) {
         //to avoid being called if not needed
         if (!this.tryReleaseLock.isClosed()) {
            try {
               if (this.maybeAcquired) {
                  release();
               }
            } finally {
               this.tryReleaseLock.close();
               this.tryAcquireLock.close();
               this.renewLock.close();
               this.isLocked.close();
               this.currentDateTime.close();
            }
         }
      }
   }

   @Override
   protected void finalize() throws Throwable {
      close();
   }

}
