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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Supplier;

import org.apache.activemq.artemis.jdbc.store.drivers.AbstractJDBCDriver;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.utils.UUID;
import org.jboss.logging.Logger;

/**
 * JDBC implementation of a {@link SharedStateManager}.
 */
@SuppressWarnings("SynchronizeOnNonFinalField")
final class JdbcSharedStateManager extends AbstractJDBCDriver implements SharedStateManager {

   private static final Logger logger = Logger.getLogger(JdbcSharedStateManager.class);
   public static final int MAX_SETUP_ATTEMPTS = 20;
   private final String holderId;
   private final long lockExpirationMillis;
   private JdbcLeaseLock liveLock;
   private JdbcLeaseLock backupLock;
   private PreparedStatement readNodeId;
   private PreparedStatement writeNodeId;
   private PreparedStatement initializeNodeId;
   private PreparedStatement readState;
   private PreparedStatement writeState;

   public static JdbcSharedStateManager usingDataSource(String holderId,
                                                        long locksExpirationMillis,
                                                        DataSource dataSource,
                                                        SQLProvider provider) {
      final JdbcSharedStateManager sharedStateManager = new JdbcSharedStateManager(holderId, locksExpirationMillis);
      sharedStateManager.setDataSource(dataSource);
      sharedStateManager.setSqlProvider(provider);
      try {
         sharedStateManager.start();
         return sharedStateManager;
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      }
   }

   public static JdbcSharedStateManager usingConnectionUrl(String holderId,
                                                           long locksExpirationMillis,
                                                           String jdbcConnectionUrl,
                                                           String jdbcDriverClass,
                                                           SQLProvider provider) {
      final JdbcSharedStateManager sharedStateManager = new JdbcSharedStateManager(holderId, locksExpirationMillis);
      sharedStateManager.setJdbcConnectionUrl(jdbcConnectionUrl);
      sharedStateManager.setJdbcDriverClass(jdbcDriverClass);
      sharedStateManager.setSqlProvider(provider);
      try {
         sharedStateManager.start();
         return sharedStateManager;
      } catch (SQLException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   protected void createSchema() throws SQLException {
      try {
         createTable(sqlProvider.createNodeManagerStoreTableSQL(), sqlProvider.createNodeIdSQL(), sqlProvider.createStateSQL(), sqlProvider.createLiveLockSQL(), sqlProvider.createBackupLockSQL());
      } catch (SQLException e) {
         //no op: if a table already exists is not a problem in this case, the prepareStatements() call will fail right after it if the table is not correctly initialized
         if (logger.isDebugEnabled()) {
            logger.debug("Error while creating the schema of the JDBC shared state manager", e);
         }
      }
   }

   static JdbcLeaseLock createLiveLock(String holderId,
                                       Connection connection,
                                       SQLProvider sqlProvider,
                                       long expirationMillis,
                                       long maxAllowableMillisDiffFromDBtime) throws SQLException {
      return new JdbcLeaseLock(holderId, connection, connection.prepareStatement(sqlProvider.tryAcquireLiveLockSQL()), connection.prepareStatement(sqlProvider.tryReleaseLiveLockSQL()), connection.prepareStatement(sqlProvider.renewLiveLockSQL()), connection.prepareStatement(sqlProvider.isLiveLockedSQL()), connection.prepareStatement(sqlProvider.currentTimestampSQL()), expirationMillis, maxAllowableMillisDiffFromDBtime);
   }

   static JdbcLeaseLock createBackupLock(String holderId,
                                         Connection connection,
                                         SQLProvider sqlProvider,
                                         long expirationMillis,
                                         long maxAllowableMillisDiffFromDBtime) throws SQLException {
      return new JdbcLeaseLock(holderId, connection, connection.prepareStatement(sqlProvider.tryAcquireBackupLockSQL()), connection.prepareStatement(sqlProvider.tryReleaseBackupLockSQL()), connection.prepareStatement(sqlProvider.renewBackupLockSQL()), connection.prepareStatement(sqlProvider.isBackupLockedSQL()), connection.prepareStatement(sqlProvider.currentTimestampSQL()), expirationMillis, maxAllowableMillisDiffFromDBtime);
   }

   @Override
   protected void prepareStatements() throws SQLException {
      this.liveLock = createLiveLock(this.holderId, this.connection, sqlProvider, lockExpirationMillis, 0);
      this.backupLock = createBackupLock(this.holderId, this.connection, sqlProvider, lockExpirationMillis, 0);
      this.readNodeId = connection.prepareStatement(sqlProvider.readNodeIdSQL());
      this.writeNodeId = connection.prepareStatement(sqlProvider.writeNodeIdSQL());
      this.initializeNodeId = connection.prepareStatement(sqlProvider.initializeNodeIdSQL());
      this.writeState = connection.prepareStatement(sqlProvider.writeStateSQL());
      this.readState = connection.prepareStatement(sqlProvider.readStateSQL());
   }

   private JdbcSharedStateManager(String holderId, long lockExpirationMillis) {
      this.holderId = holderId;
      this.lockExpirationMillis = lockExpirationMillis;
   }

   @Override
   public LeaseLock liveLock() {
      return this.liveLock;
   }

   @Override
   public LeaseLock backupLock() {
      return this.backupLock;
   }

   private UUID rawReadNodeId() throws SQLException {
      final PreparedStatement preparedStatement = this.readNodeId;
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
         if (!resultSet.next()) {
            return null;
         } else {
            final String nodeId = resultSet.getString(1);
            if (nodeId != null) {
               return new UUID(UUID.TYPE_TIME_BASED, UUID.stringToBytes(nodeId));
            } else {
               return null;
            }
         }
      }
   }

   @Override
   public UUID readNodeId() {
      synchronized (connection) {
         try {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(true);
            final UUID nodeId = rawReadNodeId();
            return nodeId;
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   @Override
   public void writeNodeId(UUID nodeId) {
      synchronized (connection) {
         try {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(true);
            rawWriteNodeId(nodeId);
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   private void rawWriteNodeId(UUID nodeId) throws SQLException {
      final PreparedStatement preparedStatement = this.writeNodeId;
      preparedStatement.setString(1, nodeId.toString());
      if (preparedStatement.executeUpdate() != 1) {
         throw new IllegalStateException("can't write NODE_ID on the JDBC Node Manager Store!");
      }
   }

   private boolean rawInitializeNodeId(UUID nodeId) throws SQLException {
      final PreparedStatement preparedStatement = this.initializeNodeId;
      preparedStatement.setString(1, nodeId.toString());
      final int rows = preparedStatement.executeUpdate();
      assert rows <= 1;
      return rows > 0;
   }

   @Override
   public UUID setup(Supplier<? extends UUID> nodeIdFactory) {
      SQLException lastError = null;
      synchronized (connection) {
         final UUID newNodeId = nodeIdFactory.get();
         for (int attempts = 0; attempts < MAX_SETUP_ATTEMPTS; attempts++) {
            lastError = null;
            try {
               final UUID nodeId = initializeOrReadNodeId(newNodeId);
               if (nodeId != null) {
                  return nodeId;
               }
            } catch (SQLException e) {
               if (logger.isDebugEnabled()) {
                  logger.debug("Error while attempting to setup the NodeId", e);
               }
               lastError = e;
            }
         }
      }
      if (lastError != null) {
         logger.error("Unable to setup a NodeId on the JDBC shared state", lastError);
      } else {
         logger.error("Unable to setup a NodeId on the JDBC shared state");
      }
      throw new IllegalStateException("FAILED TO SETUP the JDBC Shared State NodeId");
   }

   private UUID initializeOrReadNodeId(final UUID newNodeId) throws SQLException {
      final UUID nodeId;
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      connection.setAutoCommit(false);
      try {
         //optimistic try to initialize nodeId
         if (rawInitializeNodeId(newNodeId)) {
            nodeId = newNodeId;
         } else {
            nodeId = rawReadNodeId();
         }
      } catch (SQLException e) {
         connection.rollback();
         connection.setAutoCommit(true);
         if (logger.isDebugEnabled()) {
            logger.debug("Rollback while trying to update NodeId to " + newNodeId, e);
         }
         return null;
      }
      if (nodeId != null) {
         connection.commit();
         connection.setAutoCommit(true);
         return nodeId;
      } else {
         //that means that the rawInitializeNodeId has failed just due to contention or the nodeId wasn't committed yet
         connection.rollback();
         connection.setAutoCommit(true);
         if (logger.isDebugEnabled()) {
            logger.debug("Rollback after failed to update NodeId to " + newNodeId + " and haven't found any NodeId");
         }
         return null;
      }
   }

   private static State decodeState(String s) {
      if (s == null) {
         return State.NOT_STARTED;
      }
      switch (s) {
         case "L":
            return State.LIVE;
         case "F":
            return State.FAILING_BACK;
         case "P":
            return State.PAUSED;
         case "N":
            return State.NOT_STARTED;
         default:
            throw new IllegalStateException("unknown state [" + s + "]");
      }
   }

   private static String encodeState(State state) {
      switch (state) {
         case LIVE:
            return "L";
         case FAILING_BACK:
            return "F";
         case PAUSED:
            return "P";
         case NOT_STARTED:
            return "N";
         default:
            throw new IllegalStateException("unknown state [" + state + "]");
      }
   }

   @Override
   public State readState() {
      synchronized (connection) {
         try {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(true);
            final State state;
            final PreparedStatement preparedStatement = this.readState;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
               if (!resultSet.next()) {
                  state = State.FIRST_TIME_START;
               } else {
                  state = decodeState(resultSet.getString(1));
               }
            }
            return state;
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   @Override
   public void writeState(State state) {
      final String encodedState = encodeState(state);
      synchronized (connection) {
         try {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(true);
            final PreparedStatement preparedStatement = this.writeState;
            preparedStatement.setString(1, encodedState);
            if (preparedStatement.executeUpdate() != 1) {
               throw new IllegalStateException("can't write STATE to the JDBC Node Manager Store!");
            }
         } catch (SQLException e) {
            throw new IllegalStateException(e);
         }
      }
   }

   @Override
   public void stop() throws SQLException {
      //release all the managed resources inside the connection lock
      if (sqlProvider.closeConnectionOnShutdown()) {
         synchronized (connection) {
            this.readNodeId.close();
            this.writeNodeId.close();
            this.initializeNodeId.close();
            this.readState.close();
            this.writeState.close();
            this.liveLock.close();
            this.backupLock.close();
            super.stop();
         }
      }
   }

   @Override
   public void close() throws SQLException {
      stop();
   }
}
