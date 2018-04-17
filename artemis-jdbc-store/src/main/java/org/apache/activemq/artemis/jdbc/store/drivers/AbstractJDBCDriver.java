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
package org.apache.activemq.artemis.jdbc.store.drivers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.jboss.logging.Logger;

/**
 * Class to hold common database functionality such as drivers and connections
 */
@SuppressWarnings("SynchronizeOnNonFinalField")
public abstract class AbstractJDBCDriver {

   private static final Logger logger = Logger.getLogger(AbstractJDBCDriver.class);

   protected Connection connection;

   protected SQLProvider sqlProvider;

   private String jdbcConnectionUrl;

   private String jdbcDriverClass;

   private DataSource dataSource;

   private Executor networkTimeoutExecutor;

   private int networkTimeoutMillis;

   public AbstractJDBCDriver() {
      this.networkTimeoutExecutor = null;
      this.networkTimeoutMillis = -1;
   }

   public AbstractJDBCDriver(SQLProvider sqlProvider, String jdbcConnectionUrl, String jdbcDriverClass) {
      this.jdbcConnectionUrl = jdbcConnectionUrl;
      this.jdbcDriverClass = jdbcDriverClass;
      this.sqlProvider = sqlProvider;
      this.networkTimeoutExecutor = null;
      this.networkTimeoutMillis = -1;
   }

   public AbstractJDBCDriver(DataSource dataSource, SQLProvider provider) {
      this.dataSource = dataSource;
      this.sqlProvider = provider;
      this.networkTimeoutExecutor = null;
      this.networkTimeoutMillis = -1;
   }

   public void start() throws SQLException {
      connect();
      synchronized (connection) {
         createSchema();
         prepareStatements();
      }
   }

   public AbstractJDBCDriver(Connection connection, SQLProvider sqlProvider) {
      this.connection = connection;
      this.sqlProvider = sqlProvider;
      this.networkTimeoutExecutor = null;
      this.networkTimeoutMillis = -1;
   }

   public void stop() throws SQLException {
      synchronized (connection) {
         if (sqlProvider.closeConnectionOnShutdown()) {
            try {
               connection.setAutoCommit(true);
               connection.close();
            } catch (SQLException e) {
               logger.error(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), e));
               throw e;
            }
         }
      }
   }

   protected abstract void prepareStatements() throws SQLException;

   protected abstract void createSchema() throws SQLException;

   protected final void createTable(String... schemaSqls) throws SQLException {
      createTableIfNotExists(sqlProvider.getTableName(), schemaSqls);
   }

   private void connect() throws SQLException {
      if (connection == null) {
         if (dataSource != null) {
            try {
               connection = dataSource.getConnection();
            } catch (SQLException e) {
               logger.error(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), e));
               throw e;
            }
         } else {
            try {
               if (jdbcDriverClass == null || jdbcDriverClass.isEmpty()) {
                  throw new IllegalStateException("jdbcDriverClass is null or empty!");
               }
               if (jdbcConnectionUrl == null || jdbcConnectionUrl.isEmpty()) {
                  throw new IllegalStateException("jdbcConnectionUrl is null or empty!");
               }
               final Driver dbDriver = getDriver(jdbcDriverClass);
               connection = dbDriver.connect(jdbcConnectionUrl, new Properties());
               if (connection == null) {
                  throw new IllegalStateException("the driver: " + jdbcDriverClass + " isn't able to connect to the requested url: " + jdbcConnectionUrl);
               }
            } catch (SQLException e) {
               logger.error(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), e));
               ActiveMQJournalLogger.LOGGER.error("Unable to connect to database using URL: " + jdbcConnectionUrl);
               throw e;
            }
         }
         if (this.networkTimeoutMillis >= 0 && this.networkTimeoutExecutor == null) {
            logger.warn("Unable to set a network timeout on the JDBC connection: networkTimeoutExecutor is null");
         }
         if (this.networkTimeoutMillis >= 0 && this.networkTimeoutExecutor != null) {
            try {
               connection.setNetworkTimeout(this.networkTimeoutExecutor, this.networkTimeoutMillis);
            } catch (SQLException e) {
               logger.warn(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), e));
               ActiveMQJournalLogger.LOGGER.warn("Unable to set a network timeout on the JDBC connection");
            } catch (Throwable throwable) {
               //it included SecurityExceptions and UnsupportedOperationException
               logger.warn("Unable to set a network timeout on the JDBC connection", throwable);
            }
         }
      }
   }

   public void destroy() throws Exception {
      final String dropTableSql = "DROP TABLE " + sqlProvider.getTableName();
      try {
         connection.setAutoCommit(false);
         try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(dropTableSql);
         }
         connection.commit();
      } catch (SQLException e) {
         logger.error(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), e, dropTableSql));
         try {
            connection.rollback();
         } catch (SQLException rollbackEx) {
            logger.error(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), rollbackEx, dropTableSql));
            throw rollbackEx;
         }
         throw e;
      }
   }

   private void createTableIfNotExists(String tableName, String... sqls) throws SQLException {
      logger.tracef("Validating if table %s didn't exist before creating", tableName);
      try {
         connection.setAutoCommit(false);
         final boolean tableExists;
         try (ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null)) {
            if ((rs == null) || (rs != null && !rs.next())) {
               tableExists = false;
               if (logger.isTraceEnabled()) {
                  logger.tracef("Table %s did not exist, creating it with SQL=%s", tableName, Arrays.toString(sqls));
               }
               if (rs != null) {
                  final SQLWarning sqlWarning = rs.getWarnings();
                  if (sqlWarning != null) {
                     logger.warn(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), sqlWarning));
                  }
               }
            } else {
               tableExists = true;
            }
         }
         if (tableExists) {
            logger.tracef("Validating if the existing table %s is initialized or not", tableName);
            try (Statement statement = connection.createStatement();
                 ResultSet cntRs = statement.executeQuery(sqlProvider.getCountJournalRecordsSQL())) {
               logger.tracef("Validation of the existing table %s initialization is started", tableName);
               int rows;
               if (cntRs.next() && (rows = cntRs.getInt(1)) > 0) {
                  logger.tracef("Table %s did exist but is not empty. Skipping initialization. Found %d rows.", tableName, rows);
                  if (logger.isDebugEnabled()) {
                     final long expectedRows = Stream.of(sqls).map(String::toUpperCase).filter(sql -> sql.contains("INSERT INTO")).count();
                     if (rows < expectedRows) {
                        logger.debug("Table " + tableName + " was expected to contain " + expectedRows + " rows while it has " + rows + " rows.");
                     }
                  }
                  connection.commit();
                  return;
               } else {
                  sqls = Stream.of(sqls).filter(sql -> {
                     final String upperCaseSql = sql.toUpperCase();
                     return !(upperCaseSql.contains("CREATE TABLE") || upperCaseSql.contains("CREATE INDEX"));
                  }).toArray(String[]::new);
                  if (sqls.length > 0) {
                     logger.tracef("Table %s did exist but is empty. Starting initialization.", tableName);
                  } else {
                     logger.tracef("Table %s did exist but is empty. Initialization completed: no initialization statements left.", tableName);
                  }
               }
            } catch (SQLException e) {
               logger.warn(JDBCUtils.appendSQLExceptionDetails(new StringBuilder("Can't verify the initialization of table ").append(tableName).append(" due to:"), e, sqlProvider.getCountJournalRecordsSQL()));
               try {
                  connection.rollback();
               } catch (SQLException rollbackEx) {
                  logger.debug("Rollback failed while validating initialization of a table", rollbackEx);
               }
               connection.setAutoCommit(false);
               logger.tracef("Table %s seems to exist, but we can't verify the initialization. Keep trying to create and initialize.", tableName);
            }
         }
         if (sqls.length > 0) {
            try (Statement statement = connection.createStatement()) {
               for (String sql : sqls) {
                  statement.executeUpdate(sql);
                  final SQLWarning statementSqlWarning = statement.getWarnings();
                  if (statementSqlWarning != null) {
                     logger.warn(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), statementSqlWarning, sql));
                  }
               }
            }

            connection.commit();
         }
      } catch (SQLException e) {
         final String sqlStatements = Stream.of(sqls).collect(Collectors.joining("\n"));
         logger.error(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), e, sqlStatements));
         try {
            connection.rollback();
         } catch (SQLException rollbackEx) {
            logger.error(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), rollbackEx, sqlStatements));
            throw rollbackEx;
         }
         throw e;
      }
   }

   private Driver getDriver(String className) {
      try {
         Driver driver = (Driver) Class.forName(className).newInstance();

         // Shutdown the derby if using the derby embedded driver.
         if (className.equals("org.apache.derby.jdbc.EmbeddedDriver")) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
               @Override
               public void run() {
                  try {
                     DriverManager.getConnection("jdbc:derby:;shutdown=true");
                  } catch (Exception e) {
                  }
               }
            });
         }
         return driver;
      } catch (ClassNotFoundException cnfe) {
         throw new RuntimeException("Could not find class: " + className);
      } catch (Exception e) {
         throw new RuntimeException("Unable to instantiate driver class: ", e);
      }
   }

   public Connection getConnection() {
      return connection;
   }

   public final void setConnection(Connection connection) {
      if (this.connection == null) {
         this.connection = connection;
      }
   }

   public void setSqlProvider(SQLProvider sqlProvider) {
      this.sqlProvider = sqlProvider;
   }

   public void setJdbcConnectionUrl(String jdbcConnectionUrl) {
      this.jdbcConnectionUrl = jdbcConnectionUrl;
   }

   public void setJdbcDriverClass(String jdbcDriverClass) {
      this.jdbcDriverClass = jdbcDriverClass;
   }

   public void setDataSource(DataSource dataSource) {
      this.dataSource = dataSource;
   }

   public void setNetworkTimeout(Executor executor, int milliseconds) {
      this.networkTimeoutExecutor = executor;
      this.networkTimeoutMillis = milliseconds;
   }

}
