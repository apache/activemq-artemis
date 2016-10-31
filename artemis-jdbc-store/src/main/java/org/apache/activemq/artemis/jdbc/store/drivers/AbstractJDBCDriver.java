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
import java.sql.Statement;
import java.util.Properties;

import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.jboss.logging.Logger;

/**
 * Class to hold common database functionality such as drivers and connections
 */
public abstract class AbstractJDBCDriver {

   private static final Logger logger = Logger.getLogger(AbstractJDBCDriver.class);

   protected Connection connection;

   protected SQLProvider sqlProvider;

   private String jdbcConnectionUrl;

   private String jdbcDriverClass;

   private DataSource dataSource;

   public AbstractJDBCDriver() {
   }

   public AbstractJDBCDriver(SQLProvider sqlProvider, String jdbcConnectionUrl, String jdbcDriverClass) {
      this.jdbcConnectionUrl = jdbcConnectionUrl;
      this.jdbcDriverClass = jdbcDriverClass;
      this.sqlProvider = sqlProvider;
   }

   public AbstractJDBCDriver(DataSource dataSource, SQLProvider provider) {
      this.dataSource = dataSource;
      this.sqlProvider = provider;
   }

   public void start() throws Exception {
      connect();
      createSchema();
      prepareStatements();
   }

   public void stop() throws SQLException {
      if (sqlProvider.closeConnectionOnShutdown()) {
         connection.close();
      }
   }

   protected abstract void prepareStatements() throws SQLException;

   protected abstract void createSchema() throws SQLException;

   protected void createTable(String schemaSql) throws SQLException {
      createTableIfNotExists(connection, sqlProvider.getTableName(), schemaSql);
   }

   protected void connect() throws Exception {
      if (dataSource != null) {
         connection = dataSource.getConnection();
      } else {
         try {
            Driver dbDriver = getDriver(jdbcDriverClass);
            connection = dbDriver.connect(jdbcConnectionUrl, new Properties());
         } catch (SQLException e) {
            ActiveMQJournalLogger.LOGGER.error("Unable to connect to database using URL: " + jdbcConnectionUrl);
            throw new RuntimeException("Error connecting to database", e);
         }
      }
   }

   public void destroy() throws Exception {
      try {
         connection.setAutoCommit(false);
         try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE " + sqlProvider.getTableName());
         }
         connection.commit();
      } catch (SQLException e) {
         connection.rollback();
         throw e;
      }
   }

   private static void createTableIfNotExists(Connection connection, String tableName, String sql) throws SQLException {
      logger.tracef("Validating if table %s didn't exist before creating", tableName);
      try {
         connection.setAutoCommit(false);
         try (ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null)) {
            if (rs != null && !rs.next()) {
               logger.tracef("Table %s did not exist, creating it with SQL=%s", tableName, sql);
               try (Statement statement = connection.createStatement()) {
                  statement.executeUpdate(sql);
               }
            }
         }
         connection.commit();
      } catch (SQLException e) {
         connection.rollback();
      }
   }

   private Driver getDriver(String className) throws Exception {
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

   public void setConnection(Connection connection) {
      this.connection = connection;
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
}
