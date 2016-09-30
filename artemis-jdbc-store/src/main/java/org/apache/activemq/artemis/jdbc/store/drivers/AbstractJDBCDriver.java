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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.activemq.artemis.jdbc.store.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;

/**
 * Class to hold common database functionality such as drivers and connections
 */
public abstract class AbstractJDBCDriver {

   protected Connection connection;

   protected SQLProvider sqlProvider;

   protected String jdbcConnectionUrl;

   protected String jdbcDriverClass;

   protected Driver dbDriver;

   protected DataSource dataSource;

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
      JDBCUtils.createTableIfNotExists(connection, sqlProvider.getTableName(), schemaSql);
   }

   protected void connect() throws Exception {
      if (dataSource != null) {
         connection = dataSource.getConnection();
      } else {
         try {
            dbDriver = JDBCUtils.getDriver(jdbcDriverClass);
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

   public Connection getConnection() {
      return connection;
   }

   public void setConnection(Connection connection) {
      this.connection = connection;
   }

   public SQLProvider getSqlProvider() {
      return sqlProvider;
   }

   public void setSqlProvider(SQLProvider sqlProvider) {
      this.sqlProvider = sqlProvider;
   }

   public String getJdbcConnectionUrl() {
      return jdbcConnectionUrl;
   }

   public void setJdbcConnectionUrl(String jdbcConnectionUrl) {
      this.jdbcConnectionUrl = jdbcConnectionUrl;
   }

   public String getJdbcDriverClass() {
      return jdbcDriverClass;
   }

   public void setJdbcDriverClass(String jdbcDriverClass) {
      this.jdbcDriverClass = jdbcDriverClass;
   }

   public DataSource getDataSource() {
      return dataSource;
   }

   public void setDataSource(DataSource dataSource) {
      this.dataSource = dataSource;
   }
}
