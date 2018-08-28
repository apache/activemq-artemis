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

package org.apache.activemq.artemis.jdbc.store.logging;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.jboss.logging.Logger;

public class LoggingConnection implements Connection {

   private final Connection connection;

   private final String connectionID;

   private Logger logger;

   private Logger.Level level = Logger.Level.TRACE;

   public LoggingConnection(Connection connection, Logger logger) {
      this.connection = connection;
      this.logger = logger;
      this.connectionID = LoggingUtil.getID(connection);
   }

   public Connection getConnection() {
      return connection;
   }

   public String getConnectionID() {
      return connectionID;
   }

   @Override
   public Statement createStatement() throws SQLException {
      LoggingStatement statement = new LoggingStatement(connection.createStatement(), logger);
      logger.logf(level, "%s.createStatement() = %s", connectionID, statement.getStatementID());
      return statement;
   }

   @Override
   public PreparedStatement prepareStatement(String sql) throws SQLException {
      LoggingPreparedStatement statement = new LoggingPreparedStatement(connection.prepareStatement(sql), logger);
      logger.logf(level, "%s.prepareStatement(%s) = %s", connectionID, sql, statement.getStatementID());
      return statement;
   }

   @Override
   public CallableStatement prepareCall(String sql) throws SQLException {
      CallableStatement statement = connection.prepareCall(sql);
      logger.logf(level, "%s.prepareCall(%s) = %s", connectionID, sql, LoggingUtil.getID(statement));
      return statement;
   }

   @Override
   public String nativeSQL(String sql) throws SQLException {
      String x = connection.nativeSQL(sql);
      logger.logf(level, "%s.nativeSQL(%s) = %s", connectionID, sql, x);
      return x;
   }

   @Override
   public void setAutoCommit(boolean autoCommit) throws SQLException {
      logger.logf(level, "%s.setAutoCommit(%s)", connectionID, autoCommit);
      connection.setAutoCommit(autoCommit);
   }

   @Override
   public boolean getAutoCommit() throws SQLException {
      boolean x = connection.getAutoCommit();
      logger.logf(level, "%s.getAutoCommit() = %s", connectionID, x);
      return x;
   }

   @Override
   public void commit() throws SQLException {
      logger.logf(level, "%s.commit()", connectionID);
      connection.commit();
   }

   @Override
   public void rollback() throws SQLException {
      logger.logf(level, "%s.rollback()", connectionID);
      connection.rollback();
   }

   @Override
   public void close() throws SQLException {
      logger.logf(level, "%s.close()", connectionID);
      connection.close();
   }

   @Override
   public boolean isClosed() throws SQLException {
      boolean x = connection.isClosed();
      logger.logf(level, "%s.isClosed() = %s", connectionID, x);
      return x;
   }

   @Override
   public DatabaseMetaData getMetaData() throws SQLException {
      DatabaseMetaData x = connection.getMetaData();
      logger.logf(level, "%s.getMetaData() = %s", connectionID, x);
      return x;
   }

   @Override
   public void setReadOnly(boolean readOnly) throws SQLException {
      logger.logf(level, "%s.setReadOnly(%s)", connectionID, readOnly);
      connection.setReadOnly(readOnly);
   }

   @Override
   public boolean isReadOnly() throws SQLException {
      boolean x = connection.isReadOnly();
      logger.logf(level, "%s.isReadOnly() = %s", connectionID, x);
      return x;
   }

   @Override
   public void setCatalog(String catalog) throws SQLException {
      logger.logf(level, "%s.setCatalog(%s)", connectionID, catalog);
      connection.setCatalog(catalog);
   }

   @Override
   public String getCatalog() throws SQLException {
      String x = connection.getCatalog();
      logger.logf(level, "%s.getCatalog() = %s", connectionID, x);
      return x;
   }

   @Override
   public void setTransactionIsolation(int level) throws SQLException {
      logger.logf(this.level, "%s.setTransactionIsolation(%s)", connectionID, level);
      connection.setTransactionIsolation(level);
   }

   @Override
   public int getTransactionIsolation() throws SQLException {
      int x = connection.getTransactionIsolation();
      logger.logf(level, "%s.getTransactionIsolation() = %s", connectionID, x);
      return x;
   }

   @Override
   public SQLWarning getWarnings() throws SQLException {
      SQLWarning x = connection.getWarnings();
      logger.logf(level, "%s.getWarnings() = %s", connectionID, x);
      return x;
   }

   @Override
   public void clearWarnings() throws SQLException {
      logger.logf(level, "%s.clearWarnings()", connectionID);
      connection.clearWarnings();
   }

   @Override
   public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
      LoggingStatement statement = new LoggingStatement(connection.createStatement(resultSetType, resultSetConcurrency), logger);
      logger.logf(level, "%s.createStatement(%s, %s) = %s", connectionID, resultSetType, resultSetConcurrency, statement.getStatementID());
      return statement;
   }

   @Override
   public PreparedStatement prepareStatement(String sql,
                                             int resultSetType,
                                             int resultSetConcurrency) throws SQLException {
      LoggingPreparedStatement statement = new LoggingPreparedStatement(connection.prepareStatement(sql, resultSetType, resultSetConcurrency), logger);
      logger.logf(level, "%s.prepareStatement(%s, %s, %s) = %s", connectionID, sql, resultSetType, resultSetConcurrency, statement.getStatementID());
      return statement;
   }

   @Override
   public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      CallableStatement statement = connection.prepareCall(sql, resultSetType, resultSetConcurrency);
      logger.logf(level, "%s.createStatement(%s, %s) = %s", connectionID, sql, resultSetType, resultSetConcurrency, LoggingUtil.getID(statement));
      return statement;
   }

   @Override
   public Map<String, Class<?>> getTypeMap() throws SQLException {
      Map<String, Class<?>> x = connection.getTypeMap();
      logger.logf(level, "%s.getTypeMap() = %s", connectionID, x);
      return x;
   }

   @Override
   public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
      logger.logf(level, "%s.setTypeMap(%s)", connectionID, map);
      connection.setTypeMap(map);
   }

   @Override
   public void setHoldability(int holdability) throws SQLException {
      logger.logf(level, "%s.setHoldability(%s)", connectionID, holdability);
      connection.setHoldability(holdability);
   }

   @Override
   public int getHoldability() throws SQLException {
      int x = connection.getHoldability();
      logger.logf(level, "%s.getHoldability() = %s", connectionID, x);
      return x;
   }

   @Override
   public Savepoint setSavepoint() throws SQLException {
      Savepoint x = connection.setSavepoint();
      logger.logf(level, "%s.setSavepoint() = %s", connectionID, x);
      return x;
   }

   @Override
   public Savepoint setSavepoint(String name) throws SQLException {
      Savepoint x = connection.setSavepoint(name);
      logger.logf(level, "%s.setSavepoint(%s) = %s", connectionID, name, x);
      return x;
   }

   @Override
   public void rollback(Savepoint savepoint) throws SQLException {
      logger.logf(level, "%s.rollback(%s)", connectionID, savepoint);
      connection.rollback(savepoint);
   }

   @Override
   public void releaseSavepoint(Savepoint savepoint) throws SQLException {
      logger.logf(level, "%s.releaseSavepoint(%s)", connectionID, savepoint);
      connection.releaseSavepoint(savepoint);
   }

   @Override
   public Statement createStatement(int resultSetType,
                                    int resultSetConcurrency,
                                    int resultSetHoldability) throws SQLException {
      LoggingStatement statement = new LoggingStatement(connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), logger);
      logger.logf(level, "%s.createStatement(%s, %s, %s) = %s", connectionID, resultSetType, resultSetConcurrency, resultSetHoldability, statement.getStatementID());
      return statement;
   }

   @Override
   public PreparedStatement prepareStatement(String sql,
                                             int resultSetType,
                                             int resultSetConcurrency,
                                             int resultSetHoldability) throws SQLException {
      LoggingPreparedStatement statement = new LoggingPreparedStatement(connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability), logger);
      logger.logf(level, "%s.prepareStatement(%s, %s, %s, %s) = %s", connectionID, sql, resultSetType, resultSetConcurrency, resultSetHoldability, statement.getStatementID());
      return statement;
   }

   @Override
   public CallableStatement prepareCall(String sql,
                                        int resultSetType,
                                        int resultSetConcurrency,
                                        int resultSetHoldability) throws SQLException {
      CallableStatement statement = connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
      logger.logf(level, "%s.prepareCall(%s, %s, %s, %s) = %s", connectionID, sql, resultSetType, resultSetConcurrency, resultSetHoldability, LoggingUtil.getID(statement));
      return statement;
   }

   @Override
   public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
      LoggingPreparedStatement preparedStatement = new LoggingPreparedStatement(connection.prepareStatement(sql, autoGeneratedKeys), logger);
      logger.logf(level, "%s.prepareStatement(%s, %s) = %s", connectionID, sql, autoGeneratedKeys, preparedStatement.getStatementID());
      return preparedStatement;
   }

   @Override
   public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
      LoggingPreparedStatement statement = new LoggingPreparedStatement(connection.prepareStatement(sql, columnIndexes), logger);
      logger.logf(level, "%s.prepareStatement(%s, %s) = %s", connectionID, sql, Arrays.toString(columnIndexes), statement.getStatementID());
      return statement;
   }

   @Override
   public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
      LoggingPreparedStatement statement = new LoggingPreparedStatement(connection.prepareStatement(sql, columnNames), logger);
      logger.logf(level, "%s.prepareStatement(%s, %s) = %s", connectionID, sql, Arrays.toString(columnNames), statement.getStatementID());
      return statement;
   }

   @Override
   public Clob createClob() throws SQLException {
      Clob x = connection.createClob();
      logger.logf(level, "%s.createClob() = %s", connectionID, x);
      return x;
   }

   @Override
   public Blob createBlob() throws SQLException {
      Blob x = connection.createBlob();
      logger.logf(level, "%s.createBlob() = %s", connectionID, x);
      return x;
   }

   @Override
   public NClob createNClob() throws SQLException {
      NClob x = connection.createNClob();
      logger.logf(level, "%s.createNClob() = %s", connectionID, x);
      return x;
   }

   @Override
   public SQLXML createSQLXML() throws SQLException {
      SQLXML x = connection.createSQLXML();
      logger.logf(level, "%s.createSQLXML() = %s", connectionID, x);
      return x;
   }

   @Override
   public boolean isValid(int timeout) throws SQLException {
      boolean x = connection.isValid(timeout);
      logger.logf(level, "%s.isValid(%s) = %s", connectionID, timeout, x);
      return x;
   }

   @Override
   public void setClientInfo(String name, String value) throws SQLClientInfoException {
      logger.logf(level, "%s.setClientInfo(%s, %s)", connectionID, name, value);
      connection.setClientInfo(name, value);
   }

   @Override
   public void setClientInfo(Properties properties) throws SQLClientInfoException {
      logger.logf(level, "%s.setClientInfo(%s)", connectionID, properties);
      connection.setClientInfo(properties);
   }

   @Override
   public String getClientInfo(String name) throws SQLException {
      String x = connection.getClientInfo(name);
      logger.logf(level, "%s.getClientInfo(%s) = %s", connectionID, name, x);
      return x;
   }

   @Override
   public Properties getClientInfo() throws SQLException {
      Properties x = connection.getClientInfo();
      logger.logf(level, "%s.getClientInfo() = %s", connectionID, x);
      return x;
   }

   @Override
   public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
      Array x = connection.createArrayOf(typeName, elements);
      logger.logf(level, "%s.createArrayOf(%s, %s) = %s", connectionID, typeName, Arrays.toString(elements), x);
      return x;
   }

   @Override
   public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
      Struct x = connection.createStruct(typeName, attributes);
      logger.logf(level, "%s.createStruct(%s, %s) = %s", connectionID, typeName, Arrays.toString(attributes), x);
      return x;
   }

   @Override
   public void setSchema(String schema) throws SQLException {
      logger.logf(level, "%s.setSchema(%s)", connectionID, schema);
      connection.setSchema(schema);
   }

   @Override
   public String getSchema() throws SQLException {
      String x = connection.getSchema();
      logger.logf(level, "%s.getSchema() = %s", connectionID, x);
      return x;
   }

   @Override
   public void abort(Executor executor) throws SQLException {
      logger.logf(level, "%s.abort(%s)", connectionID, executor);
      connection.abort(executor);
   }

   @Override
   public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
      logger.logf(level, "%s.setNetworkTimeout(%s, %d)", connectionID, executor, milliseconds);
      connection.setNetworkTimeout(executor, milliseconds);
   }

   @Override
   public int getNetworkTimeout() throws SQLException {
      int x = connection.getNetworkTimeout();
      logger.logf(level, "%s.getNetworkTimeout() = %s", connectionID, x);
      return x;
   }

   @Override
   public <T> T unwrap(Class<T> iface) throws SQLException {
      T x = connection.unwrap(iface);
      logger.logf(level, "%s.unwrap(%s) = %s", connectionID, iface, x);
      return x;
   }

   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      boolean x = connection.isWrapperFor(iface);
      logger.logf(level, "%s.isWrapperFor() = %s", connectionID, iface, x);
      return x;
   }
}
