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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;

import org.jboss.logging.Logger;

public class LoggingStatement implements Statement {

   private final Statement statement;

   protected final String statementID;

   protected final Logger logger;

   protected static Logger.Level level = Logger.Level.TRACE;

   public LoggingStatement(Statement statement, Logger logger) {
      this.statement = statement;
      this.logger = logger;
      this.statementID = LoggingUtil.getID(statement);
   }

   public Statement getStatement() {
      return statement;
   }

   public String getStatementID() {
      return statementID;
   }

   @Override
   public ResultSet executeQuery(String sql) throws SQLException {
      LoggingResultSet rs = new LoggingResultSet(statement.executeQuery(sql), logger);
      logger.logf(level, "%s.executeQuery(%s) = %s", statementID, sql, rs.getResultSetID());
      return rs;
   }

   @Override
   public int executeUpdate(String sql) throws SQLException {
      int i = statement.executeUpdate(sql);
      logger.logf(level, "%s.executeUpdate(%s) = %s", statementID, sql, i);
      return i;
   }

   @Override
   public void close() throws SQLException {
      logger.logf(level, "%s.close()", statementID);
      statement.close();
   }

   @Override
   public int getMaxFieldSize() throws SQLException {
      int i = statement.getMaxFieldSize();
      logger.logf(level, "%s.getMaxFieldSize() = %s", statementID, i);
      return i;
   }

   @Override
   public void setMaxFieldSize(int max) throws SQLException {
      logger.logf(level, "%s.setMaxFieldSize(%d)", statementID, max);
      statement.setMaxFieldSize(max);
   }

   @Override
   public int getMaxRows() throws SQLException {
      int i = statement.getMaxRows();
      logger.logf(level, "%s.getMaxRows() = %s", statementID, i);
      return i;
   }

   @Override
   public void setMaxRows(int max) throws SQLException {
      logger.logf(level, "%s.setMaxRows()", statementID);
      statement.setMaxRows(max);
   }

   @Override
   public void setEscapeProcessing(boolean enable) throws SQLException {
      logger.logf(level, "%s.setEscapeProcessing(%s)", statementID, enable);
      statement.setEscapeProcessing(enable);
   }

   @Override
   public int getQueryTimeout() throws SQLException {
      int i = statement.getQueryTimeout();
      logger.logf(level, "%s.getQueryTimeout() = %s", statementID, i);
      return i;
   }

   @Override
   public void setQueryTimeout(int seconds) throws SQLException {
      logger.logf(level, "%s.setQueryTimeout(%d)", statementID, seconds);
      statement.setQueryTimeout(seconds);
   }

   @Override
   public void cancel() throws SQLException {
      logger.logf(level, "%s.cancel()", statementID);
      statement.cancel();
   }

   @Override
   public SQLWarning getWarnings() throws SQLException {
      logger.logf(level, "%s.getWarnings()", statementID);
      return statement.getWarnings();
   }

   @Override
   public void clearWarnings() throws SQLException {
      logger.logf(level, "%s.clearWarnings()", statementID);
      statement.clearWarnings();
   }

   @Override
   public void setCursorName(String name) throws SQLException {
      logger.logf(level, "%s.setCursorName(%s)", statementID, name);
      statement.setCursorName(name);
   }

   @Override
   public boolean execute(String sql) throws SQLException {
      boolean b = statement.execute(sql);
      logger.logf(level, "%s.execute(%s) = %s", statementID, sql, b);
      return b;
   }

   @Override
   public ResultSet getResultSet() throws SQLException {
      LoggingResultSet rs = new LoggingResultSet(statement.getResultSet(), logger);
      logger.logf(level, "%s.executeQuery() = %s", statementID, rs.getResultSetID());
      return rs;
   }

   @Override
   public int getUpdateCount() throws SQLException {
      int i = statement.getUpdateCount();
      logger.logf(level, "%s.getUpdateCount() = %s", statementID, i);
      return i;
   }

   @Override
   public boolean getMoreResults() throws SQLException {
      boolean b = statement.getMoreResults();
      logger.logf(level, "%s.getMoreResults() = %s", statementID, b);
      return b;
   }

   @Override
   public void setFetchDirection(int direction) throws SQLException {
      logger.logf(level, "%s.setFetchDirection()", statementID);
      statement.setFetchDirection(direction);
   }

   @Override
   public int getFetchDirection() throws SQLException {
      int i = statement.getFetchDirection();
      logger.logf(level, "%s.getFetchDirection() = %s", statementID, i);
      return i;
   }

   @Override
   public void setFetchSize(int rows) throws SQLException {
      logger.logf(level, "%s.setFetchSize(%d)", statementID, rows);
      statement.setFetchSize(rows);
   }

   @Override
   public int getFetchSize() throws SQLException {
      int i = statement.getFetchSize();
      logger.logf(level, "%s.getFetchSize() = %s", statementID, i);
      return i;
   }

   @Override
   public int getResultSetConcurrency() throws SQLException {
      int i = statement.getResultSetConcurrency();
      logger.logf(level, "%s.getResultSetConcurrency() = %s", statementID, i);
      return i;
   }

   @Override
   public int getResultSetType() throws SQLException {
      int i = statement.getResultSetType();
      logger.logf(level, "%s.getResultSetType() = %s", statementID, i);
      return i;
   }

   @Override
   public void addBatch(String sql) throws SQLException {
      logger.logf(level, "%s.addBatch(%d, %s)", statementID);
      statement.addBatch(sql);
   }

   @Override
   public void clearBatch() throws SQLException {
      logger.logf(level, "%s.clearBatch()", statementID);
      statement.clearBatch();
   }

   @Override
   public int[] executeBatch() throws SQLException {
      int[] i = statement.executeBatch();
      logger.logf(level, "%s.executeBatch() = %s", statementID, Arrays.toString(i));
      return i;
   }

   @Override
   public Connection getConnection() throws SQLException {
      LoggingConnection connection = new LoggingConnection(statement.getConnection(), logger);
      logger.logf(level, "%s.getConnection() = %s", statementID, connection.getConnectionID());
      return connection;
   }

   @Override
   public boolean getMoreResults(int current) throws SQLException {
      boolean b = statement.getMoreResults(current);
      logger.logf(level, "%s.getMoreResults(%s) = %s", statementID, current, b);
      return b;
   }

   @Override
   public ResultSet getGeneratedKeys() throws SQLException {
      LoggingResultSet rs = new LoggingResultSet(statement.getGeneratedKeys(), logger);
      logger.logf(level, "%s.getGeneratedKeys() = %s", statementID, rs.getResultSetID());
      return rs;
   }

   @Override
   public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      int i = statement.executeUpdate(sql, autoGeneratedKeys);
      logger.logf(level, "%s.executeUpdate(%s, %d) = %s", statementID, sql, autoGeneratedKeys, i);
      return i;
   }

   @Override
   public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
      int i = statement.executeUpdate(sql, columnIndexes);
      logger.logf(level, "%s.executeUpdate(%s, %s) = %s", statementID, sql, Arrays.toString(columnIndexes), i);
      return i;
   }

   @Override
   public int executeUpdate(String sql, String[] columnNames) throws SQLException {
      int i = statement.executeUpdate(sql, columnNames);
      logger.logf(level, "%s.executeUpdate(%s, %s) = %s", statementID, sql, Arrays.toString(columnNames), i);
      return i;
   }

   @Override
   public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
      boolean b = statement.execute(sql, autoGeneratedKeys);
      logger.logf(level, "%s.execute(%s, %s) = %s", statementID, sql, autoGeneratedKeys, b);
      return b;
   }

   @Override
   public boolean execute(String sql, int[] columnIndexes) throws SQLException {
      boolean b = statement.execute(sql, columnIndexes);
      logger.logf(level, "%s.execute(%s, %s) = %s", statementID, sql, Arrays.toString(columnIndexes), b);
      return b;
   }

   @Override
   public boolean execute(String sql, String[] columnNames) throws SQLException {
      boolean b = statement.execute(sql, columnNames);
      logger.logf(level, "%s.execute(%s, %s) = %s", statementID, sql, Arrays.toString(columnNames), b);
      return b;
   }

   @Override
   public int getResultSetHoldability() throws SQLException {
      int i = statement.getResultSetHoldability();
      logger.logf(level, "%s.getResultSetHoldability() = %s", statementID, i);
      return i;
   }

   @Override
   public boolean isClosed() throws SQLException {
      boolean b = statement.isClosed();
      logger.logf(level, "%s.isClosed() = %s", statementID, b);
      return b;
   }

   @Override
   public void setPoolable(boolean poolable) throws SQLException {
      logger.logf(level, "%s.setPoolable(%s)", statementID, poolable);
      statement.setPoolable(poolable);
   }

   @Override
   public boolean isPoolable() throws SQLException {
      boolean b = statement.isPoolable();
      logger.logf(level, "%s.isPoolable() = %s", statementID, b);
      return b;
   }

   @Override
   public void closeOnCompletion() throws SQLException {
      logger.logf(level, "%s.closeOnCompletion()", statementID);
      statement.closeOnCompletion();
   }

   @Override
   public boolean isCloseOnCompletion() throws SQLException {
      boolean b = statement.isCloseOnCompletion();
      logger.logf(level, "%s.isCloseOnCompletion() = %s", statementID, b);
      return b;
   }

   @Override
   public long getLargeUpdateCount() throws SQLException {
      long x = statement.getLargeUpdateCount();
      logger.logf(level, "%s.getLargeUpdateCount() = %s", statementID, x);
      return x;
   }

   @Override
   public void setLargeMaxRows(long max) throws SQLException {
      logger.logf(level, "%s.setLargeMaxRows(%d)", statementID, max);
      statement.setLargeMaxRows(max);
   }

   @Override
   public long getLargeMaxRows() throws SQLException {
      long x = statement.getLargeMaxRows();
      logger.logf(level, "%s.getLargeMaxRows() = %s", statementID, x);
      return x;
   }

   @Override
   public long[] executeLargeBatch() throws SQLException {
      long[] x = statement.executeLargeBatch();
      logger.logf(level, "%s.executeLargeBatch() = %s", statementID, x);
      return x;
   }

   @Override
   public long executeLargeUpdate(String sql) throws SQLException {
      long x = statement.executeLargeUpdate(sql);
      logger.logf(level, "%s.executeLargeUpdate(%s) = %s", statementID, sql, x);
      return x;
   }

   @Override
   public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      long x = statement.executeLargeUpdate(sql, autoGeneratedKeys);
      logger.logf(level, "%s.executeLargeUpdate() = %s", statementID, sql, autoGeneratedKeys, x);
      return x;
   }

   @Override
   public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
      long x = statement.executeLargeUpdate(sql, columnIndexes);
      logger.logf(level, "%s.executeLargeUpdate(%s, %s) = %s", statementID, sql, Arrays.toString(columnIndexes), x);
      return x;
   }

   @Override
   public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
      long x = statement.executeLargeUpdate(sql, columnNames);
      logger.logf(level, "%s.executeLargeUpdate(%s, %s) = %s", statementID, sql, Arrays.toString(columnNames), x);
      return x;
   }

   @Override
   public <T> T unwrap(Class<T> iface) throws SQLException {
      T x = statement.unwrap(iface);
      logger.logf(level, "%s.unwrap(%s) = %s", statementID, iface, x);
      return x;
   }

   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      boolean b = statement.isWrapperFor(iface);
      logger.logf(level, "%s.isWrapperFor(%s) = %s", statementID, iface, b);
      return b;
   }
}
