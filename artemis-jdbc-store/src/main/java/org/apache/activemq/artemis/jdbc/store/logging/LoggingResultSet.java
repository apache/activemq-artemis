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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;

import org.jboss.logging.Logger;

public class LoggingResultSet implements ResultSet {

   private final ResultSet resultSet;

   private final String resultSetID;

   private final Logger logger;

   private static final Logger.Level level = Logger.Level.TRACE;

   public LoggingResultSet(ResultSet resultSet, Logger logger) {
      this.resultSet = resultSet;
      this.logger = logger;
      this.resultSetID = LoggingUtil.getID(resultSet);
   }

   public ResultSet getResultSet() {
      return resultSet;
   }

   public String getResultSetID() {
      return resultSetID;
   }

   @Override
   public boolean next() throws SQLException {
      boolean b = resultSet.next();
      logger.logf(level, "%s.next() = %s", resultSetID, b);
      return b;
   }

   @Override
   public void close() throws SQLException {
      logger.logf(level, "%s.close()", resultSetID);
      resultSet.close();
   }

   @Override
   public boolean wasNull() throws SQLException {
      boolean b = resultSet.wasNull();
      logger.logf(level, "%s.wasNull() = %s", resultSetID, b);
      return b;
   }

   @Override
   public String getString(int columnIndex) throws SQLException {
      String x = resultSet.getString(columnIndex);
      logger.logf(level, "%s.getString(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public boolean getBoolean(int columnIndex) throws SQLException {
      boolean x = resultSet.getBoolean(columnIndex);
      logger.logf(level, "%s.getBoolean(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public byte getByte(int columnIndex) throws SQLException {
      byte x = resultSet.getByte(columnIndex);
      logger.logf(level, "%s.getByte(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public short getShort(int columnIndex) throws SQLException {
      short x = resultSet.getShort(columnIndex);
      logger.logf(level, "%s.getShort(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public int getInt(int columnIndex) throws SQLException {
      int x = resultSet.getInt(columnIndex);
      logger.logf(level, "%s.getInt(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public long getLong(int columnIndex) throws SQLException {
      long x = resultSet.getLong(columnIndex);
      logger.logf(level, "%s.getLong(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public float getFloat(int columnIndex) throws SQLException {
      float x = resultSet.getFloat(columnIndex);
      logger.logf(level, "%s.getFloat(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public double getDouble(int columnIndex) throws SQLException {
      double x = resultSet.getDouble(columnIndex);
      logger.logf(level, "%s.getDouble(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
      BigDecimal x = resultSet.getBigDecimal(columnIndex);
      logger.logf(level, "%s.getBigDecimal(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public byte[] getBytes(int columnIndex) throws SQLException {
      byte[] x = resultSet.getBytes(columnIndex);
      logger.logf(level, "%s.getBytes(%s) = %s", resultSetID, columnIndex, Arrays.toString(x));
      return x;
   }

   @Override
   public Date getDate(int columnIndex) throws SQLException {
      Date x = resultSet.getDate(columnIndex);
      logger.logf(level, "%s.getDate(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public Time getTime(int columnIndex) throws SQLException {
      Time x = resultSet.getTime(columnIndex);
      logger.logf(level, "%s.getTime(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public Timestamp getTimestamp(int columnIndex) throws SQLException {
      Timestamp x = resultSet.getTimestamp(columnIndex);
      logger.logf(level, "%s.getTimestamp(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public InputStream getAsciiStream(int columnIndex) throws SQLException {
      InputStream x = resultSet.getAsciiStream(columnIndex);
      logger.logf(level, "%s.getAsciiStream(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public InputStream getUnicodeStream(int columnIndex) throws SQLException {
      InputStream x = resultSet.getUnicodeStream(columnIndex);
      logger.logf(level, "%s.getUnicodeStream(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public InputStream getBinaryStream(int columnIndex) throws SQLException {
      InputStream x = resultSet.getBinaryStream(columnIndex);
      logger.logf(level, "%s.getBinaryStream(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public String getString(String columnLabel) throws SQLException {
      String x = resultSet.getString(columnLabel);
      logger.logf(level, "%s.getString(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public boolean getBoolean(String columnLabel) throws SQLException {
      boolean x = resultSet.getBoolean(columnLabel);
      logger.logf(level, "%s.getBoolean(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public byte getByte(String columnLabel) throws SQLException {
      byte x = resultSet.getByte(columnLabel);
      logger.logf(level, "%s.getByte(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public short getShort(String columnLabel) throws SQLException {
      short x = resultSet.getShort(columnLabel);
      logger.logf(level, "%s.getShort(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public int getInt(String columnLabel) throws SQLException {
      int x = resultSet.getInt(columnLabel);
      logger.logf(level, "%s.getInt(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public long getLong(String columnLabel) throws SQLException {
      long x = resultSet.getLong(columnLabel);
      logger.logf(level, "%s.getLong(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public float getFloat(String columnLabel) throws SQLException {
      float x = resultSet.getFloat(columnLabel);
      logger.logf(level, "%s.getFloat(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public double getDouble(String columnLabel) throws SQLException {
      double x = resultSet.getDouble(columnLabel);
      logger.logf(level, "%s.getDouble(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
      BigDecimal x = resultSet.getBigDecimal(columnLabel);
      logger.logf(level, "%s.getBigDecimal(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public byte[] getBytes(String columnLabel) throws SQLException {
      byte[] x = resultSet.getBytes(columnLabel);
      logger.logf(level, "%s.getBytes(%s) = %s", resultSetID, columnLabel, Arrays.toString(x));
      return x;
   }

   @Override
   public Date getDate(String columnLabel) throws SQLException {
      Date x = resultSet.getDate(columnLabel);
      logger.logf(level, "%s.getDate(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public Time getTime(String columnLabel) throws SQLException {
      Time x = resultSet.getTime(columnLabel);
      logger.logf(level, "%s.getTime(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public Timestamp getTimestamp(String columnLabel) throws SQLException {
      Timestamp x = resultSet.getTimestamp(columnLabel);
      logger.logf(level, "%s.getTimestamp(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public InputStream getAsciiStream(String columnLabel) throws SQLException {
      InputStream x = resultSet.getAsciiStream(columnLabel);
      logger.logf(level, "%s.getAsciiStream(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public InputStream getUnicodeStream(String columnLabel) throws SQLException {
      InputStream x = resultSet.getUnicodeStream(columnLabel);
      logger.logf(level, "%s.getUnicodeStream(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public InputStream getBinaryStream(String columnLabel) throws SQLException {
      InputStream x = resultSet.getBinaryStream(columnLabel);
      logger.logf(level, "%s.getBinaryStream(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public SQLWarning getWarnings() throws SQLException {
      SQLWarning x = resultSet.getWarnings();
      logger.logf(level, "%s.getWarnings) = %s", resultSetID, x);
      return x;
   }

   @Override
   public void clearWarnings() throws SQLException {
      logger.logf(level, "%s.clearWarnings()", resultSetID);
      resultSet.clearWarnings();
   }

   @Override
   public String getCursorName() throws SQLException {
      String x = resultSet.getCursorName();
      logger.logf(level, "%s.getCursorName() = %s", resultSetID, x);
      return x;
   }

   @Override
   public ResultSetMetaData getMetaData() throws SQLException {
      ResultSetMetaData x = resultSet.getMetaData();
      logger.logf(level, "%s.getMetaData() = %s", resultSetID, x);
      return x;
   }

   @Override
   public Object getObject(int columnIndex) throws SQLException {
      String x = resultSet.getString(columnIndex);
      logger.logf(level, "%s.getString(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public Object getObject(String columnLabel) throws SQLException {
      Object x = resultSet.getObject(columnLabel);
      logger.logf(level, "%s.getObject(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public int findColumn(String columnLabel) throws SQLException {
      int x = resultSet.findColumn(columnLabel);
      logger.logf(level, "%s.findColumn(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public Reader getCharacterStream(int columnIndex) throws SQLException {
      Reader x = resultSet.getCharacterStream(columnIndex);
      logger.logf(level, "%s.getCharacterStream(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public Reader getCharacterStream(String columnLabel) throws SQLException {
      Reader x = resultSet.getCharacterStream(columnLabel);
      logger.logf(level, "%s.getCharacterStream(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
      BigDecimal x = resultSet.getBigDecimal(columnIndex);
      logger.logf(level, "%s.getBigDecimal(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
      BigDecimal x = resultSet.getBigDecimal(columnLabel);
      logger.logf(level, "%s.getBigDecimal(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public boolean isBeforeFirst() throws SQLException {
      boolean x = resultSet.isBeforeFirst();
      logger.logf(level, "%s.isBeforeFirst() = %s", resultSetID, x);
      return x;
   }

   @Override
   public boolean isAfterLast() throws SQLException {
      boolean x = resultSet.isAfterLast();
      logger.logf(level, "%s.isAfterLast() = %s", resultSetID, x);
      return x;
   }

   @Override
   public boolean isFirst() throws SQLException {
      boolean x = resultSet.isFirst();
      logger.logf(level, "%s.isFirst() = %s", resultSetID, x);
      return x;
   }

   @Override
   public boolean isLast() throws SQLException {
      boolean x = resultSet.isLast();
      logger.logf(level, "%s.isLast() = %s", resultSetID, x);
      return x;
   }

   @Override
   public void beforeFirst() throws SQLException {
      logger.logf(level, "%s.beforeFirst()", resultSetID);
      resultSet.beforeFirst();
   }

   @Override
   public void afterLast() throws SQLException {
      logger.logf(level, "%s.afterLast()", resultSetID);
      resultSet.afterLast();
   }

   @Override
   public boolean first() throws SQLException {
      boolean x = resultSet.first();
      logger.logf(level, "%s.first() = %s", resultSetID, x);
      return x;
   }

   @Override
   public boolean last() throws SQLException {
      boolean x = resultSet.last();
      logger.logf(level, "%s.last() = %s", resultSetID, x);
      return x;
   }

   @Override
   public int getRow() throws SQLException {
      int x = resultSet.getRow();
      logger.logf(level, "%s.getRow() = %s", resultSetID, x);
      return x;
   }

   @Override
   public boolean absolute(int row) throws SQLException {
      boolean x = resultSet.absolute(row);
      logger.logf(level, "%s.absolute(%s) = %s", resultSetID, row, x);
      return x;
   }

   @Override
   public boolean relative(int rows) throws SQLException {
      boolean x = resultSet.relative(rows);
      logger.logf(level, "%s.relative(%s) = %s", resultSetID, rows, x);
      return x;
   }

   @Override
   public boolean previous() throws SQLException {
      boolean x = resultSet.previous();
      logger.logf(level, "%s.previous() = %s", resultSetID, x);
      return x;
   }

   @Override
   public void setFetchDirection(int direction) throws SQLException {
      logger.logf(level, "%s.setFetchDirection(%s)", resultSetID, direction);
      resultSet.setFetchDirection(direction);
   }

   @Override
   public int getFetchDirection() throws SQLException {
      int x = resultSet.getFetchDirection();
      logger.logf(level, "%s.getFetchDirection() = %s", resultSetID, x);
      return x;
   }

   @Override
   public void setFetchSize(int rows) throws SQLException {
      logger.logf(level, "%s.setFetchSize(%s)", resultSetID, rows);
      resultSet.setFetchSize(rows);
   }

   @Override
   public int getFetchSize() throws SQLException {
      int x = resultSet.getFetchSize();
      logger.logf(level, "%s.getFetchSize() = %s", resultSetID, x);
      return x;
   }

   @Override
   public int getType() throws SQLException {
      int x = resultSet.getType();
      logger.logf(level, "%s.getType() = %s", resultSetID, x);
      return x;
   }

   @Override
   public int getConcurrency() throws SQLException {
      int x = resultSet.getConcurrency();
      logger.logf(level, "%s.getConcurrency() = %s", resultSetID, x);
      return x;
   }

   @Override
   public boolean rowUpdated() throws SQLException {
      boolean x = resultSet.rowUpdated();
      logger.logf(level, "%s.rowUpdated() = %s", resultSetID, x);
      return x;
   }

   @Override
   public boolean rowInserted() throws SQLException {
      boolean x = resultSet.rowInserted();
      logger.logf(level, "%s.rowInserted() = %s", resultSetID, x);
      return x;
   }

   @Override
   public boolean rowDeleted() throws SQLException {
      boolean x = resultSet.rowDeleted();
      logger.logf(level, "%s.rowDeleted() = %s", resultSetID, x);
      return x;
   }

   @Override
   public void updateNull(int columnIndex) throws SQLException {
      logger.logf(level, "%s.updateNull(%s)", resultSetID, columnIndex);
      resultSet.updateNull(columnIndex);
   }

   @Override
   public void updateBoolean(int columnIndex, boolean x) throws SQLException {
      logger.logf(level, "%s.updateBoolean(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateBoolean(columnIndex, x);
   }

   @Override
   public void updateByte(int columnIndex, byte x) throws SQLException {
      logger.logf(level, "%s.updateByte(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateByte(columnIndex, x);
   }

   @Override
   public void updateShort(int columnIndex, short x) throws SQLException {
      logger.logf(level, "%s.updateShort(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateShort(columnIndex, x);
   }

   @Override
   public void updateInt(int columnIndex, int x) throws SQLException {
      logger.logf(level, "%s.updateInt(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateInt(columnIndex, x);
   }

   @Override
   public void updateLong(int columnIndex, long x) throws SQLException {
      logger.logf(level, "%s.updateLong(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateLong(columnIndex, x);
   }

   @Override
   public void updateFloat(int columnIndex, float x) throws SQLException {
      logger.logf(level, "%s.updateFloat(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateFloat(columnIndex, x);
   }

   @Override
   public void updateDouble(int columnIndex, double x) throws SQLException {
      logger.logf(level, "%s.updateDouble(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateDouble(columnIndex, x);
   }

   @Override
   public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
      logger.logf(level, "%s.updateBigDecimal(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateBigDecimal(columnIndex, x);
   }

   @Override
   public void updateString(int columnIndex, String x) throws SQLException {
      logger.logf(level, "%s.updateString(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateString(columnIndex, x);
   }

   @Override
   public void updateBytes(int columnIndex, byte[] x) throws SQLException {
      logger.logf(level, "%s.updateBytes(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateBytes(columnIndex, x);
   }

   @Override
   public void updateDate(int columnIndex, Date x) throws SQLException {
      logger.logf(level, "%s.updateDate(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateDate(columnIndex, x);
   }

   @Override
   public void updateTime(int columnIndex, Time x) throws SQLException {
      logger.logf(level, "%s.updateTime(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateTime(columnIndex, x);
   }

   @Override
   public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
      logger.logf(level, "%s.updateTimestamp(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateTimestamp(columnIndex, x);
   }

   @Override
   public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
      logger.logf(level, "%s.updateAsciiStream(%s, %s, %s)", resultSetID, columnIndex, x, length);
      resultSet.updateAsciiStream(columnIndex, x, length);
   }

   @Override
   public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
      logger.logf(level, "%s.updateBinaryStream(%s, %s, %s)", resultSetID, columnIndex, x, length);
      resultSet.updateBinaryStream(columnIndex, x, length);
   }

   @Override
   public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
      logger.logf(level, "%s.updateCharacterStream(%s, %s, %s)", resultSetID, columnIndex, x, length);
      resultSet.updateCharacterStream(columnIndex, x, length);
   }

   @Override
   public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
      logger.logf(level, "%s.updateObject(%s, %s, %s)", resultSetID, columnIndex, x, scaleOrLength);
      resultSet.updateObject(columnIndex, x, scaleOrLength);
   }

   @Override
   public void updateObject(int columnIndex, Object x) throws SQLException {
      logger.logf(level, "%s.updateObject(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateObject(columnIndex, x);
   }

   @Override
   public void updateNull(String columnLabel) throws SQLException {
      logger.logf(level, "%s.updateNull(%s)", resultSetID, columnLabel);
      resultSet.updateNull(columnLabel);
   }

   @Override
   public void updateBoolean(String columnLabel, boolean x) throws SQLException {
      logger.logf(level, "%s.updateBoolean(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateBoolean(columnLabel, x);
   }

   @Override
   public void updateByte(String columnLabel, byte x) throws SQLException {
      logger.logf(level, "%s.updateByte(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateByte(columnLabel, x);
   }

   @Override
   public void updateShort(String columnLabel, short x) throws SQLException {
      logger.logf(level, "%s.updateShort(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateShort(columnLabel, x);
   }

   @Override
   public void updateInt(String columnLabel, int x) throws SQLException {
      logger.logf(level, "%s.updateInt(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateInt(columnLabel, x);
   }

   @Override
   public void updateLong(String columnLabel, long x) throws SQLException {
      logger.logf(level, "%s.updateLong(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateLong(columnLabel, x);
   }

   @Override
   public void updateFloat(String columnLabel, float x) throws SQLException {
      logger.logf(level, "%s.updateFloat(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateFloat(columnLabel, x);
   }

   @Override
   public void updateDouble(String columnLabel, double x) throws SQLException {
      logger.logf(level, "%s.updateDouble(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateDouble(columnLabel, x);
   }

   @Override
   public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
      logger.logf(level, "%s.updateBigDecimal(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateBigDecimal(columnLabel, x);
   }

   @Override
   public void updateString(String columnLabel, String x) throws SQLException {
      logger.logf(level, "%s.updateString(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateString(columnLabel, x);
   }

   @Override
   public void updateBytes(String columnLabel, byte[] x) throws SQLException {
      logger.logf(level, "%s.updateBytes(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateBytes(columnLabel, x);
   }

   @Override
   public void updateDate(String columnLabel, Date x) throws SQLException {
      logger.logf(level, "%s.updateDate(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateDate(columnLabel, x);
   }

   @Override
   public void updateTime(String columnLabel, Time x) throws SQLException {
      logger.logf(level, "%s.updateTime(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateTime(columnLabel, x);
   }

   @Override
   public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
      logger.logf(level, "%s.updateTimestamp(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateTimestamp(columnLabel, x);
   }

   @Override
   public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
      logger.logf(level, "%s.updateAsciiStream(%s, %s, %s)", resultSetID, columnLabel, x, length);
      resultSet.updateAsciiStream(columnLabel, x, length);
   }

   @Override
   public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
      logger.logf(level, "%s.updateBinaryStream(%s, %s, %s)", resultSetID, columnLabel, x, length);
      resultSet.updateBinaryStream(columnLabel, x, length);
   }

   @Override
   public void updateCharacterStream(String columnLabel, Reader x, int length) throws SQLException {
      logger.logf(level, "%s.updateCharacterStream(%s, %s, %s)", resultSetID, columnLabel, x, length);
      resultSet.updateCharacterStream(columnLabel, x, length);
   }

   @Override
   public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
      logger.logf(level, "%s.updateObject(%s, %s, %s)", resultSetID, columnLabel, x, scaleOrLength);
      resultSet.updateObject(columnLabel, x, scaleOrLength);
   }

   @Override
   public void updateObject(String columnLabel, Object x) throws SQLException {
      logger.logf(level, "%s.updateObject(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateObject(columnLabel, x);
   }

   @Override
   public void insertRow() throws SQLException {
      logger.logf(level, "%s.insertRow()", resultSetID);
      resultSet.insertRow();
   }

   @Override
   public void updateRow() throws SQLException {
      logger.logf(level, "%s.updateRow()", resultSetID);
      resultSet.updateRow();
   }

   @Override
   public void deleteRow() throws SQLException {
      logger.logf(level, "%s.deleteRow()", resultSetID);
      resultSet.deleteRow();
   }

   @Override
   public void refreshRow() throws SQLException {
      logger.logf(level, "%s.refreshRow()", resultSetID);
      resultSet.refreshRow();
   }

   @Override
   public void cancelRowUpdates() throws SQLException {
      logger.logf(level, "%s.cancelRowUpdates()", resultSetID);
      resultSet.cancelRowUpdates();
   }

   @Override
   public void moveToInsertRow() throws SQLException {
      logger.logf(level, "%s.moveToInsertRow()", resultSetID);
      resultSet.moveToInsertRow();
   }

   @Override
   public void moveToCurrentRow() throws SQLException {
      logger.logf(level, "%s.moveToCurrentRow()", resultSetID);
      resultSet.moveToCurrentRow();
   }

   @Override
   public Statement getStatement() throws SQLException {
      Statement x = resultSet.getStatement();
      logger.logf(level, "%s.getStatement() = %s", resultSetID, x);
      return x;
   }

   @Override
   public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
      Object x = resultSet.getObject(columnIndex, map);
      logger.logf(level, "%s.getObject(%s, %s) = %s", resultSetID, columnIndex, map, x);
      return x;
   }

   @Override
   public Ref getRef(int columnIndex) throws SQLException {
      Ref x = resultSet.getRef(columnIndex);
      logger.logf(level, "%s.getRef(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public Blob getBlob(int columnIndex) throws SQLException {
      Blob x = resultSet.getBlob(columnIndex);
      logger.logf(level, "%s.getBlob(%s) = %s (length: %d)", resultSetID, columnIndex, x, x == null ? null : x.length());
      return x;
   }

   @Override
   public Clob getClob(int columnIndex) throws SQLException {
      Clob x = resultSet.getClob(columnIndex);
      logger.logf(level, "%s.getClob(%s) = %s (length: %d)", resultSetID, columnIndex, x, x == null ? null : x.length());
      return x;
   }

   @Override
   public Array getArray(int columnIndex) throws SQLException {
      Array x = resultSet.getArray(columnIndex);
      logger.logf(level, "%s.getArray(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
      Object x = resultSet.getObject(columnLabel, map);
      logger.logf(level, "%s.getObject(%s, %s) = %s", resultSetID, columnLabel, map, x);
      return x;
   }

   @Override
   public Ref getRef(String columnLabel) throws SQLException {
      Ref x = resultSet.getRef(columnLabel);
      logger.logf(level, "%s.getRef(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public Blob getBlob(String columnLabel) throws SQLException {
      Blob x = resultSet.getBlob(columnLabel);
      logger.logf(level, "%s.getBlob(%s) = %s (length: %d)", resultSetID, columnLabel, x, x == null ? null : x.length());
      return x;
   }

   @Override
   public Clob getClob(String columnLabel) throws SQLException {
      Clob x = resultSet.getClob(columnLabel);
      logger.logf(level, "%s.getClob(%s) = %s (length: %d)", resultSetID, columnLabel, x, x == null ? null : x.length());
      return x;
   }

   @Override
   public Array getArray(String columnLabel) throws SQLException {
      Array x = resultSet.getArray(columnLabel);
      logger.logf(level, "%s.getArray(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public Date getDate(int columnLabel, Calendar cal) throws SQLException {
      Date x = resultSet.getDate(columnLabel, cal);
      logger.logf(level, "%s.getDate(%s) = %s", resultSetID, columnLabel, cal, x);
      return x;
   }

   @Override
   public Date getDate(String columnLabel, Calendar cal) throws SQLException {
      Date x = resultSet.getDate(columnLabel, cal);
      logger.logf(level, "%s.getDate(%s) = %s", resultSetID, columnLabel, cal, x);
      return x;
   }

   @Override
   public Time getTime(int columnLabel, Calendar cal) throws SQLException {
      Time x = resultSet.getTime(columnLabel, cal);
      logger.logf(level, "%s.getTime(%s) = %s", resultSetID, columnLabel, cal, x);
      return x;
   }

   @Override
   public Time getTime(String columnLabel, Calendar cal) throws SQLException {
      Time x = resultSet.getTime(columnLabel, cal);
      logger.logf(level, "%s.getTime(%s) = %s", resultSetID, columnLabel, cal, x);
      return x;
   }

   @Override
   public Timestamp getTimestamp(int columnLabel, Calendar cal) throws SQLException {
      Timestamp x = resultSet.getTimestamp(columnLabel, cal);
      logger.logf(level, "%s.getTimestamp(%s) = %s", resultSetID, columnLabel, cal, x);
      return x;
   }

   @Override
   public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
      Timestamp x = resultSet.getTimestamp(columnLabel, cal);
      logger.logf(level, "%s.getTimestamp(%s) = %s", resultSetID, columnLabel, cal, x);
      return x;
   }

   @Override
   public URL getURL(int columnLabel) throws SQLException {
      URL x = resultSet.getURL(columnLabel);
      logger.logf(level, "%s.getURL(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public URL getURL(String columnLabel) throws SQLException {
      URL x = resultSet.getURL(columnLabel);
      logger.logf(level, "%s.getURL(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public void updateRef(int columnIndex, Ref x) throws SQLException {
      logger.logf(level, "%s.updateRef(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateRef(columnIndex, x);
   }

   @Override
   public void updateRef(String columnLabel, Ref x) throws SQLException {
      logger.logf(level, "%s.updateRef(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateRef(columnLabel, x);
   }

   @Override
   public void updateBlob(int columnIndex, Blob x) throws SQLException {
      logger.logf(level, "%s.updateBlob(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateBlob(columnIndex, x);
   }

   @Override
   public void updateBlob(String columnLabel, Blob x) throws SQLException {
      logger.logf(level, "%s.updateBlob(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateBlob(columnLabel, x);
   }

   @Override
   public void updateClob(int columnIndex, Clob x) throws SQLException {
      logger.logf(level, "%s.updateClob(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateClob(columnIndex, x);
   }

   @Override
   public void updateClob(String columnLabel, Clob x) throws SQLException {
      logger.logf(level, "%s.updateClob(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateClob(columnLabel, x);
   }

   @Override
   public void updateArray(int columnIndex, Array x) throws SQLException {
      logger.logf(level, "%s.updateArray(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateArray(columnIndex, x);
   }

   @Override
   public void updateArray(String columnLabel, Array x) throws SQLException {
      logger.logf(level, "%s.updateArray(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateArray(columnLabel, x);
   }

   @Override
   public RowId getRowId(int columnIndex) throws SQLException {
      RowId x = resultSet.getRowId(columnIndex);
      logger.logf(level, "%s.getRowId(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public RowId getRowId(String columnLabel) throws SQLException {
      RowId x = resultSet.getRowId(columnLabel);
      logger.logf(level, "%s.getRowId(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public void updateRowId(int columnIndex, RowId x) throws SQLException {
      logger.logf(level, "%s.updateRowId(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateRowId(columnIndex, x);
   }

   @Override
   public void updateRowId(String columnLabel, RowId x) throws SQLException {
      logger.logf(level, "%s.updateRowId(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateRowId(columnLabel, x);
   }

   @Override
   public int getHoldability() throws SQLException {
      int x = resultSet.getHoldability();
      logger.logf(level, "%s.getHoldability() = %s", resultSetID, x);
      return x;
   }

   @Override
   public boolean isClosed() throws SQLException {
      boolean x = resultSet.isClosed();
      logger.logf(level, "%s.isClosed() = %s", resultSetID, x);
      return x;
   }

   @Override
   public void updateNString(int columnIndex, String x) throws SQLException {
      logger.logf(level, "%s.updateNString(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateNString(columnIndex, x);
   }

   @Override
   public void updateNString(String columnLabel, String x) throws SQLException {
      logger.logf(level, "%s.updateNString(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateNString(columnLabel, x);
   }

   @Override
   public void updateNClob(int columnIndex, NClob x) throws SQLException {
      logger.logf(level, "%s.updateNClob(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateNClob(columnIndex, x);
   }

   @Override
   public void updateNClob(String columnLabel, NClob x) throws SQLException {
      logger.logf(level, "%s.updateNClob(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateNClob(columnLabel, x);
   }

   @Override
   public NClob getNClob(int columnIndex) throws SQLException {
      NClob x = resultSet.getNClob(columnIndex);
      logger.logf(level, "%s.getNClob(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public NClob getNClob(String columnLabel) throws SQLException {
      NClob x = resultSet.getNClob(columnLabel);
      logger.logf(level, "%s.getNClob(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public SQLXML getSQLXML(int columnIndex) throws SQLException {
      SQLXML x = resultSet.getSQLXML(columnIndex);
      logger.logf(level, "%s.getSQLXML(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public SQLXML getSQLXML(String columnLabel) throws SQLException {
      SQLXML x = resultSet.getSQLXML(columnLabel);
      logger.logf(level, "%s.getSQLXML(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public void updateSQLXML(int columnIndex, SQLXML x) throws SQLException {
      logger.logf(level, "%s.updateSQLXML(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateSQLXML(columnIndex, x);
   }

   @Override
   public void updateSQLXML(String columnLabel, SQLXML x) throws SQLException {
      logger.logf(level, "%s.updateSQLXML(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateSQLXML(columnLabel, x);
   }

   @Override
   public String getNString(int columnIndex) throws SQLException {
      String x = resultSet.getNString(columnIndex);
      logger.logf(level, "%s.getNString(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public String getNString(String columnLabel) throws SQLException {
      String x = resultSet.getNString(columnLabel);
      logger.logf(level, "%s.getNString(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public Reader getNCharacterStream(int columnIndex) throws SQLException {
      Reader x = resultSet.getNCharacterStream(columnIndex);
      logger.logf(level, "%s.getNCharacterStream(%s) = %s", resultSetID, columnIndex, x);
      return x;
   }

   @Override
   public Reader getNCharacterStream(String columnLabel) throws SQLException {
      Reader x = resultSet.getNCharacterStream(columnLabel);
      logger.logf(level, "%s.getNCharacterStream(%s) = %s", resultSetID, columnLabel, x);
      return x;
   }

   @Override
   public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
      logger.logf(level, "%s.updateNCharacterStream(%s, %s, %s)", resultSetID, columnIndex, x, length);
      resultSet.updateNCharacterStream(columnIndex, x, length);
   }

   @Override
   public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
      logger.logf(level, "%s.updateNCharacterStream(%s, %s, %s)", resultSetID, columnLabel, reader, length);
      resultSet.updateNCharacterStream(columnLabel, reader, length);
   }

   @Override
   public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
      logger.logf(level, "%s.updateAsciiStream(%s, %s, %s)", resultSetID, columnIndex, x, length);
      resultSet.updateAsciiStream(columnIndex, x, length);
   }

   @Override
   public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
      logger.logf(level, "%s.updateBinaryStream(%s, %s, %s)", resultSetID, columnIndex, x, length);
      resultSet.updateBinaryStream(columnIndex, x, length);
   }

   @Override
   public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
      logger.logf(level, "%s.updateCharacterStream(%s, %s, %s)", resultSetID, columnIndex, x, length);
      resultSet.updateCharacterStream(columnIndex, x, length);
   }

   @Override
   public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
      logger.logf(level, "%s.updateAsciiStream(%s, %s, %s)", resultSetID, columnLabel, x, length);
      resultSet.updateAsciiStream(columnLabel, x, length);
   }

   @Override
   public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
      logger.logf(level, "%s.updateBinaryStream(%s, %s, %s)", resultSetID, columnLabel, x, length);
      resultSet.updateBinaryStream(columnLabel, x, length);
   }

   @Override
   public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
      logger.logf(level, "%s.updateCharacterStream(%s, %s, %s)", resultSetID, columnLabel, reader, length);
      resultSet.updateCharacterStream(columnLabel, reader);
   }

   @Override
   public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
      logger.logf(level, "%s.updateBlob(%s, %s, %s)", resultSetID, columnIndex, inputStream, length);
      resultSet.updateBlob(columnIndex, inputStream, length);
   }

   @Override
   public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
      logger.logf(level, "%s.updateBlob(%s, %s, %s)", resultSetID, columnLabel, inputStream, length);
      resultSet.updateBlob(columnLabel, inputStream, length);
   }

   @Override
   public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
      logger.logf(level, "%s.updateClob(%s, %s, %s)", resultSetID, columnIndex, reader, length);
      resultSet.updateClob(columnIndex, reader, length);
   }

   @Override
   public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
      logger.logf(level, "%s.updateClob(%s, %s, %s)", resultSetID, columnLabel, reader, length);
      resultSet.updateClob(columnLabel, reader, length);
   }

   @Override
   public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
      logger.logf(level, "%s.updateNClob(%s, %s, %s)", resultSetID, columnIndex, reader, length);
      resultSet.updateNClob(columnIndex, reader, length);
   }

   @Override
   public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
      logger.logf(level, "%s.updateNClob(%s, %s, %s)", resultSetID, columnLabel, reader, length);
      resultSet.updateNClob(columnLabel, reader, length);
   }

   @Override
   public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
      logger.logf(level, "%s.updateNCharacterStream(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateNCharacterStream(columnIndex, x);
   }

   @Override
   public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
      logger.logf(level, "%s.updateNCharacterStream(%s, %s)", resultSetID, columnLabel, reader);
      resultSet.updateNCharacterStream(columnLabel, reader);
   }

   @Override
   public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
      logger.logf(level, "%s.updateAsciiStream(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateAsciiStream(columnIndex, x);
   }

   @Override
   public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
      logger.logf(level, "%s.updateBinaryStream(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateBinaryStream(columnIndex, x);
   }

   @Override
   public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
      logger.logf(level, "%s.updateCharacterStream(%s, %s)", resultSetID, columnIndex, x);
      resultSet.updateCharacterStream(columnIndex, x);
   }

   @Override
   public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
      logger.logf(level, "%s.updateAsciiStream(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateAsciiStream(columnLabel, x);
   }

   @Override
   public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
      logger.logf(level, "%s.updateBinaryStream(%s, %s)", resultSetID, columnLabel, x);
      resultSet.updateBinaryStream(columnLabel, x);
   }

   @Override
   public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
      logger.logf(level, "%s.updateCharacterStream(%s, %s)", resultSetID, columnLabel, reader);
      resultSet.updateCharacterStream(columnLabel, reader);
   }

   @Override
   public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
      logger.logf(level, "%s.updateBlob(%s, %s)", resultSetID, columnIndex, inputStream);
      resultSet.updateBlob(columnIndex, inputStream);
   }

   @Override
   public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
      logger.logf(level, "%s.updateBlob(%s, %s)", resultSetID, columnLabel, inputStream);
      resultSet.updateBlob(columnLabel, inputStream);
   }

   @Override
   public void updateClob(int columnIndex, Reader reader) throws SQLException {
      logger.logf(level, "%s.updateClob(%s, %s)", resultSetID, columnIndex, reader);
      resultSet.updateClob(columnIndex, reader);
   }

   @Override
   public void updateClob(String columnLabel, Reader reader) throws SQLException {
      logger.logf(level, "%s.updateClob(%s, %s)", resultSetID, columnLabel, reader);
      resultSet.updateClob(columnLabel, reader);
   }

   @Override
   public void updateNClob(int columnIndex, Reader reader) throws SQLException {
      logger.logf(level, "%s.updateNClob(%s, %s)", resultSetID, columnIndex, reader);
      resultSet.updateNClob(columnIndex, reader);
   }

   @Override
   public void updateNClob(String columnLabel, Reader reader) throws SQLException {
      logger.logf(level, "%s.updateNClob(%s, %s)", resultSetID, columnLabel, reader);
      resultSet.updateNClob(columnLabel, reader);
   }

   @Override
   public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
      T x = resultSet.getObject(columnIndex, type);
      logger.logf(level, "%s.getObject(%s, %s) = %s", resultSetID, columnIndex, type, x);
      return x;
   }

   @Override
   public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
      T x = resultSet.getObject(columnLabel, type);
      logger.logf(level, "%s.getObject(%s, %s) = %s", resultSetID, columnLabel, type, x);
      return x;
   }

   @Override
   public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
      logger.logf(level, "%s.updateObject(%s, %s, %s, %s)", resultSetID, columnIndex, x, targetSqlType, scaleOrLength);
      resultSet.updateObject(columnIndex, x, targetSqlType);
   }

   @Override
   public void updateObject(String columnLabel,
                            Object x,
                            SQLType targetSqlType,
                            int scaleOrLength) throws SQLException {
      logger.logf(level, "%s.updateObject(%s, %s, %s, %s)", resultSetID, columnLabel, x, targetSqlType, scaleOrLength);
      resultSet.updateObject(columnLabel, x, targetSqlType, scaleOrLength);
   }

   @Override
   public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
      logger.logf(level, "%s.updateObject(%s, %s, %s)", resultSetID, columnIndex, x, targetSqlType);
      resultSet.updateObject(columnIndex, x, targetSqlType);
   }

   @Override
   public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
      logger.logf(level, "%s.updateObject(%s, %s, %s)", resultSetID, columnLabel, x, targetSqlType);
      resultSet.updateObject(columnLabel, x, targetSqlType);
   }

   @Override
   public <T> T unwrap(Class<T> iface) throws SQLException {
      T x = resultSet.unwrap(iface);
      logger.logf(level, "%s.unwrap(%s) = %s", resultSetID, iface, x);
      return x;
   }

   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      boolean x = resultSet.isWrapperFor(iface);
      logger.logf(level, "%s.isWrapperFor(%s) = %s", resultSetID, iface, x);
      return x;
   }
}
