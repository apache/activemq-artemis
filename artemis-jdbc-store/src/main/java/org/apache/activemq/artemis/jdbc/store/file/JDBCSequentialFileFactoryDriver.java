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
package org.apache.activemq.artemis.jdbc.store.file;

import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.jdbc.store.drivers.AbstractJDBCDriver;

public class JDBCSequentialFileFactoryDriver extends AbstractJDBCDriver {

   protected PreparedStatement deleteFile;

   protected PreparedStatement createFile;

   protected PreparedStatement selectFileByFileName;

   protected PreparedStatement copyFileRecord;

   protected PreparedStatement renameFile;

   protected PreparedStatement readLargeObject;

   protected PreparedStatement appendToLargeObject;

   protected PreparedStatement selectFileNamesByExtension;

   public JDBCSequentialFileFactoryDriver() {
      super();
   }

   public JDBCSequentialFileFactoryDriver(String tableName, String jdbcConnectionUrl, String jdbcDriverClass) {
      super(tableName, jdbcConnectionUrl, jdbcDriverClass);
   }

   @Override
   protected void createSchema() throws SQLException {
      createTable(sqlProvider.getCreateFileTableSQL());
   }

   @Override
   protected void prepareStatements() throws SQLException {
      this.deleteFile = connection.prepareStatement(sqlProvider.getDeleteFileSQL());
      this.createFile = connection.prepareStatement(sqlProvider.getInsertFileSQL(), Statement.RETURN_GENERATED_KEYS);
      this.selectFileByFileName = connection.prepareStatement(sqlProvider.getSelectFileByFileName());
      this.copyFileRecord = connection.prepareStatement(sqlProvider.getCopyFileRecordByIdSQL());
      this.renameFile = connection.prepareStatement(sqlProvider.getUpdateFileNameByIdSQL());
      this.readLargeObject = connection.prepareStatement(sqlProvider.getReadLargeObjectSQL());
      this.appendToLargeObject = connection.prepareStatement(sqlProvider.getAppendToLargeObjectSQL());
      this.selectFileNamesByExtension = connection.prepareStatement(sqlProvider.getSelectFileNamesByExtensionSQL());
   }

   public synchronized List<String> listFiles(String extension) throws Exception {
      List<String> fileNames = new ArrayList<>();
      try {
         connection.setAutoCommit(false);
         selectFileNamesByExtension.setString(1, extension);
         try (ResultSet rs = selectFileNamesByExtension.executeQuery()) {
            while (rs.next()) {
               fileNames.add(rs.getString(1));
            }
         }
         connection.commit();
      }
      catch (SQLException e) {
         connection.rollback();
         throw e;
      }
      return fileNames;
   }

   /**
    * Opens the supplied file.  If the file does not exist in the database it will create a new one.
    *
    * @param file
    * @return
    * @throws SQLException
    */
   public void openFile(JDBCSequentialFile file) throws SQLException {
      int fileId = fileExists(file);
      if (fileId < 0) {
         createFile(file);
      }
      else {
         file.setId(fileId);
         loadFile(file);
      }
   }

   /**
    * Checks to see if a file with filename and extension exists.  If so returns the ID of the file or returns -1.
    *
    * @param file
    * @return
    * @throws SQLException
    */
   public synchronized int fileExists(JDBCSequentialFile file) throws SQLException {
      connection.setAutoCommit(false);
      selectFileByFileName.setString(1, file.getFileName());
      try (ResultSet rs = selectFileByFileName.executeQuery()) {
         int id = rs.next() ? rs.getInt(1) : -1;
         connection.commit();
         return id;
      }
      catch (Exception e) {
         connection.rollback();
         throw e;
      }
   }

   /**
    * Loads an existing file.
    *
    * @param file
    * @throws SQLException
    */
   public synchronized void loadFile(JDBCSequentialFile file) throws SQLException {
      connection.setAutoCommit(false);
      readLargeObject.setInt(1, file.getId());

      try (ResultSet rs = readLargeObject.executeQuery()) {
         if (rs.next()) {
            file.setWritePosition((int) rs.getBlob(1).length());
         }
         connection.commit();
      }
      catch (SQLException e) {
         connection.rollback();
         throw e;
      }
   }

   /**
    * Creates a new database row representing the supplied file.
    *
    * @param file
    * @throws SQLException
    */
   public synchronized void createFile(JDBCSequentialFile file) throws SQLException {
      try {
         connection.setAutoCommit(false);
         createFile.setString(1, file.getFileName());
         createFile.setString(2, file.getExtension());
         createFile.setBytes(3, new byte[0]);
         createFile.executeUpdate();
         try (ResultSet keys = createFile.getGeneratedKeys()) {
            keys.next();
            file.setId(keys.getInt(1));
         }
         connection.commit();
      }
      catch (SQLException e) {
         connection.rollback();
         throw e;
      }
   }

   /**
    * Updates the fileName field to the new value.
    *
    * @param file
    * @param newFileName
    * @throws SQLException
    */
   public synchronized void renameFile(JDBCSequentialFile file, String newFileName) throws SQLException {
      try {
         connection.setAutoCommit(false);
         renameFile.setString(1, newFileName);
         renameFile.setInt(2, file.getId());
         renameFile.executeUpdate();
         connection.commit();
      }
      catch (SQLException e) {
         connection.rollback();
         throw e;
      }
   }

   /**
    * Deletes the associated row in the database.
    *
    * @param file
    * @throws SQLException
    */
   public synchronized void deleteFile(JDBCSequentialFile file) throws SQLException {
      try {
         connection.setAutoCommit(false);
         deleteFile.setInt(1, file.getId());
         deleteFile.executeUpdate();
         connection.commit();
      }
      catch (SQLException e) {
         connection.rollback();
         throw e;
      }
   }

   /**
    * Persists data to this files associated database mapping.
    *
    * @param file
    * @param data
    * @return
    * @throws Exception
    */
   public synchronized int writeToFile(JDBCSequentialFile file, byte[] data) throws SQLException {
      try {
         connection.setAutoCommit(false);
         appendToLargeObject.setBytes(1, data);
         appendToLargeObject.setInt(2, file.getId());
         appendToLargeObject.executeUpdate();
         connection.commit();
         return data.length;
      }
      catch (SQLException e) {
         connection.rollback();
         throw e;
      }
   }

   /**
    * Reads data from the file (at file.readPosition) into the byteBuffer.
    *
    * @param file
    * @param bytes
    * @return
    * @throws Exception
    */
   public synchronized int readFromFile(JDBCSequentialFile file, ByteBuffer bytes) throws SQLException {
      connection.setAutoCommit(false);
      readLargeObject.setInt(1, file.getId());
      int readLength = 0;
      try (ResultSet rs = readLargeObject.executeQuery()) {
         if (rs.next()) {
            Blob blob = rs.getBlob(1);
            readLength = (int) calculateReadLength(blob.length(), bytes.remaining(), file.position());
            byte[] data = blob.getBytes(file.position() + 1, (int) readLength);
            bytes.put(data);
         }
         connection.commit();
         return readLength;
      }
      catch (Throwable e) {
         connection.rollback();
         throw e;
      }
   }

   /**
    * Copy the data content of FileFrom to FileTo
    *
    * @param fileFrom
    * @param fileTo
    * @throws SQLException
    */
   public synchronized void copyFileData(JDBCSequentialFile fileFrom, JDBCSequentialFile fileTo) throws SQLException {
      try {
         connection.setAutoCommit(false);
         copyFileRecord.setInt(1, fileFrom.getId());
         copyFileRecord.setInt(2, fileTo.getId());
         copyFileRecord.executeUpdate();
         connection.commit();
      }
      catch (SQLException e) {
         connection.rollback();
         throw e;
      }
   }

   /**
    * Drop all tables and data
    */
   @Override
   public synchronized void destroy() throws SQLException {
      try {
         connection.setAutoCommit(false);
         Statement statement = connection.createStatement();
         statement.executeUpdate(sqlProvider.getDropFileTableSQL());
         connection.commit();
      }
      catch (SQLException e) {
         connection.rollback();
         throw e;
      }
   }

   public long calculateReadLength(long objectLength, int bufferSpace, long readPosition) {
      long bytesRemaining = objectLength - readPosition;
      if (bytesRemaining > bufferSpace) {
         return bufferSpace;
      }
      else {
         return bytesRemaining;
      }
   }

   public int getMaxSize() {
      return sqlProvider.getMaxBlobSize();
   }
}
