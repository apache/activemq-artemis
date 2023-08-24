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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.jdbc.store.drivers.AbstractJDBCDriver;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

@SuppressWarnings("SynchronizeOnNonFinalField")
public class JDBCSequentialFileFactoryDriver extends AbstractJDBCDriver {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected String deleteFile;
   protected String createFile;
   protected String[] createFileColumnNames;
   protected int createFileAutogeneratedKeys;
   protected String selectFileByFileName;
   protected String copyFileRecord;
   protected String renameFile;
   protected String readLargeObject;
   protected String appendToLargeObject;
   protected Integer appendToLargeObjectResultSetType;
   protected Integer appendToLargeObjectResultSetConcurrency;
   protected String selectFileNamesByExtension;

   JDBCSequentialFileFactoryDriver() {
      super();
   }

   JDBCSequentialFileFactoryDriver(JDBCConnectionProvider connectionProvider, SQLProvider provider) {
      super(connectionProvider, provider);
   }

   @Override
   protected void createSchema() throws SQLException {
      createTable(sqlProvider.getCreateFileTableSQL());
   }

   @Override
   protected void prepareStatements() {
      this.deleteFile = sqlProvider.getDeleteFileSQL();
      this.createFile = sqlProvider.getInsertFileSQL();
      this.createFileColumnNames = new String[] {"ID"};
      this.selectFileByFileName = sqlProvider.getSelectFileByFileName();
      this.copyFileRecord = sqlProvider.getCopyFileRecordByIdSQL();
      this.renameFile = sqlProvider.getUpdateFileNameByIdSQL();
      this.readLargeObject = sqlProvider.getReadLargeObjectSQL();
      this.appendToLargeObject = sqlProvider.getAppendToLargeObjectSQL();
      this.appendToLargeObjectResultSetType = ResultSet.TYPE_FORWARD_ONLY;
      this.appendToLargeObjectResultSetConcurrency = ResultSet.CONCUR_UPDATABLE;
      this.selectFileNamesByExtension = sqlProvider.getSelectFileNamesByExtensionSQL();
   }

   public List<String> listFiles(String extension) throws Exception {
      List<String> fileNames = new ArrayList<>();
      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         try (PreparedStatement selectFileNamesByExtension = connection.prepareStatement(this.selectFileNamesByExtension)) {
            selectFileNamesByExtension.setString(1, extension);
            try (ResultSet rs = selectFileNamesByExtension.executeQuery()) {
               while (rs.next()) {
                  fileNames.add(rs.getString(1));
               }
            }
            connection.commit();
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
      return fileNames;
   }

   /**
    * Opens the supplied file.  If the file does not exist in the database it will create a new one.
    *
    * @param file
    * @throws SQLException
    */
   public void openFile(JDBCSequentialFile file) throws SQLException {
      final long fileId = fileExists(file);
      if (fileId < 0) {
         createFile(file);
      } else {
         file.setId(fileId);
         loadFile(file);
      }
   }

   void removeFile(JDBCSequentialFile file) {

   }

   /**
    * Checks to see if a file with filename and extension exists.  If so returns the ID of the file or returns -1.
    *
    * @param file
    * @return
    * @throws SQLException
    */
   public long fileExists(JDBCSequentialFile file) throws SQLException {
      try (Connection connection = connectionProvider.getConnection()) {
         try (PreparedStatement selectFileByFileName = connection.prepareStatement(this.selectFileByFileName)) {
            connection.setAutoCommit(false);
            selectFileByFileName.setString(1, file.getFileName());
            try (ResultSet rs = selectFileByFileName.executeQuery()) {
               final long id = rs.next() ? rs.getLong(1) : -1;
               connection.commit();
               return id;
            }
         } catch (Exception e) {
            connection.rollback();
            throw e;
         }
      } catch (NullPointerException npe) {
         npe.printStackTrace();
         throw npe;
      }
   }

   /**
    * Loads an existing file.
    *
    * @param file
    * @throws SQLException
    */
   public void loadFile(JDBCSequentialFile file) throws SQLException {
      try (Connection connection = connectionProvider.getConnection()) {
         try (PreparedStatement readLargeObject = connection.prepareStatement(this.readLargeObject)) {
            connection.setAutoCommit(false);
            readLargeObject.setLong(1, file.getId());

            try (ResultSet rs = readLargeObject.executeQuery()) {
               if (rs.next()) {
                  Blob blob = rs.getBlob(1);
                  if (blob != null) {
                     file.setWritePosition(blob.length());
                  } else {
                     if (logger.isTraceEnabled()) {
                        logger.trace("No Blob found for file: {} {}", file.getFileName(), file.getId());
                     }
                  }
               }
               connection.commit();
            }
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
   }

   /**
    * Creates a new database row representing the supplied file.
    *
    * @param file
    * @throws SQLException
    */
   public void createFile(JDBCSequentialFile file) throws SQLException {
      try (Connection connection = connectionProvider.getConnection()) {
         try {
            connection.setAutoCommit(false);
            try (PreparedStatement createFile =
                         createFileColumnNames != null ?
                                 connection.prepareStatement(this.createFile, this.createFileColumnNames) :
                                 connection.prepareStatement(this.createFile, this.createFileAutogeneratedKeys)) {
               createFile.setString(1, file.getFileName());
               createFile.setString(2, file.getExtension());
               createFile.setBytes(3, new byte[0]);
               createFile.executeUpdate();
               try (ResultSet keys = createFile.getGeneratedKeys()) {
                  keys.next();
                  file.setId(keys.getLong(1));
               }
               connection.commit();
            }
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
   }

   /**
    * Updates the fileName field to the new value.
    *
    * @param file
    * @param newFileName
    * @throws SQLException
    */
   public void renameFile(JDBCSequentialFile file, String newFileName) throws SQLException {
      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         try (PreparedStatement renameFile = connection.prepareStatement(this.renameFile)) {
            renameFile.setString(1, newFileName);
            renameFile.setLong(2, file.getId());
            renameFile.executeUpdate();
            connection.commit();
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
   }

   /**
    * Deletes the associated row in the database.
    *
    * @param file
    * @throws SQLException
    */
   public void deleteFile(JDBCSequentialFile file) throws SQLException {
      try (Connection connection = connectionProvider.getConnection()) {
         try (PreparedStatement deleteFile = connection.prepareStatement(this.deleteFile)) {
            connection.setAutoCommit(false);
            deleteFile.setLong(1, file.getId());
            deleteFile.executeUpdate();
            connection.commit();
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
   }

   /**
    * Persists data to this files associated database mapping.
    *
    * @param file
    * @param data
    * @return
    * @throws SQLException
    */
   public int writeToFile(JDBCSequentialFile file, byte[] data, boolean append) throws SQLException {
      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         try (PreparedStatement appendToLargeObject =
                      this.appendToLargeObjectResultSetType != null && this.appendToLargeObjectResultSetConcurrency != null ?
                              connection.prepareStatement(this.appendToLargeObject, this.appendToLargeObjectResultSetType, this.appendToLargeObjectResultSetConcurrency) :
                              connection.prepareStatement(this.appendToLargeObject)) {
            appendToLargeObject.setLong(1, file.getId());

            int bytesWritten = 0;
            try (ResultSet rs = appendToLargeObject.executeQuery()) {
               if (rs.next()) {
                  Blob blob = rs.getBlob(1);
                  if (blob == null) {
                     blob = connection.createBlob();
                  }
                  if (append) {
                     bytesWritten = blob.setBytes(blob.length() + 1, data);
                  } else {
                     blob.truncate(0);
                     bytesWritten = blob.setBytes(1, data);
                  }
                  rs.updateBlob(1, blob);
                  rs.updateRow();
               }
               connection.commit();
               return bytesWritten;
            } catch (SQLException e) {
               connection.rollback();
               throw e;
            }
         }
      }
   }

   /**
    * Reads data from the file (at file.readPosition) into the byteBuffer.
    *
    * @param file
    * @param bytes
    * @return
    * @throws SQLException
    */
   public int readFromFile(JDBCSequentialFile file, ByteBuffer bytes) throws SQLException {
      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         try (PreparedStatement readLargeObject = connection.prepareStatement(this.readLargeObject)) {
            readLargeObject.setLong(1, file.getId());
            int readLength = 0;
            try (ResultSet rs = readLargeObject.executeQuery()) {
               if (rs.next()) {
                  final Blob blob = rs.getBlob(1);
                  if (blob != null) {
                     final long blobLength = blob.length();
                     final int bytesRemaining = bytes.remaining();
                     final long filePosition = file.position();
                     readLength = (int) calculateReadLength(blobLength, bytesRemaining, filePosition);
                     if (logger.isDebugEnabled()) {
                        logger.debug("trying read {} bytes: blobLength = {} bytesRemaining = {} filePosition = {}",
                                readLength, blobLength, bytesRemaining, filePosition);
                     }
                     if (readLength < 0) {
                        readLength = -1;
                     } else if (readLength > 0) {
                        byte[] data = blob.getBytes(file.position() + 1, readLength);
                        bytes.put(data);
                     }
                  }
               }
               connection.commit();
               return readLength;
            } catch (SQLException e) {
               connection.rollback();
               throw e;
            }
         }
      }
   }

   /**
    * Copy the data content of FileFrom to FileTo
    *
    * @param fileFrom
    * @param fileTo
    * @throws SQLException
    */
   public void copyFileData(JDBCSequentialFile fileFrom, JDBCSequentialFile fileTo) throws SQLException {
      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         try (PreparedStatement copyFileRecord = connection.prepareStatement(this.copyFileRecord)) {
            copyFileRecord.setLong(1, fileFrom.getId());
            copyFileRecord.setLong(2, fileTo.getId());
            copyFileRecord.executeUpdate();
            connection.commit();
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
   }

   /**
    * Drop all tables and data
    */
   @Override
   public void destroy() throws SQLException {
      try (Connection connection = connectionProvider.getConnection()) {
         try {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
               statement.executeUpdate(sqlProvider.getDropFileTableSQL());
            }
            connection.commit();
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
   }

   public long calculateReadLength(long objectLength, int bufferSpace, long readPosition) {
      long bytesRemaining = objectLength - readPosition;
      if (bytesRemaining > bufferSpace) {
         return bufferSpace;
      } else {
         return bytesRemaining;
      }
   }

   public long getMaxSize() {
      return sqlProvider.getMaxBlobSize();
   }
}
