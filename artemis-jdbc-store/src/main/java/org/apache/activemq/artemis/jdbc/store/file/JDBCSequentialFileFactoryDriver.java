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

import javax.sql.DataSource;
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
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.jboss.logging.Logger;

@SuppressWarnings("SynchronizeOnNonFinalField")
public class JDBCSequentialFileFactoryDriver extends AbstractJDBCDriver {

   private static final Logger logger = Logger.getLogger(JDBCSequentialFileFactoryDriver.class);

   protected PreparedStatement deleteFile;

   protected PreparedStatement createFile;

   protected PreparedStatement selectFileByFileName;

   protected PreparedStatement copyFileRecord;

   protected PreparedStatement renameFile;

   protected PreparedStatement readLargeObject;

   protected PreparedStatement appendToLargeObject;

   protected PreparedStatement selectFileNamesByExtension;

   JDBCSequentialFileFactoryDriver() {
      super();
   }

   JDBCSequentialFileFactoryDriver(DataSource dataSource, SQLProvider provider) {
      super(dataSource, provider);
   }

   JDBCSequentialFileFactoryDriver(Connection connection, SQLProvider sqlProvider) {
      super(connection, sqlProvider);
   }

   @Override
   protected void createSchema() throws SQLException {
      createTable(sqlProvider.getCreateFileTableSQL());
   }

   @Override
   protected void prepareStatements() throws SQLException {
      this.deleteFile = connection.prepareStatement(sqlProvider.getDeleteFileSQL());
      this.createFile = connection.prepareStatement(sqlProvider.getInsertFileSQL(), new String[] {"ID"});
      this.selectFileByFileName = connection.prepareStatement(sqlProvider.getSelectFileByFileName());
      this.copyFileRecord = connection.prepareStatement(sqlProvider.getCopyFileRecordByIdSQL());
      this.renameFile = connection.prepareStatement(sqlProvider.getUpdateFileNameByIdSQL());
      this.readLargeObject = connection.prepareStatement(sqlProvider.getReadLargeObjectSQL());
      this.appendToLargeObject = connection.prepareStatement(sqlProvider.getAppendToLargeObjectSQL(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      this.selectFileNamesByExtension = connection.prepareStatement(sqlProvider.getSelectFileNamesByExtensionSQL());
   }

   public List<String> listFiles(String extension) throws Exception {
      synchronized (connection) {
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
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
         return fileNames;
      }
   }

   /**
    * Opens the supplied file.  If the file does not exist in the database it will create a new one.
    *
    * @param file
    * @throws SQLException
    */
   public void openFile(JDBCSequentialFile file) throws SQLException {
      synchronized (connection) {
         final long fileId = fileExists(file);
         if (fileId < 0) {
            createFile(file);
         } else {
            file.setId(fileId);
            loadFile(file);
         }
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
      try {
         synchronized (connection) {
            connection.setAutoCommit(false);
            selectFileByFileName.setString(1, file.getFileName());
            try (ResultSet rs = selectFileByFileName.executeQuery()) {
               final long id = rs.next() ? rs.getLong(1) : -1;
               connection.commit();
               return id;
            } catch (Exception e) {
               connection.rollback();
               throw e;
            }
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
      synchronized (connection) {
         connection.setAutoCommit(false);
         readLargeObject.setLong(1, file.getId());

         try (ResultSet rs = readLargeObject.executeQuery()) {
            if (rs.next()) {
               Blob blob = rs.getBlob(1);
               if (blob != null) {
                  file.setWritePosition(blob.length());
               } else {
                  logger.trace("No Blob found for file: " + file.getFileName() + " " + file.getId());
               }
            }
            connection.commit();
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
      synchronized (connection) {
         try {
            connection.setAutoCommit(false);
            createFile.setString(1, file.getFileName());
            createFile.setString(2, file.getExtension());
            createFile.setBytes(3, new byte[0]);
            createFile.executeUpdate();
            try (ResultSet keys = createFile.getGeneratedKeys()) {
               keys.next();
               file.setId(keys.getLong(1));
            }
            connection.commit();
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
      synchronized (connection) {
         try {
            connection.setAutoCommit(false);
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
      synchronized (connection) {
         try {
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
   public int writeToFile(JDBCSequentialFile file, byte[] data) throws SQLException {
      synchronized (connection) {
         connection.setAutoCommit(false);
         appendToLargeObject.setLong(1, file.getId());

         int bytesWritten = 0;
         try (ResultSet rs = appendToLargeObject.executeQuery()) {
            if (rs.next()) {
               Blob blob = rs.getBlob(1);
               if (blob == null) {
                  blob = connection.createBlob();
               }
               bytesWritten = blob.setBytes(blob.length() + 1, data);
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

   /**
    * Reads data from the file (at file.readPosition) into the byteBuffer.
    *
    * @param file
    * @param bytes
    * @return
    * @throws SQLException
    */
   public int readFromFile(JDBCSequentialFile file, ByteBuffer bytes) throws SQLException {
      synchronized (connection) {
         connection.setAutoCommit(false);
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
                     logger.debugf("trying read %d bytes: blobLength = %d bytesRemaining = %d filePosition = %d",
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

   /**
    * Copy the data content of FileFrom to FileTo
    *
    * @param fileFrom
    * @param fileTo
    * @throws SQLException
    */
   public void copyFileData(JDBCSequentialFile fileFrom, JDBCSequentialFile fileTo) throws SQLException {
      synchronized (connection) {
         try {
            connection.setAutoCommit(false);
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
      synchronized (connection) {
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
