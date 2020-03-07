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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

import javax.sql.DataSource;

@SuppressWarnings("SynchronizeOnNonFinalField")
public final class PostgresSequentialSequentialFileDriver extends JDBCSequentialFileFactoryDriver {

   private static final String POSTGRES_OID_KEY = "POSTGRES_OID_KEY";
   private PostgresLargeObjectManager largeObjectManager;

   public PostgresSequentialSequentialFileDriver() throws SQLException {
      super();
   }

   public PostgresSequentialSequentialFileDriver(DataSource dataSource, SQLProvider provider) {
      super();
      this.setDataSource(dataSource);
      this.setSqlProvider(provider);
   }

   public PostgresSequentialSequentialFileDriver(Connection connection, SQLProvider provider) {
      super();
      this.setConnection(connection);
      this.setSqlProvider(provider);
   }

   @Override
   protected void prepareStatements() throws SQLException {
      this.largeObjectManager = new PostgresLargeObjectManager(connection);
      this.deleteFile = connection.prepareStatement(sqlProvider.getDeleteFileSQL());
      this.createFile = connection.prepareStatement(sqlProvider.getInsertFileSQL(), Statement.RETURN_GENERATED_KEYS);
      this.selectFileByFileName = connection.prepareStatement(sqlProvider.getSelectFileByFileName());
      this.copyFileRecord = connection.prepareStatement(sqlProvider.getCopyFileRecordByIdSQL());
      this.renameFile = connection.prepareStatement(sqlProvider.getUpdateFileNameByIdSQL());
      this.readLargeObject = connection.prepareStatement(sqlProvider.getReadLargeObjectSQL());
      this.appendToLargeObject = connection.prepareStatement(sqlProvider.getAppendToLargeObjectSQL());
      this.selectFileNamesByExtension = connection.prepareStatement(sqlProvider.getSelectFileNamesByExtensionSQL());
   }

   @Override
   public void createFile(JDBCSequentialFile file) throws SQLException {
      synchronized (connection) {
         try {
            connection.setAutoCommit(false);
            Long oid = largeObjectManager.createLO();

            createFile.setString(1, file.getFileName());
            createFile.setString(2, file.getExtension());
            createFile.setLong(3, oid);
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

   @Override
   public void loadFile(JDBCSequentialFile file) throws SQLException {
      synchronized (connection) {
         connection.setAutoCommit(false);
         readLargeObject.setLong(1, file.getId());

         try (ResultSet rs = readLargeObject.executeQuery()) {
            if (rs.next()) {
               file.setWritePosition(getPostGresLargeObjectSize(file));
            }
            connection.commit();
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
   }

   @Override
   public int writeToFile(JDBCSequentialFile file, byte[] data, boolean append) throws SQLException {
      synchronized (connection) {
         Object largeObject = null;

         Long oid = getOID(file);
         try {
            connection.setAutoCommit(false);
            largeObject = largeObjectManager.open(oid, PostgresLargeObjectManager.WRITE);
            if (append) {
               largeObjectManager.seek(largeObject, largeObjectManager.size(largeObject));
            } else {
               largeObjectManager.truncate(largeObject, 0);
            }
            largeObjectManager.write(largeObject, data);
            largeObjectManager.close(largeObject);
            connection.commit();
         } catch (Exception e) {
            connection.rollback();
            throw e;
         }
         return data.length;
      }
   }

   @Override
   public int readFromFile(JDBCSequentialFile file, ByteBuffer bytes) throws SQLException {
      Object largeObject = null;
      long oid = getOID(file);
      synchronized (connection) {
         try {
            connection.setAutoCommit(false);
            largeObject = largeObjectManager.open(oid, PostgresLargeObjectManager.READ);
            int readLength = (int) calculateReadLength(largeObjectManager.size(largeObject), bytes.remaining(), file.position());

            if (readLength > 0) {
               if (file.position() > 0) {
                  largeObjectManager.seek(largeObject, (int) file.position());
               }
               byte[] data = largeObjectManager.read(largeObject, readLength);
               bytes.put(data);
            }

            largeObjectManager.close(largeObject);
            connection.commit();

            return readLength;
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
   }

   private Long getOID(JDBCSequentialFile file) throws SQLException {
      Long oid = (Long) file.getMetaData(POSTGRES_OID_KEY);
      if (oid == null) {
         synchronized (connection) {
            connection.setAutoCommit(false);
            readLargeObject.setLong(1, file.getId());
            try (ResultSet rs = readLargeObject.executeQuery()) {
               if (rs.next()) {
                  file.addMetaData(POSTGRES_OID_KEY, rs.getLong(1));
               }
               connection.commit();
            } catch (SQLException e) {
               connection.rollback();
               throw e;
            }
         }
      }
      if ((Long) file.getMetaData(POSTGRES_OID_KEY) == 0) {
         System.out.println("FD");
      }
      return (Long) file.getMetaData(POSTGRES_OID_KEY);
   }

   private int getPostGresLargeObjectSize(JDBCSequentialFile file) throws SQLException {
      int size = 0;
      Long oid = getOID(file);
      if (oid != null) {
         synchronized (connection) {
            try {
               connection.setAutoCommit(false);
               Object largeObject = largeObjectManager.open(oid, PostgresLargeObjectManager.READ);
               size = largeObjectManager.size(largeObject);
               largeObjectManager.close(largeObject);
               connection.commit();
            } catch (SQLException e) {
               connection.rollback();
               throw e;
            }
         }
      }
      return size;
   }
}
