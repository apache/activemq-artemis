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
package org.apache.activemq.artemis.jdbc.store.drivers.postgres;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFile;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFileFactoryDriver;
import org.postgresql.PGConnection;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

public class PostgresSequentialSequentialFileDriver extends JDBCSequentialFileFactoryDriver {

   private static final String POSTGRES_OID_KEY = "POSTGRES_OID_KEY";

   public PostgresSequentialSequentialFileDriver() throws SQLException {
      super();
   }

   @Override
   public synchronized void createFile(JDBCSequentialFile file) throws SQLException {
      try {
         connection.setAutoCommit(false);

         LargeObjectManager lobjManager = ((PGConnection) connection).getLargeObjectAPI();
         long oid = lobjManager.createLO();

         createFile.setString(1, file.getFileName());
         createFile.setString(2, file.getExtension());
         createFile.setLong(3, oid);
         createFile.executeUpdate();

         try (ResultSet keys = createFile.getGeneratedKeys()) {
            keys.next();
            file.setId(keys.getInt(1));
         }
         connection.commit();
      } catch (SQLException e) {
         connection.rollback();
         throw e;
      }
   }

   @Override
   public synchronized void loadFile(JDBCSequentialFile file) throws SQLException {
      connection.setAutoCommit(false);
      readLargeObject.setInt(1, file.getId());

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

   @Override
   public synchronized int writeToFile(JDBCSequentialFile file, byte[] data) throws SQLException {
      LargeObjectManager lobjManager = ((PGConnection) connection).getLargeObjectAPI();
      LargeObject largeObject = null;

      Long oid = getOID(file);
      try {
         connection.setAutoCommit(false);
         largeObject = lobjManager.open(oid, LargeObjectManager.WRITE);
         largeObject.seek(largeObject.size());
         largeObject.write(data);
         largeObject.close();
         connection.commit();
      } catch (Exception e) {
         connection.rollback();
         throw e;
      }
      return data.length;
   }

   @Override
   public synchronized int readFromFile(JDBCSequentialFile file, ByteBuffer bytes) throws SQLException {
      LargeObjectManager lobjManager = ((PGConnection) connection).getLargeObjectAPI();
      LargeObject largeObject = null;
      long oid = getOID(file);
      try {
         connection.setAutoCommit(false);
         largeObject = lobjManager.open(oid, LargeObjectManager.READ);
         int readLength = (int) calculateReadLength(largeObject.size(), bytes.remaining(), file.position());

         if (readLength > 0) {
            if (file.position() > 0)
               largeObject.seek((int) file.position());
            byte[] data = largeObject.read(readLength);
            bytes.put(data);
         }

         largeObject.close();
         connection.commit();

         return readLength;
      } catch (SQLException e) {
         connection.rollback();
         throw e;
      }
   }

   private synchronized Long getOID(JDBCSequentialFile file) throws SQLException {
      Long oid = (Long) file.getMetaData(POSTGRES_OID_KEY);
      if (oid == null) {
         connection.setAutoCommit(false);
         readLargeObject.setInt(1, file.getId());
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
      if ((Long) file.getMetaData(POSTGRES_OID_KEY) == 0) {
         System.out.println("FD");
      }
      return (Long) file.getMetaData(POSTGRES_OID_KEY);
   }

   private synchronized int getPostGresLargeObjectSize(JDBCSequentialFile file) throws SQLException {
      LargeObjectManager lobjManager = ((PGConnection) connection).getLargeObjectAPI();

      int size = 0;
      Long oid = getOID(file);
      if (oid != null) {
         try {
            connection.setAutoCommit(false);
            LargeObject largeObject = lobjManager.open(oid, LargeObjectManager.READ);
            size = largeObject.size();
            largeObject.close();
            connection.commit();
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
      return size;
   }
}
