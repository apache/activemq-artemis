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

package org.apache.activemq.artemis.jdbc.store.file;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

@SuppressWarnings("SynchronizeOnNonFinalField")
public final class Db2SequentialFileDriver extends JDBCSequentialFileFactoryDriver {

   public Db2SequentialFileDriver() {
      super();
   }

   public Db2SequentialFileDriver(DataSource dataSource, SQLProvider provider) {
      super(dataSource, provider);
   }

   public Db2SequentialFileDriver(Connection connection, SQLProvider provider) {
      super(connection, provider);
   }

   @Override
   protected void prepareStatements() throws SQLException {
      this.deleteFile = connection.prepareStatement(sqlProvider.getDeleteFileSQL());
      this.createFile = connection.prepareStatement(sqlProvider.getInsertFileSQL(), new String[]{"ID"});
      this.selectFileByFileName = connection.prepareStatement(sqlProvider.getSelectFileByFileName());
      this.copyFileRecord = connection.prepareStatement(sqlProvider.getCopyFileRecordByIdSQL());
      this.renameFile = connection.prepareStatement(sqlProvider.getUpdateFileNameByIdSQL());
      this.readLargeObject = connection.prepareStatement(sqlProvider.getReadLargeObjectSQL());
      this.appendToLargeObject = connection.prepareStatement(sqlProvider.getAppendToLargeObjectSQL());
      this.selectFileNamesByExtension = connection.prepareStatement(sqlProvider.getSelectFileNamesByExtensionSQL());
   }

   @Override
   public int writeToFile(JDBCSequentialFile file, byte[] data) throws SQLException {
      if (data == null || data.length == 0) {
         return 0;
      }
      synchronized (connection) {
         try {
            connection.setAutoCommit(false);
            int bytesWritten;
            appendToLargeObject.setBytes(1, data);
            appendToLargeObject.setLong(2, file.getId());
            final int updatesFiles = appendToLargeObject.executeUpdate();
            assert updatesFiles <= 1;
            connection.commit();
            if (updatesFiles == 0) {
               bytesWritten = 0;
            } else {
               bytesWritten = data.length;
            }
            return bytesWritten;
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
   }
}
