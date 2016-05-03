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

import java.io.File;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.jdbc.store.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.file.sql.SQLProvider;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;

public class JDBCSequentialFileFactory implements SequentialFileFactory, ActiveMQComponent {

   private Connection connection;

   private String connectionUrl;

   private final Driver driver;

   private boolean started;

   private final String tableName;

   private List<JDBCSequentialFile> files;

   private PreparedStatement selectFileNamesByExtension;

   private Executor executor;

   private SQLProvider sqlProvider;

   private Map<String, Object> fileLocks = new HashMap<>();

   public JDBCSequentialFileFactory(final String connectionUrl,
                                    final String tableName,
                                    final String className,
                                    Executor executor) throws Exception {
      this.connectionUrl = connectionUrl;
      this.executor = executor;
      this.tableName = tableName.toUpperCase();

      files = new ArrayList<>();
      sqlProvider = JDBCUtils.getSQLProvider(JDBCUtils.getDriver(className).getClass().getCanonicalName(), tableName);
      driver = JDBCUtils.getDriver(className);
   }

   public Connection getConnection() {
      return connection;
   }

   @Override
   public SequentialFile createSequentialFile(String fileName) {
      try {
         fileLocks.putIfAbsent(fileName, new Object());
         JDBCSequentialFile file = new JDBCSequentialFile(this, fileName, sqlProvider, executor, fileLocks.get(fileName));
         files.add(file);
         return file;
      }
      catch (Exception e) {
         ActiveMQJournalLogger.LOGGER.error("Could not create file", e);
      }
      return null;
   }

   @Override
   public int getMaxIO() {
      return 1;
   }

   @Override
   public List<String> listFiles(String extension) throws Exception {
      List<String> fileNames = new ArrayList<>();

      selectFileNamesByExtension.setString(1, extension);
      try (ResultSet rs = selectFileNamesByExtension.executeQuery()) {
         while (rs.next()) {
            fileNames.add(rs.getString(1));
         }
      }
      return fileNames;
   }

   @Override
   public boolean isSupportsCallbacks() {
      return true;
   }

   @Override
   public void onIOError(Exception exception, String message, SequentialFile file) {
   }

   @Override
   public ByteBuffer allocateDirectBuffer(final int size) {
      return NIOSequentialFileFactory.allocateDirectByteBuffer(size);
   }

   @Override
   public void releaseDirectBuffer(ByteBuffer buffer) {
      // nothing we can do on this case. we can just have good faith on GC
   }

   @Override
   public ByteBuffer newBuffer(final int size) {
      return ByteBuffer.allocate(size);
   }

   @Override
   public void clearBuffer(final ByteBuffer buffer) {
      final int limit = buffer.limit();
      buffer.rewind();

      for (int i = 0; i < limit; i++) {
         buffer.put((byte) 0);
      }

      buffer.rewind();
   }

   @Override
   public ByteBuffer wrapBuffer(final byte[] bytes) {
      return ByteBuffer.wrap(bytes);
   }

   @Override
   public int getAlignment() {
      return 1;
   }

   @Override
   public int calculateBlockSize(final int bytes) {
      return bytes;
   }

   @Override
   public void deactivateBuffer() {
   }

   @Override
   public void releaseBuffer(final ByteBuffer buffer) {
   }

   @Override
   public void activateBuffer(SequentialFile file) {

   }

   @Override
   public File getDirectory() {
      return null;
   }

   @Override
   public synchronized void start() {
      try {
         if (!started) {
            connection = driver.connect(connectionUrl, new Properties());
            JDBCUtils.createTableIfNotExists(connection, tableName, sqlProvider.getCreateFileTableSQL());
            selectFileNamesByExtension = connection.prepareStatement(sqlProvider.getSelectFileNamesByExtensionSQL());
            started = true;
         }
      }
      catch (SQLException e) {
         ActiveMQJournalLogger.LOGGER.error("Could not start file factory, unable to connect to database");
         started = false;
      }
   }

   @Override
   public synchronized void stop() {
      try {
         if (false)
            connection.close();
      }
      catch (SQLException e) {
         ActiveMQJournalLogger.LOGGER.error("Error stopping file factory, unable to close db connection");
      }
      started = false;
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public void createDirs() throws Exception {
   }

   @Override
   public void flush() {

   }

   public synchronized void destroy() throws SQLException {
      Statement statement = connection.createStatement();
      statement.executeUpdate(sqlProvider.getDropFileTableSQL());
      stop();
   }
}
