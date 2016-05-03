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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.buffer.TimedBuffer;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.impl.SimpleWaitIOCallback;
import org.apache.activemq.artemis.jdbc.store.file.sql.SQLProvider;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.jboss.logging.Logger;

public class JDBCSequentialFile implements SequentialFile {

   private static final Logger logger = Logger.getLogger(JDBCSequentialFile.class);

   private final String filename;

   private final String extension;

   private boolean isOpen = false;

   private boolean isCreated = false;

   private int id = -1;

   private final PreparedStatement appendToFile;

   private final PreparedStatement deleteFile;

   private final PreparedStatement readFile;

   private final PreparedStatement createFile;

   private final PreparedStatement selectFileByFileName;

   private final PreparedStatement copyFileRecord;

   private final PreparedStatement renameFile;

   private long readPosition = 0;

   private long writePosition = 0;

   private Executor executor;

   private JDBCSequentialFileFactory fileFactory;

   private int maxSize;

   private SQLProvider sqlProvider;

   private final Object writeLock;

   public JDBCSequentialFile(final JDBCSequentialFileFactory fileFactory,
                             final String filename,
                             final SQLProvider sqlProvider,
                             final Executor executor,
                             final Object writeLock) throws SQLException {
      this.fileFactory = fileFactory;
      this.filename = filename;
      this.extension = filename.contains(".") ? filename.substring(filename.lastIndexOf(".") + 1, filename.length()) : "";
      this.executor = executor;
      this.maxSize = sqlProvider.getMaxBlobSize();
      this.sqlProvider = sqlProvider;
      this.writeLock = writeLock;

      Connection connection = fileFactory.getConnection();
      this.appendToFile = connection.prepareStatement(sqlProvider.getAppendToFileSQL());
      this.deleteFile = connection.prepareStatement(sqlProvider.getDeleteFileSQL());
      this.createFile = connection.prepareStatement(sqlProvider.getInsertFileSQL(), Statement.RETURN_GENERATED_KEYS);
      this.readFile = connection.prepareStatement(sqlProvider.getReadFileSQL());
      this.selectFileByFileName = connection.prepareStatement(sqlProvider.getSelectFileByFileName());
      this.copyFileRecord = connection.prepareStatement(sqlProvider.getCopyFileRecordByIdSQL());
      this.renameFile = connection.prepareStatement(sqlProvider.getUpdateFileNameByIdSQL());
   }

   @Override
   public boolean isOpen() {
      return isOpen;
   }

   @Override
   public boolean exists() {
      return isCreated;
   }

   @Override
   public synchronized void open() throws Exception {
      if (!isOpen) {
         try {
            synchronized (writeLock) {
               selectFileByFileName.setString(1, filename);

               try (ResultSet rs = selectFileByFileName.executeQuery()) {
                  if (!rs.next()) {
                     createFile.setString(1, filename);
                     createFile.setString(2, extension);
                     createFile.setBytes(3, new byte[0]);
                     createFile.executeUpdate();
                     try (ResultSet keys = createFile.getGeneratedKeys()) {
                        keys.next();
                        this.id = keys.getInt(1);
                     }
                  }
                  else {
                     this.id = rs.getInt(1);
                     this.writePosition = rs.getBlob(4).length();
                  }
               }
            }
         }
         catch (SQLException e) {
            ActiveMQJournalLogger.LOGGER.error("Error retreiving file record", e);
            isOpen = false;
         }

         isCreated = true;
         isOpen = true;
      }
   }

   @Override
   public void open(int maxIO, boolean useExecutor) throws Exception {
      open();
   }

   @Override
   public boolean fits(int size) {
      return writePosition + size <= maxSize;
   }

   @Override
   public int getAlignment() throws Exception {
      return 0;
   }

   @Override
   public int calculateBlockStart(int position) throws Exception {
      return 0;
   }

   @Override
   public String getFileName() {
      return filename;
   }

   @Override
   public void fill(int size) throws Exception {
      // Do nothing
   }

   @Override
   public void delete() throws IOException, InterruptedException, ActiveMQException {
      try {
         if (isCreated) {
            deleteFile.setInt(1, id);
            deleteFile.executeUpdate();
         }
      }
      catch (SQLException e) {
         throw new IOException(e);
      }
   }

   private synchronized int internalWrite(byte[] data, IOCallback callback) {
      try {
         synchronized (writeLock) {
            int noBytes = data.length;
            appendToFile.setBytes(1, data);
            appendToFile.setInt(2, id);
            int result = appendToFile.executeUpdate();
            if (result < 1)
               throw new ActiveMQException("No record found for file id: " + id);
            seek(noBytes);
            if (callback != null)
               callback.done();
            return noBytes;
         }
      }
      catch (Exception e) {
         e.printStackTrace();
         if (callback != null)
            callback.onError(-1, e.getMessage());
      }
      return -1;
   }

   public synchronized int internalWrite(ActiveMQBuffer buffer, IOCallback callback) {
      byte[] data = new byte[buffer.readableBytes()];
      buffer.readBytes(data);
      return internalWrite(data, callback);
   }

   private synchronized int internalWrite(ByteBuffer buffer, IOCallback callback) {
      return internalWrite(buffer.array(), callback);
   }

   public void scheduleWrite(final ActiveMQBuffer bytes, final IOCallback callback) {
      executor.execute(new Runnable() {
         @Override
         public void run() {
            internalWrite(bytes, callback);
         }
      });
   }

   public void scheduleWrite(final ByteBuffer bytes, final IOCallback callback) {
      executor.execute(new Runnable() {
         @Override
         public void run() {
            internalWrite(bytes, callback);
         }
      });
   }

   synchronized void seek(long noBytes) {
      writePosition += noBytes;
   }

   @Override
   public void write(ActiveMQBuffer bytes, boolean sync, IOCallback callback) throws Exception {
      // We ignore sync since we schedule writes straight away.
      scheduleWrite(bytes, callback);
   }

   @Override
   public void write(ActiveMQBuffer bytes, boolean sync) throws Exception {
      write(bytes, sync, null);
   }

   @Override
   public void write(EncodingSupport bytes, boolean sync, IOCallback callback) throws Exception {
      ActiveMQBuffer data = ActiveMQBuffers.fixedBuffer(bytes.getEncodeSize());
      bytes.encode(data);
      scheduleWrite(data, callback);
   }

   @Override
   public void write(EncodingSupport bytes, boolean sync) throws Exception {
      write(bytes, sync, null);
   }

   @Override
   public void writeDirect(ByteBuffer bytes, boolean sync, IOCallback callback) {
      if (callback == null) {
         SimpleWaitIOCallback waitIOCallback = new SimpleWaitIOCallback();
         try {
            scheduleWrite(bytes, waitIOCallback);
            waitIOCallback.waitCompletion();
         }
         catch (Exception e) {
            waitIOCallback.onError(-1, e.getMessage());
         }
      }
      else {
         scheduleWrite(bytes, callback);
      }

   }

   @Override
   public void writeDirect(ByteBuffer bytes, boolean sync) throws Exception {
      writeDirect(bytes, sync, null);
      // Are we meant to block here?
   }

   @Override
   public synchronized int read(ByteBuffer bytes, IOCallback callback) throws SQLException {
      synchronized (writeLock) {
         readFile.setInt(1, id);
         try (ResultSet rs = readFile.executeQuery()) {
            if (rs.next()) {
               Blob blob = rs.getBlob(1);

               long bytesRemaining = blob.length() - readPosition;
               byte[] data;
               if (bytesRemaining > bytes.remaining()) {
                  // First index into blob is 1 (not 0)
                  data = blob.getBytes(readPosition + 1, bytes.remaining());
               }
               else {
                  // First index into blob is 1 (not 0)
                  data = blob.getBytes(readPosition + 1, (int) bytesRemaining);
               }

               bytes.put(data);
               readPosition += data.length;
               if (callback != null)
                  callback.done();

               return data.length;
            }
            return 0;
         }
         catch (Exception e) {
            if (callback != null)
               callback.onError(-1, e.getMessage());
            return 0;
         }
      }
   }

   @Override
   public int read(ByteBuffer bytes) throws Exception {
      return read(bytes, null);
   }

   @Override
   public void position(long pos) throws IOException {
      readPosition = pos;
   }

   @Override
   public long position() {
      return readPosition;
   }

   @Override
   public synchronized void close() throws Exception {
      isOpen = false;
   }

   @Override
   public void sync() throws IOException {
      // (mtaylor) We always write straight away, so we don't need to do anything here.
      // (mtaylor) Is this meant to be blocking?
   }

   @Override
   public long size() throws Exception {
      return writePosition;
   }

   @Override
   public void renameTo(String newFileName) throws Exception {
      renameFile.setString(1, newFileName);
      renameFile.setInt(2, id);
      renameFile.executeUpdate();
   }

   @Override
   public SequentialFile cloneFile() {
      try {
         JDBCSequentialFile clone = new JDBCSequentialFile(fileFactory, filename, sqlProvider, executor, writeLock);
         return clone;
      }
      catch (Exception e) {
         logger.error("Error cloning file: " + filename, e);
         return null;
      }
   }

   @Override
   public void copyTo(SequentialFile cloneFile) throws Exception {
      JDBCSequentialFile clone = (JDBCSequentialFile) cloneFile;
      clone.open();

      copyFileRecord.setInt(1, id);
      copyFileRecord.setInt(2, clone.getId());
      copyFileRecord.executeUpdate();
   }

   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   public String getFilename() {
      return filename;
   }

   public String getExtension() {
      return extension;
   }

   // Only Used by Journal, no need to implement.
   @Override
   public void setTimedBuffer(TimedBuffer buffer) {
   }

   // Only Used by replication, no need to implement.
   @Override
   public File getJavaFile() {
      return null;
   }
}
