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
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.buffer.TimedBuffer;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.impl.SimpleWaitIOCallback;
import org.jboss.logging.Logger;

public class JDBCSequentialFile implements SequentialFile {

   private static final Logger logger = Logger.getLogger(JDBCSequentialFile.class);

   private final String filename;

   private final String extension;

   private AtomicBoolean isOpen = new AtomicBoolean(false);

   private AtomicBoolean isLoaded = new AtomicBoolean(false);

   private long id = -1;

   private long readPosition = 0;

   private long writePosition = 0;

   private final Executor executor;

   private final JDBCSequentialFileFactory fileFactory;

   private final Object writeLock;

   private final JDBCSequentialFileFactoryDriver dbDriver;

   // Allows DB Drivers to cache meta data.
   private final Map<Object, Object> metaData = new ConcurrentHashMap<>();

   JDBCSequentialFile(final JDBCSequentialFileFactory fileFactory,
                      final String filename,
                      final Executor executor,
                      final JDBCSequentialFileFactoryDriver driver,
                      final Object writeLock) throws SQLException {
      this.fileFactory = fileFactory;
      this.filename = filename;
      this.extension = filename.contains(".") ? filename.substring(filename.lastIndexOf(".") + 1, filename.length()) : "";
      this.executor = executor;
      this.writeLock = writeLock;
      this.dbDriver = driver;
   }

   void setWritePosition(long writePosition) {
      this.writePosition = writePosition;
   }

   @Override
   public boolean isOpen() {
      return isOpen.get();
   }

   @Override
   public boolean exists() {
      if (isLoaded.get()) return true;
      try {
         return fileFactory.listFiles(extension).contains(filename);
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         fileFactory.onIOError(e, "Error checking JDBC file exists.", this);
         return false;
      }
   }

   @Override
   public void open() throws Exception {
      isOpen.compareAndSet(false, load());
   }

   private boolean load() {
      try {
         if (isLoaded.compareAndSet(false, true)) {
            dbDriver.openFile(this);
         }
         return true;
      } catch (SQLException e) {
         isLoaded.set(false);
         fileFactory.onIOError(e, "Error attempting to open JDBC file.", this);
      }
      return false;
   }

   @Override
   public void open(int maxIO, boolean useExecutor) throws Exception {
      open();
   }

   @Override
   public boolean fits(int size) {
      return writePosition + size <= dbDriver.getMaxSize();
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
         synchronized (writeLock) {
            if (load()) {
               dbDriver.deleteFile(this);
            }
         }
      } catch (SQLException e) {
         fileFactory.onIOError(e, "Error deleting JDBC file.", this);
      }
   }

   private synchronized int internalWrite(byte[] data, IOCallback callback) {
      try {
         open();
         synchronized (writeLock) {
            int noBytes = dbDriver.writeToFile(this, data);
            seek(noBytes);
            if (logger.isTraceEnabled()) {
               logger.trace("Write: ID: " + this.getId() + " FileName: " + this.getFileName() + size());
            }
            if (callback != null)
               callback.done();
            return noBytes;
         }
      } catch (Exception e) {
         if (callback != null)
            callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getMessage());
         fileFactory.onIOError(e, "Error writing to JDBC file.", this);
      }
      return 0;
   }

   public synchronized int internalWrite(ActiveMQBuffer buffer, IOCallback callback) {
      byte[] data = new byte[buffer.readableBytes()];
      buffer.readBytes(data);
      return internalWrite(data, callback);
   }

   private synchronized int internalWrite(ByteBuffer buffer, IOCallback callback) {
      return internalWrite(buffer.array(), callback);
   }

   private void scheduleWrite(final ActiveMQBuffer bytes, final IOCallback callback) {
      executor.execute(() -> {
         internalWrite(bytes, callback);
      });
   }

   private void scheduleWrite(final ByteBuffer bytes, final IOCallback callback) {
      executor.execute(() -> {
         internalWrite(bytes, callback);
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
         } catch (Exception e) {
            waitIOCallback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), "Error writing to JDBC file.");
            fileFactory.onIOError(e, "Failed to write to file.", this);
         }
      } else {
         scheduleWrite(bytes, callback);
      }

   }

   @Override
   public void writeDirect(ByteBuffer bytes, boolean sync) throws Exception {
      writeDirect(bytes, sync, null);
      // Are we meant to block here?
   }

   @Override
   public synchronized int read(ByteBuffer bytes, final IOCallback callback) throws SQLException {
      synchronized (writeLock) {
         try {
            int read = dbDriver.readFromFile(this, bytes);
            readPosition += read;
            if (callback != null)
               callback.done();
            return read;
         } catch (SQLException e) {
            if (callback != null)
               callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getMessage());
            fileFactory.onIOError(e, "Error reading from JDBC file.", this);
         }
         return 0;
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
   public void close() throws Exception {
      close(true);
   }

   @Override
   public void close(boolean waitOnSync) throws Exception {
      isOpen.set(false);
      if (waitOnSync) {
         sync();
      }
      fileFactory.sequentialFileClosed(this);
   }

   @Override
   public void sync() throws IOException {
      final SimpleWaitIOCallback callback = new SimpleWaitIOCallback();
      executor.execute(callback::done);

      try {
         callback.waitCompletion();
      } catch (Exception e) {
         callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), "Error during JDBC file sync.");
         fileFactory.onIOError(e, "Error during JDBC file sync.", this);
      }
   }

   @Override
   public long size() throws Exception {
      load();
      return writePosition;
   }

   @Override
   public void renameTo(String newFileName) throws Exception {
      synchronized (writeLock) {
         try {
            dbDriver.renameFile(this, newFileName);
         } catch (SQLException e) {
            fileFactory.onIOError(e, "Error renaming JDBC file.", this);
         }
      }
   }

   @Override
   public SequentialFile cloneFile() {
      try {
         JDBCSequentialFile clone = new JDBCSequentialFile(fileFactory, filename, executor, dbDriver, writeLock);
         return clone;
      } catch (Exception e) {
         fileFactory.onIOError(e, "Error cloning JDBC file.", this);
      }
      return null;
   }

   @Override
   public void copyTo(SequentialFile cloneFile) throws Exception {
      JDBCSequentialFile clone = (JDBCSequentialFile) cloneFile;
      try {
         synchronized (writeLock) {
            clone.open();
            dbDriver.copyFileData(this, clone);
         }
      } catch (Exception e) {
         fileFactory.onIOError(e, "Error copying JDBC file.", this);
      }
   }

   public long getId() {
      return id;
   }

   public void setId(long id) {
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

   public void addMetaData(Object key, Object value) {
      metaData.put(key, value);
   }

   public Object getMetaData(Object key) {
      return metaData.get(key);
   }
}
