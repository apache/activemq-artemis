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

   private boolean isOpen = false;

   private boolean isCreated = false;

   private int id = -1;

   private long readPosition = 0;

   private long writePosition = 0;

   private final Executor executor;

   private final JDBCSequentialFileFactory fileFactory;

   private final Object writeLock;

   private final JDBCSequentialFileFactoryDriver dbDriver;

   // Allows DB Drivers to cache meta data.
   private final Map<Object, Object> metaData = new ConcurrentHashMap<>();

   public JDBCSequentialFile(final JDBCSequentialFileFactory fileFactory,
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

   public void setWritePosition(int writePosition) {
      this.writePosition = writePosition;
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
         synchronized (writeLock) {
            dbDriver.openFile(this);
            isCreated = true;
            isOpen = true;
         }
      }
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
            synchronized (writeLock) {
               dbDriver.deleteFile(this);
            }
         }
      } catch (SQLException e) {
         throw new ActiveMQException(ActiveMQExceptionType.IO_ERROR, e.getMessage(), e);
      }
   }

   private synchronized int internalWrite(byte[] data, IOCallback callback) {
      try {
         synchronized (writeLock) {
            int noBytes = dbDriver.writeToFile(this, data);
            seek(noBytes);
            if (callback != null)
               callback.done();
            return noBytes;
         }
      } catch (Exception e) {
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
         } catch (Exception e) {
            waitIOCallback.onError(-1, e.getMessage());
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
         } catch (Exception e) {
            if (callback != null)
               callback.onError(-1, e.getMessage());
            e.printStackTrace();
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
      final SimpleWaitIOCallback callback = new SimpleWaitIOCallback();
      executor.execute(new Runnable() {
         @Override
         public void run() {
            callback.done();
         }
      });

      try {
         callback.waitCompletion();
      } catch (Exception e) {
         throw new IOException(e);
      }
   }

   @Override
   public long size() throws Exception {
      return writePosition;
   }

   @Override
   public void renameTo(String newFileName) throws Exception {
      synchronized (writeLock) {
         dbDriver.renameFile(this, newFileName);
      }
   }

   @Override
   public SequentialFile cloneFile() {
      try {
         JDBCSequentialFile clone = new JDBCSequentialFile(fileFactory, filename, executor, dbDriver, writeLock);
         return clone;
      } catch (Exception e) {
         logger.error("Error cloning file: " + filename, e);
         return null;
      }
   }

   @Override
   public void copyTo(SequentialFile cloneFile) throws Exception {
      JDBCSequentialFile clone = (JDBCSequentialFile) cloneFile;
      clone.open();

      synchronized (writeLock) {
         dbDriver.copyFileData(this, clone);
      }
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

   public void addMetaData(Object key, Object value) {
      metaData.put(key, value);
   }

   public Object removeMetaData(Object key) {
      return metaData.remove(key);
   }

   public Object getMetaData(Object key) {
      return metaData.get(key);
   }
}
