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
package org.apache.activemq.artemis.core.io.nio;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQIOErrorException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.AbstractSequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.journal.ActiveMQJournalBundle;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;

public final class NIOSequentialFile extends AbstractSequentialFile {

   private FileChannel channel;

   private RandomAccessFile rfile;

   /**
    * The write semaphore here is only used when writing asynchronously
    */
   private Semaphore maxIOSemaphore;

   private final int defaultMaxIO;

   private int maxIO;

   public NIOSequentialFile(final SequentialFileFactory factory,
                            final File directory,
                            final String file,
                            final int maxIO,
                            final Executor writerExecutor) {
      super(directory, file, factory, writerExecutor);
      defaultMaxIO = maxIO;
   }

   public int getAlignment() {
      return 1;
   }

   public int calculateBlockStart(final int position) {
      return position;
   }

   public synchronized boolean isOpen() {
      return channel != null;
   }

   /**
    * this.maxIO represents the default maxIO.
    * Some operations while initializing files on the journal may require a different maxIO
    */
   public synchronized void open() throws IOException {
      open(defaultMaxIO, true);
   }

   public void open(final int maxIO, final boolean useExecutor) throws IOException {
      try {
         rfile = new RandomAccessFile(getFile(), "rw");

         channel = rfile.getChannel();

         fileSize = channel.size();
      }
      catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }

      if (writerExecutor != null && useExecutor) {
         maxIOSemaphore = new Semaphore(maxIO);
         this.maxIO = maxIO;
      }
   }

   public void fill(final int size) throws IOException {
      ByteBuffer bb = ByteBuffer.allocate(size);

      bb.limit(size);
      bb.position(0);

      try {
         channel.position(0);
         channel.write(bb);
         channel.force(false);
         channel.position(0);
      }
      catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }

      fileSize = channel.size();
   }

   @Override
   public synchronized void close() throws IOException, InterruptedException, ActiveMQException {
      super.close();

      if (maxIOSemaphore != null) {
         while (!maxIOSemaphore.tryAcquire(maxIO, 60, TimeUnit.SECONDS)) {
            ActiveMQJournalLogger.LOGGER.errorClosingFile(getFileName());
         }
      }

      maxIOSemaphore = null;
      try {
         if (channel != null) {
            channel.close();
         }

         if (rfile != null) {
            rfile.close();
         }
      }
      catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }
      channel = null;

      rfile = null;

      notifyAll();
   }

   public int read(final ByteBuffer bytes) throws Exception {
      return read(bytes, null);
   }

   public synchronized int read(final ByteBuffer bytes,
                                final IOCallback callback) throws IOException, ActiveMQIllegalStateException {
      try {
         if (channel == null) {
            throw new ActiveMQIllegalStateException("File " + this.getFileName() + " has a null channel");
         }
         int bytesRead = channel.read(bytes);

         if (callback != null) {
            callback.done();
         }

         bytes.flip();

         return bytesRead;
      }
      catch (IOException e) {
         if (callback != null) {
            callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getLocalizedMessage());
         }

         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);

         throw e;
      }
   }

   public void sync() throws IOException {
      if (channel != null) {
         try {
            channel.force(false);
         }
         catch (IOException e) {
            factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
            throw e;
         }
      }
   }

   public long size() throws IOException {
      if (channel == null) {
         return getFile().length();
      }

      try {
         return channel.size();
      }
      catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }
   }

   @Override
   public void position(final long pos) throws IOException {
      try {
         super.position(pos);
         channel.position(pos);
      }
      catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }
   }

   @Override
   public String toString() {
      return "NIOSequentialFile " + getFile();
   }

   public SequentialFile cloneFile() {
      return new NIOSequentialFile(factory, directory, getFileName(), maxIO, writerExecutor);
   }

   public void writeDirect(final ByteBuffer bytes, final boolean sync, final IOCallback callback) {
      if (callback == null) {
         throw new NullPointerException("callback parameter need to be set");
      }

      try {
         internalWrite(bytes, sync, callback);
      }
      catch (Exception e) {
         callback.onError(ActiveMQExceptionType.GENERIC_EXCEPTION.getCode(), e.getMessage());
      }
   }

   public void writeDirect(final ByteBuffer bytes, final boolean sync) throws Exception {
      internalWrite(bytes, sync, null);
   }

   public void writeInternal(final ByteBuffer bytes) throws Exception {
      internalWrite(bytes, true, null);
   }

   @Override
   protected ByteBuffer newBuffer(int size, final int limit) {
      // For NIO, we don't need to allocate a buffer the entire size of the timed buffer, unlike AIO

      size = limit;

      return super.newBuffer(size, limit);
   }

   private void internalWrite(final ByteBuffer bytes,
                              final boolean sync,
                              final IOCallback callback) throws IOException, ActiveMQIOErrorException, InterruptedException {
      if (!isOpen()) {
         if (callback != null) {
            callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), "File not opened");
         }
         else {
            throw ActiveMQJournalBundle.BUNDLE.fileNotOpened();
         }
         return;
      }

      position.addAndGet(bytes.limit());

      if (maxIOSemaphore == null || callback == null) {
         // if maxIOSemaphore == null, that means we are not using executors and the writes are synchronous
         try {
            doInternalWrite(bytes, sync, callback);
         }
         catch (IOException e) {
            factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         }
      }
      else {
         // This is a flow control on writing, just like maxAIO on libaio
         maxIOSemaphore.acquire();

         writerExecutor.execute(new Runnable() {
            public void run() {
               try {
                  try {
                     doInternalWrite(bytes, sync, callback);
                  }
                  catch (IOException e) {
                     ActiveMQJournalLogger.LOGGER.errorSubmittingWrite(e);
                     factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), NIOSequentialFile.this);
                     callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getMessage());
                  }
                  catch (Throwable e) {
                     ActiveMQJournalLogger.LOGGER.errorSubmittingWrite(e);
                     callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getMessage());
                  }
               }
               finally {
                  maxIOSemaphore.release();
               }
            }
         });
      }
   }

   /**
    * @param bytes
    * @param sync
    * @param callback
    * @throws IOException
    * @throws Exception
    */
   private void doInternalWrite(final ByteBuffer bytes,
                                final boolean sync,
                                final IOCallback callback) throws IOException {
      channel.write(bytes);

      if (sync) {
         sync();
      }

      if (callback != null) {
         callback.done();
      }
   }
}
