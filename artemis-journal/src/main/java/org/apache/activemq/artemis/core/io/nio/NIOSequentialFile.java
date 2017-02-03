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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQIOErrorException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.core.io.AbstractSequentialFile;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.journal.ActiveMQJournalBundle;

public final class NIOSequentialFile extends AbstractSequentialFile {

   private FileChannel channel;

   private RandomAccessFile rfile;

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

   @Override
   public int calculateBlockStart(final int position) {
      return position;
   }

   @Override
   public synchronized boolean isOpen() {
      return channel != null;
   }

   /**
    * this.maxIO represents the default maxIO.
    * Some operations while initializing files on the journal may require a different maxIO
    */
   @Override
   public synchronized void open() throws IOException {
      open(defaultMaxIO, true);
   }

   @Override
   public void open(final int maxIO, final boolean useExecutor) throws IOException {
      try {
         rfile = new RandomAccessFile(getFile(), "rw");

         channel = rfile.getChannel();

         fileSize = channel.size();
      } catch (ClosedChannelException e) {
         throw e;
      } catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }
   }

   @Override
   public void fill(final int size) throws IOException {
      ByteBuffer bb = ByteBuffer.allocate(size);

      bb.limit(size);
      bb.position(0);

      try {
         channel.position(0);
         channel.write(bb);
         channel.force(false);
         channel.position(0);
      } catch (ClosedChannelException e) {
         throw e;
      } catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }
      channel.force(true);

      fileSize = channel.size();
   }

   public synchronized void waitForClose() throws InterruptedException {
      while (isOpen()) {
         wait();
      }
   }

   @Override
   public synchronized void close() throws IOException, InterruptedException, ActiveMQException {
      super.close();

      try {
         if (channel != null) {
            channel.close();
         }

         if (rfile != null) {
            rfile.close();
         }
      } catch (ClosedChannelException e) {
         throw e;
      } catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }
      channel = null;

      rfile = null;

      notifyAll();
   }

   @Override
   public int read(final ByteBuffer bytes) throws Exception {
      return read(bytes, null);
   }

   @Override
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
      } catch (ClosedChannelException e) {
         throw e;
      } catch (IOException e) {
         if (callback != null) {
            callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getLocalizedMessage());
         }

         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);

         throw e;
      }
   }

   @Override
   public void sync() throws IOException {
      if (factory.isDatasync() && channel != null) {
         try {
            channel.force(false);
         } catch (ClosedChannelException e) {
            throw e;
         } catch (IOException e) {
            factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
            throw e;
         }
      }
   }

   @Override
   public long size() throws IOException {
      if (channel == null) {
         return getFile().length();
      }

      try {
         return channel.size();
      } catch (ClosedChannelException e) {
         throw e;
      } catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }
   }

   @Override
   public void position(final long pos) throws IOException {
      try {
         super.position(pos);
         channel.position(pos);
      } catch (ClosedChannelException e) {
         throw e;
      } catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }
   }

   @Override
   public String toString() {
      return "NIOSequentialFile " + getFile();
   }

   @Override
   public SequentialFile cloneFile() {
      return new NIOSequentialFile(factory, directory, getFileName(), maxIO, null);
   }

   @Override
   public void writeDirect(final ByteBuffer bytes, final boolean sync, final IOCallback callback) {
      if (callback == null) {
         throw new NullPointerException("callback parameter need to be set");
      }

      try {
         internalWrite(bytes, sync, callback);
      } catch (Exception e) {
         callback.onError(ActiveMQExceptionType.GENERIC_EXCEPTION.getCode(), e.getMessage());
      }
   }

   @Override
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
         } else {
            throw ActiveMQJournalBundle.BUNDLE.fileNotOpened();
         }
         return;
      }

      position.addAndGet(bytes.limit());

      try {
         doInternalWrite(bytes, sync, callback);
      } catch (ClosedChannelException e) {
         throw e;
      } catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
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
