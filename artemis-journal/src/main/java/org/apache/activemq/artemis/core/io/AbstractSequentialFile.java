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
package org.apache.activemq.artemis.core.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIOErrorException;
import org.apache.activemq.artemis.core.io.buffer.TimedBuffer;
import org.apache.activemq.artemis.core.io.buffer.TimedBufferObserver;
import org.apache.activemq.artemis.core.io.util.FileIOUtil;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.impl.SimpleWaitIOCallback;
import org.apache.activemq.artemis.journal.ActiveMQJournalBundle;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.jboss.logging.Logger;

public abstract class AbstractSequentialFile implements SequentialFile {

   private static final Logger logger = Logger.getLogger(AbstractSequentialFile.class);

   private File file;

   protected final File directory;

   protected final SequentialFileFactory factory;

   protected long fileSize = 0;

   protected final AtomicLong position = new AtomicLong(0);

   protected TimedBuffer timedBuffer;

   /**
    * Instead of having AIOSequentialFile implementing the Observer, I have done it on an inner class.
    * This is the class returned to the factory when the file is being activated.
    */
   protected final TimedBufferObserver timedBufferObserver = new LocalBufferObserver();

   /**
    * @param file
    * @param directory
    */
   public AbstractSequentialFile(final File directory,
                                 final String file,
                                 final SequentialFileFactory factory,
                                 final Executor writerExecutor) {
      super();
      this.file = new File(directory, file);
      this.directory = directory;
      this.factory = factory;
   }

   // Public --------------------------------------------------------

   @Override
   public final boolean exists() {
      return file.exists();
   }

   @Override
   public final String getFileName() {
      return file.getName();
   }

   @Override
   public final void delete() throws IOException, InterruptedException, ActiveMQException {
      if (isOpen()) {
         close();
      }

      if (file.exists() && !file.delete()) {
         ActiveMQJournalLogger.LOGGER.errorDeletingFile(this);
      }
   }

   @Override
   public void copyTo(SequentialFile newFileName) throws Exception {
      try {
         ActiveMQJournalLogger.LOGGER.debug("Copying " + this + " as " + newFileName);
         if (!newFileName.isOpen()) {
            newFileName.open();
         }

         if (!isOpen()) {
            this.open();
         }

         ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);

         FileIOUtil.copyData(this, newFileName, buffer);
         newFileName.close();
         this.close();
      } catch (ClosedChannelException e) {
         throw e;
      } catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }
   }

   /**
    * @throws IOException only declare exception due to signature. Sub-class needs it.
    */
   @Override
   public void position(final long pos) throws IOException {
      position.set(pos);
   }

   @Override
   public long position() {
      return position.get();
   }

   @Override
   public final void renameTo(final String newFileName) throws IOException, InterruptedException, ActiveMQException {
      try {
         close();
      } catch (ClosedChannelException e) {
         throw e;
      } catch (IOException e) {
         factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         throw e;
      }

      File newFile = new File(directory + "/" + newFileName);

      if (!file.equals(newFile)) {
         if (!file.renameTo(newFile)) {
            throw ActiveMQJournalBundle.BUNDLE.ioRenameFileError(file.getName(), newFileName);
         }
         file = newFile;
      }
   }

   /**
    * @throws IOException       we declare throwing IOException because sub-classes need to do it
    * @throws ActiveMQException
    */
   @Override
   public synchronized void close() throws IOException, InterruptedException, ActiveMQException {
   }

   @Override
   public final boolean fits(final int size) {
      if (timedBuffer == null) {
         return position.get() + size <= fileSize;
      } else {
         return timedBuffer.checkSize(size);
      }
   }

   @Override
   public void setTimedBuffer(final TimedBuffer buffer) {
      if (timedBuffer != null) {
         timedBuffer.setObserver(null);
      }

      timedBuffer = buffer;

      if (buffer != null) {
         buffer.setObserver(timedBufferObserver);
      }

   }

   @Override
   public void write(final ActiveMQBuffer bytes, final boolean sync, final IOCallback callback) throws IOException {
      if (timedBuffer != null) {
         bytes.setIndex(0, bytes.capacity());
         timedBuffer.addBytes(bytes, sync, callback);
      } else {
         final int readableBytes = bytes.readableBytes();
         final ByteBuffer buffer = factory.newBuffer(readableBytes);
         //factory::newBuffer doesn't necessary return a buffer with limit == readableBytes!!
         buffer.limit(readableBytes);
         bytes.getBytes(bytes.readerIndex(), buffer);
         buffer.flip();
         writeDirect(buffer, sync, callback);
      }
   }

   @Override
   public void write(final ActiveMQBuffer bytes,
                     final boolean sync) throws IOException, InterruptedException, ActiveMQException {
      if (sync) {
         SimpleWaitIOCallback completion = new SimpleWaitIOCallback();

         write(bytes, true, completion);

         completion.waitCompletion();
      } else {
         write(bytes, false, DummyCallback.getInstance());
      }
   }

   @Override
   public void write(final EncodingSupport bytes, final boolean sync, final IOCallback callback) {
      if (timedBuffer != null) {
         timedBuffer.addBytes(bytes, sync, callback);
      } else {
         final int encodedSize = bytes.getEncodeSize();
         ByteBuffer buffer = factory.newBuffer(encodedSize);
         ActiveMQBuffer outBuffer = ActiveMQBuffers.wrappedBuffer(buffer);
         bytes.encode(outBuffer);
         buffer.clear();
         buffer.limit(encodedSize);
         writeDirect(buffer, sync, callback);
      }
   }

   @Override
   public void write(final EncodingSupport bytes, final boolean sync) throws InterruptedException, ActiveMQException {
      if (sync) {
         SimpleWaitIOCallback completion = new SimpleWaitIOCallback();

         write(bytes, true, completion);

         completion.waitCompletion();
      } else {
         write(bytes, false, DummyCallback.getInstance());
      }
   }

   protected File getFile() {
      return file;
   }

   private static final class DelegateCallback implements IOCallback {

      final List<IOCallback> delegates;

      private DelegateCallback(final List<IOCallback> delegates) {
         this.delegates = delegates;
      }

      @Override
      public void done() {
         final int size = delegates.size();
         for (int i = 0; i < size; i++) {
            try {
               delegates.get(i).done();
            } catch (Throwable e) {
               ActiveMQJournalLogger.LOGGER.errorCompletingCallback(e);
            }
         }
      }

      @Override
      public void onError(final int errorCode, final String errorMessage) {
         if (logger.isTraceEnabled()) {
            logger.trace("onError" + " code: " + errorCode + " message: " + errorMessage);
         }

         final int size = delegates.size();
         for (int i = 0; i < size; i++) {
            try {
               delegates.get(i).onError(errorCode, errorMessage);
            } catch (Throwable e) {
               ActiveMQJournalLogger.LOGGER.errorCallingErrorCallback(e);
            }
         }
      }
   }

   protected ByteBuffer newBuffer(int size, int limit) {
      size = factory.calculateBlockSize(size);
      limit = factory.calculateBlockSize(limit);

      ByteBuffer buffer = factory.newBuffer(size);
      buffer.limit(limit);
      return buffer;
   }

   protected class LocalBufferObserver implements TimedBufferObserver {

      @Override
      public void flushBuffer(final ByteBuffer buffer, final boolean requestedSync, final List<IOCallback> callbacks) {
         buffer.flip();

         if (buffer.limit() == 0) {
            factory.releaseBuffer(buffer);
         } else {
            writeDirect(buffer, requestedSync, new DelegateCallback(callbacks));
         }
      }

      @Override
      public ByteBuffer newBuffer(final int size, final int limit) {
         return AbstractSequentialFile.this.newBuffer(size, limit);
      }

      @Override
      public int getRemainingBytes() {
         if (fileSize - position.get() > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
         } else {
            return (int) (fileSize - position.get());
         }
      }

      @Override
      public String toString() {
         return "TimedBufferObserver on file (" + getFile().getName() + ")";
      }

   }

   @Override
   public File getJavaFile() {
      return getFile().getAbsoluteFile();
   }
}
