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
package org.apache.activemq.artemis.core.io.mapped;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQIOErrorException;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.buffer.TimedBuffer;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.journal.ActiveMQJournalBundle;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;

final class MappedSequentialFile implements SequentialFile {

   private final File directory;
   private final long chunkBytes;
   private final long overlapBytes;
   private final IOCriticalErrorListener criticalErrorListener;
   private final MappedSequentialFileFactory factory;
   private File file;
   private File absoluteFile;
   private String fileName;
   private MappedFile mappedFile;

   MappedSequentialFile(MappedSequentialFileFactory factory,
                        final File directory,
                        final File file,
                        final long chunkBytes,
                        final long overlapBytes,
                        final IOCriticalErrorListener criticalErrorListener) {
      this.factory = factory;
      this.directory = directory;
      this.file = file;
      this.absoluteFile = null;
      this.fileName = null;
      this.chunkBytes = chunkBytes;
      this.overlapBytes = overlapBytes;
      this.mappedFile = null;
      this.criticalErrorListener = criticalErrorListener;
   }

   private void checkIsOpen() {
      if (!isOpen()) {
         throw new IllegalStateException("File not opened!");
      }
   }

   private void checkIsOpen(IOCallback callback) {
      if (!isOpen()) {
         callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), "File not opened");
      }
   }

   private void checkIsNotOpen() {
      if (isOpen()) {
         throw new IllegalStateException("File opened!");
      }
   }

   @Override
   public boolean isOpen() {
      return this.mappedFile != null;
   }

   @Override
   public boolean exists() {
      return this.file.exists();
   }

   @Override
   public void open() throws IOException {
      if (this.mappedFile == null) {
         this.mappedFile = MappedFile.of(file, chunkBytes, overlapBytes);
      }
   }

   @Override
   public void open(int maxIO, boolean useExecutor) throws IOException {
      //ignore maxIO e useExecutor
      this.open();
   }

   @Override
   public boolean fits(int size) {
      checkIsOpen();
      final long newPosition = this.mappedFile.position() + size;
      final boolean hasRemaining = newPosition <= this.mappedFile.length();
      return hasRemaining;
   }

   @Override
   public int calculateBlockStart(int position) {
      return position;
   }

   @Override
   public String getFileName() {
      if (this.fileName == null) {
         this.fileName = this.file.getName();
      }
      return this.fileName;
   }

   @Override
   public void fill(int size) throws IOException {
      checkIsOpen();
      this.mappedFile.zeros(this.mappedFile.position(), size);
   }

   @Override
   public void delete() {
      close();
      if (file.exists() && !file.delete()) {
         ActiveMQJournalLogger.LOGGER.errorDeletingFile(this);
      }
   }

   @Override
   public void write(ActiveMQBuffer bytes, boolean sync, IOCallback callback) throws IOException {
      if (callback == null) {
         throw new NullPointerException("callback parameter need to be set");
      }
      checkIsOpen(callback);
      try {
         final ByteBuf byteBuf = bytes.byteBuf();
         final int writerIndex = byteBuf.writerIndex();
         final int readerIndex = byteBuf.readerIndex();
         final int readableBytes = writerIndex - readerIndex;
         if (readableBytes > 0) {
            this.mappedFile.write(byteBuf, readerIndex, readableBytes);
            if (factory.isDatasync() && sync) {
               this.mappedFile.force();
            }
         }
         callback.done();
      } catch (IOException e) {
         if (this.criticalErrorListener != null) {
            this.criticalErrorListener.onIOException(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         }
         callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getMessage());
         throw e;
      }
   }

   @Override
   public void write(ActiveMQBuffer bytes, boolean sync) throws IOException {
      checkIsOpen();
      final ByteBuf byteBuf = bytes.byteBuf();
      final int writerIndex = byteBuf.writerIndex();
      final int readerIndex = byteBuf.readerIndex();
      final int readableBytes = writerIndex - readerIndex;
      if (readableBytes > 0) {
         this.mappedFile.write(byteBuf, readerIndex, readableBytes);
         if (factory.isDatasync() && sync) {
            this.mappedFile.force();
         }
      }
   }

   @Override
   public void write(EncodingSupport bytes, boolean sync, IOCallback callback) throws IOException {
      if (callback == null) {
         throw new NullPointerException("callback parameter need to be set");
      }
      checkIsOpen(callback);
      try {
         this.mappedFile.write(bytes);
         if (factory.isDatasync() && sync) {
            this.mappedFile.force();
         }
         callback.done();
      } catch (IOException e) {
         if (this.criticalErrorListener != null) {
            this.criticalErrorListener.onIOException(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         }
         callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getMessage());
         throw e;
      }
   }

   @Override
   public void write(EncodingSupport bytes, boolean sync) throws IOException {
      checkIsOpen();
      this.mappedFile.write(bytes);
      if (factory.isDatasync() && sync) {
         this.mappedFile.force();
      }
   }

   @Override
   public void writeDirect(ByteBuffer bytes, boolean sync, IOCallback callback) {
      if (callback == null) {
         throw new NullPointerException("callback parameter need to be set");
      }
      checkIsOpen(callback);
      try {
         final int position = bytes.position();
         final int limit = bytes.limit();
         final int remaining = limit - position;
         if (remaining > 0) {
            this.mappedFile.write(bytes, position, remaining);
            final int newPosition = position + remaining;
            bytes.position(newPosition);
            if (factory.isDatasync() && sync) {
               this.mappedFile.force();
            }
         }
         callback.done();
      } catch (IOException e) {
         if (this.criticalErrorListener != null) {
            this.criticalErrorListener.onIOException(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         }
         callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getMessage());
         throw new RuntimeException(e);
      }
   }

   @Override
   public void writeDirect(ByteBuffer bytes, boolean sync) throws IOException {
      checkIsOpen();
      final int position = bytes.position();
      final int limit = bytes.limit();
      final int remaining = limit - position;
      if (remaining > 0) {
         this.mappedFile.write(bytes, position, remaining);
         final int newPosition = position + remaining;
         bytes.position(newPosition);
         if (factory.isDatasync() && sync) {
            this.mappedFile.force();
         }
      }
   }

   @Override
   public int read(ByteBuffer bytes, IOCallback callback) throws IOException {
      if (callback == null) {
         throw new NullPointerException("callback parameter need to be set");
      }
      checkIsOpen(callback);
      try {
         final int position = bytes.position();
         final int limit = bytes.limit();
         final int remaining = limit - position;
         if (remaining > 0) {
            final int bytesRead = this.mappedFile.read(bytes, position, remaining);
            final int newPosition = position + bytesRead;
            bytes.position(newPosition);
            bytes.flip();
            callback.done();
            return bytesRead;
         } else {
            callback.done();
            return 0;
         }
      } catch (IOException e) {
         if (this.criticalErrorListener != null) {
            this.criticalErrorListener.onIOException(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         }
         callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getMessage());
         throw e;
      }
   }

   @Override
   public int read(ByteBuffer bytes) throws IOException {
      checkIsOpen();
      final int position = bytes.position();
      final int limit = bytes.limit();
      final int remaining = limit - position;
      if (remaining > 0) {
         final int bytesRead = this.mappedFile.read(bytes, position, remaining);
         final int newPosition = position + bytesRead;
         bytes.position(newPosition);
         bytes.flip();
         return bytesRead;
      }
      return 0;
   }

   @Override
   public void position(long pos) {
      checkIsOpen();
      this.mappedFile.position(pos);
   }

   @Override
   public long position() {
      checkIsOpen();
      return this.mappedFile.position();
   }

   @Override
   public void close() {
      if (this.mappedFile != null) {
         this.mappedFile.closeAndResize(this.mappedFile.length());
         this.mappedFile = null;
      }
   }

   @Override
   public void sync() throws IOException {
      checkIsOpen();
      this.mappedFile.force();
   }

   @Override
   public long size() {
      if (this.mappedFile != null) {
         return this.mappedFile.length();
      } else {
         return this.file.length();
      }
   }

   @Override
   public void renameTo(String newFileName) throws Exception {
      try {
         close();
      } catch (Exception e) {
         if (e instanceof IOException) {
            factory.onIOError(new ActiveMQIOErrorException(e.getMessage(), e), e.getMessage(), this);
         }
         throw e;
      }
      if (this.fileName == null) {
         this.fileName = this.file.getName();
      }
      if (!this.fileName.contentEquals(newFileName)) {
         final File newFile = new File(this.directory, newFileName);
         if (!file.renameTo(newFile)) {
            throw ActiveMQJournalBundle.BUNDLE.ioRenameFileError(file.getName(), newFileName);
         } else {
            this.file = newFile;
            this.fileName = newFileName;
            this.absoluteFile = null;
         }
      }
   }

   @Override
   public SequentialFile cloneFile() {
      checkIsNotOpen();
      return new MappedSequentialFile(factory, this.directory, this.file, this.chunkBytes, this.overlapBytes, this.criticalErrorListener);
   }

   @Override
   public void copyTo(SequentialFile dstFile) throws IOException {
      checkIsNotOpen();
      if (dstFile.isOpen()) {
         throw new IllegalArgumentException("dstFile must be closed too");
      }
      try (RandomAccessFile src = new RandomAccessFile(file, "rw"); FileChannel srcChannel = src.getChannel(); FileLock srcLock = srcChannel.lock()) {
         final long readableBytes = srcChannel.size();
         if (readableBytes > 0) {
            try (RandomAccessFile dst = new RandomAccessFile(dstFile.getJavaFile(), "rw"); FileChannel dstChannel = dst.getChannel(); FileLock dstLock = dstChannel.lock()) {
               final long oldLength = dst.length();
               final long newLength = oldLength + readableBytes;
               dst.setLength(newLength);
               final long transferred = dstChannel.transferFrom(srcChannel, oldLength, readableBytes);
               if (transferred != readableBytes) {
                  dstChannel.truncate(oldLength);
                  throw new IOException("copied less then expected");
               }
            }
         }
      }
   }

   @Override
   @Deprecated
   public void setTimedBuffer(TimedBuffer buffer) {
      throw new UnsupportedOperationException("the timed buffer is not currently supported");
   }

   @Override
   public File getJavaFile() {
      if (this.absoluteFile == null) {
         this.absoluteFile = this.file.getAbsoluteFile();
      }
      return this.absoluteFile;
   }
}
