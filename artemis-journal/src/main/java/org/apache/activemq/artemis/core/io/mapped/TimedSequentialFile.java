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

package org.apache.activemq.artemis.core.io.mapped;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.io.DummyCallback;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.buffer.TimedBuffer;
import org.apache.activemq.artemis.core.io.buffer.TimedBufferObserver;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.impl.SimpleWaitIOCallback;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;

final class TimedSequentialFile implements SequentialFile {

   private final SequentialFileFactory factory;
   private final SequentialFile sequentialFile;
   private final LocalBufferObserver observer;
   private TimedBuffer timedBuffer;

   TimedSequentialFile(SequentialFileFactory factory, SequentialFile sequentialFile) {
      this.sequentialFile = sequentialFile;
      this.factory = factory;
      this.observer = new LocalBufferObserver();
   }

   @Override
   public boolean isOpen() {
      return this.sequentialFile.isOpen();
   }

   @Override
   public boolean exists() {
      return this.sequentialFile.exists();
   }

   @Override
   public void open() throws Exception {
      this.sequentialFile.open();
   }

   @Override
   public void open(int maxIO, boolean useExecutor) throws Exception {
      this.sequentialFile.open(maxIO, useExecutor);
   }

   @Override
   public boolean fits(int size) {
      if (timedBuffer == null) {
         return this.sequentialFile.fits(size);
      } else {
         return timedBuffer.checkSize(size);
      }
   }

   @Override
   public int calculateBlockStart(int position) throws Exception {
      return this.sequentialFile.calculateBlockStart(position);
   }

   @Override
   public String getFileName() {
      return this.sequentialFile.getFileName();
   }

   @Override
   public void fill(int size) throws Exception {
      this.sequentialFile.fill(size);
   }

   @Override
   public void delete() throws IOException, InterruptedException, ActiveMQException {
      this.sequentialFile.delete();
   }

   @Override
   public void write(ActiveMQBuffer bytes, boolean sync, IOCallback callback) throws Exception {
      if (this.timedBuffer != null) {
         this.timedBuffer.addBytes(bytes, sync, callback);
      } else {
         this.sequentialFile.write(bytes, sync, callback);
      }
   }

   @Override
   public void write(ActiveMQBuffer bytes, boolean sync) throws Exception {
      if (sync) {
         if (this.timedBuffer != null) {
            //the only way to avoid allocations is by using a lock-free pooled callback -> CyclicBarrier allocates on each new Generation!!!
            final SimpleWaitIOCallback callback = new SimpleWaitIOCallback();
            this.timedBuffer.addBytes(bytes, true, callback);
            callback.waitCompletion();
         } else {
            this.sequentialFile.write(bytes, true);
         }
      } else {
         if (this.timedBuffer != null) {
            this.timedBuffer.addBytes(bytes, false, DummyCallback.getInstance());
         } else {
            this.sequentialFile.write(bytes, false);
         }
      }
   }

   @Override
   public void write(EncodingSupport bytes, boolean sync, IOCallback callback) throws Exception {
      if (this.timedBuffer != null) {
         this.timedBuffer.addBytes(bytes, sync, callback);
      } else {
         this.sequentialFile.write(bytes, sync, callback);
      }
   }

   @Override
   public void write(EncodingSupport bytes, boolean sync) throws Exception {
      if (sync) {
         if (this.timedBuffer != null) {
            //the only way to avoid allocations is by using a lock-free pooled callback -> CyclicBarrier allocates on each new Generation!!!
            final SimpleWaitIOCallback callback = new SimpleWaitIOCallback();
            this.timedBuffer.addBytes(bytes, true, callback);
            callback.waitCompletion();
         } else {
            this.sequentialFile.write(bytes, true);
         }
      } else {
         if (this.timedBuffer != null) {
            this.timedBuffer.addBytes(bytes, false, DummyCallback.getInstance());
         } else {
            this.sequentialFile.write(bytes, false);
         }
      }
   }

   @Override
   public void writeDirect(ByteBuffer bytes, boolean sync, IOCallback callback) {
      this.sequentialFile.writeDirect(bytes, sync, callback);
   }

   @Override
   public void writeDirect(ByteBuffer bytes, boolean sync) throws Exception {
      this.sequentialFile.writeDirect(bytes, sync);
   }

   @Override
   public int read(ByteBuffer bytes, IOCallback callback) throws Exception {
      return this.sequentialFile.read(bytes, callback);
   }

   @Override
   public int read(ByteBuffer bytes) throws Exception {
      return this.sequentialFile.read(bytes);
   }

   @Override
   public void position(long pos) throws IOException {
      this.sequentialFile.position(pos);
   }

   @Override
   public long position() {
      return this.sequentialFile.position();
   }

   @Override
   public void close() throws Exception {
      try {
         this.sequentialFile.close();
      } finally {
         this.timedBuffer = null;
      }
   }

   @Override
   public void sync() throws IOException {
      this.sequentialFile.sync();
   }

   @Override
   public long size() throws Exception {
      return this.sequentialFile.size();
   }

   @Override
   public void renameTo(String newFileName) throws Exception {
      this.sequentialFile.renameTo(newFileName);
   }

   @Override
   public SequentialFile cloneFile() {
      return new TimedSequentialFile(factory, this.sequentialFile.cloneFile());
   }

   @Override
   public void copyTo(SequentialFile newFileName) throws Exception {
      this.sequentialFile.copyTo(newFileName);
   }

   @Override
   public void setTimedBuffer(TimedBuffer buffer) {
      if (this.timedBuffer != null) {
         this.timedBuffer.setObserver(null);
      }
      this.timedBuffer = buffer;
      if (buffer != null) {
         buffer.setObserver(this.observer);
      }
   }

   @Override
   public File getJavaFile() {
      return this.sequentialFile.getJavaFile();
   }

   private static void invokeDoneOn(List<? extends IOCallback> callbacks) {
      final int size = callbacks.size();
      for (int i = 0; i < size; i++) {
         try {
            final IOCallback callback = callbacks.get(i);
            callback.done();
         } catch (Throwable e) {
            ActiveMQJournalLogger.LOGGER.errorCompletingCallback(e);
         }
      }
   }

   private static void invokeOnErrorOn(final int errorCode,
                                       final String errorMessage,
                                       List<? extends IOCallback> callbacks) {
      final int size = callbacks.size();
      for (int i = 0; i < size; i++) {
         try {
            final IOCallback callback = callbacks.get(i);
            callback.onError(errorCode, errorMessage);
         } catch (Throwable e) {
            ActiveMQJournalLogger.LOGGER.errorCallingErrorCallback(e);
         }
      }
   }

   private static final class DelegateCallback implements IOCallback {

      List<IOCallback> delegates;

      private DelegateCallback() {
         this.delegates = null;
      }

      @Override
      public void done() {
         invokeDoneOn(delegates);
      }

      @Override
      public void onError(final int errorCode, final String errorMessage) {
         invokeOnErrorOn(errorCode, errorMessage, delegates);
      }
   }

   private final class LocalBufferObserver implements TimedBufferObserver {

      private final DelegateCallback delegateCallback = new DelegateCallback();

      @Override
      public void flushBuffer(final ByteBuffer buffer, final boolean requestedSync, final List<IOCallback> callbacks) {
         buffer.flip();

         if (buffer.limit() == 0) {
            try {
               invokeDoneOn(callbacks);
            } finally {
               factory.releaseBuffer(buffer);
            }
         } else {
            if (callbacks.isEmpty()) {
               try {
                  sequentialFile.writeDirect(buffer, requestedSync);
               } catch (Exception e) {
                  throw new IllegalStateException(e);
               }
            } else {
               delegateCallback.delegates = callbacks;
               try {
                  sequentialFile.writeDirect(buffer, requestedSync, delegateCallback);
               } finally {
                  delegateCallback.delegates = null;
               }
            }
         }
      }

      @Override
      public ByteBuffer newBuffer(final int size, final int limit) {
         return factory.newBuffer(limit);
      }

      @Override
      public int getRemainingBytes() {
         try {
            final long position = sequentialFile.position();
            final long size = sequentialFile.size();
            final long remaining = size - position;
            if (remaining > Integer.MAX_VALUE) {
               return Integer.MAX_VALUE;
            } else {
               return (int) remaining;
            }
         } catch (Exception e) {
            throw new IllegalStateException(e);
         }
      }

      @Override
      public String toString() {
         return "TimedBufferObserver on file (" + sequentialFile.getFileName() + ")";
      }

   }
}
