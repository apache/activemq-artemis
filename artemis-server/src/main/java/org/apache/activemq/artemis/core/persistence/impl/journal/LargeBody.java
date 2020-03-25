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

package org.apache.activemq.artemis.core.persistence.impl.journal;

import java.nio.ByteBuffer;

import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQIOErrorException;
import org.apache.activemq.artemis.api.core.ActiveMQInternalErrorException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.message.LargeBodyReader;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.jboss.logging.Logger;

public class LargeBody {

   private static final Logger logger = Logger.getLogger(LargeBody.class);

   private long bodySize = -1;

   long NO_PENDING_ID = -1;

   private long pendingRecordID = NO_PENDING_ID;

   StorageManager storageManager;

   private long messageID = -1;

   private LargeServerMessage message;

   private boolean paged;

   // This is to be used only for appending
   private SequentialFile file;

   public LargeBody(LargeServerMessage message, StorageManager storageManager) {
      this.storageManager = storageManager;
      this.message = message;
   }

   public LargeBody(LargeServerMessage message, StorageManager storageManager, SequentialFile file) {
      this(message, storageManager);
      this.file = file;
   }

   public StorageManager getStorageManager() {
      return storageManager;
   }

   public void setStorageManager(StorageManager storageManager) {
      this.storageManager = storageManager;
   }

   public ByteBuffer map() throws Exception {
      ensureFileExists(true);
      if (!file.isOpen()) {
         file.open();
      }
      return file.map(0, file.size());
   }

   public LargeBody(long messageID, JournalStorageManager storageManager) {
      this(null, storageManager);
      this.messageID = messageID;
   }

   public void setMessage(LargeServerMessage message) {
      this.message = message;

   }

   public void setPaged() {
      this.paged = true;
   }

   public boolean isPaged() {
      return paged;
   }

   public void clearFile() {
      if (file != null && file.isOpen()) {
         try {
            file.close();
         } catch (Exception e) {
            // this shouldn't happen anyways, this close call is here just in case it ever happened
            logger.warn(e.getMessage(), e);
         }
      }

      file = null;
   }

   public synchronized void deleteFile() {
      try {
         validateFile();
         releaseResources(false);
         storageManager.deleteLargeMessageBody(message);
      } catch (Exception e) {
         storageManager.criticalError(e);
      }
   }

   public long getMessageID() {
      if (message == null) {
         return messageID;
      } else {
         return message.getMessageID();
      }
   }

   public synchronized void addBytes(final byte[] bytes) throws Exception {
      validateFile();

      if (!file.isOpen()) {
         file.open();
      }

      storageManager.addBytesToLargeMessage(file, getMessageID(), bytes);

      bodySize += bytes.length;
   }

   public synchronized void addBytes(final ActiveMQBuffer bytes) throws Exception {
      validateFile();

      if (!file.isOpen()) {
         file.open();
      }

      final int readableBytes = bytes.readableBytes();

      storageManager.addBytesToLargeMessage(file, getMessageID(), bytes);

      bodySize += readableBytes;
   }

   public synchronized void validateFile() throws ActiveMQException {
      this.ensureFileExists(true);
   }

   public synchronized void ensureFileExists(boolean toOpen) throws ActiveMQException {
      try {
         if (file == null) {
            if (getMessageID() <= 0) {
               throw new RuntimeException("MessageID not set on LargeMessage");
            }

            file = createFile();

            if (toOpen) {
               openFile();
            }

            bodySize = file.size();
         }
      } catch (Exception e) {
         throw new ActiveMQInternalErrorException(e.getMessage(), e);
      }
   }

   /**
    * This will return the bodySize without trying to open the file, just returning what's currently stored
    */
   public long getStoredBodySize() {
      return bodySize;
   }

   public void setBodySize(long size) {
      this.bodySize = size;
   }

   public long getBodySize() throws ActiveMQException {

      try {
         if (bodySize <= 0) {
            if (file != null) {
               bodySize = file.size();
            } else {
               SequentialFile tmpFile = createFile();
               bodySize = tmpFile.size();
               tmpFile.close(false);
            }
         }
         return bodySize;
      } catch (Exception e) {
         ActiveMQIOErrorException errorException = new ActiveMQIOErrorException();
         errorException.initCause(e);
         throw errorException;
      }
   }

   public LargeBodyReader getLargeBodyReader() {
      return new LargeBodyReaderImpl();
   }

   /**
    * This will return its own File useful for reading the file on the large message while delivering, browsing.. etc
    */
   public SequentialFile getReadingFile() throws ActiveMQException {
      ensureFileExists(false);
      return file.cloneFile();
   }

   /** Meant for test-ability, be careful if you decide to use it.
    *  and in case you use it for a real reason, please change the documentation here.
    * @param file
    */
   public void replaceFile(SequentialFile file) {
      this.file = file;
   }

   public SequentialFile getAppendFile() throws ActiveMQException {
      validateFile();
      return file;
   }

   public void checkDelete() {
      if (message.toMessage().getRefCount() <= 0 && message.toMessage().getUsage() <= 0 && message.toMessage().getDurableCount() <= 0) {
         if (logger.isTraceEnabled()) {
            try {
               logger.trace("Deleting file " + getAppendFile() + " as the usage was complete");
            } catch (Exception e) {
               // this is only after a trace, no need to do any special logging handling here
               logger.warn(e.getMessage(), e);
            }
         }

         deleteFile();
      }
   }

   public void referenceOriginalMessage(final LargeBody original) {
      if (original.isPaged()) {
         this.setPaged();
      }

      if (this.paged) {
         message.toMessage().removeAnnotation(Message.HDR_ORIG_MESSAGE_ID);
      }
   }

   public ActiveMQBuffer getReadOnlyBodyBuffer() {
      try {
         validateFile();
         file.open();
         int fileSize = (int) file.size();
         ByteBuffer buffer = ByteBuffer.allocate(fileSize);
         file.read(buffer);
         return new ChannelBufferWrapper(Unpooled.wrappedBuffer(buffer));
      } catch (Exception e) {
         throw new RuntimeException(e);
      } finally {
         try {
            file.close(false);
         } catch (Exception ignored) {
         }
      }
   }

   public int getBodyBufferSize() {
      final boolean closeFile = file == null || !file.isOpen();
      try {
         openFile();
         final long fileSize = file.size();
         int fileSizeAsInt = (int) fileSize;
         if (fileSizeAsInt < 0) {
            logger.warnf("suspicious large message file size of %d bytes for %s, will use %d instead.", fileSize, file.getFileName(), Integer.MAX_VALUE);
            fileSizeAsInt = Integer.MAX_VALUE;
         }
         return fileSizeAsInt;
      } catch (Exception e) {
         throw new RuntimeException(e);
      } finally {
         if (closeFile) {
            try {
               file.close(false);
            } catch (Exception ignored) {
            }
         }
      }
   }

   public synchronized void releaseResources(boolean sync) {
      if (file != null && file.isOpen()) {
         try {
            if (sync) {
               file.sync();
            }
            file.close(false);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.largeMessageErrorReleasingResources(e);
         }
      }
   }

   public void copyInto(LargeServerMessage newMessage) throws Exception {
      //clone a SequentialFile to avoid concurrent access
      SequentialFile cloneFile = getReadingFile();

      try {
         byte[] bufferBytes = new byte[100 * 1024];

         ByteBuffer buffer = ByteBuffer.wrap(bufferBytes);

         if (!cloneFile.isOpen()) {
            cloneFile.open();
         }

         cloneFile.position(0);

         for (; ; ) {
            // The buffer is reused...
            // We need to make sure we clear the limits and the buffer before reusing it
            buffer.clear();
            int bytesRead = cloneFile.read(buffer);

            byte[] bufferToWrite;
            if (bytesRead <= 0) {
               break;
            } else if (bytesRead == bufferBytes.length && this.storageManager instanceof JournalStorageManager && !((JournalStorageManager) this.storageManager).isReplicated()) {
               // ARTEMIS-1220: We cannot reuse the same buffer if it's replicated
               // otherwise there could be another thread still using the buffer on a
               // replication.
               bufferToWrite = bufferBytes;
            } else {
               bufferToWrite = new byte[bytesRead];
               System.arraycopy(bufferBytes, 0, bufferToWrite, 0, bytesRead);
            }

            newMessage.addBytes(bufferToWrite);

            if (bytesRead < bufferBytes.length) {
               break;
            }
         }
      } finally {
         cloneFile.close();
      }
   }

   public SequentialFile createFile() {
      return storageManager.createFileForLargeMessage(getMessageID(), message.toMessage().isDurable());
   }

   protected void openFile() throws Exception {
      if (file == null) {
         validateFile();
      } else if (!file.isOpen()) {
         file.open();
      }
   }

   class LargeBodyReaderImpl implements LargeBodyReader {

      private SequentialFile cFile;

      @Override
      public void open() throws ActiveMQException {
         try {
            if (cFile != null && cFile.isOpen()) {
               cFile.close(false);
            }
            cFile = getReadingFile();
            cFile.open();
         } catch (Exception e) {
            throw new ActiveMQException(ActiveMQExceptionType.INTERNAL_ERROR, e.getMessage(), e);
         }
      }

      @Override
      public void position(long position) throws ActiveMQException {
         try {
            cFile.position(position);
         } catch (Exception e) {
            throw new ActiveMQException(ActiveMQExceptionType.INTERNAL_ERROR, e.getMessage(), e);
         }
      }

      @Override
      public long position() {
         return cFile.position();
      }

      @Override
      public void close() throws ActiveMQException {
         try {
            if (cFile != null) {
               cFile.close(false);
               cFile = null;
            }
         } catch (Exception e) {
            throw new ActiveMQInternalErrorException(e.getMessage(), e);
         }
      }

      @Override
      public int readInto(final ByteBuffer bufferRead) throws ActiveMQException {
         try {
            return cFile.read(bufferRead);
         } catch (Exception e) {
            throw new ActiveMQInternalErrorException(e.getMessage(), e);
         }
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.message.LargeBodyEncoder#getSize()
       */
      @Override
      public long getSize() throws ActiveMQException {
         return getBodySize();
      }
   }

   public boolean hasPendingRecord() {
      return pendingRecordID != NO_PENDING_ID;
   }

   public void clearPendingRecordID() {
      setPendingRecordID(NO_PENDING_ID);
   }


   public long getPendingRecordID() {
      return this.pendingRecordID;
   }

   public void setPendingRecordID(long pendingRecordID) {
      this.pendingRecordID = pendingRecordID;
   }


}
