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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQInternalErrorException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.message.LargeBodyEncoder;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.jboss.logging.Logger;

import io.netty.buffer.Unpooled;

public final class LargeServerMessageImpl extends CoreMessage implements LargeServerMessage {

   // Constants -----------------------------------------------------
   private static final Logger logger = Logger.getLogger(LargeServerMessageImpl.class);

   // Attributes ----------------------------------------------------

   private final JournalStorageManager storageManager;

   private long pendingRecordID = NO_PENDING_ID;

   private boolean paged;

   // We should only use the NIO implementation on the Journal
   private SequentialFile file;

   // set when a copyFrom is called
   // The actual copy is done when finishCopy is called
   private SequentialFile pendingCopy;

   private long bodySize = -1;

   private final AtomicInteger delayDeletionCount = new AtomicInteger(0);

   // We cache this
   private volatile int memoryEstimate = -1;

   public LargeServerMessageImpl(final JournalStorageManager storageManager) {
      this.storageManager = storageManager;
   }

   /**
    * Copy constructor
    *
    * @param properties
    * @param copy
    * @param fileCopy
    */
   private LargeServerMessageImpl(final LargeServerMessageImpl copy,
                                  TypedProperties properties,
                                  final SequentialFile fileCopy,
                                  final long newID) {
      super(copy, properties);
      storageManager = copy.storageManager;
      file = fileCopy;
      bodySize = copy.bodySize;
      setMessageID(newID);
   }

   private static String toDate(long timestamp) {
      if (timestamp == 0) {
         return "0";
      } else {
         return new java.util.Date(timestamp).toString();
      }

   }

   @Override
   public boolean isServerMessage() {
      return true;
   }

   @Override
   public long getPendingRecordID() {
      return this.pendingRecordID;
   }

   /**
    * @param pendingRecordID
    */
   @Override
   public void setPendingRecordID(long pendingRecordID) {
      this.pendingRecordID = pendingRecordID;
   }

   @Override
   public void setPaged() {
      paged = true;
   }

   @Override
   public synchronized void addBytes(final byte[] bytes) throws Exception {
      validateFile();

      if (!file.isOpen()) {
         file.open();
      }

      storageManager.addBytesToLargeMessage(file, getMessageID(), bytes);

      bodySize += bytes.length;
   }

   @Override
   public synchronized int getEncodeSize() {
      return getHeadersAndPropertiesEncodeSize();
   }

   public void encode(final ActiveMQBuffer buffer1) {
      super.encodeHeadersAndProperties(buffer1.byteBuf());
   }

   public void decode(final ActiveMQBuffer buffer1) {
      file = null;

      super.decodeHeadersAndProperties(buffer1.byteBuf());
   }

   @Override
   public synchronized void incrementDelayDeletionCount() {
      delayDeletionCount.incrementAndGet();
      try {
         incrementRefCount();
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorIncrementDelayDeletionCount(e);
      }
   }

   @Override
   public synchronized void decrementDelayDeletionCount() throws Exception {
      int count = delayDeletionCount.decrementAndGet();

      decrementRefCount();

      if (count == 0) {
         checkDelete();
      }
   }

   @Override
   public LargeBodyEncoder getBodyEncoder() throws ActiveMQException {
      validateFile();
      return new DecodingContext();
   }

   private void checkDelete() throws Exception {
      if (getRefCount() <= 0) {
         if (logger.isTraceEnabled()) {
            logger.trace("Deleting file " + file + " as the usage was complete");
         }

         try {
            deleteFile();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.error(e.getMessage(), e);
         }
      }
   }

   @Override
   public synchronized int decrementRefCount() throws Exception {
      int currentRefCount = super.decrementRefCount();

      // We use <= as this could be used by load.
      // because of a failure, no references were loaded, so we have 0... and we still need to delete the associated
      // files
      if (delayDeletionCount.get() <= 0) {
         checkDelete();
      }

      return currentRefCount;
   }

   // Even though not recommended, in certain instances
   // we may need to convert a large message back to a whole buffer
   // in a way you can convert
   @Override
   public ActiveMQBuffer getReadOnlyBodyBuffer() {
      try {
         validateFile();
         file.open();
         int fileSize = (int) file.size();
         ByteBuffer buffer = this.storageManager.largeMessagesFactory.newBuffer(fileSize);
         file.read(buffer);
         return new ChannelBufferWrapper(Unpooled.wrappedBuffer(buffer));
      } catch (Exception e) {
         throw new RuntimeException(e);
      } finally {
         try {
            file.close();
         } catch (Exception ignored) {
         }
      }
   }

   @Override
   public boolean isLargeMessage() {
      return true;
   }

   @Override
   public synchronized void deleteFile() throws Exception {
      validateFile();
      releaseResources();
      storageManager.deleteLargeMessageFile(this);
   }

   @Override
   public synchronized int getMemoryEstimate() {
      if (memoryEstimate == -1) {
         // The body won't be on memory (aways on-file), so we don't consider this for paging
         memoryEstimate = getHeadersAndPropertiesEncodeSize() + DataConstants.SIZE_INT +
            getEncodeSize() +
            (16 + 4) * 2 +
            1;
      }

      return memoryEstimate;
   }

   @Override
   public synchronized void releaseResources() {
      if (file != null && file.isOpen()) {
         try {
            file.close();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.largeMessageErrorReleasingResources(e);
         }
      }
   }

   @Override
   public void referenceOriginalMessage(final Message original, String originalQueue) {

      super.referenceOriginalMessage(original, originalQueue);

      if (original instanceof LargeServerMessageImpl) {
         LargeServerMessageImpl otherLM = (LargeServerMessageImpl) original;
         this.paged = otherLM.paged;
         if (this.paged) {
            this.removeAnnotation(Message.HDR_ORIG_MESSAGE_ID);
         }
      }
   }

   @Override
   public Message copy() {
      SequentialFile newfile = storageManager.createFileForLargeMessage(messageID, durable);

      Message newMessage = new LargeServerMessageImpl(this, properties, newfile, messageID);
      return newMessage;
   }

   @Override
   public Message copy(final long newID) {
      try {
         LargeServerMessage newMessage = storageManager.createLargeMessage(newID, this);

         boolean originallyOpen = file != null && file.isOpen();

         validateFile();

         byte[] bufferBytes = new byte[100 * 1024];

         ByteBuffer buffer = ByteBuffer.wrap(bufferBytes);

         long oldPosition = file.position();

         file.open();
         file.position(0);

         for (;;) {
            // The buffer is reused...
            // We need to make sure we clear the limits and the buffer before reusing it
            buffer.clear();
            int bytesRead = file.read(buffer);

            byte[] bufferToWrite;
            if (bytesRead <= 0) {
               break;
            } else if (bytesRead == bufferBytes.length && !this.storageManager.isReplicated()) {
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

         file.position(oldPosition);

         if (!originallyOpen) {
            file.close();
            newMessage.getFile().close();
         }

         return newMessage;

      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.lareMessageErrorCopying(e, this);
         return null;
      }
   }

   @Override
   public SequentialFile getFile() throws ActiveMQException {
      validateFile();
      return file;
   }

   @Override
   public long getPersistentSize() throws ActiveMQException {
      long size = super.getPersistentSize();
      size += getBodyEncoder().getLargeBodySize();

      return size;
   }
   @Override
   public String toString() {
      try {
         return "LargeServerMessage[messageID=" + messageID + ",durable=" + isDurable() + ",userID=" + getUserID() + ",priority=" + this.getPriority() + ", timestamp=" + toDate(getTimestamp()) + ",expiration=" + toDate(getExpiration()) + ", durable=" + durable + ", address=" + getAddress() + ",size=" + getPersistentSize() + ",properties=" + (properties != null ? properties.toString() : "") + "]@" + System.identityHashCode(this);
      } catch (Exception e) {
         e.printStackTrace();
         return "LargeServerMessage[messageID=" + messageID + "]";
      }
   }

   // Private -------------------------------------------------------

   public synchronized void validateFile() throws ActiveMQException {
      try {
         if (file == null) {
            if (messageID <= 0) {
               throw new RuntimeException("MessageID not set on LargeMessage");
            }

            file = createFile();

            openFile();

            bodySize = file.size();
         }
      } catch (Exception e) {
         // TODO: There is an IO_ERROR on trunk now, this should be used here instead
         throw new ActiveMQInternalErrorException(e.getMessage(), e);
      }
   }

   /**
    *
    */
   protected SequentialFile createFile() {
      return storageManager.createFileForLargeMessage(getMessageID(), durable);
   }

   protected void openFile() throws Exception {
      if (file == null) {
         validateFile();
      } else if (!file.isOpen()) {
         file.open();
      }
   }

   protected void closeFile() throws Exception {
      if (file != null && file.isOpen()) {
         file.close();
      }
   }

   // Inner classes -------------------------------------------------

   class DecodingContext implements LargeBodyEncoder {

      private SequentialFile cFile;

      @Override
      public void open() throws ActiveMQException {
         try {
            if (cFile != null && cFile.isOpen()) {
               cFile.close();
            }
            cFile = file.cloneFile();
            cFile.open();
         } catch (Exception e) {
            throw new ActiveMQException(ActiveMQExceptionType.INTERNAL_ERROR, e.getMessage(), e);
         }
      }

      @Override
      public void close() throws ActiveMQException {
         try {
            if (cFile != null) {
               cFile.close();
            }
         } catch (Exception e) {
            throw new ActiveMQInternalErrorException(e.getMessage(), e);
         }
      }

      @Override
      public int encode(final ByteBuffer bufferRead) throws ActiveMQException {
         try {
            return cFile.read(bufferRead);
         } catch (Exception e) {
            throw new ActiveMQInternalErrorException(e.getMessage(), e);
         }
      }

      @Override
      public int encode(final ActiveMQBuffer bufferOut, final int size) throws ActiveMQException {
         // This could maybe be optimized (maybe reading directly into bufferOut)
         ByteBuffer bufferRead = ByteBuffer.allocate(size);

         int bytesRead = encode(bufferRead);

         bufferRead.flip();

         if (bytesRead > 0) {
            bufferOut.writeBytes(bufferRead.array(), 0, bytesRead);
         }

         return bytesRead;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.message.LargeBodyEncoder#getLargeBodySize()
       */
      @Override
      public long getLargeBodySize() {
         if (bodySize < 0) {
            try {
               bodySize = file.size();
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.unableToCalculateFileSize(e);
            }
         }
         return bodySize;
      }
   }
}
