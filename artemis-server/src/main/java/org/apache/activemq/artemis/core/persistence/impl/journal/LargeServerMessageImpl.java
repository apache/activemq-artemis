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
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.message.BodyEncoder;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.TypedProperties;

public final class LargeServerMessageImpl extends ServerMessageImpl implements LargeServerMessage {

   // Constants -----------------------------------------------------
   private static boolean isTrace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private final JournalStorageManager storageManager;

   private long pendingRecordID = -1;

   private boolean paged;

   // We should only use the NIO implementation on the Journal
   private SequentialFile file;

   // set when a copyFrom is called
   // The actual copy is done when finishCopy is called
   private SequentialFile pendingCopy;

   private long bodySize = -1;

   private final AtomicInteger delayDeletionCount = new AtomicInteger(0);

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

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

   // Public --------------------------------------------------------

   /**
    * @param pendingRecordID
    */
   @Override
   public void setPendingRecordID(long pendingRecordID) {
      this.pendingRecordID = pendingRecordID;
   }

   @Override
   public long getPendingRecordID() {
      return this.pendingRecordID;
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

   public void encodeBody(final ActiveMQBuffer bufferOut, final BodyEncoder context, final int size) {
      try {
         // This could maybe be optimized (maybe reading directly into bufferOut)
         ByteBuffer bufferRead = ByteBuffer.allocate(size);

         int bytesRead = context.encode(bufferRead);

         bufferRead.flip();

         if (bytesRead > 0) {
            bufferOut.writeBytes(bufferRead.array(), 0, bytesRead);
         }

      }
      catch (Exception e) {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   @Override
   public synchronized int getEncodeSize() {
      return getHeadersAndPropertiesEncodeSize();
   }

   @Override
   public void encode(final ActiveMQBuffer buffer1) {
      super.encodeHeadersAndProperties(buffer1);
   }

   @Override
   public void decode(final ActiveMQBuffer buffer1) {
      file = null;

      super.decodeHeadersAndProperties(buffer1);
   }

   @Override
   public synchronized void incrementDelayDeletionCount() {
      delayDeletionCount.incrementAndGet();
      try {
         incrementRefCount();
      }
      catch (Exception e) {
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
   public BodyEncoder getBodyEncoder() throws ActiveMQException {
      validateFile();
      return new DecodingContext();
   }

   private void checkDelete() throws Exception {
      if (getRefCount() <= 0) {
         if (LargeServerMessageImpl.isTrace) {
            ActiveMQServerLogger.LOGGER.trace("Deleting file " + file + " as the usage was complete");
         }

         try {
            deleteFile();
         }
         catch (Exception e) {
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

   // We cache this
   private volatile int memoryEstimate = -1;

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
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.largeMessageErrorReleasingResources(e);
         }
      }
   }

   @Override
   public void setOriginalHeaders(final ServerMessage other,
                                  final MessageReference originalReference,
                                  final boolean expiry) {
      super.setOriginalHeaders(other, originalReference, expiry);

      LargeServerMessageImpl otherLM = (LargeServerMessageImpl) other;
      this.paged = otherLM.paged;
      if (this.paged) {
         this.removeProperty(Message.HDR_ORIG_MESSAGE_ID);
      }
   }

   @Override
   public ServerMessage copy() {
      SequentialFile newfile = storageManager.createFileForLargeMessage(messageID, durable);

      ServerMessage newMessage = new LargeServerMessageImpl(this, properties, newfile, messageID);
      return newMessage;
   }

   @Override
   public ServerMessage copy(final long newID) {
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
            }
            else if (bytesRead == bufferBytes.length) {
               bufferToWrite = bufferBytes;
            }
            else {
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
         }

         return newMessage;


      }
      catch (Exception e) {
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
   public String toString() {
      return "LargeServerMessage[messageID=" + messageID + ",priority=" + this.getPriority() +
         ",expiration=[" + (this.getExpiration() != 0 ? new java.util.Date(this.getExpiration()) : "null") + "]" +
         ", durable=" + durable + ", address=" + getAddress() + ",properties=" + properties.toString() + "]@" + System.identityHashCode(this);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void finalize() throws Throwable {
      releaseResources();
      super.finalize();
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
      }
      catch (Exception e) {
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
      }
      else if (!file.isOpen()) {
         file.open();
      }
   }

   protected void closeFile() throws Exception {
      if (file != null && file.isOpen()) {
         file.close();
      }
   }

   // Inner classes -------------------------------------------------

   class DecodingContext implements BodyEncoder {

      private SequentialFile cFile;

      @Override
      public void open() throws ActiveMQException {
         try {
            if (cFile != null && cFile.isOpen()) {
               cFile.close();
            }
            cFile = file.cloneFile();
            cFile.open();
         }
         catch (Exception e) {
            throw new ActiveMQException(ActiveMQExceptionType.INTERNAL_ERROR, e.getMessage(), e);
         }
      }

      @Override
      public void close() throws ActiveMQException {
         try {
            if (cFile != null) {
               cFile.close();
            }
         }
         catch (Exception e) {
            throw new ActiveMQInternalErrorException(e.getMessage(), e);
         }
      }

      @Override
      public int encode(final ByteBuffer bufferRead) throws ActiveMQException {
         try {
            return cFile.read(bufferRead);
         }
         catch (Exception e) {
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
       * @see org.apache.activemq.artemis.core.message.BodyEncoder#getLargeBodySize()
       */
      @Override
      public long getLargeBodySize() {
         if (bodySize < 0) {
            try {
               bodySize = file.size();
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }
         return bodySize;
      }
   }
}
