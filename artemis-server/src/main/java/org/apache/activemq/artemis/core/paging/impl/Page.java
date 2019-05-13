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
package org.apache.activemq.artemis.core.paging.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.cursor.LivePageCache;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscriptionCounter;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.Env;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.jboss.logging.Logger;

public final class Page implements Comparable<Page> {

   // Constants -----------------------------------------------------
   private static final Logger logger = Logger.getLogger(Page.class);

   public static final int SIZE_RECORD = DataConstants.SIZE_BYTE + DataConstants.SIZE_INT + DataConstants.SIZE_BYTE;

   private static final byte START_BYTE = (byte) '{';

   private static final byte END_BYTE = (byte) '}';

   // Attributes ----------------------------------------------------

   private final int pageId;

   private boolean suspiciousRecords = false;

   private final AtomicInteger numberOfMessages = new AtomicInteger(0);

   private final SequentialFile file;

   private final SequentialFileFactory fileFactory;

   /**
    * The page cache that will be filled with data as we write more data
    */
   private volatile LivePageCache pageCache;

   private final AtomicInteger size = new AtomicInteger(0);

   private final StorageManager storageManager;

   private final SimpleString storeName;

   /**
    * A list of subscriptions containing pending counters (with non tx adds) on this page
    */
   private Set<PageSubscriptionCounter> pendingCounters;

   public Page(final SimpleString storeName,
               final StorageManager storageManager,
               final SequentialFileFactory factory,
               final SequentialFile file,
               final int pageId) throws Exception {
      this.pageId = pageId;
      this.file = file;
      fileFactory = factory;
      this.storageManager = storageManager;
      this.storeName = storeName;
   }

   public int getPageId() {
      return pageId;
   }

   public void setLiveCache(LivePageCache pageCache) {
      this.pageCache = pageCache;
   }

   public synchronized List<PagedMessage> read(StorageManager storage) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug("reading page " + this.pageId + " on address = " + storeName);
      }

      if (!file.isOpen()) {
         throw ActiveMQMessageBundle.BUNDLE.invalidPageIO();
      }

      size.lazySet((int) file.size());

      final List<PagedMessage> messages = readFromSequentialFile(storage);

      numberOfMessages.lazySet(messages.size());

      return messages;
   }

   private ByteBuffer allocateAndReadIntoFileBuffer(ByteBuffer fileBuffer, int requiredBytes) throws Exception {
      final ByteBuffer newFileBuffer = fileFactory.newBuffer(Math.max(requiredBytes, MIN_CHUNK_SIZE));
      newFileBuffer.put(fileBuffer);
      fileFactory.releaseBuffer(fileBuffer);
      fileBuffer = newFileBuffer;
      //move the limit to allow reading as much as possible from the file
      fileBuffer.limit(fileBuffer.capacity());
      file.read(fileBuffer);
      fileBuffer.position(0);
      return fileBuffer;
   }

   /**
    * It returns a {@link ByteBuffer} that has {@link ByteBuffer#remaining()} bytes >= {@code requiredBytes}
    * of valid data from {@link #file}.
    */
   private ByteBuffer readIntoFileBufferIfNecessary(ByteBuffer fileBuffer, int requiredBytes) throws Exception {
      final int remaining = fileBuffer.remaining();
      //fileBuffer::remaining is the current size of valid data
      final int bytesToBeRead = requiredBytes - remaining;
      if (bytesToBeRead > 0) {
         final int capacity = fileBuffer.capacity();
         //fileBuffer has enough overall capacity to hold all the required bytes?
         if (capacity >= requiredBytes) {
            //we do not care to use the free space between
            //fileBuffer::limit and fileBuffer::capacity
            //to save compactions, because fileBuffer
            //is very unlikely to not be completely full
            //after each file::read
            if (fileBuffer.limit() > 0) {
               //the previous check avoid compact
               //to attempt a copy of 0 bytes
               fileBuffer.compact();
            } else {
               //compact already set the limit == capacity
               fileBuffer.limit(capacity);
            }
            file.read(fileBuffer);
            fileBuffer.position(0);
         } else {
            fileBuffer = allocateAndReadIntoFileBuffer(fileBuffer, requiredBytes);
         }
      }
      return fileBuffer;
   }

   private static ChannelBufferWrapper wrapWhole(ByteBuffer fileBuffer) {
      final int position = fileBuffer.position();
      final int limit = fileBuffer.limit();
      final int capacity = fileBuffer.capacity();
      try {
         fileBuffer.clear();
         final ByteBuf wrappedBuffer = Unpooled.wrappedBuffer(fileBuffer);
         //this check is important to avoid next ByteBuf::setIndex
         //to fail due to ByteBuf::capacity == ByteBuffer::remaining bytes
         assert wrappedBuffer.readableBytes() == capacity;
         final ChannelBufferWrapper fileBufferWrapper = new ChannelBufferWrapper(wrappedBuffer);
         return fileBufferWrapper;
      } finally {
         fileBuffer.position(position);
         fileBuffer.limit(limit);
      }
   }

   //sizeOf(START_BYTE) + sizeOf(MESSAGE LENGTH) + sizeOf(END_BYTE)
   private static final int HEADER_AND_TRAILER_SIZE = DataConstants.SIZE_INT + 2;
   private static final int MINIMUM_MSG_PERSISTENT_SIZE = HEADER_AND_TRAILER_SIZE;
   private static final int MIN_CHUNK_SIZE = Env.osPageSize();

   private List<PagedMessage> readFromSequentialFile(StorageManager storage) throws Exception {
      final List<PagedMessage> messages = new ArrayList<>();
      final int fileSize = (int) file.size();
      file.position(0);
      int processedBytes = 0;
      ByteBuffer fileBuffer = null;
      ChannelBufferWrapper fileBufferWrapper;
      try {
         int remainingBytes = fileSize - processedBytes;
         if (remainingBytes >= MINIMUM_MSG_PERSISTENT_SIZE) {
            fileBuffer = fileFactory.newBuffer(Math.min(remainingBytes, MIN_CHUNK_SIZE));
            //the wrapper is reused to avoid unnecessary allocations
            fileBufferWrapper = wrapWhole(fileBuffer);
            //no content is being added yet
            fileBuffer.limit(0);
            do {
               final ByteBuffer oldFileBuffer = fileBuffer;
               fileBuffer = readIntoFileBufferIfNecessary(fileBuffer, MINIMUM_MSG_PERSISTENT_SIZE);
               //change wrapper if fileBuffer has changed
               if (fileBuffer != oldFileBuffer) {
                  fileBufferWrapper = wrapWhole(fileBuffer);
               }
               final byte startByte = fileBuffer.get();
               if (startByte == Page.START_BYTE) {
                  final int encodedSize = fileBuffer.getInt();
                  final int nextPosition = processedBytes + HEADER_AND_TRAILER_SIZE + encodedSize;
                  if (nextPosition <= fileSize) {
                     final ByteBuffer currentFileBuffer = fileBuffer;
                     fileBuffer = readIntoFileBufferIfNecessary(fileBuffer, encodedSize + 1);
                     //change wrapper if fileBuffer has changed
                     if (fileBuffer != currentFileBuffer) {
                        fileBufferWrapper = wrapWhole(fileBuffer);
                     }
                     final int endPosition = fileBuffer.position() + encodedSize;
                     //this check must be performed upfront decoding
                     if (fileBuffer.remaining() >= (encodedSize + 1) && fileBuffer.get(endPosition) == Page.END_BYTE) {
                        final PagedMessageImpl msg = new PagedMessageImpl(storageManager);
                        fileBufferWrapper.setIndex(fileBuffer.position(), endPosition);
                        msg.decode(fileBufferWrapper);
                        fileBuffer.position(endPosition + 1);
                        assert fileBuffer.get(endPosition) == Page.END_BYTE : "decoding cannot change end byte";
                        msg.initMessage(storage);
                        if (logger.isTraceEnabled()) {
                           logger.tracef("Reading message %s on pageId=%d for address=%s", msg, pageId, storeName);
                        }
                        messages.add(msg);
                        processedBytes = nextPosition;
                     } else {
                        markFileAsSuspect(file.getFileName(), processedBytes, messages.size());
                        return messages;
                     }
                  } else {
                     markFileAsSuspect(file.getFileName(), processedBytes, messages.size());
                     return messages;
                  }
               } else {
                  markFileAsSuspect(file.getFileName(), processedBytes, messages.size());
                  return messages;
               }
               remainingBytes = fileSize - processedBytes;
            }
            while (remainingBytes >= MINIMUM_MSG_PERSISTENT_SIZE);
         }
         //ignore incomplete messages at the end of the file
         if (logger.isTraceEnabled()) {
            logger.tracef("%s has %d bytes of unknown data at position = %d", file.getFileName(), remainingBytes, processedBytes);
         }
         return messages;
      } finally {
         if (fileBuffer != null) {
            fileFactory.releaseBuffer(fileBuffer);
         }
         if (file.position() != fileSize) {
            file.position(fileSize);
         }
      }
   }

   public synchronized void write(final PagedMessage message) throws Exception {
      if (!file.isOpen()) {
         throw ActiveMQMessageBundle.BUNDLE.cannotWriteToClosedFile(file);
      }
      final int messageEncodedSize = message.getEncodeSize();
      final int bufferSize = messageEncodedSize + Page.SIZE_RECORD;
      final ByteBuffer buffer = fileFactory.newBuffer(bufferSize);
      ChannelBufferWrapper activeMQBuffer = new ChannelBufferWrapper(Unpooled.wrappedBuffer(buffer));
      activeMQBuffer.clear();
      activeMQBuffer.writeByte(Page.START_BYTE);
      activeMQBuffer.writeInt(messageEncodedSize);
      message.encode(activeMQBuffer);
      activeMQBuffer.writeByte(Page.END_BYTE);
      assert (activeMQBuffer.readableBytes() == bufferSize) : "messageEncodedSize is different from expected";
      //buffer limit and position are the same
      assert (buffer.remaining() == bufferSize) : "buffer position or limit are changed";
      file.writeDirect(buffer, false);
      if (pageCache != null) {
         pageCache.addLiveMessage(message);
      }
      //lighter than addAndGet when single writer
      numberOfMessages.lazySet(numberOfMessages.get() + 1);
      size.lazySet(size.get() + bufferSize);
      storageManager.pageWrite(message, pageId);
   }

   public void sync() throws Exception {
      file.sync();
   }

   public void open() throws Exception {
      if (!file.isOpen()) {
         file.open();
      }
      size.set((int) file.size());
      file.position(0);
   }

   public void close() throws Exception {
      close(false);
   }

   /**
    * sendEvent means it's a close happening from a major event such moveNext.
    * While reading the cache we don't need (and shouldn't inform the backup
    */
   public synchronized void close(boolean sendEvent) throws Exception {
      if (sendEvent && storageManager != null) {
         storageManager.pageClosed(storeName, pageId);
      }
      if (pageCache != null) {
         pageCache.close();
         // leave it to the soft cache to decide when to release it now
         pageCache = null;
      }
      file.close();

      Set<PageSubscriptionCounter> counters = getPendingCounters();
      if (counters != null) {
         for (PageSubscriptionCounter counter : counters) {
            counter.cleanupNonTXCounters(this.getPageId());
         }
      }
   }

   public boolean isLive() {
      return pageCache != null;
   }

   public boolean delete(final PagedMessage[] messages) throws Exception {
      if (storageManager != null) {
         storageManager.pageDeleted(storeName, pageId);
      }

      if (logger.isDebugEnabled()) {
         logger.debug("Deleting pageNr=" + pageId + " on store " + storeName);
      }

      if (messages != null) {
         for (PagedMessage msg : messages) {
            if (msg.getMessage() instanceof ICoreMessage && (msg.getMessage()).isLargeMessage()) {
               LargeServerMessage lmsg = (LargeServerMessage) msg.getMessage();

               // Remember, cannot call delete directly here
               // Because the large-message may be linked to another message
               // or it may still being delivered even though it has been acked already
               lmsg.decrementDelayDeletionCount();
            }
         }
      }

      try {
         if (suspiciousRecords) {
            ActiveMQServerLogger.LOGGER.pageInvalid(file.getFileName(), file.getFileName());
            file.renameTo(file.getFileName() + ".invalidPage");
         } else {
            file.delete();
         }

         return true;
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.pageDeleteError(e);
         return false;
      }
   }

   public int getNumberOfMessages() {
      return numberOfMessages.intValue();
   }

   public int getSize() {
      return size.intValue();
   }

   @Override
   public String toString() {
      return "Page::pageNr=" + this.pageId + ", file=" + this.file;
   }

   @Override
   public int compareTo(Page otherPage) {
      return otherPage.getPageId() - this.pageId;
   }

   @Override
   protected void finalize() {
      try {
         if (file != null && file.isOpen()) {
            file.close(false);
         }
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.pageFinaliseError(e);
      }
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + pageId;
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      Page other = (Page) obj;
      if (pageId != other.pageId)
         return false;
      return true;
   }

   /**
    * @param position
    * @param msgNumber
    */
   private void markFileAsSuspect(final String fileName, final int position, final int msgNumber) {
      ActiveMQServerLogger.LOGGER.pageSuspectFile(fileName, position, msgNumber);
      suspiciousRecords = true;
   }

   public SequentialFile getFile() {
      return file;
   }

   /**
    * This will indicate a page that will need to be called on cleanup when the page has been closed and confirmed
    *
    * @param pageSubscriptionCounter
    */
   public void addPendingCounter(PageSubscriptionCounter pageSubscriptionCounter) {
      getOrCreatePendingCounters().add(pageSubscriptionCounter);
   }

   private synchronized Set<PageSubscriptionCounter> getPendingCounters() {
      return pendingCounters;
   }

   private synchronized Set<PageSubscriptionCounter> getOrCreatePendingCounters() {
      if (pendingCounters == null) {
         pendingCounters = new ConcurrentHashSet<>();
      }

      return pendingCounters;
   }
}