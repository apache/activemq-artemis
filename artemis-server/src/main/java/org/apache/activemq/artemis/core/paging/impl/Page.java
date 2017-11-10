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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.cursor.LivePageCache;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscriptionCounter;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.utils.DataConstants;
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

   private boolean canBeMapped;

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
      this.canBeMapped = fileFactory instanceof NIOSequentialFileFactory || fileFactory instanceof MappedSequentialFileFactory;
      //pooled buffers to avoid allocations on hot paths
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

      final List<PagedMessage> messages = new ArrayList<>();

      size.lazySet((int) file.size());

      if (this.canBeMapped) {
         readFromMapped(storage, messages);
         // if the file is open to be written
         // it needs to updated the position
         file.position(file.size());
      } else {
         readFromSequentialFile(storage, messages);
      }

      numberOfMessages.lazySet(messages.size());

      return messages;
   }

   private void readFromSequentialFile(StorageManager storage, List<PagedMessage> messages) throws Exception {
      final int fileSize = (int) file.size();
      //doesn't need to be a direct buffer: that case is covered using the MMAP read
      final ByteBuffer buffer = this.fileFactory.newBuffer(fileSize);
      try {
         file.position(0);
         file.read(buffer);
         buffer.rewind();
         assert (buffer.limit() == fileSize) : "buffer doesn't contains the whole file";
         ChannelBufferWrapper activeMQBuffer = wrapBuffer(fileSize, buffer);
         read(storage, activeMQBuffer, messages);
      } finally {
         this.fileFactory.releaseBuffer(buffer);
      }
   }

   private ChannelBufferWrapper wrapBuffer(int fileSize, ByteBuffer buffer) {
      ChannelBufferWrapper activeMQBuffer = new ChannelBufferWrapper(Unpooled.wrappedBuffer(buffer));
      return activeMQBuffer;
   }

   private static MappedByteBuffer mapFileForRead(File file, int fileSize) {
      try (RandomAccessFile raf = new RandomAccessFile(file, "rw"); FileChannel channel = raf.getChannel()) {
         return channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
      } catch (Exception e) {
         throw new IllegalStateException(e);
      }
   }

   private int readFromMapped(StorageManager storage, List<PagedMessage> messages) throws IOException {
      file.position(0);
      //use a readonly mapped view of the file
      final int mappedSize = size.get();
      final MappedByteBuffer mappedByteBuffer = mapFileForRead(this.file.getJavaFile(), mappedSize);
      ChannelBufferWrapper activeMQBuffer = wrapBuffer(mappedSize, mappedByteBuffer);
      try {
         return read(storage, activeMQBuffer, messages);
      } finally {
         //unmap the file after read it to avoid GC to take care of it
         PlatformDependent.freeDirectBuffer(mappedByteBuffer);
      }
   }

   private int read(StorageManager storage, ActiveMQBuffer fileBuffer, List<PagedMessage> messages) {
      int readMessages = 0;
      while (fileBuffer.readable()) {
         final int position = fileBuffer.readerIndex();

         byte byteRead = fileBuffer.readByte();

         if (byteRead == Page.START_BYTE) {
            if (fileBuffer.readerIndex() + DataConstants.SIZE_INT < fileBuffer.capacity()) {
               int messageSize = fileBuffer.readInt();
               int oldPos = fileBuffer.readerIndex();
               if (fileBuffer.readerIndex() + messageSize < fileBuffer.capacity() && fileBuffer.getByte(oldPos + messageSize) == Page.END_BYTE) {
                  PagedMessage msg = new PagedMessageImpl(storageManager);
                  msg.decode(fileBuffer);
                  byte b = fileBuffer.readByte();
                  if (b != Page.END_BYTE) {
                     // Sanity Check: This would only happen if there is a bug on decode or any internal code, as
                     // this
                     // constraint was already checked
                     throw new IllegalStateException("Internal error, it wasn't possible to locate END_BYTE " + b);
                  }
                  msg.initMessage(storage);
                  if (logger.isTraceEnabled()) {
                     logger.trace("Reading message " + msg + " on pageId=" + this.pageId + " for address=" + storeName);
                  }
                  readMessages++;
                  messages.add(msg);
               } else {
                  markFileAsSuspect(file.getFileName(), position, messages.size());
                  break;
               }
            }
         } else {
            markFileAsSuspect(file.getFileName(), position, messages.size());
            break;
         }
      }
      return readMessages;
   }

   public synchronized void write(final PagedMessage message) throws Exception {
      if (!file.isOpen()) {
         return;
      }
      final int messageEncodedSize = message.getEncodeSize();
      final int bufferSize = messageEncodedSize + Page.SIZE_RECORD;
      final ByteBuffer buffer = fileFactory.newBuffer(bufferSize);
      ChannelBufferWrapper activeMQBuffer = wrapBuffer(bufferSize, buffer);
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
      Set<PageSubscriptionCounter> counter = getOrCreatePendingCounters();
      pendingCounters.add(pageSubscriptionCounter);
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
