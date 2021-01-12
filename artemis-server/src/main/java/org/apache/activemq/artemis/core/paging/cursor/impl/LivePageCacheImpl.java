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
package org.apache.activemq.artemis.core.paging.cursor.impl;

import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.cursor.LivePageCache;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.utils.collections.ConcurrentAppendOnlyChunkedList;
import org.jboss.logging.Logger;

/**
 * This is the same as PageCache, however this is for the page that's being currently written.
 */
public final class LivePageCacheImpl implements LivePageCache {

   private static final Logger logger = Logger.getLogger(LivePageCacheImpl.class);

   private static final int CHUNK_SIZE = 32;

   private final PagedMessage[] initialMessages;

   private final ConcurrentAppendOnlyChunkedList<PagedMessage> liveMessages;

   private final long pageId;

   private volatile boolean isLive = true;

   public LivePageCacheImpl(final long pageId) {
      this.pageId = pageId;
      this.liveMessages = new ConcurrentAppendOnlyChunkedList<>(CHUNK_SIZE);
      this.initialMessages = null;
   }

   public LivePageCacheImpl(final long pageId, PagedMessage[] initialMessages) {
      this.pageId = pageId;
      this.liveMessages = new ConcurrentAppendOnlyChunkedList<>(CHUNK_SIZE);
      this.initialMessages = initialMessages;
   }

   private int initialMessagesSize() {
      final PagedMessage[] initialMessages = this.initialMessages;
      return initialMessages == null ? 0 : initialMessages.length;
   }

   @Override
   public long getPageId() {
      return pageId;
   }

   @Override
   public int getNumberOfMessages() {
      return initialMessagesSize() + liveMessages.size();
   }

   @Override
   public PagedMessage getMessage(PagePosition pagePosition) {
      final int messageNr = pagePosition.getMessageNr();
      if (messageNr < 0) {
         return null;
      }
      final int initialOffset = initialMessagesSize();
      if (messageNr < initialOffset) {
         return initialMessages[messageNr];
      }
      final int index = messageNr - initialOffset;
      return liveMessages.get(index);
   }

   @Override
   public boolean isLive() {
      return isLive;
   }

   @Override
   public void addLiveMessage(PagedMessage message) {
      message.getMessage().usageUp();
      liveMessages.add(message);
   }

   @Override
   public void close() {
      logger.tracef("Closing %s", this);
      this.isLive = false;
   }

   @Override
   public PagedMessage[] getMessages() {
      final PagedMessage[] pagedMessages = liveMessages.toArray(size -> new PagedMessage[initialMessagesSize() + size], initialMessagesSize());
      final PagedMessage[] initialMessages = this.initialMessages;
      if (initialMessages != null) {
         System.arraycopy(initialMessages, 0, pagedMessages, 0, initialMessages.length);
      }
      return pagedMessages;
   }

   @Override
   public String toString() {
      return "LivePacheCacheImpl::page=" + pageId + " number of messages=" + getNumberOfMessages() + " isLive = " + isLive;
   }
}
