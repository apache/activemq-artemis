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

package org.apache.activemq.artemis.core.paging.cursor.impl;

import java.util.Map;
import java.util.TreeMap;

import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.cursor.PageCache;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.jboss.logging.Logger;

public class PageIndexCacheImpl implements PageCache {
   private static final Logger logger = Logger.getLogger(PageIndexCacheImpl.class);

   private final long pageId;
   private Page page;

   // message number to file position map
   private TreeMap<Integer, Integer> messageIndex;
   private int numberOfMessages;

   public PageIndexCacheImpl(long pageId) {
      this.pageId = pageId;
   }

   public void setPage(Page page) {
      this.page = page;
   }

   public void setNumberOfMessages(final int numberOfMessages) {
      this.numberOfMessages = numberOfMessages;
   }

   public void setMessageIndex(final TreeMap<Integer, Integer> messageIndex) {
      this.messageIndex = messageIndex;
   }

   @Override
   public long getPageId() {
      return pageId;
   }

   @Override
   public int getNumberOfMessages() {
      return numberOfMessages;
   }

   @Override
   public void setMessages(PagedMessage[] messages) {
      throw new UnsupportedOperationException("set messages should not be called for index page cache");
   }

   @Override
   public PagedMessage[] getMessages() {
      throw new UnsupportedOperationException("get messages should not be called for index page cache");
   }

   @Override
   public boolean isLive() {
      return false;
   }

   @Override
   public PagedMessage getMessage(int messageNumber) {
      try {
         Map.Entry<Integer, Integer> floorEntry = messageIndex.floorEntry(messageNumber);
         int startOffset = 0;
         int startMessageNumber = 0;
         if (floorEntry != null) {
            startMessageNumber = floorEntry.getKey();
            startOffset = floorEntry.getValue();
         }
         return page.readMessage(startOffset, startMessageNumber, messageNumber);
      } catch (Exception e) {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   @Override
   public void close() {
      if (page != null) {
         try {
            page.close();
         } catch (Exception e) {
            logger.warn("Closing page " + pageId + " occurs exception:", e);
         } finally {
            messageIndex.clear();
            messageIndex = null;
         }
      }
   }
}
