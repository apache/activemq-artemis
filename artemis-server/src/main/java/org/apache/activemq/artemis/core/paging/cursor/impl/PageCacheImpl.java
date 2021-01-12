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
import org.apache.activemq.artemis.core.paging.cursor.BulkPageCache;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;

/**
 * The caching associated to a single page.
 */
class PageCacheImpl implements BulkPageCache {

   private final PagedMessage[] messages;

   private final long pageId;

   PageCacheImpl(final long pageId, PagedMessage[] messages) {
      this.pageId = pageId;
      this.messages = messages;
   }

   @Override
   public PagedMessage getMessage(PagePosition pagePosition) {
      if (pagePosition.getMessageNr() < messages.length) {
         return messages[pagePosition.getMessageNr()];
      } else {
         return null;
      }
   }

   @Override
   public long getPageId() {
      return pageId;
   }

   @Override
   public int getNumberOfMessages() {
      return messages.length;
   }

   @Override
   public void close() {
   }

   @Override
   public boolean isLive() {
      return false;
   }

   @Override
   public String toString() {
      return "PageCacheImpl::page=" + pageId + " numberOfMessages = " + messages.length;
   }

   @Override
   public PagedMessage[] getMessages() {
      return messages;
   }
}
