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
package org.apache.activemq.artemis.core.paging.cursor;

import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.PagedMessage;

/**
 * The provider of Cursor for a given Address
 */
public interface PageCursorProvider {

   /**
    * Used on tests, to simulate a scenario where the VM cleared space
    */
   void clearCache();

   PageCache getPageCache(long pageNr);

   PagedReference newReference(PagePosition pos, PagedMessage msg, PageSubscription sub);

   void addPageCache(PageCache cache);

   /**
    * @param queueId The cursorID should be the same as the queueId associated for persistence
    * @return
    */
   PageSubscription getSubscription(long queueId);

   PageSubscription createSubscription(long queueId, Filter filter, boolean durable);

   PagedMessage getMessage(PagePosition pos);

   void processReload() throws Exception;

   void stop();

   void flushExecutors();

   void scheduleCleanup();

   void disableCleanup();

   void resumeCleanup();

   /**
    * Cleanup stuff as paging mode is being cleared
    */
   void onPageModeCleared();

   /**
    * Perform the cleanup at the caller's thread (for startup and recovery)
    */
   void cleanup();

   void setCacheMaxSize(int size);

   /**
    * @param pageCursorImpl
    */
   void close(PageSubscription pageCursorImpl);

   // to be used on tests -------------------------------------------

   int getCacheSize();

   void printDebug();
}
