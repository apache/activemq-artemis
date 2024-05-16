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

import java.util.function.Consumer;

import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.transaction.Transaction;

public interface PageSubscription {

   // Cursor query operations --------------------------------------

   PagingStore getPagingStore();

   // To be called before the server is down
   void stop();

   /** Save a snapshot of the current counter value in the journal */
   void counterSnapshot();

   /**
    * This is a callback to inform the PageSubscription that something was routed, so the empty flag can be cleared
    */
   void notEmpty();

   void bookmark(PagePosition position) throws Exception;

   PageSubscriptionCounter getCounter();

   long getMessageCount();

   boolean isCounterPending();

   long getPersistentSize();

   long getId();

   boolean isPersistent();

   /**
    * Used as a delegate method to {@link PagingStore#isPaging()}
    */
   boolean isPaging();

   PageIterator iterator();

   PageIterator iterator(boolean browsing);


      // To be called when the cursor is closed for good. Most likely when the queue is deleted
   void destroy() throws Exception;

   void scheduleCleanupCheck();

   void cleanupEntries(boolean completeDelete) throws Exception;

   void onPageModeCleared(Transaction tx) throws Exception;

   void disableAutoCleanup();

   void enableAutoCleanup();

   void ack(PagedReference ref) throws Exception;

   boolean contains(PagedReference ref) throws Exception;

   boolean isAcked(PagedMessage pagedMessage);

   // for internal (cursor) classes
   void confirmPosition(PagePosition ref) throws Exception;

   void ackTx(Transaction tx, PagedReference position, boolean fromDelivery) throws Exception;

   default void ackTx(Transaction tx, PagedReference position) throws Exception {
      ackTx(tx, position, true);
   }
   // for internal (cursor) classes
   void confirmPosition(Transaction tx, PagePosition position, boolean fromDelivery) throws Exception;

      /**
       * @return the first page in use or MAX_LONG if none is in use
       */
   long getFirstPage();

   // Reload operations

   /**
    * @param position
    */
   void reloadACK(PagePosition position);

   boolean reloadPageCompletion(PagePosition position) throws Exception;

   void reloadPageInfo(long pageNr);

   /**
    * To be called when the cursor decided to ignore a position.
    *
    * @param position
    */
   void positionIgnored(PagePosition position);

   void lateDeliveryRollback(PagePosition position);

   /**
    * To be used to avoid a redelivery of a prepared ACK after load
    *
    * @param position
    */
   void reloadPreparedACK(Transaction tx, PagePosition position);

   void processReload() throws Exception;

   void addPendingDelivery(PagedMessage pagedMessage);

   void redeliver(PageIterator iterator, PagedReference reference);

   void printDebug();

   /**
    * @param page
    * @return
    */
   boolean isComplete(long page);

   void forEachConsumedPage(Consumer<ConsumedPage> pageCleaner);

   /**
    * To be used to requery the reference
    *
    * @param pos
    * @return
    */
   PagedMessage queryMessage(PagePosition pos);

   void setQueue(Queue queue);

   Queue getQueue();

   /**
    * @param deletedPage
    * @throws Exception
    */
   void onDeletePage(Page deletedPage) throws Exception;

   void removePendingDelivery(PagedMessage pagedMessage);

   ConsumedPage locatePageInfo(long pageNr);
}
