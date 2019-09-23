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

import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;

public interface PageSubscription {

   // Cursor query operations --------------------------------------

   PagingStore getPagingStore();

   // To be called before the server is down
   void stop();

   /**
    * This is a callback to inform the PageSubscription that something was routed, so the empty flag can be cleared
    */
   void notEmpty();

   void bookmark(PagePosition position) throws Exception;

   PageSubscriptionCounter getCounter();

   long getMessageCount();

   long getPersistentSize();

   long getId();

   boolean isPersistent();

   /**
    * Used as a delegate method to {@link PagingStore#isPaging()}
    */
   boolean isPaging();

   PageIterator iterator();

   LinkedListIterator<PagedReference> iterator(boolean jumpRemoves);


      // To be called when the cursor is closed for good. Most likely when the queue is deleted
   void destroy() throws Exception;

   void scheduleCleanupCheck();

   void cleanupEntries(boolean completeDelete) throws Exception;

   void onPageModeCleared(Transaction tx) throws Exception;

   void disableAutoCleanup();

   void enableAutoCleanup();

   void ack(PagedReference ref) throws Exception;

   boolean contains(PagedReference ref) throws Exception;

   // for internal (cursor) classes
   void confirmPosition(PagePosition ref) throws Exception;

   void ackTx(Transaction tx, PagedReference position) throws Exception;

   // for internal (cursor) classes
   void confirmPosition(Transaction tx, PagePosition position) throws Exception;

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

   void addPendingDelivery(PagePosition position);

   /**
    * To be used on redeliveries
    *
    * @param position
    */
   void redeliver(PageIterator iterator, PagePosition position);

   void printDebug();

   /**
    * @param page
    * @return
    */
   boolean isComplete(long page);

   /**
    * wait all the scheduled runnables to finish their current execution
    */
   void flushExecutors();

   void setQueue(Queue queue);

   Queue getQueue();

   /**
    * To be used to requery the reference case the Garbage Collection removed it from the PagedReference as it's using WeakReferences
    *
    * @param pos
    * @return
    */
   PagedMessage queryMessage(PagePosition pos);

   /**
    * @return executor used by the PageSubscription
    */
   ArtemisExecutor getExecutor();

   /**
    * @param deletedPage
    * @throws Exception
    */
   void onDeletePage(Page deletedPage) throws Exception;

   long getDeliveredCount();

   long getDeliveredSize();

   void incrementDeliveredSize(long size);

   void removePendingDelivery(PagePosition position);
}
