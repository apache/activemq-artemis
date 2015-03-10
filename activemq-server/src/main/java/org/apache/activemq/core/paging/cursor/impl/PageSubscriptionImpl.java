/**
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
package org.apache.activemq.core.paging.cursor.impl;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.core.filter.Filter;
import org.apache.activemq.core.journal.IOAsyncTask;
import org.apache.activemq.core.paging.PageTransactionInfo;
import org.apache.activemq.core.paging.PagedMessage;
import org.apache.activemq.core.paging.PagingStore;
import org.apache.activemq.core.paging.cursor.PageCache;
import org.apache.activemq.core.paging.cursor.PageCursorProvider;
import org.apache.activemq.core.paging.cursor.PageIterator;
import org.apache.activemq.core.paging.cursor.PagePosition;
import org.apache.activemq.core.paging.cursor.PageSubscription;
import org.apache.activemq.core.paging.cursor.PageSubscriptionCounter;
import org.apache.activemq.core.paging.cursor.PagedReference;
import org.apache.activemq.core.paging.impl.Page;
import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.server.ActiveMQServerLogger;
import org.apache.activemq.core.server.MessageReference;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.core.transaction.Transaction;
import org.apache.activemq.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.core.transaction.impl.TransactionImpl;
import org.apache.activemq.utils.ConcurrentHashSet;
import org.apache.activemq.utils.FutureLatch;

final class PageSubscriptionImpl implements PageSubscription
{
   private final boolean isTrace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   private boolean empty = true;

   // Number of scheduled cleanups, to avoid too many schedules
   private final AtomicInteger scheduledCleanupCount = new AtomicInteger(0);

   private volatile boolean autoCleanup = true;

   private final StorageManager store;

   private final long cursorId;

   private Queue queue;

   private final boolean persistent;

   private final Filter filter;

   private final PagingStore pageStore;

   private final PageCursorProvider cursorProvider;

   private volatile PagePosition lastAckedPosition;

   private List<PagePosition> recoveredACK;

   private final SortedMap<Long, PageCursorInfo> consumedPages = new TreeMap<Long, PageCursorInfo>();

   private final PageSubscriptionCounter counter;

   private final Executor executor;

   private final AtomicLong deliveredCount = new AtomicLong(0);

   PageSubscriptionImpl(final PageCursorProvider cursorProvider,
                        final PagingStore pageStore,
                        final StorageManager store,
                        final Executor executor,
                        final Filter filter,
                        final long cursorId,
                        final boolean persistent)
   {
      this.pageStore = pageStore;
      this.store = store;
      this.cursorProvider = cursorProvider;
      this.cursorId = cursorId;
      this.executor = executor;
      this.filter = filter;
      this.persistent = persistent;
      this.counter = new PageSubscriptionCounterImpl(store, this, executor, persistent, cursorId);
   }

   // Public --------------------------------------------------------

   public PagingStore getPagingStore()
   {
      return pageStore;
   }

   public Queue getQueue()
   {
      return queue;
   }

   public boolean isPaging()
   {
      return pageStore.isPaging();
   }

   public void setQueue(Queue queue)
   {
      this.queue = queue;
   }

   public void disableAutoCleanup()
   {
      autoCleanup = false;
   }

   public void enableAutoCleanup()
   {
      autoCleanup = true;
   }

   public PageCursorProvider getProvider()
   {
      return cursorProvider;
   }

   public void notEmpty()
   {
      synchronized (consumedPages)
      {
         this.empty = false;
      }

   }

   public void bookmark(PagePosition position) throws Exception
   {
      PageCursorInfo cursorInfo = getPageInfo(position);

      if (position.getMessageNr() > 0)
      {
         cursorInfo.confirmed.addAndGet(position.getMessageNr());
      }

      confirmPosition(position);
   }

   public long getMessageCount()
   {
      if (empty)
      {
         return 0;
      }
      else
      {
         return counter.getValue() - deliveredCount.get();
      }
   }

   public PageSubscriptionCounter getCounter()
   {
      return counter;
   }

   /**
    * A page marked as complete will be ignored until it's cleared.
    * <p/>
    * Usually paging is a stream of messages but in certain scenarios (such as a pending prepared
    * TX) we may have big holes on the page streaming, and we will need to ignore such pages on the
    * cursor/subscription.
    */
   public void reloadPageCompletion(PagePosition position)
   {
      PageCursorInfo info = new PageCursorInfo(position.getPageNr(), position.getMessageNr(), null);
      info.setCompleteInfo(position);
      synchronized (consumedPages)
      {
         consumedPages.put(Long.valueOf(position.getPageNr()), info);
      }
   }

   public void scheduleCleanupCheck()
   {
      if (autoCleanup)
      {
         if (scheduledCleanupCount.get() > 2)
         {
            return;
         }

         scheduledCleanupCount.incrementAndGet();
         executor.execute(new Runnable()
         {

            public void run()
            {
               try
               {
                  cleanupEntries(false);
               }
               catch (Exception e)
               {
                  ActiveMQServerLogger.LOGGER.problemCleaningCursorPages(e);
               }
               finally
               {
                  scheduledCleanupCount.decrementAndGet();
               }
            }
         });
      }
   }

   public void onPageModeCleared(Transaction tx) throws Exception
   {
      if (counter != null)
      {
         // this could be null on testcases
         counter.delete(tx);
      }
      this.empty = true;
   }

   /**
    * It will cleanup all the records for completed pages
    */
   public void cleanupEntries(final boolean completeDelete) throws Exception
   {
      if (completeDelete)
      {
         counter.delete();
      }
      Transaction tx = new TransactionImpl(store);

      boolean persist = false;

      final ArrayList<PageCursorInfo> completedPages = new ArrayList<PageCursorInfo>();

      // First get the completed pages using a lock
      synchronized (consumedPages)
      {
         // lastAckedPosition = null means no acks were done yet, so we are not ready to cleanup
         if (lastAckedPosition == null)
         {
            return;
         }

         for (Entry<Long, PageCursorInfo> entry : consumedPages.entrySet())
         {
            PageCursorInfo info = entry.getValue();

            if (info.isDone() && !info.isPendingDelete())
            {
               Page currentPage = pageStore.getCurrentPage();

               if (currentPage != null && entry.getKey() == pageStore.getCurrentPage().getPageId() &&
                  currentPage.isLive())
               {
                  ActiveMQServerLogger.LOGGER.trace("We can't clear page " + entry.getKey() +
                                                      " now since it's the current page");
               }
               else
               {
                  info.setPendingDelete();
                  completedPages.add(entry.getValue());
               }
            }
         }
      }

      for (PageCursorInfo infoPG : completedPages)
      {
         // HORNETQ-1017: There are a few cases where a pending transaction might set a big hole on the page system
         //               where we need to ignore these pages in case of a restart.
         //               for that reason when we delete complete ACKs we store a single record per page file that will
         //               be removed once the page file is deleted
         //               notice also that this record is added as part of the same transaction where the information is deleted.
         //               In case of a TX Failure (a crash on the server) this will be recovered on the next cleanup once the
         //               server is restarted.
         // first will mark the page as complete
         if (isPersistent())
         {
            PagePosition completePage = new PagePositionImpl(infoPG.getPageId(), infoPG.getNumberOfMessages());
            infoPG.setCompleteInfo(completePage);
            store.storePageCompleteTransactional(tx.getID(), this.getId(), completePage);
            if (!persist)
            {
               persist = true;
               tx.setContainsPersistent();
            }
         }

         // it will delete the page ack records
         for (PagePosition pos : infoPG.acks)
         {
            if (pos.getRecordID() >= 0)
            {
               store.deleteCursorAcknowledgeTransactional(tx.getID(), pos.getRecordID());
               if (!persist)
               {
                  // only need to set it once
                  tx.setContainsPersistent();
                  persist = true;
               }
            }
         }

         infoPG.acks.clear();
         infoPG.removedReferences.clear();
      }

      tx.addOperation(new TransactionOperationAbstract()
      {

         @Override
         public void afterCommit(final Transaction tx1)
         {
            executor.execute(new Runnable()
            {

               public void run()
               {
                  if (!completeDelete)
                  {
                     cursorProvider.scheduleCleanup();
                  }
               }
            });
         }
      });

      tx.commit();

   }

   @Override
   public String toString()
   {
      return "PageSubscriptionImpl [cursorId=" + cursorId + ", queue=" + queue + ", filter = " + filter + "]";
   }

   private PagedReference getReference(PagePosition pos)
   {
      return cursorProvider.newReference(pos, cursorProvider.getMessage(pos), this);
   }

   @Override
   public PageIterator iterator()
   {
      return new CursorIterator();
   }

   private PagedReference internalGetNext(final PagePosition pos)
   {
      PagePosition retPos = pos.nextMessage();

      PageCache cache = cursorProvider.getPageCache(pos.getPageNr());

      if (cache != null && !cache.isLive() && retPos.getMessageNr() >= cache.getNumberOfMessages())
      {
         // The next message is beyond what's available at the current page, so we need to move to the next page
         cache = null;
      }

      // it will scan for the next available page
      while ((cache == null && retPos.getPageNr() <= pageStore.getCurrentWritingPage()) ||
         (cache != null && retPos.getPageNr() <= pageStore.getCurrentWritingPage() && cache.getNumberOfMessages() == 0))
      {
         retPos = moveNextPage(retPos);

         cache = cursorProvider.getPageCache(retPos.getPageNr());
      }

      if (cache == null)
      {
         // it will be null in the case of the current writing page
         return null;
      }
      else
      {
         PagedMessage serverMessage = cache.getMessage(retPos.getMessageNr());

         if (serverMessage != null)
         {
            return cursorProvider.newReference(retPos, serverMessage, this);
         }
         else
         {
            return null;
         }
      }
   }

   private PagePosition moveNextPage(final PagePosition pos)
   {
      PagePosition retPos = pos;
      while (true)
      {
         retPos = retPos.nextPage();
         synchronized (consumedPages)
         {
            PageCursorInfo pageInfo = consumedPages.get(retPos.getPageNr());
            // any deleted or complete page will be ignored on the moveNextPage, we will just keep going
            if (pageInfo == null || (!pageInfo.isPendingDelete() && pageInfo.getCompleteInfo() == null))
            {
               return retPos;
            }
         }
      }
   }

   private boolean routed(PagedMessage message)
   {
      long id = getId();

      for (long qid : message.getQueueIDs())
      {
         if (qid == id)
         {
            return true;
         }
      }
      return false;
   }

   /**
    *
    */
   private synchronized PagePosition getStartPosition()
   {
      return new PagePositionImpl(pageStore.getFirstPage(), -1);
   }

   public void confirmPosition(final Transaction tx, final PagePosition position) throws Exception
   {
      // if the cursor is persistent
      if (persistent)
      {
         store.storeCursorAcknowledgeTransactional(tx.getID(), cursorId, position);
      }
      installTXCallback(tx, position);

   }

   public void ackTx(final Transaction tx, final PagedReference reference) throws Exception
   {
      confirmPosition(tx, reference.getPosition());

      counter.increment(tx, -1);

      PageTransactionInfo txInfo = getPageTransaction(reference);
      if (txInfo != null)
      {
         txInfo.storeUpdate(store, pageStore.getPagingManager(), tx);
      }
   }

   @Override
   public void ack(final PagedReference reference) throws Exception
   {
      // Need to do the ACK and counter atomically (inside a TX) or the counter could get out of sync
      Transaction tx = new TransactionImpl(this.store);
      ackTx(tx, reference);
      tx.commit();
   }

   public boolean contains(PagedReference ref) throws Exception
   {
      // We first verify if the message was routed to this queue
      boolean routed = false;

      for (long idRef : ref.getPagedMessage().getQueueIDs())
      {
         if (idRef == this.cursorId)
         {
            routed = true;
            break;
         }
      }
      if (!routed)
      {
         return false;
      }
      else
      {
         // if it's been routed here, we have to verify if it was acked
         return !getPageInfo(ref.getPosition()).isAck(ref.getPosition());
      }
   }

   public void confirmPosition(final PagePosition position) throws Exception
   {
      // if we are dealing with a persistent cursor
      if (persistent)
      {
         store.storeCursorAcknowledge(cursorId, position);
      }

      store.afterCompleteOperations(new IOAsyncTask()
      {
         volatile String error = "";

         @Override
         public void onError(final int errorCode, final String errorMessage)
         {
            error = " errorCode=" + errorCode + ", msg=" + errorMessage;
            ActiveMQServerLogger.LOGGER.pageSubscriptionError(this, error);
         }

         @Override
         public void done()
         {
            processACK(position);
         }

         @Override
         public String toString()
         {
            return IOAsyncTask.class.getSimpleName() + "(" + PageSubscriptionImpl.class.getSimpleName() + ") " + error;
         }
      });
   }

   @Override
   public long getFirstPage()
   {
      synchronized (consumedPages)
      {
         if (empty && consumedPages.isEmpty())
         {
            return -1;
         }
         long lastPageSeen = 0;
         for (Map.Entry<Long, PageCursorInfo> info : consumedPages.entrySet())
         {
            lastPageSeen = info.getKey();
            if (!info.getValue().isDone() && !info.getValue().isPendingDelete())
            {
               return info.getKey();
            }
         }
         return lastPageSeen;
      }

   }

   public void addPendingDelivery(final PagePosition position)
   {
      getPageInfo(position).incrementPendingTX();
   }

   @Override
   public void redeliver(final PageIterator iterator, final PagePosition position)
   {
      iterator.redeliver(position);

      synchronized (consumedPages)
      {
         PageCursorInfo pageInfo = consumedPages.get(position.getPageNr());
         if (pageInfo != null)
         {
            pageInfo.decrementPendingTX();
         }
         else
         {
            // this shouldn't really happen.
         }
      }
   }

   @Override
   public PagedMessage queryMessage(PagePosition pos)
   {
      try
      {
         return cursorProvider.getMessage(pos);
      }
      catch (Exception e)
      {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   /**
    * Theres no need to synchronize this method as it's only called from journal load on startup
    */
   public void reloadACK(final PagePosition position)
   {
      if (recoveredACK == null)
      {
         recoveredACK = new LinkedList<PagePosition>();
      }

      recoveredACK.add(position);
   }

   @Override
   public void reloadPreparedACK(final Transaction tx, final PagePosition position)
   {
      deliveredCount.incrementAndGet();
      installTXCallback(tx, position);
   }

   @Override
   public void positionIgnored(final PagePosition position)
   {
      processACK(position);
   }

   public void lateDeliveryRollback(PagePosition position)
   {
      PageCursorInfo cursorInfo = processACK(position);
      cursorInfo.decrementPendingTX();
   }

   @Override
   public boolean isComplete(long page)
   {
      synchronized (consumedPages)
      {
         if (empty && consumedPages.isEmpty())
         {
            return true;
         }

         PageCursorInfo info = consumedPages.get(page);

         if (info == null && empty)
         {
            return true;
         }
         else
         {
            return info != null && info.isDone();
         }
      }
   }

   /**
    * All the data associated with the cursor should go away here
    */
   public void destroy() throws Exception
   {
      final long tx = store.generateID();
      try
      {

         boolean isPersistent = false;

         synchronized (consumedPages)
         {
            for (PageCursorInfo cursor : consumedPages.values())
            {
               for (PagePosition info : cursor.acks)
               {
                  if (info.getRecordID() >= 0)
                  {
                     isPersistent = true;
                     store.deleteCursorAcknowledgeTransactional(tx, info.getRecordID());
                  }
               }
               PagePosition completeInfo = cursor.getCompleteInfo();
               if (completeInfo != null && completeInfo.getRecordID() >= 0)
               {
                  store.deletePageComplete(completeInfo.getRecordID());
                  cursor.setCompleteInfo(null);
               }
            }
         }

         if (isPersistent)
         {
            store.commit(tx);
         }

         cursorProvider.close(this);
      }
      catch (Exception e)
      {
         try
         {
            store.rollback(tx);
         }
         catch (Exception ignored)
         {
            // exception of the exception.. nothing that can be done here
         }
      }
   }

   @Override
   public long getId()
   {
      return cursorId;
   }

   public boolean isPersistent()
   {
      return persistent;
   }

   public void processReload() throws Exception
   {
      if (recoveredACK != null)
      {
         if (isTrace)
         {
            ActiveMQServerLogger.LOGGER.trace("********** processing reload!!!!!!!");
         }
         Collections.sort(recoveredACK);

         long txDeleteCursorOnReload = -1;

         for (PagePosition pos : recoveredACK)
         {
            lastAckedPosition = pos;
            PageCursorInfo pageInfo = getPageInfo(pos);

            if (pageInfo == null)
            {
               ActiveMQServerLogger.LOGGER.pageNotFound(pos);
               if (txDeleteCursorOnReload == -1)
               {
                  txDeleteCursorOnReload = store.generateID();
               }
               store.deleteCursorAcknowledgeTransactional(txDeleteCursorOnReload, pos.getRecordID());
            }
            else
            {
               pageInfo.loadACK(pos);
            }
         }

         if (txDeleteCursorOnReload >= 0)
         {
            store.commit(txDeleteCursorOnReload);
         }

         recoveredACK.clear();
         recoveredACK = null;
      }
   }

   public void flushExecutors()
   {
      FutureLatch future = new FutureLatch();
      executor.execute(future);
      while (!future.await(1000))
      {
         ActiveMQServerLogger.LOGGER.timedOutFlushingExecutorsPagingCursor(this);
      }
   }

   public void stop()
   {
      flushExecutors();
   }

   public void printDebug()
   {
      printDebug(toString());
   }

   public void printDebug(final String msg)
   {
      System.out.println("Debug information on PageCurorImpl- " + msg);
      for (PageCursorInfo info : consumedPages.values())
      {
         System.out.println(info);
      }
   }


   public void onDeletePage(Page deletedPage) throws Exception
   {
      PageCursorInfo info;
      synchronized (consumedPages)
      {
         info = consumedPages.remove(Long.valueOf(deletedPage.getPageId()));
      }
      if (info != null)
      {
         PagePosition completeInfo = info.getCompleteInfo();
         if (completeInfo != null)
         {
            try
            {
               store.deletePageComplete(completeInfo.getRecordID());
            }
            catch (Exception e)
            {
               ActiveMQServerLogger.LOGGER.warn("Error while deleting page-complete-record", e);
            }
            info.setCompleteInfo(null);
         }
         for (PagePosition deleteInfo : info.acks)
         {
            if (deleteInfo.getRecordID() >= 0)
            {
               try
               {
                  store.deleteCursorAcknowledge(deleteInfo.getRecordID());
               }
               catch (Exception e)
               {
                  ActiveMQServerLogger.LOGGER.warn("Error while deleting page-complete-record", e);
               }
            }
         }
         info.acks.clear();
      }
   }

   public Executor getExecutor()
   {
      return executor;
   }


   public void reloadPageInfo(long pageNr)
   {
      getPageInfo(pageNr, true);
   }

   private PageCursorInfo getPageInfo(final PagePosition pos)
   {
      return getPageInfo(pos.getPageNr(), true);
   }

   private PageCursorInfo getPageInfo(final long pageNr, boolean create)
   {
      synchronized (consumedPages)
      {
         PageCursorInfo pageInfo = consumedPages.get(pageNr);

         if (create && pageInfo == null)
         {
            PageCache cache = cursorProvider.getPageCache(pageNr);
            if (cache == null)
            {
               return null;
            }
            pageInfo = new PageCursorInfo(pageNr, cache.getNumberOfMessages(), cache);
            consumedPages.put(pageNr, pageInfo);
         }
         return pageInfo;
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   private boolean match(final ServerMessage message)
   {
      if (filter == null)
      {
         return true;
      }
      else
      {
         return filter.match(message);
      }
   }

   // Private -------------------------------------------------------

   // To be called only after the ACK has been processed and guaranteed to be on storage
   // The only exception is on non storage events such as not matching messages
   private PageCursorInfo processACK(final PagePosition pos)
   {
      if (lastAckedPosition == null || pos.compareTo(lastAckedPosition) > 0)
      {
         if (isTrace)
         {
            ActiveMQServerLogger.LOGGER.trace("a new position is being processed as ACK");
         }
         if (lastAckedPosition != null && lastAckedPosition.getPageNr() != pos.getPageNr())
         {
            if (isTrace)
            {
               ActiveMQServerLogger.LOGGER.trace("Scheduling cleanup on pageSubscription for address = " + pageStore.getAddress() + " queue = " + this.getQueue().getName());
            }

            // there's a different page being acked, we will do the check right away
            if (autoCleanup)
            {
               scheduleCleanupCheck();
            }
         }
         lastAckedPosition = pos;
      }
      PageCursorInfo info = getPageInfo(pos);

      // This could be null if the page file was removed
      if (info == null)
      {
         // This could become null if the page file was deleted, or if the queue was removed maybe?
         // it's better to diagnose it (based on support tickets) instead of NPE
         ActiveMQServerLogger.LOGGER.warn("PageCursorInfo == null on address " + this.getPagingStore().getAddress() + ", pos = " + pos + ", queue = " + cursorId);
      }
      else
      {
         info.addACK(pos);
      }

      return info;
   }

   /**
    * @param tx
    * @param position
    */
   private void installTXCallback(final Transaction tx, final PagePosition position)
   {
      if (position.getRecordID() >= 0)
      {
         // It needs to persist, otherwise the cursor will return to the fist page position
         tx.setContainsPersistent();
      }

      getPageInfo(position).remove(position);

      PageCursorTX cursorTX = (PageCursorTX) tx.getProperty(TransactionPropertyIndexes.PAGE_CURSOR_POSITIONS);

      if (cursorTX == null)
      {
         cursorTX = new PageCursorTX();
         tx.putProperty(TransactionPropertyIndexes.PAGE_CURSOR_POSITIONS, cursorTX);
         tx.addOperation(cursorTX);
      }

      cursorTX.addPositionConfirmation(this, position);

   }

   private PageTransactionInfo getPageTransaction(final PagedReference reference)
   {
      if (reference.getPagedMessage().getTransactionID() >= 0)
      {
         return pageStore.getPagingManager().getTransaction(reference.getPagedMessage().getTransactionID());
      }
      else
      {
         return null;
      }
   }

   /**
    * A callback from the PageCursorInfo. It will be called when all the messages on a page have been acked
    *
    * @param info
    */
   private void onPageDone(final PageCursorInfo info)
   {
      if (autoCleanup)
      {
         scheduleCleanupCheck();
      }
   }

   // Inner classes -------------------------------------------------

   /**
    * This will hold information about the pending ACKs towards a page.
    * <p/>
    * This instance will be released as soon as the entire page is consumed, releasing the memory at
    * that point The ref counts are increased also when a message is ignored for any reason.
    */
   private final class PageCursorInfo
   {
      // Number of messages existent on this page
      private final int numberOfMessages;

      private final long pageId;

      // Confirmed ACKs on this page
      private final Set<PagePosition> acks = Collections.synchronizedSet(new LinkedHashSet<PagePosition>());

      private WeakReference<PageCache> cache;

      private final Set<PagePosition> removedReferences = new ConcurrentHashSet<PagePosition>();

      // The page was live at the time of the creation
      private final boolean wasLive;

      // There's a pending TX to add elements on this page
      private final AtomicInteger pendingTX = new AtomicInteger(0);

      // There's a pending delete on the async IO pipe
      // We're holding this object to avoid delete the pages before the IO is complete,
      // however we can't delete these records again
      private boolean pendingDelete;

      /**
       * This is to be set when all the messages are complete on a given page, and we cleanup the records that are marked on it
       */
      private PagePosition completePage;

      // We need a separate counter as the cursor may be ignoring certain values because of incomplete transactions or
      // expressions
      private final AtomicInteger confirmed = new AtomicInteger(0);


      public boolean isAck(PagePosition position)
      {
         return completePage != null ||
                acks.contains(position);
      }

      @Override
      public String toString()
      {
         return "PageCursorInfo::PageID=" + pageId +
            " numberOfMessage = " +
            numberOfMessages +
            ", confirmed = " +
            confirmed +
            ", isDone=" +
            this.isDone();
      }

      public PageCursorInfo(final long pageId, final int numberOfMessages, final PageCache cache)
      {
         this.pageId = pageId;
         this.numberOfMessages = numberOfMessages;
         if (cache != null)
         {
            wasLive = cache.isLive();
            this.cache = new WeakReference<PageCache>(cache);
         }
         else
         {
            wasLive = false;
         }
      }

      /**
       * @param completePage
       */
      public void setCompleteInfo(final PagePosition completePage)
      {
         this.completePage = completePage;
      }

      public PagePosition getCompleteInfo()
      {
         return completePage;
      }

      public boolean isDone()
      {
         return completePage != null || (getNumberOfMessages() == confirmed.get() && pendingTX.get() == 0);
      }

      public boolean isPendingDelete()
      {
         return pendingDelete || completePage != null;
      }

      public void setPendingDelete()
      {
         pendingDelete = true;
      }

      /**
       * @return the pageId
       */
      public long getPageId()
      {
         return pageId;
      }

      public void incrementPendingTX()
      {
         pendingTX.incrementAndGet();
      }

      public void decrementPendingTX()
      {
         pendingTX.decrementAndGet();
         checkDone();
      }

      public boolean isRemoved(final PagePosition pos)
      {
         return removedReferences.contains(pos);
      }

      public void remove(final PagePosition position)
      {
         removedReferences.add(position);
      }

      public void addACK(final PagePosition posACK)
      {

         if (isTrace)
         {
            ActiveMQServerLogger.LOGGER.trace("numberOfMessages =  " + getNumberOfMessages() +
                                                " confirmed =  " +
                                                (confirmed.get() + 1) +
                                                " pendingTX = " + pendingTX +
                                                ", page = " +
                                                pageId + " posACK = " + posACK);
         }

         boolean added = internalAddACK(posACK);

         // Negative could mean a bookmark on the first element for the page (example -1)
         if (added && posACK.getMessageNr() >= 0)
         {
            confirmed.incrementAndGet();
            checkDone();
         }
      }

      // To be called during reload
      public void loadACK(final PagePosition posACK)
      {
         if (internalAddACK(posACK) && posACK.getMessageNr() >= 0)
         {
            confirmed.incrementAndGet();
         }
      }

      private boolean internalAddACK(final PagePosition posACK)
      {
         removedReferences.add(posACK);
         return acks.add(posACK);
      }

      /**
       *
       */
      protected void checkDone()
      {
         if (isDone())
         {
            onPageDone(this);
         }
      }

      private int getNumberOfMessages()
      {
         if (wasLive)
         {
            // if the page was live at any point, we need to
            // get the number of messages from the page-cache
            PageCache localcache = this.cache.get();
            if (localcache == null)
            {
               localcache = cursorProvider.getPageCache(pageId);
               this.cache = new WeakReference<PageCache>(localcache);
            }

            return localcache.getNumberOfMessages();
         }
         else
         {
            return numberOfMessages;
         }
      }

   }

   private static final class PageCursorTX extends TransactionOperationAbstract
   {
      private final Map<PageSubscriptionImpl, List<PagePosition>> pendingPositions =
         new HashMap<PageSubscriptionImpl, List<PagePosition>>();

      private void addPositionConfirmation(final PageSubscriptionImpl cursor, final PagePosition position)
      {
         List<PagePosition> list = pendingPositions.get(cursor);

         if (list == null)
         {
            list = new LinkedList<PagePosition>();
            pendingPositions.put(cursor, list);
         }

         list.add(position);
      }

      @Override
      public void afterCommit(final Transaction tx)
      {
         for (Entry<PageSubscriptionImpl, List<PagePosition>> entry : pendingPositions.entrySet())
         {
            PageSubscriptionImpl cursor = entry.getKey();

            List<PagePosition> positions = entry.getValue();

            for (PagePosition confirmed : positions)
            {
               cursor.processACK(confirmed);
               cursor.deliveredCount.decrementAndGet();
            }

         }
      }

      @Override
      public List<MessageReference> getRelatedMessageReferences()
      {
         return Collections.emptyList();
      }

   }

   private class CursorIterator implements PageIterator
   {
      private PagePosition position = null;

      private PagePosition lastOperation = null;

      private volatile boolean isredelivery = false;

      private PagedReference currentDelivery = null;

      private volatile PagedReference lastRedelivery = null;

      // We only store the position for redeliveries. They will be read from the SoftCache again during delivery.
      private final java.util.Queue<PagePosition> redeliveries = new LinkedList<PagePosition>();

      /**
       * next element taken on hasNext test.
       * it has to be delivered on next next operation
       */
      private volatile PagedReference cachedNext;

      public CursorIterator()
      {
      }

      public void redeliver(PagePosition reference)
      {
         synchronized (redeliveries)
         {
            redeliveries.add(reference);
         }
      }

      public void repeat()
      {
         if (isredelivery)
         {
            synchronized (redeliveries)
            {
               cachedNext = lastRedelivery;
            }
         }
         else
         {
            if (lastOperation == null)
            {
               position = null;
            }
            else
            {
               position = lastOperation;
            }
         }
      }

      @Override
      public synchronized PagedReference next()
      {

         if (cachedNext != null)
         {
            currentDelivery = cachedNext;
            cachedNext = null;
            return currentDelivery;
         }

         try
         {
            if (position == null)
            {
               position = getStartPosition();
            }

            currentDelivery = moveNext();
            return currentDelivery;
         }
         catch (RuntimeException e)
         {
            e.printStackTrace();
            throw e;
         }
      }

      private PagedReference moveNext()
      {
         synchronized (PageSubscriptionImpl.this)
         {
            boolean match = false;

            PagedReference message;

            PagePosition lastPosition = position;
            PagePosition tmpPosition = position;

            do
            {
               synchronized (redeliveries)
               {
                  PagePosition redelivery = redeliveries.poll();

                  if (redelivery != null)
                  {
                     // There's a redelivery pending, we will get it out of that pool instead
                     isredelivery = true;
                     PagedReference redeliveredMsg = getReference(redelivery);
                     lastRedelivery = redeliveredMsg;

                     return redeliveredMsg;
                  }
                  else
                  {
                     lastRedelivery = null;
                     isredelivery = false;
                  }

                  message = internalGetNext(tmpPosition);
               }

               if (message == null)
               {
                  break;
               }

               tmpPosition = message.getPosition();

               boolean valid = true;
               boolean ignored = false;

               // Validate the scenarios where the message should be considered not valid even to be considered

               // 1st... is it routed?

               valid = routed(message.getPagedMessage());
               if (!valid)
               {
                  ignored = true;
               }

               PageCursorInfo info = getPageInfo(message.getPosition().getPageNr(), false);

               if (info != null && (info.isRemoved(message.getPosition()) || info.getCompleteInfo() != null))
               {
                  continue;
               }

               // 2nd ... if TX, is it committed?
               if (valid && message.getPagedMessage().getTransactionID() >= 0)
               {
                  PageTransactionInfo tx = pageStore.getPagingManager().getTransaction(message.getPagedMessage()
                                                                                          .getTransactionID());
                  if (tx == null)
                  {
                     ActiveMQServerLogger.LOGGER.pageSubscriptionCouldntLoad(message.getPagedMessage().getTransactionID(),
                                                                            message.getPosition(), pageStore.getAddress(), queue.getName());
                     valid = false;
                     ignored = true;
                  }
                  else
                  {
                     if (tx.deliverAfterCommit(CursorIterator.this, PageSubscriptionImpl.this, message.getPosition()))
                     {
                        valid = false;
                        ignored = false;
                     }
                  }
               }

               // 3rd... was it previously removed?
               if (valid)
               {
                  // We don't create a PageCursorInfo unless we are doing a write operation (ack or removing)
                  // Say you have a Browser that will only read the files... there's no need to control PageCursors is
                  // nothing
                  // is being changed. That's why the false is passed as a parameter here

                  if (info != null && info.isRemoved(message.getPosition()))
                  {
                     valid = false;
                  }
               }

               if (!ignored)
               {
                  position = message.getPosition();
               }

               if (valid)
               {
                  match = match(message.getMessage());

                  if (!match)
                  {
                     processACK(message.getPosition());
                  }
               }
               else if (ignored)
               {
                  positionIgnored(message.getPosition());
               }
            }
            while (message != null && !match);

            if (message != null)
            {
               lastOperation = lastPosition;
            }

            return message;
         }
      }

      /**
       * QueueImpl::deliver could be calling hasNext while QueueImpl.depage could be using next and hasNext as well.
       * It would be a rare race condition but I would prefer avoiding that scenario
       */
      public synchronized boolean hasNext()
      {
         // if an unbehaved program called hasNext twice before next, we only cache it once.
         if (cachedNext != null)
         {
            return true;
         }

         if (!pageStore.isPaging())
         {
            return false;
         }

         cachedNext = next();

         return cachedNext != null;
      }

      @Override
      public void remove()
      {
         deliveredCount.incrementAndGet();
         if (currentDelivery != null)
         {
            PageCursorInfo info = PageSubscriptionImpl.this.getPageInfo(currentDelivery.getPosition());
            if (info != null)
            {
               info.remove(currentDelivery.getPosition());
            }
         }
      }

      @Override
      public void close()
      {
      }
   }
}
