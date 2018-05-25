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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.NonExistentPage;
import org.apache.activemq.artemis.core.paging.cursor.PageCache;
import org.apache.activemq.artemis.core.paging.cursor.PageCursorProvider;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.PagedReference;
import org.apache.activemq.artemis.core.paging.cursor.PagedReferenceImpl;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.utils.SoftValueHashMap;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.jboss.logging.Logger;

/**
 * A PageProviderIMpl
 *
 * TODO: this may be moved entirely into PagingStore as there's an one-to-one relationship here
 * However I want to keep this isolated as much as possible during development
 */
public class PageCursorProviderImpl implements PageCursorProvider {
   // Constants -----------------------------------------------------

   private static final Logger logger = Logger.getLogger(PageCursorProviderImpl.class);

   // Attributes ----------------------------------------------------

   /**
    * As an optimization, avoid subsequent schedules as they are unnecessary
    */
   protected final AtomicInteger scheduledCleanup = new AtomicInteger(0);

   protected volatile boolean cleanupEnabled = true;

   protected final PagingStore pagingStore;

   protected final StorageManager storageManager;

   // This is the same executor used at the PageStoreImpl. One Executor per pageStore
   private final ArtemisExecutor executor;

   private final SoftValueHashMap<Long, PageCache> softCache;

   private final ConcurrentMap<Long, PageSubscription> activeCursors = new ConcurrentHashMap<>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageCursorProviderImpl(final PagingStore pagingStore,
                                 final StorageManager storageManager,
                                 final ArtemisExecutor executor,
                                 final int maxCacheSize) {
      this.pagingStore = pagingStore;
      this.storageManager = storageManager;
      this.executor = executor;
      this.softCache = new SoftValueHashMap<>(maxCacheSize);
   }

   // Public --------------------------------------------------------

   @Override
   public synchronized PageSubscription createSubscription(long cursorID, Filter filter, boolean persistent) {
      if (logger.isTraceEnabled()) {
         logger.trace(this.pagingStore.getAddress() + " creating subscription " + cursorID + " with filter " + filter, new Exception("trace"));
      }

      if (activeCursors.containsKey(cursorID)) {
         throw new IllegalStateException("Cursor " + cursorID + " had already been created");
      }

      PageSubscription activeCursor = new PageSubscriptionImpl(this, pagingStore, storageManager, executor, filter, cursorID, persistent);
      activeCursors.put(cursorID, activeCursor);
      return activeCursor;
   }

   @Override
   public synchronized PageSubscription getSubscription(long cursorID) {
      return activeCursors.get(cursorID);
   }

   @Override
   public PagedMessage getMessage(final PagePosition pos) {
      PageCache cache = getPageCache(pos.getPageNr());

      if (cache == null || pos.getMessageNr() >= cache.getNumberOfMessages()) {
         // sanity check, this should never happen unless there's a bug
         throw new NonExistentPage("Invalid messageNumber passed = " + pos + " on " + cache);
      }

      return cache.getMessage(pos.getMessageNr());
   }

   @Override
   public PagedReference newReference(final PagePosition pos,
                                      final PagedMessage msg,
                                      final PageSubscription subscription) {
      return new PagedReferenceImpl(pos, msg, subscription);
   }

   @Override
   public PageCache getPageCache(final long pageId) {
      try {
         PageCache cache;
         synchronized (softCache) {
            if (pageId > pagingStore.getCurrentWritingPage()) {
               return null;
            }

            cache = softCache.get(pageId);
            if (cache == null) {
               if (!pagingStore.checkPageFileExists((int) pageId)) {
                  return null;
               }

               cache = createPageCache(pageId);
               // anyone reading from this cache will have to wait reading to finish first
               // we also want only one thread reading this cache
               logger.tracef("adding pageCache pageNr=%d into cursor = %s", pageId, this.pagingStore.getAddress());
               readPage((int) pageId, cache);
               softCache.put(pageId, cache);
            }
         }

         return cache;
      } catch (Exception e) {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   private void readPage(int pageId, PageCache cache) throws Exception {
      Page page = null;
      try {
         page = pagingStore.createPage(pageId);

         storageManager.beforePageRead();
         page.open();

         List<PagedMessage> pgdMessages = page.read(storageManager);
         cache.setMessages(pgdMessages.toArray(new PagedMessage[pgdMessages.size()]));
      } finally {
         try {
            if (page != null) {
               page.close(false);
            }
         } catch (Throwable ignored) {
         }
         storageManager.afterPageRead();
      }
   }

   @Override
   public void addPageCache(PageCache cache) {
      logger.tracef("Add page cache %s", cache);
      synchronized (softCache) {
         softCache.put(cache.getPageId(), cache);
      }
   }

   @Override
   public void setCacheMaxSize(final int size) {
      softCache.setMaxElements(size);
   }

   @Override
   public int getCacheSize() {
      synchronized (softCache) {
         return softCache.size();
      }
   }

   @Override
   public void clearCache() {
      synchronized (softCache) {
         softCache.clear();
      }
   }

   @Override
   public void processReload() throws Exception {
      Collection<PageSubscription> cursorList = this.activeCursors.values();
      for (PageSubscription cursor : cursorList) {
         cursor.processReload();
      }

      if (!cursorList.isEmpty()) {
         // https://issues.jboss.org/browse/JBPAPP-10338 if you ack out of order,
         // the min page could be beyond the first page.
         // we have to reload any previously acked message
         long cursorsMinPage = checkMinPage(cursorList);

         // checkMinPage will return MaxValue if there aren't any pages or any cursors
         if (cursorsMinPage != Long.MAX_VALUE) {
            for (long startPage = pagingStore.getFirstPage(); startPage < cursorsMinPage; startPage++) {
               for (PageSubscription cursor : cursorList) {
                  cursor.reloadPageInfo(startPage);
               }
            }
         }
      }

      cleanup();

   }

   @Override
   public void stop() {
      for (PageSubscription cursor : activeCursors.values()) {
         cursor.stop();
      }
      final int pendingCleanupTasks = scheduledCleanup.get();
      if (pendingCleanupTasks > 0) {
         logger.tracef("Stopping with %d cleanup tasks to be completed yet", pendingCleanupTasks);
      }
   }

   private void waitForFuture() {
      if (!executor.flush(10, TimeUnit.SECONDS)) {
         ActiveMQServerLogger.LOGGER.timedOutStoppingPagingCursor(executor);

      }
   }

   @Override
   public void flushExecutors() {
      for (PageSubscription cursor : activeCursors.values()) {
         cursor.flushExecutors();
      }
      waitForFuture();
   }

   @Override
   public void close(PageSubscription cursor) {
      activeCursors.remove(cursor.getId());

      scheduleCleanup();
   }

   @Override
   public void scheduleCleanup() {

      if (logger.isTraceEnabled()) {
         logger.trace("scheduling cleanup", new Exception("trace"));
      }
      if (!cleanupEnabled || scheduledCleanup.intValue() > 2) {
         // Scheduled cleanup was already scheduled before.. never mind!
         // or we have cleanup disabled
         return;
      }

      scheduledCleanup.incrementAndGet();

      executor.execute(new Runnable() {
         @Override
         public void run() {
            storageManager.setContext(storageManager.newSingleThreadContext());
            try {
               if (cleanupEnabled) {
                  cleanup();
               }
            } finally {
               storageManager.clearContext();
               scheduledCleanup.decrementAndGet();
            }
         }
      });
   }

   /**
    * Delete everything associated with any queue on this address.
    * This is to be called when the address is about to be released from paging.
    * Hence the PagingStore will be holding a write lock, meaning no messages are going to be paged at this time.
    * So, we shouldn't lock anything after this method, to avoid dead locks between the writeLock and any synchronization with the CursorProvider.
    */
   @Override
   public void onPageModeCleared() {
      ArrayList<PageSubscription> subscriptions = cloneSubscriptions();

      Transaction tx = new TransactionImpl(storageManager);
      for (PageSubscription sub : subscriptions) {
         try {
            sub.onPageModeCleared(tx);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorCleaningPagingOnQueue(e, sub.getQueue().getName().toString());
         }
      }

      try {
         tx.commit();
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorCleaningPagingDuringCommit(e);
      }
   }

   @Override
   public void disableCleanup() {
      this.cleanupEnabled = false;
   }

   @Override
   public void resumeCleanup() {
      this.cleanupEnabled = true;
      scheduleCleanup();
   }

   @Override
   public void cleanup() {

      logger.tracef("performing page cleanup %s", this);

      ArrayList<Page> depagedPages = new ArrayList<>();

      while (true) {
         if (pagingStore.lock(100)) {
            break;
         }
         if (!pagingStore.isStarted())
            return;
      }

      logger.tracef("%s locked", this);

      synchronized (this) {
         try {
            if (!pagingStore.isStarted()) {
               return;
            }

            if (pagingStore.getNumberOfPages() == 0) {
               return;
            }

            ArrayList<PageSubscription> cursorList = cloneSubscriptions();

            long minPage = checkMinPage(cursorList);

            logger.debugf("Asserting cleanup for address %s, firstPage=%d", pagingStore.getAddress(), minPage);

            // if the current page is being written...
            // on that case we need to move to verify it in a different way
            if (minPage == pagingStore.getCurrentWritingPage() && pagingStore.getCurrentPage().getNumberOfMessages() > 0) {
               boolean complete = checkPageCompletion(cursorList, minPage);

               if (!pagingStore.isStarted()) {
                  return;
               }

               // All the pages on the cursor are complete.. so we will cleanup everything and store a bookmark
               if (complete) {

                  cleanupComplete(cursorList);
               }
            }

            for (long i = pagingStore.getFirstPage(); i < minPage; i++) {
               if (!checkPageCompletion(cursorList, i)) {
                  break;
               }
               Page page = pagingStore.depage();
               if (page == null) {
                  break;
               }
               depagedPages.add(page);
            }

            if (pagingStore.getNumberOfPages() == 0 || pagingStore.getNumberOfPages() == 1 && pagingStore.getCurrentPage().getNumberOfMessages() == 0) {
               pagingStore.stopPaging();
            } else {
               if (logger.isTraceEnabled()) {
                  logger.trace("Couldn't cleanup page on address " + this.pagingStore.getAddress() +
                                  " as numberOfPages == " +
                                  pagingStore.getNumberOfPages() +
                                  " and currentPage.numberOfMessages = " +
                                  pagingStore.getCurrentPage().getNumberOfMessages());
               }
            }
         } catch (Exception ex) {
            ActiveMQServerLogger.LOGGER.problemCleaningPageAddress(ex, pagingStore.getAddress());
            return;
         } finally {
            pagingStore.unlock();
         }
      }
      finishCleanup(depagedPages);

   }

   // Protected as a way to inject testing
   protected void cleanupComplete(ArrayList<PageSubscription> cursorList) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug("Address " + pagingStore.getAddress() +
                         " is leaving page mode as all messages are consumed and acknowledged from the page store");
      }

      pagingStore.forceAnotherPage();

      Page currentPage = pagingStore.getCurrentPage();

      storeBookmark(cursorList, currentPage);

      pagingStore.stopPaging();
   }

   // Protected as a way to inject testing
   protected void finishCleanup(ArrayList<Page> depagedPages) {
      logger.tracef("this(%s) finishing cleanup on %s", this, depagedPages);
      try {
         for (Page depagedPage : depagedPages) {
            PageCache cache;
            PagedMessage[] pgdMessages;
            synchronized (softCache) {
               cache = softCache.get((long) depagedPage.getPageId());
            }

            if (logger.isTraceEnabled()) {
               logger.trace("Removing pageNr=" + depagedPage.getPageId() + " from page-cache");
            }

            if (cache == null) {
               // The page is not on cache any more
               // We need to read the page-file before deleting it
               // to make sure we remove any large-messages pending
               storageManager.beforePageRead();

               List<PagedMessage> pgdMessagesList = null;
               try {
                  depagedPage.open();
                  pgdMessagesList = depagedPage.read(storageManager);
               } finally {
                  try {
                     depagedPage.close(false);
                  } catch (Exception e) {
                  }

                  storageManager.afterPageRead();
               }
               depagedPage.close(false);
               pgdMessages = pgdMessagesList.toArray(new PagedMessage[pgdMessagesList.size()]);
            } else {
               pgdMessages = cache.getMessages();
            }

            depagedPage.delete(pgdMessages);
            onDeletePage(depagedPage);

            synchronized (softCache) {
               softCache.remove((long) depagedPage.getPageId());
            }
         }
      } catch (Exception ex) {
         ActiveMQServerLogger.LOGGER.problemCleaningPageAddress(ex, pagingStore.getAddress());
         return;
      }

   }

   private boolean checkPageCompletion(ArrayList<PageSubscription> cursorList, long minPage) {

      logger.tracef("checkPageCompletion(%d)", minPage);

      boolean complete = true;

      for (PageSubscription cursor : cursorList) {
         if (!cursor.isComplete(minPage)) {
            if (logger.isDebugEnabled()) {
               logger.debug("Cursor " + cursor + " was considered incomplete at pageNr=" + minPage);
            }

            complete = false;
            break;
         } else {
            if (logger.isDebugEnabled()) {
               logger.debug("Cursor " + cursor + " was considered **complete** at pageNr=" + minPage);
            }
         }
      }
      return complete;
   }

   /**
    * @return
    */
   private synchronized ArrayList<PageSubscription> cloneSubscriptions() {
      ArrayList<PageSubscription> cursorList = new ArrayList<>(activeCursors.values());
      return cursorList;
   }

   protected void onDeletePage(Page deletedPage) throws Exception {
      List<PageSubscription> subscriptions = cloneSubscriptions();
      for (PageSubscription subs : subscriptions) {
         subs.onDeletePage(deletedPage);
      }
   }

   /**
    * @param cursorList
    * @param currentPage
    * @throws Exception
    */
   protected void storeBookmark(ArrayList<PageSubscription> cursorList, Page currentPage) throws Exception {
      try {
         // First step: Move every cursor to the next bookmarked page (that was just created)
         for (PageSubscription cursor : cursorList) {
            cursor.confirmPosition(new PagePositionImpl(currentPage.getPageId(), -1));
         }

         // we just need to make sure the storage is done..
         // if the thread pool is full, we will just log it once instead of looping
         if (!storageManager.waitOnOperations(5000)) {
            ActiveMQServerLogger.LOGGER.problemCompletingOperations(storageManager.getContext());
         }
      } finally {
         for (PageSubscription cursor : cursorList) {
            cursor.enableAutoCleanup();
         }
      }
   }

   @Override
   public void printDebug() {
      System.out.println("Debug information for PageCursorProviderImpl:");
      for (PageCache cache : softCache.values()) {
         System.out.println("Cache " + cache);
      }
   }

   @Override
   public String toString() {
      return "PageCursorProviderImpl{" +
         "pagingStore=" + pagingStore +
         '}';
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /* Protected as we may let test cases to instrument the test */
   protected PageCacheImpl createPageCache(final long pageId) throws Exception {
      return new PageCacheImpl(pagingStore.createPage((int) pageId));
   }

   // Private -------------------------------------------------------

   /**
    * This method is synchronized because we want it to be atomic with the cursors being used
    */
   private long checkMinPage(Collection<PageSubscription> cursorList) {
      long minPage = Long.MAX_VALUE;

      for (PageSubscription cursor : cursorList) {
         long firstPage = cursor.getFirstPage();
         if (logger.isDebugEnabled()) {
            logger.debug(this.pagingStore.getAddress() + " has a cursor " + cursor + " with first page=" + firstPage);
         }

         // the cursor will return -1 if the cursor is empty
         if (firstPage >= 0 && firstPage < minPage) {
            minPage = firstPage;
         }
      }

      if (logger.isDebugEnabled()) {
         logger.debug(this.pagingStore.getAddress() + " has minPage=" + minPage);
      }

      return minPage;

   }

   // Inner classes -------------------------------------------------

}
