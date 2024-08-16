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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.paging.cursor.PageCursorProvider;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.impl.MessageReferenceImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.PageFullMessagePolicy;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperation;
import org.apache.activemq.artemis.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.artemis.utils.ArtemisCloseable;
import org.apache.activemq.artemis.utils.FutureLatch;
import org.apache.activemq.artemis.utils.SizeAwareMetric;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.activemq.artemis.utils.runnables.AtomicRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;
import java.util.function.Function;

/**
 * @see PagingStore
 */
public class PagingStoreImpl implements PagingStore {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final SimpleString address;

   private final StorageManager storageManager;

   private final DecimalFormat format = new DecimalFormat("000000000");

   private final PageCache usedPages = new PageCache(this);

   //it's being guarded by lock.writeLock().lock() and never read concurrently
   private long currentPageSize = 0;

   private final SimpleString storeName;

   // The FileFactory is created lazily as soon as the first write is attempted
   private volatile SequentialFileFactory fileFactory;

   private final PagingStoreFactory storeFactory;

   // Used to schedule sync threads
   private final PageSyncTimer syncTimer;

   private long maxSize;

   private int maxPageReadBytes = -1;

   private int maxPageReadMessages = -1;

   private int prefetchPageBytes = -1;

   private int prefetchPageMessages = -1;

   private long maxMessages;

   private volatile boolean pageFull;

   private Long pageLimitBytes;

   private Long estimatedMaxPages;

   private Long pageLimitMessages;

   private PageFullMessagePolicy pageFullMessagePolicy;

   private int pageSize;

   private volatile AddressFullMessagePolicy addressFullMessagePolicy;

   // Internal components such as mirroring could enforce a different page full message policy
   // differing from the AddressSettings
   // Example: User configured sync mirroring while default address-settings is PAGE. We must use Block on that case
   //          User configured non sync mirroring while configured drop. We must use page. (always paged)
   private volatile AddressFullMessagePolicy enforcedAddressFullMessagePolicy;

   private boolean printedDropMessagesWarning;

   private final PagingManager pagingManager;

   private final boolean usingGlobalMaxSize;

   private final ArtemisExecutor executor;

   // Bytes consumed by the queue on the memory
   private final SizeAwareMetric size;

   private volatile boolean full;

   private long numberOfPages;

   private long firstPageId;

   private volatile long currentPageId;

   private volatile Page currentPage;

   private volatile boolean paging = false;

   private final PageCursorProvider cursorProvider;

   private final ReadWriteLock lock = new ReentrantReadWriteLock();

   private volatile boolean running = false;

   private final boolean syncNonTransactional;

   private volatile boolean blocking = false;

   private volatile boolean blockedViaAddressControl = false;

   private long rejectThreshold;

   public PagingStoreImpl(final SimpleString address,
                          final ScheduledExecutorService scheduledExecutor,
                          final long syncTimeout,
                          final PagingManager pagingManager,
                          final StorageManager storageManager,
                          final SequentialFileFactory fileFactory,
                          final PagingStoreFactory storeFactory,
                          final SimpleString storeName,
                          final AddressSettings addressSettings,
                          final ArtemisExecutor executor,
                          final ArtemisExecutor ioExecutor,
                          final boolean syncNonTransactional) {
      if (pagingManager == null) {
         throw new IllegalStateException("Paging Manager can't be null");
      }

      this.address = address;

      this.storageManager = storageManager;

      this.storeName = storeName;

      this.size = new SizeAwareMetric(maxSize, maxSize, maxMessages, maxMessages).
         setUnderCallback(this::underSized).setOverCallback(this::overSized).
         setOnSizeCallback(pagingManager::addSize);

      applySetting(addressSettings, true);

      this.executor = executor;

      this.pagingManager = pagingManager;

      this.fileFactory = fileFactory;

      this.storeFactory = storeFactory;

      this.syncNonTransactional = syncNonTransactional;

      if (scheduledExecutor != null && syncTimeout > 0) {
         this.syncTimer = new PageSyncTimer(this, scheduledExecutor, ioExecutor, syncTimeout);
      } else {
         this.syncTimer = null;
      }

      this.cursorProvider = storeFactory.newCursorProvider(this, this.storageManager, addressSettings, executor);

      this.usingGlobalMaxSize = pagingManager.isUsingGlobalSize();
   }

   private void overSized() {
      full = true;
   }

   private void underSized() {
      full = false;
      checkReleasedMemory();
   }

   private void configureSizeMetric() {
      size.setMax(maxSize, maxSize, maxMessages, maxMessages);
   }

   /**
    * @param addressSettings
    */
   @Override
   public void applySetting(final AddressSettings addressSettings) {
      applySetting(addressSettings, false);
   }

   private void applySetting(final AddressSettings addressSettings, final boolean firstTime) {
      maxSize = addressSettings.getMaxSizeBytes();

      maxPageReadMessages = addressSettings.getMaxReadPageMessages();

      prefetchPageMessages = addressSettings.getPrefetchPageMessages();

      maxPageReadBytes = addressSettings.getMaxReadPageBytes();

      prefetchPageBytes = addressSettings.getPrefetchPageBytes();

      maxMessages = addressSettings.getMaxSizeMessages();

      configureSizeMetric();

      // JDBC has a maximum page size of 100K by default.
      // it can be reconfigured through jdbc-max-page-size-bytes in the JDBC configuration section
      pageSize = storageManager.getAllowedPageSize(addressSettings.getPageSizeBytes());

      if (enforcedAddressFullMessagePolicy != null) {
         this.addressFullMessagePolicy = enforcedAddressFullMessagePolicy;
      } else {
         addressFullMessagePolicy = addressSettings.getAddressFullMessagePolicy();
      }

      rejectThreshold = addressSettings.getMaxSizeBytesRejectThreshold();

      pageFullMessagePolicy = addressSettings.getPageFullMessagePolicy();

      pageLimitBytes = addressSettings.getPageLimitBytes();

      if (pageLimitBytes != null && pageLimitBytes < 0) {
         logger.debug("address {} had pageLimitBytes<0, setting it as null", address);
         pageLimitBytes = null;
      }

      Long originalLimitMessages = this.pageLimitMessages;
      this.pageLimitMessages = addressSettings.getPageLimitMessages();

      if (pageLimitMessages != null && pageLimitMessages < 0) {
         logger.debug("address {} had pageLimitMessages<0, setting it as null", address);
         pageLimitMessages = null;
      }

      if (pageLimitBytes == null && pageLimitMessages == null && pageFullMessagePolicy != null) {
         ActiveMQServerLogger.LOGGER.noPageLimitsSet(address, pageFullMessagePolicy);
         this.pageFullMessagePolicy = null;
      }

      if (pageFullMessagePolicy == null) {
         if (pageLimitBytes != null || pageLimitMessages != null) {
            ActiveMQServerLogger.LOGGER.noPagefullPolicySet(address, pageLimitBytes, pageLimitMessages);
         }
         this.pageFullMessagePolicy = null;
         this.pageLimitMessages = null;
         this.pageLimitBytes = null;
      }

      boolean pageLimitMessagesChanged = !Objects.equals(this.pageLimitMessages, originalLimitMessages);
      boolean estimatedMaxPagesChanged = false;

      if (pageLimitBytes != null && pageSize > 0) {
         Long originalEstimatedMaxPages = this.estimatedMaxPages;
         estimatedMaxPages = pageLimitBytes / pageSize;
         logger.debug("Address {} should not allow more than {} pages", storeName, estimatedMaxPages);
         estimatedMaxPagesChanged = !Objects.equals(estimatedMaxPages, originalEstimatedMaxPages);
      }

      if (!firstTime && (estimatedMaxPagesChanged || pageLimitMessagesChanged)) {
         if (estimatedMaxPagesChanged) {
            checkNumberOfPages();
         }
         cursorProvider.checkClearPageLimit();
      }
   }

   @Override
   public String toString() {
      return "PagingStoreImpl(" + this.address + ")";
   }

   @Override
   public PageFullMessagePolicy getPageFullMessagePolicy() {
      return pageFullMessagePolicy;
   }

   @Override
   public Long getPageLimitMessages() {
      return pageLimitMessages;
   }

   @Override
   public Long getPageLimitBytes() {
      return pageLimitBytes;
   }

   @Override
   public void pageFull(PageSubscription subscription) {
      this.pageFull = true;
      try {
         ActiveMQServerLogger.LOGGER.pageFull(subscription.getQueue().getName(), subscription.getQueue().getAddress(), pageLimitMessages, subscription.getCounter().getValue());
      } catch (Throwable e) {
         // I don't think subscription would ever have a null queue. I'm being cautious here for tests
         logger.warn(e.getMessage(), e);
      }
   }

   @Override
   public boolean isPageFull() {
      return pageFull;
   }

   private boolean isBelowPageLimitBytes() {
      if (estimatedMaxPages != null) {
         return (numberOfPages <= estimatedMaxPages.longValue());
      }  else {
         return true;
      }
   }

   private void checkNumberOfPages() {
      if (!isBelowPageLimitBytes()) {
         this.pageFull = true;
         ActiveMQServerLogger.LOGGER.pageFullMaxBytes(storeName, numberOfPages, estimatedMaxPages, pageLimitBytes, pageSize);
      }
   }

   @Override
   public void checkPageLimit(long numberOfMessages) {
      boolean pageMessageMessagesClear = true;
      Long pageLimitMessages = getPageLimitMessages();

      if (pageLimitMessages != null) {
         if (logger.isDebugEnabled()) { // gate to avoid boxing of numberOfMessages
            logger.debug("Address {} has {} messages on the larger queue", storeName, numberOfMessages);
         }

         pageMessageMessagesClear = (numberOfMessages < pageLimitMessages.longValue());
      }

      boolean pageMessageBytesClear = isBelowPageLimitBytes();

      if (pageMessageBytesClear && pageMessageMessagesClear) {
         pageLimitReleased();
      }
   }

   private void pageLimitReleased() {
      if (pageFull) {
         ActiveMQServerLogger.LOGGER.pageFree(getAddress());
         this.pageFull = false;
      }
   }

   @Override
   public boolean lock(long timeout) {
      if (timeout == -1) {
         lock.writeLock().lock();
         return true;
      }
      try {
         return lock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
         return false;
      }
   }

   @Override
   public void unlock() {
      lock.writeLock().unlock();
   }

   @Override
   public PageCursorProvider getCursorProvider() {
      return cursorProvider;
   }

   @Override
   public long getFirstPage() {
      return firstPageId;
   }

   @Override
   public SimpleString getAddress() {
      return address;
   }

   @Override
   public long getAddressSize() {
      return size.getSize();
   }

   @Override
   public long getAddressElements() {
      return size.getElements();
   }

   @Override
   public long getMaxSize() {
      if (maxSize <= 0) {
         // if maxSize <= 0, we will return 2 pages for de-page purposes
         return pageSize * 2L;
      } else {
         return maxSize;
      }
   }

   @Override
   public int getMaxPageReadBytes() {
      return maxPageReadBytes;
   }

   @Override
   public int getPrefetchPageBytes() {
      return prefetchPageBytes;
   }

   @Override
   public int getMaxPageReadMessages() {
      return maxPageReadMessages;
   }

   @Override
   public int getPrefetchPageMessages() {
      return prefetchPageMessages;
   }

   @Override
   public AddressFullMessagePolicy getAddressFullMessagePolicy() {
      return addressFullMessagePolicy;
   }

   @Override
   public PagingStoreImpl enforceAddressFullMessagePolicy(AddressFullMessagePolicy enforcedAddressFullMessagePolicy) {
      this.addressFullMessagePolicy = enforcedAddressFullMessagePolicy;
      this.enforcedAddressFullMessagePolicy = enforcedAddressFullMessagePolicy;
      return this;
   }

   @Override
   public int getPageSizeBytes() {
      return pageSize;
   }

   @Override
   public File getFolder() {
      final SequentialFileFactory factoryUsed = this.fileFactory;
      if (factoryUsed != null) {
         return factoryUsed.getDirectory();
      } else {
         return null;
      }
   }

   @Override
   public String getFolderName() {
      return fileFactory.getDirectoryName();
   }

   @Override
   public boolean isPaging() {
      AddressFullMessagePolicy policy = this.addressFullMessagePolicy;
      if (policy == AddressFullMessagePolicy.BLOCK) {
         return false;
      }
      if (policy == AddressFullMessagePolicy.FAIL) {
         return isFull();
      }
      if (policy == AddressFullMessagePolicy.DROP) {
         return isFull();
      }
      return paging;
   }

   @Override
   public long getNumberOfPages() {
      return numberOfPages;
   }

   @Override
   public long getCurrentWritingPage() {
      return currentPageId;
   }

   @Override
   public SimpleString getStoreName() {
      return storeName;
   }

   @Override
   public void addSyncPoint(OperationContext context) throws Exception {
      if (fileFactory == null || !fileFactory.supportsIndividualContext()) {
         if (syncTimer != null) {
            syncTimer.addSync(context);
         } else {
            ioSync();
         }
      }
   }

   @Override
   public void ioSync() throws Exception {
      if (!fileFactory.supportsIndividualContext()) {
         Page page;
         lock.readLock().lock();

         try {
            page = currentPage;
         } finally {
            lock.readLock().unlock();
         }

         if (page != null) {
            page.trySync();
         }
      }
   }

   @Override
   public void processReload() throws Exception {
      cursorProvider.processReload();
   }

   @Override
   public PagingManager getPagingManager() {
      return pagingManager;
   }

   @Override
   public boolean isStarted() {
      return running;
   }

   @Override
   public void counterSnapshot() {
      cursorProvider.counterSnapshot();
   }

   @Override
   public void stop() throws Exception {
      synchronized (this) {
         if (running) {
            cursorProvider.stop();
            running = false;
         } else {
            return;
         }
      }

      final List<Runnable> pendingTasks = new ArrayList<>();

      final Page page = currentPage;
      if (page != null) {
         page.close(true);
         currentPage = null;
      }
   }

   @Override
   public ArtemisExecutor getExecutor() {
      return executor;
   }

   @Override
   public void execute(Runnable run) {
      executor.execute(run);
   }

   @Override
   public void flushExecutors() {
      FutureLatch future = new FutureLatch();

      try {
         executor.execute(future);

         if (!future.await(60000)) {
            ActiveMQServerLogger.LOGGER.pageStoreTimeout(address);
         }
      } catch (Exception ignored) {
      }
   }

   public int getNumberOfFiles() throws Exception {
      final SequentialFileFactory fileFactory = this.fileFactory;
      if (fileFactory != null) {
         List<String> files = fileFactory.listFiles("page");
         return files.size();
      }

      return 0;
   }

   @Override
   public void start() throws Exception {
      lock.writeLock().lock();

      try {

         if (running) {
            // don't throw an exception.
            // You could have two threads adding PagingStore to a
            // ConcurrentHashMap,
            // and having both threads calling init. One of the calls should just
            // need to be ignored
            return;
         } else {
            running = true;
            firstPageId = Long.MAX_VALUE;

            // There are no files yet on this Storage. We will just return it empty
            final SequentialFileFactory fileFactory = this.fileFactory;
            if (fileFactory != null) {

               int pageId = 0;
               currentPageId = pageId;
               assert currentPage == null;
               currentPage = null;

               List<String> files = fileFactory.listFiles("page");

               numberOfPages = files.size();

               checkNumberOfPages();

               for (String fileName : files) {
                  final int fileId = PagingStoreImpl.getPageIdFromFileName(fileName);

                  if (fileId > pageId) {
                     pageId = fileId;
                  }

                  if (fileId < firstPageId) {
                     firstPageId = fileId;
                  }
               }

               currentPageId = pageId;

               if (pageId != 0) {
                  reloadLivePage(pageId);
               }

               // We will not mark it for paging if there's only a single empty file
               final Page page = currentPage;
               if (page != null && !(numberOfPages == 1 && page.getSize() == 0)) {
                  startPaging();
               }
            }
         }

      } finally {
         lock.writeLock().unlock();
      }
   }

   protected void reloadLivePage(long pageId) throws Exception {
      Page page = newPageObject(pageId);
      page.open(true);

      currentPageSize = page.getSize();

      page.getMessages();

      resetCurrentPage(page);

      /**
       * The page file might be incomplete in the cases: 1) last message incomplete 2) disk damaged.
       * In case 1 we can keep writing the file. But in case 2 we'd better not bcs old data might be overwritten.
       * Here we open a new page so the incomplete page would be reserved for recovery if needed.
       */
      if (page.getSize() != page.getFile().size()) {
         openNewPage();
      }
   }

   private void resetCurrentPage(Page newCurrentPage) {

      Page theCurrentPage = this.currentPage;

      if (theCurrentPage != null) {
         theCurrentPage.usageDown();
      }

      if (newCurrentPage != null) {
         newCurrentPage.usageUp();
         injectPage(newCurrentPage);
      }

      this.currentPage = newCurrentPage;
   }

   @Override
   public void stopPaging() {
      logger.debug("stopPaging being called, while isPaging={} on {}", this.paging, this.storeName);
      lock.writeLock().lock();
      try {
         final boolean isPaging = this.paging;
         if (isPaging) {
            paging = false;
            ActiveMQServerLogger.LOGGER.pageStoreStop(storeName, getPageInfo());
            pageLimitReleased();
         }
         this.cursorProvider.onPageModeCleared();
      } finally {
         lock.writeLock().unlock();
      }
   }

   private String getPageInfo() {
      return String.format("size=%d bytes (%d messages); maxSize=%d bytes (%d messages); globalSize=%d bytes (%d messages); globalMaxSize=%d bytes (%d messages);", size.getSize(), size.getElements(), maxSize, maxMessages, pagingManager.getGlobalSize(), pagingManager.getGlobalMessages(), pagingManager.getMaxSize(), pagingManager.getMaxMessages());
   }

   @Override
   public boolean startPaging() {
      if (!running) {
         return false;
      }

      lock.readLock().lock();
      try {
         // I'm not calling isPaging() here because i need to be atomic and hold a lock.
         if (paging) {
            return false;
         }
      } finally {
         lock.readLock().unlock();
      }

      // if the first check failed, we do it again under a global currentPageLock
      // (writeLock) this time
      lock.writeLock().lock();

      try {
         // Same notes from previous if (paging) on this method will apply here
         if (paging) {
            return false;
         }

         try {
            if (currentPage == null) {
               openNewPage();
            } else {
               if (!currentPage.getFile().exists() || !currentPage.getFile().isOpen()) {
                  currentPage.getFile().open();
               }
            }
         } catch (Exception e) {
            // If not possible to starting page due to an IO error, we will just consider it non paging.
            // This shouldn't happen anyway
            ActiveMQServerLogger.LOGGER.pageStoreStartIOError(e);
            storageManager.criticalError(e);
            return false;
         }
         paging = true;
         ActiveMQServerLogger.LOGGER.pageStoreStart(storeName, getPageInfo());

         return true;
      } finally {
         lock.writeLock().unlock();
      }
   }

   @Override
   public Page getCurrentPage() {
      return currentPage;
   }

   @Override
   public boolean checkPageFileExists(final long pageNumber) {
      String fileName = createFileName(pageNumber);

      SequentialFileFactory factory = null;
      try {
         factory = checkFileFactory();
         SequentialFile file = factory.createSequentialFile(fileName);
         return file.exists() && file.size() > 0;
      } catch (Exception ignored) {
         // never supposed to happen, but just in case
         logger.warn("PagingStoreFactory::checkPageFileExists never-throws assumption failed.", ignored);
         return true; // returning false would make the acks to the page to go missing.
                      // since we are not sure on the result for this case, we just return true
      }
   }

   @Override
   public Page newPageObject(final long pageNumber) throws Exception {
      String fileName = createFileName(pageNumber);

      SequentialFileFactory factory = checkFileFactory();

      SequentialFile file = factory.createSequentialFile(fileName);

      Page page = new Page(storeName, storageManager, factory, file, pageNumber);

      return page;
   }

   @Override
   public final Page usePage(final long pageId) {
      return usePage(pageId, true);
   }

   @Override
   public Page usePage(final long pageId, final boolean create) {
      return usePage(pageId, create, create);
   }

   @Override
   public Page usePage(final long pageId, final boolean createEntry, final boolean createFile) {
      synchronized (usedPages) {
         try {
            Page page = usedPages.get(pageId);
            if (createEntry && page == null) {
               page = newPageObject(pageId);
               if (page.getFile().exists()) {
                  page.getMessages();
                  injectPage(page);
               } else {
                  if (!createFile) {
                     page = null;
                  }
               }
            }
            if (page != null) {
               page.usageUp();
            }
            return page;
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            if (fileFactory != null) {
               SequentialFile file = fileFactory.createSequentialFile(createFileName(pageId));
               fileFactory.onIOError(e, e.getMessage(), file);
            }
            // in most cases this exception will not happen since the onIOError should halt the VM
            // it could eventually happen in tests though
            throw new RuntimeException(e.getMessage(), e);
         }
      }
   }


   protected SequentialFileFactory getFileFactory() throws Exception {
      checkFileFactory();
      return fileFactory;
   }

   private SequentialFileFactory checkFileFactory() throws Exception {
      SequentialFileFactory factory = fileFactory;
      if (factory == null) {
         factory = storeFactory.newFileFactory(getStoreName());
         fileFactory = factory;
      }
      return factory;
   }

   @Override
   public void forceAnotherPage() throws Exception {
      openNewPage();
   }


   /**
    * Returns a Page out of the Page System without reading it.
    * <p>
    * The method calling this method will remove the page and will start reading it outside of any
    * locks. This method could also replace the current file by a new file, and that process is done
    * through acquiring a writeLock on currentPageLock.
    * </p>
    * <p>
    * Observation: This method is used internally as part of the regular depage process, but
    * externally is used only on tests, and that's why this method is part of the Testable Interface
    * </p>
    */
   @Override
   public Page removePage(int pageId) {
      try {
         lock.writeLock().lock(); // Make sure no checks are done on currentPage while we are depaging
         try {
            if (!running) {
               return null;
            }

            if (currentPageId == pageId) {
               if (logger.isDebugEnabled()) {
                  logger.debug("Ignoring remove({}) as this is the current writing page", pageId);
               }
               // we don't deal with the current page, we let that one to be cleared from the regular depage
               return null;
            }

            Page page = usePage(pageId, false);

            if (page == null) {
               page = newPageObject(pageId);
            }

            if (page != null && page.getFile().exists()) {
               page.usageDown();
               // we only decrement numberOfPages if the file existed
               // it could have been removed by a previous delete
               // on this case we just need to ignore this and move on
               numberOfPages--;
            }

            if (logger.isTraceEnabled()) {
               logger.trace("Removing page {}, now containing numberOfPages={}", pageId, numberOfPages);
            }

            if (numberOfPages == 0) {
               if (logger.isTraceEnabled()) {
                  logger.trace("Page has no pages after removing last page {}", pageId, new Exception("Trace"));
               }
            }

            assert numberOfPages >= 0 : "numberOfPages should never be negative. on removePage(" + pageId + "). numberOfPages=" + numberOfPages;

            return page;
         } finally {
            lock.writeLock().unlock();
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
         if (e instanceof AssertionError) {
            // this will give a chance to callers log an AssertionError if assertion flag is enabled
            throw (AssertionError)e;
         }
         storageManager.criticalError(e);
         return null;
      }
   }

   /**
    * Returns a Page out of the Page System without reading it.
    * <p>
    * The method calling this method will remove the page and will start reading it outside of any
    * locks. This method could also replace the current file by a new file, and that process is done
    * through acquiring a writeLock on currentPageLock.
    * </p>
    * <p>
    * Observation: This method is used internally as part of the regular depage process, but
    * externally is used only on tests, and that's why this method is part of the Testable Interface
    * </p>
    */
   @Override
   public Page depage() throws Exception {
      lock.writeLock().lock(); // Make sure no checks are done on currentPage while we are depaging
      try {
         if (!running) {
            return null;
         }

         if (numberOfPages == 0) {
            return null;
         } else {
            final Page returnPage;

            numberOfPages--;

            // We are out of old pages, all that is left now is the current page.
            // On that case we need to replace it by a new empty page, and return the current page immediately
            if (currentPageId == firstPageId) {
               firstPageId = Integer.MAX_VALUE;
               logger.trace("Setting up firstPageID=MAX_VALUE");

               if (currentPage == null) {
                  // sanity check... it shouldn't happen!
                  throw new IllegalStateException("CurrentPage is null");
               }

               returnPage = currentPage;
               returnPage.close(true);
               resetCurrentPage(null);

               // The current page is empty... which means we reached the end of the pages
               if (returnPage.getNumberOfMessages() == 0) {
                  stopPaging();
                  returnPage.open(true);
                  returnPage.delete(null);

                  // This will trigger this address to exit the page mode,
                  // and this will make ActiveMQ Artemis start using the journal again
                  return null;
               } else {
                  // We need to create a new page, as we can't lock the address until we finish depaging.
                  openNewPage();
               }
            } else {
               if (logger.isTraceEnabled()) {
                  logger.trace("firstPageId++ = beforeIncrement={}", firstPageId);
               }
               long pageNR = firstPageId++;

               // first we look for the page on the used Pages cache
               // if non existing, we just create a new one outside of the cache
               // as we should not introduce any extras
               Page usedPage = usePage(pageNR, false);
               if (usedPage == null) {
                  returnPage = newPageObject(pageNR);
               } else {
                  returnPage = usedPage;
               }
            }

            if (!returnPage.getFile().exists()) {
               // if the file does not exist, we will just increment back to where it was before
               numberOfPages++;
            }

            // we make this assertion after checking the file existed before.
            // this could be eventually negative for a short period of time
            // but after compensating the non existent file the assertion should still hold true
            assert numberOfPages >= 0 : "numberOfPages should never be negative. on depage(). currentPageId=" + currentPageId + ", firstPageId=" + firstPageId + "";

            return returnPage;
         }
      } finally {
         lock.writeLock().unlock();
      }

   }

   private final Queue<Runnable> onMemoryFreedRunnables = new ConcurrentLinkedQueue<>();

   private void memoryReleased() {
      Runnable runnable;

      while ((runnable = onMemoryFreedRunnables.poll()) != null) {
         runnable.run();
      }
   }

   @Override
   public boolean checkMemory(final Runnable runWhenAvailable, Consumer<AtomicRunnable> blockedCallback) {
      return checkMemory(true, runWhenAvailable, null, blockedCallback);
   }

   private void addToBlockList(AtomicRunnable atomicRunnable, Consumer<AtomicRunnable> accepted) {
      atomicRunnable.setCancel(onMemoryFreedRunnables::remove);
      onMemoryFreedRunnables.add(atomicRunnable);
      if (accepted != null) {
         accepted.accept(atomicRunnable);
      }
   }

   @Override
   public boolean checkMemory(boolean runOnFailure, Runnable runWhenAvailableParameter, Runnable runWhenBlocking, Consumer<AtomicRunnable> blockedCallback) {
      AtomicRunnable runWhenAvailable = AtomicRunnable.checkAtomic(runWhenAvailableParameter);

      if (blockedViaAddressControl) {
         if (runWhenAvailable != null) {
            addToBlockList(runWhenAvailable, blockedCallback);
         }
         return false;
      }

      if (addressFullMessagePolicy == AddressFullMessagePolicy.FAIL && (maxSize != -1 || maxMessages != -1 || usingGlobalMaxSize || pagingManager.isDiskFull())) {
         if (isFull()) {
            if (runOnFailure && runWhenAvailable != null) {
               addToBlockList(runWhenAvailable, blockedCallback);
            }
            return false;
         }
      } else if (pagingManager.isDiskFull() || addressFullMessagePolicy == AddressFullMessagePolicy.BLOCK && (maxMessages != -1 || maxSize != -1 || usingGlobalMaxSize)) {
         if (pagingManager.isDiskFull() || this.full || pagingManager.isGlobalFull()) {
            if (runWhenBlocking != null) {
               runWhenBlocking.run();
            }

            addToBlockList(runWhenAvailable, blockedCallback);

            // We check again to avoid a race condition where the size can come down just after the element
            // has been added, but the check to execute was done before the element was added
            // NOTE! We do not fix this race by locking the whole thing, doing this check provides
            // MUCH better performance in a highly concurrent environment
            if (!pagingManager.isGlobalFull() && !full) {
               // run it now
               runWhenAvailable.run();
               onMemoryFreedRunnables.remove(runWhenAvailable);
            } else {
               if (usingGlobalMaxSize || pagingManager.isDiskFull()) {
                  pagingManager.addBlockedStore(this);
               }

               if (!blocking) {
                  if (pagingManager.isDiskFull()) {
                     ActiveMQServerLogger.LOGGER.blockingDiskFull(address);
                  } else {
                     ActiveMQServerLogger.LOGGER.blockingMessageProduction(address, getPageInfo());
                  }
                  blocking = true;
               }
            }

            return true;
         }
      }

      if (runWhenAvailable != null) {
         runWhenAvailable.run();
      }

      return true;
   }

   @Override
   public void addSize(final int size, boolean sizeOnly, boolean affectGlobal) {
      long newSize = this.size.addSize(size, sizeOnly, affectGlobal);
      boolean globalFull = pagingManager.isGlobalFull();

      if (newSize < 0) {
         ActiveMQServerLogger.LOGGER.negativeAddressSize(address.toString(), newSize);
      }

      if (addressFullMessagePolicy == AddressFullMessagePolicy.BLOCK || addressFullMessagePolicy == AddressFullMessagePolicy.FAIL) {
         if (usingGlobalMaxSize && !globalFull || maxSize != -1) {
            checkReleasedMemory();
         }

         return;
      } else if (addressFullMessagePolicy == AddressFullMessagePolicy.PAGE) {
         if (size > 0) {
            if (globalFull || full) {
               startPaging();
            }
         }

         return;
      }
   }

   @Override
   public boolean checkReleasedMemory() {
      if (!blockedViaAddressControl && !pagingManager.isGlobalFull() && !full) {
         executor.execute(this::memoryReleased);
         if (blocking) {
            ActiveMQServerLogger.LOGGER.unblockingMessageProduction(address, getPageInfo());
            blocking = false;
            return true;
         }
      }

      return false;
   }

   @Override
   public boolean page(Message message,
                       final Transaction tx,
                       RouteContextList listCtx) throws Exception {
      return page(message, tx, listCtx, null);
   }

   @Override
   public boolean page(Message message,
                       final Transaction tx,
                       RouteContextList listCtx,
                       Function<Message, Message> pageDecorator) throws Exception {

      if (!running) {
         return false;
      }

      boolean full = isFull();

      if (addressFullMessagePolicy == AddressFullMessagePolicy.DROP || addressFullMessagePolicy == AddressFullMessagePolicy.FAIL) {
         if (full) {
            if (message.isLargeMessage()) {
               ((LargeServerMessage) message).deleteFile();
            }

            if (addressFullMessagePolicy == AddressFullMessagePolicy.FAIL) {
               throw ActiveMQMessageBundle.BUNDLE.addressIsFull(address.toString());
            }

            // Address is full, we just pretend we are paging, and drop the data
            if (!printedDropMessagesWarning) {
               printedDropMessagesWarning = true;
               ActiveMQServerLogger.LOGGER.pageStoreDropMessages(storeName, getPageInfo());
            }
            return true;
         } else {
            return false;
         }
      } else if (addressFullMessagePolicy == AddressFullMessagePolicy.BLOCK) {
         return false;
      }

      // We need to ensure a read lock, as depage could change the paging state
      lock.readLock().lock();

      try {
         // First check done concurrently, to avoid synchronization and increase throughput
         if (!paging) {
            return false;
         }
      } finally {
         lock.readLock().unlock();
      }

      if (pageFull) {
         if (message.isLargeMessage()) {
            ((LargeServerMessage) message).deleteFile();
         }

         if (pageFullMessagePolicy == PageFullMessagePolicy.FAIL) {
            throw ActiveMQMessageBundle.BUNDLE.addressIsFull(address.toString());
         }

         if (!printedDropMessagesWarning) {
            printedDropMessagesWarning = true;
            ActiveMQServerLogger.LOGGER.pageStoreDropMessages(storeName, getPageInfo());
         }

         // we are in page mode, if we got to this point, we are dropping the message while still paging
         // this needs to return true as it is paging
         return true;
      }

      return writePage(message, tx, listCtx, pageDecorator);
   }

   private boolean writePage(Message message,
                             Transaction tx,
                             RouteContextList listCtx,
                             Function<Message, Message> pageDecorator) throws Exception {
      lock.writeLock().lock();

      try {
         if (!paging) {
            return false;
         }

         final long transactionID = (tx != null && tx.isAllowPageTransaction()) ? tx.getID() : -1L;

         if (pageDecorator != null) {
            message = pageDecorator.apply(message);
         }

         message.setPaged();

         PagedMessage pagedMessage = new PagedMessageImpl(message, routeQueues(tx, listCtx), transactionID);

         int bytesToWrite = pagedMessage.getEncodeSize() + PageReadWriter.SIZE_RECORD;

         currentPageSize += bytesToWrite;
         if (currentPageSize > pageSize && currentPage.getNumberOfMessages() > 0) {
            // Make sure nothing is currently validating or using currentPage
            openNewPage();
            currentPageSize += bytesToWrite;
         }

         if (tx != null && tx.isAllowPageTransaction()) {
            installPageTransaction(tx, listCtx);
         }

         // the apply counter will make sure we write a record on journal
         // especially on the case for non transactional sends and paging
         // doing this will give us a possibility of recovering the page counters
         long persistentSize = pagedMessage.getPersistentSize() > 0 ? pagedMessage.getPersistentSize() : 0;
         final Page page = currentPage;
         applyPageCounters(tx, page, listCtx, persistentSize);

         page.write(pagedMessage);

         if (tx == null && syncNonTransactional && message.isDurable()) {
            addSyncPoint(storageManager.getContext());
         }

         if (logger.isTraceEnabled()) {
            logger.trace("Paging message {} on pageStore {} pageNr={}", pagedMessage, getStoreName(), page.getPageId());
         }

         return true;
      } finally {
         lock.writeLock().unlock();
      }
   }

   /**
    * This method will disable cleanup of pages. No page will be deleted after this call.
    */
   @Override
   public void disableCleanup() {
      getCursorProvider().disableCleanup();
   }

   /**
    * This method will re-enable cleanup of pages. Notice that it will also start cleanup threads.
    */
   @Override
   public void enableCleanup() {
      getCursorProvider().resumeCleanup();
   }

   private long[] routeQueues(Transaction tx, RouteContextList ctx) throws Exception {
      List<org.apache.activemq.artemis.core.server.Queue> durableQueues = ctx.getDurableQueues();
      List<org.apache.activemq.artemis.core.server.Queue> nonDurableQueues = ctx.getNonDurableQueues();
      long[] ids = new long[durableQueues.size() + nonDurableQueues.size()];
      int i = 0;

      for (org.apache.activemq.artemis.core.server.Queue q : durableQueues) {
         q.getPageSubscription().notEmpty();
         ids[i++] = q.getID();
      }

      for (org.apache.activemq.artemis.core.server.Queue q : nonDurableQueues) {
         q.getPageSubscription().notEmpty();
         ids[i++] = q.getID();
      }
      return ids;
   }

   /**
    * This is done to prevent non tx to get out of sync in case of failures
    *
    * @param tx
    * @param page
    * @param ctx
    * @throws Exception
    */
   private void applyPageCounters(Transaction tx, Page page, RouteContextList ctx, long size) throws Exception {
      List<org.apache.activemq.artemis.core.server.Queue> durableQueues = ctx.getDurableQueues();
      List<org.apache.activemq.artemis.core.server.Queue> nonDurableQueues = ctx.getNonDurableQueues();
      for (org.apache.activemq.artemis.core.server.Queue q : durableQueues) {
         q.getPageSubscription().getCounter().increment(tx, 1, size);
      }

      for (org.apache.activemq.artemis.core.server.Queue q : nonDurableQueues) {
         q.getPageSubscription().getCounter().increment(tx, 1, size);
      }

   }

   @Override
   public void durableDown(Message message, int durableCount) {
      refDown(message, durableCount);
   }

   @Override
   public void durableUp(Message message, int durableCount) {
      refUp(message, durableCount);
   }

   @Override
   public void refUp(Message message, int count) {
      this.addSize(MessageReferenceImpl.getMemoryEstimate(), true);
   }

   @Override
   public void refDown(Message message, int count) {
      if (count < 0) {
         // this could happen on paged messages since they are not routed and refUp is never called
         return;
      }
      this.addSize(-MessageReferenceImpl.getMemoryEstimate(), true);
   }

   private void installPageTransaction(final Transaction tx, final RouteContextList listCtx) throws Exception {
      FinishPageMessageOperation pgOper = (FinishPageMessageOperation) tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);
      if (pgOper == null) {
         PageTransactionInfo pgTX = new PageTransactionInfoImpl(tx.getID());
         pagingManager.addTransaction(pgTX);
         pgOper = new FinishPageMessageOperation(pgTX, storageManager, pagingManager);
         tx.putProperty(TransactionPropertyIndexes.PAGE_TRANSACTION, pgOper);
         tx.addOperation(pgOper);
      }

      if (!tx.isAsync()) {
         pgOper.addStore(this);
      }

      pgOper.pageTransaction.increment(listCtx.getNumberOfDurableQueues(), listCtx.getNumberOfNonDurableQueues());

      return;
   }

   @Override
   public void destroy() throws Exception {
      // destroy has to be executed in the same executor as the cleanup
      execute(this::internalDestroy);
      OperationContext context = OperationContextImpl.getContext();
      if (context != null) {
         // this is to make clients to wait the delete completion of the storage
         context.storeLineUp();
         execute(context::done);
      }
   }

   private void internalDestroy() {
      try (ArtemisCloseable readLock = storageManager.closeableReadLock()) {
         while (true) {
            if (lock(100)) {
               break;
            }
         }

         try {
            SequentialFileFactory factory = fileFactory;
            if (factory != null) {
               try {
                  storeFactory.removeFileFactory(factory);
               } catch (Exception e) {
                  logger.warn(e.getMessage(), e);
               }
            }
         } finally {
            unlock();
            try {
               stop();
            } catch (Exception e2) {
               logger.debug(e2.getMessage(), e2);
            }
         }
      }
   }

   private static class FinishPageMessageOperation implements TransactionOperation {

      private final PageTransactionInfo pageTransaction;
      private final StorageManager storageManager;
      private final PagingManager pagingManager;
      private final Set<PagingStore> usedStores = new HashSet<>();

      private boolean stored = false;

      public void addStore(PagingStore store) {
         this.usedStores.add(store);
      }

      private FinishPageMessageOperation(final PageTransactionInfo pageTransaction,
                                         final StorageManager storageManager,
                                         final PagingManager pagingManager) {
         this.pageTransaction = pageTransaction;
         this.storageManager = storageManager;
         this.pagingManager = pagingManager;
      }

      @Override
      public void afterCommit(final Transaction tx) {
         // If part of the transaction goes to the queue, and part goes to paging, we can't let depage start for the
         // transaction until all the messages were added to the queue
         // or else we could deliver the messages out of order

         if (pageTransaction != null) {
            pageTransaction.commit();
         }
      }

      @Override
      public void afterPrepare(final Transaction tx) {
      }

      @Override
      public void afterRollback(final Transaction tx) {
         if (pageTransaction != null) {
            pageTransaction.rollback();
         }
      }

      @Override
      public void beforeCommit(final Transaction tx) throws Exception {
         if (!tx.isAsync()) {
            syncStore();
         }
         storePageTX(tx);
      }

      /**
       * @throws Exception
       */
      private void syncStore() throws Exception {
         for (PagingStore store : usedStores) {
            store.addSyncPoint(storageManager.getContext());
         }
      }

      @Override
      public void beforePrepare(final Transaction tx) throws Exception {
         syncStore();
         storePageTX(tx);
      }

      private void storePageTX(final Transaction tx) throws Exception {
         if (!stored) {
            tx.setContainsPersistent();
            pageTransaction.store(storageManager, pagingManager, tx);
            stored = true;
         }
      }

      @Override
      public void beforeRollback(final Transaction tx) throws Exception {
      }

      @Override
      public List<MessageReference> getRelatedMessageReferences() {
         return Collections.emptyList();
      }

      @Override
      public List<MessageReference> getListOnConsumer(long consumerID) {
         return Collections.emptyList();
      }

   }

   private void openNewPage() throws Exception {
      lock.writeLock().lock();

      try {
         numberOfPages++;

         checkNumberOfPages();

         final long newPageId = currentPageId + 1;

         if (logger.isTraceEnabled()) {
            logger.trace("destination {} new pageNr={}", storeName, newPageId);
         }

         final Page oldPage = currentPage;
         if (oldPage != null) {
            oldPage.close(true);
            oldPage.usageDown();
            currentPage = null;
         }

         final Page newPage = newPageObject(newPageId);

         resetCurrentPage(newPage);

         currentPageSize = 0;

         newPage.open(true);

         currentPageId = newPageId;

         if (newPageId < firstPageId) {
            logger.debug("open new page, setting firstPageId = {}, it was {} before", newPageId, firstPageId);
            firstPageId = newPageId;
         }
      } finally {
         lock.writeLock().unlock();
      }
   }

   public String createFileName(final long pageID) {
      /** {@link DecimalFormat} is not thread safe. */
      synchronized (format) {
         return format.format(pageID) + ".page";
      }
   }

   private static int getPageIdFromFileName(final String fileName) {
      return Integer.parseInt(fileName.substring(0, fileName.indexOf('.')));
   }

   // To be used on isDropMessagesWhenFull
   @Override
   public boolean isFull() {
      return full || pagingManager.isGlobalFull();
   }


   @Override
   public int getAddressLimitPercent() {
      final long currentUsage = getAddressSize();
      if (maxSize > 0) {
         return (int) (currentUsage * 100 / maxSize);
      } else if (pagingManager.isUsingGlobalSize()) {
         return (int) (currentUsage * 100 / pagingManager.getMaxSize());
      }
      return 0;
   }

   @Override
   public void block() {
      if (!blockedViaAddressControl) {
         ActiveMQServerLogger.LOGGER.blockingViaControl(address);
      }
      blockedViaAddressControl = true;
   }

   @Override
   public void unblock() {
      if (blockedViaAddressControl) {
         ActiveMQServerLogger.LOGGER.unblockingViaControl(address);
      }
      blockedViaAddressControl = false;
      checkReleasedMemory();
   }

   @Override
   public boolean isRejectingMessages() {
      if (addressFullMessagePolicy != AddressFullMessagePolicy.BLOCK) {
         return false;
      }
      return rejectThreshold != AddressSettings.DEFAULT_ADDRESS_REJECT_THRESHOLD && getAddressSize() > rejectThreshold;
   }

   @Override
   public Collection<Integer> getCurrentIds() throws Exception {
      lock.readLock().lock();
      try {
         List<Integer> ids = new ArrayList<>();
         SequentialFileFactory factory = fileFactory;
         if (factory != null) {
            for (String fileName : factory.listFiles("page")) {
               ids.add(getPageIdFromFileName(fileName));
            }
         }
         return ids;
      } finally {
         lock.readLock().unlock();
      }
   }

   @Override
   public void sendPages(ReplicationManager replicator, Collection<Integer> pageIds) throws Exception {
      final SequentialFileFactory factory = fileFactory;
      for (Integer id : pageIds) {
         SequentialFile sFile = factory.createSequentialFile(createFileName(id));
         if (!sFile.exists()) {
            continue;
         }
         ActiveMQServerLogger.LOGGER.replicaSyncFile(sFile, sFile.size());
         replicator.syncPages(sFile, id, getAddress());
      }
   }

   private void injectPage(Page page) {
      usedPages.injectPage(page);
   }

   protected int getUsedPagesSize() {
      return usedPages.size();
   }

   protected void forEachUsedPage(Consumer<Page> consumerPage) {
      usedPages.forEachUsedPage(consumerPage);
   }

   @Override
   public StorageManager getStorageManager() {
      return storageManager;
   }

}
