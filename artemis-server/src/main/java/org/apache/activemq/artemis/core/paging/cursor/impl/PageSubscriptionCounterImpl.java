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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscriptionCounter;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperation;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.utils.ArtemisCloseable;
import org.jboss.logging.Logger;

/**
 * This class will encapsulate the persistent counters for the PagingSubscription
 */
public class PageSubscriptionCounterImpl implements PageSubscriptionCounter {

   private static final Logger logger = Logger.getLogger(PageSubscriptionCounterImpl.class);

   private static final int FLUSH_COUNTER = 1000;

   private final long subscriptionID;

   // the journal record id that is holding the current value
   private long recordID = -1;

   private boolean persistent;

   private final PageSubscription subscription;

   private final StorageManager storage;

   private final Executor executor;

   private final AtomicLong value = new AtomicLong(0);
   private final AtomicLong persistentSize = new AtomicLong(0);

   private final AtomicLong added = new AtomicLong(0);
   private final AtomicLong addedPersistentSize = new AtomicLong(0);

   private final AtomicLong pendingValue = new AtomicLong(0);
   private final AtomicLong pendingPersistentSize = new AtomicLong(0);

   private final LinkedList<Long> incrementRecords = new LinkedList<>();

   // We are storing pending counters for non transactional writes on page
   // we will recount a page case we still see pending records
   // as soon as we close a page we remove these records replacing by a regular page increment record
   // A Map per pageID, each page will have a set of IDs, with the increment on each one
   private final Map<Long, PendingCounter> pendingCounters = new HashMap<>();

   private LinkedList<PendingCounter> loadList;

   private final Runnable cleanupCheck = new Runnable() {
      @Override
      public void run() {
         cleanup();
      }
   };

   public PageSubscriptionCounterImpl(final StorageManager storage,
                                      final PageSubscription subscription,
                                      final Executor executor,
                                      final boolean persistent,
                                      final long subscriptionID) {
      this.subscriptionID = subscriptionID;
      this.executor = executor;
      this.storage = storage;
      this.persistent = persistent;
      this.subscription = subscription;
   }

   @Override
   public long getValueAdded() {
      return added.get() + pendingValue.get();
   }

   @Override
   public long getValue() {
      return value.get() + pendingValue.get();
   }

   @Override
   public long getPersistentSizeAdded() {
      return addedPersistentSize.get() + pendingPersistentSize.get();
   }

   @Override
   public long getPersistentSize() {
      return persistentSize.get() + pendingPersistentSize.get();
   }

   /**
    * This is used only on non transactional paging
    *
    * @param page
    * @param increment
    * @throws Exception
    */
   @Override
   public synchronized void pendingCounter(Page page, int increment, long size) throws Exception {
      if (!persistent) {
         return; // nothing to be done
      }

      PendingCounter pendingInfo = pendingCounters.get((long) page.getPageId());
      if (pendingInfo == null) {
         // We have to make sure this is sync here
         // not syncing this to disk may cause the page files to be out of sync on pages.
         // we can't afford the case where a page file is written without a record here
         long id = storage.storePendingCounter(this.subscriptionID, page.getPageId());
         pendingInfo = new PendingCounter(id, increment, size);
         pendingCounters.put((long) page.getPageId(), pendingInfo);
      } else {
         pendingInfo.addAndGet(increment, size);
      }

      pendingValue.addAndGet(increment);
      pendingPersistentSize.addAndGet(size);

      page.addPendingCounter(this);
   }

   /**
    * Cleanup temporary page counters on non transactional paged messages
    *
    * @param pageID
    */
   @Override
   public void cleanupNonTXCounters(final long pageID) throws Exception {
      PendingCounter pendingInfo;
      synchronized (this) {
         pendingInfo = pendingCounters.remove(pageID);
      }

      if (pendingInfo != null) {
         final int valueCleaned = pendingInfo.getCount();
         final long valueSizeCleaned = pendingInfo.getPersistentSize();
         Transaction tx = new TransactionImpl(storage);
         storage.deletePendingPageCounter(tx.getID(), pendingInfo.getId());

         // To apply the increment of the value just being cleaned
         increment(tx, valueCleaned, valueSizeCleaned);

         tx.addOperation(new TransactionOperationAbstract() {
            @Override
            public void afterCommit(Transaction tx) {
               pendingValue.addAndGet(-valueCleaned);
               pendingPersistentSize.updateAndGet(val -> val >= valueSizeCleaned ? val - valueSizeCleaned : 0);
            }
         });

         tx.commit();
      }
   }

   @Override
   public void increment(Transaction tx, int add, long size) throws Exception {
      if (tx == null) {
         if (persistent) {
            long id = storage.storePageCounterInc(this.subscriptionID, add, size);
            incrementProcessed(id, add, size);
         } else {
            incrementProcessed(-1, add, size);
         }
      } else {
         if (persistent) {
            tx.setContainsPersistent();
            long id = storage.storePageCounterInc(tx.getID(), this.subscriptionID, add, size);
            applyIncrementOnTX(tx, id, add, size);
         } else {
            applyIncrementOnTX(tx, -1, add, size);
         }
      }
   }

   /**
    * This method will install the TXs
    *
    * @param tx
    * @param recordID1
    * @param add
    */
   @Override
   public void applyIncrementOnTX(Transaction tx, long recordID1, int add, long size) {
      CounterOperations oper = (CounterOperations) tx.getProperty(TransactionPropertyIndexes.PAGE_COUNT_INC);

      if (oper == null) {
         oper = new CounterOperations();
         tx.putProperty(TransactionPropertyIndexes.PAGE_COUNT_INC, oper);
         tx.addOperation(oper);
      }

      oper.operations.add(new ItemOper(this, recordID1, add, size));
   }

   @Override
   public synchronized void loadValue(final long recordID1, final long value1, long size) {
      if (this.subscription != null) {
         // it could be null on testcases... which is ok
         this.subscription.notEmpty();
      }
      this.value.set(value1);
      this.added.set(value1);
      this.persistentSize.set(size);
      this.addedPersistentSize.set(size);
      this.recordID = recordID1;
   }

   public synchronized void incrementProcessed(long id, int add, long size) {
      addInc(id, add, size);
      if (incrementRecords.size() > FLUSH_COUNTER) {
         executor.execute(cleanupCheck);
      }

   }

   @Override
   public void delete() throws Exception {
      Transaction tx = new TransactionImpl(storage);

      delete(tx);

      tx.commit();
   }

   @Override
   public void delete(Transaction tx) throws Exception {
      // always lock the StorageManager first.
      try (ArtemisCloseable lock = storage.closeableReadLock()) {
         synchronized (this) {
            for (Long record : incrementRecords) {
               storage.deleteIncrementRecord(tx.getID(), record.longValue());
               tx.setContainsPersistent();
            }

            if (recordID >= 0) {
               storage.deletePageCounter(tx.getID(), this.recordID);
               tx.setContainsPersistent();
            }

            recordID = -1;
            value.set(0);
            incrementRecords.clear();
         }
      }
   }

   @Override
   public void loadInc(long id, int add, long size) {
      if (loadList == null) {
         loadList = new LinkedList<>();
      }

      loadList.add(new PendingCounter(id, add, size));
   }

   @Override
   public void processReload() {
      if (loadList != null) {
         if (subscription != null) {
            // it could be null on testcases
            subscription.notEmpty();
         }

         for (PendingCounter incElement : loadList) {
            value.addAndGet(incElement.getCount());
            added.addAndGet(incElement.getCount());
            persistentSize.addAndGet(incElement.getPersistentSize());
            addedPersistentSize.addAndGet(incElement.getPersistentSize());
            incrementRecords.add(incElement.getId());
         }
         loadList.clear();
         loadList = null;
      }
   }

   @Override
   public synchronized void addInc(long id, int variance, long size) {
      value.addAndGet(variance);
      this.persistentSize.addAndGet(size);
      if (variance > 0) {
         added.addAndGet(variance);
      }
      if (size > 0) {
         addedPersistentSize.addAndGet(size);
      }
      if (id >= 0) {
         incrementRecords.add(id);
      }
   }

   /**
    * used on testing only
    */
   public void setPersistent(final boolean persistent) {
      this.persistent = persistent;
   }

   /**
    * This method should always be called from a single threaded executor
    */
   protected void cleanup() {
      ArrayList<Long> deleteList;

      long valueReplace;
      long sizeReplace;
      synchronized (this) {
         if (incrementRecords.size() <= FLUSH_COUNTER) {
            return;
         }
         valueReplace = value.get();
         sizeReplace = persistentSize.get();
         deleteList = new ArrayList<>(incrementRecords);
         incrementRecords.clear();
      }

      long newRecordID = -1;

      long txCleanup = storage.generateID();

      try {
         for (Long value1 : deleteList) {
            storage.deleteIncrementRecord(txCleanup, value1);
         }

         if (recordID >= 0) {
            storage.deletePageCounter(txCleanup, recordID);
         }

         newRecordID = storage.storePageCounter(txCleanup, subscriptionID, valueReplace, sizeReplace);

         if (logger.isTraceEnabled()) {
            logger.trace("Replacing page-counter record = " + recordID + " by record = " + newRecordID + " on subscriptionID = " + this.subscriptionID + " for queue = " + this.subscription.getQueue().getName());
         }

         storage.commit(txCleanup);
      } catch (Exception e) {
         newRecordID = recordID;

         ActiveMQServerLogger.LOGGER.problemCleaningPagesubscriptionCounter(e);
         try {
            storage.rollback(txCleanup);
         } catch (Exception ignored) {
         }
      } finally {
         recordID = newRecordID;
      }
   }

   private static class ItemOper {

      private ItemOper(PageSubscriptionCounterImpl counter, long id, int add, long persistentSize) {
         this.counter = counter;
         this.id = id;
         this.amount = add;
         this.persistentSize = persistentSize;
      }

      PageSubscriptionCounterImpl counter;

      long id;

      int amount;

      long persistentSize;
   }

   private static class CounterOperations extends TransactionOperationAbstract implements TransactionOperation {

      LinkedList<ItemOper> operations = new LinkedList<>();

      @Override
      public void afterCommit(Transaction tx) {
         for (ItemOper oper : operations) {
            oper.counter.incrementProcessed(oper.id, oper.amount, oper.persistentSize);
         }
      }
   }

   private static class PendingCounter {
      private static final AtomicIntegerFieldUpdater<PendingCounter> COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PendingCounter.class, "count");

      private static final AtomicLongFieldUpdater<PendingCounter> SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(PendingCounter.class, "persistentSize");

      private final long id;
      private volatile int count;
      private volatile long persistentSize;

      /**
       * @param id
       * @param count
       * @param persistentSize
       */
      PendingCounter(long id, int count, long persistentSize) {
         super();
         this.id = id;
         this.count = count;
         this.persistentSize = persistentSize;
      }
      /**
       * @return the id
       */
      public long getId() {
         return id;
      }
      /**
       * @return the count
       */
      public int getCount() {
         return count;
      }
      /**
       * @return the size
       */
      public long getPersistentSize() {
         return persistentSize;
      }

      public void addAndGet(int count, long persistentSize) {
         COUNT_UPDATER.addAndGet(this, count);
         SIZE_UPDATER.addAndGet(this, persistentSize);
      }
   }
}
