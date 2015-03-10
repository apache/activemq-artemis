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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.api.core.Pair;
import org.apache.activemq.core.paging.cursor.PageSubscription;
import org.apache.activemq.core.paging.cursor.PageSubscriptionCounter;
import org.apache.activemq.core.paging.impl.Page;
import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.server.ActiveMQServerLogger;
import org.apache.activemq.core.transaction.Transaction;
import org.apache.activemq.core.transaction.TransactionOperation;
import org.apache.activemq.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.core.transaction.impl.TransactionImpl;

/**
 * This class will encapsulate the persistent counters for the PagingSubscription
 */
public class PageSubscriptionCounterImpl implements PageSubscriptionCounter
{
   private static final boolean isTrace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   private static final int FLUSH_COUNTER = 1000;

   private final long subscriptionID;

   // the journal record id that is holding the current value
   private long recordID = -1;

   private boolean persistent;

   private final PageSubscription subscription;

   private final StorageManager storage;

   private final Executor executor;

   private final AtomicLong value = new AtomicLong(0);

   private final AtomicLong pendingValue = new AtomicLong(0);

   private final LinkedList<Long> incrementRecords = new LinkedList<Long>();


   // We are storing pending counters for non transactional writes on page
   // we will recount a page case we still see pending records
   // as soon as we close a page we remove these records replacing by a regular page increment record
   // A Map per pageID, each page will have a set of IDs, with the increment on each one
   private final Map<Long, Pair<Long, AtomicInteger>> pendingCounters = new HashMap<Long, Pair<Long, AtomicInteger>>();

   private LinkedList<Pair<Long, Integer>> loadList;

   private final Runnable cleanupCheck = new Runnable()
   {
      public void run()
      {
         cleanup();
      }
   };

   public PageSubscriptionCounterImpl(final StorageManager storage,
                                      final PageSubscription subscription,
                                      final Executor executor,
                                      final boolean persistent,
                                      final long subscriptionID)
   {
      this.subscriptionID = subscriptionID;
      this.executor = executor;
      this.storage = storage;
      this.persistent = persistent;
      this.subscription = subscription;
   }

   @Override
   public long getValue()
   {
      return value.get() + pendingValue.get();
   }


   /**
    * This is used only on non transactional paging
    * @param page
    * @param increment
    * @param context
    * @throws Exception
    */
   @Override
   public synchronized void pendingCounter(Page page, int increment) throws Exception
   {
      if (!persistent)
      {
         return; // nothing to be done
      }

      Pair<Long, AtomicInteger> pendingInfo = pendingCounters.get((long)page.getPageId());
      if (pendingInfo == null)
      {
         // We have to make sure this is sync here
         // not syncing this to disk may cause the page files to be out of sync on pages.
         // we can't afford the case where a page file is written without a record here
         long id = storage.storePendingCounter(this.subscriptionID, page.getPageId(), increment);
         pendingInfo = new Pair<Long, AtomicInteger>(id, new AtomicInteger(1));
         pendingCounters.put((long)page.getPageId(), pendingInfo);
      }
      else
      {
         pendingInfo.getB().addAndGet(increment);
      }

      pendingValue.addAndGet(increment);

      page.addPendingCounter(this);
   }

   /**
    * Cleanup temporary page counters on non transactional paged messages
    * @param pageID
    */
   public void cleanupNonTXCounters(final long pageID) throws Exception
   {
      Pair<Long, AtomicInteger> pendingInfo;
      synchronized (this)
      {
         pendingInfo = pendingCounters.remove(pageID);
      }

      if (pendingInfo != null)
      {
         final AtomicInteger valueCleaned = pendingInfo.getB();
         Transaction tx = new TransactionImpl(storage);
         storage.deletePendingPageCounter(tx.getID(), pendingInfo.getA());

         // To apply the increment of the value just being cleaned
         increment(tx, valueCleaned.get());

         tx.addOperation(new TransactionOperationAbstract()
         {
            @Override
            public void afterCommit(Transaction tx)
            {
               pendingValue.addAndGet(-valueCleaned.get());
            }
         });

         tx.commit();
      }
   }


   @Override
   public void increment(Transaction tx, int add) throws Exception
   {
      if (tx == null)
      {
         if (persistent)
         {
            long id = storage.storePageCounterInc(this.subscriptionID, add);
            incrementProcessed(id, add);
         }
         else
         {
            incrementProcessed(-1, add);
         }
      }
      else
      {
         if (persistent)
         {
            tx.setContainsPersistent();
            long id = storage.storePageCounterInc(tx.getID(), this.subscriptionID, add);
            applyIncrementOnTX(tx, id, add);
         }
         else
         {
            applyIncrementOnTX(tx, -1, add);
         }
      }
   }

   /**
    * This method will install the TXs
    * @param tx
    * @param recordID1
    * @param add
    */
   public void applyIncrementOnTX(Transaction tx, long recordID1, int add)
   {
      CounterOperations oper = (CounterOperations)tx.getProperty(TransactionPropertyIndexes.PAGE_COUNT_INC);

      if (oper == null)
      {
         oper = new CounterOperations();
         tx.putProperty(TransactionPropertyIndexes.PAGE_COUNT_INC, oper);
         tx.addOperation(oper);
      }

      oper.operations.add(new ItemOper(this, recordID1, add));
   }

   public synchronized void loadValue(final long recordID1, final long value1)
   {
      if (this.subscription != null)
      {
         // it could be null on testcases... which is ok
         this.subscription.notEmpty();
      }
      this.value.set(value1);
      this.recordID = recordID1;
   }

   public synchronized void incrementProcessed(long id, int add)
   {
      addInc(id, add);
      if (incrementRecords.size() > FLUSH_COUNTER)
      {
         executor.execute(cleanupCheck);
      }

   }

   public void delete() throws Exception
   {
      Transaction tx = new TransactionImpl(storage);

      delete(tx);

      tx.commit();
   }

   public void delete(Transaction tx) throws Exception
   {
      // always lock the StorageManager first.
      storage.readLock();
      try
      {
         synchronized (this)
         {
            for (Long record : incrementRecords)
            {
               storage.deleteIncrementRecord(tx.getID(), record.longValue());
               tx.setContainsPersistent();
            }

            if (recordID >= 0)
            {
               storage.deletePageCounter(tx.getID(), this.recordID);
               tx.setContainsPersistent();
            }

            recordID = -1;
            value.set(0);
            incrementRecords.clear();
         }
      }
      finally
      {
         storage.readUnLock();
      }
   }

   public void loadInc(long id, int add)
   {
      if (loadList == null)
      {
         loadList = new LinkedList<Pair<Long, Integer>>();
      }

      loadList.add(new Pair<Long, Integer>(id, add));
   }

   public void processReload()
   {
      if (loadList != null)
      {
         if (subscription != null)
         {
            // it could be null on testcases
            subscription.notEmpty();
         }

         for (Pair<Long, Integer> incElement : loadList)
         {
            value.addAndGet(incElement.getB());
            incrementRecords.add(incElement.getA());
         }
         loadList.clear();
         loadList = null;
      }
   }

   public synchronized void addInc(long id, int variance)
   {
      value.addAndGet(variance);

      if (id >= 0)
      {
         incrementRecords.add(id);
      }
   }

   /** used on testing only */
   public void setPersistent(final boolean persistent)
   {
      this.persistent = persistent;
   }


   /** This method should always be called from a single threaded executor */
   protected void cleanup()
   {
      ArrayList<Long> deleteList;

      long valueReplace;
      synchronized (this)
      {
         if (incrementRecords.size() <= FLUSH_COUNTER)
         {
            return;
         }
         valueReplace = value.get();
         deleteList = new ArrayList<Long>(incrementRecords);
         incrementRecords.clear();
      }

      long newRecordID = -1;

      long txCleanup = storage.generateID();

      try
      {
         for (Long value1 : deleteList)
         {
            storage.deleteIncrementRecord(txCleanup, value1);
         }

         if (recordID >= 0)
         {
            storage.deletePageCounter(txCleanup, recordID);
         }

         newRecordID = storage.storePageCounter(txCleanup, subscriptionID, valueReplace);

         if (isTrace)
         {
            ActiveMQServerLogger.LOGGER.trace("Replacing page-counter record = "  + recordID + " by record = " + newRecordID + " on subscriptionID = " + this.subscriptionID + " for queue = " + this.subscription.getQueue().getName());
         }

         storage.commit(txCleanup);
      }
      catch (Exception e)
      {
         newRecordID = recordID;

         ActiveMQServerLogger.LOGGER.problemCleaningPagesubscriptionCounter(e);
         try
         {
            storage.rollback(txCleanup);
         }
         catch (Exception ignored)
         {
         }
      }
      finally
      {
         recordID = newRecordID;
      }
   }

   private static class ItemOper
   {

      public ItemOper(PageSubscriptionCounterImpl counter, long id, int add)
      {
         this.counter = counter;
         this.id = id;
         this.ammount = add;
      }

      PageSubscriptionCounterImpl counter;

      long id;

      int ammount;
   }

   private static class CounterOperations extends TransactionOperationAbstract implements TransactionOperation
   {
      LinkedList<ItemOper> operations = new LinkedList<ItemOper>();

      @Override
      public void afterCommit(Transaction tx)
      {
         for (ItemOper oper : operations)
         {
            oper.counter.incrementProcessed(oper.id, oper.ammount);
         }
      }
   }
}
