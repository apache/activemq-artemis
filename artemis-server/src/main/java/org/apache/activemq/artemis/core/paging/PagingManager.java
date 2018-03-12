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
package org.apache.activemq.artemis.core.paging;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.settings.HierarchicalRepositoryChangeListener;

/**
 * <PRE>
 *
 * +--------------+      1  +----------------+       N +--------------+       N +--------+       1 +-------------------+
 * | {@link org.apache.activemq.artemis.core.postoffice.PostOffice} |-------&gt; |{@link PagingManager}|-------&gt; |{@link PagingStore} | ------&gt; | {@link org.apache.activemq.artemis.core.paging.impl.Page}  | ------&gt; | {@link org.apache.activemq.artemis.core.io.SequentialFile} |
 * +--------------+         +----------------+         +--------------+         +--------+         +-------------------+
 * |                  1 ^
 * |                    |
 * |                    |
 * |                    | 1
 * |            N +----------+
 * +------------&gt; | {@link org.apache.activemq.artemis.core.postoffice.Address} |
 * +----------+
 * </PRE>
 */
public interface PagingManager extends ActiveMQComponent, HierarchicalRepositoryChangeListener {

   /**
    * Returns the PageStore associated with the address. A new page store is created if necessary.
    */
   PagingStore getPageStore(SimpleString address) throws Exception;

   /**
    * Point to inform/restoring Transactions used when the messages were added into paging
    */
   void addTransaction(PageTransactionInfo pageTransaction);

   /**
    * Point to inform/restoring Transactions used when the messages were added into paging
    */
   PageTransactionInfo getTransaction(long transactionID);

   /**
    * @param transactionID
    */
   void removeTransaction(long transactionID);

   Map<Long, PageTransactionInfo> getTransactions();

   /**
    * Reload previously created PagingStores into memory
    *
    * @throws Exception
    */
   void reloadStores() throws Exception;

   SimpleString[] getStoreNames();

   void deletePageStore(SimpleString storeName) throws Exception;

   void processReload() throws Exception;

   void disableCleanup();

   void resumeCleanup();

   void addBlockedStore(Blockable store);

   void injectMonitor(FileStoreMonitor monitor) throws Exception;

   /**
    * Lock the manager. This method should not be called during normal PagingManager usage.
    */
   void lock();

   /**
    * Unlock the manager.
    *
    * @see #lock()
    */
   void unlock();

   /**
    * Add size at the global count level.
    * if totalSize &gt; globalMaxSize it will return true
    */
   PagingManager addSize(int size);

   boolean isUsingGlobalSize();

   boolean isGlobalFull();

   boolean isDiskFull();

   default long getGlobalSize() {
      return 0;
   }

   boolean checkMemory(Runnable runnable);

   // To be used when the memory is oversized either by local settings or global settings on blocking addresses
   final class OverSizedRunnable implements Runnable {

      private final AtomicBoolean ran = new AtomicBoolean(false);

      private final Runnable runnable;

      public OverSizedRunnable(final Runnable runnable) {
         this.runnable = runnable;
      }

      @Override
      public void run() {
         if (ran.compareAndSet(false, true)) {
            runnable.run();
         }
      }
   }

   interface Blockable {
      /**
       * It will return true if the destination is leaving blocking.
       */
      boolean checkReleasedMemory();
   }

   final class MemoryFreedRunnablesExecutor implements Runnable {

      private final Queue<OverSizedRunnable> onMemoryFreedRunnables = new ConcurrentLinkedQueue<>();

      public void addRunnable(PagingManager.OverSizedRunnable runnable) {
         onMemoryFreedRunnables.add(runnable);
      }

      @Override
      public void run() {
         Runnable runnable;

         while ((runnable = onMemoryFreedRunnables.poll()) != null) {
            runnable.run();
         }
      }

      public boolean isEmpty() {
         return onMemoryFreedRunnables.isEmpty();
      }
   }

}
