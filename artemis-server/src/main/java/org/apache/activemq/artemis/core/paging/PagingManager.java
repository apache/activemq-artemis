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

   void addBlockedStore(PagingStore store);

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

   long getDiskUsableSpace();

   long getDiskTotalSpace();

   default long getGlobalSize() {
      return 0;
   }

   /**
    * Use this when you have no refernce of an address. (anonymous AMQP Producers for example)
    * @param runWhenAvailable
    */
   void checkMemory(Runnable runWhenAvailable);


   /**
    * Use this when you have no refernce of an address. (anonymous AMQP Producers for example)
    * @param runWhenAvailable
    */
   default void checkStorage(Runnable runWhenAvailable) {
      checkMemory(runWhenAvailable);
   }

}
