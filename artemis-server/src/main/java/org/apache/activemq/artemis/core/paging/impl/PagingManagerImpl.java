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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.jboss.logging.Logger;

public final class PagingManagerImpl implements PagingManager {

   private static final Logger logger = Logger.getLogger(PagingManagerImpl.class);

   private volatile boolean started = false;

   /**
    * Lock used at the start of synchronization between a live server and its backup.
    * Synchronization will lock all {@link PagingStore} instances, and so any operation here that
    * requires a lock on a {@link PagingStore} instance needs to take a read-lock on
    * {@link #syncLock} to avoid dead-locks.
    */
   private final ReentrantReadWriteLock syncLock = new ReentrantReadWriteLock();

   private final ConcurrentMap<SimpleString, PagingStore> stores = new ConcurrentHashMap<>();

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final PagingStoreFactory pagingStoreFactory;

   private volatile boolean cleanupEnabled = true;

   private final ConcurrentMap</*TransactionID*/Long, PageTransactionInfo> transactions = new ConcurrentHashMap<>();

   // Static
   // --------------------------------------------------------------------------------------------------------------------------

   // Constructors
   // --------------------------------------------------------------------------------------------------------------------

   public PagingManagerImpl(final PagingStoreFactory pagingSPI,
                            final HierarchicalRepository<AddressSettings> addressSettingsRepository) {
      pagingStoreFactory = pagingSPI;
      this.addressSettingsRepository = addressSettingsRepository;
      addressSettingsRepository.registerListener(this);
   }

   @Override
   public void onChange() {
      reaplySettings();
   }

   private void reaplySettings() {
      for (PagingStore store : stores.values()) {
         AddressSettings settings = this.addressSettingsRepository.getMatch(store.getAddress().toString());
         store.applySetting(settings);
      }
   }

   @Override
   public void disableCleanup() {
      if (!cleanupEnabled) {
         return;
      }

      lock();
      try {
         cleanupEnabled = false;
         for (PagingStore store : stores.values()) {
            store.disableCleanup();
         }
      }
      finally {
         unlock();
      }
   }

   @Override
   public void resumeCleanup() {
      if (cleanupEnabled) {
         return;
      }

      lock();
      try {
         cleanupEnabled = true;
         for (PagingStore store : stores.values()) {
            store.enableCleanup();
         }
      }
      finally {
         unlock();
      }
   }

   @Override
   public SimpleString[] getStoreNames() {
      Set<SimpleString> names = stores.keySet();
      return names.toArray(new SimpleString[names.size()]);
   }

   @Override
   public void reloadStores() throws Exception {
      lock();
      try {
         List<PagingStore> reloadedStores = pagingStoreFactory.reloadStores(addressSettingsRepository);

         for (PagingStore store : reloadedStores) {
            // when reloading, we need to close the previously loaded version of this
            // store
            PagingStore oldStore = stores.remove(store.getStoreName());
            if (oldStore != null) {
               oldStore.stop();
               oldStore = null;
            }
            store.start();
            stores.put(store.getStoreName(), store);
         }
      }
      finally {
         unlock();
      }

   }

   @Override
   public void deletePageStore(final SimpleString storeName) throws Exception {
      syncLock.readLock().lock();
      try {
         PagingStore store = stores.remove(storeName);
         if (store != null) {
            store.stop();
         }
      }
      finally {
         syncLock.readLock().unlock();
      }
   }

   /**
    * stores is a ConcurrentHashMap, so we don't need to synchronize this method
    */
   @Override
   public PagingStore getPageStore(final SimpleString storeName) throws Exception {
      PagingStore store = stores.get(storeName);

      if (store != null) {
         return store;
      }
      return newStore(storeName);
   }

   @Override
   public void addTransaction(final PageTransactionInfo pageTransaction) {
      if (logger.isTraceEnabled()) {
         logger.trace("Adding pageTransaction " + pageTransaction.getTransactionID());
      }
      transactions.put(pageTransaction.getTransactionID(), pageTransaction);
   }

   @Override
   public void removeTransaction(final long id) {
      if (logger.isTraceEnabled()) {
         logger.trace("Removing pageTransaction " + id);
      }
      transactions.remove(id);
   }

   @Override
   public PageTransactionInfo getTransaction(final long id) {
      if (logger.isTraceEnabled()) {
         logger.trace("looking up pageTX = " + id);
      }
      return transactions.get(id);
   }

   @Override
   public Map<Long, PageTransactionInfo> getTransactions() {
      return transactions;
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public void start() throws Exception {
      lock();
      try {
         if (started) {
            return;
         }

         pagingStoreFactory.setPagingManager(this);

         reloadStores();

         started = true;
      }
      finally {
         unlock();
      }
   }

   @Override
   public synchronized void stop() throws Exception {
      if (!started) {
         return;
      }
      started = false;

      lock();
      try {

         for (PagingStore store : stores.values()) {
            store.stop();
         }

         pagingStoreFactory.stop();
      }
      finally {
         unlock();
      }
   }

   @Override
   public void processReload() throws Exception {
      for (PagingStore store : stores.values()) {
         store.processReload();
      }
   }

   private PagingStore newStore(final SimpleString address) throws Exception {
      syncLock.readLock().lock();
      try {
         PagingStore store = stores.get(address);
         if (store == null) {
            store = pagingStoreFactory.newStore(address, addressSettingsRepository.getMatch(address.toString()));
            store.start();
            if (!cleanupEnabled) {
               store.disableCleanup();
            }
            stores.put(address, store);
         }
         return store;
      }
      finally {
         syncLock.readLock().unlock();
      }
   }

   @Override
   public void unlock() {
      syncLock.writeLock().unlock();
   }

   @Override
   public void lock() {
      syncLock.writeLock().lock();
   }

}
