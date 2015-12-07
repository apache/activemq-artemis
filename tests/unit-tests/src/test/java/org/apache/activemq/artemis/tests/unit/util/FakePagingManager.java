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
package org.apache.activemq.artemis.tests.unit.util;

import java.util.Collection;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ServerMessage;

public final class FakePagingManager implements PagingManager {

   public void activate() {
   }

   public long addSize(final long size) {
      return 0;
   }

   @Override
   public void addTransaction(final PageTransactionInfo pageTransaction) {
   }

   public PagingStore createPageStore(final SimpleString destination) throws Exception {
      return null;
   }

   public long getTotalMemory() {
      return 0;
   }

   @Override
   public SimpleString[] getStoreNames() {
      return null;
   }

   public long getMaxMemory() {
      return 0;
   }

   @Override
   public PagingStore getPageStore(final SimpleString address) throws Exception {
      return null;
   }

   @Override
   public void deletePageStore(SimpleString storeName) throws Exception {
   }

   @Override
   public PageTransactionInfo getTransaction(final long transactionID) {
      return null;
   }

   public boolean isBackup() {
      return false;
   }

   public boolean isGlobalPageMode() {
      return false;
   }

   public boolean isPaging(final SimpleString destination) throws Exception {
      return false;
   }

   public boolean page(final ServerMessage message, final boolean duplicateDetection) throws Exception {
      return false;
   }

   public boolean page(final ServerMessage message,
                       final long transactionId,
                       final boolean duplicateDetection) throws Exception {
      return false;
   }

   @Override
   public void reloadStores() throws Exception {
   }

   @Override
   public void removeTransaction(final long transactionID) {

   }

   public void setGlobalPageMode(final boolean globalMode) {
   }

   public void setPostOffice(final PostOffice postOffice) {
   }

   public void resumeDepages() {
   }

   public void sync(final Collection<SimpleString> destinationsToSync) throws Exception {
   }

   @Override
   public boolean isStarted() {
      return false;
   }

   @Override
   public void start() throws Exception {
   }

   @Override
   public void stop() throws Exception {
   }

   /*
    * (non-Javadoc)
    * @see org.apache.activemq.artemis.core.paging.PagingManager#isGlobalFull()
    */
   public boolean isGlobalFull() {
      return false;
   }

   /*
    * (non-Javadoc)
    * @see org.apache.activemq.artemis.core.paging.PagingManager#getTransactions()
    */
   @Override
   public Map<Long, PageTransactionInfo> getTransactions() {
      return null;
   }

   /*
    * (non-Javadoc)
    * @see org.apache.activemq.artemis.core.paging.PagingManager#processReload()
    */
   @Override
   public void processReload() {
   }

   @Override
   public void disableCleanup() {
   }

   @Override
   public void resumeCleanup() {
   }

   /*
    * (non-Javadoc)
    * @see org.apache.activemq.artemis.core.settings.HierarchicalRepositoryChangeListener#onChange()
    */
   @Override
   public void onChange() {
   }

   @Override
   public void lock() {
      // no-op
   }

   @Override
   public void unlock() {
      // no-op
   }

}
