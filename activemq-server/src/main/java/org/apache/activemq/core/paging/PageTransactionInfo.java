/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.core.paging;

import org.apache.activemq.core.journal.EncodingSupport;
import org.apache.activemq.core.paging.cursor.PagePosition;
import org.apache.activemq.core.paging.cursor.PageSubscription;
import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.transaction.Transaction;

/**
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface PageTransactionInfo extends EncodingSupport
{
   boolean isCommit();

   boolean isRollback();

   void setCommitted(boolean committed);

   void commit();

   void rollback();

   long getRecordID();

   void setRecordID(long id);

   long getTransactionID();

   void store(StorageManager storageManager, PagingManager pagingManager, Transaction tx) throws Exception;

   void storeUpdate(StorageManager storageManager, PagingManager pagingManager, Transaction tx) throws Exception;

   void reloadUpdate(final StorageManager storageManager, final PagingManager pagingManager, final Transaction tx, final int increment) throws Exception;

   // To be used after the update was stored or reload
   void onUpdate(int update, StorageManager storageManager, PagingManager pagingManager);

   void increment(int durableSize, int nonDurableSize);

   int getNumberOfMessages();

   /**
    * This method will hold the position to be delivered later in case this transaction is pending.
    * If the tx is not pending, it will return false, so the caller can deliver it right away
    * @param cursor
    * @param cursorPos
    * @return true if the message will be delivered later, false if it should be delivered right away
    */
   boolean deliverAfterCommit(PageSubscription cursor, PagePosition cursorPos);

}
