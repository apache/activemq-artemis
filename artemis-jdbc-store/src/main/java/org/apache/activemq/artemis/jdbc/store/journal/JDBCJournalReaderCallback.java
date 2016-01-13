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

package org.apache.activemq.artemis.jdbc.store.journal;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.artemis.core.journal.LoaderCallback;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalReaderCallback;
import org.apache.activemq.artemis.core.journal.impl.JournalTransaction;

public class JDBCJournalReaderCallback implements JournalReaderCallback {

   private final Map<Long, TransactionHolder> loadTransactions = new LinkedHashMap<Long, TransactionHolder>();

   private final LoaderCallback loadManager;

   private final ConcurrentMap<Long, JournalTransaction> transactions = new ConcurrentHashMap<Long, JournalTransaction>();

   public JDBCJournalReaderCallback(LoaderCallback loadManager) {
      this.loadManager = loadManager;
   }

   public void onReadAddRecord(final RecordInfo info) throws Exception {
      loadManager.addRecord(info);
   }

   public void onReadUpdateRecord(final RecordInfo info) throws Exception {
      loadManager.updateRecord(info);
   }

   public void onReadDeleteRecord(final long recordID) throws Exception {
      loadManager.deleteRecord(recordID);
   }

   public void onReadUpdateRecordTX(final long transactionID, final RecordInfo info) throws Exception {
      onReadAddRecordTX(transactionID, info);
   }

   public void onReadAddRecordTX(final long transactionID, final RecordInfo info) throws Exception {
      TransactionHolder tx = loadTransactions.get(transactionID);
      if (tx == null) {
         tx = new TransactionHolder(transactionID);
         loadTransactions.put(transactionID, tx);
      }
      tx.recordInfos.add(info);
   }

   public void onReadDeleteRecordTX(final long transactionID, final RecordInfo info) throws Exception {
      TransactionHolder tx = loadTransactions.get(transactionID);
      if (tx == null) {
         tx = new TransactionHolder(transactionID);
         loadTransactions.put(transactionID, tx);
      }
      tx.recordsToDelete.add(info);
   }

   public void onReadPrepareRecord(final long transactionID,
                                   final byte[] extraData,
                                   final int numberOfRecords) throws Exception {
      TransactionHolder tx = loadTransactions.get(transactionID);
      if (tx == null) {
         tx = new TransactionHolder(transactionID);
         loadTransactions.put(transactionID, tx);
      }
      tx.prepared = true;
      tx.extraData = extraData;
   }

   public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception {
      TransactionHolder tx = loadTransactions.remove(transactionID);
      if (tx != null) {
         //         JournalTransaction journalTransaction = transactions.remove(transactionID);
         //         if (journalTransaction == null)
         //         {
         //            throw new IllegalStateException("Cannot Commit, tx not found with ID: " + transactionID);
         //         }

         for (RecordInfo txRecord : tx.recordInfos) {
            if (txRecord.isUpdate) {
               loadManager.updateRecord(txRecord);
            }
            else {
               loadManager.addRecord(txRecord);
            }
         }

         for (RecordInfo deleteValue : tx.recordsToDelete) {
            loadManager.deleteRecord(deleteValue.id);
         }
      }
   }

   public void onReadRollbackRecord(final long transactionID) throws Exception {
      TransactionHolder tx = loadTransactions.remove(transactionID);
      if (tx == null) {
         throw new IllegalStateException("Cannot rollback, tx not found with ID: " + transactionID);
      }
   }

   @Override
   public void markAsDataFile(JournalFile file) {
      // Not needed for JDBC journal impl
   }
}