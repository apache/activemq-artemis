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
package org.apache.activemq.artemis.core.persistence.impl.journal;

import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;

public final class TXLargeMessageConfirmationOperation extends TransactionOperationAbstract {

   private AbstractJournalStorageManager journalStorageManager;
   public List<Long> confirmedMessages = new LinkedList<>();

   public TXLargeMessageConfirmationOperation(AbstractJournalStorageManager journalStorageManager) {
      this.journalStorageManager = journalStorageManager;
   }

   @Override
   public void afterRollback(Transaction tx) {
      for (Long msg : confirmedMessages) {
         try {
            journalStorageManager.confirmPendingLargeMessage(msg);
         } catch (Throwable e) {
            ActiveMQServerLogger.LOGGER.journalErrorConfirmingLargeMessage(e, msg);
         }
      }
   }
}
