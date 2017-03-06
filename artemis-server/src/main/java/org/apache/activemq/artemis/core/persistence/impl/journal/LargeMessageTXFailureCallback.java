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

import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;

import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ADD_LARGE_MESSAGE;

public class LargeMessageTXFailureCallback implements TransactionFailureCallback {

   private AbstractJournalStorageManager journalStorageManager;
   private final Map<Long, Message> messages;

   public LargeMessageTXFailureCallback(AbstractJournalStorageManager journalStorageManager,
                                        final Map<Long, Message> messages) {
      super();
      this.journalStorageManager = journalStorageManager;
      this.messages = messages;
   }

   @Override
   public void failedTransaction(final long transactionID,
                                 final List<RecordInfo> records,
                                 final List<RecordInfo> recordsToDelete) {
      for (RecordInfo record : records) {
         if (record.userRecordType == ADD_LARGE_MESSAGE) {
            byte[] data = record.data;

            ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(data);

            try {
               LargeServerMessage serverMessage = journalStorageManager.parseLargeMessage(messages, buff);
               serverMessage.decrementDelayDeletionCount();
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.journalError(e);
            }
         }
      }
   }

}
