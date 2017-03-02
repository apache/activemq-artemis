/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.journal;

import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.protocol.core.impl.CoreProtocolManagerFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManagerFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.proton.message.Message;
import org.junit.Assert;
import org.junit.Test;

public class MessageJournalTest extends ActiveMQTestBase {

   @Test
   public void testStoreCore() throws Throwable {
      ActiveMQServer server = createServer(true);

      server.start();

      CoreMessage message = new CoreMessage().initBuffer(10 * 1024).setDurable(true);

      message.setMessageID(333);

      CoreProtocolManagerFactory factory = (CoreProtocolManagerFactory) server.getRemotingService().getProtocolFactoryMap().get("CORE");

      Assert.assertNotNull(factory);

      message.getBodyBuffer().writeByte((byte)'Z');

      server.getStorageManager().storeMessage(message);

      server.getStorageManager().stop();

      JournalStorageManager journalStorageManager = (JournalStorageManager) server.getStorageManager();

      List<RecordInfo> committedRecords = new LinkedList<>();

      List<PreparedTransactionInfo> preparedTransactions = new LinkedList<>();

      TransactionFailureCallback transactionFailure = new TransactionFailureCallback() {
         @Override
         public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> recordsToDelete) {

         }
      };

      try {
         journalStorageManager.getMessageJournal().start();

         journalStorageManager.getMessageJournal().load(committedRecords, preparedTransactions, transactionFailure);

         Assert.assertEquals(1, committedRecords.size());
      } finally {
         journalStorageManager.getMessageJournal().stop();
      }

   }


   @Test
   public void testStoreAMQP() throws Throwable {
      ActiveMQServer server = createServer(true);

      server.start();

      ProtonProtocolManagerFactory factory = (ProtonProtocolManagerFactory) server.getRemotingService().getProtocolFactoryMap().get("AMQP");

      Message protonJMessage = Message.Factory.create();

      AMQPMessage message = new AMQPMessage(protonJMessage);

      message.setMessageID(333);

      Assert.assertNotNull(factory);

      server.getStorageManager().storeMessage(message);

      server.getStorageManager().stop();

      JournalStorageManager journalStorageManager = (JournalStorageManager) server.getStorageManager();

      List<RecordInfo> committedRecords = new LinkedList<>();

      List<PreparedTransactionInfo> preparedTransactions = new LinkedList<>();

      TransactionFailureCallback transactionFailure = new TransactionFailureCallback() {
         @Override
         public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> recordsToDelete) {

         }
      };

      try {
         journalStorageManager.getMessageJournal().start();

         journalStorageManager.getMessageJournal().load(committedRecords, preparedTransactions, transactionFailure);

         Assert.assertEquals(1, committedRecords.size());
      } finally {
         journalStorageManager.getMessageJournal().stop();
      }

   }

}
