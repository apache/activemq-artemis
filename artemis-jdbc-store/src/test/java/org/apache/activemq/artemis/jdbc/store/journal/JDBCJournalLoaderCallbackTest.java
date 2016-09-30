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

import java.sql.DriverManager;
import java.util.ArrayList;

import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JDBCJournalLoaderCallbackTest {

   @Rule
   public ThreadLeakCheckRule threadLeakCheckRule = new ThreadLeakCheckRule();

   @Test
   public void testAddDeleteRecord() throws Exception {

      ArrayList<RecordInfo> committedRecords = new ArrayList<>();
      ArrayList<PreparedTransactionInfo> preparedTransactions = new ArrayList<>();
      TransactionFailureCallback failureCallback = null;
      boolean fixBadTX = false;

      JDBCJournalLoaderCallback cb = new JDBCJournalLoaderCallback(committedRecords, preparedTransactions, failureCallback, fixBadTX);

      RecordInfo record = new RecordInfo(42, (byte) 0, null, false, (short) 0);
      cb.addRecord(record);
      assertEquals(1, committedRecords.size());
      assertTrue(committedRecords.contains(record));

      cb.deleteRecord(record.id);
      assertTrue(committedRecords.isEmpty());
   }

   @After
   public void shutdownDerby() {
      try {
         DriverManager.getConnection("jdbc:derby:;shutdown=true");
      } catch (Exception ignored) {
      }
   }

}
