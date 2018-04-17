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
package org.apache.activemq.artemis.tests.integration.jdbc.store.journal;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.journal.JDBCJournalImpl;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class JDBCJournalTest extends ActiveMQTestBase {

   @Rule
   public ThreadLeakCheckRule threadLeakCheckRule = new ThreadLeakCheckRule();

   private JDBCJournalImpl journal;

   private ScheduledExecutorService scheduledExecutorService;

   private ExecutorService executorService;

   private SQLProvider sqlProvider;

   private DatabaseStorageConfiguration dbConf;

   @After
   @Override
   public void tearDown() throws Exception {
      journal.destroy();
      try {
         DriverManager.getConnection("jdbc:derby:;shutdown=true");
      } catch (Exception ignored) {
      }
      scheduledExecutorService.shutdown();
      scheduledExecutorService = null;
      executorService.shutdown();
      executorService = null;

   }

   @Before
   public void setup() throws Exception {
      dbConf = createDefaultDatabaseStorageConfiguration();
      sqlProvider = JDBCUtils.getSQLProvider(
         dbConf.getJdbcDriverClassName(),
         dbConf.getMessageTableName(),
         SQLProvider.DatabaseStoreType.MESSAGE_JOURNAL);
      scheduledExecutorService = new ScheduledThreadPoolExecutor(5);
      executorService = Executors.newSingleThreadExecutor();
      journal = new JDBCJournalImpl(dbConf.getJdbcConnectionUrl(), dbConf.getJdbcDriverClassName(), sqlProvider, scheduledExecutorService, executorService, new IOCriticalErrorListener() {
         @Override
         public void onIOException(Throwable code, String message, SequentialFile file) {

         }
      }, 5);
      journal.start();
   }

   @Test
   public void testRestartEmptyJournal() throws SQLException {
      Assert.assertTrue(journal.isStarted());
      Assert.assertEquals(0, journal.getNumberOfRecords());
      journal.stop();
      journal.start();
      Assert.assertTrue(journal.isStarted());
   }

   @Test
   public void testConcurrentEmptyJournal() throws SQLException {
      Assert.assertTrue(journal.isStarted());
      Assert.assertEquals(0, journal.getNumberOfRecords());
      final JDBCJournalImpl secondJournal = new JDBCJournalImpl(dbConf.getJdbcConnectionUrl(),
                                                                          dbConf.getJdbcDriverClassName(),
                                                                          sqlProvider, scheduledExecutorService,
                                                                          executorService, (code, message, file) -> {
         Assert.fail(message);
      }, 5);
      secondJournal.start();
      try {
         Assert.assertTrue(secondJournal.isStarted());
      } finally {
         secondJournal.stop();
      }
   }

   @Test
   public void testInsertRecords() throws Exception {
      int noRecords = 10;
      for (int i = 0; i < noRecords; i++) {
         journal.appendAddRecord(i, (byte) 1, new byte[0], true);
      }

      assertEquals(noRecords, journal.getNumberOfRecords());
   }

   @Test
   public void testCallbacks() throws Exception {
      final int noRecords = 10;
      final CountDownLatch done = new CountDownLatch(noRecords);

      IOCompletion completion = new IOCompletion() {
         @Override
         public void storeLineUp() {
         }

         @Override
         public void done() {
            done.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      };

      for (int i = 0; i < noRecords; i++) {
         journal.appendAddRecord(1, (byte) 1, new FakeEncodingSupportImpl(new byte[0]), true, completion);
      }
      journal.sync();

      done.await(5, TimeUnit.SECONDS);
      assertEquals(done.getCount(), 0);
   }

   @Test
   public void testReadJournal() throws Exception {
      int noRecords = 100;

      // Standard Add Records
      for (int i = 0; i < noRecords; i++) {
         journal.appendAddRecord(i, (byte) i, new byte[i], true);
      }

      // TX Records
      int noTx = 10;
      int noTxRecords = 100;
      for (int i = 1000; i < 1000 + noTx; i++) {
         for (int j = 0; j < noTxRecords; j++) {
            journal.appendAddRecordTransactional(i, Long.valueOf(i + "" + j), (byte) 1, new byte[0]);
         }
         journal.appendPrepareRecord(i, new byte[0], true);
         journal.appendCommitRecord(i, true);
      }

      Thread.sleep(2000);
      List<RecordInfo> recordInfos = new ArrayList<>();
      List<PreparedTransactionInfo> txInfos = new ArrayList<>();

      journal.load(recordInfos, txInfos, null);

      assertEquals(noRecords + (noTxRecords * noTx), recordInfos.size());
   }

}
