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
package org.apache.activemq.artemis.tests.unit.core.journal.impl;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.fakes.SimpleEncoding;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JournalAsyncTest extends ActiveMQTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private FakeSequentialFileFactory factory;

   private JournalImpl journalImpl = null;

   private ArrayList<RecordInfo> records = null;

   private ArrayList<PreparedTransactionInfo> transactions = null;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testAsynchronousCommit() throws Exception {
      doAsynchronousTest(true);
   }

   @Test
   public void testAsynchronousRollback() throws Exception {
      doAsynchronousTest(false);
   }

   public void doAsynchronousTest(final boolean isCommit) throws Exception {
      final int JOURNAL_SIZE = 20000;

      setupJournal(JOURNAL_SIZE, 100, 5);

      factory.setHoldCallbacks(true, null);

      final CountDownLatch latch = new CountDownLatch(1);

      class LocalThread extends Thread {

         Exception e;

         @Override
         public void run() {
            try {
               for (int i = 0; i < 10; i++) {
                  journalImpl.appendAddRecordTransactional(1L, i, (byte) 1, new SimpleEncoding(1, (byte) 0));
               }

               journalImpl.debugWait();

               latch.countDown();
               factory.setHoldCallbacks(false, null);
               if (isCommit) {
                  journalImpl.appendCommitRecord(1L, true);
               } else {
                  journalImpl.appendRollbackRecord(1L, true);
               }
            } catch (Exception e) {
               e.printStackTrace();
               this.e = e;
            }
         }
      }

      LocalThread t = new LocalThread();
      t.start();

      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

      Thread.yield();

      Thread.sleep(100);

      Assert.assertTrue(t.isAlive());

      factory.flushAllCallbacks();

      t.join();

      if (t.e != null) {
         throw t.e;
      }
   }

   // If a callback error already arrived, we should just throw the exception right away
   @Test
   public void testPreviousError() throws Exception {
      final int JOURNAL_SIZE = 20000;

      setupJournal(JOURNAL_SIZE, 100, 5);

      factory.setHoldCallbacks(true, null);
      factory.setGenerateErrors(true);

      journalImpl.appendAddRecordTransactional(1L, 1, (byte) 1, new SimpleEncoding(1, (byte) 0));

      journalImpl.debugWait();

      factory.flushAllCallbacks();

      factory.setGenerateErrors(false);
      factory.setHoldCallbacks(false, null);

      try {
         journalImpl.appendAddRecordTransactional(1L, 2, (byte) 1, new SimpleEncoding(1, (byte) 0));
         journalImpl.appendCommitRecord(1L, true);
         Assert.fail("Exception expected");
         // An exception already happened in one of the elements on this transaction.
         // We can't accept any more elements on the transaction
      } catch (Exception ignored) {

      }
   }

   @Test
   public void testSyncNonTransaction() throws Exception {
      final int JOURNAL_SIZE = 20000;

      setupJournal(JOURNAL_SIZE, 100, 5);

      factory.setGenerateErrors(true);

      try {
         journalImpl.appendAddRecord(1L, (byte) 0, new SimpleEncoding(1, (byte) 0), true);
         Assert.fail("Exception expected");
      } catch (Exception ignored) {

      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      records = new ArrayList<>();

      transactions = new ArrayList<>();

      factory = null;

      journalImpl = null;

   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (journalImpl != null) {
         try {
            journalImpl.stop();
         } catch (Throwable ignored) {
         }
      }

      super.tearDown();
   }

   // Private -------------------------------------------------------
   private void setupJournal(final int journalSize,
                             final int alignment,
                             final int numberOfMinimalFiles) throws Exception {
      if (factory == null) {
         factory = new FakeSequentialFileFactory(alignment, true);
      }

      if (journalImpl != null) {
         journalImpl.stop();
      }

      journalImpl = new JournalImpl(journalSize, numberOfMinimalFiles, numberOfMinimalFiles, 0, 0, factory, "tt", "tt", 1000);

      journalImpl.start();

      records.clear();
      transactions.clear();

      journalImpl.load(records, transactions, null);
   }

   // Inner classes -------------------------------------------------

}
