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
package org.apache.activemq.artemis.tests.integration.journal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.LoaderCallback;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.SpawnedVMSupport;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test spawns a remote VM, as we want to "crash" the VM right after the journal is filled with data
 */
public class ValidateTransactionHealthTest extends ActiveMQTestBase {

   private static final int OK = 10;

   @Test
   public void testAIO() throws Exception {
      internalTest("aio", getTestDir(), 10000, 100, true, true, 1);
   }

   @Test
   public void testAIOHugeTransaction() throws Exception {
      internalTest("aio", getTestDir(), 10000, 10000, true, true, 1);
   }

   @Test
   public void testAIOMultiThread() throws Exception {
      internalTest("aio", getTestDir(), 1000, 100, true, true, 10);
   }

   @Test
   public void testAIONonTransactionalNoExternalProcess() throws Exception {
      internalTest("aio", getTestDir(), 1000, 0, true, false, 10);
   }

   @Test
   public void testNIO() throws Exception {
      internalTest("nio", getTestDir(), 10000, 100, true, true, 1);
   }

   @Test
   public void testNIOHugeTransaction() throws Exception {
      internalTest("nio", getTestDir(), 10000, 10000, true, true, 1);
   }

   @Test
   public void testNIOMultiThread() throws Exception {
      internalTest("nio", getTestDir(), 1000, 100, true, true, 10);
   }

   @Test
   public void testNIONonTransactional() throws Exception {
      internalTest("nio", getTestDir(), 10000, 0, true, true, 1);
   }

   @Test
   public void testNIO2() throws Exception {
      internalTest("nio2", getTestDir(), 10000, 100, true, true, 1);
   }

   @Test
   public void testNIO2HugeTransaction() throws Exception {
      internalTest("nio2", getTestDir(), 10000, 10000, true, true, 1);
   }

   @Test
   public void testNIO2MultiThread() throws Exception {
      internalTest("nio2", getTestDir(), 1000, 100, true, true, 10);
   }

   @Test
   public void testNIO2NonTransactional() throws Exception {
      internalTest("nio2", getTestDir(), 10000, 0, true, true, 1);
   }

   // Package protected ---------------------------------------------

   private void internalTest(final String type,
                             final String journalDir,
                             final long numberOfRecords,
                             final int transactionSize,
                             final boolean append,
                             final boolean externalProcess,
                             final int numberOfThreads) throws Exception {
      try {
         if (type.equals("aio") && !LibaioContext.isLoaded()) {
            // Using System.out as this output will go towards junit report
            System.out.println("AIO not found, test being ignored on this platform");
            return;
         }

         // This property could be set to false for debug purposes.
         if (append) {
            if (externalProcess) {
               Process process = SpawnedVMSupport.spawnVM(ValidateTransactionHealthTest.class.getCanonicalName(), type, journalDir, Long.toString(numberOfRecords), Integer.toString(transactionSize), Integer.toString(numberOfThreads));
               process.waitFor();
               Assert.assertEquals(ValidateTransactionHealthTest.OK, process.exitValue());
            } else {
               JournalImpl journal = ValidateTransactionHealthTest.appendData(type, journalDir, numberOfRecords, transactionSize, numberOfThreads);
               journal.stop();
            }
         }

         reload(type, journalDir, numberOfRecords, numberOfThreads);
      } finally {
         File file = new File(journalDir);
         deleteDirectory(file);
      }
   }

   private void reload(final String type,
                       final String journalDir,
                       final long numberOfRecords,
                       final int numberOfThreads) throws Exception {
      JournalImpl journal = ValidateTransactionHealthTest.createJournal(type, journalDir);

      journal.start();
      Loader loadTest = new Loader(numberOfRecords);
      journal.load(loadTest);
      Assert.assertEquals(numberOfRecords * numberOfThreads, loadTest.numberOfAdds);
      Assert.assertEquals(0, loadTest.numberOfPreparedTransactions);
      Assert.assertEquals(0, loadTest.numberOfUpdates);
      Assert.assertEquals(0, loadTest.numberOfDeletes);

      journal.stop();

      if (loadTest.ex != null) {
         throw loadTest.ex;
      }
   }

   // Inner classes -------------------------------------------------

   static class Loader implements LoaderCallback {

      int numberOfPreparedTransactions = 0;

      int numberOfAdds = 0;

      int numberOfDeletes = 0;

      int numberOfUpdates = 0;

      long expectedRecords = 0;

      Exception ex = null;

      long lastID = 0;

      Loader(final long expectedRecords) {
         this.expectedRecords = expectedRecords;
      }

      @Override
      public void addPreparedTransaction(final PreparedTransactionInfo preparedTransaction) {
         numberOfPreparedTransactions++;

      }

      @Override
      public void addRecord(final RecordInfo info) {
         if (info.id == lastID) {
            System.out.println("id = " + info.id + " last id = " + lastID);
         }

         ByteBuffer buffer = ByteBuffer.wrap(info.data);
         long recordValue = buffer.getLong();

         if (recordValue != info.id) {
            ex = new Exception("Content not as expected (" + recordValue + " != " + info.id + ")");

         }

         lastID = info.id;
         numberOfAdds++;

      }

      @Override
      public void deleteRecord(final long id) {
         numberOfDeletes++;

      }

      @Override
      public void updateRecord(final RecordInfo info) {
         numberOfUpdates++;

      }

      @Override
      public void failedTransaction(final long transactionID,
                                    final List<RecordInfo> records,
                                    final List<RecordInfo> recordsToDelete) {
      }

   }

   // Remote part of the test =================================================================

   public static void main(final String[] args) throws Exception {

      if (args.length != 5) {
         System.err.println("Use: java -cp <classpath> " + ValidateTransactionHealthTest.class.getCanonicalName() +
                               " aio|nio <journalDirectory> <NumberOfElements> <TransactionSize> <NumberOfThreads>");
         System.exit(-1);
      }
      System.out.println("Running");
      String journalType = args[0];
      String journalDir = args[1];
      long numberOfElements = Long.parseLong(args[2]);
      int transactionSize = Integer.parseInt(args[3]);
      int numberOfThreads = Integer.parseInt(args[4]);

      try {
         ValidateTransactionHealthTest.appendData(journalType, journalDir, numberOfElements, transactionSize, numberOfThreads);

         // We don't stop the journal on the case of an external process...
         // The test is making sure that committed data can be reloaded fine...
         // i.e. commits are sync on disk as stated on the transaction.
         // The journal shouldn't leave any state impeding reloading the server
      } catch (Exception e) {
         e.printStackTrace(System.out);
         System.exit(-1);
      }

      // Simulating a crash on the system right after the data was committed.
      Runtime.getRuntime().halt(ValidateTransactionHealthTest.OK);
   }

   public static JournalImpl appendData(final String journalType,
                                        final String journalDir,
                                        final long numberOfElements,
                                        final int transactionSize,
                                        final int numberOfThreads) throws Exception {
      final JournalImpl journal = ValidateTransactionHealthTest.createJournal(journalType, journalDir);

      journal.start();
      journal.load(new LoaderCallback() {

         @Override
         public void addPreparedTransaction(final PreparedTransactionInfo preparedTransaction) {
         }

         @Override
         public void addRecord(final RecordInfo info) {
         }

         @Override
         public void deleteRecord(final long id) {
         }

         @Override
         public void updateRecord(final RecordInfo info) {
         }

         @Override
         public void failedTransaction(final long transactionID,
                                       final List<RecordInfo> records,
                                       final List<RecordInfo> recordsToDelete) {
         }
      });

      LocalThread[] threads = new LocalThread[numberOfThreads];
      final AtomicLong sequenceTransaction = new AtomicLong();

      for (int i = 0; i < numberOfThreads; i++) {
         threads[i] = new LocalThread(journal, numberOfElements, transactionSize, sequenceTransaction);
         threads[i].start();
      }

      Exception e = null;
      for (LocalThread t : threads) {
         t.join();

         if (t.e != null) {
            e = t.e;
         }
      }

      if (e != null) {
         throw e;
      }

      return journal;
   }

   public static JournalImpl createJournal(final String journalType, final String journalDir) {
      JournalImpl journal = new JournalImpl(10485760, 2, 2, 0, 0, ValidateTransactionHealthTest.getFactory(journalType, journalDir), "journaltst", "tst", 500);
      return journal;
   }

   public static SequentialFileFactory getFactory(final String factoryType, final String directory) {
      if (factoryType.equals("aio")) {
         return new AIOSequentialFileFactory(new File(directory), ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, 10, false);
      } else if (factoryType.equals("nio2")) {
         return new NIOSequentialFileFactory(new File(directory), true, 1);
      } else {
         return new NIOSequentialFileFactory(new File(directory), false, 1);
      }
   }

   static class LocalThread extends Thread {

      final JournalImpl journal;

      final long numberOfElements;

      final int transactionSize;

      final AtomicLong nextID;

      Exception e;

      LocalThread(final JournalImpl journal,
                  final long numberOfElements,
                  final int transactionSize,
                  final AtomicLong nextID) {
         super();
         this.journal = journal;
         this.numberOfElements = numberOfElements;
         this.transactionSize = transactionSize;
         this.nextID = nextID;
      }

      @Override
      public void run() {
         try {
            int transactionCounter = 0;

            long transactionId = nextID.incrementAndGet();

            for (long i = 0; i < numberOfElements; i++) {

               long id = nextID.incrementAndGet();

               ByteBuffer buffer = ByteBuffer.allocate(512 * 3);
               buffer.putLong(id);

               if (transactionSize != 0) {
                  journal.appendAddRecordTransactional(transactionId, id, (byte) 99, buffer.array());

                  if (++transactionCounter == transactionSize) {
                     System.out.println("Commit transaction " + transactionId);
                     journal.appendCommitRecord(transactionId, true);
                     transactionCounter = 0;
                     transactionId = nextID.incrementAndGet();
                  }
               } else {
                  journal.appendAddRecord(id, (byte) 99, buffer.array(), false);
               }
            }

            if (transactionCounter != 0) {
               journal.appendCommitRecord(transactionId, true);
            }

            if (transactionSize == 0) {
               journal.debugWait();
            }
         } catch (Exception e) {
            this.e = e;
         }

      }
   }

}
