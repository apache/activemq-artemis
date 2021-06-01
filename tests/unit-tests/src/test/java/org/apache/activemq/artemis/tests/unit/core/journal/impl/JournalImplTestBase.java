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

import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.activemq.artemis.cli.commands.tools.journal.DecodeJournal;
import org.apache.activemq.artemis.cli.commands.tools.journal.EncodeJournal;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TestableJournal;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.journal.impl.JournalReaderCallback;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.collections.SparseArrayLinkedList;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public abstract class JournalImplTestBase extends ActiveMQTestBase {

   @Before
   public void startLogger() {
      AssertionLoggerHandler.startCapture();
   }

   @After
   public void stopLogger() {
      try {
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ144009"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   private static final Logger log = Logger.getLogger(JournalImplTestBase.class);

   protected List<RecordInfo> records = new LinkedList<>();

   protected TestableJournal journal;

   protected int recordLength = 1024;

   protected Map<Long, TransactionHolder> transactions = new LinkedHashMap<>();

   protected int maxAIO;

   protected int minFiles;

   protected int poolSize;

   protected int fileSize;

   protected boolean sync;

   protected String filePrefix = "amq";

   protected String fileExtension = "amq";

   protected SequentialFileFactory fileFactory;

   private final ReusableLatch latchDone = new ReusableLatch(0);

   private final ReusableLatch latchWait = new ReusableLatch(0);

   private Thread compactThread;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      resetFileFactory();

      fileFactory.start();

      transactions.clear();

      records.clear();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      stopComponent(journal);

      if (fileFactory != null) {
         fileFactory.stop();
      }

      fileFactory = null;

      journal = null;

      super.tearDown();
   }

   protected void resetFileFactory() throws Exception {
      if (fileFactory != null) {
         fileFactory.stop();
      }
      fileFactory = getFileFactory();
   }

   protected void checkAndReclaimFiles() throws Exception {
      journal.debugWait();
      boolean originalAutoReclaim = journal.isAutoReclaim();
      journal.setAutoReclaim(true);
      journal.checkReclaimStatus();
      journal.setAutoReclaim(originalAutoReclaim);
      journal.debugWait();
   }

   protected abstract SequentialFileFactory getFileFactory() throws Exception;

   // Private
   // ---------------------------------------------------------------------------------

   protected void setup(final int minFreeFiles, final int fileSize, final boolean sync, final int maxAIO) {
      this.minFiles = minFreeFiles;
      this.poolSize = minFreeFiles;
      this.fileSize = fileSize;
      this.sync = sync;
      this.maxAIO = maxAIO;
   }

   protected void setup(final int minFreeFiles,
                        final int poolSize,
                        final int fileSize,
                        final boolean sync,
                        final int maxAIO) {
      minFiles = minFreeFiles;
      this.poolSize = poolSize;
      this.fileSize = fileSize;
      this.sync = sync;
      this.maxAIO = maxAIO;
   }

   protected void setup(final int minFreeFiles, final int fileSize, final boolean sync) {
      minFiles = minFreeFiles;
      poolSize = minFreeFiles;
      this.fileSize = fileSize;
      this.sync = sync;
      maxAIO = 50;
   }

   protected boolean suportsRetention() {
      return true;
   }

   public void createJournal() throws Exception {
      journal = new JournalImpl(fileSize, minFiles, poolSize, 0, 0, fileFactory, filePrefix, fileExtension, maxAIO) {
         @Override
         public void onCompactDone() {
            latchDone.countDown();
            try {
               latchWait.await();
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
      };

      if (suportsRetention()) {
         // FakeSequentialFile won't support retention
         File fileBackup = new File(getTestDir(), "backupFoler");
         fileBackup.mkdirs();
         ((JournalImpl) journal).setHistoryFolder(fileBackup, -1, -1);
      }

      journal.setAutoReclaim(false);
      addActiveMQComponent(journal);
   }

   // It will start compacting, but it will let the thread in wait mode at onCompactDone, so we can validate command
   // executions
   protected void startCompact() throws Exception {
      latchDone.setCount(1);
      latchWait.setCount(1);
      this.compactThread = new Thread() {
         @Override
         public void run() {
            try {
               journal.testCompact();
            } catch (Throwable e) {
               e.printStackTrace();
            }
         }
      };

      this.compactThread.start();

      latchDone.await();
   }

   protected void finishCompact() throws Exception {
      latchWait.countDown();
      compactThread.join();
   }

   protected void startJournal() throws Exception {
      journal.start();
   }

   protected void stopJournal() throws Exception {
      stopJournal(true);
   }

   protected void stopJournal(final boolean reclaim) throws Exception {
      journal.flush();

      // We do a reclaim in here
      if (reclaim) {
         checkAndReclaimFiles();
      }

      journal.stop();
   }

   /**
    * @throws Exception
    */
   protected void exportImportJournal() throws Exception {
      log.debug("Exporting to " + getTestDir() + "/output.log");

      EncodeJournal.exportJournal(getTestDir(), this.filePrefix, this.fileExtension, this.minFiles, this.fileSize, getTestDir() + "/output.log");

      File dir = new File(getTestDir());

      FilenameFilter fnf = new FilenameFilter() {
         @Override
         public boolean accept(final File file, final String name) {
            return name.endsWith("." + fileExtension);
         }
      };

      log.debug("file = " + dir);

      File[] files = dir.listFiles(fnf);

      for (File file : files) {
         log.debug("Deleting " + file);
         file.delete();
      }

      DecodeJournal.importJournal(getTestDir(), filePrefix, fileExtension, minFiles, fileSize, getTestDir() + "/output.log");
   }

   protected void loadAndCheck() throws Exception {
      loadAndCheck(false);
   }

   /**
    * @param fileFactory
    * @param journal
    * @throws Exception
    */
   private static void describeJournal(SequentialFileFactory fileFactory,
                                       JournalImpl journal,
                                       final File path,
                                       PrintStream out) throws Exception {
      List<JournalFile> files = journal.orderFiles();

      out.println("Journal path: " + path);

      for (JournalFile file : files) {
         out.println("#" + file + " (size=" + file.getFile().size() + ")");

         JournalImpl.readJournalFile(fileFactory, file, new JournalReaderCallback() {

            @Override
            public void onReadUpdateRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception {
               out.println("operation@UpdateTX;txID=" + transactionID + "," + recordInfo);
            }

            @Override
            public void onReadUpdateRecord(final RecordInfo recordInfo) throws Exception {
               out.println("operation@Update;" + recordInfo);
            }

            @Override
            public void onReadRollbackRecord(final long transactionID) throws Exception {
               out.println("operation@Rollback;txID=" + transactionID);
            }

            @Override
            public void onReadPrepareRecord(final long transactionID,
                                            final byte[] extraData,
                                            final int numberOfRecords) throws Exception {
               out.println("operation@Prepare,txID=" + transactionID + ",numberOfRecords=" + numberOfRecords);
            }

            @Override
            public void onReadDeleteRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception {
               out.println("operation@DeleteRecordTX;txID=" + transactionID + "," + recordInfo);
            }

            @Override
            public void onReadDeleteRecord(final long recordID) throws Exception {
               out.println("operation@DeleteRecord;recordID=" + recordID);
            }

            @Override
            public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception {
               out.println("operation@Commit;txID=" + transactionID + ",numberOfRecords=" + numberOfRecords);
            }

            @Override
            public void onReadAddRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception {
               out.println("operation@AddRecordTX;txID=" + transactionID + "," + recordInfo);
            }

            @Override
            public void onReadAddRecord(final RecordInfo recordInfo) throws Exception {
               out.println("operation@AddRecord;" + recordInfo);
            }

            @Override
            public void markAsDataFile(final JournalFile file1) {
            }

         });
      }

      out.println();

   }

   protected void loadAndCheck(final boolean printDebugJournal) throws Exception {
      List<RecordInfo> committedRecords = new ArrayList<>();

      List<PreparedTransactionInfo> preparedTransactions = new ArrayList<>();

      journal.load(committedRecords, preparedTransactions, null);

      checkRecordsEquivalent(records, committedRecords);

      if (printDebugJournal) {
         printJournalLists(records, committedRecords);
      }

      // check prepared transactions

      List<PreparedTransactionInfo> prepared = new ArrayList<>();

      for (Map.Entry<Long, TransactionHolder> entry : transactions.entrySet()) {
         if (entry.getValue().prepared) {
            PreparedTransactionInfo info = new PreparedTransactionInfo(entry.getKey(), null);

            info.getRecords().addAll(entry.getValue().records);

            info.getRecordsToDelete().addAll(entry.getValue().deletes);

            prepared.add(info);
         }
      }

      checkTransactionsEquivalent(prepared, preparedTransactions);
   }

   protected void load() throws Exception {
      journal.load(new SparseArrayLinkedList<>(), null, null);
   }

   protected void beforeJournalOperation() throws Exception {
   }

   protected void add(final long... arguments) throws Exception {
      addWithSize(recordLength, arguments);
   }

   protected void addWithSize(final int size, final long... arguments) throws Exception {
      for (long element : arguments) {
         byte[] record = generateRecord(size);

         beforeJournalOperation();

         journal.appendAddRecord(element, (byte) 0, record, sync);

         records.add(new RecordInfo(element, (byte) 0, record, false, (short) 0));
      }

      journal.debugWait();
   }

   protected boolean tryUpdate(final long  argument) throws Exception {
      byte[] updateRecord = generateRecord(recordLength);

      beforeJournalOperation();

      boolean result = journal.tryAppendUpdateRecord(argument, (byte) 0, updateRecord, sync);

      if (result) {
         records.add(new RecordInfo(argument, (byte) 0, updateRecord, true, (short) 0));
      }

      return result;
   }

   protected void update(final long... arguments) throws Exception {
      for (long element : arguments) {
         byte[] updateRecord = generateRecord(recordLength);

         beforeJournalOperation();

         journal.appendUpdateRecord(element, (byte) 0, updateRecord, sync);

         records.add(new RecordInfo(element, (byte) 0, updateRecord, true, (short) 0));
      }

      journal.debugWait();
   }

   protected void delete(final long... arguments) throws Exception {
      for (long element : arguments) {
         beforeJournalOperation();

         journal.appendDeleteRecord(element, sync);

         removeRecordsForID(element);
      }

      journal.debugWait();
   }

   protected boolean tryDelete(final long argument) throws Exception {
      beforeJournalOperation();

      boolean result = journal.tryAppendDeleteRecord(argument, sync);

      if (result) {
         removeRecordsForID(argument);
      }

      journal.debugWait();

      return result;
   }

   protected void addTx(final long txID, final long... arguments) throws Exception {
      TransactionHolder tx = getTransaction(txID);

      for (long element : arguments) {
         // SIZE_BYTE + SIZE_LONG + SIZE_LONG + SIZE_INT + record.length +
         // SIZE_BYTE
         byte[] record = generateRecord(recordLength - (JournalImpl.SIZE_ADD_RECORD_TX + 1));

         beforeJournalOperation();

         journal.appendAddRecordTransactional(txID, element, (byte) 0, record);

         tx.records.add(new RecordInfo(element, (byte) 0, record, false, (short) 0));

      }

      journal.debugWait();
   }

   protected void updateTx(final long txID, final long... arguments) throws Exception {
      TransactionHolder tx = getTransaction(txID);

      for (long element : arguments) {
         byte[] updateRecord = generateRecord(recordLength - (JournalImpl.SIZE_ADD_RECORD_TX + 1));

         beforeJournalOperation();

         journal.appendUpdateRecordTransactional(txID, element, (byte) 0, updateRecord);

         tx.records.add(new RecordInfo(element, (byte) 0, updateRecord, true, (short) 0));
      }
      journal.debugWait();
   }

   protected void deleteTx(final long txID, final long... arguments) throws Exception {
      TransactionHolder tx = getTransaction(txID);

      for (long element : arguments) {
         beforeJournalOperation();

         journal.appendDeleteRecordTransactional(txID, element);

         tx.deletes.add(new RecordInfo(element, (byte) 0, null, true, (short) 0));
      }

      journal.debugWait();
   }

   protected void prepare(final long txID, final EncodingSupport xid) throws Exception {
      TransactionHolder tx = transactions.get(txID);

      if (tx == null) {
         tx = new TransactionHolder();
         transactions.put(txID, tx);
      }

      if (tx.prepared) {
         throw new IllegalStateException("Transaction is already prepared");
      }

      beforeJournalOperation();

      journal.appendPrepareRecord(txID, xid, sync);

      tx.prepared = true;

      journal.debugWait();
   }

   protected void commit(final long txID) throws Exception {
      TransactionHolder tx = transactions.remove(txID);

      if (tx == null) {
         throw new IllegalStateException("Cannot find tx " + txID);
      }

      beforeJournalOperation();

      journal.appendCommitRecord(txID, sync);

      records.addAll(tx.records);

      for (RecordInfo l : tx.deletes) {
         removeRecordsForID(l.id);
      }

      journal.debugWait();
   }

   protected void rollback(final long txID) throws Exception {
      TransactionHolder tx = transactions.remove(txID);

      if (tx == null) {
         throw new IllegalStateException("Cannot find tx " + txID);
      }

      beforeJournalOperation();

      journal.appendRollbackRecord(txID, sync);

      journal.debugWait();
   }

   protected void removeRecordsForID(final long id) {
      for (ListIterator<RecordInfo> iter = records.listIterator(); iter.hasNext(); ) {
         RecordInfo info = iter.next();

         if (info.id == id) {
            iter.remove();
         }
      }
   }

   protected TransactionHolder getTransaction(final long txID) {
      TransactionHolder tx = transactions.get(txID);

      if (tx == null) {
         tx = new TransactionHolder();

         transactions.put(txID, tx);
      }

      return tx;
   }

   protected void checkTransactionsEquivalent(final List<PreparedTransactionInfo> expected,
                                              final List<PreparedTransactionInfo> actual) {
      Assert.assertEquals("Lists not same length", expected.size(), actual.size());

      Iterator<PreparedTransactionInfo> iterExpected = expected.iterator();

      Iterator<PreparedTransactionInfo> iterActual = actual.iterator();

      while (iterExpected.hasNext()) {
         PreparedTransactionInfo rexpected = iterExpected.next();

         PreparedTransactionInfo ractual = iterActual.next();

         Assert.assertEquals("ids not same", rexpected.getId(), ractual.getId());

         checkRecordsEquivalent(rexpected.getRecords(), ractual.getRecords());

         Assert.assertEquals("deletes size not same", rexpected.getRecordsToDelete().size(), ractual.getRecordsToDelete().size());

         Iterator<RecordInfo> iterDeletesExpected = rexpected.getRecordsToDelete().iterator();

         Iterator<RecordInfo> iterDeletesActual = ractual.getRecordsToDelete().iterator();

         while (iterDeletesExpected.hasNext()) {
            long lexpected = iterDeletesExpected.next().id;

            long lactual = iterDeletesActual.next().id;

            Assert.assertEquals("Delete ids not same", lexpected, lactual);
         }
      }
   }

   protected void checkRecordsEquivalent(final List<RecordInfo> expected, final List<RecordInfo> actual) {
      if (expected.size() != actual.size()) {
         printJournalLists(expected, actual);
      }

      Assert.assertEquals("Lists not same length", expected.size(), actual.size());

      Iterator<RecordInfo> iterExpected = expected.iterator();

      Iterator<RecordInfo> iterActual = actual.iterator();

      while (iterExpected.hasNext()) {
         RecordInfo rexpected = iterExpected.next();

         RecordInfo ractual = iterActual.next();

         if (rexpected.id != ractual.id || rexpected.isUpdate != ractual.isUpdate) {
            printJournalLists(expected, actual);
         }

         Assert.assertEquals("ids not same", rexpected.id, ractual.id);

         Assert.assertEquals("type not same", rexpected.isUpdate, ractual.isUpdate);

         ActiveMQTestBase.assertEqualsByteArrays(rexpected.data, ractual.data);
      }
   }

   /**
    * @param expected
    * @param actual
    */
   protected void printJournalLists(final List<RecordInfo> expected, final List<RecordInfo> actual) {
      try {

         HashSet<RecordInfo> expectedSet = new HashSet<>();
         expectedSet.addAll(expected);

         Assert.assertEquals("There are duplicated on the expected list", expectedSet.size(), expected.size());

         HashSet<RecordInfo> actualSet = new HashSet<>();
         actualSet.addAll(actual);

         expectedSet.removeAll(actualSet);

         for (RecordInfo info : expectedSet) {
            log.warn("The following record is missing:: " + info);
         }

         Assert.assertEquals("There are duplicates on the actual list", actualSet.size(), actualSet.size());

         RecordInfo[] expectedArray = expected.toArray(new RecordInfo[expected.size()]);
         RecordInfo[] actualArray = actual.toArray(new RecordInfo[actual.size()]);
         Assert.assertArrayEquals(expectedArray, actualArray);
      } catch (AssertionError e) {

         HashSet<RecordInfo> hashActual = new HashSet<>();
         hashActual.addAll(actual);

         HashSet<RecordInfo> hashExpected = new HashSet<>();
         hashExpected.addAll(expected);

         log.debug("#Summary **********************************************************************************************************************");
         for (RecordInfo r : hashActual) {
            if (!hashExpected.contains(r)) {
               log.debug("Record " + r + " was supposed to be removed and it exists");
            }
         }

         for (RecordInfo r : hashExpected) {
            if (!hashActual.contains(r)) {
               log.debug("Record " + r + " was not found on actual list");
            }
         }

         log.debug("#expected **********************************************************************************************************************");
         for (RecordInfo recordInfo : expected) {
            log.debug("Record::" + recordInfo);
         }
         log.debug("#actual ************************************************************************************************************************");
         for (RecordInfo recordInfo : actual) {
            log.debug("Record::" + recordInfo);
         }

         log.debug("#records ***********************************************************************************************************************");

         try {
            describeJournal(journal.getFileFactory(), (JournalImpl) journal, journal.getFileFactory().getDirectory(), System.out);
         } catch (Exception e2) {
            e2.printStackTrace();
         }

      }
   }

   protected byte[] generateRecord(final int length) {
      byte[] record = new byte[length];
      for (int i = 0; i < length; i++) {
         // record[i] = RandomUtil.randomByte();
         record[i] = ActiveMQTestBase.getSamplebyte(i);
      }
      return record;
   }

   protected String debugJournal() throws Exception {
      return "***************************************************\n" + ((JournalImpl) journal).debug() +
         "***************************************************\n";
   }

   static final class TransactionHolder {

      List<RecordInfo> records = new ArrayList<>();

      List<RecordInfo> deletes = new ArrayList<>();

      boolean prepared;
   }

}
