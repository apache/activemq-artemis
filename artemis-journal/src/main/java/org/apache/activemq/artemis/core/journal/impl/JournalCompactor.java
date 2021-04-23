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
package org.apache.activemq.artemis.core.journal.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.LongObjectHashMap;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncoderPersister;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.dataformat.ByteArrayEncoding;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalAddRecord;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalAddRecordTX;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalCompleteRecordTX;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalCompleteRecordTX.TX_RECORD_TYPE;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalDeleteRecordTX;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalInternalRecord;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalRollbackRecordTX;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.apache.activemq.artemis.utils.RunnableEx;
import org.apache.activemq.artemis.utils.collections.ConcurrentLongHashMap;
import org.apache.activemq.artemis.utils.collections.ConcurrentLongHashSet;
import org.jboss.logging.Logger;

public class JournalCompactor extends AbstractJournalUpdateTask implements JournalRecordProvider {

   private static final Logger logger = Logger.getLogger(JournalCompactor.class);

   LongObjectHashMap<LinkedList<RunnableEx>> pendingWritesOnTX = new LongObjectHashMap<>();
   IntObjectHashMap<LongObjectHashMap<RunnableEx>> pendingUpdates = new IntObjectHashMap<>();

   // We try to separate old record from new ones when doing the compacting
   // this is a split line
   // We will force a moveNextFiles when the compactCount is bellow than COMPACT_SPLIT_LINE
   private static final short COMPACT_SPLIT_LINE = 2;

   // Snapshot of transactions that were pending when the compactor started
   private final ConcurrentLongHashMap<PendingTransaction> pendingTransactions = new ConcurrentLongHashMap<>();

   private final ConcurrentLongHashMap<JournalRecord> newRecords = new ConcurrentLongHashMap<>();

   private final ConcurrentLongHashMap<JournalTransaction> newTransactions = new ConcurrentLongHashMap<>();

   /**
    * Commands that happened during compacting
    * We can't process any counts during compacting, as we won't know in what files the records are taking place, so
    * we cache those updates. As soon as we are done we take the right account.
    */
   private final LinkedList<CompactCommand> pendingCommands = new LinkedList<>();

   public List<JournalFile> getNewDataFiles() {
      return newDataFiles;
   }

   public ConcurrentLongHashMap<JournalRecord> getNewRecords() {
      return newRecords;
   }

   public ConcurrentLongHashMap<JournalTransaction> getNewTransactions() {
      return newTransactions;
   }

   public JournalCompactor(final SequentialFileFactory fileFactory,
                           final JournalImpl journal,
                           final JournalFilesRepository filesRepository,
                           final ConcurrentLongHashSet recordsSnapshot,
                           final long firstFileID) {
      super(fileFactory, journal, filesRepository, recordsSnapshot, firstFileID);
   }

   /**
    * This methods informs the Compactor about the existence of a pending (non committed) transaction
    */
   public void addPendingTransaction(final long transactionID, final long[] ids) {
      if (logger.isTraceEnabled()) {
         logger.trace("addPendingTransaction::tx=" + transactionID + ", ids=" + Arrays.toString(ids));
      }
      pendingTransactions.put(transactionID, new PendingTransaction(ids));
   }

   public void addCommandCommit(final JournalTransaction liveTransaction, final JournalFile currentFile) {
      if (logger.isTraceEnabled()) {
         logger.trace("addCommandCommit " + liveTransaction.getId());
      }
      pendingCommands.add(new CommitCompactCommand(liveTransaction, currentFile));

      long[] ids = liveTransaction.getPositiveArray();

      PendingTransaction oldTransaction = pendingTransactions.get(liveTransaction.getId());
      long[] ids2 = null;

      if (oldTransaction != null) {
         ids2 = oldTransaction.pendingIDs;
      }

      /** If a delete comes for these records, while the compactor still working, we need to be able to take them into account for later deletes
       *  instead of throwing exceptions about non existent records */
      if (ids != null) {
         for (long id : ids) {
            addToRecordsSnaptshot(id);
         }
      }

      if (ids2 != null) {
         for (long id : ids2) {
            addToRecordsSnaptshot(id);
         }
      }
   }

   public void addCommandRollback(final JournalTransaction liveTransaction, final JournalFile currentFile) {
      if (logger.isTraceEnabled()) {
         logger.trace("addCommandRollback " + liveTransaction + " currentFile " + currentFile);
      }
      pendingCommands.add(new RollbackCompactCommand(liveTransaction, currentFile));
   }

   /**
    * @param id
    * @param usedFile
    */
   public void addCommandDelete(final long id, final JournalFile usedFile) {
      if (logger.isTraceEnabled()) {
         logger.trace("addCommandDelete id " + id + " usedFile " + usedFile);
      }
      pendingCommands.add(new DeleteCompactCommand(id, usedFile));
   }

   /**
    * @param id
    * @param usedFile
    */
   public void addCommandUpdate(final long id, final JournalFile usedFile, final int size) {
      if (logger.isTraceEnabled()) {
         logger.trace("addCommandUpdate id " + id + " usedFile " + usedFile + " size " + size);
      }
      pendingCommands.add(new UpdateCompactCommand(id, usedFile, size));
   }

   private void checkSize(final int size) throws Exception {
      checkSize(size, -1);
   }

   private void checkSize(final int size, final int compactCount) throws Exception {
      if (getWritingChannel() == null) {
         if (!checkCompact(compactCount)) {
            // will need to open a file either way
            openFile();
         }
      } else {
         if (compactCount >= 0) {
            if (checkCompact(compactCount)) {
               // The file was already moved on this case, no need to check for the size.
               // otherwise we will also need to check for the size
               return;
            }
         }

         if (getWritingChannel().writerIndex() + size > getWritingChannel().capacity()) {
            openFile();
         }
      }
   }

   int currentCount;

   // This means we will need to split when the compactCount is bellow the watermark
   boolean willNeedToSplit = false;

   boolean splitted = false;

   private boolean checkCompact(final int compactCount) throws Exception {
      if (compactCount >= COMPACT_SPLIT_LINE && !splitted) {
         willNeedToSplit = true;
      }

      if (willNeedToSplit && compactCount < COMPACT_SPLIT_LINE) {
         willNeedToSplit = false;
         splitted = false;
         openFile();
         return true;
      } else {
         return false;
      }
   }

   /**
    * Replay pending counts that happened during compacting
    */
   public void replayPendingCommands() {
      for (CompactCommand command : pendingCommands) {
         if (logger.isTraceEnabled()) {
            logger.trace("Replay " + command);
         }
         try {
            command.execute();
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            ActiveMQJournalLogger.LOGGER.errorReplayingCommands(e);
         }
      }

      pendingCommands.clear();
   }

   public void flushUpdates() throws Exception {
      Collection<LongObjectHashMap<RunnableEx>> recordsUpdate = this.pendingUpdates.values();
      for (LongObjectHashMap<RunnableEx> recordMap : recordsUpdate) {
         for (RunnableEx ex : recordMap.values()) {
            ex.run();
         }
         // a little hand for GC
         recordMap.clear();
      }
      // a little hand for GC
      recordsUpdate.clear();
   }
   // JournalReaderCallback implementation -------------------------------------------

   @Override
   public void onReadAddRecord(final RecordInfo info) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("Read Record " + info);
      }
      if (containsRecord(info.id)) {
         JournalInternalRecord addRecord = new JournalAddRecord(true, info.id, info.getUserRecordType(), EncoderPersister.getInstance(), new ByteArrayEncoding(info.data));
         addRecord.setCompactCount((short) (info.compactCount + 1));

         checkSize(addRecord.getEncodeSize(), info.compactCount);

         writeEncoder(addRecord);

         newRecords.put(info.id, new JournalRecord(currentFile, addRecord.getEncodeSize()));
      }
   }

   @Override
   public void onReadAddRecordTX(final long transactionID, final RecordInfo info) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("Read Add Record TX " + transactionID + " info " + info);
      }
      if (pendingTransactions.get(transactionID) != null) {
         produceAddRecordTX(transactionID, info);
      } else if (containsRecord(info.id)) {
         addTX(transactionID, () -> produceAddRecordTX(transactionID, info));
      }
   }

   private void produceAddRecordTX(long transactionID, RecordInfo info) throws Exception {
      JournalTransaction newTransaction = getNewJournalTransaction(transactionID);

      JournalInternalRecord record = new JournalAddRecordTX(true, transactionID, info.id, info.getUserRecordType(), EncoderPersister.getInstance(), new ByteArrayEncoding(info.data));

      record.setCompactCount((short) (info.compactCount + 1));

      checkSize(record.getEncodeSize(), info.compactCount);

      newTransaction.addPositive(currentFile, info.id, record.getEncodeSize());

      writeEncoder(record);
   }

   @Override
   public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception {

      if (logger.isTraceEnabled()) {
         logger.trace("onReadCommitRecord " + transactionID);
      }

      if (pendingTransactions.get(transactionID) != null) {
         // Sanity check, this should never happen
         ActiveMQJournalLogger.LOGGER.inconsistencyDuringCompacting(transactionID);
      } else {
         flushTX(transactionID);
         JournalTransaction newTransaction = newTransactions.remove(transactionID);
         if (newTransaction != null) {
            JournalInternalRecord commitRecord = new JournalCompleteRecordTX(TX_RECORD_TYPE.COMMIT, transactionID, null);

            checkSize(commitRecord.getEncodeSize());

            writeEncoder(commitRecord, newTransaction.getCounter(currentFile));

            newTransaction.commit(currentFile);
         }
      }
   }

   @Override
   public void onReadDeleteRecord(final long recordID) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("onReadDeleteRecord " + recordID);
      }

      if (newRecords.get(recordID) != null) {
         // Sanity check, it should never happen
         ActiveMQJournalLogger.LOGGER.inconsistencyDuringCompactingDelete(recordID);
      }

   }

   @Override
   public void onReadDeleteRecordTX(final long transactionID, final RecordInfo info) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("onReadDeleteRecordTX " + transactionID + " info " + info);
      }

      if (pendingTransactions.get(transactionID) != null) {
         produceDeleteRecordTX(transactionID, info);
      }
      // else.. nothing to be done
   }

   private void produceDeleteRecordTX(long transactionID, RecordInfo info) throws Exception {
      JournalTransaction newTransaction = getNewJournalTransaction(transactionID);

      JournalInternalRecord record = new JournalDeleteRecordTX(transactionID, info.id, new ByteArrayEncoding(info.data));

      checkSize(record.getEncodeSize());

      writeEncoder(record);

      newTransaction.addNegative(currentFile, info.id);
   }

   @Override
   public void markAsDataFile(final JournalFile file) {
      // nothing to be done here
   }

   private void addTX(long tx, RunnableEx runnable) {
      LinkedList<RunnableEx> runnables = pendingWritesOnTX.get(tx);
      if (runnables == null) {
         runnables = new LinkedList<>();
         pendingWritesOnTX.put(tx, runnables);
      }
      runnables.add(runnable);
   }

   private void flushTX(long tx) throws Exception {
      LinkedList<RunnableEx> runnables = pendingWritesOnTX.remove(tx);
      if (runnables != null) {
         for (RunnableEx runnableEx : runnables) {
            runnableEx.run();
         }
         // give a hand to GC...
         runnables.clear();
      }
   }

   private void dropTX(long tx) {
      LinkedList objects = pendingWritesOnTX.remove(tx);
      if (objects != null) {
         // a little hand to GC
         objects.clear();
      }
   }


   @Override
   public void onReadPrepareRecord(final long transactionID,
                                   final byte[] extraData,
                                   final int numberOfRecords) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("onReadPrepareRecord " + transactionID);
      }

      if (pendingTransactions.get(transactionID) != null) {
         flushTX(transactionID);
         producePrepareRecord(transactionID, extraData);
      }
   }

   private void producePrepareRecord(long transactionID, byte[] extraData) throws Exception {
      JournalTransaction newTransaction = getNewJournalTransaction(transactionID);

      JournalInternalRecord prepareRecord = new JournalCompleteRecordTX(TX_RECORD_TYPE.PREPARE, transactionID, new ByteArrayEncoding(extraData));

      checkSize(prepareRecord.getEncodeSize());

      writeEncoder(prepareRecord, newTransaction.getCounter(currentFile));

      newTransaction.prepare(currentFile);
   }

   @Override
   public void onReadRollbackRecord(final long transactionID) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("onReadRollbackRecord " + transactionID);
      }

      if (pendingTransactions.get(transactionID) != null) {
         // Sanity check, this should never happen
         throw new IllegalStateException("Inconsistency during compacting: RollbackRecord ID = " + transactionID +
                                            " for an already rolled back transaction during compacting");
      } else {
         JournalTransaction newTransaction = newTransactions.remove(transactionID);
         if (newTransaction != null) {
            flushTX(transactionID); // ths should be a moot operation, but just in case
            // we should do this only if there's a prepare record
            produceRollbackRecord(transactionID, newTransaction);
         } else {
            dropTX(transactionID);
         }

      }
   }

   private void produceRollbackRecord(long transactionID, JournalTransaction newTransaction) throws Exception {
      JournalInternalRecord rollbackRecord = new JournalRollbackRecordTX(transactionID);

      checkSize(rollbackRecord.getEncodeSize());

      writeEncoder(rollbackRecord);

      newTransaction.rollback(currentFile);
   }

   public void replaceableRecord(int recordType) {
      LongObjectHashMap<RunnableEx> longmap = new LongObjectHashMap();
      pendingUpdates.put(recordType, longmap);
   }

   @Override
   public void onReadUpdateRecord(final RecordInfo info) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("onReadUpdateRecord " + info);
      }

      if (containsRecord(info.id)) {
         LongObjectHashMap<RunnableEx> longmap = pendingUpdates.get(info.userRecordType);
         if (longmap == null) {
            // if not replaceable, we have to always produce the update
            produceUpdateRecord(info);
         } else {
            longmap.put(info.id, () -> produceUpdateRecord(info));
         }
      }
   }

   private void produceUpdateRecord(RecordInfo info) throws Exception {
      JournalInternalRecord updateRecord = new JournalAddRecord(false, info.id, info.userRecordType, EncoderPersister.getInstance(), new ByteArrayEncoding(info.data));

      updateRecord.setCompactCount((short) (info.compactCount + 1));

      checkSize(updateRecord.getEncodeSize(), info.compactCount);

      JournalRecord newRecord = newRecords.get(info.id);

      if (newRecord == null) {
         ActiveMQJournalLogger.LOGGER.compactingWithNoAddRecord(info.id);
      } else {
         newRecord.addUpdateFile(currentFile, updateRecord.getEncodeSize());
      }

      writeEncoder(updateRecord);
   }

   @Override
   public void onReadUpdateRecordTX(final long transactionID, final RecordInfo info) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("onReadUpdateRecordTX " + info);
      }

      if (pendingTransactions.get(transactionID) != null) {
         produceUpdateRecordTX(transactionID, info);
      } else if (containsRecord(info.id)) {
         addTX(transactionID, () -> produceUpdateRecordTX(transactionID, info));
      }
   }

   private void produceUpdateRecordTX(long transactionID, RecordInfo info) throws Exception {
      JournalTransaction newTransaction = getNewJournalTransaction(transactionID);

      JournalInternalRecord updateRecordTX = new JournalAddRecordTX(false, transactionID, info.id, info.userRecordType, EncoderPersister.getInstance(), new ByteArrayEncoding(info.data));

      updateRecordTX.setCompactCount((short) (info.compactCount + 1));

      checkSize(updateRecordTX.getEncodeSize(), info.compactCount);

      writeEncoder(updateRecordTX);

      newTransaction.addPositive(currentFile, info.id, updateRecordTX.getEncodeSize());
   }

   /**
    * @param transactionID
    * @return
    */
   private JournalTransaction getNewJournalTransaction(final long transactionID) {
      JournalTransaction newTransaction = newTransactions.get(transactionID);
      if (newTransaction == null) {
         if (logger.isTraceEnabled()) {
            logger.trace("creating new journal Transaction " + transactionID);
         }
         newTransaction = new JournalTransaction(transactionID, this);
         newTransactions.put(transactionID, newTransaction);
      } else if (logger.isTraceEnabled()) {
         // just logging
         logger.trace("reusing TX " + transactionID);

      }
      return newTransaction;
   }

   private abstract static class CompactCommand {

      abstract void execute() throws Exception;
   }

   private class DeleteCompactCommand extends CompactCommand {

      long id;

      JournalFile usedFile;

      private DeleteCompactCommand(final long id, final JournalFile usedFile) {
         this.id = id;
         this.usedFile = usedFile;
      }

      @Override
      void execute() throws Exception {
         JournalRecord deleteRecord = journal.getRecords().remove(id);
         if (deleteRecord == null) {
            ActiveMQJournalLogger.LOGGER.noRecordDuringCompactReplay(id);
         } else {
            deleteRecord.delete(usedFile);
         }
      }
   }

   private static class PendingTransaction {

      long[] pendingIDs;

      PendingTransaction(final long[] ids) {
         pendingIDs = ids;
      }

   }

   private class UpdateCompactCommand extends CompactCommand {

      private final long id;

      private final JournalFile usedFile;

      private final int size;

      private UpdateCompactCommand(final long id, final JournalFile usedFile, final int size) {
         this.id = id;
         this.usedFile = usedFile;
         this.size = size;
      }

      @Override
      void execute() throws Exception {
         JournalRecord updateRecord = journal.getRecords().get(id);
         if (updateRecord == null) {
            ActiveMQJournalLogger.LOGGER.noRecordDuringCompactReplay(id);
         } else {
            updateRecord.addUpdateFile(usedFile, size);
         }
      }

      @Override
      public String toString() {
         return "UpdateCompactCommand{" +
            "id=" + id +
            ", usedFile=" + usedFile +
            ", size=" + size +
            '}';
      }
   }

   private class CommitCompactCommand extends CompactCommand {

      private final JournalTransaction liveTransaction;

      /**
       * File containing the commit record
       */
      private final JournalFile commitFile;

      private CommitCompactCommand(final JournalTransaction liveTransaction, final JournalFile commitFile) {
         this.liveTransaction = liveTransaction;
         this.commitFile = commitFile;
      }

      @Override
      void execute() throws Exception {
         JournalTransaction newTransaction = newTransactions.get(liveTransaction.getId());
         if (newTransaction != null) {
            liveTransaction.merge(newTransaction);
            liveTransaction.commit(commitFile);
         }
         newTransactions.remove(liveTransaction.getId());
      }

      @Override
      public String toString() {
         return "CommitCompactCommand{" +
            "commitFile=" + commitFile +
            ", liveTransaction=" + liveTransaction +
            '}';
      }
   }

   private class RollbackCompactCommand extends CompactCommand {

      private final JournalTransaction liveTransaction;

      /**
       * File containing the commit record
       */
      private final JournalFile rollbackFile;

      private RollbackCompactCommand(final JournalTransaction liveTransaction, final JournalFile rollbackFile) {
         this.liveTransaction = liveTransaction;
         this.rollbackFile = rollbackFile;
      }

      @Override
      void execute() throws Exception {
         JournalTransaction newTransaction = newTransactions.get(liveTransaction.getId());
         if (newTransaction != null) {
            liveTransaction.merge(newTransaction);
            liveTransaction.rollback(rollbackFile);
         }
         newTransactions.remove(liveTransaction.getId());
      }

      @Override
      public String toString() {
         return "RollbackCompactCommand{" +
            "liveTransaction=" + liveTransaction +
            ", rollbackFile=" + rollbackFile +
            '}';
      }
   }

   @Override
   public JournalCompactor getCompactor() {
      return null;
   }

   @Override
   public ConcurrentLongHashMap<JournalRecord> getRecords() {
      return newRecords;
   }

}
