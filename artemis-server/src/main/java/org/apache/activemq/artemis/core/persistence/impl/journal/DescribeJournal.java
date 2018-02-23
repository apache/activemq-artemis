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

import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ACKNOWLEDGE_CURSOR;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ACKNOWLEDGE_REF;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ADDRESS_BINDING_RECORD;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ADDRESS_SETTING_RECORD;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ADD_LARGE_MESSAGE;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ADD_LARGE_MESSAGE_PENDING;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ADD_MESSAGE;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ADD_MESSAGE_PROTOCOL;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ADD_REF;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.DUPLICATE_ID;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.HEURISTIC_COMPLETION;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ID_COUNTER_RECORD;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.PAGE_CURSOR_COMPLETE;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.PAGE_CURSOR_COUNTER_INC;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.PAGE_CURSOR_PENDING_COUNTER;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.PAGE_TRANSACTION;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.QUEUE_BINDING_RECORD;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.QUEUE_STATUS_RECORD;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.SECURITY_RECORD;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.UPDATE_DELIVERY_COUNT;

import java.io.File;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.journal.impl.JournalReaderCallback;
import org.apache.activemq.artemis.core.paging.cursor.impl.PageSubscriptionCounterImpl;
import org.apache.activemq.artemis.core.paging.impl.PageTransactionInfoImpl;
import org.apache.activemq.artemis.core.persistence.impl.journal.BatchingIDGenerator.IDCounterEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.CursorAckRecordEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.DeliveryCountUpdateEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.DuplicateIDEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.HeuristicCompletionEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.LargeMessagePersister;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PageCountPendingImpl;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PageCountRecord;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PageCountRecordInc;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PageUpdateTXEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PendingLargeMessageEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PersistentAddressBindingEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PersistentQueueBindingEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.RefEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.ScheduledDeliveryEncoding;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.XidCodecSupport;


/**
 * Outputs a String description of the Journals contents.
 * <p>
 * Meant to be used in debugging.
 */
public final class DescribeJournal {

   private final List<RecordInfo> records;
   private final List<PreparedTransactionInfo> preparedTransactions;

   public DescribeJournal(List<RecordInfo> records, List<PreparedTransactionInfo> preparedTransactions) {
      this.records = records;
      this.preparedTransactions = preparedTransactions;
   }

   public List<RecordInfo> getRecords() {
      return records;
   }

   public List<PreparedTransactionInfo> getPreparedTransactions() {
      return preparedTransactions;
   }

   public static void describeBindingsJournal(final File bindingsDir) throws Exception {
      describeBindingsJournal(bindingsDir, System.out, false);
   }

   public static void describeBindingsJournal(final File bindingsDir, PrintStream out, boolean safe) throws Exception {

      SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir, null, 1);

      JournalImpl bindings = new JournalImpl(1024 * 1024, 2, 2, -1, 0, bindingsFF, "activemq-bindings", "bindings", 1);
      describeJournal(bindingsFF, bindings, bindingsDir, out, safe);
   }

   public static DescribeJournal describeMessagesJournal(final File messagesDir) throws Exception {
      return describeMessagesJournal(messagesDir, System.out, false);
   }

   public static DescribeJournal describeMessagesJournal(final File messagesDir, PrintStream out, boolean safe) throws Exception {
      SequentialFileFactory messagesFF = new NIOSequentialFileFactory(messagesDir, null, 1);

      // Will use only default values. The load function should adapt to anything different
      ConfigurationImpl defaultValues = new ConfigurationImpl();

      JournalImpl messagesJournal = new JournalImpl(defaultValues.getJournalFileSize(), defaultValues.getJournalMinFiles(), defaultValues.getJournalPoolFiles(), 0, 0, messagesFF, "activemq-data", "amq", 1);

      return describeJournal(messagesFF, messagesJournal, messagesDir, out, safe);
   }

   /**
    * @param fileFactory
    * @param journal
    * @throws Exception
    */
   private static DescribeJournal describeJournal(SequentialFileFactory fileFactory,
                                                  JournalImpl journal,
                                                  final File path,
                                                  PrintStream out,
                                                  boolean safe) throws Exception {
      List<JournalFile> files = journal.orderFiles();

      final Map<Long, PageSubscriptionCounterImpl> counters = new HashMap<>();

      out.println("Journal path: " + path);

      for (JournalFile file : files) {
         out.println("#" + file + " (size=" + file.getFile().size() + ")");

         JournalImpl.readJournalFile(fileFactory, file, new JournalReaderCallback() {

            @Override
            public void onReadUpdateRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception {
               out.println("operation@UpdateTX;txID=" + transactionID + "," + describeRecord(recordInfo, safe));
               checkRecordCounter(recordInfo);
            }

            @Override
            public void onReadUpdateRecord(final RecordInfo recordInfo) throws Exception {
               out.println("operation@Update;" + describeRecord(recordInfo, safe));
               checkRecordCounter(recordInfo);
            }

            @Override
            public void onReadRollbackRecord(final long transactionID) throws Exception {
               out.println("operation@Rollback;txID=" + transactionID);
            }

            @Override
            public void onReadPrepareRecord(final long transactionID,
                                            final byte[] extraData,
                                            final int numberOfRecords) throws Exception {
               out.println("operation@Prepare,txID=" + transactionID + ",numberOfRecords=" + numberOfRecords +
                              ",extraData=" + encode(extraData) + ", xid=" + toXid(extraData));
            }

            @Override
            public void onReadDeleteRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception {
               out.println("operation@DeleteRecordTX;txID=" + transactionID + "," + describeRecord(recordInfo, safe));
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
               out.println("operation@AddRecordTX;txID=" + transactionID + "," + describeRecord(recordInfo, safe));
            }

            @Override
            public void onReadAddRecord(final RecordInfo recordInfo) throws Exception {
               out.println("operation@AddRecord;" + describeRecord(recordInfo, safe));
            }

            @Override
            public void markAsDataFile(final JournalFile file1) {
            }

            public void checkRecordCounter(RecordInfo info) {
               if (info.getUserRecordType() == JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE) {
                  PageCountRecord encoding = (PageCountRecord) newObjectEncoding(info);
                  long queueIDForCounter = encoding.getQueueID();

                  PageSubscriptionCounterImpl subsCounter = lookupCounter(counters, queueIDForCounter);

                  if (subsCounter.getValue() != 0 && subsCounter.getValue() != encoding.getValue()) {
                     out.println("####### Counter replace wrongly on queue " + queueIDForCounter + " oldValue=" + subsCounter.getValue() + " newValue=" + encoding.getValue());
                  }

                  subsCounter.loadValue(info.id, encoding.getValue(), encoding.getPersistentSize());
                  subsCounter.processReload();
                  out.print("#Counter queue " + queueIDForCounter + " value=" + subsCounter.getValue() + " persistentSize=" + subsCounter.getPersistentSize() + ", result=" + subsCounter.getValue());
                  if (subsCounter.getValue() < 0) {
                     out.println(" #NegativeCounter!!!!");
                  } else {
                     out.println();
                  }
                  out.println();
               } else if (info.getUserRecordType() == JournalRecordIds.PAGE_CURSOR_COUNTER_INC) {
                  PageCountRecordInc encoding = (PageCountRecordInc) newObjectEncoding(info);
                  long queueIDForCounter = encoding.getQueueID();

                  PageSubscriptionCounterImpl subsCounter = lookupCounter(counters, queueIDForCounter);

                  subsCounter.loadInc(info.id, encoding.getValue(), encoding.getPersistentSize());
                  subsCounter.processReload();
                  out.print("#Counter queue " + queueIDForCounter + " value=" + subsCounter.getValue() + " persistentSize=" + subsCounter.getPersistentSize() + " increased by " + encoding.getValue());
                  if (subsCounter.getValue() < 0) {
                     out.println(" #NegativeCounter!!!!");
                  } else {
                     out.println();
                  }
                  out.println();
               }
            }
         });
      }

      out.println();

      if (counters.size() != 0) {
         out.println("#Counters during initial load:");
         printCounters(out, counters);
      }

      return printSurvivingRecords(journal, out, safe);
   }

   public static DescribeJournal printSurvivingRecords(Journal journal,
                                                       PrintStream out,
                                                       boolean safe) throws Exception {

      final Map<Long, PageSubscriptionCounterImpl> counters = new HashMap<>();
      out.println("### Surviving Records Summary ###");

      List<RecordInfo> records = new LinkedList<>();
      List<PreparedTransactionInfo> preparedTransactions = new LinkedList<>();

      journal.start();

      final StringBuffer bufferFailingTransactions = new StringBuffer();

      int messageCount = 0;
      Map<Long, Integer> messageRefCounts = new HashMap<>();
      int preparedMessageCount = 0;
      Map<Long, Integer> preparedMessageRefCount = new HashMap<>();
      journal.load(records, preparedTransactions, new TransactionFailureCallback() {

         @Override
         public void failedTransaction(long transactionID,
                                       List<RecordInfo> records1,
                                       List<RecordInfo> recordsToDelete) {
            bufferFailingTransactions.append("Transaction " + transactionID + " failed with these records:\n");
            for (RecordInfo info : records1) {
               bufferFailingTransactions.append("- " + describeRecord(info, safe) + "\n");
            }

            for (RecordInfo info : recordsToDelete) {
               bufferFailingTransactions.append("- " + describeRecord(info, safe) + " <marked to delete>\n");
            }

         }
      }, false);

      for (RecordInfo info : records) {
         PageSubscriptionCounterImpl subsCounter = null;
         long queueIDForCounter = 0;

         Object o = newObjectEncoding(info);
         if (info.getUserRecordType() == JournalRecordIds.ADD_MESSAGE) {
            messageCount++;
         } else if (info.getUserRecordType() == JournalRecordIds.ADD_REF) {
            ReferenceDescribe ref = (ReferenceDescribe) o;
            Integer count = messageRefCounts.get(ref.refEncoding.queueID);
            if (count == null) {
               count = 1;
               messageRefCounts.put(ref.refEncoding.queueID, count);
            } else {
               messageRefCounts.put(ref.refEncoding.queueID, count + 1);
            }
         } else if (info.getUserRecordType() == JournalRecordIds.ACKNOWLEDGE_REF) {
            AckDescribe ref = (AckDescribe) o;
            Integer count = messageRefCounts.get(ref.refEncoding.queueID);
            if (count == null) {
               messageRefCounts.put(ref.refEncoding.queueID, 0);
            } else {
               messageRefCounts.put(ref.refEncoding.queueID, count - 1);
            }
         } else if (info.getUserRecordType() == JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE) {
            PageCountRecord encoding = (PageCountRecord) o;
            queueIDForCounter = encoding.getQueueID();

            subsCounter = lookupCounter(counters, queueIDForCounter);

            subsCounter.loadValue(info.id, encoding.getValue(), encoding.getPersistentSize());
            subsCounter.processReload();
         } else if (info.getUserRecordType() == JournalRecordIds.PAGE_CURSOR_COUNTER_INC) {
            PageCountRecordInc encoding = (PageCountRecordInc) o;
            queueIDForCounter = encoding.getQueueID();

            subsCounter = lookupCounter(counters, queueIDForCounter);

            subsCounter.loadInc(info.id, encoding.getValue(), encoding.getPersistentSize());
            subsCounter.processReload();
         }

         out.println(describeRecord(info, o, safe));

         if (subsCounter != null) {
            out.println("##SubsCounter for queue=" + queueIDForCounter + ", value=" + subsCounter.getValue());
            out.println();
         }
      }

      if (counters.size() > 0) {
         out.println("### Page Counters");
         printCounters(out, counters);
      }

      out.println();
      out.println("### Prepared TX ###");

      for (PreparedTransactionInfo tx : preparedTransactions) {
         out.println(tx.getId());
         for (RecordInfo info : tx.getRecords()) {
            Object o = newObjectEncoding(info);
            out.println("- " + describeRecord(info, o, safe));
            if (info.getUserRecordType() == 31) {
               preparedMessageCount++;
            } else if (info.getUserRecordType() == 32) {
               ReferenceDescribe ref = (ReferenceDescribe) o;
               Integer count = preparedMessageRefCount.get(ref.refEncoding.queueID);
               if (count == null) {
                  count = 1;
                  preparedMessageRefCount.put(ref.refEncoding.queueID, count);
               } else {
                  preparedMessageRefCount.put(ref.refEncoding.queueID, count + 1);
               }
            }
         }

         for (RecordInfo info : tx.getRecordsToDelete()) {
            out.println("- " + describeRecord(info, safe) + " <marked to delete>");
         }
      }

      String missingTX = bufferFailingTransactions.toString();

      if (missingTX.length() > 0) {
         out.println();
         out.println("### Failed Transactions (Missing commit/prepare/rollback record) ###");
      }

      out.println(bufferFailingTransactions.toString());

      out.println("### Message Counts ###");
      out.println("message count=" + messageCount);
      out.println("message reference count");
      for (Map.Entry<Long, Integer> longIntegerEntry : messageRefCounts.entrySet()) {
         out.println("queue id " + longIntegerEntry.getKey() + ",count=" + longIntegerEntry.getValue());
      }

      out.println("prepared message count=" + preparedMessageCount);

      for (Map.Entry<Long, Integer> longIntegerEntry : preparedMessageRefCount.entrySet()) {
         out.println("queue id " + longIntegerEntry.getKey() + ",count=" + longIntegerEntry.getValue());
      }

      journal.stop();

      return new DescribeJournal(records, preparedTransactions);
   }

   protected static void printCounters(final PrintStream out, final Map<Long, PageSubscriptionCounterImpl> counters) {
      for (Map.Entry<Long, PageSubscriptionCounterImpl> entry : counters.entrySet()) {
         out.println("Queue " + entry.getKey() + " value=" + entry.getValue().getValue());
      }
   }

   protected static PageSubscriptionCounterImpl lookupCounter(Map<Long, PageSubscriptionCounterImpl> counters,
                                                              long queueIDForCounter) {
      PageSubscriptionCounterImpl subsCounter;
      subsCounter = counters.get(queueIDForCounter);
      if (subsCounter == null) {
         subsCounter = new PageSubscriptionCounterImpl(null, null, null, false, -1);
         counters.put(queueIDForCounter, subsCounter);
      }
      return subsCounter;
   }

   private static boolean isSafe(Object obj) {
      // these classes will have user's data and not considered safe
      return !(obj instanceof PersistentAddressBindingEncoding ||
              obj instanceof MessageDescribe ||
              obj instanceof PersistentQueueBindingEncoding);
   }

   private static String toString(Object obj, boolean safe) {
      if (obj == null) {
         return "** null **";
      }
      if (safe && !isSafe(obj)) {
         if (obj instanceof MessageDescribe) {
            MessageDescribe describe = (MessageDescribe)obj;
            try {
               return describe.getMsg().getClass().getSimpleName() + "(safe data, size=" + describe.getMsg().getPersistentSize() + ")";
            } catch (Throwable e) {
               e.printStackTrace();
               return describe.getMsg().getClass().getSimpleName() + "(safe data)";
            }
         } else {
            return obj.getClass().getSimpleName() + "(safe data)";
         }
      } else {
         return obj.toString();
      }
   }

   private static String describeRecord(RecordInfo info, boolean safe) {
      return "recordID=" + info.id + ";userRecordType=" + info.userRecordType + ";isUpdate=" + info.isUpdate + ";compactCount=" + info.compactCount + ";" + toString(newObjectEncoding(info), safe);
   }

   private static String describeRecord(RecordInfo info, Object o, boolean safe) {
      return "recordID=" + info.id + ";userRecordType=" + info.userRecordType + ";isUpdate=" + info.isUpdate + ";compactCount=" + info.compactCount + ";" + toString(o, safe);
   }

   private static String encode(final byte[] data) {
      return Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   private static Xid toXid(final byte[] data) {
      try {
         return XidCodecSupport.decodeXid(ActiveMQBuffers.wrappedBuffer(data));
      } catch (Exception e) {
         return null;
      }
   }

   public static Object newObjectEncoding(RecordInfo info) {
      return newObjectEncoding(info, null);
   }

   public static Object newObjectEncoding(RecordInfo info, JournalStorageManager storageManager) {
      ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(info.data);
      long id = info.id;
      int rec = info.getUserRecordType();

      switch (rec) {
         case ADD_LARGE_MESSAGE_PENDING: {
            PendingLargeMessageEncoding lmEncoding = new PendingLargeMessageEncoding();
            lmEncoding.decode(buffer);

            return lmEncoding;
         }
         case ADD_LARGE_MESSAGE: {

            LargeServerMessage largeMessage = new LargeServerMessageImpl(storageManager);

            LargeMessagePersister.getInstance().decode(buffer, largeMessage);

            return new MessageDescribe(largeMessage);
         }
         case ADD_MESSAGE: {
            return "ADD-MESSAGE is not supported any longer, use export/import";
         }
         case ADD_MESSAGE_PROTOCOL: {
            Message message = MessagePersister.getInstance().decode(buffer, null);

            return new MessageDescribe(message);
         }
         case ADD_REF: {
            final RefEncoding encoding = new RefEncoding();
            encoding.decode(buffer);
            return new ReferenceDescribe(encoding);
         }

         case ACKNOWLEDGE_REF: {
            final RefEncoding encoding = new RefEncoding();
            encoding.decode(buffer);
            return new AckDescribe(encoding);
         }

         case UPDATE_DELIVERY_COUNT: {
            DeliveryCountUpdateEncoding updateDeliveryCount = new DeliveryCountUpdateEncoding();
            updateDeliveryCount.decode(buffer);
            return updateDeliveryCount;
         }

         case PAGE_TRANSACTION: {
            if (info.isUpdate) {
               PageUpdateTXEncoding pageUpdate = new PageUpdateTXEncoding();

               pageUpdate.decode(buffer);

               return pageUpdate;
            } else {
               PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

               pageTransactionInfo.decode(buffer);

               pageTransactionInfo.setRecordID(info.id);

               return pageTransactionInfo;
            }
         }

         case SET_SCHEDULED_DELIVERY_TIME: {
            ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case DUPLICATE_ID: {
            DuplicateIDEncoding encoding = new DuplicateIDEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case HEURISTIC_COMPLETION: {
            HeuristicCompletionEncoding encoding = new HeuristicCompletionEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case ACKNOWLEDGE_CURSOR: {
            CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case PAGE_CURSOR_COUNTER_VALUE: {
            PageCountRecord encoding = new PageCountRecord();

            encoding.decode(buffer);

            return encoding;
         }

         case PAGE_CURSOR_COMPLETE: {
            CursorAckRecordEncoding encoding = new PageCompleteCursorAckRecordEncoding();

            encoding.decode(buffer);

            return encoding;
         }

         case PAGE_CURSOR_COUNTER_INC: {
            PageCountRecordInc encoding = new PageCountRecordInc();

            encoding.decode(buffer);

            return encoding;
         }

         case PAGE_CURSOR_PENDING_COUNTER: {
            PageCountPendingImpl encoding = new PageCountPendingImpl();
            encoding.decode(buffer);
            encoding.setID(info.id);
            return encoding;
         }

         case QUEUE_STATUS_RECORD:
            return AbstractJournalStorageManager.newQueueStatusEncoding(id, buffer);

         case QUEUE_BINDING_RECORD:
            return AbstractJournalStorageManager.newQueueBindingEncoding(id, buffer);

         case ID_COUNTER_RECORD:
            EncodingSupport idReturn = new IDCounterEncoding();
            idReturn.decode(buffer);

            return idReturn;

         case JournalRecordIds.GROUP_RECORD:
            return AbstractJournalStorageManager.newGroupEncoding(id, buffer);

         case ADDRESS_SETTING_RECORD:
            return AbstractJournalStorageManager.newAddressEncoding(id, buffer);

         case SECURITY_RECORD:
            return AbstractJournalStorageManager.newSecurityRecord(id, buffer);

         case ADDRESS_BINDING_RECORD:
            return AbstractJournalStorageManager.newAddressBindingEncoding(id, buffer);

         default:
            return null;
      }
   }

   private static final class PageCompleteCursorAckRecordEncoding extends CursorAckRecordEncoding {

      @Override
      public String toString() {
         return "PGComplete [queueID=" + queueID + ", position=" + position + "]";
      }
   }

   public static final class MessageDescribe {

      public MessageDescribe(Message msg) {
         this.msg = msg;
      }

      Message msg;

      @Override
      public String toString() {
         StringBuffer buffer = new StringBuffer();
         buffer.append(msg.isLargeMessage() ? "LargeMessage(" : "Message(");
         buffer.append("messageID=" + msg.getMessageID());
         if (msg.getUserID() != null) {
            buffer.append(";userMessageID=" + msg.getUserID().toString());
         }

         buffer.append(";msg=" + msg.toString());

         return buffer.toString();
      }

      public Message getMsg() {
         return msg;
      }

   }

   public static final class ReferenceDescribe {

      public RefEncoding refEncoding;

      public ReferenceDescribe(RefEncoding refEncoding) {
         this.refEncoding = refEncoding;
      }

      @Override
      public String toString() {
         return "AddRef;" + refEncoding;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((refEncoding == null) ? 0 : refEncoding.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (!(obj instanceof ReferenceDescribe))
            return false;
         ReferenceDescribe other = (ReferenceDescribe) obj;
         if (refEncoding == null) {
            if (other.refEncoding != null)
               return false;
         } else if (!refEncoding.equals(other.refEncoding))
            return false;
         return true;
      }
   }

}
