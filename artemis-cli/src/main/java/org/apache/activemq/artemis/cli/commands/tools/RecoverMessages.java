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
package org.apache.activemq.artemis.cli.commands.tools;

import java.io.File;
import java.util.HashSet;
import java.util.List;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.journal.impl.JournalReaderCallback;
import org.apache.activemq.artemis.core.message.impl.CoreMessagePersister;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.utils.ByteUtil;

@Command(name = "recover", description = "Recover (undelete) every message on the journal by creating a new output journal. Rolled backed and acked messages will be sent out to the output as much as possible.")
public class RecoverMessages extends DBOption {

   static {
      MessagePersister.registerPersister(CoreMessagePersister.getInstance());
   }

   @Option(name = "--reclaimed", description = "This option will try to recover as many records as possible from reclaimed files")
   private boolean reclaimed = false;

   @Option(name = "--target", description = "Output folder container the new journal with all the generated messages", required = true)
   private String outputJournal;


   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      Configuration configuration = getParameterConfiguration();

      File journalOutput = new File(outputJournal);
      journalOutput.mkdirs();

      if (!journalOutput.isDirectory()) {
         throw new IllegalStateException(outputJournal + " is not a directory");
      }

      try {
         if (configuration.isJDBC()) {
            throw new IllegalAccessException("JDBC Not supported on recover");
         } else {
            recover(configuration, journalOutput, reclaimed);
         }
      } catch (Exception e) {
         treatError(e, "data", "print");
      }
      return null;
   }

   public static void recover(Configuration configuration, File journalOutput, boolean reclaimed) throws Exception {

      File journal = configuration.getJournalLocation();

      journalOutput.mkdirs();

      SequentialFileFactory outputFF = new NIOSequentialFileFactory(journalOutput, null, 1);
      outputFF.setDatasync(false);
      JournalImpl targetJournal = new JournalImpl(configuration.getJournalFileSize(), 2, 2, 0, 0, outputFF, "activemq-data", "amq", 1);

      targetJournal.start();
      targetJournal.loadInternalOnly();

      SequentialFileFactory messagesFF = new NIOSequentialFileFactory(journal, null, 1);

      // Will use only default values. The load function should adapt to anything different
      JournalImpl messagesJournal = new JournalImpl(configuration.getJournalFileSize(), configuration.getJournalMinFiles(), configuration.getJournalPoolFiles(), 0, 0, messagesFF, "activemq-data", "amq", 1);

      List<JournalFile> files = messagesJournal.orderFiles();

      HashSet<Byte> userRecordsOfInterest = new HashSet<>();
      userRecordsOfInterest.add(JournalRecordIds.ADD_LARGE_MESSAGE);
      userRecordsOfInterest.add(JournalRecordIds.ADD_MESSAGE);
      userRecordsOfInterest.add(JournalRecordIds.ADD_MESSAGE_PROTOCOL);
      userRecordsOfInterest.add(JournalRecordIds.ADD_REF);
      userRecordsOfInterest.add(JournalRecordIds.PAGE_TRANSACTION);

      for (JournalFile file : files) {
         System.out.println("Recovering messages from file " + file);

         HashSet<Pair<Long, Long>> routeBindigns = new HashSet<>();

         JournalImpl.readJournalFile(messagesFF, file, new JournalReaderCallback() {
            @Override
            public void onReadAddRecord(RecordInfo info) throws Exception {
               if (userRecordsOfInterest.contains(info.getUserRecordType())) {

                  if (targetJournal.getRecords().get(info.id) != null) {
                     System.out.println("RecordID " + info.id + " would been duplicated, ignoring it");
                     return;
                  }
                  try {
                     targetJournal.appendAddRecord(info.id, info.userRecordType, info.data, true);
                  } catch (Exception e) {
                     System.out.println("Cannot append record for " + info.id + "->" + e.getMessage());
                  }
               }
            }

            @Override
            public void onReadUpdateRecord(RecordInfo info) throws Exception {
               if (userRecordsOfInterest.contains(info.getUserRecordType())) {
                  if (info.getUserRecordType() == JournalRecordIds.ADD_REF) {
                     long queue = ByteUtil.bytesToLong(info.data);
                     Pair<Long, Long> pairQueue = new Pair<>(info.id, queue);
                     if (routeBindigns.contains(pairQueue)) {
                        System.out.println("AddRef on " + info.id + " / queue=" + queue + " has already been recorded, ignoring it");
                        return;
                     }

                     routeBindigns.add(pairQueue);
                  }
                  try {
                     targetJournal.appendUpdateRecord(info.id, info.userRecordType, info.data, true);
                  } catch (Exception e) {
                     System.out.println("Cannot update record " + info.id + "-> " + e.getMessage());
                  }
               }
            }

            @Override
            public void onReadDeleteRecord(long recordID) throws Exception {

            }

            @Override
            public void onReadAddRecordTX(long transactionID, RecordInfo info) throws Exception {
               onReadAddRecord(info);
            }

            @Override
            public void onReadUpdateRecordTX(long transactionID, RecordInfo info) throws Exception {
               onReadUpdateRecord(info);
            }

            @Override
            public void onReadDeleteRecordTX(long transactionID, RecordInfo recordInfo) throws Exception {

            }

            @Override
            public void onReadPrepareRecord(long transactionID,
                                            byte[] extraData,
                                            int numberOfRecords) throws Exception {

            }

            @Override
            public void onReadCommitRecord(long transactionID, int numberOfRecords) throws Exception {

            }

            @Override
            public void onReadRollbackRecord(long transactionID) throws Exception {

            }

            @Override
            public void markAsDataFile(JournalFile file) {

            }
         }, null, reclaimed);
      }

      targetJournal.flush();

      targetJournal.stop();
      outputFF.stop();
   }



}
