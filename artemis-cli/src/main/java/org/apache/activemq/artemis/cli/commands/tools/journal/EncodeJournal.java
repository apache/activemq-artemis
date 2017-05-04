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
package org.apache.activemq.artemis.cli.commands.tools.journal;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.tools.LockAbstract;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.journal.impl.JournalReaderCallback;
import org.apache.activemq.artemis.utils.Base64;

@Command(name = "encode", description = "Encode a set of journal files into an internal encoded data format")
public class EncodeJournal extends LockAbstract {

   @Option(name = "--directory", description = "The journal folder (default the journal folder from broker.xml)")
   public String directory;

   @Option(name = "--prefix", description = "The journal prefix (default activemq-data)")
   public String prefix = "activemq-data";

   @Option(name = "--suffix", description = "The journal suffix (default amq)")
   public String suffix = "amq";

   @Option(name = "--file-size", description = "The journal size (default 10485760)")
   public int size = 10485760;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      try {
         if (directory == null) {
            directory = getFileConfiguration().getJournalDirectory();
         }

         exportJournal(directory, prefix, suffix, 2, size);
      } catch (Exception e) {
         treatError(e, "data", "encode");
      }

      return null;
   }

   private static void exportJournal(final String directory,
                                     final String journalPrefix,
                                     final String journalSuffix,
                                     final int minFiles,
                                     final int fileSize) throws Exception {

      exportJournal(directory, journalPrefix, journalSuffix, minFiles, fileSize, System.out);
   }

   public static void exportJournal(final String directory,
                                    final String journalPrefix,
                                    final String journalSuffix,
                                    final int minFiles,
                                    final int fileSize,
                                    final String fileName) throws Exception {
      try (FileOutputStream fileOutputStream = new FileOutputStream(fileName);
           BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
           PrintStream out = new PrintStream(bufferedOutputStream)) {
         exportJournal(directory, journalPrefix, journalSuffix, minFiles, fileSize, out);
      }
   }

   public static void exportJournal(final String directory,
                                    final String journalPrefix,
                                    final String journalSuffix,
                                    final int minFiles,
                                    final int fileSize,
                                    final PrintStream out) throws Exception {
      NIOSequentialFileFactory nio = new NIOSequentialFileFactory(new File(directory), null, 1);

      JournalImpl journal = new JournalImpl(fileSize, minFiles, minFiles, 0, 0, nio, journalPrefix, journalSuffix, 1);

      List<JournalFile> files = journal.orderFiles();

      for (JournalFile file : files) {
         out.println("#File," + file);

         exportJournalFile(out, nio, file);
      }
   }

   private static void exportJournalFile(final PrintStream out,
                                         final SequentialFileFactory fileFactory,
                                         final JournalFile file) throws Exception {
      JournalImpl.readJournalFile(fileFactory, file, new JournalReaderCallback() {

         @Override
         public void onReadUpdateRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception {
            out.println("operation@UpdateTX,txID@" + transactionID + "," + describeRecord(recordInfo));
         }

         @Override
         public void onReadUpdateRecord(final RecordInfo recordInfo) throws Exception {
            out.println("operation@Update," + describeRecord(recordInfo));
         }

         @Override
         public void onReadRollbackRecord(final long transactionID) throws Exception {
            out.println("operation@Rollback,txID@" + transactionID);
         }

         @Override
         public void onReadPrepareRecord(final long transactionID,
                                         final byte[] extraData,
                                         final int numberOfRecords) throws Exception {
            out.println("operation@Prepare,txID@" + transactionID +
                           ",numberOfRecords@" +
                           numberOfRecords +
                           ",extraData@" +
                           encode(extraData));
         }

         @Override
         public void onReadDeleteRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception {
            out.println("operation@DeleteRecordTX,txID@" + transactionID +
                           "," +
                           describeRecord(recordInfo));
         }

         @Override
         public void onReadDeleteRecord(final long recordID) throws Exception {
            out.println("operation@DeleteRecord,id@" + recordID);
         }

         @Override
         public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception {
            out.println("operation@Commit,txID@" + transactionID + ",numberOfRecords@" + numberOfRecords);
         }

         @Override
         public void onReadAddRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception {
            out.println("operation@AddRecordTX,txID@" + transactionID + "," + describeRecord(recordInfo));
         }

         @Override
         public void onReadAddRecord(final RecordInfo recordInfo) throws Exception {
            out.println("operation@AddRecord," + describeRecord(recordInfo));
         }

         @Override
         public void markAsDataFile(final JournalFile file) {
         }
      });
   }

   private static String describeRecord(final RecordInfo recordInfo) {
      return "id@" + recordInfo.id +
         ",userRecordType@" +
         recordInfo.userRecordType +
         ",length@" +
         recordInfo.data.length +
         ",isUpdate@" +
         recordInfo.isUpdate +
         ",compactCount@" +
         recordInfo.compactCount +
         ",data@" +
         encode(recordInfo.data);
   }

   private static String encode(final byte[] data) {
      return Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

}
