/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.core.journal.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.activemq.core.journal.RecordInfo;
import org.apache.activemq.core.journal.SequentialFileFactory;
import org.apache.activemq.utils.Base64;

/**
 * Use this class to export the journal data. You can use it as a main class or through its native method {@link ExportJournal#exportJournal(String, String, String, int, int, String)}
 *
 * If you use the main method, use it as  <JournalDirectory> <JournalPrefix> <FileExtension> <MinFiles> <FileSize> <FileOutput>
 *
 * Example: java -cp hornetq-core.jar org.apache.activemq.core.journal.impl.ExportJournal /journalDir hornetq-data hq 2 10485760 /tmp/export.dat
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ExportJournal
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static void main(final String[] arg)
   {
      if (arg.length != 5)
      {
         System.err.println("Use: java -cp hornetq-core.jar org.apache.activemq.core.journal.impl.ExportJournal <JournalDirectory> <JournalPrefix> <FileExtension> <FileSize> <FileOutput>");
         return;
      }

      try
      {
         ExportJournal.exportJournal(arg[0], arg[1], arg[2], 2, Integer.parseInt(arg[3]), arg[4]);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

   }

   public static void exportJournal(final String directory,
                                    final String journalPrefix,
                                    final String journalSuffix,
                                    final int minFiles,
                                    final int fileSize,
                                    final String fileOutput) throws Exception
   {

      FileOutputStream fileOut = new FileOutputStream(new File(fileOutput));

      BufferedOutputStream buffOut = new BufferedOutputStream(fileOut);

      PrintStream out = new PrintStream(buffOut);

      ExportJournal.exportJournal(directory, journalPrefix, journalSuffix, minFiles, fileSize, out);

      out.close();
   }

   public static void exportJournal(final String directory,
                                    final String journalPrefix,
                                    final String journalSuffix,
                                    final int minFiles,
                                    final int fileSize,
                                    final PrintStream out) throws Exception
   {
      NIOSequentialFileFactory nio = new NIOSequentialFileFactory(directory, null);

      JournalImpl journal = new JournalImpl(fileSize, minFiles, 0, 0, nio, journalPrefix, journalSuffix, 1);

      List<JournalFile> files = journal.orderFiles();

      for (JournalFile file : files)
      {
         out.println("#File," + file);

         ExportJournal.exportJournalFile(out, nio, file);
      }
   }

   /**
    * @param out
    * @param fileFactory
    * @param file
    * @throws Exception
    */
   public static void exportJournalFile(final PrintStream out,
                                        final SequentialFileFactory fileFactory,
                                        final JournalFile file) throws Exception
   {
      JournalImpl.readJournalFile(fileFactory, file, new JournalReaderCallback()
      {

         public void onReadUpdateRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
         {
            out.println("operation@UpdateTX,txID@" + transactionID + "," + ExportJournal.describeRecord(recordInfo));
         }

         public void onReadUpdateRecord(final RecordInfo recordInfo) throws Exception
         {
            out.println("operation@Update," + ExportJournal.describeRecord(recordInfo));
         }

         public void onReadRollbackRecord(final long transactionID) throws Exception
         {
            out.println("operation@Rollback,txID@" + transactionID);
         }

         public void onReadPrepareRecord(final long transactionID, final byte[] extraData, final int numberOfRecords) throws Exception
         {
            out.println("operation@Prepare,txID@" + transactionID +
                        ",numberOfRecords@" +
                        numberOfRecords +
                        ",extraData@" +
                        ExportJournal.encode(extraData));
         }

         public void onReadDeleteRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
         {
            out.println("operation@DeleteRecordTX,txID@" + transactionID +
                        "," +
                        ExportJournal.describeRecord(recordInfo));
         }

         public void onReadDeleteRecord(final long recordID) throws Exception
         {
            out.println("operation@DeleteRecord,id@" + recordID);
         }

         public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception
         {
            out.println("operation@Commit,txID@" + transactionID + ",numberOfRecords@" + numberOfRecords);
         }

         public void onReadAddRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
         {
            out.println("operation@AddRecordTX,txID@" + transactionID + "," + ExportJournal.describeRecord(recordInfo));
         }

         public void onReadAddRecord(final RecordInfo recordInfo) throws Exception
         {
            out.println("operation@AddRecord," + ExportJournal.describeRecord(recordInfo));
         }

         public void markAsDataFile(final JournalFile file)
         {
         }
      });
   }

   private static String describeRecord(final RecordInfo recordInfo)
   {
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
             ExportJournal.encode(recordInfo.data);
   }

   private static String encode(final byte[] data)
   {
      return Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
