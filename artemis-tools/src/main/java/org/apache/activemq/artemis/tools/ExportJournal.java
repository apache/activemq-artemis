/**
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
package org.apache.activemq.artemis.tools;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.journal.impl.JournalReaderCallback;
import org.apache.activemq.artemis.core.journal.impl.NIOSequentialFileFactory;
import org.apache.activemq.artemis.utils.Base64;

/**
 * Use this class to export the journal data. You can use it as a main class or through its static method {@link #exportJournal(String, String, String, int, int, String)}
 * <p/>
 * If you use the main method, use it as  <JournalDirectory> <JournalPrefix> <FileExtension> <MinFiles> <FileSize> <FileOutput>
 * <p/>
 * Example: java -cp activemq-tools*-jar-with-dependencies.jar export-journal /journalDir activemq-data amq 2 10485760 /tmp/export.dat
 */
public class ExportJournal
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void process(final String[] arg)
   {
      if (arg.length != 6)
      {
         for (int i = 0; i < arg.length; i++)
         {
            System.out.println("arg[" + i + "] = " + arg[i]);
         }
         printUsage();
         System.exit(-1);
      }

      try
      {
         exportJournal(arg[1], arg[2], arg[3], 2, Integer.parseInt(arg[4]), arg[5]);
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

      exportJournal(directory, journalPrefix, journalSuffix, minFiles, fileSize, out);

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

         exportJournalFile(out, nio, file);
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
            out.println("operation@UpdateTX,txID@" + transactionID + "," + describeRecord(recordInfo));
         }

         public void onReadUpdateRecord(final RecordInfo recordInfo) throws Exception
         {
            out.println("operation@Update," + describeRecord(recordInfo));
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
                           encode(extraData));
         }

         public void onReadDeleteRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
         {
            out.println("operation@DeleteRecordTX,txID@" + transactionID +
                           "," +
                           describeRecord(recordInfo));
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
            out.println("operation@AddRecordTX,txID@" + transactionID + "," + describeRecord(recordInfo));
         }

         public void onReadAddRecord(final RecordInfo recordInfo) throws Exception
         {
            out.println("operation@AddRecord," + describeRecord(recordInfo));
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
         encode(recordInfo.data);
   }

   private static String encode(final byte[] data)
   {
      return Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }


   public void printUsage()
   {
      for (int i = 0; i < 10; i++)
      {
         System.err.println();
      }
      System.err.println("This method will export the journal at low level record.");
      System.err.println();
      System.err.println(Main.USAGE + " export-journal <JournalDirectory> <JournalPrefix> <FileExtension> <FileSize> <FileOutput>");
      System.err.println();
      for (int i = 0; i < 10; i++)
      {
         System.err.println();
      }
   }


}
