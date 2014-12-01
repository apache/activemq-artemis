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
package org.apache.activemq.core.journal.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.core.journal.RecordInfo;
import org.apache.activemq.utils.Base64;

/**
 * Use this class to import the journal data from a listed file. You can use it as a main class or
 * through its native method
 * {@link ImportJournal#importJournal(String, String, String, int, int, String)}
 * <p>
 * If you use the main method, use its arguments as:
 *
 * <pre>
 * JournalDirectory JournalPrefix FileExtension MinFiles FileSize FileOutput
 * </pre>
 * <p>
 * Example:
 *
 * <pre>
 * java -cp activemq-core.jar org.apache.activemq.core.journal.impl.ExportJournal /journalDir activemq-data amq 2 10485760 /tmp/export.dat
 * </pre>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ImportJournal
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
         System.err.println("Use: java -cp activemq-core.jar:netty.jar org.apache.activemq.core.journal.impl.ImportJournal <JournalDirectory> <JournalPrefix> <FileExtension> <FileSize> <FileOutput>");
         return;
      }

      try
      {
         ImportJournal.importJournal(arg[0], arg[1], arg[2], 2, Integer.parseInt(arg[3]), arg[4]);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

   }

   public static void importJournal(final String directory,
                                    final String journalPrefix,
                                    final String journalSuffix,
                                    final int minFiles,
                                    final int fileSize,
                                    final String fileInput) throws Exception
   {
      FileInputStream fileInputStream = new FileInputStream(new File(fileInput));
      ImportJournal.importJournal(directory, journalPrefix, journalSuffix, minFiles, fileSize, fileInputStream);

   }

   public static void importJournal(final String directory,
                                    final String journalPrefix,
                                    final String journalSuffix,
                                    final int minFiles,
                                    final int fileSize,
                                    final InputStream stream) throws Exception
   {
      Reader reader = new InputStreamReader(stream);
      ImportJournal.importJournal(directory, journalPrefix, journalSuffix, minFiles, fileSize, reader);
   }

   public static void importJournal(final String directory,
                                    final String journalPrefix,
                                    final String journalSuffix,
                                    final int minFiles,
                                    final int fileSize,
                                    final Reader reader) throws Exception
   {

      File journalDir = new File(directory);

      if (!journalDir.exists())
      {
         if (!journalDir.mkdirs())
            System.err.println("Could not create directory " + directory);
      }

      NIOSequentialFileFactory nio = new NIOSequentialFileFactory(directory, null);

      JournalImpl journal = new JournalImpl(fileSize, minFiles, 0, 0, nio, journalPrefix, journalSuffix, 1);

      if (journal.orderFiles().size() != 0)
      {
         throw new IllegalStateException("Import needs to create a brand new journal");
      }

      journal.start();

      // The journal is empty, as we checked already. Calling load just to initialize the internal data
      journal.loadInternalOnly();

      BufferedReader buffReader = new BufferedReader(reader);

      String line;

      HashMap<Long, AtomicInteger> txCounters = new HashMap<Long, AtomicInteger>();

      long lineNumber = 0;

      Map<Long, JournalRecord> journalRecords = journal.getRecords();

      while ((line = buffReader.readLine()) != null)
      {
         lineNumber++;
         String[] splitLine = line.split(",");
         if (splitLine[0].equals("#File"))
         {
            txCounters.clear();
            continue;
         }

         Properties lineProperties = ImportJournal.parseLine(splitLine);

         String operation = null;
         try
         {
            operation = lineProperties.getProperty("operation");

            if (operation.equals("AddRecord"))
            {
               RecordInfo info = ImportJournal.parseRecord(lineProperties);
               journal.appendAddRecord(info.id, info.userRecordType, info.data, false);
            }
            else if (operation.equals("AddRecordTX"))
            {
               long txID = ImportJournal.parseLong("txID", lineProperties);
               AtomicInteger counter = ImportJournal.getCounter(txID, txCounters);
               counter.incrementAndGet();
               RecordInfo info = ImportJournal.parseRecord(lineProperties);
               journal.appendAddRecordTransactional(txID, info.id, info.userRecordType, info.data);
            }
            else if (operation.equals("AddRecordTX"))
            {
               long txID = ImportJournal.parseLong("txID", lineProperties);
               AtomicInteger counter = ImportJournal.getCounter(txID, txCounters);
               counter.incrementAndGet();
               RecordInfo info = ImportJournal.parseRecord(lineProperties);
               journal.appendAddRecordTransactional(txID, info.id, info.userRecordType, info.data);
            }
            else if (operation.equals("UpdateTX"))
            {
               long txID = ImportJournal.parseLong("txID", lineProperties);
               AtomicInteger counter = ImportJournal.getCounter(txID, txCounters);
               counter.incrementAndGet();
               RecordInfo info = ImportJournal.parseRecord(lineProperties);
               journal.appendUpdateRecordTransactional(txID, info.id, info.userRecordType, info.data);
            }
            else if (operation.equals("Update"))
            {
               RecordInfo info = ImportJournal.parseRecord(lineProperties);
               journal.appendUpdateRecord(info.id, info.userRecordType, info.data, false);
            }
            else if (operation.equals("DeleteRecord"))
            {
               long id = ImportJournal.parseLong("id", lineProperties);

               // If not found it means the append/update records were reclaimed already
               if (journalRecords.get(id) != null)
               {
                  journal.appendDeleteRecord(id, false);
               }
            }
            else if (operation.equals("DeleteRecordTX"))
            {
               long txID = ImportJournal.parseLong("txID", lineProperties);
               long id = ImportJournal.parseLong("id", lineProperties);
               AtomicInteger counter = ImportJournal.getCounter(txID, txCounters);
               counter.incrementAndGet();

               // If not found it means the append/update records were reclaimed already
               if (journalRecords.get(id) != null)
               {
                  journal.appendDeleteRecordTransactional(txID, id);
               }
            }
            else if (operation.equals("Prepare"))
            {
               long txID = ImportJournal.parseLong("txID", lineProperties);
               int numberOfRecords = ImportJournal.parseInt("numberOfRecords", lineProperties);
               AtomicInteger counter = ImportJournal.getCounter(txID, txCounters);
               byte[] data = ImportJournal.parseEncoding("extraData", lineProperties);

               if (counter.get() == numberOfRecords)
               {
                  journal.appendPrepareRecord(txID, data, false);
               }
               else
               {
                  System.err.println("Transaction " + txID +
                                     " at line " +
                                     lineNumber +
                                     " is incomplete. The prepare record expected " +
                                     numberOfRecords +
                                     " while the import only had " +
                                     counter);
               }
            }
            else if (operation.equals("Commit"))
            {
               long txID = ImportJournal.parseLong("txID", lineProperties);
               int numberOfRecords = ImportJournal.parseInt("numberOfRecords", lineProperties);
               AtomicInteger counter = ImportJournal.getCounter(txID, txCounters);
               if (counter.get() == numberOfRecords)
               {
                  journal.appendCommitRecord(txID, false);
               }
               else
               {
                  System.err.println("Transaction " + txID +
                                     " at line " +
                                     lineNumber +
                                     " is incomplete. The commit record expected " +
                                     numberOfRecords +
                                     " while the import only had " +
                                     counter);
               }
            }
            else if (operation.equals("Rollback"))
            {
               long txID = ImportJournal.parseLong("txID", lineProperties);
               journal.appendRollbackRecord(txID, false);
            }
            else
            {
               System.err.println("Invalid opeartion " + operation + " at line " + lineNumber);
            }
         }
         catch (Exception ex)
         {
            System.err.println("Error at line " + lineNumber + ", operation=" + operation + " msg = " + ex.getMessage());
         }
      }

      journal.stop();
   }

   protected static AtomicInteger getCounter(final Long txID, final Map<Long, AtomicInteger> txCounters)
   {

      AtomicInteger counter = txCounters.get(txID);
      if (counter == null)
      {
         counter = new AtomicInteger(0);
         txCounters.put(txID, counter);
      }

      return counter;
   }

   protected static RecordInfo parseRecord(final Properties properties) throws Exception
   {
      long id = ImportJournal.parseLong("id", properties);
      byte userRecordType = ImportJournal.parseByte("userRecordType", properties);
      boolean isUpdate = ImportJournal.parseBoolean("isUpdate", properties);
      byte[] data = ImportJournal.parseEncoding("data", properties);
      return new RecordInfo(id, userRecordType, data, isUpdate, (short)0);
   }

   private static byte[] parseEncoding(final String name, final Properties properties) throws Exception
   {
      String value = ImportJournal.parseString(name, properties);

      return ImportJournal.decode(value);
   }

   /**
    * @param properties
    * @return
    */
   private static int parseInt(final String name, final Properties properties) throws Exception
   {
      String value = ImportJournal.parseString(name, properties);

      return Integer.parseInt(value);
   }

   private static long parseLong(final String name, final Properties properties) throws Exception
   {
      String value = ImportJournal.parseString(name, properties);

      return Long.parseLong(value);
   }

   private static boolean parseBoolean(final String name, final Properties properties) throws Exception
   {
      String value = ImportJournal.parseString(name, properties);

      return Boolean.parseBoolean(value);
   }

   private static byte parseByte(final String name, final Properties properties) throws Exception
   {
      String value = ImportJournal.parseString(name, properties);

      return Byte.parseByte(value);
   }

   /**
    * @param name
    * @param properties
    * @return
    * @throws Exception
    */
   private static String parseString(final String name, final Properties properties) throws Exception
   {
      String value = properties.getProperty(name);

      if (value == null)
      {
         throw new Exception("property " + name + " not found");
      }
      return value;
   }

   protected static Properties parseLine(final String[] splitLine)
   {
      Properties properties = new Properties();

      for (String el : splitLine)
      {
         String[] tuple = el.split("@");
         if (tuple.length == 2)
         {
            properties.put(tuple[0], tuple[1]);
         }
         else
         {
            properties.put(tuple[0], tuple[0]);
         }
      }

      return properties;
   }

   private static byte[] decode(final String data)
   {
      return Base64.decode(data, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
