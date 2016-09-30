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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.journal.impl.JournalRecord;
import org.apache.activemq.artemis.utils.Base64;

@Command(name = "decode", description = "Decode a journal's internal format into a new journal set of files")
public class DecodeJournal extends LockAbstract {

   @Option(name = "--directory", description = "The journal folder (default journal folder from broker.xml)")
   public String directory;

   @Option(name = "--prefix", description = "The journal prefix (default activemq-data)")
   public String prefix = "activemq-data";

   @Option(name = "--suffix", description = "The journal suffix (default amq)")
   public String suffix = "amq";

   @Option(name = "--file-size", description = "The journal size (default 10485760)")
   public int size = 10485760;

   @Option(name = "--input", description = "The input file name (default=exp.dmp)", required = true)
   public String input = "exp.dmp";

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      try {
         if (directory == null) {
            directory = getFileConfiguration().getJournalDirectory();
         }
         importJournal(directory, prefix, suffix, 2, size, input);
      } catch (Exception e) {
         treatError(e, "data", "decode");
      }

      return null;
   }

   public static void importJournal(final String directory,
                                    final String journalPrefix,
                                    final String journalSuffix,
                                    final int minFiles,
                                    final int fileSize,
                                    final String fileInput) throws Exception {
      FileInputStream fileInputStream = new FileInputStream(new File(fileInput));
      importJournal(directory, journalPrefix, journalSuffix, minFiles, fileSize, fileInputStream);

   }

   public static void importJournal(final String directory,
                                    final String journalPrefix,
                                    final String journalSuffix,
                                    final int minFiles,
                                    final int fileSize,
                                    final InputStream stream) throws Exception {
      Reader reader = new InputStreamReader(stream);
      importJournal(directory, journalPrefix, journalSuffix, minFiles, fileSize, reader);
   }

   public static void importJournal(final String directory,
                                    final String journalPrefix,
                                    final String journalSuffix,
                                    final int minFiles,
                                    final int fileSize,
                                    final Reader reader) throws Exception {

      File journalDir = new File(directory);

      if (!journalDir.exists()) {
         if (!journalDir.mkdirs())
            System.err.println("Could not create directory " + directory);
      }

      NIOSequentialFileFactory nio = new NIOSequentialFileFactory(new File(directory), null, 1);

      JournalImpl journal = new JournalImpl(fileSize, minFiles, minFiles, 0, 0, nio, journalPrefix, journalSuffix, 1);

      if (journal.orderFiles().size() != 0) {
         throw new IllegalStateException("Import needs to create a brand new journal");
      }

      journal.start();

      // The journal is empty, as we checked already. Calling load just to initialize the internal data
      journal.loadInternalOnly();

      BufferedReader buffReader = new BufferedReader(reader);

      String line;

      HashMap<Long, AtomicInteger> txCounters = new HashMap<>();

      long lineNumber = 0;

      Map<Long, JournalRecord> journalRecords = journal.getRecords();

      while ((line = buffReader.readLine()) != null) {
         lineNumber++;
         String[] splitLine = line.split(",");
         if (splitLine[0].equals("#File")) {
            txCounters.clear();
            continue;
         }

         Properties lineProperties = parseLine(splitLine);

         String operation = null;
         try {
            operation = lineProperties.getProperty("operation");

            if (operation.equals("AddRecord")) {
               RecordInfo info = parseRecord(lineProperties);
               journal.appendAddRecord(info.id, info.userRecordType, info.data, false);
            } else if (operation.equals("AddRecordTX")) {
               long txID = parseLong("txID", lineProperties);
               AtomicInteger counter = getCounter(txID, txCounters);
               counter.incrementAndGet();
               RecordInfo info = parseRecord(lineProperties);
               journal.appendAddRecordTransactional(txID, info.id, info.userRecordType, info.data);
            } else if (operation.equals("AddRecordTX")) {
               long txID = parseLong("txID", lineProperties);
               AtomicInteger counter = getCounter(txID, txCounters);
               counter.incrementAndGet();
               RecordInfo info = parseRecord(lineProperties);
               journal.appendAddRecordTransactional(txID, info.id, info.userRecordType, info.data);
            } else if (operation.equals("UpdateTX")) {
               long txID = parseLong("txID", lineProperties);
               AtomicInteger counter = getCounter(txID, txCounters);
               counter.incrementAndGet();
               RecordInfo info = parseRecord(lineProperties);
               journal.appendUpdateRecordTransactional(txID, info.id, info.userRecordType, info.data);
            } else if (operation.equals("Update")) {
               RecordInfo info = parseRecord(lineProperties);
               journal.appendUpdateRecord(info.id, info.userRecordType, info.data, false);
            } else if (operation.equals("DeleteRecord")) {
               long id = parseLong("id", lineProperties);

               // If not found it means the append/update records were reclaimed already
               if (journalRecords.get(id) != null) {
                  journal.appendDeleteRecord(id, false);
               }
            } else if (operation.equals("DeleteRecordTX")) {
               long txID = parseLong("txID", lineProperties);
               long id = parseLong("id", lineProperties);
               AtomicInteger counter = getCounter(txID, txCounters);
               counter.incrementAndGet();

               // If not found it means the append/update records were reclaimed already
               if (journalRecords.get(id) != null) {
                  journal.appendDeleteRecordTransactional(txID, id);
               }
            } else if (operation.equals("Prepare")) {
               long txID = parseLong("txID", lineProperties);
               int numberOfRecords = parseInt("numberOfRecords", lineProperties);
               AtomicInteger counter = getCounter(txID, txCounters);
               byte[] data = parseEncoding("extraData", lineProperties);

               if (counter.get() == numberOfRecords) {
                  journal.appendPrepareRecord(txID, data, false);
               } else {
                  System.err.println("Transaction " + txID +
                                        " at line " +
                                        lineNumber +
                                        " is incomplete. The prepare record expected " +
                                        numberOfRecords +
                                        " while the import only had " +
                                        counter);
               }
            } else if (operation.equals("Commit")) {
               long txID = parseLong("txID", lineProperties);
               int numberOfRecords = parseInt("numberOfRecords", lineProperties);
               AtomicInteger counter = getCounter(txID, txCounters);
               if (counter.get() == numberOfRecords) {
                  journal.appendCommitRecord(txID, false);
               } else {
                  System.err.println("Transaction " + txID +
                                        " at line " +
                                        lineNumber +
                                        " is incomplete. The commit record expected " +
                                        numberOfRecords +
                                        " while the import only had " +
                                        counter);
               }
            } else if (operation.equals("Rollback")) {
               long txID = parseLong("txID", lineProperties);
               journal.appendRollbackRecord(txID, false);
            } else {
               System.err.println("Invalid operation " + operation + " at line " + lineNumber);
            }
         } catch (Exception ex) {
            System.err.println("Error at line " + lineNumber + ", operation=" + operation + " msg = " + ex.getMessage());
         }
      }

      journal.stop();
   }

   protected static AtomicInteger getCounter(final Long txID, final Map<Long, AtomicInteger> txCounters) {

      AtomicInteger counter = txCounters.get(txID);
      if (counter == null) {
         counter = new AtomicInteger(0);
         txCounters.put(txID, counter);
      }

      return counter;
   }

   protected static RecordInfo parseRecord(final Properties properties) throws Exception {
      long id = parseLong("id", properties);
      byte userRecordType = parseByte("userRecordType", properties);
      boolean isUpdate = parseBoolean("isUpdate", properties);
      byte[] data = parseEncoding("data", properties);
      return new RecordInfo(id, userRecordType, data, isUpdate, (short) 0);
   }

   private static byte[] parseEncoding(final String name, final Properties properties) throws Exception {
      String value = parseString(name, properties);

      return decode(value);
   }

   /**
    * @param properties
    * @return
    */
   private static int parseInt(final String name, final Properties properties) throws Exception {
      String value = parseString(name, properties);

      return Integer.parseInt(value);
   }

   private static long parseLong(final String name, final Properties properties) throws Exception {
      String value = parseString(name, properties);

      return Long.parseLong(value);
   }

   private static boolean parseBoolean(final String name, final Properties properties) throws Exception {
      String value = parseString(name, properties);

      return Boolean.parseBoolean(value);
   }

   private static byte parseByte(final String name, final Properties properties) throws Exception {
      String value = parseString(name, properties);

      return Byte.parseByte(value);
   }

   /**
    * @param name
    * @param properties
    * @return
    * @throws Exception
    */
   private static String parseString(final String name, final Properties properties) throws Exception {
      String value = properties.getProperty(name);

      if (value == null) {
         throw new Exception("property " + name + " not found");
      }
      return value;
   }

   protected static Properties parseLine(final String[] splitLine) {
      Properties properties = new Properties();

      for (String el : splitLine) {
         String[] tuple = el.split("@");
         if (tuple.length == 2) {
            properties.put(tuple[0], tuple[1]);
         } else {
            properties.put(tuple[0], tuple[0]);
         }
      }

      return properties;
   }

   private static byte[] decode(final String data) {
      return Base64.decode(data, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   public void printUsage() {
      for (int i = 0; i < 10; i++) {
         System.err.println();
      }
      System.err.println("This method will export the journal at low level record.");
      System.err.println();
      System.err.println();
      for (int i = 0; i < 10; i++) {
         System.err.println();
      }
   }

}
