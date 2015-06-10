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
package org.apache.activemq.artemis.tests.util;

import java.util.ArrayList;

import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.utils.TimeAndCounterIDGenerator;

/**
 * A JournalExample: Just an example on how to use the Journal Directly
 * <br>
 * TODO: find a better place to store this example
 */
public class JournalExample
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   public static void main(final String[] arg)
   {
      TimeAndCounterIDGenerator idgenerator = new TimeAndCounterIDGenerator();
      try
      {
         SequentialFileFactory fileFactory = new AIOSequentialFileFactory("/tmp"); // any dir you want
         // SequentialFileFactory fileFactory = new NIOSequentialFileFactory("/tmp"); // any dir you want
         JournalImpl journalExample = new JournalImpl(10 * 1024 * 1024, // 10M.. we believe that's the usual cilinder
                                                      // bufferSize.. not an exact science here
                                                      2, // number of files pre-allocated
                                                      0,
                                                      0,
                                                      fileFactory, // AIO or NIO
                                                      "exjournal", // file name
                                                      "dat", // extension
                                                      10000); // it's like a semaphore for callback on the AIO layer

         ArrayList<RecordInfo> committedRecords = new ArrayList<RecordInfo>();
         ArrayList<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();
         journalExample.start();
         System.out.println("Loading records and creating data files");
         journalExample.load(committedRecords, preparedTransactions, null);

         System.out.println("Loaded Record List:");

         for (RecordInfo record : committedRecords)
         {
            System.out.println("Record id = " + record.id +
                                  " userType = " +
                                  record.userRecordType +
                                  " with " +
                                  record.data.length +
                                  " bytes is stored on the journal");
         }

         System.out.println("Adding Records:");

         for (int i = 0; i < 10; i++)
         {
            journalExample.appendAddRecord(idgenerator.generateID(), (byte) 1, new byte[]{
               0,
               1,
               2,
               0,
               1,
               2,
               0,
               1,
               2,
               0,
               1,
               2,
               0,
               1,
               2,
               0,
               1,
               2,
               0,
               1,
               2}, false);
         }

         long tx = idgenerator.generateID(); // some id generation system

         for (int i = 0; i < 100; i++)
         {
            journalExample.appendAddRecordTransactional(tx, idgenerator.generateID(), (byte) 2, new byte[]{0,
               1,
               2,
               0,
               1,
               2,
               0,
               1,
               2,
               0,
               1,
               2,
               0,
               1,
               2,
               0,
               1,
               2,
               0,
               1,
               2,
               5});
         }

         // After this is complete, you're sure the records are there
         journalExample.appendCommitRecord(tx, true);

         System.out.println("Done!");

         journalExample.stop();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

}
