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

import java.io.File;
import java.util.ArrayList;

import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.journal.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.journal.impl.NIOSequentialFileFactory;

public class DataTool
{

   private static final String BINDING_TYPE = "binding";
   private static final String JOURNAL_TYPE = "journal";
   private static final String JMS_TYPE = "jms";


   private static final String ROLLBACK = "rollback";

   private static final String DELETE = "delete";

   public void process(String[] arg)
   {
      if (arg.length < 5)
      {
         printUsage();
         System.exit(-1);
      }

      String type = arg[1];
      String directoryName = arg[2];
      String sizeStr = arg[3];

      long sizeLong;

      if (!type.equals(BINDING_TYPE) && !type.equals(JOURNAL_TYPE))
      {
         System.err.println("Invalid type: " + type);
         printUsage();
         System.exit(-1);
      }

      File directory = new File(directoryName);
      if (!directory.exists() || !directory.isDirectory())
      {
         System.err.println("Invalid directory " + directoryName);
         printUsage();
         System.exit(-1);
      }

      try
      {
         sizeLong = Long.parseLong(sizeStr);

         if (sizeLong <= 0)
         {
            System.err.println("Invalid size " + sizeLong);
            printUsage();
            System.exit(-1);
         }
      }
      catch (Throwable e)
      {
         System.err.println("Error converting journal size: " + e.getMessage() + " couldn't convert size " + sizeStr);
         printUsage();
         System.exit(-1);
      }

      final String journalName;
      final String exension;

      if (type.equals(JOURNAL_TYPE))
      {
         journalName = "activemq-data";
         exension = "amq";
      }
      else if (type.equals(BINDING_TYPE))
      {
         journalName = "activemq-bindings";
         exension = "bindings";
      }
      else if (type.equals(JMS_TYPE))
      {
         journalName = "activemq-jms";
         exension = "jms";
      }
      else
      {
         printUsage();
         System.exit(-1);
         return; // dumb compiler don't know System.exit interrupts the execution, some variables wouldn't be init
      }

      SequentialFileFactory messagesFF = new NIOSequentialFileFactory(directoryName, null);

      // Will use only default values. The load function should adapt to anything different
      ConfigurationImpl defaultValues = new ConfigurationImpl();

      try
      {
         ArrayList<Long> txsToRollback = new ArrayList<Long>();

         ArrayList<Long> idsToDelete = new ArrayList<Long>();


         ArrayList<Long> listInUse = null;

         for (int i = 4; i < arg.length; i++)
         {
            String str = arg[i];
            if (str.equals(DELETE))
            {
               listInUse = idsToDelete;
            }
            else if (str.equals(ROLLBACK))
            {
               listInUse = txsToRollback;
            }
            else
            {
               try
               {
                  if (listInUse == null)
                  {
                     System.err.println("You must specify either " + DELETE + " or " + ROLLBACK + " as a command for the IDs you're using");
                     printUsage();
                     System.exit(-1);
                  }

                  long id = Long.parseLong(str);
                  listInUse.add(id);
               }
               catch (Throwable e)
               {
                  System.err.println("Error converting id " + str + " as a recordID");
                  printUsage();
                  System.exit(-1);
               }

            }
         }

         JournalImpl messagesJournal = new JournalImpl(defaultValues.getJournalFileSize(),
                                                       defaultValues.getJournalMinFiles(),
                                                       0,
                                                       0,
                                                       messagesFF,
                                                       journalName,
                                                       exension,
                                                       1);

         messagesJournal.start();

         messagesJournal.loadInternalOnly();


         for (long tx : txsToRollback)
         {
            System.out.println("Rolling back " + tx);

            try
            {
               messagesJournal.appendRollbackRecord(tx, true);
            }
            catch (Throwable e)
            {
               e.printStackTrace();
            }
         }


         for (long id : idsToDelete)
         {
            System.out.println("Deleting record " + id);

            try
            {
               messagesJournal.appendDeleteRecord(id, true);
            }
            catch (Throwable e)
            {
               e.printStackTrace();
            }
         }

         messagesJournal.stop();

      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }


   public void printUsage()
   {
      for (int i = 0; i < 10; i++)
      {
         System.err.println();
      }
      System.err.println(Main.USAGE + " binding|journal <directory> <size> [rollback | delete] record1,record2..recordN");
      System.err.println();
      System.err.println("Example:");
      System.err.println("say you wanted to rollback a prepared TXID=100, and you want to remove records 300, 301, 302:");
      System.err.println(Main.USAGE + " journal /tmp/your-folder 10485760 rollback 100 delete 300 301 302");
      System.err.println();
      System.err.println(".. and you can specify as many rollback and delete you like");
      for (int i = 0; i < 10; i++)
      {
         System.err.println();
      }
   }
}
