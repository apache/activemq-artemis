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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.airlift.airline.Command;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.Action;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.paging.cursor.impl.PagePositionImpl;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.paging.impl.PageTransactionInfoImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreFactoryNIO;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.DescribeJournal;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.utils.ExecutorFactory;

@Command(name = "print", description = "Print data records information (WARNING: don't use while a production server is running)")
public class PrintData extends DataAbstract implements Action
{
   @Override
   public Object execute(ActionContext context) throws Exception
   {
      super.execute(context);
      try
      {
         printData(new File(getBinding()), new File(getJournal()), new File(getPaging()));
      }
      catch (Exception e)
      {
         treatError(e, "data", "print");
      }
      return null;
   }

   public static void printData(File bindingsDirectory, File messagesDirectory, File pagingDirectory) throws Exception
   {
      // Having the version on the data report is an information very useful to understand what happened
      // When debugging stuff
      Artemis.printBanner();

      File serverLockFile = new File(messagesDirectory, "server.lock");

      if (serverLockFile.isFile())
      {
         try
         {
            FileLockNodeManager fileLock = new FileLockNodeManager(messagesDirectory, false);
            fileLock.start();
            System.out.println("********************************************");
            System.out.println("Server's ID=" + fileLock.getNodeId().toString());
            System.out.println("********************************************");
            fileLock.stop();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }

      System.out.println("********************************************");
      System.out.println("B I N D I N G S  J O U R N A L");
      System.out.println("********************************************");

      try
      {
         DescribeJournal.describeBindingsJournal(bindingsDirectory);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

      System.out.println();
      System.out.println("********************************************");
      System.out.println("M E S S A G E S   J O U R N A L");
      System.out.println("********************************************");

      DescribeJournal describeJournal = null;
      try
      {
         describeJournal = DescribeJournal.describeMessagesJournal(messagesDirectory);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         return;
      }


      try
      {
         System.out.println();
         System.out.println("********************************************");
         System.out.println("P A G I N G");
         System.out.println("********************************************");

         printPages(pagingDirectory, describeJournal);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         return;
      }


   }


   private static void printPages(File pageDirectory, DescribeJournal describeJournal)
   {
      try
      {

         PageCursorsInfo cursorACKs = calculateCursorsInfo(describeJournal.getRecords());

         Set<Long> pgTXs = cursorACKs.getPgTXs();

         ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
         final ExecutorService executor = Executors.newFixedThreadPool(10);
         ExecutorFactory execfactory = new ExecutorFactory()
         {
            @Override
            public Executor getExecutor()
            {
               return executor;
            }
         };
         final StorageManager sm = new NullStorageManager();
         PagingStoreFactory pageStoreFactory =
            new PagingStoreFactoryNIO(sm, pageDirectory, 1000L, scheduled, execfactory, false, null);
         HierarchicalRepository<AddressSettings> addressSettingsRepository = new HierarchicalObjectRepository<AddressSettings>();
         addressSettingsRepository.setDefault(new AddressSettings());
         PagingManager manager = new PagingManagerImpl(pageStoreFactory, addressSettingsRepository);

         manager.start();

         SimpleString[] stores = manager.getStoreNames();

         for (SimpleString store : stores)
         {
            PagingStore pgStore = manager.getPageStore(store);
            File folder = null;

            if (pgStore != null)
            {
               folder = pgStore.getFolder();
            }
            System.out.println("####################################################################################################");
            System.out.println("Exploring store " + store + " folder = " + folder);
            int pgid = (int) pgStore.getFirstPage();
            for (int pg = 0; pg < pgStore.getNumberOfPages(); pg++)
            {
               System.out.println("*******   Page " + pgid);
               Page page = pgStore.createPage(pgid);
               page.open();
               List<PagedMessage> msgs = page.read(sm);
               page.close();

               int msgID = 0;

               for (PagedMessage msg : msgs)
               {
                  msg.initMessage(sm);
                  System.out.print("pg=" + pgid + ", msg=" + msgID + ",pgTX=" + msg.getTransactionID() + ",userMessageID=" + (msg.getMessage().getUserID() != null ? msg.getMessage().getUserID() : "") + ", msg=" + msg.getMessage());
                  System.out.print(",Queues = ");
                  long[] q = msg.getQueueIDs();
                  for (int i = 0; i < q.length; i++)
                  {
                     System.out.print(q[i]);

                     PagePosition posCheck = new PagePositionImpl(pgid, msgID);

                     boolean acked = false;

                     Set<PagePosition> positions = cursorACKs.getCursorRecords().get(q[i]);
                     if (positions != null)
                     {
                        acked = positions.contains(posCheck);
                     }

                     if (acked)
                     {
                        System.out.print(" (ACK)");
                     }

                     if (cursorACKs.getCompletePages(q[i]).contains(Long.valueOf(pgid)))
                     {
                        System.out.println(" (PG-COMPLETE)");
                     }


                     if (i + 1 < q.length)
                     {
                        System.out.print(",");
                     }
                  }
                  if (msg.getTransactionID() >= 0 && !pgTXs.contains(msg.getTransactionID()))
                  {
                     System.out.print(", **PG_TX_NOT_FOUND**");
                  }
                  System.out.println();
                  msgID++;
               }

               pgid++;

            }
         }

      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }


   /** Calculate the acks on the page system */
   protected static PageCursorsInfo calculateCursorsInfo(List<RecordInfo> records) throws Exception
   {

      PageCursorsInfo cursorInfo = new PageCursorsInfo();


      for (RecordInfo record : records)
      {
         byte[] data = record.data;

         ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(data);

         if (record.userRecordType == JournalRecordIds.ACKNOWLEDGE_CURSOR)
         {
            JournalStorageManager.CursorAckRecordEncoding encoding = new JournalStorageManager.CursorAckRecordEncoding();
            encoding.decode(buff);

            Set<PagePosition> set = cursorInfo.getCursorRecords().get(encoding.queueID);

            if (set == null)
            {
               set = new HashSet<PagePosition>();
               cursorInfo.getCursorRecords().put(encoding.queueID, set);
            }

            set.add(encoding.position);
         }
         else if (record.userRecordType == JournalRecordIds.PAGE_CURSOR_COMPLETE)
         {
            JournalStorageManager.CursorAckRecordEncoding encoding = new JournalStorageManager.CursorAckRecordEncoding();
            encoding.decode(buff);

            Long queueID = Long.valueOf(encoding.queueID);
            Long pageNR = Long.valueOf(encoding.position.getPageNr());

            if (!cursorInfo.getCompletePages(queueID).add(pageNR))
            {
               System.err.println("Page " + pageNR + " has been already set as complete on queue " + queueID);
            }
         }
         else if (record.userRecordType == JournalRecordIds.PAGE_TRANSACTION)
         {
            if (record.isUpdate)
            {
               JournalStorageManager.PageUpdateTXEncoding pageUpdate = new JournalStorageManager.PageUpdateTXEncoding();

               pageUpdate.decode(buff);
               cursorInfo.getPgTXs().add(pageUpdate.pageTX);
            }
            else
            {
               PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

               pageTransactionInfo.decode(buff);

               pageTransactionInfo.setRecordID(record.id);
               cursorInfo.getPgTXs().add(pageTransactionInfo.getTransactionID());
            }
         }
      }

      return cursorInfo;
   }


   private static class PageCursorsInfo
   {
      private final Map<Long, Set<PagePosition>> cursorRecords = new HashMap<Long, Set<PagePosition>>();

      private final Set<Long> pgTXs = new HashSet<Long>();

      private final Map<Long, Set<Long>> completePages = new HashMap<Long, Set<Long>>();

      public PageCursorsInfo()
      {
      }


      /**
       * @return the pgTXs
       */
      public Set<Long> getPgTXs()
      {
         return pgTXs;
      }


      /**
       * @return the cursorRecords
       */
      public Map<Long, Set<PagePosition>> getCursorRecords()
      {
         return cursorRecords;
      }


      /**
       * @return the completePages
       */
      public Map<Long, Set<Long>> getCompletePages()
      {
         return completePages;
      }

      public Set<Long> getCompletePages(Long queueID)
      {
         Set<Long> completePagesSet = completePages.get(queueID);

         if (completePagesSet == null)
         {
            completePagesSet = new HashSet<Long>();
            completePages.put(queueID, completePagesSet);
         }

         return completePagesSet;
      }

   }


}
