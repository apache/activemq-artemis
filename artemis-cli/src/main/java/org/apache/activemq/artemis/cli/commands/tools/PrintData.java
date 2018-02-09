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
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.message.impl.CoreMessagePersister;
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
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.CursorAckRecordEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PageUpdateTXEncoding;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;

@Command(name = "print", description = "Print data records information (WARNING: don't use while a production server is running)")
public class PrintData extends DBOption {


   @Option(name = "--safe", description = "It will print your data structure without showing your data")
   private boolean safe = false;

   private static final String BINDINGS_BANNER = "B I N D I N G S  J O U R N A L";
   private static final String MESSAGES_BANNER = "M E S S A G E S   J O U R N A L";
   static {
      MessagePersister.registerPersister(CoreMessagePersister.getInstance());
   }

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      Configuration configuration = getParameterConfiguration();

      try {
         if (configuration.isJDBC()) {
            printDataJDBC(configuration, context.out);
         } else {
            printData(new File(getBinding()), new File(getJournal()), new File(getPaging()), context.out, safe);
         }
      } catch (Exception e) {
         treatError(e, "data", "print");
      }
      return null;
   }


   public void printDataJDBC(Configuration configuration, PrintStream out) throws Exception {
      initializeJournal(configuration);

      Artemis.printBanner(out);

      printBanner(out, BINDINGS_BANNER);

      DescribeJournal.printSurvivingRecords(storageManager.getBindingsJournal(), out, safe);

      printBanner(out, MESSAGES_BANNER);

      DescribeJournal describeJournal = DescribeJournal.printSurvivingRecords(storageManager.getMessageJournal(), out, safe);

      printPages(describeJournal, storageManager, pagingmanager, out, safe);

      cleanup();

   }
   public static void printData(File bindingsDirectory, File messagesDirectory, File pagingDirectory) throws Exception {
      printData(bindingsDirectory, messagesDirectory, pagingDirectory, false);
   }

   public static void printData(File bindingsDirectory, File messagesDirectory, File pagingDirectory, boolean secret) throws Exception {
      printData(bindingsDirectory, messagesDirectory, pagingDirectory, System.out, secret);
   }

   public static void printData(File bindingsDirectory, File messagesDirectory, File pagingDirectory, PrintStream out, boolean safe) throws Exception {
         // Having the version on the data report is an information very useful to understand what happened
      // When debugging stuff
      Artemis.printBanner(out);

      File serverLockFile = new File(messagesDirectory, "server.lock");

      if (serverLockFile.isFile()) {
         try {
            FileLockNodeManager fileLock = new FileLockNodeManager(messagesDirectory, false);
            fileLock.start();
            printBanner(out, "Server's ID=" + fileLock.getNodeId().toString());
            fileLock.stop();
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

      printBanner(out, BINDINGS_BANNER);

      try {
         DescribeJournal.describeBindingsJournal(bindingsDirectory, out, safe);
      } catch (Exception e) {
         e.printStackTrace();
      }

      printBanner(out, MESSAGES_BANNER);

      DescribeJournal describeJournal = null;
      try {
         describeJournal = DescribeJournal.describeMessagesJournal(messagesDirectory, out, safe);
      } catch (Exception e) {
         e.printStackTrace();
         return;
      }

      try {
         printBanner(out, "P A G I N G");

         printPages(pagingDirectory, describeJournal, out, safe);
      } catch (Exception e) {
         e.printStackTrace();
         return;
      }

   }

   protected static void printBanner(PrintStream out, String x2) {
      out.println();
      out.println("********************************************");
      out.println(x2);
      out.println("********************************************");
   }

   private static void printPages(File pageDirectory, DescribeJournal describeJournal, PrintStream out, boolean safe) {
      try {

         ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, ActiveMQThreadFactory.defaultThreadFactory());
         final ExecutorService executor = Executors.newFixedThreadPool(10, ActiveMQThreadFactory.defaultThreadFactory());
         ExecutorFactory execfactory = new ExecutorFactory() {
            @Override
            public ArtemisExecutor getExecutor() {
               return ArtemisExecutor.delegate(executor);
            }
         };
         final StorageManager sm = new NullStorageManager();
         PagingStoreFactory pageStoreFactory = new PagingStoreFactoryNIO(sm, pageDirectory, 1000L, scheduled, execfactory, false, null);
         HierarchicalRepository<AddressSettings> addressSettingsRepository = new HierarchicalObjectRepository<>();
         addressSettingsRepository.setDefault(new AddressSettings());
         PagingManager manager = new PagingManagerImpl(pageStoreFactory, addressSettingsRepository);

         printPages(describeJournal, sm, manager, out, safe);
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private static void printPages(DescribeJournal describeJournal,
                                  StorageManager sm,
                                  PagingManager manager,
                                  PrintStream out,
                                  boolean safe) throws Exception {
      PageCursorsInfo cursorACKs = calculateCursorsInfo(describeJournal.getRecords());

      Set<Long> pgTXs = cursorACKs.getPgTXs();

      manager.start();

      SimpleString[] stores = manager.getStoreNames();

      for (SimpleString store : stores) {
         PagingStore pgStore = manager.getPageStore(store);
         File folder = null;

         if (pgStore != null) {
            folder = pgStore.getFolder();
         }
         out.println("####################################################################################################");
         out.println("Exploring store " + store + " folder = " + folder);
         int pgid = (int) pgStore.getFirstPage();
         for (int pg = 0; pg < pgStore.getNumberOfPages(); pg++) {
            out.println("*******   Page " + pgid);
            Page page = pgStore.createPage(pgid);
            page.open();
            List<PagedMessage> msgs = page.read(sm);
            page.close();

            int msgID = 0;

            for (PagedMessage msg : msgs) {
               msg.initMessage(sm);
               if (safe) {
                  try {
                     out.print("pg=" + pgid + ", msg=" + msgID + ",pgTX=" + msg.getTransactionID() + ", msg=" + msg.getMessage().getClass().getSimpleName() + "(safe data, size=" + msg.getMessage().getPersistentSize() + ")");
                  } catch (Exception e) {
                     out.print("pg=" + pgid + ", msg=" + msgID + ",pgTX=" + msg.getTransactionID() + ", msg=" + msg.getMessage().getClass().getSimpleName() + "(safe data)");
                  }
               } else {
                  out.print("pg=" + pgid + ", msg=" + msgID + ",pgTX=" + msg.getTransactionID() + ",userMessageID=" + (msg.getMessage().getUserID() != null ? msg.getMessage().getUserID() : "") + ", msg=" + msg.getMessage());
               }
               out.print(",Queues = ");
               long[] q = msg.getQueueIDs();
               for (int i = 0; i < q.length; i++) {
                  out.print(q[i]);

                  PagePosition posCheck = new PagePositionImpl(pgid, msgID);

                  boolean acked = false;

                  Set<PagePosition> positions = cursorACKs.getCursorRecords().get(q[i]);
                  if (positions != null) {
                     acked = positions.contains(posCheck);
                  }

                  if (acked) {
                     out.print(" (ACK)");
                  }

                  if (cursorACKs.getCompletePages(q[i]).contains(Long.valueOf(pgid))) {
                     out.println(" (PG-COMPLETE)");
                  }

                  if (i + 1 < q.length) {
                     out.print(",");
                  }
               }
               if (msg.getTransactionID() >= 0 && !pgTXs.contains(msg.getTransactionID())) {
                  out.print(", **PG_TX_NOT_FOUND**");
               }
               out.println();
               msgID++;
            }
            pgid++;
         }
      }
   }

   /**
    * Calculate the acks on the page system
    */
   private static PageCursorsInfo calculateCursorsInfo(List<RecordInfo> records) throws Exception {

      PageCursorsInfo cursorInfo = new PageCursorsInfo();

      for (RecordInfo record : records) {
         byte[] data = record.data;

         ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(data);

         if (record.userRecordType == JournalRecordIds.ACKNOWLEDGE_CURSOR) {
            CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
            encoding.decode(buff);

            Set<PagePosition> set = cursorInfo.getCursorRecords().get(encoding.queueID);

            if (set == null) {
               set = new HashSet<>();
               cursorInfo.getCursorRecords().put(encoding.queueID, set);
            }

            set.add(encoding.position);
         } else if (record.userRecordType == JournalRecordIds.PAGE_CURSOR_COMPLETE) {
            CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
            encoding.decode(buff);

            Long queueID = Long.valueOf(encoding.queueID);
            Long pageNR = Long.valueOf(encoding.position.getPageNr());

            if (!cursorInfo.getCompletePages(queueID).add(pageNR)) {
               System.err.println("Page " + pageNR + " has been already set as complete on queue " + queueID);
            }
         } else if (record.userRecordType == JournalRecordIds.PAGE_TRANSACTION) {
            if (record.isUpdate) {
               PageUpdateTXEncoding pageUpdate = new PageUpdateTXEncoding();

               pageUpdate.decode(buff);
               cursorInfo.getPgTXs().add(pageUpdate.pageTX);
            } else {
               PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

               pageTransactionInfo.decode(buff);

               pageTransactionInfo.setRecordID(record.id);
               cursorInfo.getPgTXs().add(pageTransactionInfo.getTransactionID());
            }
         }
      }

      return cursorInfo;
   }

   private static class PageCursorsInfo {

      private final Map<Long, Set<PagePosition>> cursorRecords = new HashMap<>();

      private final Set<Long> pgTXs = new HashSet<>();

      private final Map<Long, Set<Long>> completePages = new HashMap<>();

      private PageCursorsInfo() {
      }

      /**
       * @return the pgTXs
       */
      Set<Long> getPgTXs() {
         return pgTXs;
      }

      /**
       * @return the cursorRecords
       */
      Map<Long, Set<PagePosition>> getCursorRecords() {
         return cursorRecords;
      }

      Set<Long> getCompletePages(Long queueID) {
         Set<Long> completePagesSet = completePages.get(queueID);

         if (completePagesSet == null) {
            completePagesSet = new HashSet<>();
            completePages.put(queueID, completePagesSet);
         }

         return completePagesSet;
      }

   }

}
