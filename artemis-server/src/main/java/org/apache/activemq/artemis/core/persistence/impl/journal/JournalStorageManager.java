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

package org.apache.activemq.artemis.core.persistence.impl.journal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.ActiveMQInternalErrorException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.LargeMessagePersister;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PendingLargeMessageEncoding;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.apache.activemq.artemis.core.replication.ReplicatedJournal;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.jboss.logging.Logger;

public class JournalStorageManager extends AbstractJournalStorageManager {

   private static final Logger logger = Logger.getLogger(JournalStorageManager.class);

   protected SequentialFileFactory journalFF;

   protected SequentialFileFactory bindingsFF;

   SequentialFileFactory largeMessagesFactory;

   protected Journal originalMessageJournal;

   protected Journal originalBindingsJournal;

   protected String largeMessagesDirectory;

   protected ReplicationManager replicator;

   public JournalStorageManager(final Configuration config,
                                final CriticalAnalyzer analyzer,
                                final ExecutorFactory executorFactory,
                                final ScheduledExecutorService scheduledExecutorService,
                                final ExecutorFactory ioExecutors) {
      this(config, analyzer, executorFactory, scheduledExecutorService, ioExecutors, null);
   }

   public JournalStorageManager(final Configuration config, CriticalAnalyzer analyzer, final ExecutorFactory executorFactory, final ExecutorFactory ioExecutors) {
      this(config, analyzer, executorFactory, null, ioExecutors, null);
   }

   public JournalStorageManager(final Configuration config,
                                final CriticalAnalyzer analyzer,
                                final ExecutorFactory executorFactory,
                                final ScheduledExecutorService scheduledExecutorService,
                                final ExecutorFactory ioExecutors,
                                final IOCriticalErrorListener criticalErrorListener) {
      super(config, analyzer, executorFactory, scheduledExecutorService, ioExecutors, criticalErrorListener);
   }

   public JournalStorageManager(final Configuration config,
                                final CriticalAnalyzer analyzer,
                                final ExecutorFactory executorFactory,
                                final ExecutorFactory ioExecutors,
                                final IOCriticalErrorListener criticalErrorListener) {
      super(config, analyzer, executorFactory, null, ioExecutors, criticalErrorListener);
   }

   @Override
   public SequentialFileFactory getJournalSequentialFileFactory() {
      return journalFF;
   }

   @Override
   protected void init(Configuration config, IOCriticalErrorListener criticalErrorListener) {

      if (!EnumSet.allOf(JournalType.class).contains(config.getJournalType())) {
         throw ActiveMQMessageBundle.BUNDLE.invalidJournal();
      }

      bindingsFF = new NIOSequentialFileFactory(config.getBindingsLocation(), criticalErrorListener, config.getJournalMaxIO_NIO());
      bindingsFF.setDatasync(config.isJournalDatasync());

      Journal localBindings = new JournalImpl(ioExecutorFactory, 1024 * 1024, 2, config.getJournalCompactMinFiles(), config.getJournalPoolFiles(), config.getJournalCompactPercentage(), config.getJournalFileOpenTimeout(), bindingsFF, "activemq-bindings", "bindings", 1, 0, criticalErrorListener);

      bindingsJournal = localBindings;
      originalBindingsJournal = localBindings;

      switch (config.getJournalType()) {

         case NIO:
            if (criticalErrorListener != null) {
               ActiveMQServerLogger.LOGGER.journalUseNIO();
            }
            journalFF = new NIOSequentialFileFactory(config.getJournalLocation(), true, config.getJournalBufferSize_NIO(), config.getJournalBufferTimeout_NIO(), config.getJournalMaxIO_NIO(), config.isLogJournalWriteRate(), criticalErrorListener, getCriticalAnalyzer());
            break;
         case ASYNCIO:
            if (criticalErrorListener != null) {
               ActiveMQServerLogger.LOGGER.journalUseAIO();
            }
            journalFF = new AIOSequentialFileFactory(config.getJournalLocation(), config.getJournalBufferSize_AIO(), config.getJournalBufferTimeout_AIO(), config.getJournalMaxIO_AIO(), config.isLogJournalWriteRate(), criticalErrorListener, getCriticalAnalyzer());
            break;
         case MAPPED:
            if (criticalErrorListener != null) {
               ActiveMQServerLogger.LOGGER.journalUseMAPPED();
            }
            journalFF = new MappedSequentialFileFactory(config.getJournalLocation(), config.getJournalFileSize(), true, config.getJournalBufferSize_NIO(), config.getJournalBufferTimeout_NIO(), criticalErrorListener);
            break;
         default:
            throw ActiveMQMessageBundle.BUNDLE.invalidJournalType2(config.getJournalType());
      }

      journalFF.setDatasync(config.isJournalDatasync());


      int fileSize = config.getJournalFileSize();
      // we need to correct the file size if its not a multiple of the alignement
      int modulus = fileSize % journalFF.getAlignment();
      if (modulus != 0) {
         int difference = modulus;
         int low = config.getJournalFileSize() - difference;
         int high = low + journalFF.getAlignment();
         fileSize = difference < journalFF.getAlignment() / 2 ? low : high;
         ActiveMQServerLogger.LOGGER.invalidJournalFileSize(config.getJournalFileSize(), fileSize, journalFF.getAlignment());
      }
      Journal localMessage = createMessageJournal(config, criticalErrorListener, fileSize);

      messageJournal = localMessage;
      originalMessageJournal = localMessage;

      largeMessagesDirectory = config.getLargeMessagesDirectory();

      largeMessagesFactory = new NIOSequentialFileFactory(config.getLargeMessagesLocation(), false, criticalErrorListener, 1);

      if (config.getPageMaxConcurrentIO() != 1) {
         pageMaxConcurrentIO = new Semaphore(config.getPageMaxConcurrentIO());
      } else {
         pageMaxConcurrentIO = null;
      }
   }

   protected Journal createMessageJournal(Configuration config,
                                        IOCriticalErrorListener criticalErrorListener,
                                        int fileSize) {
      return new JournalImpl(ioExecutorFactory, fileSize, config.getJournalMinFiles(), config.getJournalPoolFiles(), config.getJournalCompactMinFiles(), config.getJournalCompactPercentage(), config.getJournalFileOpenTimeout(), journalFF, "activemq-data", "amq", journalFF.getMaxIO(), 0, criticalErrorListener);
   }

   // Life Cycle Handlers
   @Override
   protected void beforeStart() throws Exception {
      checkAndCreateDir(config.getBindingsLocation(), config.isCreateBindingsDir());
      checkAndCreateDir(config.getJournalLocation(), config.isCreateJournalDir());
      checkAndCreateDir(config.getLargeMessagesLocation(), config.isCreateJournalDir());
      cleanupIncompleteFiles();
   }

   @Override
   protected void beforeStop() throws Exception {
      if (replicator != null) {
         replicator.stop();
      }
   }

   @Override
   public void stop() throws Exception {
      stop(false, true);
   }

   public boolean isReplicated() {
      return replicator != null;
   }

   private void cleanupIncompleteFiles() throws Exception {
      if (largeMessagesFactory != null) {
         List<String> tmpFiles = largeMessagesFactory.listFiles("tmp");
         for (String tmpFile : tmpFiles) {
            SequentialFile file = largeMessagesFactory.createSequentialFile(tmpFile);
            file.delete();
         }
      }
   }

   @Override
   public synchronized void stop(boolean ioCriticalError, boolean sendFailover) throws Exception {
      if (!started) {
         return;
      }

      if (!ioCriticalError) {
         performCachedLargeMessageDeletes();
         // Must call close to make sure last id is persisted
         if (journalLoaded && idGenerator != null)
            idGenerator.persistCurrentID();
      }

      final CountDownLatch latch = new CountDownLatch(1);
      try {
         executor.execute(new Runnable() {
            @Override
            public void run() {
               latch.countDown();
            }
         });

         latch.await(30, TimeUnit.SECONDS);
      } catch (RejectedExecutionException ignored) {
         // that's ok
      }

      // We cache the variable as the replicator could be changed between here and the time we call stop
      // since sendLiveIsStopping may issue a close back from the channel
      // and we want to ensure a stop here just in case
      ReplicationManager replicatorInUse = replicator;
      if (replicatorInUse != null) {
         if (sendFailover) {
            final OperationContext token = replicator.sendLiveIsStopping(ReplicationLiveIsStoppingMessage.LiveStopping.FAIL_OVER);
            if (token != null) {
               try {
                  token.waitCompletion(5000);
               } catch (Exception e) {
                  // ignore it
               }
            }
         }
         replicatorInUse.stop();
      }
      bindingsJournal.stop();

      messageJournal.stop();

      journalLoaded = false;

      started = false;
   }

   /**
    * Assumption is that this is only called with a writeLock on the StorageManager.
    */
   @Override
   protected void performCachedLargeMessageDeletes() {
      for (Long largeMsgId : largeMessagesToDelete) {
         SequentialFile msg = createFileForLargeMessage(largeMsgId, LargeMessageExtension.DURABLE);
         try {
            msg.delete();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.journalErrorDeletingMessage(e, largeMsgId);
         }
         if (replicator != null) {
            replicator.largeMessageDelete(largeMsgId, JournalStorageManager.this);
         }
      }
      largeMessagesToDelete.clear();
   }

   protected SequentialFile createFileForLargeMessage(final long messageID, final boolean durable) {
      if (durable) {
         return createFileForLargeMessage(messageID, LargeMessageExtension.DURABLE);
      } else {
         return createFileForLargeMessage(messageID, LargeMessageExtension.TEMPORARY);
      }
   }

   @Override
   /**
    * @param messages
    * @param buff
    * @return
    * @throws Exception
    */ protected LargeServerMessage parseLargeMessage(final Map<Long, Message> messages,
                                                      final ActiveMQBuffer buff) throws Exception {
      LargeServerMessage largeMessage = createLargeMessage();

      LargeMessagePersister.getInstance().decode(buff, largeMessage);

      if (largeMessage.containsProperty(Message.HDR_ORIG_MESSAGE_ID)) {
         // for compatibility: couple with old behaviour, copying the old file to avoid message loss
         long originalMessageID = largeMessage.getLongProperty(Message.HDR_ORIG_MESSAGE_ID);

         SequentialFile currentFile = createFileForLargeMessage(largeMessage.getMessageID(), true);

         if (!currentFile.exists()) {
            SequentialFile linkedFile = createFileForLargeMessage(originalMessageID, true);
            if (linkedFile.exists()) {
               linkedFile.copyTo(currentFile);
               linkedFile.close();
            }
         }

         currentFile.close();
      }

      return largeMessage;
   }

   @Override
   public void pageClosed(final SimpleString storeName, final int pageNumber) {
      if (isReplicated()) {
         readLock();
         try {
            if (isReplicated())
               replicator.pageClosed(storeName, pageNumber);
         } finally {
            readUnLock();
         }
      }
   }

   @Override
   public void pageDeleted(final SimpleString storeName, final int pageNumber) {
      if (isReplicated()) {
         readLock();
         try {
            if (isReplicated())
               replicator.pageDeleted(storeName, pageNumber);
         } finally {
            readUnLock();
         }
      }
   }

   @Override
   public void pageWrite(final PagedMessage message, final int pageNumber) {
      if (isReplicated()) {
         // Note: (https://issues.jboss.org/browse/HORNETQ-1059)
         // We have to replicate durable and non-durable messages on paging
         // since acknowledgments are written using the page-position.
         // Say you are sending durable and non-durable messages to a page
         // The ACKs would be done to wrong positions, and the backup would be a mess

         readLock();
         try {
            if (isReplicated())
               replicator.pageWrite(message, pageNumber);
         } finally {
            readUnLock();
         }
      }
   }

   @Override
   public ByteBuffer allocateDirectBuffer(int size) {
      return journalFF.allocateDirectBuffer(size);
   }

   @Override
   public void freeDirectBuffer(ByteBuffer buffer) {
      journalFF.releaseBuffer(buffer);
   }

   public long storePendingLargeMessage(final long messageID, long recordID) throws Exception {
      readLock();
      try {
         if (recordID == LargeServerMessage.NO_PENDING_ID) {
            recordID = generateID();
         } else {
            //this means the large message doesn't
            //have a pendingRecordID, but one has been
            //generated (coming from live server) for use.
            recordID = -recordID;
         }

         messageJournal.appendAddRecord(recordID, JournalRecordIds.ADD_LARGE_MESSAGE_PENDING, new PendingLargeMessageEncoding(messageID), true, getContext(true));

         return recordID;
      } finally {
         readUnLock();
      }
   }

   // This should be accessed from this package only
   void deleteLargeMessageFile(final LargeServerMessage largeServerMessage) throws ActiveMQException {
      if (largeServerMessage.getPendingRecordID() < 0) {
         try {
            // The delete file happens asynchronously
            // And the client won't be waiting for the actual file to be deleted.
            // We set a temporary record (short lived) on the journal
            // to avoid a situation where the server is restarted and pending large message stays on forever
            largeServerMessage.setPendingRecordID(storePendingLargeMessage(largeServerMessage.getMessageID(), largeServerMessage.getPendingRecordID()));
         } catch (Exception e) {
            throw new ActiveMQInternalErrorException(e.getMessage(), e);
         }
      }
      final SequentialFile file = largeServerMessage.getFile();
      if (file == null) {
         return;
      }

      if (largeServerMessage.isDurable() && isReplicated()) {
         readLock();
         try {
            if (isReplicated() && replicator.isSynchronizing()) {
               synchronized (largeMessagesToDelete) {
                  largeMessagesToDelete.add(Long.valueOf(largeServerMessage.getMessageID()));
                  confirmLargeMessage(largeServerMessage);
               }
               return;
            }
         } finally {
            readUnLock();
         }
      }
      Runnable deleteAction = new Runnable() {
         @Override
         public void run() {
            try {
               readLock();
               try {
                  if (replicator != null) {
                     replicator.largeMessageDelete(largeServerMessage.getMessageID(), JournalStorageManager.this);
                  }
                  file.delete();

                  // The confirm could only be done after the actual delete is done
                  confirmLargeMessage(largeServerMessage);
               } finally {
                  readUnLock();
               }
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.journalErrorDeletingMessage(e, largeServerMessage.getMessageID());
            }
         }

      };

      if (executor == null) {
         deleteAction.run();
      } else {
         executor.execute(deleteAction);
      }
   }

   @Override
   public LargeServerMessage createLargeMessage() {
      return new LargeServerMessageImpl(this);
   }

   @Override
   public LargeServerMessage createLargeMessage(final long id, final Message message) throws Exception {
      readLock();
      try {
         if (isReplicated()) {
            replicator.largeMessageBegin(id);
         }

         LargeServerMessageImpl largeMessage = (LargeServerMessageImpl) createLargeMessage();

         largeMessage.copyHeadersAndProperties(message);

         largeMessage.setMessageID(id);

         // We do this here to avoid a case where the replication gets a list without this file
         // to avoid a race
         largeMessage.validateFile();

         if (largeMessage.isDurable()) {
            // We store a marker on the journal that the large file is pending
            long pendingRecordID = storePendingLargeMessage(id, LargeServerMessage.NO_PENDING_ID);

            largeMessage.setPendingRecordID(pendingRecordID);
         }

         return largeMessage;
      } finally {
         readUnLock();
      }
   }

   @Override
   public SequentialFile createFileForLargeMessage(final long messageID, LargeMessageExtension extension) {
      return largeMessagesFactory.createSequentialFile(messageID + extension.getExtension());
   }

   /**
    * Send an entire journal file to a replicating backup server.
    */
   private void sendJournalFile(JournalFile[] journalFiles, JournalContent type) throws Exception {
      for (JournalFile jf : journalFiles) {
         if (!started)
            return;
         replicator.syncJournalFile(jf, type);
      }
   }

   private JournalFile[] prepareJournalForCopy(Journal journal,
                                               JournalContent contentType,
                                               String nodeID,
                                               boolean autoFailBack) throws Exception {
      journal.forceMoveNextFile();
      JournalFile[] datafiles = journal.getDataFiles();
      replicator.sendStartSyncMessage(datafiles, contentType, nodeID, autoFailBack);
      return datafiles;
   }

   @Override
   public void startReplication(ReplicationManager replicationManager,
                                PagingManager pagingManager,
                                String nodeID,
                                final boolean autoFailBack,
                                long initialReplicationSyncTimeout) throws Exception {
      if (!started) {
         throw new IllegalStateException("JournalStorageManager must be started...");
      }
      assert replicationManager != null;

      if (!(messageJournal instanceof JournalImpl) || !(bindingsJournal instanceof JournalImpl)) {
         throw ActiveMQMessageBundle.BUNDLE.notJournalImpl();
      }

      // We first do a compact without any locks, to avoid copying unnecessary data over the network.
      // We do this without holding the storageManager lock, so the journal stays open while compact is being done
      originalMessageJournal.scheduleCompactAndBlock(-1);
      originalBindingsJournal.scheduleCompactAndBlock(-1);

      JournalFile[] messageFiles = null;
      JournalFile[] bindingsFiles = null;

      // We get a picture of the current sitaution on the large messages
      // and we send the current messages while more state is coming
      Map<Long, Pair<String, Long>> pendingLargeMessages = null;

      try {
         Map<SimpleString, Collection<Integer>> pageFilesToSync;
         storageManagerLock.writeLock().lock();
         try {
            if (isReplicated())
               throw new ActiveMQIllegalStateException("already replicating");
            replicator = replicationManager;

            if (!((JournalImpl) originalMessageJournal).flushAppendExecutor(10, TimeUnit.SECONDS)) {
               throw new Exception("Live message journal is busy");
            }

            if (!((JournalImpl) originalBindingsJournal).flushAppendExecutor(10, TimeUnit.SECONDS)) {
               throw new Exception("Live bindings journal is busy");
            }

            // Establishes lock
            originalMessageJournal.synchronizationLock();
            originalBindingsJournal.synchronizationLock();

            try {
               originalBindingsJournal.replicationSyncPreserveOldFiles();
               originalMessageJournal.replicationSyncPreserveOldFiles();

               pagingManager.lock();
               try {
                  pagingManager.disableCleanup();
                  messageFiles = prepareJournalForCopy(originalMessageJournal, JournalContent.MESSAGES, nodeID, autoFailBack);
                  bindingsFiles = prepareJournalForCopy(originalBindingsJournal, JournalContent.BINDINGS, nodeID, autoFailBack);
                  pageFilesToSync = getPageInformationForSync(pagingManager);
                  pendingLargeMessages = recoverPendingLargeMessages();
               } finally {
                  pagingManager.unlock();
               }
            } finally {
               originalMessageJournal.synchronizationUnlock();
               originalBindingsJournal.synchronizationUnlock();
            }
            bindingsJournal = new ReplicatedJournal(((byte) 0), originalBindingsJournal, replicator);
            messageJournal = new ReplicatedJournal((byte) 1, originalMessageJournal, replicator);

            // We need to send the list while locking otherwise part of the body might get sent too soon
            // it will send a list of IDs that we are allocating
            replicator.sendLargeMessageIdListMessage(pendingLargeMessages);
         } finally {
            storageManagerLock.writeLock().unlock();
         }

         sendJournalFile(messageFiles, JournalContent.MESSAGES);
         sendJournalFile(bindingsFiles, JournalContent.BINDINGS);
         sendLargeMessageFiles(pendingLargeMessages);
         sendPagesToBackup(pageFilesToSync, pagingManager);

         storageManagerLock.writeLock().lock();
         try {
            if (replicator != null) {
               replicator.sendSynchronizationDone(nodeID, initialReplicationSyncTimeout);
               performCachedLargeMessageDeletes();
            }
         } finally {
            storageManagerLock.writeLock().unlock();
         }
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.unableToStartReplication(e);
         stopReplication();
         throw e;
      } finally {
         // Re-enable compact and reclaim of journal files
         originalBindingsJournal.replicationSyncFinished();
         originalMessageJournal.replicationSyncFinished();
         pagingManager.resumeCleanup();
      }
   }

   private void sendLargeMessageFiles(final Map<Long, Pair<String, Long>> pendingLargeMessages) throws Exception {
      Iterator<Map.Entry<Long, Pair<String, Long>>> iter = pendingLargeMessages.entrySet().iterator();
      while (started && iter.hasNext()) {
         Map.Entry<Long, Pair<String, Long>> entry = iter.next();
         String fileName = entry.getValue().getA();
         final long id = entry.getKey();
         long size = entry.getValue().getB();
         SequentialFile seqFile = largeMessagesFactory.createSequentialFile(fileName);
         if (!seqFile.exists())
            continue;
         if (replicator != null) {
            replicator.syncLargeMessageFile(seqFile, size, id);
         } else {
            throw ActiveMQMessageBundle.BUNDLE.replicatorIsNull();
         }
      }
   }

   /**
    * @param pagingManager
    * @return
    * @throws Exception
    */
   private Map<SimpleString, Collection<Integer>> getPageInformationForSync(PagingManager pagingManager) throws Exception {
      Map<SimpleString, Collection<Integer>> info = new HashMap<>();
      for (SimpleString storeName : pagingManager.getStoreNames()) {
         PagingStore store = pagingManager.getPageStore(storeName);
         info.put(storeName, store.getCurrentIds());
         store.forceAnotherPage();
      }
      return info;
   }

   private void checkAndCreateDir(final File dir, final boolean create) {
      if (!dir.exists()) {
         if (create) {
            if (!dir.mkdirs()) {
               throw new IllegalStateException("Failed to create directory " + dir);
            }
         } else {
            throw ActiveMQMessageBundle.BUNDLE.cannotCreateDir(dir.getAbsolutePath());
         }
      }
   }

   /**
    * Sets a list of large message files into the replicationManager for synchronization.
    * <p>
    * Collects a list of existing large messages and their current size, passing re.
    * <p>
    * So we know how much of a given message to sync with the backup. Further data appends to the
    * messages will be replicated normally.
    *
    * @throws Exception
    */
   private Map<Long, Pair<String, Long>> recoverPendingLargeMessages() throws Exception {

      Map<Long, Pair<String, Long>> largeMessages = new HashMap<>();
      // only send durable messages... // listFiles append a "." to anything...
      List<String> filenames = largeMessagesFactory.listFiles("msg");

      List<Long> idList = new ArrayList<>();
      for (String filename : filenames) {
         Long id = getLargeMessageIdFromFilename(filename);
         if (!largeMessagesToDelete.contains(id)) {
            idList.add(id);
            SequentialFile seqFile = largeMessagesFactory.createSequentialFile(filename);
            long size = seqFile.size();
            largeMessages.put(id, new Pair<>(filename, size));
         }
      }

      return largeMessages;
   }

   /**
    * @param pageFilesToSync
    * @throws Exception
    */
   private void sendPagesToBackup(Map<SimpleString, Collection<Integer>> pageFilesToSync,
                                  PagingManager manager) throws Exception {
      for (Map.Entry<SimpleString, Collection<Integer>> entry : pageFilesToSync.entrySet()) {
         if (!started)
            return;
         PagingStore store = manager.getPageStore(entry.getKey());
         store.sendPages(replicator, entry.getValue());
      }
   }

   private long getLargeMessageIdFromFilename(String filename) {
      return Long.parseLong(filename.split("\\.")[0]);
   }

   /**
    * Stops replication by resetting replication-related fields to their 'unreplicated' state.
    */
   @Override
   public void stopReplication() {
      logger.trace("stopReplication()");
      storageManagerLock.writeLock().lock();
      try {
         if (replicator == null)
            return;
         bindingsJournal = originalBindingsJournal;
         messageJournal = originalMessageJournal;
         try {
            replicator.stop();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorStoppingReplicationManager(e);
         }
         replicator = null;
         // delete inside the writeLock. Avoids a lot of state checking and races with
         // startReplication.
         // This method should not be called under normal circumstances
         performCachedLargeMessageDeletes();
      } finally {
         storageManagerLock.writeLock().unlock();
      }
   }

   @Override
   public final void addBytesToLargeMessage(final SequentialFile file,
                                            final long messageId,
                                            final byte[] bytes) throws Exception {
      readLock();
      try {
         file.position(file.size());

         file.writeDirect(ByteBuffer.wrap(bytes), false);

         if (isReplicated()) {
            replicator.largeMessageWrite(messageId, bytes);
         }
      } finally {
         readUnLock();
      }
   }

   @Override
   public void injectMonitor(FileStoreMonitor monitor) throws Exception {
      if (journalFF != null) {
         monitor.addStore(journalFF.getDirectory());
      }
      if (largeMessagesFactory != null) {
         monitor.addStore(largeMessagesFactory.getDirectory());
      }
      if (bindingsFF != null) {
         monitor.addStore(bindingsFF.getDirectory());
      }
   }
}
