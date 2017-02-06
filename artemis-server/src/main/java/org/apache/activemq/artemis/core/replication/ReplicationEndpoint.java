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
package org.apache.activemq.artemis.core.replication;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.Journal.JournalState;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.impl.FileWrapperJournal;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreFactoryNIO;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.AbstractJournalStorageManager.JournalContent;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageInSync;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.ChannelHandler;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ActiveMQExceptionMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.BackupReplicationStartFailedMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationAddMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationAddTXMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationCommitMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationDeleteMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationDeleteTXMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLargeMessageBeginMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLargeMessageEndMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLargeMessageWriteMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationPageEventMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationPageWriteMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationPrepareMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationResponseMessageV2;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationStartSyncMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationStartSyncMessage.SyncDataType;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationSyncFileMessage;
import org.apache.activemq.artemis.core.replication.ReplicationManager.ADD_OPERATION_TYPE;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.cluster.qourum.SharedNothingBackupQuorum;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.SharedNothingBackupActivation;
import org.jboss.logging.Logger;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.maps.NonBlockingHashMapLong;

/**
 * Handles all the synchronization necessary for replication on the backup side (that is the
 * backup's side of the "remote backup" use case).
 */
public final class ReplicationEndpoint implements ChannelHandler, ActiveMQComponent {

   private static final Logger logger = Logger.getLogger(ReplicationEndpoint.class);

   private final IOCriticalErrorListener criticalErrorListener;
   private final ActiveMQServerImpl server;
   private final boolean wantedFailBack;
   private final SharedNothingBackupActivation activation;
   private final boolean noSync = false;
   private Channel channel;

   private Journal[] journals;
   private final JournalLoadInformation[] journalLoadInformation = new JournalLoadInformation[2];

   /**
    * Files reserved in each journal for synchronization of existing data from the 'live' server.
    */
   private final Map<JournalContent, Map<Long, JournalSyncFile>> filesReservedForSync = new HashMap<>();

   /**
    * Used to hold the real Journals before the backup is synchronized. This field should be
    * {@code null} on an up-to-date server.
    */
   private Map<JournalContent, Journal> journalsHolder = new HashMap<>();

   private StorageManager storageManager;

   private PagingManager pageManager;

   private final ConcurrentMap<SimpleString, NonBlockingHashMapLong<Page>> pageIndex = new NonBlockingHashMap<>();
   private final NonBlockingHashMapLong<ReplicatedLargeMessage> largeMessages = new NonBlockingHashMapLong<>();

   // Used on tests, to simulate failures on delete pages
   private boolean deletePages = true;
   private volatile boolean started;

   private SharedNothingBackupQuorum backupQuorum;

   private Executor executor;

   // Constructors --------------------------------------------------
   public ReplicationEndpoint(final ActiveMQServerImpl server,
                              IOCriticalErrorListener criticalErrorListener,
                              boolean wantedFailBack,
                              SharedNothingBackupActivation activation) {
      this.server = server;
      this.criticalErrorListener = criticalErrorListener;
      this.wantedFailBack = wantedFailBack;
      this.activation = activation;
   }

   // Public --------------------------------------------------------

   public synchronized void registerJournal(final byte id, final Journal journal) {
      if (journals == null || id >= journals.length) {
         Journal[] oldJournals = journals;
         journals = new Journal[id + 1];

         if (oldJournals != null) {
            System.arraycopy(oldJournals, 0, journals, 0, oldJournals.length);
         }
      }

      journals[id] = journal;
   }

   @Override
   public void handlePacket(final Packet packet) {
      if (logger.isTraceEnabled()) {
         logger.trace("handlePacket::handling " + packet);
      }
      PacketImpl response = new ReplicationResponseMessage();
      final byte type = packet.getType();

      try {
         if (!started) {
            if (logger.isTraceEnabled()) {
               logger.trace("handlePacket::ignoring " + packet);
            }

            return;
         }

         if (type == PacketImpl.REPLICATION_APPEND) {
            handleAppendAddRecord((ReplicationAddMessage) packet);
         } else if (type == PacketImpl.REPLICATION_APPEND_TX) {
            handleAppendAddTXRecord((ReplicationAddTXMessage) packet);
         } else if (type == PacketImpl.REPLICATION_DELETE) {
            handleAppendDelete((ReplicationDeleteMessage) packet);
         } else if (type == PacketImpl.REPLICATION_DELETE_TX) {
            handleAppendDeleteTX((ReplicationDeleteTXMessage) packet);
         } else if (type == PacketImpl.REPLICATION_PREPARE) {
            handlePrepare((ReplicationPrepareMessage) packet);
         } else if (type == PacketImpl.REPLICATION_COMMIT_ROLLBACK) {
            handleCommitRollback((ReplicationCommitMessage) packet);
         } else if (type == PacketImpl.REPLICATION_PAGE_WRITE) {
            handlePageWrite((ReplicationPageWriteMessage) packet);
         } else if (type == PacketImpl.REPLICATION_PAGE_EVENT) {
            handlePageEvent((ReplicationPageEventMessage) packet);
         } else if (type == PacketImpl.REPLICATION_LARGE_MESSAGE_BEGIN) {
            handleLargeMessageBegin((ReplicationLargeMessageBeginMessage) packet);
         } else if (type == PacketImpl.REPLICATION_LARGE_MESSAGE_WRITE) {
            handleLargeMessageWrite((ReplicationLargeMessageWriteMessage) packet);
         } else if (type == PacketImpl.REPLICATION_LARGE_MESSAGE_END) {
            handleLargeMessageEnd((ReplicationLargeMessageEndMessage) packet);
         } else if (type == PacketImpl.REPLICATION_START_FINISH_SYNC) {
            response = handleStartReplicationSynchronization((ReplicationStartSyncMessage) packet);
         } else if (type == PacketImpl.REPLICATION_SYNC_FILE) {
            handleReplicationSynchronization((ReplicationSyncFileMessage) packet);
         } else if (type == PacketImpl.REPLICATION_SCHEDULED_FAILOVER) {
            handleLiveStopping((ReplicationLiveIsStoppingMessage) packet);
         } else if (type == PacketImpl.BACKUP_REGISTRATION_FAILED) {
            handleFatalError((BackupReplicationStartFailedMessage) packet);
         } else {
            ActiveMQServerLogger.LOGGER.invalidPacketForReplication(packet);
         }
      } catch (ActiveMQException e) {
         ActiveMQServerLogger.LOGGER.errorHandlingReplicationPacket(e, packet);
         response = new ActiveMQExceptionMessage(e);
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorHandlingReplicationPacket(e, packet);
         response = new ActiveMQExceptionMessage(ActiveMQMessageBundle.BUNDLE.replicationUnhandledError(e));
      }
      channel.send(response);
   }

   /**
    * @param packet
    */
   private void handleFatalError(BackupReplicationStartFailedMessage packet) {
      ActiveMQServerLogger.LOGGER.errorStartingReplication(packet.getRegistrationProblem());
      server.stopTheServer(false);
   }

   /**
    * @param packet
    * @throws ActiveMQException
    */
   private void handleLiveStopping(ReplicationLiveIsStoppingMessage packet) throws ActiveMQException {
      activation.remoteFailOver(packet.isFinalMessage());
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public synchronized void start() throws Exception {
      Configuration config = server.getConfiguration();
      try {
         storageManager = server.getStorageManager();
         storageManager.start();

         server.getManagementService().setStorageManager(storageManager);

         journalsHolder.put(JournalContent.BINDINGS, storageManager.getBindingsJournal());
         journalsHolder.put(JournalContent.MESSAGES, storageManager.getMessageJournal());

         for (JournalContent jc : EnumSet.allOf(JournalContent.class)) {
            filesReservedForSync.put(jc, new HashMap<Long, JournalSyncFile>());
            // We only need to load internal structures on the backup...
            journalLoadInformation[jc.typeByte] = journalsHolder.get(jc).loadSyncOnly(JournalState.SYNCING);
         }

         pageManager = new PagingManagerImpl(new PagingStoreFactoryNIO(storageManager, config.getPagingLocation(), config.getJournalBufferSize_NIO(), server.getScheduledPool(), server.getExecutorFactory(), config.isJournalSyncNonTransactional(), criticalErrorListener), server.getAddressSettingsRepository());

         pageManager.start();

         started = true;
      } catch (Exception e) {
         if (server.isStarted())
            throw e;
      }
   }

   @Override
   public synchronized void stop() throws Exception {
      if (!started) {
         return;
      }

      // Channel may be null if there isn't a connection to a live server
      if (channel != null) {
         channel.close();
      }

      for (ReplicatedLargeMessage largeMessage : largeMessages.values()) {
         largeMessage.releaseResources();
      }
      largeMessages.clear();

      for (Entry<JournalContent, Map<Long, JournalSyncFile>> entry : filesReservedForSync.entrySet()) {
         for (JournalSyncFile filesReserved : entry.getValue().values()) {
            filesReserved.close();
         }
      }

      filesReservedForSync.clear();
      if (journals != null) {
         for (Journal j : journals) {
            if (j instanceof FileWrapperJournal)
               j.stop();
         }
      }

      pageIndex.forEach((k, map) -> map.forEach((id, page) -> {
         try {
            page.sync();
            page.close(false);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorClosingPageOnReplication(e);
         }
      }));
      pageManager.stop();

      pageIndex.clear();
      final CountDownLatch latch = new CountDownLatch(1);
      executor.execute(new Runnable() {

         @Override
         public void run() {
            latch.countDown();
         }
      });
      latch.await(30, TimeUnit.SECONDS);

      // Storage needs to be the last to stop
      storageManager.stop();

      started = false;
   }

   public Channel getChannel() {
      return channel;
   }

   public void setChannel(final Channel channel) {
      this.channel = channel;
   }

   private void finishSynchronization(String liveID) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("finishSynchronization::" + liveID);
      }
      for (JournalContent jc : EnumSet.allOf(JournalContent.class)) {
         Journal journal = journalsHolder.remove(jc);
         journal.synchronizationLock();
         try {
            // files should be already in place.
            filesReservedForSync.remove(jc);
            registerJournal(jc.typeByte, journal);
            journal.stop();
            journal.start();
            journal.loadSyncOnly(JournalState.SYNCING_UP_TO_DATE);
         } finally {
            journal.synchronizationUnlock();
         }
      }
      ByteBuffer buffer = ByteBuffer.allocate(4 * 1024);
      for (Entry<Long, ReplicatedLargeMessage> entry : largeMessages.entrySet()) {
         ReplicatedLargeMessage lm = entry.getValue();
         if (lm instanceof LargeServerMessageInSync) {
            LargeServerMessageInSync lmSync = (LargeServerMessageInSync) lm;
            lmSync.joinSyncedData(buffer);
         }
      }

      journalsHolder = null;
      backupQuorum.liveIDSet(liveID);
      activation.setRemoteBackupUpToDate();
      ActiveMQServerLogger.LOGGER.backupServerSynched(server);
      return;
   }

   /**
    * Receives 'raw' journal/page/large-message data from live server for synchronization of logs.
    *
    * @param msg
    * @throws Exception
    */
   private void handleReplicationSynchronization(ReplicationSyncFileMessage msg) throws Exception {
      Long id = Long.valueOf(msg.getId());
      byte[] data = msg.getData();
      SequentialFile channel1;
      switch (msg.getFileType()) {
         case LARGE_MESSAGE: {
            ReplicatedLargeMessage largeMessage = lookupLargeMessage(id, false, false);
            if (!(largeMessage instanceof LargeServerMessageInSync)) {
               ActiveMQServerLogger.LOGGER.largeMessageIncompatible();
               return;
            }
            LargeServerMessageInSync largeMessageInSync = (LargeServerMessageInSync) largeMessage;
            channel1 = largeMessageInSync.getSyncFile();
            break;
         }
         case PAGE: {
            Page page = getPage(msg.getPageStore(), (int) msg.getId());
            channel1 = page.getFile();
            break;
         }
         case JOURNAL: {
            JournalSyncFile journalSyncFile = filesReservedForSync.get(msg.getJournalContent()).get(id);
            FileChannel channel2 = journalSyncFile.getChannel();
            if (data == null) {
               channel2.close();
               return;
            }
            channel2.write(ByteBuffer.wrap(data));
            return;
         }
         default:
            throw ActiveMQMessageBundle.BUNDLE.replicationUnhandledFileType(msg.getFileType());
      }

      if (data == null) {
         return;
      }

      if (!channel1.isOpen()) {
         channel1.open();
      }
      channel1.writeDirect(ByteBuffer.wrap(data), true);
   }

   /**
    * Reserves files (with the given fileID) in the specified journal, and places a
    * {@link FileWrapperJournal} in place to store messages while synchronization is going on.
    *
    * @param packet
    * @return if the incoming packet indicates the synchronization is finished then return an acknowledgement otherwise
    * return an empty response
    * @throws Exception
    */
   private ReplicationResponseMessageV2 handleStartReplicationSynchronization(final ReplicationStartSyncMessage packet) throws Exception {

      if (logger.isTraceEnabled()) {
         logger.trace("handleStartReplicationSynchronization:: nodeID = " + packet);
      }
      ReplicationResponseMessageV2 replicationResponseMessage = new ReplicationResponseMessageV2();
      if (!started)
         return replicationResponseMessage;

      if (packet.isSynchronizationFinished()) {
         finishSynchronization(packet.getNodeID());
         replicationResponseMessage.setSynchronizationIsFinishedAcknowledgement(true);
         return replicationResponseMessage;
      }

      switch (packet.getDataType()) {
         case LargeMessages:
            for (long msgID : packet.getFileIds()) {
               createLargeMessage(msgID, true);
            }
            break;
         case JournalBindings:
         case JournalMessages:
            if (wantedFailBack && !packet.isServerToFailBack()) {
               ActiveMQServerLogger.LOGGER.autoFailBackDenied();
            }

            final JournalContent journalContent = SyncDataType.getJournalContentType(packet.getDataType());
            final Journal journal = journalsHolder.get(journalContent);

            if (packet.getNodeID() != null) {
               // At the start of replication, we still do not know which is the nodeID that the live uses.
               // This is the point where the backup gets this information.
               backupQuorum.liveIDSet(packet.getNodeID());
            }
            Map<Long, JournalSyncFile> mapToFill = filesReservedForSync.get(journalContent);

            for (Entry<Long, JournalFile> entry : journal.createFilesForBackupSync(packet.getFileIds()).entrySet()) {
               mapToFill.put(entry.getKey(), new JournalSyncFile(entry.getValue()));
            }
            FileWrapperJournal syncJournal = new FileWrapperJournal(journal);
            registerJournal(journalContent.typeByte, syncJournal);
            break;
         default:
            throw ActiveMQMessageBundle.BUNDLE.replicationUnhandledDataType();
      }

      return replicationResponseMessage;
   }

   private void handleLargeMessageEnd(final ReplicationLargeMessageEndMessage packet) {
      if (logger.isTraceEnabled()) {
         logger.trace("handleLargeMessageEnd on " + packet.getMessageId());
      }
      final ReplicatedLargeMessage message = lookupLargeMessage(packet.getMessageId(), true, false);
      if (message != null) {
         executor.execute(new Runnable() {
            @Override
            public void run() {
               try {
                  if (logger.isTraceEnabled()) {
                     logger.trace("Deleting LargeMessage " + packet.getMessageId() + " on the executor @ handleLargeMessageEnd");
                  }
                  message.deleteFile();
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.errorDeletingLargeMessage(e, packet.getMessageId());
               }
            }
         });
      }
   }

   /**
    * @param packet
    */
   private void handleLargeMessageWrite(final ReplicationLargeMessageWriteMessage packet) throws Exception {
      ReplicatedLargeMessage message = lookupLargeMessage(packet.getMessageId(), false, true);
      if (message != null) {
         message.addBytes(packet.getBody());
      }
   }

   private ReplicatedLargeMessage lookupLargeMessage(final long messageId,
                                                     final boolean delete,
                                                     final boolean createIfNotExists) {
      ReplicatedLargeMessage message;

      if (delete) {
         message = largeMessages.remove(messageId);
      } else {
         message = largeMessages.get(messageId);
         if (message == null) {
            if (createIfNotExists) {
               createLargeMessage(messageId, false);
               message = largeMessages.get(messageId);
            } else {
               // No warnings if it's a delete, as duplicate deletes may be sent repeatedly.
               ActiveMQServerLogger.LOGGER.largeMessageNotAvailable(messageId);
            }
         }
      }

      return message;

   }

   /**
    * @param packet
    */
   private void handleLargeMessageBegin(final ReplicationLargeMessageBeginMessage packet) {
      final long id = packet.getMessageId();
      createLargeMessage(id, false);
      if (logger.isTraceEnabled()) {
         logger.trace("Receiving Large Message Begin " + id + " on backup");
      }
   }

   private void createLargeMessage(final long id, boolean liveToBackupSync) {
      ReplicatedLargeMessage msg;
      if (liveToBackupSync) {
         msg = new LargeServerMessageInSync(storageManager);
      } else {
         msg = storageManager.createLargeMessage();
      }

      msg.setDurable(true);
      msg.setMessageID(id);
      largeMessages.put(id, msg);
   }

   /**
    * @param packet
    */
   private void handleCommitRollback(final ReplicationCommitMessage packet) throws Exception {
      Journal journalToUse = getJournal(packet.getJournalID());
      if (packet.isRollback()) {
         journalToUse.appendRollbackRecord(packet.getTxId(), noSync);
      } else {
         journalToUse.appendCommitRecord(packet.getTxId(), noSync);
      }
   }

   /**
    * @param packet
    */
   private void handlePrepare(final ReplicationPrepareMessage packet) throws Exception {
      Journal journalToUse = getJournal(packet.getJournalID());
      journalToUse.appendPrepareRecord(packet.getTxId(), packet.getRecordData(), noSync);
   }

   /**
    * @param packet
    */
   private void handleAppendDeleteTX(final ReplicationDeleteTXMessage packet) throws Exception {
      Journal journalToUse = getJournal(packet.getJournalID());

      journalToUse.appendDeleteRecordTransactional(packet.getTxId(), packet.getId(), packet.getRecordData());
   }

   /**
    * @param packet
    */
   private void handleAppendDelete(final ReplicationDeleteMessage packet) throws Exception {
      Journal journalToUse = getJournal(packet.getJournalID());
      journalToUse.appendDeleteRecord(packet.getId(), noSync);
   }

   /**
    * @param packet
    */
   private void handleAppendAddTXRecord(final ReplicationAddTXMessage packet) throws Exception {
      Journal journalToUse = getJournal(packet.getJournalID());

      if (packet.getOperation() == ADD_OPERATION_TYPE.UPDATE) {
         journalToUse.appendUpdateRecordTransactional(packet.getTxId(), packet.getId(), packet.getRecordType(), packet.getRecordData());
      } else {
         journalToUse.appendAddRecordTransactional(packet.getTxId(), packet.getId(), packet.getRecordType(), packet.getRecordData());
      }
   }

   /**
    * @param packet
    * @throws Exception
    */
   private void handleAppendAddRecord(final ReplicationAddMessage packet) throws Exception {
      Journal journalToUse = getJournal(packet.getJournalID());
      if (packet.getRecord() == ADD_OPERATION_TYPE.UPDATE) {
         if (logger.isTraceEnabled()) {
            logger.trace("Endpoint appendUpdate id = " + packet.getId());
         }
         journalToUse.appendUpdateRecord(packet.getId(), packet.getJournalRecordType(), packet.getRecordData(), noSync);
      } else {
         if (logger.isTraceEnabled()) {
            logger.trace("Endpoint append id = " + packet.getId());
         }
         journalToUse.appendAddRecord(packet.getId(), packet.getJournalRecordType(), packet.getRecordData(), noSync);
      }
   }

   /**
    * @param packet
    */
   private void handlePageEvent(final ReplicationPageEventMessage packet) throws Exception {
      NonBlockingHashMapLong<Page> pages = getPageMap(packet.getStoreName());

      Page page = pages.remove(packet.getPageNumber());

      if (page == null) {
         page = getPage(packet.getStoreName(), packet.getPageNumber());
      }

      if (page != null) {
         if (packet.isDelete()) {
            if (deletePages) {
               page.delete(null);
            }
         } else {
            page.close(false);
         }
      }

   }

   /**
    * @param packet
    */
   private void handlePageWrite(final ReplicationPageWriteMessage packet) throws Exception {
      PagedMessage pgdMessage = packet.getPagedMessage();
      pgdMessage.initMessage(storageManager);
      ServerMessage msg = pgdMessage.getMessage();
      Page page = getPage(msg.getAddress(), packet.getPageNumber());
      page.write(pgdMessage);
   }

   private NonBlockingHashMapLong<Page> getPageMap(final SimpleString storeName) {
      NonBlockingHashMapLong<Page> resultIndex = pageIndex.get(storeName);

      if (resultIndex == null) {
         resultIndex = new NonBlockingHashMapLong<>();
         NonBlockingHashMapLong<Page> mapResult = pageIndex.putIfAbsent(storeName, resultIndex);
         if (mapResult != null) {
            resultIndex = mapResult;
         }
      }

      return resultIndex;
   }

   private Page getPage(final SimpleString storeName, final int pageId) throws Exception {
      NonBlockingHashMapLong<Page> map = getPageMap(storeName);

      Page page = map.get(pageId);

      if (page == null) {
         page = newPage(pageId, storeName, map);
      }

      return page;
   }

   /**
    * @param pageId
    * @param map
    * @return
    */
   private synchronized Page newPage(final int pageId,
                                     final SimpleString storeName,
                                     final NonBlockingHashMapLong<Page> map) throws Exception {
      Page page = map.get(pageId);

      if (page == null) {
         page = pageManager.getPageStore(storeName).createPage(pageId);
         page.open();
         map.put(pageId, page);
      }

      return page;
   }

   /**
    * @param journalID
    * @return
    */
   private Journal getJournal(final byte journalID) {
      return journals[journalID];
   }

   public static final class JournalSyncFile {

      private FileChannel channel;
      private final File file;
      private FileOutputStream fos;

      public JournalSyncFile(JournalFile jFile) throws Exception {
         SequentialFile seqFile = jFile.getFile();
         file = seqFile.getJavaFile();
         seqFile.close();
      }

      synchronized FileChannel getChannel() throws Exception {
         if (channel == null) {
            fos = new FileOutputStream(file);
            channel = fos.getChannel();
         }
         return channel;
      }

      synchronized void close() throws IOException {
         if (fos != null)
            fos.close();
         if (channel != null)
            channel.close();
      }

      @Override
      public String toString() {
         return "JournalSyncFile(file=" + file.getAbsolutePath() + ")";
      }
   }

   /**
    * Sets the quorumManager used by the server in the replicationEndpoint. It is used to inform the
    * backup server of the live's nodeID.
    *
    * @param backupQuorum
    */
   public void setBackupQuorum(SharedNothingBackupQuorum backupQuorum) {
      this.backupQuorum = backupQuorum;
   }

   /**
    * @param executor2
    */
   public void setExecutor(Executor executor2) {
      this.executor = executor2;
   }
}
