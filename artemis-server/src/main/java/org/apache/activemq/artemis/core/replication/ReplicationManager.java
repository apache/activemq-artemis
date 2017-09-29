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

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.SessionFailureListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.persistence.impl.journal.AbstractJournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.ChannelHandler;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.ChannelImpl.CHANNEL_ID;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationAddMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationAddTXMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationCommitMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationDeleteMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationDeleteTXMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLargeMessageBeginMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLargeMessageEndMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLargeMessageWriteMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage.LiveStopping;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationPageEventMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationPageWriteMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationPrepareMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationResponseMessageV2;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationStartSyncMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationSyncFileMessage;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.jboss.logging.Logger;

/**
 * Manages replication tasks on the live server (that is the live server side of a "remote backup"
 * use case).
 * <p>
 * Its equivalent in the backup server is {@link ReplicationEndpoint}.
 *
 * @see ReplicationEndpoint
 */
public final class ReplicationManager implements ActiveMQComponent {

   private static final Logger logger = Logger.getLogger(ReplicationManager.class);

   public enum ADD_OPERATION_TYPE {
      UPDATE {
         @Override
         public boolean toBoolean() {
            return true;
         }
      }, ADD {
         @Override
         public boolean toBoolean() {
            return false;
         }
      };

      public abstract boolean toBoolean();

      public static ADD_OPERATION_TYPE toOperation(boolean isUpdate) {
         return isUpdate ? UPDATE : ADD;
      }
   }

   private final ResponseHandler responseHandler = new ResponseHandler();

   private final Channel replicatingChannel;

   private boolean started;

   private volatile boolean enabled;

   private final AtomicBoolean writable = new AtomicBoolean(true);

   private final Queue<OperationContext> pendingTokens = new ConcurrentLinkedQueue<>();

   private final ExecutorFactory executorFactory;

   private final Executor replicationStream;

   private SessionFailureListener failureListener;

   private CoreRemotingConnection remotingConnection;

   private final long timeout;

   private final long initialReplicationSyncTimeout;

   private volatile boolean inSync = true;

   private final ReusableLatch synchronizationIsFinishedAcknowledgement = new ReusableLatch(0);

   /**
    * @param remotingConnection
    */
   public ReplicationManager(CoreRemotingConnection remotingConnection,
                             final long timeout,
                             final long initialReplicationSyncTimeout,
                             final ExecutorFactory executorFactory) {
      this.executorFactory = executorFactory;
      this.initialReplicationSyncTimeout = initialReplicationSyncTimeout;
      this.replicatingChannel = remotingConnection.getChannel(CHANNEL_ID.REPLICATION.id, -1);
      this.remotingConnection = remotingConnection;
      this.replicationStream = executorFactory.getExecutor();
      this.timeout = timeout;
   }

   public void appendUpdateRecord(final byte journalID,
                                  final ADD_OPERATION_TYPE operation,
                                  final long id,
                                  final byte recordType,
                                  final Persister persister,
                                  final Object record) throws Exception {
      if (enabled) {
         sendReplicatePacket(new ReplicationAddMessage(journalID, operation, id, recordType, persister, record));
      }
   }

   public void appendDeleteRecord(final byte journalID, final long id) throws Exception {
      if (enabled) {
         sendReplicatePacket(new ReplicationDeleteMessage(journalID, id));
      }
   }

   public void appendAddRecordTransactional(final byte journalID,
                                            final ADD_OPERATION_TYPE operation,
                                            final long txID,
                                            final long id,
                                            final byte recordType,
                                            final Persister persister,
                                            final Object record) throws Exception {
      if (enabled) {
         sendReplicatePacket(new ReplicationAddTXMessage(journalID, operation, txID, id, recordType, persister, record));
      }
   }

   public void appendCommitRecord(final byte journalID,
                                  final long txID,
                                  boolean sync,
                                  final boolean lineUp) throws Exception {
      if (enabled) {
         sendReplicatePacket(new ReplicationCommitMessage(journalID, false, txID), lineUp);
      }
   }

   public void appendDeleteRecordTransactional(final byte journalID,
                                               final long txID,
                                               final long id,
                                               final EncodingSupport record) throws Exception {
      if (enabled) {
         sendReplicatePacket(new ReplicationDeleteTXMessage(journalID, txID, id, record));
      }
   }

   public void appendDeleteRecordTransactional(final byte journalID, final long txID, final long id) throws Exception {
      if (enabled) {
         sendReplicatePacket(new ReplicationDeleteTXMessage(journalID, txID, id, NullEncoding.instance));
      }
   }

   public void appendPrepareRecord(final byte journalID,
                                   final long txID,
                                   final EncodingSupport transactionData) throws Exception {
      if (enabled) {
         sendReplicatePacket(new ReplicationPrepareMessage(journalID, txID, transactionData));
      }
   }

   public void appendRollbackRecord(final byte journalID, final long txID) throws Exception {
      if (enabled) {
         sendReplicatePacket(new ReplicationCommitMessage(journalID, true, txID));
      }
   }

   /**
    * @param storeName
    * @param pageNumber
    */
   public void pageClosed(final SimpleString storeName, final int pageNumber) {
      if (enabled) {
         sendReplicatePacket(new ReplicationPageEventMessage(storeName, pageNumber, false));
      }
   }

   public void pageDeleted(final SimpleString storeName, final int pageNumber) {
      if (enabled) {
         sendReplicatePacket(new ReplicationPageEventMessage(storeName, pageNumber, true));
      }
   }

   public void pageWrite(final PagedMessage message, final int pageNumber) {
      if (enabled) {
         sendReplicatePacket(new ReplicationPageWriteMessage(message, pageNumber));
      }
   }

   public void largeMessageBegin(final long messageId) {
      if (enabled) {
         sendReplicatePacket(new ReplicationLargeMessageBeginMessage(messageId));
      }
   }

   //we pass in storageManager to generate ID only if enabled
   public void largeMessageDelete(final Long messageId, JournalStorageManager storageManager) {
      if (enabled) {
         long pendingRecordID = storageManager.generateID();
         sendReplicatePacket(new ReplicationLargeMessageEndMessage(messageId, pendingRecordID));
      }
   }

   public void largeMessageWrite(final long messageId, final byte[] body) {
      if (enabled) {
         sendReplicatePacket(new ReplicationLargeMessageWriteMessage(messageId, body));
      }
   }

   @Override
   public synchronized boolean isStarted() {
      return started;
   }

   @Override
   public synchronized void start() throws ActiveMQException {
      if (started) {
         throw new IllegalStateException("ReplicationManager is already started");
      }

      replicatingChannel.setHandler(responseHandler);
      failureListener = new ReplicatedSessionFailureListener();
      remotingConnection.addFailureListener(failureListener);

      started = true;

      enabled = true;
   }

   @Override
   public void stop() throws Exception {
      synchronized (this) {
         if (!started) {
            logger.trace("Stopping being ignored as it hasn't been started");
            return;
         }
      }

      // This is to avoid the write holding a lock while we are trying to close it
      if (replicatingChannel != null) {
         replicatingChannel.close();
         replicatingChannel.getConnection().getTransportConnection().fireReady(true);
      }

      enabled = false;
      writable.set(true);
      clearReplicationTokens();

      RemotingConnection toStop = remotingConnection;
      if (toStop != null) {
         toStop.removeFailureListener(failureListener);
      }
      remotingConnection = null;
      started = false;
   }

   /**
    * Completes any pending operations.
    * <p>
    * This can be necessary in case the live loses connection to the backup (network failure, or
    * backup crashing).
    */
   public void clearReplicationTokens() {
      logger.trace("clearReplicationTokens initiating");
      while (!pendingTokens.isEmpty()) {
         OperationContext ctx = pendingTokens.poll();
         logger.trace("Calling ctx.replicationDone()");
         try {
            ctx.replicationDone();
         } catch (Throwable e) {
            ActiveMQServerLogger.LOGGER.errorCompletingCallbackOnReplicationManager(e);
         }
      }
      logger.trace("clearReplicationTokens finished");
   }

   /**
    * A list of tokens that are still waiting for replications to be completed
    */
   public Set<OperationContext> getActiveTokens() {

      LinkedHashSet<OperationContext> activeContexts = new LinkedHashSet<>();

      // The same context will be replicated on the pending tokens...
      // as the multiple operations will be replicated on the same context

      for (OperationContext ctx : pendingTokens) {
         activeContexts.add(ctx);
      }

      return activeContexts;

   }

   private OperationContext sendReplicatePacket(final Packet packet) {
      return sendReplicatePacket(packet, true);
   }

   private OperationContext sendReplicatePacket(final Packet packet, boolean lineUp) {
      if (!enabled) {
         packet.release();
         return null;
      }

      final OperationContext repliToken = OperationContextImpl.getContext(executorFactory);
      if (lineUp) {
         repliToken.replicationLineUp();
      }

      replicationStream.execute(() -> {
         if (enabled) {
            pendingTokens.add(repliToken);
            flowControl(packet.expectedEncodeSize());
            replicatingChannel.send(packet);
         } else {
            packet.release();
            repliToken.replicationDone();
         }
      });

      return repliToken;
   }

   /**
    * This was written as a refactoring of sendReplicatePacket.
    * In case you refactor this in any way, this method must hold a lock on replication lock. .
    */
   private boolean flowControl(int size) {
      boolean flowWorked = replicatingChannel.getConnection().blockUntilWritable(size, timeout);

      if (!flowWorked) {
         try {
            ActiveMQServerLogger.LOGGER.slowReplicationResponse();
            stop();
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
         }
      }

      return flowWorked;
   }

   /**
    * @throws IllegalStateException By default, all replicated packets generate a replicated
    *                               response. If your packets are triggering this exception, it may be because the
    *                               packets were not sent with {@link #sendReplicatePacket(Packet)}.
    */
   private void replicated() {
      OperationContext ctx = pendingTokens.poll();

      if (ctx == null) {
         ActiveMQServerLogger.LOGGER.missingReplicationTokenOnQueue();
         return;
      }
      ctx.replicationDone();
   }

   // Inner classes -------------------------------------------------

   private final class ReplicatedSessionFailureListener implements SessionFailureListener {

      @Override
      public void connectionFailed(final ActiveMQException me, boolean failedOver) {
         if (me.getType() == ActiveMQExceptionType.DISCONNECTED) {
            // Backup has shut down - no need to log a stack trace
            ActiveMQServerLogger.LOGGER.replicationStopOnBackupShutdown();
         } else {
            ActiveMQServerLogger.LOGGER.replicationStopOnBackupFail(me);
         }

         try {
            stop();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorStoppingReplication(e);
         }
      }

      @Override
      public void connectionFailed(final ActiveMQException me, boolean failedOver, String scaleDownTargetNodeID) {
         connectionFailed(me, failedOver);
      }

      @Override
      public void beforeReconnect(final ActiveMQException me) {
      }
   }

   private final class ResponseHandler implements ChannelHandler {

      @Override
      public void handlePacket(final Packet packet) {
         if (packet.getType() == PacketImpl.REPLICATION_RESPONSE || packet.getType() == PacketImpl.REPLICATION_RESPONSE_V2) {
            replicated();
            if (packet.getType() == PacketImpl.REPLICATION_RESPONSE_V2) {
               ReplicationResponseMessageV2 replicationResponseMessage = (ReplicationResponseMessageV2) packet;
               if (replicationResponseMessage.isSynchronizationIsFinishedAcknowledgement()) {
                  synchronizationIsFinishedAcknowledgement.countDown();
               }
            }
         }
      }

   }

   private static final class NullEncoding implements EncodingSupport {

      static final NullEncoding instance = new NullEncoding();

      @Override
      public void decode(final ActiveMQBuffer buffer) {
      }

      @Override
      public void encode(final ActiveMQBuffer buffer) {
      }

      @Override
      public int getEncodeSize() {
         return 0;
      }
   }

   /**
    * Sends the whole content of the file to be duplicated.
    *
    * @throws ActiveMQException
    * @throws Exception
    */
   public void syncJournalFile(JournalFile jf, AbstractJournalStorageManager.JournalContent content) throws Exception {
      if (!enabled) {
         return;
      }
      SequentialFile file = jf.getFile().cloneFile();
      try {
         ActiveMQServerLogger.LOGGER.replicaSyncFile(file, file.size());
         sendLargeFile(content, null, jf.getFileID(), file, Long.MAX_VALUE);
      } finally {
         if (file.isOpen())
            file.close();
      }
   }

   public void syncLargeMessageFile(SequentialFile file, long size, long id) throws Exception {
      if (enabled) {
         sendLargeFile(null, null, id, file, size);
      }
   }

   public void syncPages(SequentialFile file, long id, SimpleString queueName) throws Exception {
      if (enabled)
         sendLargeFile(null, queueName, id, file, Long.MAX_VALUE);
   }

   private class FlushAction implements Runnable {

      ReusableLatch latch = new ReusableLatch(1);

      public void reset() {
         latch.setCount(1);
      }

      public boolean await(long timeout, TimeUnit unit) throws Exception {
         return latch.await(timeout, unit);
      }

      @Override
      public void run() {
         latch.countDown();
      }
   }

   /**
    * Sends large files in reasonably sized chunks to the backup during replication synchronization.
    *
    * @param content        journal type or {@code null} for large-messages and pages
    * @param pageStore      page store name for pages, or {@code null} otherwise
    * @param id             journal file id or (large) message id
    * @param file
    * @param maxBytesToSend maximum number of bytes to read and send from the file
    * @throws Exception
    */
   private void sendLargeFile(AbstractJournalStorageManager.JournalContent content,
                              SimpleString pageStore,
                              final long id,
                              SequentialFile file,
                              long maxBytesToSend) throws Exception {
      if (!enabled)
         return;
      if (!file.isOpen()) {
         file.open();
      }
      int size = 32 * 1024;

      int flowControlSize = 10;

      int packetsSent = 0;
      FlushAction action = new FlushAction();

      try {
         try (FileInputStream fis = new FileInputStream(file.getJavaFile()); FileChannel channel = fis.getChannel()) {

            // We can afford having a single buffer here for this entire loop
            // because sendReplicatePacket will encode the packet as a NettyBuffer
            // through ActiveMQBuffer class leaving this buffer free to be reused on the next copy
            while (true) {
               final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(size, size);
               buffer.clear();
               ByteBuffer byteBuffer = buffer.writerIndex(size).readerIndex(0).nioBuffer();
               final int bytesRead = channel.read(byteBuffer);
               int toSend = bytesRead;
               if (bytesRead > 0) {
                  if (bytesRead >= maxBytesToSend) {
                     toSend = (int) maxBytesToSend;
                     maxBytesToSend = 0;
                  } else {
                     maxBytesToSend = maxBytesToSend - bytesRead;
                  }
               }
               logger.debug("sending " + buffer.writerIndex() + " bytes on file " + file.getFileName());
               // sending -1 or 0 bytes will close the file at the backup
               // We cannot simply send everything of a file through the executor,
               // otherwise we would run out of memory.
               // so we don't use the executor here
               sendReplicatePacket(new ReplicationSyncFileMessage(content, pageStore, id, toSend, buffer), true);
               packetsSent++;

               if (packetsSent % flowControlSize == 0) {
                  flushReplicationStream(action);
               }
               if (bytesRead == -1 || bytesRead == 0 || maxBytesToSend == 0)
                  break;
            }
         }
         flushReplicationStream(action);
      } finally {
         if (file.isOpen())
            file.close();
      }
   }

   private void flushReplicationStream(FlushAction action) throws Exception {
      action.reset();
      replicationStream.execute(action);
      if (!action.await(this.timeout, TimeUnit.MILLISECONDS)) {
         throw ActiveMQMessageBundle.BUNDLE.replicationSynchronizationTimeout(initialReplicationSyncTimeout);
      }
   }

   /**
    * Reserve the following fileIDs in the backup server.
    *
    * @param datafiles
    * @param contentType
    * @throws ActiveMQException
    */
   public void sendStartSyncMessage(JournalFile[] datafiles,
                                    AbstractJournalStorageManager.JournalContent contentType,
                                    String nodeID,
                                    boolean allowsAutoFailBack) throws ActiveMQException {
      if (enabled)
         sendReplicatePacket(new ReplicationStartSyncMessage(datafiles, contentType, nodeID, allowsAutoFailBack));
   }

   /**
    * Informs backup that data synchronization is done.
    * <p>
    * So if 'live' fails, the (up-to-date) backup now may take over its duties. To do so, it must
    * know which is the live's {@code nodeID}.
    *
    * @param nodeID
    */
   public void sendSynchronizationDone(String nodeID, long initialReplicationSyncTimeout) {
      if (enabled) {

         if (logger.isTraceEnabled()) {
            logger.trace("sendSynchronizationDone ::" + nodeID + ", " + initialReplicationSyncTimeout);
         }

         synchronizationIsFinishedAcknowledgement.countUp();
         sendReplicatePacket(new ReplicationStartSyncMessage(nodeID));
         try {
            if (!synchronizationIsFinishedAcknowledgement.await(initialReplicationSyncTimeout)) {
               logger.trace("sendSynchronizationDone wasn't finished in time");
               throw ActiveMQMessageBundle.BUNDLE.replicationSynchronizationTimeout(initialReplicationSyncTimeout);
            }
         } catch (InterruptedException e) {
            logger.debug(e);
         }
         inSync = false;

         logger.trace("sendSynchronizationDone finished");
      }
   }

   /**
    * Reserves several LargeMessage IDs in the backup.
    * <p>
    * Doing this before hand removes the need of synchronizing large-message deletes with the
    * largeMessageSyncList.
    *
    * @param largeMessages
    */
   public void sendLargeMessageIdListMessage(Map<Long, Pair<String, Long>> largeMessages) {
      ArrayList<Long> idsToSend;
      idsToSend = new ArrayList<>(largeMessages.keySet());

      if (enabled)
         sendReplicatePacket(new ReplicationStartSyncMessage(idsToSend));
   }

   /**
    * Notifies the backup that the live server is stopping.
    * <p>
    * This notification allows the backup to skip quorum voting (or any other measure to avoid
    * 'split-brain') and do a faster fail-over.
    *
    * @return
    */
   public OperationContext sendLiveIsStopping(final LiveStopping finalMessage) {
      logger.debug("LIVE IS STOPPING?!? message=" + finalMessage + " enabled=" + enabled);
      if (enabled) {
         logger.debug("LIVE IS STOPPING?!? message=" + finalMessage + " " + enabled);
         return sendReplicatePacket(new ReplicationLiveIsStoppingMessage(finalMessage));
      }
      return null;
   }

   /**
    * Used while stopping the server to ensure that we freeze communications with the backup.
    *
    * @return remoting connection with the backup
    */
   public CoreRemotingConnection getBackupTransportConnection() {
      return remotingConnection;
   }

   /**
    * @return
    */
   public boolean isSynchronizing() {
      return inSync;
   }
}
