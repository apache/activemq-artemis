/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.core.replication;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.JournalContent;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.ChannelImpl.CHANNEL_ID;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationAddMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationAddTXMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationCommitMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationDeleteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationDeleteTXMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageBeginMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageEndMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage.LiveStopping;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageEventMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPrepareMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationStartSyncMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationSyncFileMessage;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.ExecutorFactory;
import org.omg.CORBA.portable.ResponseHandler;

/**
 * Manages replication tasks on the live server (that is the live server side of a "remote backup"
 * use case).
 * <p/>
 * Its equivalent in the backup server is {@link ReplicationEndpoint}.
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @see ReplicationEndpoint
 */
public final class ReplicationManager implements HornetQComponent
{
   public enum ADD_OPERATION_TYPE
   {
      UPDATE
      {
         @Override
         public boolean toBoolean()
         {
            return true;
         }
      },
      ADD
      {
         @Override
         public boolean toBoolean()
         {
            return false;
         }
      };

      public abstract boolean toBoolean();

      public static ADD_OPERATION_TYPE toOperation(boolean isUpdate)
      {
         return isUpdate ? UPDATE : ADD;
      }
   }

   private final ResponseHandler responseHandler = new ResponseHandler();

   private final Channel replicatingChannel;

   private boolean started;

   private volatile boolean enabled;

   private final Object replicationLock = new Object();
   private final Object largeMessageSyncGuard = new Object();
   private final HashMap<Long, Pair<String, Long>> largeMessagesToSync = new HashMap<Long, Pair<String, Long>>();

   private final Queue<OperationContext> pendingTokens = new ConcurrentLinkedQueue<OperationContext>();

   private final ExecutorFactory executorFactory;

   private SessionFailureListener failureListener;

   private CoreRemotingConnection remotingConnection;

   private volatile boolean inSync = true;

   /**
    * @param remotingConnection
    */
   public ReplicationManager(CoreRemotingConnection remotingConnection, final ExecutorFactory executorFactory)
   {
      this.executorFactory = executorFactory;
      this.replicatingChannel = remotingConnection.getChannel(CHANNEL_ID.REPLICATION.id, -1);
      this.remotingConnection = remotingConnection;
   }

   public void appendUpdateRecord(final byte journalID, final ADD_OPERATION_TYPE operation, final long id,
                                  final byte recordType,
                                  final EncodingSupport record) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationAddMessage(journalID, operation, id, recordType, record));
      }
   }

   public void appendDeleteRecord(final byte journalID, final long id) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationDeleteMessage(journalID, id));
      }
   }

   public void
   appendAddRecordTransactional(final byte journalID, final ADD_OPERATION_TYPE operation, final long txID,
                                final long id,
                                final byte recordType,
                                final EncodingSupport record) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationAddTXMessage(journalID, operation, txID, id, recordType, record));
      }
   }

   public void appendCommitRecord(final byte journalID, final long txID, boolean sync, final boolean lineUp) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationCommitMessage(journalID, false, txID), lineUp);
      }
   }

   public void appendDeleteRecordTransactional(final byte journalID,
                                               final long txID,
                                               final long id,
                                               final EncodingSupport record) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationDeleteTXMessage(journalID, txID, id, record));
      }
   }

   public void appendDeleteRecordTransactional(final byte journalID, final long txID, final long id) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationDeleteTXMessage(journalID, txID, id, NullEncoding.instance));
      }
   }

   public void
   appendPrepareRecord(final byte journalID, final long txID, final EncodingSupport transactionData) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPrepareMessage(journalID, txID, transactionData));
      }
   }

   public void appendRollbackRecord(final byte journalID, final long txID) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationCommitMessage(journalID, true, txID));
      }
   }

   /**
    * @param storeName
    * @param pageNumber
    */
   public void pageClosed(final SimpleString storeName, final int pageNumber)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPageEventMessage(storeName, pageNumber, false));
      }
   }

   public void pageDeleted(final SimpleString storeName, final int pageNumber)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPageEventMessage(storeName, pageNumber, true));
      }
   }

   public void pageWrite(final PagedMessage message, final int pageNumber)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPageWriteMessage(message, pageNumber));
      }
   }

   public void largeMessageBegin(final long messageId)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationLargeMessageBeginMessage(messageId));
      }
   }

   public void largeMessageDelete(final Long messageId)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationLargeMessageEndMessage(messageId));
      }
   }

   public void largeMessageWrite(final long messageId, final byte[] body)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationLargeMessageWriteMessage(messageId, body));
      }
   }

   @Override
   public synchronized boolean isStarted()
   {
      return started;
   }

   @Override
   public synchronized void start() throws HornetQException
   {
      if (started)
      {
         throw new IllegalStateException("ReplicationManager is already started");
      }

      replicatingChannel.setHandler(responseHandler);
      failureListener = new ReplicatedSessionFailureListener();
      remotingConnection.addFailureListener(failureListener);

      started = true;

      enabled = true;
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      synchronized (replicationLock)
      {
         enabled = false;
         if (replicatingChannel != null)
         {
            replicatingChannel.close();
         }
         clearReplicationTokens();
      }

      RemotingConnection toStop = remotingConnection;
      if (toStop != null)
      {
         toStop.removeFailureListener(failureListener);
      }
      remotingConnection = null;
      started = false;
   }

   /**
    * Completes any pending operations.
    * <p/>
    * This can be necessary in case the live loses connection to the backup (network failure, or
    * backup crashing).
    */
   public void clearReplicationTokens()
   {
      synchronized (replicationLock)
      {
         while (!pendingTokens.isEmpty())
         {
            OperationContext ctx = pendingTokens.poll();
            try
            {
               ctx.replicationDone();
            }
            catch (Throwable e)
            {
               HornetQServerLogger.LOGGER.errorCompletingCallbackOnReplicationManager(e);
            }
         }
      }
   }

   /**
    * A list of tokens that are still waiting for replications to be completed
    */
   public Set<OperationContext> getActiveTokens()
   {

      LinkedHashSet<OperationContext> activeContexts = new LinkedHashSet<OperationContext>();

      // The same context will be replicated on the pending tokens...
      // as the multiple operations will be replicated on the same context

      for (OperationContext ctx : pendingTokens)
      {
         activeContexts.add(ctx);
      }

      return activeContexts;

   }

   private OperationContext sendReplicatePacket(final Packet packet)
   {
      return sendReplicatePacket(packet, true);
   }

   private OperationContext sendReplicatePacket(final Packet packet, boolean lineUp)
   {
      if (!enabled)
         return null;
      boolean runItNow = false;

      OperationContext repliToken = OperationContextImpl.getContext(executorFactory);
      if (lineUp)
      {
         repliToken.replicationLineUp();
      }

      synchronized (replicationLock)
      {
         if (enabled)
         {
            pendingTokens.add(repliToken);
            replicatingChannel.send(packet);
         }
         else
         {
            // Already replicating channel failed, so just play the action now
            runItNow = true;
         }
      }

      // Execute outside lock

      if (runItNow)
      {
         repliToken.replicationDone();
      }

      return repliToken;
   }

   /**
    * @throws IllegalStateException By default, all replicated packets generate a replicated
    *                               response. If your packets are triggering this exception, it may be because the
    *                               packets were not sent with {@link #sendReplicatePacket(Packet)}.
    */
   private void replicated()
   {
      OperationContext ctx = pendingTokens.poll();

      if (ctx == null)
      {
         throw new IllegalStateException("Missing replication token on the queue.");
      }

      ctx.replicationDone();
   }

   // Inner classes -------------------------------------------------

   private final class ReplicatedSessionFailureListener implements SessionFailureListener
   {
      @Override
      public void connectionFailed(final HornetQException me, boolean failedOver)
      {
         if (me.getType() == HornetQExceptionType.DISCONNECTED)
         {
            // Backup has shut down - no need to log a stack trace
            HornetQServerLogger.LOGGER.replicationStopOnBackupShutdown();
         }
         else
         {
            HornetQServerLogger.LOGGER.replicationStopOnBackupFail(me);
         }

         try
         {
            stop();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorStoppingReplication(e);
         }
      }

      @Override
      public void connectionFailed(final HornetQException me, boolean failedOver, String scaleDownTargetNodeID)
      {
         connectionFailed(me, failedOver);
      }

      public void beforeReconnect(final HornetQException me)
      {
      }
   }

   private final class ResponseHandler implements ChannelHandler
   {
      public void handlePacket(final Packet packet)
      {
         if (packet.getType() == PacketImpl.REPLICATION_RESPONSE)
         {
            replicated();
         }
      }

   }

   private static final class NullEncoding implements EncodingSupport
   {
      static final NullEncoding instance = new NullEncoding();

      public void decode(final HornetQBuffer buffer)
      {
      }

      public void encode(final HornetQBuffer buffer)
      {
      }

      public int getEncodeSize()
      {
         return 0;
      }
   }

   /**
    * Sends the whole content of the file to be duplicated.
    *
    * @throws HornetQException
    * @throws Exception
    */
   public void syncJournalFile(JournalFile jf, JournalContent content) throws Exception
   {
      if (!enabled)
      {
         return;
      }
      SequentialFile file = jf.getFile().cloneFile();
      try
      {
         HornetQServerLogger.LOGGER.journalSynch(jf, file.size(), file);
         sendLargeFile(content, null, jf.getFileID(), file, Long.MAX_VALUE);
      }
      finally
      {
         if (file.isOpen())
            file.close();
      }
   }

   /**
    * @return
    */
   public Map.Entry<Long, Pair<String, Long>> getNextLargeMessageToSync()
   {
      Iterator<Entry<Long, Pair<String, Long>>> iter = largeMessagesToSync.entrySet().iterator();
      if (!iter.hasNext())
      {
         return null;
      }

      Entry<Long, Pair<String, Long>> entry = iter.next();
      iter.remove();
      return entry;
   }

   public void syncLargeMessageFile(SequentialFile file, long size, long id) throws Exception
   {
      if (enabled)
      {
         sendLargeFile(null, null, id, file, size);
      }
   }

   public void syncPages(SequentialFile file, long id, SimpleString queueName) throws Exception
   {
      if (enabled)
         sendLargeFile(null, queueName, id, file, Long.MAX_VALUE);
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
   private void sendLargeFile(JournalContent content, SimpleString pageStore, final long id, SequentialFile file,
                              long maxBytesToSend) throws Exception
   {
      if (!enabled)
         return;
      if (!file.isOpen())
      {
         file.open();
      }
      try
      {
         final FileInputStream fis = new FileInputStream(file.getJavaFile());
         try
         {
            final FileChannel channel = fis.getChannel();
            try
            {
               final ByteBuffer buffer = ByteBuffer.allocate(1 << 17);
               while (true)
               {
                  buffer.clear();
                  final int bytesRead = channel.read(buffer);
                  int toSend = bytesRead;
                  if (bytesRead > 0)
                  {
                     if (bytesRead >= maxBytesToSend)
                     {
                        toSend = (int) maxBytesToSend;
                        maxBytesToSend = 0;
                     }
                     else
                     {
                        maxBytesToSend = maxBytesToSend - bytesRead;
                     }
                     buffer.limit(toSend);
                  }
                  buffer.rewind();

                  // sending -1 or 0 bytes will close the file at the backup
                  sendReplicatePacket(new ReplicationSyncFileMessage(content, pageStore, id, toSend, buffer));
                  if (bytesRead == -1 || bytesRead == 0 || maxBytesToSend == 0)
                     break;
               }
            }
            finally
            {
               channel.close();
            }
         }
         finally
         {
            fis.close();
         }
      }
      finally
      {
         if (file.isOpen())
            file.close();
      }
   }

   /**
    * Reserve the following fileIDs in the backup server.
    *
    * @param datafiles
    * @param contentType
    * @throws HornetQException
    */
   public void sendStartSyncMessage(JournalFile[] datafiles, JournalContent contentType, String nodeID,
                                    boolean allowsAutoFailBack) throws HornetQException
   {
      if (enabled)
         sendReplicatePacket(new ReplicationStartSyncMessage(datafiles, contentType, nodeID, allowsAutoFailBack));
   }

   /**
    * Informs backup that data synchronization is done.
    * <p/>
    * So if 'live' fails, the (up-to-date) backup now may take over its duties. To do so, it must
    * know which is the live's {@code nodeID}.
    *
    * @param nodeID
    */
   public void sendSynchronizationDone(String nodeID)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationStartSyncMessage(nodeID));
         inSync = false;
      }
   }

   /**
    * Reserves several LargeMessage IDs in the backup.
    * <p/>
    * Doing this before hand removes the need of synchronizing large-message deletes with the
    * largeMessageSyncList.
    *
    * @param largeMessages
    */
   public void sendLargeMessageIdListMessage(Map<Long, Pair<String, Long>> largeMessages)
   {
      ArrayList<Long> idsToSend;
      largeMessagesToSync.putAll(largeMessages);
      idsToSend = new ArrayList<Long>(largeMessagesToSync.keySet());

      if (enabled)
         sendReplicatePacket(new ReplicationStartSyncMessage(idsToSend));
   }

   /**
    * Notifies the backup that the live server is stopping.
    * <p/>
    * This notification allows the backup to skip quorum voting (or any other measure to avoid
    * 'split-brain') and do a faster fail-over.
    *
    * @return
    */
   public OperationContext sendLiveIsStopping(final LiveStopping finalMessage)
   {
      HornetQServerLogger.LOGGER.warn("LIVE IS STOPPING?!? message=" + finalMessage + " enabled=" + enabled);
      if (enabled)
      {
         HornetQServerLogger.LOGGER.warn("LIVE IS STOPPING?!? message=" + finalMessage + " " + enabled);
         return sendReplicatePacket(new ReplicationLiveIsStoppingMessage(finalMessage));
      }
      return null;
   }

   /**
    * Used while stopping the server to ensure that we freeze communications with the backup.
    *
    * @return remoting connection with the backup
    */
   public CoreRemotingConnection getBackupTransportConnection()
   {
      return remotingConnection;
   }

   /**
    * @return
    */
   public boolean isSynchronizing()
   {
      return inSync;
   }
}
