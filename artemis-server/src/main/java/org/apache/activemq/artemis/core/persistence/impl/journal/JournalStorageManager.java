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

import javax.transaction.xa.Xid;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.DigestInputStream;
import java.security.InvalidParameterException;
import java.security.MessageDigest;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.ActiveMQInternalErrorException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.PagedReferenceImpl;
import org.apache.activemq.artemis.core.paging.cursor.impl.PagePositionImpl;
import org.apache.activemq.artemis.core.paging.impl.PageTransactionInfoImpl;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.config.PersistedAddressSetting;
import org.apache.activemq.artemis.core.persistence.config.PersistedRoles;
import org.apache.activemq.artemis.core.persistence.impl.PageCountPending;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage.LiveStopping;
import org.apache.activemq.artemis.core.replication.ReplicatedJournal;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.group.impl.GroupBinding;
import org.apache.activemq.artemis.core.server.impl.JournalLoader;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.Transaction.State;
import org.apache.activemq.artemis.core.transaction.TransactionOperation;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.XidCodecSupport;

import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ACKNOWLEDGE_CURSOR;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ADD_LARGE_MESSAGE;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.ADD_LARGE_MESSAGE_PENDING;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.DUPLICATE_ID;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.PAGE_CURSOR_COUNTER_INC;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE;
import static org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME;

/**
 * Controls access to the journals and other storage files such as the ones used to store pages and
 * large messages. This class must control writing of any non-transient data, as it is the key point
 * for synchronizing a replicating backup server.
 * <p>
 * Using this class also ensures that locks are acquired in the right order, avoiding dead-locks.
 * <p>
 * Notice that, turning on and off replication (on the live server side) is _mostly_ a matter of
 * using {@link ReplicatedJournal}s instead of regular {@link JournalImpl}, and sync the existing
 * data. For details see the Javadoc of
 * {@link #startReplication(ReplicationManager, PagingManager, String, boolean)}.
 * <p>
 */
public class JournalStorageManager implements StorageManager
{
   private static final long CHECKPOINT_BATCH_SIZE = Integer.MAX_VALUE;

   private final Semaphore pageMaxConcurrentIO;

   private final BatchingIDGenerator idGenerator;

   private final ReentrantReadWriteLock storageManagerLock = new ReentrantReadWriteLock(true);

   private ReplicationManager replicator;

   public enum JournalContent
   {
      BINDINGS((byte) 0), MESSAGES((byte) 1);

      public final byte typeByte;

      JournalContent(byte b)
      {
         typeByte = b;
      }

      public static JournalContent getType(byte type)
      {
         if (MESSAGES.typeByte == type)
            return MESSAGES;
         if (BINDINGS.typeByte == type)
            return BINDINGS;
         throw new InvalidParameterException("invalid byte: " + type);
      }
   }

   private final SequentialFileFactory journalFF;

   private Journal messageJournal;

   private Journal bindingsJournal;

   private final Journal originalMessageJournal;

   private final Journal originalBindingsJournal;

   private final SequentialFileFactory largeMessagesFactory;

   private volatile boolean started;

   /**
    * Used to create Operation Contexts
    */
   private final ExecutorFactory executorFactory;

   private final Executor executor;

   private ExecutorService singleThreadExecutor;

   private final boolean syncTransactional;

   private final boolean syncNonTransactional;

   private final int perfBlastPages;

   private final String largeMessagesDirectory;

   private boolean journalLoaded = false;

   private final Configuration config;

   // Persisted core configuration
   private final Map<SimpleString, PersistedRoles> mapPersistedRoles =
      new ConcurrentHashMap<SimpleString, PersistedRoles>();

   private final Map<SimpleString, PersistedAddressSetting> mapPersistedAddressSettings =
      new ConcurrentHashMap<SimpleString, PersistedAddressSetting>();

   private final Set<Long> largeMessagesToDelete = new HashSet<Long>();

   public JournalStorageManager(final Configuration config, final ExecutorFactory executorFactory)
   {
      this(config, executorFactory, null);
   }

   public JournalStorageManager(final Configuration config, final ExecutorFactory executorFactory,
                                final IOCriticalErrorListener criticalErrorListener)
   {
      this.executorFactory = executorFactory;

      this.config = config;

      executor = executorFactory.getExecutor();

      if (config.getJournalType() != JournalType.NIO && config.getJournalType() != JournalType.ASYNCIO)
      {
         throw ActiveMQMessageBundle.BUNDLE.invalidJournal();
      }


      SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(config.getBindingsLocation(),
                                                                      criticalErrorListener,
                                                                      config.getJournalMaxIO_NIO());

      Journal localBindings = new JournalImpl(1024 * 1024,
                                              2,
                                              config.getJournalCompactMinFiles(),
                                              config.getJournalCompactPercentage(),
                                              bindingsFF,
                                              "activemq-bindings",
                                              "bindings",
                                              1);

      bindingsJournal = localBindings;
      originalBindingsJournal = localBindings;

      syncNonTransactional = config.isJournalSyncNonTransactional();

      syncTransactional = config.isJournalSyncTransactional();

      if (config.getJournalType() == JournalType.ASYNCIO)
      {
         ActiveMQServerLogger.LOGGER.journalUseAIO();

         journalFF = new AIOSequentialFileFactory(config.getJournalLocation(),
                                                  config.getJournalBufferSize_AIO(),
                                                  config.getJournalBufferTimeout_AIO(),
                                                  config.getJournalMaxIO_AIO(),
                                                  config.isLogJournalWriteRate(),
                                                  criticalErrorListener);
      }
      else if (config.getJournalType() == JournalType.NIO)
      {
         ActiveMQServerLogger.LOGGER.journalUseNIO();
         journalFF = new NIOSequentialFileFactory(config.getJournalLocation(),
                                                  true,
                                                  config.getJournalBufferSize_NIO(),
                                                  config.getJournalBufferTimeout_NIO(),
                                                  config.getJournalMaxIO_NIO(),
                                                  config.isLogJournalWriteRate(),
                                                  criticalErrorListener);
      }
      else
      {
         throw ActiveMQMessageBundle.BUNDLE.invalidJournalType2(config.getJournalType());
      }

      idGenerator = new BatchingIDGenerator(0, JournalStorageManager.CHECKPOINT_BATCH_SIZE, this);

      Journal localMessage = new JournalImpl(config.getJournalFileSize(),
                                             config.getJournalMinFiles(),
                                             config.getJournalCompactMinFiles(),
                                             config.getJournalCompactPercentage(),
                                             journalFF,
                                             "activemq-data",
                                             "amq",
                                             config.getJournalType() == JournalType.ASYNCIO ? config.getJournalMaxIO_AIO()
                                                : config.getJournalMaxIO_NIO());

      messageJournal = localMessage;
      originalMessageJournal = localMessage;

      largeMessagesDirectory = config.getLargeMessagesDirectory();

      largeMessagesFactory = new NIOSequentialFileFactory(config.getLargeMessagesLocation(), false, criticalErrorListener,
                                                          1);

      perfBlastPages = config.getJournalPerfBlastPages();

      if (config.getPageMaxConcurrentIO() != 1)
      {
         pageMaxConcurrentIO = new Semaphore(config.getPageMaxConcurrentIO());
      }
      else
      {
         pageMaxConcurrentIO = null;
      }
   }

   public void clearContext()
   {
      OperationContextImpl.clearContext();
   }

   public boolean isReplicated()
   {
      return replicator != null;
   }

   /**
    * Starts replication at the live-server side.
    * <p>
    * In practice that means 2 things:<br>
    * (1) all currently existing data must be sent to the backup.<br>
    * (2) every new persistent information is replicated (sent) to the backup.
    * <p>
    * To achieve (1), we lock the entire journal while collecting the list of files to send to the
    * backup. The journal does not remain locked during actual synchronization.
    * <p>
    * To achieve (2), instead of writing directly to instances of {@link JournalImpl}, we write to
    * instances of {@link ReplicatedJournal}.
    * <p>
    * At the backup-side replication is handled by {@link org.apache.activemq.artemis.core.replication.ReplicationEndpoint}.
    *
    * @param replicationManager
    * @param pagingManager
    * @throws ActiveMQException
    */
   @Override
   public void startReplication(ReplicationManager replicationManager, PagingManager pagingManager, String nodeID,
                                final boolean autoFailBack) throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException("JournalStorageManager must be started...");
      }
      assert replicationManager != null;

      if (!(messageJournal instanceof JournalImpl) || !(bindingsJournal instanceof JournalImpl))
      {
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

      try
      {
         Map<SimpleString, Collection<Integer>> pageFilesToSync;
         storageManagerLock.writeLock().lock();
         try
         {
            if (isReplicated())
               throw new ActiveMQIllegalStateException("already replicating");
            replicator = replicationManager;

            // Establishes lock
            originalMessageJournal.synchronizationLock();
            originalBindingsJournal.synchronizationLock();

            try
            {
               originalBindingsJournal.replicationSyncPreserveOldFiles();
               originalMessageJournal.replicationSyncPreserveOldFiles();

               pagingManager.lock();
               try
               {
                  pagingManager.disableCleanup();
                  messageFiles =
                     prepareJournalForCopy(originalMessageJournal, JournalContent.MESSAGES, nodeID, autoFailBack);
                  bindingsFiles =
                     prepareJournalForCopy(originalBindingsJournal, JournalContent.BINDINGS, nodeID, autoFailBack);
                  pageFilesToSync = getPageInformationForSync(pagingManager);
                  pendingLargeMessages = recoverPendingLargeMessages();
               }
               finally
               {
                  pagingManager.unlock();
               }
            }
            finally
            {
               originalMessageJournal.synchronizationUnlock();
               originalBindingsJournal.synchronizationUnlock();
            }
            bindingsJournal = new ReplicatedJournal(((byte) 0), originalBindingsJournal, replicator);
            messageJournal = new ReplicatedJournal((byte) 1, originalMessageJournal, replicator);
         }
         finally
         {
            storageManagerLock.writeLock().unlock();
         }

         // it will send a list of IDs that we are allocating
         replicator.sendLargeMessageIdListMessage(pendingLargeMessages);
         sendJournalFile(messageFiles, JournalContent.MESSAGES);
         sendJournalFile(bindingsFiles, JournalContent.BINDINGS);
         sendLargeMessageFiles(pendingLargeMessages);
         sendPagesToBackup(pageFilesToSync, pagingManager);

         storageManagerLock.writeLock().lock();
         try
         {
            if (replicator != null)
            {
               replicator.sendSynchronizationDone(nodeID);
               performCachedLargeMessageDeletes();
            }
         }
         finally
         {
            storageManagerLock.writeLock().unlock();
         }
      }
      catch (Exception e)
      {
         stopReplication();
         throw e;
      }
      finally
      {
         pagingManager.resumeCleanup();
         // Re-enable compact and reclaim of journal files
         originalBindingsJournal.replicationSyncFinished();
         originalMessageJournal.replicationSyncFinished();
      }
   }

   public static String md5(File file)
   {
      try
      {
         byte[] buffer = new byte[1 << 4];
         MessageDigest md = MessageDigest.getInstance("MD5");

         FileInputStream is = new FileInputStream(file);
         DigestInputStream is2 = new DigestInputStream(is, md);
         while (is2.read(buffer) > 0)
         {
            continue;
         }
         byte[] digest = md.digest();
         is.close();
         is2.close();
         return Base64.encodeBytes(digest);
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   /**
    * Stops replication by resetting replication-related fields to their 'unreplicated' state.
    */
   @Override
   public void stopReplication()
   {
      storageManagerLock.writeLock().lock();
      try
      {
         if (replicator == null)
            return;
         bindingsJournal = originalBindingsJournal;
         messageJournal = originalMessageJournal;
         try
         {
            replicator.stop();
         }
         catch (Exception e)
         {
            ActiveMQServerLogger.LOGGER.errorStoppingReplicationManager(e);
         }
         replicator = null;
         // delete inside the writeLock. Avoids a lot of state checking and races with
         // startReplication.
         // This method should not be called under normal circumstances
         performCachedLargeMessageDeletes();
      }
      finally
      {
         storageManagerLock.writeLock().unlock();
      }
   }

   /**
    * Assumption is that this is only called with a writeLock on the StorageManager.
    */
   private void performCachedLargeMessageDeletes()
   {
      for (Long largeMsgId : largeMessagesToDelete)
      {
         SequentialFile msg = createFileForLargeMessage(largeMsgId, LargeMessageExtension.DURABLE);
         try
         {
            msg.delete();
         }
         catch (Exception e)
         {
            ActiveMQServerLogger.LOGGER.journalErrorDeletingMessage(e, largeMsgId);
         }
         if (replicator != null)
         {
            replicator.largeMessageDelete(largeMsgId);
         }
      }
      largeMessagesToDelete.clear();
   }

   /**
    * @param pageFilesToSync
    * @throws Exception
    */
   private void sendPagesToBackup(Map<SimpleString, Collection<Integer>> pageFilesToSync, PagingManager manager) throws Exception
   {
      for (Entry<SimpleString, Collection<Integer>> entry : pageFilesToSync.entrySet())
      {
         if (!started)
            return;
         PagingStore store = manager.getPageStore(entry.getKey());
         store.sendPages(replicator, entry.getValue());
      }
   }

   /**
    * @param pagingManager
    * @return
    * @throws Exception
    */
   private Map<SimpleString, Collection<Integer>> getPageInformationForSync(PagingManager pagingManager) throws Exception
   {
      Map<SimpleString, Collection<Integer>> info = new HashMap<SimpleString, Collection<Integer>>();
      for (SimpleString storeName : pagingManager.getStoreNames())
      {
         PagingStore store = pagingManager.getPageStore(storeName);
         info.put(storeName, store.getCurrentIds());
         store.forceAnotherPage();
      }
      return info;
   }

   private void sendLargeMessageFiles(final Map<Long, Pair<String, Long>> pendingLargeMessages) throws Exception
   {
      Iterator<Entry<Long, Pair<String, Long>>> iter = pendingLargeMessages.entrySet().iterator();
      while (started && iter.hasNext())
      {
         Map.Entry<Long, Pair<String, Long>> entry = iter.next();
         String fileName = entry.getValue().getA();
         final long id = entry.getKey();
         long size = entry.getValue().getB();
         SequentialFile seqFile = largeMessagesFactory.createSequentialFile(fileName);
         if (!seqFile.exists())
            continue;
         replicator.syncLargeMessageFile(seqFile, size, id);
      }
   }

   private long getLargeMessageIdFromFilename(String filename)
   {
      return Long.parseLong(filename.split("\\.")[0]);
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
   private Map<Long, Pair<String, Long>> recoverPendingLargeMessages() throws Exception
   {

      Map<Long, Pair<String, Long>> largeMessages = new HashMap<Long, Pair<String, Long>>();
      // only send durable messages... // listFiles append a "." to anything...
      List<String> filenames = largeMessagesFactory.listFiles("msg");

      List<Long> idList = new ArrayList<Long>();
      for (String filename : filenames)
      {
         Long id = getLargeMessageIdFromFilename(filename);
         if (!largeMessagesToDelete.contains(id))
         {
            idList.add(id);
            SequentialFile seqFile = largeMessagesFactory.createSequentialFile(filename);
            long size = seqFile.size();
            largeMessages.put(id, new Pair<String, Long>(filename, size));
         }
      }

      return largeMessages;
   }

   /**
    * Send an entire journal file to a replicating backup server.
    */
   private void sendJournalFile(JournalFile[] journalFiles, JournalContent type) throws Exception
   {
      for (JournalFile jf : journalFiles)
      {
         if (!started)
            return;
         replicator.syncJournalFile(jf, type);
      }
   }

   private JournalFile[] prepareJournalForCopy(Journal journal, JournalContent contentType, String nodeID,
                                               boolean autoFailBack) throws Exception
   {
      journal.forceMoveNextFile();
      JournalFile[] datafiles = journal.getDataFiles();
      replicator.sendStartSyncMessage(datafiles, contentType, nodeID, autoFailBack);
      return datafiles;
   }


   @Override
   public final void waitOnOperations() throws Exception
   {
      if (!started)
      {
         ActiveMQServerLogger.LOGGER.serverIsStopped();
         throw new IllegalStateException("Server is stopped");
      }
      waitOnOperations(0);
   }

   @Override
   public final boolean waitOnOperations(final long timeout) throws Exception
   {
      if (!started)
      {
         ActiveMQServerLogger.LOGGER.serverIsStopped();
         throw new IllegalStateException("Server is stopped");
      }
      return getContext().waitCompletion(timeout);
   }

   @Override
   public void pageClosed(final SimpleString storeName, final int pageNumber)
   {
      if (isReplicated())
      {
         readLock();
         try
         {
            if (isReplicated())
               replicator.pageClosed(storeName, pageNumber);
         }
         finally
         {
            readUnLock();
         }
      }
   }

   @Override
   public void pageDeleted(final SimpleString storeName, final int pageNumber)
   {
      if (isReplicated())
      {
         readLock();
         try
         {
            if (isReplicated())
               replicator.pageDeleted(storeName, pageNumber);
         }
         finally
         {
            readUnLock();
         }
      }
   }

   @Override
   public void pageWrite(final PagedMessage message, final int pageNumber)
   {
      if (isReplicated())
      {
         // Note: (https://issues.jboss.org/browse/HORNETQ-1059)
         // We have to replicate durable and non-durable messages on paging
         // since acknowledgments are written using the page-position.
         // Say you are sending durable and non-durable messages to a page
         // The ACKs would be done to wrong positions, and the backup would be a mess

         readLock();
         try
         {
            if (isReplicated())
               replicator.pageWrite(message, pageNumber);
         }
         finally
         {
            readUnLock();
         }
      }
   }

   public OperationContext getContext()
   {
      return OperationContextImpl.getContext(executorFactory);
   }

   public void setContext(final OperationContext context)
   {
      OperationContextImpl.setContext(context);
   }

   public Executor getSingleThreadExecutor()
   {
      return singleThreadExecutor;
   }

   public OperationContext newSingleThreadContext()
   {
      return newContext(singleThreadExecutor);
   }

   public OperationContext newContext(final Executor executor1)
   {
      return new OperationContextImpl(executor1);
   }

   public void afterCompleteOperations(final IOCallback run)
   {
      getContext().executeOnCompletion(run);
   }

   public long generateID()
   {
      return idGenerator.generateID();
   }

   public long getCurrentID()
   {
      return idGenerator.getCurrentID();
   }

   public LargeServerMessage createLargeMessage()
   {
      return new LargeServerMessageImpl(this);
   }

   public final void addBytesToLargeMessage(final SequentialFile file,
                                            final long messageId, final byte[] bytes) throws Exception
   {
      readLock();
      try
      {
         file.position(file.size());

         file.writeDirect(ByteBuffer.wrap(bytes), false);

         if (isReplicated())
         {
            replicator.largeMessageWrite(messageId, bytes);
         }
      }
      finally
      {
         readUnLock();
      }
   }

   public LargeServerMessage createLargeMessage(final long id, final MessageInternal message) throws Exception
   {
      readLock();
      try
      {
         if (isReplicated())
         {
            replicator.largeMessageBegin(id);
         }

         LargeServerMessageImpl largeMessage = (LargeServerMessageImpl) createLargeMessage();

         largeMessage.copyHeadersAndProperties(message);

         largeMessage.setMessageID(id);

         if (largeMessage.isDurable())
         {
            // We store a marker on the journal that the large file is pending
            long pendingRecordID = storePendingLargeMessage(id);

            largeMessage.setPendingRecordID(pendingRecordID);
         }

         return largeMessage;
      }
      finally
      {
         readUnLock();
      }
   }

   // Non transactional operations

   public long storePendingLargeMessage(final long messageID) throws Exception
   {
      readLock();
      try
      {
         long recordID = generateID();

         messageJournal.appendAddRecord(recordID, JournalRecordIds.ADD_LARGE_MESSAGE_PENDING,
                                        new PendingLargeMessageEncoding(messageID),
                                        true,
                                        getContext(true));

         return recordID;
      }
      finally
      {
         readUnLock();
      }
   }

   public void confirmPendingLargeMessageTX(final Transaction tx, long messageID, long recordID) throws Exception
   {
      readLock();
      try
      {
         installLargeMessageConfirmationOnTX(tx, recordID);
         messageJournal.appendDeleteRecordTransactional(tx.getID(), recordID,
                                                        new DeleteEncoding(JournalRecordIds.ADD_LARGE_MESSAGE_PENDING,
                                                                           messageID));
      }
      finally
      {
         readUnLock();
      }
   }

   /**
    * We don't need messageID now but we are likely to need it we ever decide to support a database
    */
   public void confirmPendingLargeMessage(long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecord(recordID, true, getContext());
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeMessage(final ServerMessage message) throws Exception
   {
      if (message.getMessageID() <= 0)
      {
         // Sanity check only... this shouldn't happen unless there is a bug
         throw ActiveMQMessageBundle.BUNDLE.messageIdNotAssigned();
      }

      readLock();
      try
      {
         // Note that we don't sync, the add reference that comes immediately after will sync if
         // appropriate

         if (message.isLargeMessage())
         {
            messageJournal.appendAddRecord(message.getMessageID(), JournalRecordIds.ADD_LARGE_MESSAGE,
                                           new LargeMessageEncoding((LargeServerMessage) message), false,
                                           getContext(false));
         }
         else
         {
            messageJournal.appendAddRecord(message.getMessageID(), JournalRecordIds.ADD_MESSAGE, message, false,
                                           getContext(false));
         }
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeReference(final long queueID, final long messageID, final boolean last) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecord(messageID, JournalRecordIds.ADD_REF, new RefEncoding(queueID), last &&
            syncNonTransactional, getContext(last && syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   @Override
   public void readLock()
   {
      storageManagerLock.readLock().lock();
   }

   @Override
   public void readUnLock()
   {
      storageManagerLock.readLock().unlock();
   }

   public void storeAcknowledge(final long queueID, final long messageID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecord(messageID, JournalRecordIds.ACKNOWLEDGE_REF, new RefEncoding(queueID),
                                           syncNonTransactional, getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeCursorAcknowledge(long queueID, PagePosition position) throws Exception
   {
      readLock();
      try
      {
         long ackID = idGenerator.generateID();
         position.setRecordID(ackID);
         messageJournal.appendAddRecord(ackID,
                                        JournalRecordIds.ACKNOWLEDGE_CURSOR,
                                        new CursorAckRecordEncoding(queueID, position),
                                        syncNonTransactional,
                                        getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteMessage(final long messageID) throws Exception
   {
      readLock();
      try
      {
         // Messages are deleted on postACK, one after another.
         // If these deletes are synchronized, we would build up messages on the Executor
         // increasing chances of losing deletes.
         // The StorageManager should verify messages without references
         messageJournal.appendDeleteRecord(messageID, false, getContext(false));
      }
      finally
      {
         readUnLock();
      }
   }

   public void updateScheduledDeliveryTime(final MessageReference ref) throws Exception
   {
      ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding(ref.getScheduledDeliveryTime(), ref.getQueue()
         .getID());
      readLock();
      try
      {
         messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(),
                                           JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME,
                                           encoding,
                                           syncNonTransactional,
                                           getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
   {
      readLock();
      try
      {
         DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

         messageJournal.appendAddRecord(recordID,
                                        JournalRecordIds.DUPLICATE_ID,
                                        encoding,
                                        syncNonTransactional,
                                        getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteDuplicateID(final long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecord(recordID, syncNonTransactional, getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   // Transactional operations

   public void storeMessageTransactional(final long txID, final ServerMessage message) throws Exception
   {
      if (message.getMessageID() <= 0)
      {
         throw ActiveMQMessageBundle.BUNDLE.messageIdNotAssigned();
      }

      readLock();
      try
      {
         if (message.isLargeMessage())
         {
            messageJournal.appendAddRecordTransactional(txID, message.getMessageID(),
                                                        JournalRecordIds.ADD_LARGE_MESSAGE,
                                                        new LargeMessageEncoding(((LargeServerMessage) message)));
         }
         else
         {
            messageJournal.appendAddRecordTransactional(txID, message.getMessageID(),
                                                        JournalRecordIds.ADD_MESSAGE,
                                                        message);
         }

      }
      finally
      {
         readUnLock();
      }
   }

   public void storePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception
   {
      readLock();
      try
      {
         pageTransaction.setRecordID(generateID());
         messageJournal.appendAddRecordTransactional(txID, pageTransaction.getRecordID(),
                                                     JournalRecordIds.PAGE_TRANSACTION, pageTransaction);
      }
      finally
      {
         readUnLock();
      }
   }

   public void updatePageTransaction(final long txID, final PageTransactionInfo pageTransaction, final int depages) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecordTransactional(txID, pageTransaction.getRecordID(),
                                                        JournalRecordIds.PAGE_TRANSACTION,
                                                        new PageUpdateTXEncoding(pageTransaction.getTransactionID(),
                                                                                 depages));
      }
      finally
      {
         readUnLock();
      }
   }


   public void updatePageTransaction(final PageTransactionInfo pageTransaction, final int depages) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecord(pageTransaction.getRecordID(), JournalRecordIds.PAGE_TRANSACTION,
                                           new PageUpdateTXEncoding(pageTransaction.getTransactionID(), depages),
                                           syncNonTransactional, getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeReferenceTransactional(final long txID, final long queueID, final long messageID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecordTransactional(txID, messageID, JournalRecordIds.ADD_REF,
                                                        new RefEncoding(queueID));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeAcknowledgeTransactional(final long txID, final long queueID, final long messageID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecordTransactional(txID, messageID, JournalRecordIds.ACKNOWLEDGE_REF,
                                                        new RefEncoding(queueID));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position) throws Exception
   {
      readLock();
      try
      {
         long ackID = idGenerator.generateID();
         position.setRecordID(ackID);
         messageJournal.appendAddRecordTransactional(txID,
                                                     ackID,
                                                     JournalRecordIds.ACKNOWLEDGE_CURSOR,
                                                     new CursorAckRecordEncoding(queueID, position));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storePageCompleteTransactional(long txID, long queueID, PagePosition position) throws Exception
   {
      long recordID = idGenerator.generateID();
      position.setRecordID(recordID);
      messageJournal.appendAddRecordTransactional(txID,
                                                  recordID,
                                                  JournalRecordIds.PAGE_CURSOR_COMPLETE,
                                                  new CursorAckRecordEncoding(queueID, position));
   }

   public void deletePageComplete(long ackID) throws Exception
   {
      messageJournal.appendDeleteRecord(ackID, false);
   }

   public void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecordTransactional(txID, ackID);
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteCursorAcknowledge(long ackID) throws Exception
   {
      messageJournal.appendDeleteRecord(ackID, false);
   }

   public long storeHeuristicCompletion(final Xid xid, final boolean isCommit) throws Exception
   {
      readLock();
      try
      {
         long id = generateID();

         messageJournal.appendAddRecord(id,
                                        JournalRecordIds.HEURISTIC_COMPLETION,
                                        new HeuristicCompletionEncoding(xid, isCommit),
                                        true,
                                        getContext(true));
         return id;
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteHeuristicCompletion(final long id) throws Exception
   {
      readLock();
      try
      {

         messageJournal.appendDeleteRecord(id, true, getContext(true));
      }
      finally
      {
         readUnLock();
      }
   }

   public void deletePageTransactional(final long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecord(recordID, false);
      }
      finally
      {
         readUnLock();
      }
   }

   public void updateScheduledDeliveryTimeTransactional(final long txID, final MessageReference ref) throws Exception
   {
      ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding(ref.getScheduledDeliveryTime(), ref.getQueue()
         .getID());
      readLock();
      try
      {

         messageJournal.appendUpdateRecordTransactional(txID,
                                                        ref.getMessage().getMessageID(),
                                                        JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME,
                                                        encoding);
      }
      finally
      {
         readUnLock();
      }
   }

   public void prepare(final long txID, final Xid xid) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendPrepareRecord(txID, new XidEncoding(xid), syncTransactional, getContext(syncTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void commit(final long txID) throws Exception
   {
      commit(txID, true);
   }

   public void commitBindings(final long txID) throws Exception
   {
      bindingsJournal.appendCommitRecord(txID, true);
   }

   public void rollbackBindings(final long txID) throws Exception
   {
      // no need to sync, it's going away anyways
      bindingsJournal.appendRollbackRecord(txID, false);
   }

   public void commit(final long txID, final boolean lineUpContext) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendCommitRecord(txID, syncTransactional, getContext(syncTransactional), lineUpContext);
         if (!lineUpContext && !syncTransactional)
         {
            /**
             * If {@code lineUpContext == false}, it means that we have previously lined up a
             * context somewhere else (specifically see @{link TransactionImpl#asyncAppendCommit}),
             * hence we need to mark it as done even if {@code syncTransactional = false} as in this
             * case {@code getContext(syncTransactional=false)} would pass a dummy context to the
             * {@code messageJournal.appendCommitRecord(...)} call above.
             */
            getContext(true).done();
         }
      }
      finally
      {
         readUnLock();
      }
   }

   public void rollback(final long txID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendRollbackRecord(txID, syncTransactional, getContext(syncTransactional));
      }
      finally
      {
         readUnLock();
      }
   }


   public void storeDuplicateIDTransactional(final long txID,
                                             final SimpleString address,
                                             final byte[] duplID,
                                             final long recordID) throws Exception
   {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      readLock();
      try
      {
         messageJournal.appendAddRecordTransactional(txID, recordID, JournalRecordIds.DUPLICATE_ID, encoding);
      }
      finally
      {
         readUnLock();
      }
   }

   public void updateDuplicateIDTransactional(final long txID,
                                              final SimpleString address,
                                              final byte[] duplID,
                                              final long recordID) throws Exception
   {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      readLock();
      try
      {
         messageJournal.appendUpdateRecordTransactional(txID, recordID, JournalRecordIds.DUPLICATE_ID, encoding);
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteDuplicateIDTransactional(final long txID, final long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecordTransactional(txID, recordID);
      }
      finally
      {
         readUnLock();
      }
   }

   // Other operations

   public void updateDeliveryCount(final MessageReference ref) throws Exception
   {
      // no need to store if it's the same value
      // otherwise the journal will get OME in case of lots of redeliveries
      if (ref.getDeliveryCount() == ref.getPersistedCount())
      {
         return;
      }

      ref.setPersistedCount(ref.getDeliveryCount());
      DeliveryCountUpdateEncoding updateInfo =
         new DeliveryCountUpdateEncoding(ref.getQueue().getID(), ref.getDeliveryCount());

      readLock();
      try
      {
         messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(),
                                           JournalRecordIds.UPDATE_DELIVERY_COUNT,
                                           updateInfo,
                                           syncNonTransactional, getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeAddressSetting(PersistedAddressSetting addressSetting) throws Exception
   {
      deleteAddressSetting(addressSetting.getAddressMatch());
      readLock();
      try
      {
         long id = idGenerator.generateID();
         addressSetting.setStoreId(id);
         bindingsJournal.appendAddRecord(id, JournalRecordIds.ADDRESS_SETTING_RECORD, addressSetting, true);
         mapPersistedAddressSettings.put(addressSetting.getAddressMatch(), addressSetting);
      }
      finally
      {
         readUnLock();
      }
   }

   public List<PersistedAddressSetting> recoverAddressSettings() throws Exception
   {
      ArrayList<PersistedAddressSetting> list = new ArrayList<PersistedAddressSetting>(mapPersistedAddressSettings.values());
      return list;
   }

   public List<PersistedRoles> recoverPersistedRoles() throws Exception
   {
      ArrayList<PersistedRoles> list = new ArrayList<PersistedRoles>(mapPersistedRoles.values());
      return list;
   }


   public void storeSecurityRoles(PersistedRoles persistedRoles) throws Exception
   {

      deleteSecurityRoles(persistedRoles.getAddressMatch());
      readLock();
      try
      {
         final long id = idGenerator.generateID();
         persistedRoles.setStoreId(id);
         bindingsJournal.appendAddRecord(id, JournalRecordIds.SECURITY_RECORD, persistedRoles, true);
         mapPersistedRoles.put(persistedRoles.getAddressMatch(), persistedRoles);
      }
      finally
      {
         readUnLock();
      }
   }

   @Override
   public final void storeID(final long journalID, final long id) throws Exception
   {
      readLock();
      try
      {
         bindingsJournal.appendAddRecord(journalID, JournalRecordIds.ID_COUNTER_RECORD,
                                         BatchingIDGenerator.createIDEncodingSupport(id), true);
      }
      finally
      {
         readUnLock();
      }
   }

   @Override
   public void deleteID(long journalD) throws Exception
   {
      readLock();
      try
      {
         bindingsJournal.appendDeleteRecord(journalD, false);
      }
      finally
      {
         readUnLock();
      }
   }


   public void deleteAddressSetting(SimpleString addressMatch) throws Exception
   {
      PersistedAddressSetting oldSetting = mapPersistedAddressSettings.remove(addressMatch);
      if (oldSetting != null)
      {
         readLock();
         try
         {
            bindingsJournal.appendDeleteRecord(oldSetting.getStoreId(), false);
         }
         finally
         {
            readUnLock();
         }
      }
   }

   public void deleteSecurityRoles(SimpleString addressMatch) throws Exception
   {
      PersistedRoles oldRoles = mapPersistedRoles.remove(addressMatch);
      if (oldRoles != null)
      {
         readLock();
         try
         {
            bindingsJournal.appendDeleteRecord(oldRoles.getStoreId(), false);
         }
         finally
         {
            readUnLock();
         }
      }
   }

   @Override
   public JournalLoadInformation loadMessageJournal(final PostOffice postOffice,
                                                    final PagingManager pagingManager,
                                                    final ResourceManager resourceManager,
                                                    Map<Long, QueueBindingInfo> queueInfos,
                                                    final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                                    final Set<Pair<Long, Long>> pendingLargeMessages,
                                                    List<PageCountPending> pendingNonTXPageCounter,
                                                    final JournalLoader journalLoader) throws Exception
   {
      List<RecordInfo> records = new ArrayList<RecordInfo>();

      List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();

      Map<Long, ServerMessage> messages = new HashMap<Long, ServerMessage>();
      readLock();
      try
      {

         JournalLoadInformation info = messageJournal.load(records,
                                                           preparedTransactions,
                                                           new LargeMessageTXFailureCallback(messages));

         ArrayList<LargeServerMessage> largeMessages = new ArrayList<LargeServerMessage>();

         Map<Long, Map<Long, AddMessageRecord>> queueMap = new HashMap<Long, Map<Long, AddMessageRecord>>();

         Map<Long, PageSubscription> pageSubscriptions = new HashMap<Long, PageSubscription>();

         final int totalSize = records.size();

         for (int reccount = 0; reccount < totalSize; reccount++)
         {
            // It will show log.info only with large journals (more than 1 million records)
            if (reccount > 0 && reccount % 1000000 == 0)
            {
               long percent = (long) ((((double) reccount) / ((double) totalSize)) * 100f);

               ActiveMQServerLogger.LOGGER.percentLoaded(percent);
            }

            RecordInfo record = records.get(reccount);
            byte[] data = record.data;

            ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(data);

            byte recordType = record.getUserRecordType();

            switch (recordType)
            {
               case JournalRecordIds.ADD_LARGE_MESSAGE_PENDING:
               {
                  PendingLargeMessageEncoding pending = new PendingLargeMessageEncoding();

                  pending.decode(buff);

                  if (pendingLargeMessages != null)
                  {
                     // it could be null on tests, and we don't need anything on that case
                     pendingLargeMessages.add(new Pair<Long, Long>(record.id, pending.largeMessageID));
                  }
                  break;
               }
               case JournalRecordIds.ADD_LARGE_MESSAGE:
               {
                  LargeServerMessage largeMessage = parseLargeMessage(messages, buff);

                  messages.put(record.id, largeMessage);

                  largeMessages.add(largeMessage);

                  break;
               }
               case JournalRecordIds.ADD_MESSAGE:
               {
                  ServerMessage message = new ServerMessageImpl(record.id, 50);

                  message.decode(buff);

                  messages.put(record.id, message);

                  break;
               }
               case JournalRecordIds.ADD_REF:
               {
                  long messageID = record.id;

                  RefEncoding encoding = new RefEncoding();

                  encoding.decode(buff);

                  Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

                  if (queueMessages == null)
                  {
                     queueMessages = new LinkedHashMap<Long, AddMessageRecord>();

                     queueMap.put(encoding.queueID, queueMessages);
                  }

                  ServerMessage message = messages.get(messageID);

                  if (message == null)
                  {
                     ActiveMQServerLogger.LOGGER.cannotFindMessage(record.id);
                  }
                  else
                  {
                     queueMessages.put(messageID, new AddMessageRecord(message));
                  }

                  break;
               }
               case JournalRecordIds.ACKNOWLEDGE_REF:
               {
                  long messageID = record.id;

                  RefEncoding encoding = new RefEncoding();

                  encoding.decode(buff);

                  Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

                  if (queueMessages == null)
                  {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueue(encoding.queueID, messageID);
                  }
                  else
                  {
                     AddMessageRecord rec = queueMessages.remove(messageID);

                     if (rec == null)
                     {
                        ActiveMQServerLogger.LOGGER.cannotFindMessage(messageID);
                     }
                  }

                  break;
               }
               case JournalRecordIds.UPDATE_DELIVERY_COUNT:
               {
                  long messageID = record.id;

                  DeliveryCountUpdateEncoding encoding = new DeliveryCountUpdateEncoding();

                  encoding.decode(buff);

                  Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

                  if (queueMessages == null)
                  {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueDelCount(encoding.queueID);
                  }
                  else
                  {
                     AddMessageRecord rec = queueMessages.get(messageID);

                     if (rec == null)
                     {
                        ActiveMQServerLogger.LOGGER.journalCannotFindMessageDelCount(messageID);
                     }
                     else
                     {
                        rec.deliveryCount = encoding.count;
                     }
                  }

                  break;
               }
               case JournalRecordIds.PAGE_TRANSACTION:
               {
                  if (record.isUpdate)
                  {
                     PageUpdateTXEncoding pageUpdate = new PageUpdateTXEncoding();

                     pageUpdate.decode(buff);

                     PageTransactionInfo pageTX = pagingManager.getTransaction(pageUpdate.pageTX);

                     pageTX.onUpdate(pageUpdate.recods, null, null);
                  }
                  else
                  {
                     PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

                     pageTransactionInfo.decode(buff);

                     pageTransactionInfo.setRecordID(record.id);

                     pagingManager.addTransaction(pageTransactionInfo);
                  }

                  break;
               }
               case JournalRecordIds.SET_SCHEDULED_DELIVERY_TIME:
               {
                  long messageID = record.id;

                  ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding();

                  encoding.decode(buff);

                  Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

                  if (queueMessages == null)
                  {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueScheduled(encoding.queueID, messageID);
                  }
                  else
                  {

                     AddMessageRecord rec = queueMessages.get(messageID);

                     if (rec == null)
                     {
                        ActiveMQServerLogger.LOGGER.cannotFindMessage(messageID);
                     }
                     else
                     {
                        rec.scheduledDeliveryTime = encoding.scheduledDeliveryTime;
                     }
                  }

                  break;
               }
               case JournalRecordIds.DUPLICATE_ID:
               {
                  DuplicateIDEncoding encoding = new DuplicateIDEncoding();

                  encoding.decode(buff);

                  List<Pair<byte[], Long>> ids = duplicateIDMap.get(encoding.address);

                  if (ids == null)
                  {
                     ids = new ArrayList<Pair<byte[], Long>>();

                     duplicateIDMap.put(encoding.address, ids);
                  }

                  ids.add(new Pair<byte[], Long>(encoding.duplID, record.id));

                  break;
               }
               case JournalRecordIds.HEURISTIC_COMPLETION:
               {
                  HeuristicCompletionEncoding encoding = new HeuristicCompletionEncoding();
                  encoding.decode(buff);
                  resourceManager.putHeuristicCompletion(record.id, encoding.xid, encoding.isCommit);
                  break;
               }
               case JournalRecordIds.ACKNOWLEDGE_CURSOR:
               {
                  CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
                  encoding.decode(buff);

                  encoding.position.setRecordID(record.id);

                  PageSubscription sub = locateSubscription(encoding.queueID, pageSubscriptions, queueInfos, pagingManager);

                  if (sub != null)
                  {
                     sub.reloadACK(encoding.position);
                  }
                  else
                  {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueReloading(encoding.queueID);
                     messageJournal.appendDeleteRecord(record.id, false);

                  }

                  break;
               }
               case JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE:
               {
                  PageCountRecord encoding = new PageCountRecord();

                  encoding.decode(buff);

                  PageSubscription sub = locateSubscription(encoding.queueID, pageSubscriptions, queueInfos, pagingManager);

                  if (sub != null)
                  {
                     sub.getCounter().loadValue(record.id, encoding.value);
                  }
                  else
                  {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueReloadingPage(encoding.queueID);
                     messageJournal.appendDeleteRecord(record.id, false);
                  }

                  break;
               }

               case JournalRecordIds.PAGE_CURSOR_COUNTER_INC:
               {
                  PageCountRecordInc encoding = new PageCountRecordInc();

                  encoding.decode(buff);

                  PageSubscription sub = locateSubscription(encoding.queueID, pageSubscriptions, queueInfos, pagingManager);

                  if (sub != null)
                  {
                     sub.getCounter().loadInc(record.id, encoding.value);
                  }
                  else
                  {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueReloadingPageCursor(encoding.queueID);
                     messageJournal.appendDeleteRecord(record.id, false);
                  }

                  break;
               }

               case JournalRecordIds.PAGE_CURSOR_COMPLETE:
               {
                  CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
                  encoding.decode(buff);

                  encoding.position.setRecordID(record.id);

                  PageSubscription sub = locateSubscription(encoding.queueID, pageSubscriptions, queueInfos, pagingManager);

                  if (sub != null)
                  {
                     sub.reloadPageCompletion(encoding.position);
                  }
                  else
                  {
                     ActiveMQServerLogger.LOGGER.cantFindQueueOnPageComplete(encoding.queueID);
                     messageJournal.appendDeleteRecord(record.id, false);
                  }

                  break;
               }

               case JournalRecordIds.PAGE_CURSOR_PENDING_COUNTER:
               {

                  PageCountPendingImpl pendingCountEncoding = new PageCountPendingImpl();
                  pendingCountEncoding.decode(buff);
                  pendingCountEncoding.setID(record.id);

                  // This can be null on testcases not interested on this outcome
                  if (pendingNonTXPageCounter != null)
                  {
                     pendingNonTXPageCounter.add(pendingCountEncoding);
                  }
                  break;
               }


               default:
               {
                  throw new IllegalStateException("Invalid record type " + recordType);
               }
            }

            // This will free up memory sooner. The record is not needed any more
            // and its byte array would consume memory during the load process even though it's not necessary any longer
            // what would delay processing time during load
            records.set(reccount, null);
         }

         // Release the memory as soon as not needed any longer
         records.clear();
         records = null;

         journalLoader.handleAddMessage(queueMap);

         loadPreparedTransactions(postOffice,
                                  pagingManager,
                                  resourceManager,
                                  queueInfos,
                                  preparedTransactions,
                                  duplicateIDMap,
                                  pageSubscriptions,
                                  pendingLargeMessages,
                                  journalLoader);

         for (PageSubscription sub : pageSubscriptions.values())
         {
            sub.getCounter().processReload();
         }

         for (LargeServerMessage msg : largeMessages)
         {
            if (msg.getRefCount() == 0)
            {
               ActiveMQServerLogger.LOGGER.largeMessageWithNoRef(msg.getMessageID());
               msg.decrementDelayDeletionCount();
            }
         }

         journalLoader.handleNoMessageReferences(messages);

         // To recover positions on Iterators
         if (pagingManager != null)
         {
            // it could be null on certain tests that are not dealing with paging
            // This could also be the case in certain embedded conditions
            pagingManager.processReload();
         }

         if (perfBlastPages != -1)
         {
            messageJournal.perfBlast(perfBlastPages);
         }

         journalLoader.postLoad(messageJournal, resourceManager, duplicateIDMap);
         journalLoaded = true;
         return info;
      }
      finally
      {
         readUnLock();
      }
   }

   /**
    * @param queueID
    * @param pageSubscriptions
    * @param queueInfos
    * @return
    */
   private static PageSubscription locateSubscription(final long queueID,
                                                      final Map<Long, PageSubscription> pageSubscriptions,
                                                      final Map<Long, QueueBindingInfo> queueInfos,
                                                      final PagingManager pagingManager) throws Exception
   {

      PageSubscription subs = pageSubscriptions.get(queueID);
      if (subs == null)
      {
         QueueBindingInfo queueInfo = queueInfos.get(queueID);

         if (queueInfo != null)
         {
            SimpleString address = queueInfo.getAddress();
            PagingStore store = pagingManager.getPageStore(address);
            subs = store.getCursorProvider().getSubscription(queueID);
            pageSubscriptions.put(queueID, subs);
         }
      }

      return subs;
   }

   // grouping handler operations
   public void addGrouping(final GroupBinding groupBinding) throws Exception
   {
      GroupingEncoding groupingEncoding = new GroupingEncoding(groupBinding.getId(),
                                                               groupBinding.getGroupId(),
                                                               groupBinding.getClusterName());
      readLock();
      try
      {
         bindingsJournal.appendAddRecord(groupBinding.getId(), JournalRecordIds.GROUP_RECORD, groupingEncoding,
                                         true);
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteGrouping(long tx, final GroupBinding groupBinding) throws Exception
   {
      readLock();
      try
      {
         bindingsJournal.appendDeleteRecordTransactional(tx, groupBinding.getId());
      }
      finally
      {
         readUnLock();
      }
   }

   // BindingsImpl operations

   public void addQueueBinding(final long tx, final Binding binding) throws Exception
   {
      Queue queue = (Queue) binding.getBindable();

      Filter filter = queue.getFilter();

      SimpleString filterString = filter == null ? null : filter.getFilterString();

      PersistentQueueBindingEncoding bindingEncoding = new PersistentQueueBindingEncoding(queue.getName(),
                                                                                          binding.getAddress(),
                                                                                          filterString,
                                                                                          queue.getUser(),
                                                                                          queue.isAutoCreated());

      readLock();
      try
      {
         bindingsJournal.appendAddRecordTransactional(tx, binding.getID(), JournalRecordIds.QUEUE_BINDING_RECORD,
                                                      bindingEncoding);
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteQueueBinding(long tx, final long queueBindingID) throws Exception
   {
      readLock();
      try
      {
         bindingsJournal.appendDeleteRecordTransactional(tx, queueBindingID);
      }
      finally
      {
         readUnLock();
      }
   }

   public long storePageCounterInc(long txID, long queueID, int value) throws Exception
   {
      readLock();
      try
      {
         long recordID = idGenerator.generateID();
         messageJournal.appendAddRecordTransactional(txID, recordID, JournalRecordIds.PAGE_CURSOR_COUNTER_INC,
                                                     new PageCountRecordInc(queueID, value));
         return recordID;
      }
      finally
      {
         readUnLock();
      }
   }

   public long storePageCounterInc(long queueID, int value) throws Exception
   {
      readLock();
      try
      {
         final long recordID = idGenerator.generateID();
         messageJournal.appendAddRecord(recordID,
                                        JournalRecordIds.PAGE_CURSOR_COUNTER_INC,
                                        new PageCountRecordInc(queueID, value),
                                        true,
                                        getContext());
         return recordID;
      }
      finally
      {
         readUnLock();
      }
   }

   @Override
   public long storePageCounter(long txID, long queueID, long value) throws Exception
   {
      readLock();
      try
      {
         final long recordID = idGenerator.generateID();
         messageJournal.appendAddRecordTransactional(txID, recordID, JournalRecordIds.PAGE_CURSOR_COUNTER_VALUE,
                                                     new PageCountRecord(queueID, value));
         return recordID;
      }
      finally
      {
         readUnLock();
      }
   }

   @Override
   public long storePendingCounter(final long queueID, final long pageID, final int inc) throws Exception
   {
      readLock();
      try
      {
         final long recordID = idGenerator.generateID();
         PageCountPendingImpl pendingInc = new PageCountPendingImpl(queueID, pageID, inc);
         // We must guarantee the record sync before we actually write on the page otherwise we may get out of sync
         // on the counter
         messageJournal.appendAddRecord(recordID, JournalRecordIds.PAGE_CURSOR_PENDING_COUNTER, pendingInc, true);
         return recordID;
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteIncrementRecord(long txID, long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecordTransactional(txID, recordID);
      }
      finally
      {
         readUnLock();
      }
   }

   public void deletePageCounter(long txID, long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecordTransactional(txID, recordID);
      }
      finally
      {
         readUnLock();
      }
   }

   public void deletePendingPageCounter(long txID, long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecordTransactional(txID, recordID);
      }
      finally
      {
         readUnLock();
      }
   }

   public JournalLoadInformation loadBindingJournal(final List<QueueBindingInfo> queueBindingInfos,
                                                    final List<GroupingInfo> groupingInfos) throws Exception
   {
      List<RecordInfo> records = new ArrayList<RecordInfo>();

      List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();

      JournalLoadInformation bindingsInfo = bindingsJournal.load(records, preparedTransactions, null);

      for (RecordInfo record : records)
      {
         long id = record.id;

         ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(record.data);

         byte rec = record.getUserRecordType();

         if (rec == JournalRecordIds.QUEUE_BINDING_RECORD)
         {
            PersistentQueueBindingEncoding bindingEncoding = newBindingEncoding(id, buffer);

            queueBindingInfos.add(bindingEncoding);
         }
         else if (rec == JournalRecordIds.ID_COUNTER_RECORD)
         {
            idGenerator.loadState(record.id, buffer);
         }
         else if (rec == JournalRecordIds.GROUP_RECORD)
         {
            GroupingEncoding encoding = newGroupEncoding(id, buffer);
            groupingInfos.add(encoding);
         }
         else if (rec == JournalRecordIds.ADDRESS_SETTING_RECORD)
         {
            PersistedAddressSetting setting = newAddressEncoding(id, buffer);
            mapPersistedAddressSettings.put(setting.getAddressMatch(), setting);
         }
         else if (rec == JournalRecordIds.SECURITY_RECORD)
         {
            PersistedRoles roles = newSecurityRecord(id, buffer);
            mapPersistedRoles.put(roles.getAddressMatch(), roles);
         }
         else
         {
            throw new IllegalStateException("Invalid record type " + rec);
         }
      }

      // This will instruct the IDGenerator to cleanup old records
      idGenerator.cleanup();

      return bindingsInfo;
   }

   public void lineUpContext()
   {
      readLock();
      try
      {
         messageJournal.lineUpContext(getContext());
      }
      finally
      {
         readUnLock();
      }
   }

   // ActiveMQComponent implementation
   // ------------------------------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      checkAndCreateDir(config.getBindingsLocation(), config.isCreateBindingsDir());

      checkAndCreateDir(config.getJournalLocation(), config.isCreateJournalDir());

      checkAndCreateDir(config.getLargeMessagesLocation(), config.isCreateJournalDir());

      cleanupIncompleteFiles();

      singleThreadExecutor = Executors.newSingleThreadExecutor(new ActiveMQThreadFactory("ActiveMQ-IO-SingleThread",
                                                                                        true,
                                                                                        getThisClassLoader()));

      bindingsJournal.start();

      messageJournal.start();

      started = true;
   }

   public void stop() throws Exception
   {
      stop(false);
   }

   @Override
   public synchronized void persistIdGenerator()
   {
      if (journalLoaded && idGenerator != null)
      {
         // Must call close to make sure last id is persisted
         idGenerator.persistCurrentID();
      }
   }

   public synchronized void stop(boolean ioCriticalError) throws Exception
   {
      if (!started)
      {
         return;
      }

      if (!ioCriticalError)
      {
         performCachedLargeMessageDeletes();
         // Must call close to make sure last id is persisted
         if (journalLoaded && idGenerator != null)
            idGenerator.persistCurrentID();
      }

      final CountDownLatch latch = new CountDownLatch(1);
      executor.execute(new Runnable()
      {
         @Override
         public void run()
         {
            latch.countDown();
         }
      });

      latch.await(30, TimeUnit.SECONDS);


      // We cache the variable as the replicator could be changed between here and the time we call stop
      // since sendLiveIsStoping my issue a close back from the channel
      // and we want to ensure a stop here just in case
      ReplicationManager replicatorInUse = replicator;
      if (replicatorInUse != null)
      {
         final OperationContext token = replicator.sendLiveIsStopping(LiveStopping.FAIL_OVER);
         if (token != null)
         {
            try
            {
               token.waitCompletion(5000);
            }
            catch (Exception e)
            {
               // ignore it
            }
         }
         replicatorInUse.stop();
      }
      bindingsJournal.stop();

      messageJournal.stop();

      singleThreadExecutor.shutdown();

      journalLoaded = false;

      started = false;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   /**
    * TODO: Is this still being used ?
    */
   public JournalLoadInformation[] loadInternalOnly() throws Exception
   {
      readLock();
      try
      {
         JournalLoadInformation[] info = new JournalLoadInformation[2];
         info[0] = bindingsJournal.loadInternalOnly();
         info[1] = messageJournal.loadInternalOnly();

         return info;
      }
      finally
      {
         readUnLock();
      }
   }

   public void beforePageRead() throws Exception
   {
      if (pageMaxConcurrentIO != null)
      {
         pageMaxConcurrentIO.acquire();
      }
   }

   public void afterPageRead() throws Exception
   {
      if (pageMaxConcurrentIO != null)
      {
         pageMaxConcurrentIO.release();
      }
   }

   @Override
   public ByteBuffer allocateDirectBuffer(int size)
   {
      return journalFF.allocateDirectBuffer(size);
   }

   @Override
   public void freeDirectBuffer(ByteBuffer buffer)
   {
      journalFF.releaseBuffer(buffer);
   }

   // Public -----------------------------------------------------------------------------------

   public Journal getMessageJournal()
   {
      return messageJournal;
   }

   public Journal getBindingsJournal()
   {
      return bindingsJournal;
   }

   // Package protected ---------------------------------------------

   private void confirmLargeMessage(final LargeServerMessage largeServerMessage)
   {
      if (largeServerMessage.getPendingRecordID() >= 0)
      {
         try
         {
            confirmPendingLargeMessage(largeServerMessage.getPendingRecordID());
            largeServerMessage.setPendingRecordID(-1);
         }
         catch (Exception e)
         {
            ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
         }
      }
   }

   // This should be accessed from this package only
   void deleteLargeMessageFile(final LargeServerMessage largeServerMessage) throws ActiveMQException
   {
      if (largeServerMessage.getPendingRecordID() < 0)
      {
         try
         {
            // The delete file happens asynchronously
            // And the client won't be waiting for the actual file to be deleted.
            // We set a temporary record (short lived) on the journal
            // to avoid a situation where the server is restarted and pending large message stays on forever
            largeServerMessage.setPendingRecordID(storePendingLargeMessage(largeServerMessage.getMessageID()));
         }
         catch (Exception e)
         {
            throw new ActiveMQInternalErrorException(e.getMessage(), e);
         }
      }
      final SequentialFile file = largeServerMessage.getFile();
      if (file == null)
      {
         return;
      }

      if (largeServerMessage.isDurable() && isReplicated())
      {
         readLock();
         try
         {
            if (isReplicated() && replicator.isSynchronizing())
            {
               synchronized (largeMessagesToDelete)
               {
                  largeMessagesToDelete.add(Long.valueOf(largeServerMessage.getMessageID()));
                  confirmLargeMessage(largeServerMessage);
               }
               return;
            }
         }
         finally
         {
            readUnLock();
         }
      }
      Runnable deleteAction = new Runnable()
      {
         public void run()
         {
            try
            {
               readLock();
               try
               {
                  if (replicator != null)
                  {
                     replicator.largeMessageDelete(largeServerMessage.getMessageID());
                  }
                  file.delete();

                  // The confirm could only be done after the actual delete is done
                  confirmLargeMessage(largeServerMessage);
               }
               finally
               {
                  readUnLock();
               }
            }
            catch (Exception e)
            {
               ActiveMQServerLogger.LOGGER.journalErrorDeletingMessage(e, largeServerMessage.getMessageID());
            }
         }

      };

      if (executor == null)
      {
         deleteAction.run();
      }
      else
      {
         executor.execute(deleteAction);
      }
   }

   SequentialFile createFileForLargeMessage(final long messageID, final boolean durable)
   {
      if (durable)
      {
         return createFileForLargeMessage(messageID, LargeMessageExtension.DURABLE);
      }
      else
      {
         return createFileForLargeMessage(messageID, LargeMessageExtension.TEMPORARY);
      }
   }


   public SequentialFile createFileForLargeMessage(final long messageID, LargeMessageExtension extension)
   {
      return largeMessagesFactory.createSequentialFile(messageID + extension.getExtension());
   }


   // Private ----------------------------------------------------------------------------------

   private void checkAndCreateDir(final File dir, final boolean create)
   {
      if (!dir.exists())
      {
         if (create)
         {
            if (!dir.mkdirs())
            {
               throw new IllegalStateException("Failed to create directory " + dir);
            }
         }
         else
         {
            throw ActiveMQMessageBundle.BUNDLE.cannotCreateDir(dir.getAbsolutePath());
         }
      }
   }

   /**
    * @param messages
    * @param buff
    * @return
    * @throws Exception
    */
   private LargeServerMessage parseLargeMessage(final Map<Long, ServerMessage> messages, final ActiveMQBuffer buff) throws Exception
   {
      LargeServerMessage largeMessage = createLargeMessage();

      LargeMessageEncoding messageEncoding = new LargeMessageEncoding(largeMessage);

      messageEncoding.decode(buff);

      if (largeMessage.containsProperty(Message.HDR_ORIG_MESSAGE_ID))
      {
         // for compatibility: couple with old behaviour, copying the old file to avoid message loss
         long originalMessageID = largeMessage.getLongProperty(Message.HDR_ORIG_MESSAGE_ID);

         SequentialFile currentFile = createFileForLargeMessage(largeMessage.getMessageID(), true);

         if (!currentFile.exists())
         {
            SequentialFile linkedFile = createFileForLargeMessage(originalMessageID, true);
            if (linkedFile.exists())
            {
               linkedFile.copyTo(currentFile);
               linkedFile.close();
            }
         }

         currentFile.close();
      }

      return largeMessage;
   }

   private void loadPreparedTransactions(final PostOffice postOffice,
                                         final PagingManager pagingManager,
                                         final ResourceManager resourceManager,
                                         final Map<Long, QueueBindingInfo> queueInfos,
                                         final List<PreparedTransactionInfo> preparedTransactions,
                                         final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                         final Map<Long, PageSubscription> pageSubscriptions,
                                         final Set<Pair<Long, Long>> pendingLargeMessages, JournalLoader journalLoader) throws Exception
   {
      // recover prepared transactions
      for (PreparedTransactionInfo preparedTransaction : preparedTransactions)
      {
         XidEncoding encodingXid = new XidEncoding(preparedTransaction.extraData);

         Xid xid = encodingXid.xid;

         Transaction tx = new TransactionImpl(preparedTransaction.id, xid, this);

         List<MessageReference> referencesToAck = new ArrayList<MessageReference>();

         Map<Long, ServerMessage> messages = new HashMap<Long, ServerMessage>();

         // Use same method as load message journal to prune out acks, so they don't get added.
         // Then have reacknowledge(tx) methods on queue, which needs to add the page size

         // first get any sent messages for this tx and recreate
         for (RecordInfo record : preparedTransaction.records)
         {
            byte[] data = record.data;

            ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(data);

            byte recordType = record.getUserRecordType();

            switch (recordType)
            {
               case JournalRecordIds.ADD_LARGE_MESSAGE:
               {
                  messages.put(record.id, parseLargeMessage(messages, buff));

                  break;
               }
               case JournalRecordIds.ADD_MESSAGE:
               {
                  ServerMessage message = new ServerMessageImpl(record.id, 50);

                  message.decode(buff);

                  messages.put(record.id, message);

                  break;
               }
               case JournalRecordIds.ADD_REF:
               {
                  long messageID = record.id;

                  RefEncoding encoding = new RefEncoding();

                  encoding.decode(buff);

                  ServerMessage message = messages.get(messageID);

                  if (message == null)
                  {
                     throw new IllegalStateException("Cannot find message with id " + messageID);
                  }

                  journalLoader.handlePreparedSendMessage(message, tx, encoding.queueID);

                  break;
               }
               case JournalRecordIds.ACKNOWLEDGE_REF:
               {
                  long messageID = record.id;

                  RefEncoding encoding = new RefEncoding();

                  encoding.decode(buff);

                  journalLoader.handlePreparedAcknowledge(messageID, referencesToAck, encoding.queueID);

                  break;
               }
               case JournalRecordIds.PAGE_TRANSACTION:
               {

                  PageTransactionInfo pageTransactionInfo = new PageTransactionInfoImpl();

                  pageTransactionInfo.decode(buff);

                  if (record.isUpdate)
                  {
                     PageTransactionInfo pgTX = pagingManager.getTransaction(pageTransactionInfo.getTransactionID());
                     pgTX.reloadUpdate(this, pagingManager, tx, pageTransactionInfo.getNumberOfMessages());
                  }
                  else
                  {
                     pageTransactionInfo.setCommitted(false);

                     tx.putProperty(TransactionPropertyIndexes.PAGE_TRANSACTION, pageTransactionInfo);

                     pagingManager.addTransaction(pageTransactionInfo);

                     tx.addOperation(new FinishPageMessageOperation());
                  }

                  break;
               }
               case SET_SCHEDULED_DELIVERY_TIME:
               {
                  // Do nothing - for prepared txs, the set scheduled delivery time will only occur in a send in which
                  // case the message will already have the header for the scheduled delivery time, so no need to do
                  // anything.

                  break;
               }
               case DUPLICATE_ID:
               {
                  // We need load the duplicate ids at prepare time too
                  DuplicateIDEncoding encoding = new DuplicateIDEncoding();

                  encoding.decode(buff);

                  DuplicateIDCache cache = postOffice.getDuplicateIDCache(encoding.address);

                  cache.load(tx, encoding.duplID);

                  break;
               }
               case ACKNOWLEDGE_CURSOR:
               {
                  CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
                  encoding.decode(buff);

                  encoding.position.setRecordID(record.id);

                  PageSubscription sub = locateSubscription(encoding.queueID,
                                                            pageSubscriptions,
                                                            queueInfos,
                                                            pagingManager);

                  if (sub != null)
                  {
                     sub.reloadPreparedACK(tx, encoding.position);
                     referencesToAck.add(new PagedReferenceImpl(encoding.position, null, sub));
                  }
                  else
                  {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueReloadingACK(encoding.queueID);
                  }
                  break;
               }
               case PAGE_CURSOR_COUNTER_VALUE:
               {
                  ActiveMQServerLogger.LOGGER.journalPAGEOnPrepared();

                  break;
               }

               case PAGE_CURSOR_COUNTER_INC:
               {
                  PageCountRecordInc encoding = new PageCountRecordInc();

                  encoding.decode(buff);

                  PageSubscription sub = locateSubscription(encoding.queueID,
                                                            pageSubscriptions,
                                                            queueInfos,
                                                            pagingManager);

                  if (sub != null)
                  {
                     sub.getCounter().applyIncrementOnTX(tx, record.id, encoding.value);
                     sub.notEmpty();
                  }
                  else
                  {
                     ActiveMQServerLogger.LOGGER.journalCannotFindQueueReloadingACK(encoding.queueID);
                  }

                  break;
               }

               default:
               {
                  ActiveMQServerLogger.LOGGER.journalInvalidRecordType(recordType);
               }
            }
         }

         for (RecordInfo recordDeleted : preparedTransaction.recordsToDelete)
         {
            byte[] data = recordDeleted.data;

            if (data.length > 0)
            {
               ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(data);
               byte b = buff.readByte();

               switch (b)
               {
                  case ADD_LARGE_MESSAGE_PENDING:
                  {
                     long messageID = buff.readLong();
                     if (!pendingLargeMessages.remove(new Pair<Long, Long>(recordDeleted.id, messageID)))
                     {
                        ActiveMQServerLogger.LOGGER.largeMessageNotFound(recordDeleted.id);
                     }
                     installLargeMessageConfirmationOnTX(tx, recordDeleted.id);
                     break;
                  }
                  default:
                     ActiveMQServerLogger.LOGGER.journalInvalidRecordTypeOnPreparedTX(b);
               }
            }

         }

         journalLoader.handlePreparedTransaction(tx, referencesToAck, xid, resourceManager);
      }
   }

   private void cleanupIncompleteFiles() throws Exception
   {
      if (largeMessagesFactory != null)
      {
         List<String> tmpFiles = largeMessagesFactory.listFiles("tmp");
         for (String tmpFile : tmpFiles)
         {
            SequentialFile file = largeMessagesFactory.createSequentialFile(tmpFile);
            file.delete();
         }
      }
   }

   private OperationContext getContext(final boolean sync)
   {
      if (sync)
      {
         return getContext();
      }
      else
      {
         return DummyOperationContext.getInstance();
      }
   }

   private static ClassLoader getThisClassLoader()
   {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return JournalStorageManager.class.getClassLoader();
         }
      });
   }

   // Inner Classes
   // ----------------------------------------------------------------------------

   private static final class DummyOperationContext implements OperationContext
   {

      private static DummyOperationContext instance = new DummyOperationContext();

      public static OperationContext getInstance()
      {
         return DummyOperationContext.instance;
      }

      public void executeOnCompletion(final IOCallback runnable)
      {
         // There are no executeOnCompletion calls while using the DummyOperationContext
         // However we keep the code here for correctness
         runnable.done();
      }

      public void replicationDone()
      {
      }

      public void replicationLineUp()
      {
      }

      public void storeLineUp()
      {
      }

      public void done()
      {
      }

      public void onError(final int errorCode, final String errorMessage)
      {
      }

      public void waitCompletion()
      {
      }

      public boolean waitCompletion(final long timeout)
      {
         return true;
      }

      public void pageSyncLineUp()
      {
      }

      public void pageSyncDone()
      {
      }
   }

   /**
    * It's public as other classes may want to unparse data on tools
    */
   public static class XidEncoding implements EncodingSupport
   {
      public final Xid xid;

      XidEncoding(final Xid xid)
      {
         this.xid = xid;
      }

      XidEncoding(final byte[] data)
      {
         xid = XidCodecSupport.decodeXid(ActiveMQBuffers.wrappedBuffer(data));
      }

      public void decode(final ActiveMQBuffer buffer)
      {
         throw new IllegalStateException("Non Supported Operation");
      }

      public void encode(final ActiveMQBuffer buffer)
      {
         XidCodecSupport.encodeXid(xid, buffer);
      }

      public int getEncodeSize()
      {
         return XidCodecSupport.getXidEncodeLength(xid);
      }
   }

   protected static class HeuristicCompletionEncoding implements EncodingSupport
   {
      public Xid xid;

      public boolean isCommit;

      @Override
      public String toString()
      {
         return "HeuristicCompletionEncoding [xid=" + xid + ", isCommit=" + isCommit + "]";
      }

      HeuristicCompletionEncoding(final Xid xid, final boolean isCommit)
      {
         this.xid = xid;
         this.isCommit = isCommit;
      }

      HeuristicCompletionEncoding()
      {
      }

      public void decode(final ActiveMQBuffer buffer)
      {
         xid = XidCodecSupport.decodeXid(buffer);
         isCommit = buffer.readBoolean();
      }

      public void encode(final ActiveMQBuffer buffer)
      {
         XidCodecSupport.encodeXid(xid, buffer);
         buffer.writeBoolean(isCommit);
      }

      public int getEncodeSize()
      {
         return XidCodecSupport.getXidEncodeLength(xid) + DataConstants.SIZE_BOOLEAN;
      }
   }

   private static class GroupingEncoding implements EncodingSupport, GroupingInfo
   {
      public long id;

      public SimpleString groupId;

      public SimpleString clusterName;

      public GroupingEncoding(final long id, final SimpleString groupId, final SimpleString clusterName)
      {
         this.id = id;
         this.groupId = groupId;
         this.clusterName = clusterName;
      }

      public GroupingEncoding()
      {
      }

      public int getEncodeSize()
      {
         return SimpleString.sizeofString(groupId) + SimpleString.sizeofString(clusterName);
      }

      public void encode(final ActiveMQBuffer buffer)
      {
         buffer.writeSimpleString(groupId);
         buffer.writeSimpleString(clusterName);
      }

      public void decode(final ActiveMQBuffer buffer)
      {
         groupId = buffer.readSimpleString();
         clusterName = buffer.readSimpleString();
      }

      public long getId()
      {
         return id;
      }

      public void setId(final long id)
      {
         this.id = id;
      }

      public SimpleString getGroupId()
      {
         return groupId;
      }

      public SimpleString getClusterName()
      {
         return clusterName;
      }

      @Override
      public String toString()
      {
         return "GroupingEncoding [id=" + id + ", groupId=" + groupId + ", clusterName=" + clusterName + "]";
      }
   }

   public static class PersistentQueueBindingEncoding implements EncodingSupport, QueueBindingInfo
   {
      public long id;

      public SimpleString name;

      public SimpleString address;

      public SimpleString filterString;

      public boolean autoCreated;

      public SimpleString user;

      public PersistentQueueBindingEncoding()
      {
      }

      @Override
      public String toString()
      {
         return "PersistentQueueBindingEncoding [id=" + id +
            ", name=" +
            name +
            ", address=" +
            address +
            ", filterString=" +
            filterString +
            ", user=" +
            user +
            ", autoCreated=" +
            autoCreated +
            "]";
      }

      public PersistentQueueBindingEncoding(final SimpleString name,
                                            final SimpleString address,
                                            final SimpleString filterString,
                                            final SimpleString user,
                                            final boolean autoCreated)
      {
         this.name = name;
         this.address = address;
         this.filterString = filterString;
         this.user = user;
         this.autoCreated = autoCreated;
      }

      public long getId()
      {
         return id;
      }

      public void setId(final long id)
      {
         this.id = id;
      }

      public SimpleString getAddress()
      {
         return address;
      }

      public void replaceQueueName(SimpleString newName)
      {
         this.name = newName;
      }

      public SimpleString getFilterString()
      {
         return filterString;
      }

      public SimpleString getQueueName()
      {
         return name;
      }

      public SimpleString getUser()
      {
         return user;
      }

      public boolean isAutoCreated()
      {
         return autoCreated;
      }

      public void decode(final ActiveMQBuffer buffer)
      {
         name = buffer.readSimpleString();
         address = buffer.readSimpleString();
         filterString = buffer.readNullableSimpleString();

         String metadata = buffer.readNullableSimpleString().toString();
         if (metadata != null)
         {
            String[] elements = metadata.split(";");
            for (String element : elements)
            {
               String[] keyValuePair = element.split("=");
               if (keyValuePair.length == 2)
               {
                  if (keyValuePair[0].equals("user"))
                  {
                     user = SimpleString.toSimpleString(keyValuePair[1]);
                  }
               }
            }
         }

         autoCreated = buffer.readBoolean();
      }

      public void encode(final ActiveMQBuffer buffer)
      {
         buffer.writeSimpleString(name);
         buffer.writeSimpleString(address);
         buffer.writeNullableSimpleString(filterString);
         buffer.writeNullableSimpleString(createMetadata());
         buffer.writeBoolean(autoCreated);
      }

      public int getEncodeSize()
      {
         return SimpleString.sizeofString(name) + SimpleString.sizeofString(address) +
            SimpleString.sizeofNullableString(filterString) + DataConstants.SIZE_BOOLEAN +
            SimpleString.sizeofNullableString(createMetadata());
      }

      private SimpleString createMetadata()
      {
         StringBuilder metadata = new StringBuilder();
         metadata.append("user=").append(user).append(";");
         return SimpleString.toSimpleString(metadata.toString());
      }
   }

   public static class LargeMessageEncoding implements EncodingSupport
   {
      public final LargeServerMessage message;

      public LargeMessageEncoding(final LargeServerMessage message)
      {
         this.message = message;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.journal.EncodingSupport#decode(org.apache.activemq.artemis.spi.core.remoting.ActiveMQBuffer)
       */
      public void decode(final ActiveMQBuffer buffer)
      {
         message.decodeHeadersAndProperties(buffer);
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.journal.EncodingSupport#encode(org.apache.activemq.artemis.spi.core.remoting.ActiveMQBuffer)
       */
      public void encode(final ActiveMQBuffer buffer)
      {
         message.encode(buffer);
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return message.getEncodeSize();
      }

   }

   public static class PendingLargeMessageEncoding implements EncodingSupport
   {
      public long largeMessageID;

      public PendingLargeMessageEncoding(final long pendingLargeMessageID)
      {
         this.largeMessageID = pendingLargeMessageID;
      }

      public PendingLargeMessageEncoding()
      {
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.journal.EncodingSupport#decode(org.apache.activemq.artemis.spi.core.remoting.ActiveMQBuffer)
       */
      public void decode(final ActiveMQBuffer buffer)
      {
         largeMessageID = buffer.readLong();
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.journal.EncodingSupport#encode(org.apache.activemq.artemis.spi.core.remoting.ActiveMQBuffer)
       */
      public void encode(final ActiveMQBuffer buffer)
      {
         buffer.writeLong(largeMessageID);
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG;
      }

      @Override
      public String toString()
      {
         return "PendingLargeMessageEncoding::MessageID=" + largeMessageID;
      }

   }

   public static class DeliveryCountUpdateEncoding implements EncodingSupport
   {
      public long queueID;

      public int count;

      public DeliveryCountUpdateEncoding()
      {
         super();
      }

      public DeliveryCountUpdateEncoding(final long queueID, final int count)
      {
         super();
         this.queueID = queueID;
         this.count = count;
      }

      public void decode(final ActiveMQBuffer buffer)
      {
         queueID = buffer.readLong();
         count = buffer.readInt();
      }

      public void encode(final ActiveMQBuffer buffer)
      {
         buffer.writeLong(queueID);
         buffer.writeInt(count);
      }

      public int getEncodeSize()
      {
         return 8 + 4;
      }

      @Override
      public String toString()
      {
         return "DeliveryCountUpdateEncoding [queueID=" + queueID + ", count=" + count + "]";
      }

   }

   public static class QueueEncoding implements EncodingSupport
   {
      public long queueID;

      public QueueEncoding(final long queueID)
      {
         super();
         this.queueID = queueID;
      }

      public QueueEncoding()
      {
         super();
      }

      public void decode(final ActiveMQBuffer buffer)
      {
         queueID = buffer.readLong();
      }

      public void encode(final ActiveMQBuffer buffer)
      {
         buffer.writeLong(queueID);
      }

      public int getEncodeSize()
      {
         return 8;
      }

      @Override
      public String toString()
      {
         return "QueueEncoding [queueID=" + queueID + "]";
      }

   }

   private static class DeleteEncoding implements EncodingSupport
   {
      public byte recordType;

      public long id;

      public DeleteEncoding(final byte recordType, final long id)
      {
         this.recordType = recordType;
         this.id = id;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.journal.EncodingSupport#getEncodeSize()
       */
      @Override
      public int getEncodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_LONG;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.journal.EncodingSupport#encode(org.apache.activemq.artemis.api.core.ActiveMQBuffer)
       */
      @Override
      public void encode(ActiveMQBuffer buffer)
      {
         buffer.writeByte(recordType);
         buffer.writeLong(id);
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.journal.EncodingSupport#decode(org.apache.activemq.artemis.api.core.ActiveMQBuffer)
       */
      @Override
      public void decode(ActiveMQBuffer buffer)
      {
         recordType = buffer.readByte();
         id = buffer.readLong();
      }
   }

   public static class RefEncoding extends QueueEncoding
   {
      public RefEncoding()
      {
         super();
      }

      public RefEncoding(final long queueID)
      {
         super(queueID);
      }
   }

   public static class PageUpdateTXEncoding implements EncodingSupport
   {

      public long pageTX;

      public int recods;

      @Override
      public String toString()
      {
         return "PageUpdateTXEncoding [pageTX=" + pageTX + ", recods=" + recods + "]";
      }

      public PageUpdateTXEncoding()
      {
      }

      public PageUpdateTXEncoding(final long pageTX, final int records)
      {
         this.pageTX = pageTX;
         this.recods = records;
      }

      public void decode(ActiveMQBuffer buffer)
      {
         this.pageTX = buffer.readLong();
         this.recods = buffer.readInt();
      }

      @Override
      public void encode(ActiveMQBuffer buffer)
      {
         buffer.writeLong(pageTX);
         buffer.writeInt(recods);
      }

      @Override
      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG + DataConstants.SIZE_INT;
      }

      public List<MessageReference> getRelatedMessageReferences()
      {
         return null;
      }
   }

   protected static class ScheduledDeliveryEncoding extends QueueEncoding
   {
      long scheduledDeliveryTime;

      @Override
      public String toString()
      {
         return "ScheduledDeliveryEncoding [scheduledDeliveryTime=" + scheduledDeliveryTime + "]";
      }

      private ScheduledDeliveryEncoding(final long scheduledDeliveryTime, final long queueID)
      {
         super(queueID);
         this.scheduledDeliveryTime = scheduledDeliveryTime;
      }

      public ScheduledDeliveryEncoding()
      {
      }

      @Override
      public int getEncodeSize()
      {
         return super.getEncodeSize() + 8;
      }

      @Override
      public void encode(final ActiveMQBuffer buffer)
      {
         super.encode(buffer);
         buffer.writeLong(scheduledDeliveryTime);
      }

      @Override
      public void decode(final ActiveMQBuffer buffer)
      {
         super.decode(buffer);
         scheduledDeliveryTime = buffer.readLong();
      }
   }

   public static class DuplicateIDEncoding implements EncodingSupport
   {
      SimpleString address;

      byte[] duplID;

      public DuplicateIDEncoding(final SimpleString address, final byte[] duplID)
      {
         this.address = address;

         this.duplID = duplID;
      }

      public DuplicateIDEncoding()
      {
      }

      public void decode(final ActiveMQBuffer buffer)
      {
         address = buffer.readSimpleString();

         int size = buffer.readInt();

         duplID = new byte[size];

         buffer.readBytes(duplID);
      }

      public void encode(final ActiveMQBuffer buffer)
      {
         buffer.writeSimpleString(address);

         buffer.writeInt(duplID.length);

         buffer.writeBytes(duplID);
      }

      public int getEncodeSize()
      {
         return SimpleString.sizeofString(address) + DataConstants.SIZE_INT + duplID.length;
      }

      @Override
      public String toString()
      {
         // this would be useful when testing. Most tests on the testsuite will use a SimpleString on the duplicate ID
         // and this may be useful to validate the journal on those tests
         // You may uncomment these two lines on that case and replcate the toString for the PrintData

         // SimpleString simpleStr = new SimpleString(duplID);
         // return "DuplicateIDEncoding [address=" + address + ", duplID=" + simpleStr + "]";

         String bridgeRepresentation = null;

         // The bridge will generate IDs on these terms:
         // This will make them easier to read
         if (address.toString().startsWith("BRIDGE") && duplID.length == 24)
         {
            try
            {
               ByteBuffer buff = ByteBuffer.wrap(duplID);

               // 16 for UUID
               byte[] bytesUUID = new byte[16];

               buff.get(bytesUUID);

               UUID uuid = new UUID(UUID.TYPE_TIME_BASED, bytesUUID);

               long id = buff.getLong();
               bridgeRepresentation = "nodeUUID=" + uuid.toString() + " messageID=" + id;
            }
            catch (Throwable ignored)
            {
               bridgeRepresentation = null;
            }
         }

         if (bridgeRepresentation != null)
         {
            return "DuplicateIDEncoding [address=" + address + ", duplID=" + ByteUtil.bytesToHex(duplID, 2) + " / " +
               bridgeRepresentation + "]";
         }
         else
         {
            return "DuplicateIDEncoding [address=" + address + ", duplID=" + ByteUtil.bytesToHex(duplID, 2) + "]";
         }
      }
   }

   /**
    * This is only used when loading a transaction.
    * <p>
    * it might be possible to merge the functionality of this class with
    * {@link org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager.FinishPageMessageOperation}
    */
   // TODO: merge this class with the one on the PagingStoreImpl
   private static class FinishPageMessageOperation extends TransactionOperationAbstract implements TransactionOperation
   {

      @Override
      public void afterCommit(final Transaction tx)
      {
         // If part of the transaction goes to the queue, and part goes to paging, we can't let depage start for the
         // transaction until all the messages were added to the queue
         // or else we could deliver the messages out of order

         PageTransactionInfo pageTransaction = (PageTransactionInfo) tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);

         if (pageTransaction != null)
         {
            pageTransaction.commit();
         }
      }

      @Override
      public void afterRollback(final Transaction tx)
      {
         PageTransactionInfo pageTransaction = (PageTransactionInfo) tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);

         if (tx.getState() == State.PREPARED && pageTransaction != null)
         {
            pageTransaction.rollback();
         }
      }
   }

   protected static final class PageCountRecord implements EncodingSupport
   {

      @Override
      public String toString()
      {
         return "PageCountRecord [queueID=" + queueID + ", value=" + value + "]";
      }

      PageCountRecord()
      {

      }

      PageCountRecord(long queueID, long value)
      {
         this.queueID = queueID;
         this.value = value;
      }

      long queueID;

      long value;

      @Override
      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG * 2;
      }

      @Override
      public void encode(ActiveMQBuffer buffer)
      {
         buffer.writeLong(queueID);
         buffer.writeLong(value);
      }

      @Override
      public void decode(ActiveMQBuffer buffer)
      {
         queueID = buffer.readLong();
         value = buffer.readLong();
      }

   }

   protected static final class PageCountPendingImpl implements EncodingSupport, PageCountPending
   {

      @Override
      public String toString()
      {
         return "PageCountPending [queueID=" + queueID + ", pageID=" + pageID + "]";
      }

      PageCountPendingImpl()
      {

      }

      PageCountPendingImpl(long queueID, long pageID, int inc)
      {
         this.queueID = queueID;
         this.pageID = pageID;
      }

      long id;

      long queueID;

      long pageID;


      public void setID(long id)
      {
         this.id = id;
      }

      public long getID()
      {
         return id;
      }

      public long getQueueID()
      {
         return queueID;
      }

      public long getPageID()
      {
         return pageID;
      }

      @Override
      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG * 2;
      }

      @Override
      public void encode(ActiveMQBuffer buffer)
      {
         buffer.writeLong(queueID);
         buffer.writeLong(pageID);
      }

      @Override
      public void decode(ActiveMQBuffer buffer)
      {
         queueID = buffer.readLong();
         pageID = buffer.readLong();
      }

   }

   protected static final class PageCountRecordInc implements EncodingSupport
   {

      @Override
      public String toString()
      {
         return "PageCountRecordInc [queueID=" + queueID + ", value=" + value + "]";
      }

      PageCountRecordInc()
      {

      }

      PageCountRecordInc(long queueID, int value)
      {
         this.queueID = queueID;
         this.value = value;
      }

      long queueID;

      int value;

      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG + DataConstants.SIZE_INT;
      }

      public void encode(ActiveMQBuffer buffer)
      {
         buffer.writeLong(queueID);
         buffer.writeInt(value);
      }

      public void decode(ActiveMQBuffer buffer)
      {
         queueID = buffer.readLong();
         value = buffer.readInt();
      }

   }

   public static class CursorAckRecordEncoding implements EncodingSupport
   {
      public CursorAckRecordEncoding(final long queueID, final PagePosition position)
      {
         this.queueID = queueID;
         this.position = position;
      }

      public CursorAckRecordEncoding()
      {
         this.position = new PagePositionImpl();
      }

      @Override
      public String toString()
      {
         return "CursorAckRecordEncoding [queueID=" + queueID + ", position=" + position + "]";
      }

      public long queueID;

      public PagePosition position;

      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG + DataConstants.SIZE_LONG + DataConstants.SIZE_INT;
      }

      public void encode(ActiveMQBuffer buffer)
      {
         buffer.writeLong(queueID);
         buffer.writeLong(position.getPageNr());
         buffer.writeInt(position.getMessageNr());
      }

      public void decode(ActiveMQBuffer buffer)
      {
         queueID = buffer.readLong();
         long pageNR = buffer.readLong();
         int messageNR = buffer.readInt();
         this.position = new PagePositionImpl(pageNR, messageNR);
      }
   }

   private class LargeMessageTXFailureCallback implements TransactionFailureCallback
   {
      private final Map<Long, ServerMessage> messages;

      public LargeMessageTXFailureCallback(final Map<Long, ServerMessage> messages)
      {
         super();
         this.messages = messages;
      }

      public void failedTransaction(final long transactionID,
                                    final List<RecordInfo> records,
                                    final List<RecordInfo> recordsToDelete)
      {
         for (RecordInfo record : records)
         {
            if (record.userRecordType == ADD_LARGE_MESSAGE)
            {
               byte[] data = record.data;

               ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(data);

               try
               {
                  LargeServerMessage serverMessage = parseLargeMessage(messages, buff);
                  serverMessage.decrementDelayDeletionCount();
               }
               catch (Exception e)
               {
                  ActiveMQServerLogger.LOGGER.journalError(e);
               }
            }
         }
      }

   }

   public static final class AckDescribe
   {
      public RefEncoding refEncoding;

      public AckDescribe(RefEncoding refEncoding)
      {
         this.refEncoding = refEncoding;
      }

      @Override
      public String toString()
      {
         return "ACK;" + refEncoding;
      }

   }


   /**
    * @param id
    * @param buffer
    * @return
    */
   protected static PersistedRoles newSecurityRecord(long id, ActiveMQBuffer buffer)
   {
      PersistedRoles roles = new PersistedRoles();
      roles.decode(buffer);
      roles.setStoreId(id);
      return roles;
   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   static PersistedAddressSetting newAddressEncoding(long id, ActiveMQBuffer buffer)
   {
      PersistedAddressSetting setting = new PersistedAddressSetting();
      setting.decode(buffer);
      setting.setStoreId(id);
      return setting;
   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   static GroupingEncoding newGroupEncoding(long id, ActiveMQBuffer buffer)
   {
      GroupingEncoding encoding = new GroupingEncoding();
      encoding.decode(buffer);
      encoding.setId(id);
      return encoding;
   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   protected static PersistentQueueBindingEncoding newBindingEncoding(long id, ActiveMQBuffer buffer)
   {
      PersistentQueueBindingEncoding bindingEncoding = new PersistentQueueBindingEncoding();

      bindingEncoding.decode(buffer);

      bindingEncoding.setId(id);
      return bindingEncoding;
   }

   @Override
   public boolean
   addToPage(PagingStore store, ServerMessage msg, Transaction tx, RouteContextList listCtx) throws Exception
   {
      /**
       * Exposing the read-lock here is an encapsulation violation done in order to keep the code
       * simpler. The alternative would be to add a second method, say 'verifyPaging', to
       * PagingStore.
       * <p>
       * Adding this second method would also be more surprise prone as it would require a certain
       * calling order.
       * <p>
       * The reasoning is that exposing the lock is more explicit and therefore `less bad`.
       */
      return store.page(msg, tx, listCtx, storageManagerLock.readLock());
   }

   private void installLargeMessageConfirmationOnTX(Transaction tx, long recordID)
   {
      TXLargeMessageConfirmationOperation txoper = (TXLargeMessageConfirmationOperation) tx.getProperty(TransactionPropertyIndexes.LARGE_MESSAGE_CONFIRMATIONS);
      if (txoper == null)
      {
         txoper = new TXLargeMessageConfirmationOperation();
         tx.putProperty(TransactionPropertyIndexes.LARGE_MESSAGE_CONFIRMATIONS, txoper);
      }
      txoper.confirmedMessages.add(recordID);
   }

   final class TXLargeMessageConfirmationOperation extends TransactionOperationAbstract
   {
      public List<Long> confirmedMessages = new LinkedList<Long>();

      @Override
      public void afterRollback(Transaction tx)
      {
         for (Long msg : confirmedMessages)
         {
            try
            {
               JournalStorageManager.this.confirmPendingLargeMessage(msg);
            }
            catch (Throwable e)
            {
               ActiveMQServerLogger.LOGGER.journalErrorConfirmingLargeMessage(e, msg);
            }
         }
      }
   }
}
