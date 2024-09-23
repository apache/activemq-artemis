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

package org.apache.activemq.artemis.protocol.amqp.connect.mirror;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import io.netty.util.collection.LongObjectHashMap;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.collections.JournalHashMap;
import org.apache.activemq.artemis.core.journal.collections.JournalHashMapProvider;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.PagedReference;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.AckRetry;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.mirror.MirrorController;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AckManager implements ActiveMQComponent {

   // we first retry on the queue a few times
   private static DisabledAckMirrorController disabledAckMirrorController = new DisabledAckMirrorController();

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final Set<AMQPMirrorControllerTarget> mirrorControllerTargets = new HashSet<>();
   final LongSupplier sequenceGenerator;
   final JournalHashMapProvider<AckRetry, AckRetry, Queue> journalHashMapProvider;
   final ActiveMQServer server;
   final Configuration configuration;
   final ReferenceIDSupplier referenceIDSupplier;
   final IOCriticalErrorListener ioCriticalErrorListener;
   volatile MultiStepProgress progress;
   ActiveMQScheduledComponent scheduledComponent;

   public AckManager(ActiveMQServer server) {
      assert server != null && server.getConfiguration() != null;
      this.server = server;
      this.configuration = server.getConfiguration();
      this.ioCriticalErrorListener = server.getIoCriticalErrorListener();
      this.sequenceGenerator = server.getStorageManager()::generateID;

      // The JournalHashMap has to use the storage manager to guarantee we are using the Replicated Journal Wrapper in case this is a replicated journal
      journalHashMapProvider = new JournalHashMapProvider<>(sequenceGenerator, server.getStorageManager(), AckRetry.getPersister(), JournalRecordIds.ACK_RETRY, OperationContextImpl::getContext, server.getPostOffice()::findQueue, server.getIoCriticalErrorListener());
      this.referenceIDSupplier = new ReferenceIDSupplier(server);
   }

   public void reload(RecordInfo recordInfo) {
      journalHashMapProvider.reload(recordInfo);
   }

   @Override
   public synchronized void stop() {
      if (scheduledComponent != null) {
         scheduledComponent.stop();
         scheduledComponent = null;
      }
      AckManagerProvider.remove(this.server);
      logger.debug("Stopping ackmanager on server {}", server);
   }

   @Override
   public synchronized boolean isStarted() {
      return scheduledComponent != null && scheduledComponent.isStarted();
   }

   @Override
   public synchronized void start() {
      if (logger.isDebugEnabled()) {
         logger.debug("Starting ACKManager on {} with period = {}, minQueueAttempts={}, maxPageAttempts={}", server, configuration.getMirrorAckManagerRetryDelay(), configuration.getMirrorAckManagerQueueAttempts(), configuration.getMirrorAckManagerPageAttempts());
      }
      if (!isStarted()) {
         scheduledComponent = new ActiveMQScheduledComponent(server.getScheduledPool(), server.getExecutorFactory().getExecutor(), server.getConfiguration().getMirrorAckManagerRetryDelay(), server.getConfiguration().getMirrorAckManagerRetryDelay(), TimeUnit.MILLISECONDS, true) {
            @Override
            public void run() {
               beginRetry();
            }
         };
         scheduledComponent.start();
         scheduledComponent.delay();
      } else {
         logger.debug("Starting ignored on server {}", server);
      }
   }

   public void beginRetry() {
      logger.trace("being retry server {}", server);
      if (initRetry()) {
         logger.trace("Starting process to retry, server={}", server);
         progress.nextStep();
      } else {
         logger.trace("Retry already happened");
      }
   }

   public void endRetry() {
      logger.trace("Retry done on server {}", server);
      progress = null;

      // schedule a retry
      if (!sortRetries().isEmpty()) {
         ActiveMQScheduledComponent scheduleComponentReference = scheduledComponent;
         if (scheduleComponentReference != null) {
            try {
               scheduleComponentReference.delay();
            } catch (RejectedExecutionException ree) {
               logger.debug("AckManager could not schedule a new retry due to the executor being shutdown {}", ree.getMessage(), ree);
            }
         }
      }
   }

   public boolean initRetry() {
      if (progress != null) {
         logger.trace("Retry already in progress, we will wait next time, server={}", server);
         return false;
      }

      HashMap<SimpleString, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>>> retries = sortRetries();

      flushMirrorTargets();

      if (retries.isEmpty()) {
         logger.trace("Nothing to retry!, server={}", server);
         return false;
      }

      progress = new MultiStepProgress(retries);
      return true;
   }

   public synchronized void registerMirror(AMQPMirrorControllerTarget mirrorTarget) {
      this.mirrorControllerTargets.add(mirrorTarget);
   }

   public synchronized void unregisterMirror(AMQPMirrorControllerTarget mirrorTarget) {
      this.mirrorControllerTargets.remove(mirrorTarget);
   }

   private void flushMirrorTargets() {
      logger.debug("scanning and flushing mirror targets");
      List<AMQPMirrorControllerTarget> targetCopy = copyTargets();
      targetCopy.forEach(AMQPMirrorControllerTarget::flush);
   }

   private synchronized List<AMQPMirrorControllerTarget> copyTargets() {
      return new ArrayList<>(mirrorControllerTargets);
   }

   // Sort the ACK list by address
   // We have the retries by queue, we need to sort them by address
   // as we will perform all the retries on the same addresses at the same time (in the Multicast case with multiple queues acking)
   public HashMap<SimpleString, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>>> sortRetries() {
      // We will group the retries by address,
      // so we perform all of the queues in the same address at once
      HashMap<SimpleString, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>>> retriesByAddress = new HashMap<>();

      Iterator<JournalHashMap<AckRetry, AckRetry, Queue>> queueRetriesIterator = journalHashMapProvider.getMaps().iterator();

      while (queueRetriesIterator.hasNext()) {
         JournalHashMap<AckRetry, AckRetry, Queue> ackRetries = queueRetriesIterator.next();
         if (!ackRetries.isEmpty()) {
            Queue queue = ackRetries.getContext();
            if (queue != null) {
               SimpleString address = queue.getAddress();
               LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>> queueRetriesOnAddress = retriesByAddress.get(address);
               if (queueRetriesOnAddress == null) {
                  queueRetriesOnAddress = new LongObjectHashMap<>();
                  retriesByAddress.put(address, queueRetriesOnAddress);
               }
               queueRetriesOnAddress.put(queue.getID(), ackRetries);
            }
         }
      }

      return retriesByAddress;
   }

   private boolean isEmpty(LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>> queuesToRetry) {
      AtomicBoolean empty = new AtomicBoolean(true);

      queuesToRetry.forEach((id, journalHashMap) -> {
         if (!journalHashMap.isEmpty()) {
            empty.set(false);
         }
      });

      return empty.get();
   }



   // to be used with the same executor as the PagingStore executor
   public void retryAddress(SimpleString address, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>> acksToRetry) {
      MirrorController previousController = AMQPMirrorControllerTarget.getControllerInUse();
      logger.trace("retrying address {} on server {}", address, server);
      try {
         AMQPMirrorControllerTarget.setControllerInUse(disabledAckMirrorController);

         if (checkRetriesAndPaging(acksToRetry)) {
            logger.trace("scanning paging for {}", address);
            AckRetry key = new AckRetry();

            PagingStore store = server.getPagingManager().getPageStore(address);
            for (long pageId = store.getFirstPage(); pageId <= store.getCurrentWritingPage(); pageId++) {
               if (isEmpty(acksToRetry)) {
                  logger.trace("Retry stopped while reading page {} on address {} as the outcome is now empty, server={}", pageId, address, server);
                  break;
               }
               Page page = openPage(store, pageId);
               if (page == null) {
                  continue;
               }
               try {
                  retryPage(acksToRetry, address, page, key);
               } finally {
                  page.usageDown();
               }
            }
            validateExpiredSet(address, acksToRetry);
         } else {
            logger.trace("Page Scan not required for address {}", address);
         }

      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      } finally {
         AMQPMirrorControllerTarget.setControllerInUse(previousController);
      }
   }

   private Page openPage(PagingStore store, long pageID) throws Throwable {
      Page page = store.newPageObject(pageID);
      if (page.getFile().exists()) {
         page.getMessages();
         return page;
      } else {
         return null;
      }

   }

   private void validateExpiredSet(SimpleString address, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>> queuesToRetry) {
      queuesToRetry.forEach((q, r) -> this.validateExpireSet(address, q, r));
   }

   private void validateExpireSet(SimpleString address, long queueID, JournalHashMap<AckRetry, AckRetry, Queue> retries) {
      for (AckRetry retry : retries.valuesCopy()) {
         if (retry.getQueueAttempts() >= configuration.getMirrorAckManagerQueueAttempts()) {
            if (retry.attemptedPage() >= configuration.getMirrorAckManagerPageAttempts()) {
               if (configuration.isMirrorAckManagerWarnUnacked()) {
                  ActiveMQAMQPProtocolLogger.LOGGER.ackRetryFailed(retry, address, queueID);
               }
               if (logger.isDebugEnabled()) {
                  logger.debug("Retried {} {} times, giving up on the entry now. Configured Page Attempts={}", retry, retry.getPageAttempts(), configuration.getMirrorAckManagerPageAttempts());
               }
               retries.remove(retry);
            } else {
               if (logger.isDebugEnabled()) {
                  logger.debug("Retry {} attempted {} times on paging, Configuration Page Attempts={}", retry, retry.getPageAttempts(), configuration.getMirrorAckManagerPageAttempts());
               }
            }
         } else {
            logger.debug("Retry {} queue attempted {} times on paging, QueueAttempts {} Configuration Page Attempts={}", retry, retry.getQueueAttempts(), retry.getPageAttempts(), configuration.getMirrorAckManagerPageAttempts());
         }
      }
   }

   private void retryPage(LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>> queuesToRetry,
                          SimpleString address,
                          Page page,
                          AckRetry key) throws Exception {
      logger.debug("scanning for acks on page {} on address {}", page.getPageId(), address);
      TransactionImpl transaction = new TransactionImpl(server.getStorageManager()).setAsync(true);
      // scan each page for acks
      page.getMessages().forEach(pagedMessage -> {
         for (int i = 0; i < pagedMessage.getQueueIDs().length; i++) {
            long queueID = pagedMessage.getQueueIDs()[i];
            JournalHashMap<AckRetry, AckRetry, Queue> retries = queuesToRetry.get(queueID);
            if (retries != null) {
               String serverID = referenceIDSupplier.getServerID(pagedMessage.getMessage());
               if (serverID == null) {
                  serverID = referenceIDSupplier.getDefaultNodeID();
               }
               long id = referenceIDSupplier.getID(pagedMessage.getMessage());

               logger.trace("Looking for retry on serverID={}, id={} on server={}", serverID, id, server);
               key.setNodeID(serverID).setMessageID(id);

               AckRetry ackRetry = retries.get(key);

               // we first retry messages in the queue first.
               // this is to avoid messages that are in transit from being depaged into the queue
               if (ackRetry != null && ackRetry.getQueueAttempts() > configuration.getMirrorAckManagerQueueAttempts()) {
                  Queue queue = retries.getContext();

                  if (queue != null) {
                     PageSubscription subscription = queue.getPageSubscription();
                     if (!subscription.isAcked(pagedMessage)) {
                        PagedReference reference = retries.getContext().getPagingStore().getCursorProvider().newReference(pagedMessage, subscription);
                        try {
                           subscription.ackTx(transaction, reference, false);
                           subscription.getQueue().postAcknowledge(reference, ackRetry.getReason(), false);
                        } catch (Exception e) {
                           logger.warn(e.getMessage(), e);
                           if (ioCriticalErrorListener != null) {
                              ioCriticalErrorListener.onIOException(e, e.getMessage(), null);
                           }
                        }
                     }
                     retries.remove(ackRetry, transaction.getID());
                     transaction.setContainsPersistent();
                     logger.trace("retry performed ok, ackRetry={} for message={} on queue", ackRetry, pagedMessage);
                  }
               }
            } else {
               logger.trace("Retry key={} not found server={}", key, server);
            }
         }
      });

      try {
         transaction.commit();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         if (ioCriticalErrorListener != null) {
            ioCriticalErrorListener.onIOException(e, e.getMessage(), null);
         }
      }
   }

   /** returns true if there are retries ready to be scanned on paging */
   private boolean checkRetriesAndPaging(LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>> queuesToRetry) {
      boolean needScanOnPaging = false;
      Iterator<Map.Entry<Long, JournalHashMap<AckRetry, AckRetry, Queue>>> iter = queuesToRetry.entrySet().iterator();

      while (iter.hasNext()) {
         Map.Entry<Long, JournalHashMap<AckRetry, AckRetry, Queue>> entry = iter.next();
         JournalHashMap<AckRetry, AckRetry, Queue> queueRetries = entry.getValue();
         Queue queue = queueRetries.getContext();
         for (AckRetry retry : queueRetries.valuesCopy()) {
            if (ack(retry.getNodeID(), queue, retry.getMessageID(), retry.getReason(), false)) {
               logger.trace("Removing retry {} as the retry went ok", retry);
               queueRetries.remove(retry);
            } else {
               int retried = retry.attemptedQueue();
               if (logger.isTraceEnabled()) {
                  logger.trace("retry {} attempted {} times on the queue", retry, retried);
               }
               if (retried >= configuration.getMirrorAckManagerQueueAttempts()) {
                  needScanOnPaging = true;
               }
            }
         }
      }

      return needScanOnPaging;
   }

   public synchronized void addRetry(String nodeID, Queue queue, long messageID, AckReason reason) {
      if (nodeID == null) {
         nodeID = referenceIDSupplier.getDefaultNodeID();
      }
      AckRetry retry = new AckRetry(nodeID, messageID, reason);
      journalHashMapProvider.getMap(queue.getID(), queue).put(retry, retry);
      if (scheduledComponent != null) {
         // we set the retry delay again in case it was changed.
         scheduledComponent.setPeriod(configuration.getMirrorAckManagerRetryDelay());
         scheduledComponent.delay();
      }
   }

   public boolean ack(String nodeID, Queue targetQueue, long messageID, AckReason reason, boolean allowRetry) {
      if (logger.isTraceEnabled()) {
         logger.trace("performAck (nodeID={}, messageID={}), targetQueue={}, allowRetry={})", nodeID, messageID, targetQueue.getName(), allowRetry);
      }

      MessageReference reference = targetQueue.removeWithSuppliedID(nodeID, messageID, referenceIDSupplier);

      if (reference == null) {
         if (logger.isDebugEnabled()) {
            logger.debug("ACK Manager could not find reference nodeID={} (while localID={}), messageID={} on queue {}, server={}. Adding retry with minQueue={}, maxPage={}, delay={}", nodeID, referenceIDSupplier.getDefaultNodeID(), messageID, targetQueue.getName(), server, configuration.getMirrorAckManagerQueueAttempts(), configuration.getMirrorAckManagerPageAttempts(), configuration.getMirrorAckManagerRetryDelay());
         }

         if (allowRetry) {
            if (configuration != null && configuration.isMirrorAckManagerWarnUnacked() && targetQueue.getConsumerCount() > 0) {
               ActiveMQAMQPProtocolLogger.LOGGER.unackWithConsumer(targetQueue.getConsumerCount(), targetQueue.getName(), nodeID, messageID);
            } else {
               logger.debug("There are {} consumers on queue {}, what made Ack for message with nodeID={}, messageID={} enter a retry list", targetQueue.getConsumerCount(), targetQueue.getName(), nodeID, messageID);
            }
            addRetry(nodeID, targetQueue, messageID, reason);
         }
         return false;
      } else  {
         if (logger.isTraceEnabled()) {
            logger.trace("ack worked well for messageID={} nodeID={} queue={}, reference={}", messageID, nodeID, reference.getQueue().getName(), reference);
            if (reference.isPaged()) {
               logger.trace("position for messageID={} = {}", messageID, ((PagedReference)reference).getPosition());
            }
         }
         doACK(targetQueue, reference, reason);
         return true;
      }
   }

   private void doACK(Queue targetQueue, MessageReference reference, AckReason reason) {
      try {
         switch (reason) {
            case EXPIRED:
               targetQueue.expire(reference, null, false);
               break;
            default:
               TransactionImpl transaction = new TransactionImpl(server.getStorageManager());
               targetQueue.acknowledge(transaction, reference, reason, null, false);
               transaction.commit();
               if (logger.isTraceEnabled()) {
                  logger.trace("Transaction {} committed on acking reference {}", transaction.getID(), reference);
               }
               break;
         }
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      } finally {
         targetQueue.deliverAsync();
      }
   }
   /** The ACKManager will perform the retry on each address's pageStore executor.
    *  it will perform each address individually, one by one. */
   class MultiStepProgress {
      HashMap<SimpleString, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>>> retryList;

      Iterator<Map.Entry<SimpleString, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>>>> retryIterator;


      MultiStepProgress(HashMap<SimpleString, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>>> retryList) {
         this.retryList = retryList;
         retryIterator = retryList.entrySet().iterator();
      }

      public void nextStep() {
         try {
            if (!retryIterator.hasNext()) {
               logger.trace("Iterator is done on retry, server={}", server);
               AckManager.this.endRetry();
            } else {
               Map.Entry<SimpleString, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>>> entry = retryIterator.next();

               //////////////////////////////////////////////////////////////////////
               // Issue a deliverAsync on each queue before doing the retries
               // to make it more likely to hit the ack retry on each queue
               entry.getValue().values().forEach(this::deliveryAsync);

               PagingStore pagingStore = server.getPagingManager().getPageStore(entry.getKey());
               pagingStore.execute(() -> {
                  AckManager.this.retryAddress(entry.getKey(), entry.getValue());
                  nextStep();
               });
            }
         } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
            // there was an exception, I'm clearing the current progress to allow a new one
            AckManager.this.endRetry();
         }
      }

      private void deliveryAsync(JournalHashMap<AckRetry, AckRetry, Queue> map) {
         Queue queue = map.getContext();
         if (queue != null) {
            queue.deliverAsync();
         }
      }
   }



   private static class DisabledAckMirrorController implements MirrorController {

      @Override
      public boolean isRetryACK() {
         return true;
      }

      @Override
      public void addAddress(AddressInfo addressInfo) throws Exception {

      }

      @Override
      public void deleteAddress(AddressInfo addressInfo) throws Exception {

      }

      @Override
      public void createQueue(QueueConfiguration queueConfiguration) throws Exception {

      }

      @Override
      public void deleteQueue(SimpleString addressName, SimpleString queueName) throws Exception {

      }

      @Override
      public void sendMessage(Transaction tx, Message message, RoutingContext context) {

      }

      @Override
      public void postAcknowledge(MessageReference ref, AckReason reason) throws Exception {

      }

      @Override
      public void preAcknowledge(Transaction tx, MessageReference ref, AckReason reason) throws Exception {

      }

      @Override
      public String getRemoteMirrorId() {
         return null;
      }
   }
}
