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
package org.apache.activemq.artemis.core.server.impl;

import javax.transaction.xa.Xid;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.FilterUtils;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscriptionCounter;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.QueueStatus;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.PageCountPending;
import org.apache.activemq.artemis.core.persistence.impl.journal.AddMessageRecord;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.QueueStatusEncoding;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueConfig;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.group.GroupingHandler;
import org.apache.activemq.artemis.core.server.group.impl.GroupBinding;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.jboss.logging.Logger;

public class PostOfficeJournalLoader implements JournalLoader {

   private static final Logger logger = Logger.getLogger(PostOfficeJournalLoader.class);

   protected final PostOffice postOffice;
   protected final PagingManager pagingManager;
   private final StorageManager storageManager;
   private final QueueFactory queueFactory;
   protected final NodeManager nodeManager;
   private final ManagementService managementService;
   private final GroupingHandler groupingHandler;
   private final Configuration configuration;
   private Map<Long, Queue> queues;

   public PostOfficeJournalLoader(PostOffice postOffice,
                                  PagingManager pagingManager,
                                  StorageManager storageManager,
                                  QueueFactory queueFactory,
                                  NodeManager nodeManager,
                                  ManagementService managementService,
                                  GroupingHandler groupingHandler,
                                  Configuration configuration) {

      this.postOffice = postOffice;
      this.pagingManager = pagingManager;
      this.storageManager = storageManager;
      this.queueFactory = queueFactory;
      this.nodeManager = nodeManager;
      this.managementService = managementService;
      this.groupingHandler = groupingHandler;
      this.configuration = configuration;
      queues = new HashMap<>();
   }

   public PostOfficeJournalLoader(PostOffice postOffice,
                                  PagingManager pagingManager,
                                  StorageManager storageManager,
                                  QueueFactory queueFactory,
                                  NodeManager nodeManager,
                                  ManagementService managementService,
                                  GroupingHandler groupingHandler,
                                  Configuration configuration,
                                  Map<Long, Queue> queues) {

      this(postOffice, pagingManager, storageManager, queueFactory, nodeManager, managementService, groupingHandler, configuration);
      this.queues = queues;
   }

   @Override
   public void initQueues(Map<Long, QueueBindingInfo> queueBindingInfosMap,
                          List<QueueBindingInfo> queueBindingInfos) throws Exception {
      int duplicateID = 0;
      for (final QueueBindingInfo queueBindingInfo : queueBindingInfos) {
         queueBindingInfosMap.put(queueBindingInfo.getId(), queueBindingInfo);

         final Filter filter = FilterImpl.createFilter(queueBindingInfo.getFilterString());

         final boolean isTopicIdentification = FilterUtils.isTopicIdentification(filter);

         if (postOffice.getBinding(queueBindingInfo.getQueueName()) != null) {

            if (isTopicIdentification) {
               final long tx = storageManager.generateID();
               storageManager.deleteQueueBinding(tx, queueBindingInfo.getId());
               storageManager.commitBindings(tx);
               continue;
            } else {
               final SimpleString newName = queueBindingInfo.getQueueName().concat("-" + (duplicateID++));
               ActiveMQServerLogger.LOGGER.queueDuplicatedRenaming(queueBindingInfo.getQueueName().toString(), newName.toString());
               queueBindingInfo.replaceQueueName(newName);
            }
         }
         final QueueConfig.Builder queueConfigBuilder;
         if (queueBindingInfo.getAddress() == null) {
            queueConfigBuilder = QueueConfig.builderWith(queueBindingInfo.getId(), queueBindingInfo.getQueueName());
         } else {
            queueConfigBuilder = QueueConfig.builderWith(queueBindingInfo.getId(), queueBindingInfo.getQueueName(), queueBindingInfo.getAddress());
         }
         queueConfigBuilder.filter(filter).pagingManager(pagingManager)
            .user(queueBindingInfo.getUser())
            .durable(true)
            .temporary(false)
            .autoCreated(queueBindingInfo.isAutoCreated())
            .deleteOnNoConsumers(queueBindingInfo.isDeleteOnNoConsumers())
            .maxConsumers(queueBindingInfo.getMaxConsumers())
            .routingType(RoutingType.getType(queueBindingInfo.getRoutingType()));
         final Queue queue = queueFactory.createQueueWith(queueConfigBuilder.build());
         queue.setConsumersRefCount(new AutoCreatedQueueManagerImpl(((PostOfficeImpl)postOffice).getServer(), queueBindingInfo.getQueueName()));

         if (queueBindingInfo.getQueueStatusEncodings() != null) {
            for (QueueStatusEncoding encoding : queueBindingInfo.getQueueStatusEncodings()) {
               if (encoding.getStatus() == QueueStatus.PAUSED)
               queue.reloadPause(encoding.getId());
            }
         }

         final Binding binding = new LocalQueueBinding(queue.getAddress(), queue, nodeManager.getNodeId());

         queues.put(queue.getID(), queue);
         postOffice.addBinding(binding);
         managementService.registerQueue(queue, queue.getAddress(), storageManager);

      }
   }

   @Override
   public void initAddresses(Map<Long, AddressBindingInfo> addressBindingInfosMap,
                          List<AddressBindingInfo> addressBindingInfos) throws Exception {
      for (AddressBindingInfo addressBindingInfo : addressBindingInfos) {
         addressBindingInfosMap.put(addressBindingInfo.getId(), addressBindingInfo);

         AddressInfo addressInfo = new AddressInfo(addressBindingInfo.getName()).setRoutingTypes(addressBindingInfo.getRoutingTypes());
         postOffice.addAddressInfo(addressInfo);
      }
   }

   @Override
   public void handleAddMessage(Map<Long, Map<Long, AddMessageRecord>> queueMap) throws Exception {
      for (Map.Entry<Long, Map<Long, AddMessageRecord>> entry : queueMap.entrySet()) {
         long queueID = entry.getKey();

         Map<Long, AddMessageRecord> queueRecords = entry.getValue();

         Queue queue = this.queues.get(queueID);

         if (queue == null) {
            if (queueRecords.values().size() != 0) {
               ActiveMQServerLogger.LOGGER.journalCannotFindQueueForMessage(queueID);
            }

            continue;
         }

         // Redistribution could install a Redistributor while we are still loading records, what will be an issue with
         // prepared ACKs
         // We make sure te Queue is paused before we reroute values.
         queue.pause();

         Collection<AddMessageRecord> valueRecords = queueRecords.values();

         long currentTime = System.currentTimeMillis();

         for (AddMessageRecord record : valueRecords) {
            long scheduledDeliveryTime = record.getScheduledDeliveryTime();

            if (scheduledDeliveryTime != 0 && scheduledDeliveryTime <= currentTime) {
               scheduledDeliveryTime = 0;
               record.getMessage().removeProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
            }

            if (scheduledDeliveryTime != 0) {
               record.getMessage().putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, scheduledDeliveryTime);
            }

            MessageReference ref = postOffice.reroute(record.getMessage(), queue, null);

            ref.setDeliveryCount(record.getDeliveryCount());

            if (scheduledDeliveryTime != 0) {
               record.getMessage().removeProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
            }
         }
      }
   }

   @Override
   public void handleNoMessageReferences(Map<Long, ServerMessage> messages) {
      for (ServerMessage msg : messages.values()) {
         if (msg.getRefCount() == 0) {
            ActiveMQServerLogger.LOGGER.journalUnreferencedMessage(msg.getMessageID());
            try {
               storageManager.deleteMessage(msg.getMessageID());
            } catch (Exception ignored) {
               ActiveMQServerLogger.LOGGER.journalErrorDeletingMessage(ignored, msg.getMessageID());
            }
         }
      }
   }

   @Override
   public void handleGroupingBindings(List<GroupingInfo> groupingInfos) {
      for (GroupingInfo groupingInfo : groupingInfos) {
         if (groupingHandler != null) {
            groupingHandler.addGroupBinding(new GroupBinding(groupingInfo.getId(), groupingInfo.getGroupId(), groupingInfo.getClusterName()));
         }
      }
   }

   @Override
   public void handleDuplicateIds(Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception {
      for (Map.Entry<SimpleString, List<Pair<byte[], Long>>> entry : duplicateIDMap.entrySet()) {
         SimpleString address = entry.getKey();

         DuplicateIDCache cache = postOffice.getDuplicateIDCache(address);

         if (configuration.isPersistIDCache()) {
            cache.load(entry.getValue());
         }
      }
   }

   @Override
   public void postLoad(Journal messageJournal,
                        ResourceManager resourceManager,
                        Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception {
      for (Queue queue : queues.values()) {
         if (!queue.isPersistedPause()) {
            queue.resume();
         }
      }

      if (System.getProperty("org.apache.activemq.opt.directblast") != null) {
         messageJournal.runDirectJournalBlast();
      }
   }

   @Override
   public void handlePreparedSendMessage(ServerMessage message, Transaction tx, long queueID) throws Exception {
      Queue queue = queues.get(queueID);

      if (queue == null) {
         ActiveMQServerLogger.LOGGER.journalMessageInPreparedTX(queueID);
         return;
      }
      postOffice.reroute(message, queue, tx);
   }

   @Override
   public void handlePreparedAcknowledge(long messageID,
                                         List<MessageReference> referencesToAck,
                                         long queueID) throws Exception {
      Queue queue = queues.get(queueID);

      if (queue == null) {
         throw new IllegalStateException("Cannot find queue with id " + queueID);
      }

      MessageReference removed = queue.removeReferenceWithID(messageID);

      if (removed == null) {
         ActiveMQServerLogger.LOGGER.journalErrorRemovingRef(messageID);
      } else {
         referencesToAck.add(removed);
      }
   }

   @Override
   public void handlePreparedTransaction(Transaction tx,
                                         List<MessageReference> referencesToAck,
                                         Xid xid,
                                         ResourceManager resourceManager) throws Exception {
      for (MessageReference ack : referencesToAck) {
         ack.getQueue().reacknowledge(tx, ack);
      }

      tx.setState(Transaction.State.PREPARED);

      resourceManager.putTransaction(xid, tx);
   }

   /**
    * This method will recover the counters after failures making sure the page counter doesn't get out of sync
    *
    * @param pendingNonTXPageCounter
    * @throws Exception
    */
   @Override
   public void recoverPendingPageCounters(List<PageCountPending> pendingNonTXPageCounter) throws Exception {
      // We need a structure of the following
      // Address -> PageID -> QueueID -> List<PageCountPending>
      // The following loop will sort the records according to the hierarchy we need

      Transaction txRecoverCounter = new TransactionImpl(storageManager);

      Map<SimpleString, Map<Long, Map<Long, List<PageCountPending>>>> perAddressMap = generateMapsOnPendingCount(queues, pendingNonTXPageCounter, txRecoverCounter);

      for (Map.Entry<SimpleString, Map<Long, Map<Long, List<PageCountPending>>>> addressPageMapEntry : perAddressMap.entrySet()) {
         PagingStore store = pagingManager.getPageStore(addressPageMapEntry.getKey());
         Map<Long, Map<Long, List<PageCountPending>>> perPageMap = addressPageMapEntry.getValue();

         // We have already generated this before, so it can't be null
         assert (perPageMap != null);

         for (Long pageId : perPageMap.keySet()) {
            Map<Long, List<PageCountPending>> perQueue = perPageMap.get(pageId);

            // This can't be true!
            assert (perQueue != null);

            if (store.checkPageFileExists(pageId.intValue())) {
               // on this case we need to recalculate the records
               Page pg = store.createPage(pageId.intValue());
               pg.open();

               List<PagedMessage> pgMessages = pg.read(storageManager);
               Map<Long, AtomicInteger> countsPerQueueOnPage = new HashMap<>();

               for (PagedMessage pgd : pgMessages) {
                  if (pgd.getTransactionID() <= 0) {
                     for (long q : pgd.getQueueIDs()) {
                        AtomicInteger countQ = countsPerQueueOnPage.get(q);
                        if (countQ == null) {
                           countQ = new AtomicInteger(0);
                           countsPerQueueOnPage.put(q, countQ);
                        }
                        countQ.incrementAndGet();
                     }
                  }
               }

               for (Map.Entry<Long, List<PageCountPending>> entry : perQueue.entrySet()) {
                  for (PageCountPending record : entry.getValue()) {
                     logger.debug("Deleting pg tempCount " + record.getID());
                     storageManager.deletePendingPageCounter(txRecoverCounter.getID(), record.getID());
                  }

                  PageSubscriptionCounter counter = store.getCursorProvider().getSubscription(entry.getKey()).getCounter();

                  AtomicInteger value = countsPerQueueOnPage.get(entry.getKey());

                  if (value == null) {
                     logger.debug("Page " + entry.getKey() + " wasn't open, so we will just ignore");
                  } else {
                     logger.debug("Replacing counter " + value.get());
                     counter.increment(txRecoverCounter, value.get());
                  }
               }
            } else {
               // on this case the page file didn't exist, we just remove all the records since the page is already gone
               logger.debug("Page " + pageId + " didn't exist on address " + addressPageMapEntry.getKey() + ", so we are just removing records");
               for (List<PageCountPending> records : perQueue.values()) {
                  for (PageCountPending record : records) {
                     logger.debug("Removing pending page counter " + record.getID());
                     storageManager.deletePendingPageCounter(txRecoverCounter.getID(), record.getID());
                     txRecoverCounter.setContainsPersistent();
                  }
               }
            }
         }
      }

      txRecoverCounter.commit();
   }

   @Override
   public void cleanUp() {
      queues.clear();
   }

   /**
    * This generates a map for use on the recalculation and recovery of pending maps after reloading it
    *
    * @param queues
    * @param pendingNonTXPageCounter
    * @param txRecoverCounter
    * @return
    * @throws Exception
    */
   private Map<SimpleString, Map<Long, Map<Long, List<PageCountPending>>>> generateMapsOnPendingCount(Map<Long, Queue> queues,
                                                                                                      List<PageCountPending> pendingNonTXPageCounter,
                                                                                                      Transaction txRecoverCounter) throws Exception {
      Map<SimpleString, Map<Long, Map<Long, List<PageCountPending>>>> perAddressMap = new HashMap<>();
      for (PageCountPending pgCount : pendingNonTXPageCounter) {
         long queueID = pgCount.getQueueID();
         long pageID = pgCount.getPageID();

         // We first figure what Queue is based on the queue id
         Queue queue = queues.get(queueID);

         if (queue == null) {
            logger.debug("removing pending page counter id = " + pgCount.getID() + " as queueID=" + pgCount.getID() + " no longer exists");
            // this means the queue doesn't exist any longer, we will remove it from the storage
            storageManager.deletePendingPageCounter(txRecoverCounter.getID(), pgCount.getID());
            txRecoverCounter.setContainsPersistent();
            continue;
         }

         // Level 1 on the structure, per address
         SimpleString address = queue.getAddress();

         Map<Long, Map<Long, List<PageCountPending>>> perPageMap = perAddressMap.get(address);

         if (perPageMap == null) {
            perPageMap = new HashMap<>();
            perAddressMap.put(address, perPageMap);
         }

         Map<Long, List<PageCountPending>> perQueueMap = perPageMap.get(pageID);

         if (perQueueMap == null) {
            perQueueMap = new HashMap<>();
            perPageMap.put(pageID, perQueueMap);
         }

         List<PageCountPending> pendingCounters = perQueueMap.get(queueID);

         if (pendingCounters == null) {
            pendingCounters = new LinkedList<>();
            perQueueMap.put(queueID, pendingCounters);
         }

         pendingCounters.add(pgCount);

         perQueueMap.put(queueID, pendingCounters);
      }
      return perAddressMap;
   }
}
