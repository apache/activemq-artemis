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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;

/**
 * A queue that will discard messages if a newer message with the same
 * {@link org.apache.activemq.artemis.core.message.impl.CoreMessage#HDR_LAST_VALUE_NAME} property value. In other words it only retains the last
 * value
 * <p>
 * This is useful for example, for stock prices, where you're only interested in the latest value
 * for a particular stock
 */
@SuppressWarnings("ALL")
public class LastValueQueue extends QueueImpl {

   private final Map<SimpleString, MessageReference> map = new ConcurrentHashMap<>();
   private final SimpleString lastValueKey;


   @Deprecated
   public LastValueQueue(final long persistenceID,
                         final SimpleString address,
                         final SimpleString name,
                         final Filter filter,
                         final PagingStore pagingStore,
                         final PageSubscription pageSubscription,
                         final SimpleString user,
                         final boolean durable,
                         final boolean temporary,
                         final boolean autoCreated,
                         final RoutingType routingType,
                         final Integer maxConsumers,
                         final Boolean exclusive,
                         final Boolean groupRebalance,
                         final Integer groupBuckets,
                         final SimpleString groupFirstKey,
                         final Integer consumersBeforeDispatch,
                         final Long delayBeforeDispatch,
                         final Boolean purgeOnNoConsumers,
                         final SimpleString lastValueKey,
                         final Boolean nonDestructive,
                         final Boolean autoDelete,
                         final Long autoDeleteDelay,
                         final Long autoDeleteMessageCount,
                         final boolean configurationManaged,
                         final ScheduledExecutorService scheduledExecutor,
                         final PostOffice postOffice,
                         final StorageManager storageManager,
                         final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                         final ArtemisExecutor executor,
                         final ActiveMQServer server,
                         final QueueFactory factory) {
      this(QueueConfiguration.of(name)
              .setId(persistenceID)
              .setAddress(address)
              .setFilterString(filter.getFilterString())
              .setUser(user)
              .setDurable(durable)
              .setTemporary(temporary)
              .setAutoCreated(autoCreated)
              .setRoutingType(routingType)
              .setMaxConsumers(maxConsumers)
              .setExclusive(exclusive)
              .setGroupRebalance(groupRebalance)
              .setGroupBuckets(groupBuckets)
              .setGroupFirstKey(groupFirstKey)
              .setNonDestructive(nonDestructive)
              .setConsumersBeforeDispatch(consumersBeforeDispatch)
              .setDelayBeforeDispatch(delayBeforeDispatch)
              .setPurgeOnNoConsumers(purgeOnNoConsumers)
              .setAutoDelete(autoDelete)
              .setAutoDeleteDelay(autoDeleteDelay)
              .setAutoDeleteMessageCount(autoDeleteMessageCount)
              .setConfigurationManaged(configurationManaged)
              .setLastValueKey(lastValueKey),
           filter,
           pagingStore,
           pageSubscription,
           scheduledExecutor,
           postOffice,
           storageManager,
           addressSettingsRepository,
           executor,
           server,
           factory);
   }

   public LastValueQueue(final QueueConfiguration queueConfiguration,
                         final Filter filter,
                         final PagingStore pagingStore,
                         final PageSubscription pageSubscription,
                         final ScheduledExecutorService scheduledExecutor,
                         final PostOffice postOffice,
                         final StorageManager storageManager,
                         final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                         final ArtemisExecutor executor,
                         final ActiveMQServer server,
                         final QueueFactory factory) {
      super(queueConfiguration, filter, pagingStore, pageSubscription, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executor, server, factory);
      this.lastValueKey = queueConfiguration.getLastValueKey();
   }

   @Override
   public synchronized void addTail(final MessageReference ref, final boolean direct) {
      if (!scheduleIfPossible(ref)) {
         trackLastValue(ref);
         super.addTail(ref, isNonDestructive() ? false : direct);
      }
   }

   @Override
   public void addHead(final MessageReference ref, boolean scheduling) {
      if (scheduling) {
         // track last value when scheduled message is actually enqueued
         trackLastValue(ref);
      } else if (isNonDestructive() == false) {
         // for released messages from a consumer or tx that have been destroyed,
         // use as a last value in the absence of any newer value, it may be stale
         trackLastValueIfAbsent(ref);
      }
      super.addHead(ref, scheduling);
   }

   @Override
   public void addSorted(final MessageReference ref, boolean scheduling) {
      addHead(ref, scheduling);
   }

   private void trackLastValue(MessageReference ref) {
      final SimpleString lastValueProperty = ref.getLastValueProperty();
      if (lastValueProperty != null) {
         map.put(lastValueProperty, ref);
      }
   }

   private void trackLastValueIfAbsent(MessageReference ref) {
      final SimpleString lastValueProperty = ref.getLastValueProperty();
      if (lastValueProperty != null) {
         map.putIfAbsent(lastValueProperty, ref);
      }
   }

   @Override
   public long getMessageCount() {
      // with LV - delivered messages can remain on the queue so the delivering count
      // count must be discounted else we are accounting the same message more than once
      return super.getMessageCount() - getDeliveringCount();
   }

   @Override
   public boolean allowsReferenceCallback() {
      return false;
   }

   @Override
   public QueueConfiguration getQueueConfiguration() {
      return super.getQueueConfiguration().setLastValue(true).setLastValueKey(lastValueKey);
   }

   @Override
   protected void pruneLastValues() {
      // called with synchronized(this) from super.deliver()
      try (LinkedListIterator<MessageReference> iter = messageReferences.iterator()) {
         while (iter.hasNext()) {
            MessageReference ref = iter.next();
            if (!currentLastValue(ref)) {
               iter.remove();
               try {
                  referenceHandled(ref);
                  super.refRemoved(ref);
                  ref.acknowledge(null, AckReason.REPLACED, null);
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.errorAckingOldReference(e);
               }
            }
         }
      }
   }

   private boolean currentLastValue(final MessageReference ref) {
      boolean currentLastValue = false;
      SimpleString lastValueProp = ref.getLastValueProperty();
      if (lastValueProp != null) {
         MessageReference current = map.get(lastValueProp);
         if (current == ref) {
            currentLastValue = true;
         }
      } else {
         // if the ref has no last value
         return true;
      }
      return currentLastValue;
   }

   @Override
   protected void refRemoved(MessageReference ref) {
      removeIfCurrent(ref);
      super.refRemoved(ref);
   }

   @Override
   public void acknowledge(final MessageReference ref, final AckReason reason, final ServerConsumer consumer) throws Exception {
      if (reason == AckReason.EXPIRED || reason == AckReason.KILLED) {
         removeIfCurrent(ref);
      }
      super.acknowledge(ref, reason, consumer);
   }

   @Override
   public void acknowledge(Transaction tx,
                           MessageReference ref,
                           AckReason reason,
                           ServerConsumer consumer, boolean delivering) throws Exception {
      if (reason == AckReason.EXPIRED || reason == AckReason.KILLED) {
         removeIfCurrent(ref);
      }
      super.acknowledge(tx, ref, reason, consumer, delivering);
   }

   @Override
   public synchronized void reload(final MessageReference newRef) {
      trackLastValue(newRef);
      super.reload(newRef);
   }

   private synchronized void removeIfCurrent(MessageReference ref) {
      SimpleString lastValueProp = ref.getLastValueProperty();
      if (lastValueProp != null) {
         MessageReference current = map.get(lastValueProp);
         if (current == ref) {
            map.remove(lastValueProp);
         }
      }
   }

   @Override
   QueueIterateAction createDeleteMatchingAction(AckReason ackReason) {
      QueueIterateAction queueIterateAction = super.createDeleteMatchingAction(ackReason);
      return new QueueIterateAction() {
         @Override
         public boolean actMessage(Transaction tx, MessageReference ref) throws Exception {
            removeIfCurrent(ref);
            return queueIterateAction.actMessage(tx, ref);
         }
      };
   }

   @Override
   public boolean isLastValue() {
      return true;
   }

   @Override
   public SimpleString getLastValueKey() {
      return lastValueKey;
   }

   public synchronized Set<SimpleString> getLastValueKeys() {
      return Collections.unmodifiableSet(map.keySet());
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((map == null) ? 0 : map.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (!(obj instanceof LastValueQueue)) {
         return false;
      }
      LastValueQueue other = (LastValueQueue) obj;
      if (map == null) {
         if (other.map != null) {
            return false;
         }
      } else if (!map.equals(other.map)) {
         return false;
      }
      return true;
   }
}
