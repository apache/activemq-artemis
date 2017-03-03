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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.Message;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.Transaction;

/**
 * A queue that will discard messages if a newer message with the same
 * {@link org.apache.activemq.artemis.core.message.impl.MessageImpl#HDR_LAST_VALUE_NAME} property value. In other words it only retains the last
 * value
 * <p>
 * This is useful for example, for stock prices, where you're only interested in the latest value
 * for a particular stock
 */
public class LastValueQueue extends QueueImpl {

   private final Map<SimpleString, HolderReference> map = new ConcurrentHashMap<>();

   public LastValueQueue(final long persistenceID,
                         final SimpleString address,
                         final SimpleString name,
                         final Filter filter,
                         final PageSubscription pageSubscription,
                         final SimpleString user,
                         final boolean durable,
                         final boolean temporary,
                         final boolean autoCreated,
                         final RoutingType routingType,
                         final Integer maxConsumers,
                         final Boolean purgeOnNoConsumers,
                         final ScheduledExecutorService scheduledExecutor,
                         final PostOffice postOffice,
                         final StorageManager storageManager,
                         final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                         final Executor executor) {
      super(persistenceID, address, name, filter, pageSubscription, user, durable, temporary, autoCreated, routingType, maxConsumers, purgeOnNoConsumers, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executor);
   }

   @Override
   public synchronized void addTail(final MessageReference ref, final boolean direct) {
      if (scheduleIfPossible(ref)) {
         return;
      }

      SimpleString prop = ref.getMessage().getSimpleStringProperty(Message.HDR_LAST_VALUE_NAME.toString());

      if (prop != null) {
         HolderReference hr = map.get(prop);

         if (hr != null) {
            // We need to overwrite the old ref with the new one and ack the old one

            replaceLVQMessage(ref, hr);

         } else {
            hr = new HolderReference(prop, ref);

            map.put(prop, hr);

            super.addTail(hr, direct);
         }
      } else {
         super.addTail(ref, direct);
      }
   }

   @Override
   public synchronized void addHead(final MessageReference ref, boolean scheduling) {
      SimpleString prop = ref.getMessage().getSimpleStringProperty(Message.HDR_LAST_VALUE_NAME.toString());

      if (prop != null) {
         HolderReference hr = map.get(prop);

         if (hr != null) {
            if (scheduling) {
               // We need to overwrite the old ref with the new one and ack the old one

               replaceLVQMessage(ref, hr);
            } else {
               // We keep the current ref and ack the one we are returning

               super.referenceHandled();

               try {
                  super.acknowledge(ref);
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.errorAckingOldReference(e);
               }
            }
         } else {
            hr = new HolderReference(prop, ref);

            map.put(prop, hr);

            super.addHead(hr, scheduling);
         }
      } else {
         super.addHead(ref, scheduling);
      }
   }

   private void replaceLVQMessage(MessageReference ref, HolderReference hr) {
      MessageReference oldRef = hr.getReference();

      referenceHandled();

      try {
         oldRef.acknowledge();
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorAckingOldReference(e);
      }

      hr.setReference(ref);
   }

   @Override
   protected void refRemoved(MessageReference ref) {
      synchronized (this) {
         SimpleString prop = ref.getMessage().getSimpleStringProperty(Message.HDR_LAST_VALUE_NAME.toString());

         if (prop != null) {
            map.remove(prop);
         }
      }

      super.refRemoved(ref);
   }

   private class HolderReference implements MessageReference {

      private final SimpleString prop;

      private volatile MessageReference ref;

      private Long consumerId;

      HolderReference(final SimpleString prop, final MessageReference ref) {
         this.prop = prop;

         this.ref = ref;
      }

      MessageReference getReference() {
         return ref;
      }

      @Override
      public void handled() {
         ref.handled();
         // We need to remove the entry from the map just before it gets delivered
         map.remove(prop);
      }

      @Override
      public Object getProtocolData() {
         return ref.getProtocolData();
      }

      @Override
      public void setProtocolData(Object data) {
         ref.setProtocolData(data);
      }

      @Override
      public void setAlreadyAcked() {
         ref.setAlreadyAcked();
      }

      @Override
      public boolean isAlreadyAcked() {
         return ref.isAlreadyAcked();
      }

      void setReference(final MessageReference ref) {
         this.ref = ref;
      }

      @Override
      public MessageReference copy(final Queue queue) {
         return ref.copy(queue);
      }

      @Override
      public void decrementDeliveryCount() {
         ref.decrementDeliveryCount();
      }

      @Override
      public int getDeliveryCount() {
         return ref.getDeliveryCount();
      }

      @Override
      public Message getMessage() {
         return ref.getMessage();
      }

      @Override
      public Queue getQueue() {
         return ref.getQueue();
      }

      @Override
      public long getScheduledDeliveryTime() {
         return ref.getScheduledDeliveryTime();
      }

      @Override
      public void incrementDeliveryCount() {
         ref.incrementDeliveryCount();
      }

      @Override
      public void setDeliveryCount(final int deliveryCount) {
         ref.setDeliveryCount(deliveryCount);
      }

      @Override
      public void setScheduledDeliveryTime(final long scheduledDeliveryTime) {
         ref.setScheduledDeliveryTime(scheduledDeliveryTime);
      }

      @Override
      public void acknowledge(Transaction tx) throws Exception {
         ref.acknowledge(tx);
      }

      @Override
      public void acknowledge(Transaction tx, AckReason reason) throws Exception {
         ref.acknowledge(tx, reason);
      }

      @Override
      public void setPersistedCount(int count) {
         ref.setPersistedCount(count);
      }

      @Override
      public int getPersistedCount() {
         return ref.getPersistedCount();
      }

      @Override
      public boolean isPaged() {
         return false;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.server.MessageReference#acknowledge(org.apache.activemq.artemis.core.server.MessageReference)
       */
      @Override
      public void acknowledge() throws Exception {
         ref.getQueue().acknowledge(this);
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.server.MessageReference#getMessageMemoryEstimate()
       */
      @Override
      public int getMessageMemoryEstimate() {
         return ref.getMessage().getMemoryEstimate();
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.server.MessageReference#setConsumerId(java.lang.Long)
       */
      @Override
      public void setConsumerId(Long consumerID) {
         this.consumerId = consumerID;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq.artemis.core.server.MessageReference#getConsumerId()
       */
      @Override
      public Long getConsumerId() {
         return this.consumerId;
      }
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
