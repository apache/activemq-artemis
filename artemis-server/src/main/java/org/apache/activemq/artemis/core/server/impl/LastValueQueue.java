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
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;

/**
 * A queue that will discard messages if a newer message with the same
 * {@link org.apache.activemq.artemis.core.message.impl.CoreMessage#HDR_LAST_VALUE_NAME} property value. In other words it only retains the last
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
                         final Boolean exclusive,
                         final Boolean purgeOnNoConsumers,
                         final ScheduledExecutorService scheduledExecutor,
                         final PostOffice postOffice,
                         final StorageManager storageManager,
                         final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                         final ArtemisExecutor executor,
                         final ActiveMQServer server,
                         final QueueFactory factory) {
      super(persistenceID, address, name, filter, pageSubscription, user, durable, temporary, autoCreated, routingType, maxConsumers, exclusive, purgeOnNoConsumers, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executor, server, factory);
   }

   @Override
   public synchronized void addTail(final MessageReference ref, final boolean direct) {
      if (scheduleIfPossible(ref)) {
         return;
      }

      SimpleString prop = ref.getMessage().getLastValueProperty();

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

      SimpleString lastValueProp = ref.getMessage().getLastValueProperty();

      if (lastValueProp != null) {
         HolderReference hr = map.get(lastValueProp);

         if (hr != null) {
            if (scheduling) {
               // We need to overwrite the old ref with the new one and ack the old one

               replaceLVQMessage(ref, hr);
            } else {
               // We keep the current ref and ack the one we are returning

               super.referenceHandled(ref);

               try {
                  super.acknowledge(ref);
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.errorAckingOldReference(e);
               }
            }
         } else {
            hr = new HolderReference(lastValueProp, ref);

            map.put(lastValueProp, hr);

            super.addHead(hr, scheduling);
         }
      } else {
         super.addHead(ref, scheduling);
      }
   }

   private void replaceLVQMessage(MessageReference ref, HolderReference hr) {
      MessageReference oldRef = hr.getReference();

      referenceHandled(ref);

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
         SimpleString prop = ref.getMessage().getLastValueProperty();

         if (prop != null) {
            map.remove(prop);
         }
      }

      super.refRemoved(ref);
   }

   @Override
   public boolean isLastValue() {
      return true;
   }

   private class HolderReference implements MessageReference {

      private final SimpleString prop;

      private volatile MessageReference ref;

      private long consumerID;

      private boolean hasConsumerID = false;

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
      public long getMessageID() {
         return getMessage().getMessageID();
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
      public void acknowledge(Transaction tx, ServerConsumer consumer) throws Exception {
         ref.acknowledge(tx, consumer);
      }

      @Override
      public void acknowledge(Transaction tx, AckReason reason, ServerConsumer consumer) throws Exception {
         ref.acknowledge(tx, reason, consumer);
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

      @Override
      public void emptyConsumerID() {
         this.hasConsumerID = false;
      }

      @Override
      public void setConsumerId(long consumerID) {
         this.hasConsumerID = true;
         this.consumerID = consumerID;
      }

      @Override
      public boolean hasConsumerId() {
         return hasConsumerID;
      }

      @Override
      public long getConsumerId() {
         if (!this.hasConsumerID) {
            throw new IllegalStateException("consumerID isn't specified: please check hasConsumerId first");
         }
         return this.consumerID;
      }

      @Override
      public long getPersistentSize() throws ActiveMQException {
         return ref.getPersistentSize();
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
