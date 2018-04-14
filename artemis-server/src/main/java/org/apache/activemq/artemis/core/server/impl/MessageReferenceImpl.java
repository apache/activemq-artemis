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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.collections.LinkedListImpl;

/**
 * Implementation of a MessageReference
 */
public class MessageReferenceImpl extends LinkedListImpl.Node<MessageReferenceImpl> implements MessageReference {

   private static final AtomicIntegerFieldUpdater<MessageReferenceImpl> DELIVERY_COUNT_UPDATER = AtomicIntegerFieldUpdater
      .newUpdater(MessageReferenceImpl.class, "deliveryCount");

   @SuppressWarnings("unused")
   private volatile int deliveryCount = 0;

   private volatile int persistedCount;

   private volatile long scheduledDeliveryTime;

   private final Message message;

   private final Queue queue;

   private long consumerID;

   private boolean hasConsumerID = false;

   private boolean alreadyAcked;

   private Object protocolData;

   // Static --------------------------------------------------------

   private static final int memoryOffset = 64;

   // Constructors --------------------------------------------------

   public MessageReferenceImpl() {
      queue = null;

      message = null;
   }

   public MessageReferenceImpl(final MessageReferenceImpl other, final Queue queue) {
      DELIVERY_COUNT_UPDATER.set(this, other.getDeliveryCount());

      scheduledDeliveryTime = other.scheduledDeliveryTime;

      message = other.message;

      this.queue = queue;
   }

   public MessageReferenceImpl(final Message message, final Queue queue) {
      this.message = message;

      this.queue = queue;
   }

   // MessageReference implementation -------------------------------

   @Override
   public Object getProtocolData() {
      return protocolData;
   }

   @Override
   public void setProtocolData(Object protocolData) {
      this.protocolData = protocolData;
   }

   /**
    * @return the persistedCount
    */
   @Override
   public int getPersistedCount() {
      return persistedCount;
   }

   /**
    * @param persistedCount the persistedCount to set
    */
   @Override
   public void setPersistedCount(int persistedCount) {
      this.persistedCount = persistedCount;
   }

   @Override
   public MessageReference copy(final Queue queue) {
      return new MessageReferenceImpl(this, queue);
   }

   public static int getMemoryEstimate() {
      return MessageReferenceImpl.memoryOffset;
   }

   @Override
   public int getDeliveryCount() {
      return DELIVERY_COUNT_UPDATER.get(this);
   }

   @Override
   public void setDeliveryCount(final int deliveryCount) {
      DELIVERY_COUNT_UPDATER.set(this, deliveryCount);
      this.persistedCount = deliveryCount;
   }

   @Override
   public void incrementDeliveryCount() {
      DELIVERY_COUNT_UPDATER.incrementAndGet(this);
   }

   @Override
   public void decrementDeliveryCount() {
      DELIVERY_COUNT_UPDATER.decrementAndGet(this);
   }

   @Override
   public long getScheduledDeliveryTime() {
      return scheduledDeliveryTime;
   }

   @Override
   public void setScheduledDeliveryTime(final long scheduledDeliveryTime) {
      this.scheduledDeliveryTime = scheduledDeliveryTime;
   }

   @Override
   public Message getMessage() {
      return message;
   }

   @Override
   public long getMessageID() {
      return getMessage().getMessageID();
   }

   @Override
   public Queue getQueue() {
      return queue;
   }

   @Override
   public void handled() {
      queue.referenceHandled(this);
   }

   @Override
   public void setAlreadyAcked() {
      alreadyAcked = true;
   }

   @Override
   public boolean isAlreadyAcked() {
      return alreadyAcked;
   }

   @Override
   public boolean isPaged() {
      return false;
   }

   @Override
   public void acknowledge() throws Exception {
      this.acknowledge(null);
   }

   @Override
   public void acknowledge(Transaction tx) throws Exception {
      acknowledge(tx, null);
   }

   @Override
   public void acknowledge(Transaction tx, ServerConsumer consumer) throws Exception {
      acknowledge(tx, AckReason.NORMAL, consumer);
   }

   @Override
   public void acknowledge(Transaction tx, AckReason reason, ServerConsumer consumer) throws Exception {
      if (tx == null) {
         getQueue().acknowledge(this, reason, consumer);
      } else {
         getQueue().acknowledge(tx, this, reason, consumer);
      }
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
   public int getMessageMemoryEstimate() {
      return message.getMemoryEstimate();
   }

   @Override
   public String toString() {
      return "Reference[" + getMessage().getMessageID() +
         "]:" +
         (getMessage().isDurable() ? "RELIABLE" : "NON-RELIABLE") +
         ":" +
         getMessage();
   }

   @Override
   public boolean equals(Object other) {
      if (this == other) {
         return true;
      }

      if (other instanceof MessageReferenceImpl) {
         MessageReferenceImpl reference = (MessageReferenceImpl) other;

         if (this.getMessage().equals(reference.getMessage()))
            return true;
      }

      return false;
   }

   @Override
   public int hashCode() {
      return this.getMessage().hashCode();
   }

   @Override
   public long getPersistentSize() throws ActiveMQException {
      return this.getMessage().getPersistentSize();
   }
}
