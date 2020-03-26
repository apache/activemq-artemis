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
package org.apache.activemq.artemis.core.paging.cursor;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.collections.LinkedListImpl;
import org.jboss.logging.Logger;

public class PagedReferenceImpl extends LinkedListImpl.Node<PagedReferenceImpl> implements PagedReference, Runnable {

   private static final Logger logger = Logger.getLogger(PagedReferenceImpl.class);

   private static final AtomicIntegerFieldUpdater<PagedReferenceImpl> DELIVERY_COUNT_UPDATER = AtomicIntegerFieldUpdater
      .newUpdater(PagedReferenceImpl.class, "deliveryCount");

   private final PagePosition position;

   private WeakReference<PagedMessage> message;

   private static final long UNDEFINED_DELIVERY_TIME = Long.MIN_VALUE;
   private long deliveryTime = UNDEFINED_DELIVERY_TIME;

   private int persistedCount;

   private int messageEstimate = -1;

   private long consumerID;

   private boolean hasConsumerID = false;

   @SuppressWarnings("unused")
   private volatile int deliveryCount = 0;

   private final PageSubscription subscription;

   private boolean alreadyAcked;

   private Object protocolData;

   //0 is false, 1 is true, 2 not defined
   private static final byte IS_NOT_LARGE_MESSAGE = 0;
   private static final byte IS_LARGE_MESSAGE = 1;
   private static final byte UNDEFINED_IS_LARGE_MESSAGE = 2;
   private byte largeMessage;

   private long transactionID = -2;

   private long messageID = -1;

   private long messageSize = -1;

   private Consumer<? super MessageReference> onDelivery;

   //Durable field : 0 is false, 1 is true, -1 not defined
   private static final byte IS_NOT_DURABLE = 0;
   private static final byte IS_DURABLE = 1;
   private static final byte UNDEFINED_IS_DURABLE = -1;
   private byte durable = UNDEFINED_IS_DURABLE;

   @Override
   public Object getProtocolData() {
      return protocolData;
   }

   @Override
   public void setProtocolData(Object protocolData) {
      this.protocolData = protocolData;
   }

   @Override
   public Message getMessage() {
      return getPagedMessage().getMessage();
   }

   @Override
   public void onDelivery(Consumer<? super MessageReference> onDelivery) {
      this.onDelivery = onDelivery;
   }

   /**
    * It will call {@link Consumer#accept(Object)} on {@code this} of the {@link Consumer} registered in {@link #onDelivery(Consumer)}, if any.
    */
   @Override
   public void run() {
      final Consumer<? super MessageReference> onDelivery = this.onDelivery;
      if (onDelivery != null) {
         try {
            onDelivery.accept(this);
         } finally {
            this.onDelivery = null;
         }
      }
   }

   @Override
   public synchronized PagedMessage getPagedMessage() {
      PagedMessage returnMessage = message != null ? message.get() : null;

      // We only keep a few references on the Queue from paging...
      // Besides those references are SoftReferenced on page cache...
      // So, this will unlikely be null,
      // unless the Queue has stalled for some time after paging
      if (returnMessage == null) {
         // reference is gone, we will reconstruct it
         returnMessage = subscription.queryMessage(position);
         message = new WeakReference<>(returnMessage);
      }
      return returnMessage;
   }

   @Override
   public PagePosition getPosition() {
      return position;
   }

   public PagedReferenceImpl(final PagePosition position,
                             final PagedMessage message,
                             final PageSubscription subscription) {
      this.position = position;
      this.message = new WeakReference<>(message);
      this.subscription = subscription;
      if (message != null) {
         this.largeMessage = message.getMessage().isLargeMessage() ? IS_LARGE_MESSAGE : IS_NOT_LARGE_MESSAGE;
         this.transactionID = message.getTransactionID();
         this.messageID = message.getMessage().getMessageID();
         this.durable = message.getMessage().isDurable() ? IS_DURABLE : IS_NOT_DURABLE;
         this.deliveryTime = message.getMessage().getScheduledDeliveryTime();
         //pre-cache the message size so we don't have to reload the message later if it is GC'd
         getPersistentSize();
      } else {
         this.largeMessage = UNDEFINED_IS_LARGE_MESSAGE;
         this.transactionID = -2;
         this.messageID = -1;
         this.messageSize = -1;
         this.durable = UNDEFINED_IS_DURABLE;
         this.deliveryTime = UNDEFINED_DELIVERY_TIME;
      }
   }

   @Override
   public boolean isPaged() {
      return true;
   }

   @Override
   public void setPersistedCount(int count) {
      this.persistedCount = count;
   }

   @Override
   public int getPersistedCount() {
      return persistedCount;
   }

   @Override
   public int getMessageMemoryEstimate() {
      if (messageEstimate <= 0) {
         try {
            messageEstimate = getMessage().getMemoryEstimate();
         } catch (Throwable e) {
            ActiveMQServerLogger.LOGGER.errorCalculateMessageMemoryEstimate(e);
         }
      }
      return messageEstimate;
   }

   @Override
   public MessageReference copy(final Queue queue) {
      return new PagedReferenceImpl(this.position, this.getPagedMessage(), this.subscription);
   }

   @Override
   public long getScheduledDeliveryTime() {
      if (deliveryTime == UNDEFINED_DELIVERY_TIME) {
         try {
            Message msg = getMessage();
            return msg.getScheduledDeliveryTime();
         } catch (Throwable e) {
            ActiveMQServerLogger.LOGGER.errorCalculateScheduledDeliveryTime(e);
            return 0L;
         }
      }
      return deliveryTime;
   }

   @Override
   public void setScheduledDeliveryTime(final long scheduledDeliveryTime) {
      assert scheduledDeliveryTime != UNDEFINED_DELIVERY_TIME : "can't use a reserved value";
      deliveryTime = scheduledDeliveryTime;
   }

   @Override
   public int getDeliveryCount() {
      return DELIVERY_COUNT_UPDATER.get(this);
   }

   @Override
   public void setDeliveryCount(final int deliveryCount) {
      DELIVERY_COUNT_UPDATER.set(this, deliveryCount);
   }

   @Override
   public void incrementDeliveryCount() {
      DELIVERY_COUNT_UPDATER.incrementAndGet(this);
      if (logger.isTraceEnabled()) {
         logger.trace("++deliveryCount = " + deliveryCount + " for " + this, new Exception("trace"));
      }
   }

   @Override
   public void decrementDeliveryCount() {
      DELIVERY_COUNT_UPDATER.decrementAndGet(this);
      if (logger.isTraceEnabled()) {
         logger.trace("--deliveryCount = " + deliveryCount + " for " + this, new Exception("trace"));
      }
   }

   @Override
   public Queue getQueue() {
      return subscription.getQueue();
   }

   @Override
   public void handled() {
      getQueue().referenceHandled(this);
   }

   @Override
   public void setInDelivery(boolean inDelivery) {

   }

   @Override
   public boolean isInDelivery() {
      return false;
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
   public void acknowledge() throws Exception {
      subscription.ack(this);
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

   /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
   @Override
   public String toString() {
      String msgToString;
      try {
         msgToString = getPagedMessage().toString();
      } catch (Throwable e) {
         // in case of an exception because of a missing page, we just want toString to return null
         msgToString = "error:" + e.getMessage();
      }
      return "PagedReferenceImpl [position=" + position +
         ", message=" +
         msgToString +
         ", deliveryTime=" +
         (deliveryTime == UNDEFINED_DELIVERY_TIME ? null : deliveryTime) +
         ", persistedCount=" +
         persistedCount +
         ", deliveryCount=" +
         deliveryCount +
         ", subscription=" +
         subscription +
         "]";
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
   public boolean isLargeMessage() {
      if (largeMessage == UNDEFINED_IS_LARGE_MESSAGE && message != null) {
         initializeIsLargeMessage();
      }
      return largeMessage == IS_LARGE_MESSAGE;
   }

   private void initializeIsLargeMessage() {
      assert largeMessage == UNDEFINED_IS_LARGE_MESSAGE && message != null;
      largeMessage = getMessage().isLargeMessage() ? IS_LARGE_MESSAGE : IS_NOT_LARGE_MESSAGE;
   }

   @Override
   public long getTransactionID() {
      if (transactionID < -1) {
         transactionID = getPagedMessage().getTransactionID();
      }
      return transactionID;
   }

   @Override
   public void addPendingFlag() {
      subscription.addPendingDelivery(position);
   }

   @Override
   public void removePendingFlag() {
      subscription.removePendingDelivery(position);
   }

   @Override
   public long getMessageID() {
      if (messageID < 0) {
         messageID = getPagedMessage().getMessage().getMessageID();
      }
      return messageID;
   }

   @Override
   public SimpleString getLastValueProperty() {
      SimpleString lastValue = getMessage().getSimpleStringProperty(getQueue().getLastValueKey());
      if (lastValue == null) {
         lastValue = getMessage().getLastValueProperty();
      }
      return lastValue;
   }

   @Override
   public long getPersistentSize() {
      if (messageSize == -1) {
         try {
            messageSize = getPagedMessage().getPersistentSize();
         } catch (Throwable e) {
            ActiveMQServerLogger.LOGGER.errorCalculatePersistentSize(e);
         }
      }
      return messageSize;
   }

   @Override
   public boolean isDurable() {
      if (durable == UNDEFINED_IS_DURABLE) {
         durable = getMessage().isDurable() ? IS_DURABLE : IS_NOT_DURABLE;
      }
      return durable == IS_DURABLE;
   }

}
