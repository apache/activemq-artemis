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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.collections.LinkedListImpl;
import org.jboss.logging.Logger;

public class PagedReferenceImpl extends LinkedListImpl.Node<PagedReferenceImpl> implements PagedReference {

   private static final Logger logger = Logger.getLogger(PagedReferenceImpl.class);

   private final PagePosition position;

   private WeakReference<PagedMessage> message;

   private Long deliveryTime = null;

   private int persistedCount;

   private int messageEstimate = -1;

   private Long consumerId;

   private final AtomicInteger deliveryCount = new AtomicInteger(0);

   private final PageSubscription subscription;

   private boolean alreadyAcked;

   private Object protocolData;

   private Boolean largeMessage;

   private long transactionID = -1;

   private long messageID = -1;

   private long messageSize = -1;

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
         this.largeMessage = message.getMessage().isLargeMessage();
         this.transactionID = message.getTransactionID();
         this.messageID = message.getMessage().getMessageID();

         //pre-cache the message size so we don't have to reload the message later if it is GC'd
         getPersistentSize();
      } else {
         this.largeMessage = null;
         this.transactionID = -1;
         this.messageID = -1;
         this.messageSize = -1;
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
      if (deliveryTime == null) {
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
      deliveryTime = scheduledDeliveryTime;
   }

   @Override
   public int getDeliveryCount() {
      return deliveryCount.get();
   }

   @Override
   public void setDeliveryCount(final int deliveryCount) {
      this.deliveryCount.set(deliveryCount);
   }

   @Override
   public void incrementDeliveryCount() {
      deliveryCount.incrementAndGet();
      if (logger.isTraceEnabled()) {
         logger.trace("++deliveryCount = " + deliveryCount + " for " + this, new Exception("trace"));
      }

   }

   @Override
   public void decrementDeliveryCount() {
      deliveryCount.decrementAndGet();
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
      acknowledge(tx, AckReason.NORMAL);
   }

   @Override
   public void acknowledge(Transaction tx, AckReason reason) throws Exception {
      if (tx == null) {
         getQueue().acknowledge(this, reason);
      } else {
         getQueue().acknowledge(tx, this, reason);
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
         deliveryTime +
         ", persistedCount=" +
         persistedCount +
         ", deliveryCount=" +
         deliveryCount +
         ", subscription=" +
         subscription +
         "]";
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

   @Override
   public boolean isLargeMessage() {
      if (largeMessage == null && message != null) {
         largeMessage = getMessage().isLargeMessage();
      }
      return largeMessage;
   }

   @Override
   public long getTransactionID() {
      if (transactionID < 0) {
         transactionID = getPagedMessage().getTransactionID();
      }
      return transactionID;
   }

   @Override
   public long getMessageID() {
      if (messageID < 0) {
         messageID = getPagedMessage().getMessage().getMessageID();
      }
      return messageID;
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

}
