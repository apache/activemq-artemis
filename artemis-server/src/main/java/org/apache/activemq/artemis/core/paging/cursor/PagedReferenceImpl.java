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
import org.apache.activemq.artemis.core.server.ServerMessage;

public class PagedReferenceImpl implements PagedReference {

   private static final boolean isTrace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   private final PagePosition position;

   private WeakReference<PagedMessage> message;

   private Long deliveryTime = null;

   private int persistedCount;

   private int messageEstimate;

   private Long consumerId;

   private final AtomicInteger deliveryCount = new AtomicInteger(0);

   private final PageSubscription subscription;

   private boolean alreadyAcked;

   @Override
   public ServerMessage getMessage() {
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

      if (message == null) {
         this.messageEstimate = -1;
      }
      else {
         this.messageEstimate = message.getMessage().getMemoryEstimate();
      }
      this.message = new WeakReference<>(message);
      this.subscription = subscription;
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
      if (messageEstimate < 0) {
         try {
            messageEstimate = getMessage().getMemoryEstimate();
         }
         catch (Throwable e) {
            ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
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
            ServerMessage msg = getMessage();
            if (msg.containsProperty(Message.HDR_SCHEDULED_DELIVERY_TIME)) {
               deliveryTime = getMessage().getLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
            }
            else {
               deliveryTime = 0L;
            }
         }
         catch (Throwable e) {
            ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
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
      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("++deliveryCount = " + deliveryCount + " for " + this, new Exception("trace"));
      }

   }

   @Override
   public void decrementDeliveryCount() {
      deliveryCount.decrementAndGet();
      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("--deliveryCount = " + deliveryCount + " for " + this, new Exception("trace"));
      }
   }

   @Override
   public Queue getQueue() {
      return subscription.getQueue();
   }

   @Override
   public void handled() {
      getQueue().referenceHandled();
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

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      String msgToString;
      try {
         msgToString = getPagedMessage().toString();
      }
      catch (Throwable e) {
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

}
