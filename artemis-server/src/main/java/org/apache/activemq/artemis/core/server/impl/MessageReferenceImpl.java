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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.MemorySize;

/**
 * Implementation of a MessageReference
 */
public class MessageReferenceImpl implements MessageReference {

   private final AtomicInteger deliveryCount = new AtomicInteger();

   private volatile int persistedCount;

   private volatile long scheduledDeliveryTime;

   private final ServerMessage message;

   private final Queue queue;

   private Long consumerID;

   private boolean alreadyAcked;

   private Object protocolData;

   // Static --------------------------------------------------------

   private static final int memoryOffset;

   static {
      // This is an estimate of how much memory a ServerMessageImpl takes up, exclusing body and properties
      // Note, it is only an estimate, it's not possible to be entirely sure with Java
      // This figure is calculated using the test utilities in org.apache.activemq.tests.unit.util.sizeof
      // The value is somewhat higher on 64 bit architectures, probably due to different alignment

      if (MemorySize.is64bitArch()) {
         memoryOffset = 48;
      } else {
         memoryOffset = 32;
      }
   }

   // Constructors --------------------------------------------------

   public MessageReferenceImpl() {
      queue = null;

      message = null;
   }

   public MessageReferenceImpl(final MessageReferenceImpl other, final Queue queue) {
      deliveryCount.set(other.deliveryCount.get());

      scheduledDeliveryTime = other.scheduledDeliveryTime;

      message = other.message;

      this.queue = queue;
   }

   protected MessageReferenceImpl(final ServerMessage message, final Queue queue) {
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
      return deliveryCount.get();
   }

   @Override
   public void setDeliveryCount(final int deliveryCount) {
      this.deliveryCount.set(deliveryCount);
      this.persistedCount = this.deliveryCount.get();
   }

   @Override
   public void incrementDeliveryCount() {
      deliveryCount.incrementAndGet();
   }

   @Override
   public void decrementDeliveryCount() {
      deliveryCount.decrementAndGet();
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
   public ServerMessage getMessage() {
      return message;
   }

   @Override
   public Queue getQueue() {
      return queue;
   }

   @Override
   public void handled() {
      queue.referenceHandled();
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

   @Override
   public void setConsumerId(Long consumerID) {
      this.consumerID = consumerID;
   }

   @Override
   public Long getConsumerId() {
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
}
