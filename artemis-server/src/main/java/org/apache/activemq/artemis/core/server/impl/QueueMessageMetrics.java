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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.activemq.artemis.utils.Preconditions;

import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.jboss.logging.Logger;

public class QueueMessageMetrics {

   private static final Logger logger = Logger.getLogger(QueueMessageMetrics.class);

   private static final AtomicIntegerFieldUpdater<QueueMessageMetrics> COUNT_UPDATER =
         AtomicIntegerFieldUpdater.newUpdater(QueueMessageMetrics.class, "messageCount");

   private static final AtomicIntegerFieldUpdater<QueueMessageMetrics> DURABLE_COUNT_UPDATER =
         AtomicIntegerFieldUpdater.newUpdater(QueueMessageMetrics.class, "durableMessageCount");

   private static final AtomicLongFieldUpdater<QueueMessageMetrics> SIZE_UPDATER =
         AtomicLongFieldUpdater.newUpdater(QueueMessageMetrics.class, "persistentSize");

   private static final AtomicLongFieldUpdater<QueueMessageMetrics> DURABLE_SIZE_UPDATER =
         AtomicLongFieldUpdater.newUpdater(QueueMessageMetrics.class, "durablePersistentSize");

   private volatile int messageCount;

   private volatile long persistentSize;

   private volatile int durableMessageCount;

   private volatile long durablePersistentSize;

   private final Queue queue;

   private final String name;

   public QueueMessageMetrics(final Queue queue, final String name) {
      Preconditions.checkNotNull(queue);
      this.queue = queue;
      this.name = name;
   }

   public void incrementMetrics(final MessageReference reference) {
      long size = getPersistentSize(reference);
      COUNT_UPDATER.incrementAndGet(this);
      if (logger.isDebugEnabled()) {
         logger.debugf("%s increment messageCount to %d: %s", this, messageCount, reference);
      }
      SIZE_UPDATER.addAndGet(this, size);
      if (queue.isDurable() && reference.isDurable()) {
         DURABLE_COUNT_UPDATER.incrementAndGet(this);
         DURABLE_SIZE_UPDATER.addAndGet(this, size);
      }
   }

   public void decrementMetrics(final MessageReference reference) {
      long size = -getPersistentSize(reference);
      COUNT_UPDATER.decrementAndGet(this);
      if (logger.isDebugEnabled()) {
         logger.debugf("%s decrement messageCount to %d: %s", this, messageCount, reference);
      }
      SIZE_UPDATER.addAndGet(this, size);
      if (queue.isDurable() && reference.isDurable()) {
         DURABLE_COUNT_UPDATER.decrementAndGet(this);
         DURABLE_SIZE_UPDATER.addAndGet(this, size);
      }
   }



   /**
    * @return the messageCount
    */
   public int getMessageCount() {
      return messageCount;
   }

   /**
    * @param messageCount the messageCount to set
    */
   public void setMessageCount(int messageCount) {
      this.messageCount = messageCount;
   }

   /**
    * @return the persistentSize
    */
   public long getPersistentSize() {
      return persistentSize;
   }

   /**
    * @param persistentSize the persistentSize to set
    */
   public void setPersistentSize(long persistentSize) {
      this.persistentSize = persistentSize;
   }

   /**
    * @return the durableMessageCount
    */
   public int getDurableMessageCount() {
      return durableMessageCount;
   }

   /**
    * @param durableMessageCount the durableMessageCount to set
    */
   public void setDurableMessageCount(int durableMessageCount) {
      this.durableMessageCount = durableMessageCount;
   }

   /**
    * @return the durablePersistentSize
    */
   public long getDurablePersistentSize() {
      return durablePersistentSize;
   }

   /**
    * @param durablePersistentSize the durablePersistentSize to set
    */
   public void setDurablePersistentSize(long durablePersistentSize) {
      this.durablePersistentSize = durablePersistentSize;
   }

   private long getPersistentSize(final MessageReference reference) {
      long size = 0;

      try {
         size = reference.getPersistentSize() > 0 ? reference.getPersistentSize() : 0;
      } catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.errorCalculatePersistentSize(e);
      }

      return size;
   }

   @Override
   public String toString() {
      return "QueuePendingMessageMetrics[queue=" + queue.getName() + ", name=" + name + "]";
   }

}
