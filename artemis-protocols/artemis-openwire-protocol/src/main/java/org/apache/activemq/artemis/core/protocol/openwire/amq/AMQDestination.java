/**
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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

import java.io.IOException;
import java.util.List;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.Usage;

public interface AMQDestination
{
   AMQDeadLetterStrategy DEFAULT_DEAD_LETTER_STRATEGY = new AMQSharedDeadLetterStrategy();
   long DEFAULT_BLOCKED_PRODUCER_WARNING_INTERVAL = 30000;

   void addSubscription(AMQConnectionContext context, AMQSubscription sub) throws Exception;

   void removeSubscription(AMQConnectionContext context, AMQSubscription sub,
         long lastDeliveredSequenceId) throws Exception;

   void addProducer(AMQConnectionContext context, ProducerInfo info) throws Exception;

   void removeProducer(AMQConnectionContext context, ProducerInfo info) throws Exception;

   void send(AMQProducerBrokerExchange producerExchange, Message messageSend) throws Exception;

   void acknowledge(AMQConnectionContext context, AMQSubscription sub,
         final MessageAck ack, final MessageReference node) throws IOException;

   long getInactiveTimoutBeforeGC();

   void markForGC(long timeStamp);

   boolean canGC();

   void gc();

   ActiveMQDestination getActiveMQDestination();

   MemoryUsage getMemoryUsage();

   void setMemoryUsage(MemoryUsage memoryUsage);

   void dispose(AMQConnectionContext context) throws IOException;

   boolean isDisposed();

   AMQDestinationStatistics getDestinationStatistics();

   AMQDeadLetterStrategy getDeadLetterStrategy();

   Message[] browse();

   String getName();

   AMQMessageStore getMessageStore();

   boolean isProducerFlowControl();

   void setProducerFlowControl(boolean value);

   boolean isAlwaysRetroactive();

   void setAlwaysRetroactive(boolean value);

   /**
    * Set's the interval at which warnings about producers being blocked by
    * resource usage will be triggered. Values of 0 or less will disable
    * warnings
    *
    * @param blockedProducerWarningInterval
    *           the interval at which warning about blocked producers will be
    *           triggered.
    */
   void setBlockedProducerWarningInterval(long blockedProducerWarningInterval);

   /**
    *
    * @return the interval at which warning about blocked producers will be
    *         triggered.
    */
   long getBlockedProducerWarningInterval();

   int getMaxProducersToAudit();

   void setMaxProducersToAudit(int maxProducersToAudit);

   int getMaxAuditDepth();

   void setMaxAuditDepth(int maxAuditDepth);

   boolean isEnableAudit();

   void setEnableAudit(boolean enableAudit);

   boolean isActive();

   int getMaxPageSize();

   void setMaxPageSize(int maxPageSize);

   int getMaxBrowsePageSize();

   void setMaxBrowsePageSize(int maxPageSize);

   boolean isUseCache();

   void setUseCache(boolean useCache);

   int getMinimumMessageSize();

   void setMinimumMessageSize(int minimumMessageSize);

   int getCursorMemoryHighWaterMark();

   void setCursorMemoryHighWaterMark(int cursorMemoryHighWaterMark);

   /**
    * optionally called by a Subscriber - to inform the Destination its ready
    * for more messages
    */
   void wakeup();

   /**
    * @return true if lazyDispatch is enabled
    */
   boolean isLazyDispatch();

   /**
    * set the lazy dispatch - default is false
    *
    * @param value
    */
   void setLazyDispatch(boolean value);

   /**
    * Inform the Destination a message has expired
    *
    * @param context
    * @param subs
    * @param node
    */
   void messageExpired(AMQConnectionContext context, AMQSubscription subs,
         MessageReference node);

   /**
    * called when message is consumed
    *
    * @param context
    * @param messageReference
    */
   void messageConsumed(AMQConnectionContext context,
         MessageReference messageReference);

   /**
    * Called when message is delivered to the broker
    *
    * @param context
    * @param messageReference
    */
   void messageDelivered(AMQConnectionContext context,
         MessageReference messageReference);

   /**
    * Called when a message is discarded - e.g. running low on memory This will
    * happen only if the policy is enabled - e.g. non durable topics
    *
    * @param context
    * @param messageReference
    * @param sub
    */
   void messageDiscarded(AMQConnectionContext context, AMQSubscription sub,
         MessageReference messageReference);

   /**
    * Called when there is a slow consumer
    *
    * @param context
    * @param subs
    */
   void slowConsumer(AMQConnectionContext context, AMQSubscription subs);

   /**
    * Called to notify a producer is too fast
    *
    * @param context
    * @param producerInfo
    */
   void fastProducer(AMQConnectionContext context, ProducerInfo producerInfo);

   /**
    * Called when a Usage reaches a limit
    *
    * @param context
    * @param usage
    */
   void isFull(AMQConnectionContext context, Usage<?> usage);

   List<AMQSubscription> getConsumers();

   /**
    * called on Queues in slave mode to allow dispatch to follow subscription
    * choice of master
    *
    * @param messageDispatchNotification
    * @throws Exception
    */
   void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception;

   boolean isPrioritizedMessages();

   AMQSlowConsumerStrategy getSlowConsumerStrategy();

   boolean isDoOptimzeMessageStorage();

   void setDoOptimzeMessageStorage(boolean doOptimzeMessageStorage);

   void clearPendingMessages();

   boolean isDLQ();

   void duplicateFromStore(Message message, AMQSubscription subscription);

}
