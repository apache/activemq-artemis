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
package org.apache.activemq.artemis.core.server;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;

public class QueueQueryResult {

   private SimpleString name;

   private boolean exists;

   private boolean durable;

   private int consumerCount;

   private long messageCount;

   private SimpleString filterString;

   private SimpleString address;

   private boolean temporary;

   private boolean autoCreateQueues;

   private boolean autoCreated;

   private boolean purgeOnNoConsumers;

   private RoutingType routingType;

   private int maxConsumers;

   private Boolean exclusive;

   private Boolean groupRebalance;

   private Boolean groupRebalancePauseDispatch;

   private Integer groupBuckets;

   private SimpleString groupFirstKey;

   private Boolean lastValue;

   private SimpleString lastValueKey;

   private Boolean nonDestructive;

   private Integer consumersBeforeDispatch;

   private Long delayBeforeDispatch;

   private Boolean autoDelete;

   private Long autoDeleteDelay;

   private Long autoDeleteMessageCount;

   private Integer defaultConsumerWindowSize;

   private Long ringSize;

   private Boolean enabled;

   public QueueQueryResult(final SimpleString name,
                           final SimpleString address,
                           final boolean durable,
                           final boolean temporary,
                           final SimpleString filterString,
                           final int consumerCount,
                           final long messageCount,
                           final boolean autoCreateQueues,
                           final boolean exists,
                           final boolean autoCreated,
                           final boolean purgeOnNoConsumers,
                           final RoutingType routingType,
                           final int maxConsumers,
                           final Boolean exclusive,
                           final Boolean groupRebalance,
                           final Boolean groupRebalancePauseDispatch,
                           final Integer groupBuckets,
                           final SimpleString groupFirstKey,
                           final Boolean lastValue,
                           final SimpleString lastValueKey,
                           final Boolean nonDestructive,
                           final Integer consumersBeforeDispatch,
                           final Long delayBeforeDispatch,
                           final Boolean autoDelete,
                           final Long autoDeleteDelay,
                           final Long autoDeleteMessageCount,
                           final Integer defaultConsumerWindowSize,
                           final Long ringSize,
                           final Boolean enabled) {
      this.durable = durable;

      this.temporary = temporary;

      this.consumerCount = consumerCount;

      this.messageCount = messageCount;

      this.filterString = filterString;

      this.address = address;

      this.name = name;

      this.autoCreateQueues = autoCreateQueues;

      this.exists = exists;

      this.autoCreated = autoCreated;

      this.purgeOnNoConsumers = purgeOnNoConsumers;

      this.routingType = routingType;

      this.maxConsumers = maxConsumers;

      this.exclusive = exclusive;

      this.groupRebalance = groupRebalance;

      this.groupRebalancePauseDispatch = groupRebalancePauseDispatch;

      this.groupBuckets = groupBuckets;

      this.groupFirstKey = groupFirstKey;

      this.lastValue = lastValue;

      this.lastValueKey = lastValueKey;

      this.nonDestructive = nonDestructive;

      this.consumersBeforeDispatch = consumersBeforeDispatch;

      this.delayBeforeDispatch = delayBeforeDispatch;

      this.autoDelete = autoDelete;

      this.autoDeleteDelay = autoDeleteDelay;

      this.autoDeleteMessageCount = autoDeleteMessageCount;

      this.defaultConsumerWindowSize = defaultConsumerWindowSize;

      this.ringSize = ringSize;

      this.enabled = enabled;
   }

   public boolean isExists() {
      return exists;
   }

   public boolean isDurable() {
      return durable;
   }

   public int getConsumerCount() {
      return consumerCount;
   }

   public long getMessageCount() {
      return messageCount;
   }

   public SimpleString getFilterString() {
      return filterString;
   }

   public SimpleString getAddress() {
      return address;
   }

   public SimpleString getName() {
      return name;
   }

   public boolean isTemporary() {
      return temporary;
   }

   public boolean isAutoCreateQueues() {
      return autoCreateQueues;
   }

   public boolean isAutoCreated() {
      return autoCreated;
   }

   public boolean isPurgeOnNoConsumers() {
      return purgeOnNoConsumers;
   }

   public RoutingType getRoutingType() {
      return routingType;
   }

   public int getMaxConsumers() {
      return maxConsumers;
   }

   public void setAddress(SimpleString address) {
      this.address = address;
   }

   public Boolean isExclusive() {
      return exclusive;
   }

   public Boolean isLastValue() {
      return lastValue;
   }

   public SimpleString getLastValueKey() {
      return lastValueKey;
   }

   public Boolean isNonDestructive() {
      return nonDestructive;
   }

   public Integer getConsumersBeforeDispatch() {
      return consumersBeforeDispatch;
   }

   public Long getDelayBeforeDispatch() {
      return delayBeforeDispatch;
   }

   public Integer getDefaultConsumerWindowSize() {
      return defaultConsumerWindowSize;
   }

   public Boolean isGroupRebalance() {
      return groupRebalance;
   }

   public Boolean isGroupRebalancePauseDispatch() {
      return groupRebalancePauseDispatch;
   }

   public Integer getGroupBuckets() {
      return groupBuckets;
   }

   public SimpleString getGroupFirstKey() {
      return groupFirstKey;
   }

   public Boolean isAutoDelete() {
      return autoDelete;
   }

   public Long getAutoDeleteDelay() {
      return autoDeleteDelay;
   }

   public Long getAutoDeleteMessageCount() {
      return autoDeleteMessageCount;
   }

   public Long getRingSize() {
      return ringSize;
   }

   public Boolean isEnabled() {
      return enabled;
   }
}
