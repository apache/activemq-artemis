/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.api.core;

import java.io.Serializable;

public class QueueAttributes implements Serializable {

   public static final String ROUTING_TYPE = "routing-type";
   public static final String FILTER_STRING = "filter-string";
   public static final String DURABLE = "durable";
   public static final String MAX_CONSUMERS = "max-consumers";
   public static final String EXCLUSIVE = "exclusive";
   public static final String LAST_VALUE = "last-value";
   public static final String LAST_VALUE_KEY = "last-value-key";
   public static final String NON_DESTRUCTIVE = "non-destructive";
   public static final String PURGE_ON_NO_CONSUMERS = "purge-on-no-consumers";
   public static final String CONSUMERS_BEFORE_DISPATCH = "consumers-before-dispatch";
   public static final String DELAY_BEFORE_DISPATCH = "delay-before-dispatch";
   public static final String CONSUMER_PRIORITY = "consumer-priority";

   private RoutingType routingType;
   private SimpleString filterString;
   private Boolean durable;
   private Integer maxConsumers;
   private Boolean exclusive;
   private Boolean lastValue;
   private SimpleString lastValueKey;
   private Boolean nonDestructive;
   private Boolean purgeOnNoConsumers;
   private Integer consumersBeforeDispatch;
   private Long delayBeforeDispatch;
   private Integer consumerPriority;

   public void set(String key, String value) {
      if (key != null && value != null) {
         if (key.equals(ROUTING_TYPE)) {
            setRoutingType(RoutingType.valueOf(value.toUpperCase()));
         } else if (key.equals(FILTER_STRING)) {
            setFilterString(SimpleString.toSimpleString(value));
         } else if (key.equals(DURABLE)) {
            setDurable(Boolean.valueOf(value));
         } else if (key.equals(MAX_CONSUMERS)) {
            setMaxConsumers(Integer.valueOf(value));
         } else if (key.equals(EXCLUSIVE)) {
            setExclusive(Boolean.valueOf(value));
         } else if (key.equals(LAST_VALUE)) {
            setLastValue(Boolean.valueOf(value));
         } else if (key.equals(LAST_VALUE_KEY)) {
            setLastValueKey(SimpleString.toSimpleString(value));
         } else if (key.equals(NON_DESTRUCTIVE)) {
            setNonDestructive(Boolean.valueOf(value));
         } else if (key.equals(PURGE_ON_NO_CONSUMERS)) {
            setPurgeOnNoConsumers(Boolean.valueOf(value));
         } else if (key.equals(CONSUMERS_BEFORE_DISPATCH)) {
            setConsumersBeforeDispatch(Integer.valueOf(value));
         } else if (key.equals(DELAY_BEFORE_DISPATCH)) {
            setDelayBeforeDispatch(Long.valueOf(value));
         } else if (key.equals(CONSUMER_PRIORITY)) {
            setConsumerPriority(Integer.valueOf(value));
         }
      }
   }

   public RoutingType getRoutingType() {
      return routingType;
   }

   public QueueAttributes setRoutingType(RoutingType routingType) {
      this.routingType = routingType;
      return this;
   }

   public SimpleString getFilterString() {
      return filterString;
   }

   public QueueAttributes setFilterString(SimpleString filterString) {
      this.filterString = filterString;
      return this;
   }

   public Boolean getDurable() {
      return durable;
   }

   public QueueAttributes setDurable(Boolean durable) {
      this.durable = durable;
      return this;
   }

   public Integer getMaxConsumers() {
      return maxConsumers;
   }

   public QueueAttributes setMaxConsumers(Integer maxConsumers) {
      this.maxConsumers = maxConsumers;
      return this;
   }

   public Boolean getExclusive() {
      return exclusive;
   }

   public QueueAttributes setExclusive(Boolean exclusive) {
      this.exclusive = exclusive;
      return this;
   }

   public Boolean getLastValue() {
      return lastValue;
   }

   public QueueAttributes setLastValue(Boolean lastValue) {
      this.lastValue = lastValue;
      return this;
   }

   public SimpleString getLastValueKey() {
      return lastValueKey;
   }

   public QueueAttributes setLastValueKey(SimpleString lastValueKey) {
      this.lastValueKey = lastValueKey;
      return this;
   }

   public Boolean getNonDestructive() {
      return nonDestructive;
   }

   public QueueAttributes setNonDestructive(Boolean nonDestructive) {
      this.nonDestructive = nonDestructive;
      return this;
   }

   public Boolean getPurgeOnNoConsumers() {
      return purgeOnNoConsumers;
   }

   public QueueAttributes setPurgeOnNoConsumers(Boolean purgeOnNoConsumers) {
      this.purgeOnNoConsumers = purgeOnNoConsumers;
      return this;
   }

   public Integer getConsumersBeforeDispatch() {
      return consumersBeforeDispatch;
   }

   public QueueAttributes setConsumersBeforeDispatch(Integer consumersBeforeDispatch) {
      this.consumersBeforeDispatch = consumersBeforeDispatch;
      return this;
   }

   public Long getDelayBeforeDispatch() {
      return delayBeforeDispatch;
   }

   public QueueAttributes setDelayBeforeDispatch(Long delayBeforeDispatch) {
      this.delayBeforeDispatch = delayBeforeDispatch;
      return this;
   }

   public Integer getConsumerPriority() {
      return consumerPriority;
   }

   public QueueAttributes setConsumerPriority(Integer consumerPriority) {
      this.consumerPriority = consumerPriority;
      return this;
   }

}
