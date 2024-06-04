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
package org.apache.activemq.artemis.core.config;

import java.io.Serializable;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;

@Deprecated
public class CoreQueueConfiguration implements Serializable {

   private static final long serialVersionUID = 650404974977490254L;

   private String address = null;

   private String name = null;

   private String filterString = null;

   private boolean durable = true;

   private String user = null;

   private Boolean exclusive;

   private Boolean groupRebalance;

   private Integer groupBuckets;

   private String groupFirstKey;

   private Boolean lastValue;

   private String lastValueKey;

   private Boolean nonDestructive;

   private Integer maxConsumers;

   private Integer consumersBeforeDispatch;

   private Long delayBeforeDispatch;

   private Boolean enabled;

   private Long ringSize = ActiveMQDefaultConfiguration.getDefaultRingSize();

   private Boolean purgeOnNoConsumers = ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers();

   private RoutingType routingType = ActiveMQDefaultConfiguration.getDefaultRoutingType();

   public CoreQueueConfiguration() {
   }

   public String getAddress() {
      return address;
   }

   public String getName() {
      return name;
   }

   public String getFilterString() {
      return filterString;
   }

   public boolean isDurable() {
      return durable;
   }

   public String getUser() {
      return user;
   }

   public Boolean isExclusive() {
      return exclusive;
   }

   public Boolean isGroupRebalance() {
      return groupRebalance;
   }

   public Integer getGroupBuckets() {
      return groupBuckets;
   }

   public String getGroupFirstKey() {
      return groupFirstKey;
   }

   public Boolean isLastValue() {
      return lastValue;
   }

   public String getLastValueKey() {
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

   public Long getRingSize() {
      return ringSize;
   }

   public Boolean isEnabled() {
      return enabled;
   }

   public QueueConfiguration toQueueConfiguration() {
      return QueueConfiguration.of(this.getName())
         .setAddress(this.getAddress())
         .setDurable(this.isDurable())
         .setRoutingType(this.getRoutingType())
         .setExclusive(this.isExclusive())
         .setRingSize(this.getRingSize())
         .setGroupRebalance(this.isGroupRebalance())
         .setNonDestructive(this.isNonDestructive())
         .setLastValue(this.isLastValue())
         .setFilterString(this.getFilterString())
         .setMaxConsumers(this.getMaxConsumers())
         .setPurgeOnNoConsumers(this.getPurgeOnNoConsumers())
         .setConsumersBeforeDispatch(this.getConsumersBeforeDispatch())
         .setDelayBeforeDispatch(this.getDelayBeforeDispatch())
         .setGroupBuckets(this.getGroupBuckets())
         .setGroupFirstKey(this.getGroupFirstKey())
         .setUser(this.getUser())
         .setLastValueKey(this.getLastValueKey())
         .setEnabled(this.isEnabled());
   }

   public static CoreQueueConfiguration fromQueueConfiguration(QueueConfiguration queueConfiguration) {
      return new CoreQueueConfiguration()
         .setAddress(queueConfiguration.getAddress() != null ? queueConfiguration.getAddress().toString() : null)
         .setName(queueConfiguration.getName() != null ? queueConfiguration.getName().toString() : null)
         .setFilterString(queueConfiguration.getFilterString() != null ? queueConfiguration.getFilterString().toString() : null)
         .setDurable(queueConfiguration.isDurable() != null ? queueConfiguration.isDurable() : true)
         .setUser(queueConfiguration.getUser() != null ? queueConfiguration.getUser().toString() : null)
         .setExclusive(queueConfiguration.isExclusive())
         .setGroupRebalance(queueConfiguration.isGroupRebalance())
         .setGroupBuckets(queueConfiguration.getGroupBuckets())
         .setGroupFirstKey(queueConfiguration.getGroupFirstKey() != null ? queueConfiguration.getGroupFirstKey().toString() : null)
         .setLastValue(queueConfiguration.isLastValue())
         .setLastValueKey(queueConfiguration.getLastValueKey() != null ? queueConfiguration.getLastValueKey().toString() : null)
         .setNonDestructive(queueConfiguration.isNonDestructive())
         .setMaxConsumers(queueConfiguration.getMaxConsumers())
         .setConsumersBeforeDispatch(queueConfiguration.getConsumersBeforeDispatch())
         .setDelayBeforeDispatch(queueConfiguration.getDelayBeforeDispatch())
         .setRingSize(queueConfiguration.getRingSize() != null ? queueConfiguration.getRingSize() : ActiveMQDefaultConfiguration.getDefaultRingSize())
         .setEnabled(queueConfiguration.isEnabled() != null ? queueConfiguration.isEnabled() : ActiveMQDefaultConfiguration.getDefaultEnabled())
         .setPurgeOnNoConsumers(queueConfiguration.isPurgeOnNoConsumers() != null ? queueConfiguration.isPurgeOnNoConsumers() : ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers())
         .setRoutingType(queueConfiguration.getRoutingType() != null ? queueConfiguration.getRoutingType() : ActiveMQDefaultConfiguration.getDefaultRoutingType());
   }

   /**
    * @param address the address to set
    */
   public CoreQueueConfiguration setAddress(final String address) {
      this.address = address;
      return this;
   }

   /**
    * @param name the name to set
    */
   public CoreQueueConfiguration setName(final String name) {
      this.name = name;
      return this;
   }

   /**
    * @param filterString the filterString to set
    */
   public CoreQueueConfiguration setFilterString(final String filterString) {
      this.filterString = filterString;
      return this;
   }

   /**
    * @param durable the durable to set; default value is true
    */
   public CoreQueueConfiguration setDurable(final boolean durable) {
      this.durable = durable;
      return this;
   }

   /**
    * @param maxConsumers for this queue, default is -1 (unlimited)
    */
   public CoreQueueConfiguration setMaxConsumers(Integer maxConsumers) {
      this.maxConsumers = maxConsumers;
      return this;
   }

   /**
    * @param consumersBeforeDispatch for this queue, default is 0 (dispatch as soon as 1 consumer)
    */
   public CoreQueueConfiguration setConsumersBeforeDispatch(Integer consumersBeforeDispatch) {
      this.consumersBeforeDispatch = consumersBeforeDispatch;
      return this;
   }

   /**
    * @param delayBeforeDispatch for this queue, default is 0 (start dispatch with no delay)
    */
   public CoreQueueConfiguration setDelayBeforeDispatch(Long delayBeforeDispatch) {
      this.delayBeforeDispatch = delayBeforeDispatch;
      return this;
   }

   /**
    * @param ringSize for this queue, default is -1
    */
   public CoreQueueConfiguration setRingSize(Long ringSize) {
      this.ringSize = ringSize;
      return this;
   }

   /**
    * @param enabled for this queue, default is true
    */
   public CoreQueueConfiguration setEnabled(Boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * @param purgeOnNoConsumers delete this queue when consumer count reaches 0, default is false
    */
   public CoreQueueConfiguration setPurgeOnNoConsumers(Boolean purgeOnNoConsumers) {
      this.purgeOnNoConsumers = purgeOnNoConsumers;
      return this;
   }

   /**
    * @param user the use you want to associate with creating the queue
    */
   public CoreQueueConfiguration setUser(String user) {
      this.user = user;
      return this;
   }

   public CoreQueueConfiguration setExclusive(Boolean exclusive) {
      this.exclusive = exclusive;
      return this;
   }

   public CoreQueueConfiguration setGroupRebalance(Boolean groupRebalance) {
      this.groupRebalance = groupRebalance;
      return this;
   }

   public CoreQueueConfiguration setGroupBuckets(Integer groupBuckets) {
      this.groupBuckets = groupBuckets;
      return this;
   }

   public CoreQueueConfiguration setGroupFirstKey(String groupFirstKey) {
      this.groupFirstKey = groupFirstKey;
      return this;
   }

   public CoreQueueConfiguration setLastValue(Boolean lastValue) {
      this.lastValue = lastValue;
      return this;
   }

   public CoreQueueConfiguration setLastValueKey(String lastValueKey) {
      this.lastValueKey = lastValueKey;
      return this;
   }

   public CoreQueueConfiguration setNonDestructive(Boolean nonDestructive) {
      this.nonDestructive = nonDestructive;
      return this;
   }

   public boolean getPurgeOnNoConsumers() {
      return purgeOnNoConsumers;
   }

   public Integer getMaxConsumers() {
      return maxConsumers;
   }

   public RoutingType getRoutingType() {
      return routingType;
   }

   public CoreQueueConfiguration setRoutingType(RoutingType routingType) {
      this.routingType = routingType;
      return this;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((address == null) ? 0 : address.hashCode());
      result = prime * result + (durable ? 1231 : 1237);
      result = prime * result + ((filterString == null) ? 0 : filterString.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((maxConsumers == null) ? 0 : maxConsumers.hashCode());
      result = prime * result + ((purgeOnNoConsumers == null) ? 0 : purgeOnNoConsumers.hashCode());
      result = prime * result + ((exclusive == null) ? 0 : exclusive.hashCode());
      result = prime * result + ((groupRebalance == null) ? 0 : groupRebalance.hashCode());
      result = prime * result + ((groupBuckets == null) ? 0 : groupBuckets.hashCode());
      result = prime * result + ((groupFirstKey == null) ? 0 : groupFirstKey.hashCode());
      result = prime * result + ((lastValue == null) ? 0 : lastValue.hashCode());
      result = prime * result + ((lastValueKey == null) ? 0 : lastValueKey.hashCode());
      result = prime * result + ((nonDestructive == null) ? 0 : nonDestructive.hashCode());
      result = prime * result + ((consumersBeforeDispatch == null) ? 0 : consumersBeforeDispatch.hashCode());
      result = prime * result + ((delayBeforeDispatch == null) ? 0 : delayBeforeDispatch.hashCode());
      result = prime * result + ((routingType == null) ? 0 : routingType.hashCode());
      result = prime * result + ((ringSize == null) ? 0 : ringSize.hashCode());
      result = prime * result + ((enabled == null) ? 0 : enabled.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      CoreQueueConfiguration other = (CoreQueueConfiguration) obj;
      if (address == null) {
         if (other.address != null)
            return false;
      } else if (!address.equals(other.address))
         return false;
      if (durable != other.durable)
         return false;
      if (filterString == null) {
         if (other.filterString != null)
            return false;
      } else if (!filterString.equals(other.filterString))
         return false;
      if (name == null) {
         if (other.name != null)
            return false;
      } else if (!name.equals(other.name))
         return false;
      if (maxConsumers == null) {
         if (other.maxConsumers != null)
            return false;
      } else if (!maxConsumers.equals(other.maxConsumers))
         return false;
      if (purgeOnNoConsumers == null) {
         if (other.purgeOnNoConsumers != null)
            return false;
      } else if (!purgeOnNoConsumers.equals(other.purgeOnNoConsumers)) {
         return false;
      }
      if (ringSize == null) {
         if (other.ringSize != null)
            return false;
      } else if (!ringSize.equals(other.ringSize)) {
         return false;
      }
      if (enabled == null) {
         if (other.enabled != null)
            return false;
      } else if (!enabled.equals(other.enabled)) {
         return false;
      }
      if (exclusive == null) {
         if (other.exclusive != null)
            return false;
      } else if (!exclusive.equals(other.exclusive)) {
         return false;
      }

      if (groupRebalance == null) {
         if (other.groupRebalance != null)
            return false;
      } else if (!groupRebalance.equals(other.groupRebalance)) {
         return false;
      }

      if (groupBuckets == null) {
         if (other.groupBuckets != null)
            return false;
      } else if (!groupBuckets.equals(other.groupBuckets)) {
         return false;
      }

      if (groupFirstKey == null) {
         if (other.groupFirstKey != null)
            return false;
      } else if (!groupFirstKey.equals(other.groupFirstKey)) {
         return false;
      }

      if (lastValue == null) {
         if (other.lastValue != null)
            return false;
      } else if (!lastValue.equals(other.lastValue)) {
         return false;
      }
      if (lastValueKey == null) {
         if (other.lastValueKey != null)
            return false;
      } else if (!lastValueKey.equals(other.lastValueKey)) {
         return false;
      }
      if (nonDestructive == null) {
         if (other.nonDestructive != null)
            return false;
      } else if (!nonDestructive.equals(other.nonDestructive)) {
         return false;
      }
      if (consumersBeforeDispatch == null) {
         if (other.consumersBeforeDispatch != null)
            return false;
      } else if (!consumersBeforeDispatch.equals(other.consumersBeforeDispatch)) {
         return false;
      }
      if (delayBeforeDispatch == null) {
         if (other.delayBeforeDispatch != null)
            return false;
      } else if (!delayBeforeDispatch.equals(other.delayBeforeDispatch)) {
         return false;
      }
      if (routingType == null) {
         if (other.routingType != null)
            return false;
      } else if (!routingType.equals(other.routingType)) {
         return false;
      }
      return true;
   }

   @Override
   public String toString() {
      return "CoreQueueConfiguration[" +
         "name=" + name +
         ", address=" + address +
         ", routingType=" + routingType +
         ", durable=" + durable +
         ", filterString=" + filterString +
         ", maxConsumers=" + maxConsumers +
         ", purgeOnNoConsumers=" + purgeOnNoConsumers +
         ", exclusive=" + exclusive +
         ", groupRebalance=" + groupRebalance +
         ", groupBuckets=" + groupBuckets +
         ", groupFirstKey=" + groupFirstKey +
         ", lastValue=" + lastValue +
         ", lastValueKey=" + lastValueKey +
         ", nonDestructive=" + nonDestructive +
         ", consumersBeforeDispatch=" + consumersBeforeDispatch +
         ", delayBeforeDispatch=" + delayBeforeDispatch +
         ", ringSize=" + ringSize +
         ", enabled=" + enabled +
         "]";
   }
}
