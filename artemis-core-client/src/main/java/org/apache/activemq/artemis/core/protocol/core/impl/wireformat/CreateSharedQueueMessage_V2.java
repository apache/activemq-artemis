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
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.utils.BufferHelper;

public class CreateSharedQueueMessage_V2 extends CreateSharedQueueMessage {

   private RoutingType routingType;
   private Integer maxConsumers;
   private Boolean purgeOnNoConsumers;
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
   private Long ringSize;
   private Boolean enabled;

   public CreateSharedQueueMessage_V2(final QueueConfiguration queueConfiguration, boolean requiresResponse) {
      this(
         queueConfiguration.getAddress(),
         queueConfiguration.getName(),
         queueConfiguration.getRoutingType(),
         queueConfiguration.getFilterString(),
         queueConfiguration.isDurable(),
         queueConfiguration.getMaxConsumers(),
         queueConfiguration.isPurgeOnNoConsumers(),
         queueConfiguration.isExclusive(),
         queueConfiguration.isGroupRebalance(),
         queueConfiguration.isGroupRebalancePauseDispatch(),
         queueConfiguration.getGroupBuckets(),
         queueConfiguration.getGroupFirstKey(),
         queueConfiguration.isLastValue(),
         queueConfiguration.getLastValueKey(),
         queueConfiguration.isNonDestructive(),
         queueConfiguration.getConsumersBeforeDispatch(),
         queueConfiguration.getDelayBeforeDispatch(),
         queueConfiguration.isAutoDelete(),
         queueConfiguration.getAutoDeleteDelay(),
         queueConfiguration.getAutoDeleteMessageCount(),
         queueConfiguration.getRingSize(),
         queueConfiguration.isEnabled(),
         requiresResponse
      );
   }

   public CreateSharedQueueMessage_V2(final SimpleString address,
                                      final SimpleString queueName,
                                      final RoutingType routingType,
                                      final SimpleString filterString,
                                      final boolean durable,
                                      final Integer maxConsumers,
                                      final Boolean purgeOnNoConsumers,
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
                                      final Long ringSize,
                                      final Boolean enabled,
                                      final boolean requiresResponse) {
      this();

      this.address = address;
      this.queueName = queueName;
      this.filterString = filterString;
      this.durable = durable;
      this.routingType = routingType;
      this.maxConsumers = maxConsumers;
      this.purgeOnNoConsumers = purgeOnNoConsumers;
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
      this.ringSize = ringSize;
      this.enabled = enabled;
      this.requiresResponse = requiresResponse;
   }

   public CreateSharedQueueMessage_V2() {
      super(CREATE_SHARED_QUEUE_V2);
   }

   public RoutingType getRoutingType() {
      return routingType;
   }

   public void setRoutingType(RoutingType routingType) {
      this.routingType = routingType;
   }

   public Integer getMaxConsumers() {
      return maxConsumers;
   }

   public void setMaxConsumers(Integer maxConsumers) {
      this.maxConsumers = maxConsumers;
   }

   public Boolean isPurgeOnNoConsumers() {
      return purgeOnNoConsumers;
   }

   public void setPurgeOnNoConsumers(Boolean purgeOnNoConsumers) {
      this.purgeOnNoConsumers = purgeOnNoConsumers;
   }

   public Boolean isExclusive() {
      return exclusive;
   }

   public void setExclusive(Boolean exclusive) {
      this.exclusive = exclusive;
   }

   public Boolean isLastValue() {
      return lastValue;
   }

   public void setLastValue(Boolean lastValue) {
      this.lastValue = lastValue;
   }

   public SimpleString getLastValueKey() {
      return lastValueKey;
   }

   public void setLastValueKey(SimpleString lastValueKey) {
      this.lastValueKey = lastValueKey;
   }

   public Boolean isNonDestructive() {
      return nonDestructive;
   }

   public void setNonDestructive(Boolean nonDestructive) {
      this.nonDestructive = nonDestructive;
   }

   public Integer getConsumersBeforeDispatch() {
      return consumersBeforeDispatch;
   }

   public void setConsumersBeforeDispatch(Integer consumersBeforeDispatch) {
      this.consumersBeforeDispatch = consumersBeforeDispatch;
   }

   public Long getDelayBeforeDispatch() {
      return delayBeforeDispatch;
   }

   public void setDelayBeforeDispatch(Long delayBeforeDispatch) {
      this.delayBeforeDispatch = delayBeforeDispatch;
   }

   public Boolean isGroupRebalance() {
      return groupRebalance;
   }

   public void setGroupRebalance(Boolean groupRebalance) {
      this.groupRebalance = groupRebalance;
   }

   public Boolean isGroupRebalancePauseDispatch() {
      return groupRebalancePauseDispatch;
   }

   public void setGroupRebalancePauseDispatch(Boolean groupRebalancePauseDispatch) {
      this.groupRebalancePauseDispatch = groupRebalancePauseDispatch;
   }

   public Integer getGroupBuckets() {
      return groupBuckets;
   }

   public void setGroupBuckets(Integer groupBuckets) {
      this.groupBuckets = groupBuckets;
   }

   public SimpleString getGroupFirstKey() {
      return groupFirstKey;
   }

   public void setGroupBuckets(SimpleString groupFirstKey) {
      this.groupFirstKey = groupFirstKey;
   }

   public Boolean isAutoDelete() {
      return autoDelete;
   }

   public void setAutoDelete(Boolean autoDelete) {
      this.autoDelete = autoDelete;
   }

   public Long getAutoDeleteDelay() {
      return autoDeleteDelay;
   }

   public void setAutoDeleteDelay(Long autoDeleteDelay) {
      this.autoDeleteDelay = autoDeleteDelay;
   }

   public Long getAutoDeleteMessageCount() {
      return autoDeleteMessageCount;
   }

   public void setAutoDeleteMessageCount(Long autoDeleteMessageCount) {
      this.autoDeleteMessageCount = autoDeleteMessageCount;
   }

   public Long getRingSize() {
      return ringSize;
   }

   public void setRingSize(Long ringSize) {
      this.ringSize = ringSize;
   }

   public Boolean isEnabled() {
      return enabled;
   }

   public void setEnabled(Boolean enabled) {
      this.enabled = enabled;
   }

   public QueueConfiguration toQueueConfiguration() {
      return QueueConfiguration.of(queueName)
         .setAddress(address)
         .setDurable(durable)
         .setRoutingType(routingType)
         .setExclusive(exclusive)
         .setGroupRebalance(groupRebalance)
         .setGroupRebalancePauseDispatch(groupRebalancePauseDispatch)
         .setNonDestructive(nonDestructive)
         .setLastValue(lastValue)
         .setFilterString(filterString)
         .setMaxConsumers(maxConsumers)
         .setPurgeOnNoConsumers(purgeOnNoConsumers)
         .setConsumersBeforeDispatch(consumersBeforeDispatch)
         .setDelayBeforeDispatch(delayBeforeDispatch)
         .setGroupBuckets(groupBuckets)
         .setGroupFirstKey(groupFirstKey)
         .setLastValueKey(lastValueKey)
         .setAutoDelete(autoDelete)
         .setAutoDeleteDelay(autoDeleteDelay)
         .setAutoDeleteMessageCount(autoDeleteMessageCount)
         .setRingSize(ringSize)
         .setEnabled(enabled);
   }

   @Override
   protected String getPacketString() {
      StringBuffer buff = new StringBuffer(super.getPacketString());
      buff.append(", address=" + address);
      buff.append(", queueName=" + queueName);
      buff.append(", filterString=" + filterString);
      buff.append(", durable=" + durable);
      buff.append(", routingType=" + routingType);
      buff.append(", maxConsumers=" + maxConsumers);
      buff.append(", purgeOnNoConsumers=" + purgeOnNoConsumers);
      buff.append(", exclusive=" + exclusive);
      buff.append(", groupRebalance=" + groupRebalance);
      buff.append(", groupRebalancePauseDispatch=" + groupRebalancePauseDispatch);
      buff.append(", groupBuckets=" + groupBuckets);
      buff.append(", groupFirstKey=" + groupFirstKey);
      buff.append(", lastValue=" + lastValue);
      buff.append(", lastValueKey=" + lastValueKey);
      buff.append(", nonDestructive=" + nonDestructive);
      buff.append(", consumersBeforeDispatch=" + consumersBeforeDispatch);
      buff.append(", delayBeforeDispatch=" + delayBeforeDispatch);
      buff.append(", autoDelete=" + autoDelete);
      buff.append(", autoDeleteDelay=" + autoDeleteDelay);
      buff.append(", autoDeleteMessageCount=" + autoDeleteMessageCount);
      buff.append(", ringSize=" + ringSize);
      buff.append(", enabled=" + enabled);
      buff.append(", requiresResponse=" + requiresResponse);
      return buff.toString();
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeSimpleString(address);
      buffer.writeSimpleString(queueName);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeBoolean(durable);
      buffer.writeByte(routingType == null ? -1 : routingType.getType());
      buffer.writeBoolean(requiresResponse);
      BufferHelper.writeNullableInteger(buffer, maxConsumers);
      BufferHelper.writeNullableBoolean(buffer, purgeOnNoConsumers);
      BufferHelper.writeNullableBoolean(buffer, exclusive);
      BufferHelper.writeNullableBoolean(buffer, lastValue);
      buffer.writeNullableSimpleString(lastValueKey);
      BufferHelper.writeNullableBoolean(buffer, nonDestructive);
      BufferHelper.writeNullableInteger(buffer, consumersBeforeDispatch);
      BufferHelper.writeNullableLong(buffer, delayBeforeDispatch);
      BufferHelper.writeNullableBoolean(buffer, groupRebalance);
      BufferHelper.writeNullableInteger(buffer, groupBuckets);
      BufferHelper.writeNullableBoolean(buffer, autoDelete);
      BufferHelper.writeNullableLong(buffer, autoDeleteDelay);
      BufferHelper.writeNullableLong(buffer, autoDeleteMessageCount);
      buffer.writeNullableSimpleString(groupFirstKey);
      BufferHelper.writeNullableLong(buffer, ringSize);
      BufferHelper.writeNullableBoolean(buffer, enabled);
      BufferHelper.writeNullableBoolean(buffer, groupRebalancePauseDispatch);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      address = buffer.readSimpleString();
      queueName = buffer.readSimpleString();
      filterString = buffer.readNullableSimpleString();
      durable = buffer.readBoolean();
      routingType = RoutingType.getType(buffer.readByte());
      requiresResponse = buffer.readBoolean();
      if (buffer.readableBytes() > 0) {
         maxConsumers = BufferHelper.readNullableInteger(buffer);
         purgeOnNoConsumers = BufferHelper.readNullableBoolean(buffer);
         exclusive = BufferHelper.readNullableBoolean(buffer);
         lastValue = BufferHelper.readNullableBoolean(buffer);
      }
      if (buffer.readableBytes() > 0) {
         lastValueKey = buffer.readNullableSimpleString();
         nonDestructive = BufferHelper.readNullableBoolean(buffer);
         consumersBeforeDispatch = BufferHelper.readNullableInteger(buffer);
         delayBeforeDispatch = BufferHelper.readNullableLong(buffer);
         groupRebalance = BufferHelper.readNullableBoolean(buffer);
         groupBuckets = BufferHelper.readNullableInteger(buffer);
         autoDelete = BufferHelper.readNullableBoolean(buffer);
         autoDeleteDelay = BufferHelper.readNullableLong(buffer);
         autoDeleteMessageCount = BufferHelper.readNullableLong(buffer);
      }
      if (buffer.readableBytes() > 0) {
         groupFirstKey = buffer.readNullableSimpleString();
      }
      if (buffer.readableBytes() > 0) {
         ringSize = buffer.readNullableLong();
      }
      if (buffer.readableBytes() > 0) {
         enabled = buffer.readNullableBoolean();
      }
      if (buffer.readableBytes() > 0) {
         groupRebalancePauseDispatch = buffer.readNullableBoolean();
      }
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((address == null) ? 0 : address.hashCode());
      result = prime * result + ((filterString == null) ? 0 : filterString.hashCode());
      result = prime * result + ((queueName == null) ? 0 : queueName.hashCode());
      result = prime * result + (durable ? 1231 : 1237);
      result = prime * result + routingType.getType();
      result = prime * result + (requiresResponse ? 1231 : 1237);
      result = prime * result + (maxConsumers == null ? 0 : maxConsumers.hashCode());
      result = prime * result + (purgeOnNoConsumers == null ? 0 : purgeOnNoConsumers ? 1231 : 1237);
      result = prime * result + (exclusive == null ? 0 : exclusive ? 1231 : 1237);
      result = prime * result + (groupRebalance == null ? 0 : groupRebalance ? 1231 : 1237);
      result = prime * result + (groupRebalancePauseDispatch == null ? 0 : groupRebalancePauseDispatch ? 1231 : 1237);
      result = prime * result + (groupBuckets == null ? 0 : groupBuckets.hashCode());
      result = prime * result + (groupFirstKey == null ? 0 : groupFirstKey.hashCode());
      result = prime * result + (lastValue == null ? 0 : lastValue ? 1231 : 1237);
      result = prime * result + (lastValueKey == null ? 0 : lastValueKey.hashCode());
      result = prime * result + (nonDestructive == null ? 0 : nonDestructive ? 1231 : 1237);
      result = prime * result + (consumersBeforeDispatch == null ? 0 : consumersBeforeDispatch.hashCode());
      result = prime * result + (delayBeforeDispatch == null ? 0 : delayBeforeDispatch.hashCode());
      result = prime * result + (autoDelete == null ? 0 : autoDelete.hashCode());
      result = prime * result + (autoDeleteDelay == null ? 0 : autoDeleteDelay.hashCode());
      result = prime * result + (autoDeleteMessageCount == null ? 0 : autoDeleteMessageCount.hashCode());
      result = prime * result + (ringSize == null ? 0 : ringSize.hashCode());
      result = prime * result + (enabled ? 1231 : 1237);
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof CreateSharedQueueMessage_V2))
         return false;
      CreateSharedQueueMessage_V2 other = (CreateSharedQueueMessage_V2) obj;
      if (address == null) {
         if (other.address != null)
            return false;
      } else if (!address.equals(other.address))
         return false;
      if (filterString == null) {
         if (other.filterString != null)
            return false;
      } else if (!filterString.equals(other.filterString))
         return false;
      if (queueName == null) {
         if (other.queueName != null)
            return false;
      } else if (!queueName.equals(other.queueName))
         return false;
      if (durable != other.durable)
         return false;
      if (routingType != other.routingType)
         return false;
      if (requiresResponse != other.requiresResponse)
         return false;
      if (maxConsumers == null) {
         if (other.maxConsumers != null)
            return false;
      } else if (!maxConsumers.equals(other.maxConsumers))
         return false;
      if (purgeOnNoConsumers == null) {
         if (other.purgeOnNoConsumers != null)
            return false;
      } else if (!purgeOnNoConsumers.equals(other.purgeOnNoConsumers))
         return false;
      if (exclusive == null) {
         if (other.exclusive != null)
            return false;
      } else if (!exclusive.equals(other.exclusive))
         return false;
      if (groupRebalance == null) {
         if (other.groupRebalance != null)
            return false;
      } else if (!groupRebalance.equals(other.groupRebalance))
         return false;
      if (groupRebalancePauseDispatch == null) {
         if (other.groupRebalancePauseDispatch != null)
            return false;
      } else if (!groupRebalancePauseDispatch.equals(other.groupRebalancePauseDispatch))
         return false;
      if (groupBuckets == null) {
         if (other.groupBuckets != null)
            return false;
      } else if (!groupBuckets.equals(other.groupBuckets))
         return false;
      if (groupFirstKey == null) {
         if (other.groupFirstKey != null)
            return false;
      } else if (!groupFirstKey.equals(other.groupFirstKey))
         return false;
      if (lastValue == null) {
         if (other.lastValue != null)
            return false;
      } else if (!lastValue.equals(other.lastValue))
         return false;
      if (lastValueKey == null) {
         if (other.lastValueKey != null)
            return false;
      } else if (!lastValueKey.equals(other.lastValueKey))
         return false;
      if (nonDestructive == null) {
         if (other.nonDestructive != null)
            return false;
      } else if (!nonDestructive.equals(other.nonDestructive))
         return false;
      if (consumersBeforeDispatch == null) {
         if (other.consumersBeforeDispatch != null)
            return false;
      } else if (!consumersBeforeDispatch.equals(other.consumersBeforeDispatch))
         return false;
      if (delayBeforeDispatch == null) {
         if (other.delayBeforeDispatch != null)
            return false;
      } else if (!delayBeforeDispatch.equals(other.delayBeforeDispatch))
         return false;
      if (autoDelete == null) {
         if (other.autoDelete != null)
            return false;
      } else if (!autoDelete.equals(other.autoDelete))
         return false;
      if (autoDeleteDelay == null) {
         if (other.autoDeleteDelay != null)
            return false;
      } else if (!autoDeleteDelay.equals(other.autoDeleteDelay))
         return false;
      if (autoDeleteMessageCount == null) {
         if (other.autoDeleteMessageCount != null)
            return false;
      } else if (!autoDeleteMessageCount.equals(other.autoDeleteMessageCount))
         return false;
      if (ringSize == null) {
         if (other.ringSize != null)
            return false;
      } else if (!ringSize.equals(other.ringSize))
         return false;
      if (enabled == null) {
         if (other.enabled != null)
            return false;
      } else if (!enabled.equals(other.enabled))
         return false;
      return true;
   }
}
