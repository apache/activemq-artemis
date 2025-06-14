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

import java.util.Objects;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.client.impl.QueueQueryImpl;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.utils.BufferHelper;

public class SessionQueueQueryResponseMessage_V3 extends SessionQueueQueryResponseMessage_V2 {

   protected boolean autoCreated;

   protected boolean purgeOnNoConsumers;

   protected RoutingType routingType;

   protected int maxConsumers;

   protected Boolean exclusive;

   protected Boolean groupRebalance;

   protected Boolean groupRebalancePauseDispatch;

   protected Integer groupBuckets;

   protected SimpleString groupFirstKey;

   protected Boolean lastValue;

   protected SimpleString lastValueKey;

   protected Boolean nonDestructive;

   private Integer consumersBeforeDispatch;

   private Long delayBeforeDispatch;

   private Boolean autoDelete;

   private Long autoDeleteDelay;

   private Long autoDeleteMessageCount;

   protected Integer defaultConsumerWindowSize;

   private Long ringSize;

   private Boolean enabled;

   private Boolean configurationManaged;

   public SessionQueueQueryResponseMessage_V3(final QueueQueryResult result) {
      this(result.getName(), result.getAddress(), result.isDurable(), result.isTemporary(), result.getFilterString(), result.getConsumerCount(), result.getMessageCount(), result.isExists(), result.isAutoCreateQueues(), result.isAutoCreated(), result.isPurgeOnNoConsumers(), result.getRoutingType(), result.getMaxConsumers(), result.isExclusive(), result.isGroupRebalance(), result.isGroupRebalancePauseDispatch(), result.getGroupBuckets(), result.getGroupFirstKey(), result.isLastValue(), result.getLastValueKey(), result.isNonDestructive(), result.getConsumersBeforeDispatch(), result.getDelayBeforeDispatch(), result.isAutoDelete(), result.getAutoDeleteDelay(), result.getAutoDeleteMessageCount(), result.getDefaultConsumerWindowSize(), result.getRingSize(), result.isEnabled(), result.isConfigurationManaged());
   }

   public SessionQueueQueryResponseMessage_V3() {
      this(null, null, false, false, null, 0, 0, false, false, false, false, RoutingType.MULTICAST, -1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
   }

   private SessionQueueQueryResponseMessage_V3(final SimpleString name,
                                               final SimpleString address,
                                               final boolean durable,
                                               final boolean temporary,
                                               final SimpleString filterString,
                                               final int consumerCount,
                                               final long messageCount,
                                               final boolean exists,
                                               final boolean autoCreateQueues,
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
                                               final Boolean enabled,
                                               final Boolean configurationManaged) {
      super(SESS_QUEUEQUERY_RESP_V3);

      this.durable = durable;

      this.temporary = temporary;

      this.consumerCount = consumerCount;

      this.messageCount = messageCount;

      this.filterString = filterString;

      this.address = address;

      this.name = name;

      this.exists = exists;

      this.autoCreateQueues = autoCreateQueues;

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

      this.configurationManaged = configurationManaged;
   }

   public boolean isAutoCreated() {
      return autoCreated;
   }

   public void setAutoCreated(boolean autoCreated) {
      this.autoCreated = autoCreated;
   }

   public boolean isPurgeOnNoConsumers() {
      return purgeOnNoConsumers;
   }

   public void setPurgeOnNoConsumers(boolean purgeOnNoConsumers) {
      this.purgeOnNoConsumers = purgeOnNoConsumers;
   }

   public RoutingType getRoutingType() {
      return routingType;
   }

   public void setRoutingType(RoutingType routingType) {
      this.routingType = routingType;
   }

   public int getMaxConsumers() {
      return maxConsumers;
   }

   public void setMaxConsumers(int maxConsumers) {
      this.maxConsumers = maxConsumers;
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

   public Integer getDefaultConsumerWindowSize() {
      return defaultConsumerWindowSize;
   }

   public void setDefaultConsumerWindowSize(Integer defaultConsumerWindowSize) {
      this.defaultConsumerWindowSize = defaultConsumerWindowSize;
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

   public void setGroupFirstKey(SimpleString groupFirstKey) {
      this.groupFirstKey = groupFirstKey;
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

   public void setRingSize(Long ringSize) {
      this.ringSize = ringSize;
   }

   public Boolean isEnabled() {
      return enabled;
   }

   public void setEnabled(Boolean enabled) {
      this.enabled = enabled;
   }

   public Boolean isConfigurationManaged() {
      return configurationManaged;
   }

   public void setConfigurationManaged(Boolean configurationManaged) {
      this.configurationManaged = configurationManaged;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      super.encodeRest(buffer);
      buffer.writeBoolean(autoCreated);
      buffer.writeBoolean(purgeOnNoConsumers);
      buffer.writeByte(routingType.getType());
      buffer.writeInt(maxConsumers);
      BufferHelper.writeNullableBoolean(buffer, exclusive);
      BufferHelper.writeNullableBoolean(buffer, lastValue);
      BufferHelper.writeNullableInteger(buffer, defaultConsumerWindowSize);
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
      BufferHelper.writeNullableBoolean(buffer, configurationManaged);

   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      super.decodeRest(buffer);
      autoCreated = buffer.readBoolean();
      purgeOnNoConsumers = buffer.readBoolean();
      routingType = RoutingType.getType(buffer.readByte());
      maxConsumers = buffer.readInt();
      if (buffer.readableBytes() > 0) {
         exclusive = BufferHelper.readNullableBoolean(buffer);
         lastValue = BufferHelper.readNullableBoolean(buffer);
      }
      if (buffer.readableBytes() > 0) {
         defaultConsumerWindowSize = BufferHelper.readNullableInteger(buffer);
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
         ringSize = BufferHelper.readNullableLong(buffer);
      }
      if (buffer.readableBytes() > 0) {
         enabled = BufferHelper.readNullableBoolean(buffer);
      }
      if (buffer.readableBytes() > 0) {
         groupRebalancePauseDispatch = BufferHelper.readNullableBoolean(buffer);
      }
      if (buffer.readableBytes() > 0) {
         configurationManaged = BufferHelper.readNullableBoolean(buffer);
      }
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), autoCreated, purgeOnNoConsumers, routingType, maxConsumers, exclusive,
                          groupRebalance, groupRebalancePauseDispatch, groupBuckets, groupFirstKey, lastValue,
                          lastValueKey, nonDestructive, consumersBeforeDispatch, delayBeforeDispatch, autoDelete,
                          autoDeleteDelay, autoDeleteMessageCount, defaultConsumerWindowSize, ringSize, enabled,
                          configurationManaged);
   }

   @Override
   protected String getPacketString() {
      StringBuilder sb = new StringBuilder(super.getPacketString());
      sb.append(", autoCreated=" + autoCreated);
      sb.append(", purgeOnNoConsumers=" + purgeOnNoConsumers);
      sb.append(", routingType=" + routingType);
      sb.append(", maxConsumers=" + maxConsumers);
      sb.append(", exclusive=" + exclusive);
      sb.append(", groupRebalance=" + groupRebalance);
      sb.append(", groupRebalancePauseDispatch=" + groupRebalancePauseDispatch);
      sb.append(", groupBuckets=" + groupBuckets);
      sb.append(", groupFirstKey=" + groupFirstKey);
      sb.append(", lastValue=" + lastValue);
      sb.append(", lastValueKey=" + lastValueKey);
      sb.append(", nonDestructive=" + nonDestructive);
      sb.append(", consumersBeforeDispatch=" + consumersBeforeDispatch);
      sb.append(", delayBeforeDispatch=" + delayBeforeDispatch);
      sb.append(", autoDelete=" + autoDelete);
      sb.append(", autoDeleteDelay=" + autoDeleteDelay);
      sb.append(", autoDeleteMessageCount=" + autoDeleteMessageCount);
      sb.append(", defaultConsumerWindowSize=" + defaultConsumerWindowSize);
      sb.append(", ringSize=" + ringSize);
      sb.append(", enabled=" + enabled);
      sb.append(", configurationManaged=" + configurationManaged);
      return sb.toString();
   }

   @Override
   public ClientSession.QueueQuery toQueueQuery() {
      return new QueueQueryImpl(isDurable(), isTemporary(), getConsumerCount(), getMessageCount(), getFilterString(), getAddress(), getName(), isExists(), isAutoCreateQueues(), getMaxConsumers(), isAutoCreated(), isPurgeOnNoConsumers(), getRoutingType(), isExclusive(), isGroupRebalance(), isGroupRebalancePauseDispatch(), getGroupBuckets(), getGroupFirstKey(), isLastValue(), getLastValueKey(), isNonDestructive(), getConsumersBeforeDispatch(), getDelayBeforeDispatch(), isAutoDelete(), getAutoDeleteDelay(), getAutoDeleteMessageCount(), getDefaultConsumerWindowSize(), getRingSize(), isEnabled(), isConfigurationManaged());
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionQueueQueryResponseMessage_V3 other))
         return false;
      if (autoCreated != other.autoCreated)
         return false;
      if (purgeOnNoConsumers != other.purgeOnNoConsumers)
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
      if (defaultConsumerWindowSize == null) {
         if (other.defaultConsumerWindowSize != null)
            return false;
      } else if (!defaultConsumerWindowSize.equals(other.defaultConsumerWindowSize))
         return false;
      if (routingType == null) {
         if (other.routingType != null)
            return false;
      } else if (!routingType.equals(other.routingType))
         return false;
      if (maxConsumers != other.maxConsumers)
         return false;
      if (configurationManaged == null) {
         if (other.configurationManaged != null)
            return false;
      } else if (!configurationManaged.equals(other.configurationManaged))
         return false;
      return true;
   }
}
