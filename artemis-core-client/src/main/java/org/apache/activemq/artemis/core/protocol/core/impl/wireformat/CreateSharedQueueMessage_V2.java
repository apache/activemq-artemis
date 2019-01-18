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
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.utils.BufferHelper;

public class CreateSharedQueueMessage_V2 extends CreateSharedQueueMessage {

   private RoutingType routingType;
   private Integer maxConsumers;
   private Boolean purgeOnNoConsumers;
   private Boolean exclusive;
   private Boolean lastValue;
   private SimpleString lastValueKey;
   private Boolean nonDestructive;
   private Integer consumersBeforeDispatch;
   private Long delayBeforeDispatch;

   public CreateSharedQueueMessage_V2(final SimpleString address,
                                      final SimpleString queueName,
                                      final RoutingType routingType,
                                      final SimpleString filterString,
                                      final boolean durable,
                                      final Integer maxConsumers,
                                      final Boolean purgeOnNoConsumers,
                                      final Boolean exclusive,
                                      final Boolean lastValue,
                                      final SimpleString lastValueKey,
                                      final Boolean nonDestructive,
                                      final Integer consumersBeforeDispatch,
                                      final Long delayBeforeDispatch,
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
      this.lastValue = lastValue;
      this.lastValueKey = lastValueKey;
      this.nonDestructive = nonDestructive;
      this.consumersBeforeDispatch = consumersBeforeDispatch;
      this.delayBeforeDispatch = delayBeforeDispatch;
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

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", address=" + address);
      buff.append(", queueName=" + queueName);
      buff.append(", filterString=" + filterString);
      buff.append(", durable=" + durable);
      buff.append(", routingType=" + routingType);
      buff.append(", maxConsumers=" + maxConsumers);
      buff.append(", purgeOnNoConsumers=" + purgeOnNoConsumers);
      buff.append(", exclusive=" + exclusive);
      buff.append(", lastValue=" + lastValue);
      buff.append(", lastValueKey=" + lastValueKey);
      buff.append(", nonDestructive=" + nonDestructive);
      buff.append(", consumersBeforeDispatch=" + consumersBeforeDispatch);
      buff.append(", delayBeforeDispatch=" + delayBeforeDispatch);
      buff.append(", requiresResponse=" + requiresResponse);
      buff.append("]");
      return buff.toString();
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeSimpleString(address);
      buffer.writeSimpleString(queueName);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeBoolean(durable);
      buffer.writeByte(routingType.getType());
      buffer.writeBoolean(requiresResponse);
      BufferHelper.writeNullableInteger(buffer, maxConsumers);
      BufferHelper.writeNullableBoolean(buffer, purgeOnNoConsumers);
      BufferHelper.writeNullableBoolean(buffer, exclusive);
      BufferHelper.writeNullableBoolean(buffer, lastValue);
      buffer.writeNullableSimpleString(lastValueKey);
      BufferHelper.writeNullableBoolean(buffer, nonDestructive);
      BufferHelper.writeNullableInteger(buffer, consumersBeforeDispatch);
      BufferHelper.writeNullableLong(buffer, delayBeforeDispatch);
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
      result = prime * result + (lastValue == null ? 0 : lastValue ? 1231 : 1237);
      result = prime * result + (lastValueKey == null ? 0 : lastValueKey.hashCode());
      result = prime * result + (nonDestructive == null ? 0 : nonDestructive ? 1231 : 1237);
      result = prime * result + (consumersBeforeDispatch == null ? 0 : consumersBeforeDispatch.hashCode());
      result = prime * result + (delayBeforeDispatch == null ? 0 : delayBeforeDispatch.hashCode());

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
      return true;
   }
}
