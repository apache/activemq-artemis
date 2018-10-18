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
import org.apache.activemq.artemis.api.core.QueueAttributes;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.utils.BufferHelper;

public class CreateQueueMessage_V2 extends CreateQueueMessage {

   protected boolean autoCreated;

   private RoutingType routingType;

   private int maxConsumers;

   private boolean purgeOnNoConsumers;

   private Boolean exclusive;

   private Boolean lastValue;

   private SimpleString lastValueKey;

   private Boolean nonDestructive;

   private Integer consumersBeforeDispatch;

   private Long delayBeforeDispatch;

   public CreateQueueMessage_V2(final SimpleString address,
                                final SimpleString queueName,
                                final boolean temporary,
                                final boolean autoCreated,
                                final boolean requiresResponse,
                                final QueueAttributes queueAttributes) {
      this(
         address,
         queueName,
         queueAttributes.getRoutingType(),
         queueAttributes.getFilterString(),
         queueAttributes.getDurable(),
         temporary,
         queueAttributes.getMaxConsumers(),
         queueAttributes.getPurgeOnNoConsumers(),
         autoCreated,
         requiresResponse,
         queueAttributes.getExclusive(),
         queueAttributes.getLastValue(),
         queueAttributes.getLastValueKey(),
         queueAttributes.getNonDestructive(),
         queueAttributes.getConsumersBeforeDispatch(),
         queueAttributes.getDelayBeforeDispatch()
      );
   }

   public CreateQueueMessage_V2(final SimpleString address,
                                final SimpleString queueName,
                                final RoutingType routingType,
                                final SimpleString filterString,
                                final boolean durable,
                                final boolean temporary,
                                final int maxConsumers,
                                final boolean purgeOnNoConsumers,
                                final boolean autoCreated,
                                final boolean requiresResponse,
                                final Boolean exclusive,
                                final Boolean lastValue,
                                final SimpleString lastValueKey,
                                final Boolean nonDestructive,
                                final Integer consumersBeforeDispatch,
                                final Long delayBeforeDispatch) {
      this();

      this.address = address;
      this.queueName = queueName;
      this.filterString = filterString;
      this.durable = durable;
      this.temporary = temporary;
      this.autoCreated = autoCreated;
      this.requiresResponse = requiresResponse;
      this.routingType = routingType;
      this.maxConsumers = maxConsumers;
      this.purgeOnNoConsumers = purgeOnNoConsumers;
      this.exclusive = exclusive;
      this.lastValue = lastValue;
      this.lastValueKey = lastValueKey;
      this.nonDestructive = nonDestructive;
      this.consumersBeforeDispatch = consumersBeforeDispatch;
      this.delayBeforeDispatch = delayBeforeDispatch;
   }

   public CreateQueueMessage_V2() {
      super(CREATE_QUEUE_V2);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(super.getParentString());
      buff.append(", autoCreated=" + autoCreated);
      buff.append(", routingType=" + routingType);
      buff.append(", maxConsumers=" + maxConsumers);
      buff.append(", purgeOnNoConsumers=" + purgeOnNoConsumers);
      buff.append(", exclusive=" + exclusive);
      buff.append(", lastValue=" + lastValue);
      buff.append(", lastValueKey=" + lastValue);
      buff.append(", nonDestructive=" + nonDestructive);
      buff.append(", consumersBeforeDispatch=" + consumersBeforeDispatch);
      buff.append(", delayBeforeDispatch=" + delayBeforeDispatch);
      buff.append("]");
      return buff.toString();
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

   public boolean isPurgeOnNoConsumers() {
      return purgeOnNoConsumers;
   }

   public void setPurgeOnNoConsumers(boolean purgeOnNoConsumers) {
      this.purgeOnNoConsumers = purgeOnNoConsumers;
   }

   public boolean isAutoCreated() {
      return autoCreated;
   }

   public void setAutoCreated(boolean autoCreated) {
      this.autoCreated = autoCreated;
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
   public void encodeRest(final ActiveMQBuffer buffer) {
      super.encodeRest(buffer);
      buffer.writeBoolean(autoCreated);
      buffer.writeByte(routingType == null ? -1 : routingType.getType());
      buffer.writeInt(maxConsumers);
      buffer.writeBoolean(purgeOnNoConsumers);
      BufferHelper.writeNullableBoolean(buffer, exclusive);
      BufferHelper.writeNullableBoolean(buffer, lastValue);
      buffer.writeNullableSimpleString(lastValueKey);
      BufferHelper.writeNullableBoolean(buffer, nonDestructive);
      BufferHelper.writeNullableInteger(buffer, consumersBeforeDispatch);
      BufferHelper.writeNullableLong(buffer, delayBeforeDispatch);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      super.decodeRest(buffer);
      autoCreated = buffer.readBoolean();
      routingType = RoutingType.getType(buffer.readByte());
      maxConsumers = buffer.readInt();
      purgeOnNoConsumers = buffer.readBoolean();
      if (buffer.readableBytes() > 0) {
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
      result = prime * result + (autoCreated ? 1231 : 1237);
      result = prime * result + (routingType.getType());
      result = prime * result + (maxConsumers);
      result = prime * result + (purgeOnNoConsumers ? 1231 : 1237);
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
      if (!(obj instanceof CreateQueueMessage_V2))
         return false;
      CreateQueueMessage_V2 other = (CreateQueueMessage_V2) obj;
      if (autoCreated != other.autoCreated)
         return false;
      if (maxConsumers != other.maxConsumers)
         return false;
      if (purgeOnNoConsumers != other.purgeOnNoConsumers)
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
      if (routingType == null) {
         if (other.routingType != null)
            return false;
      } else if (!routingType.equals(other.routingType))
         return false;
      return true;
   }
}
