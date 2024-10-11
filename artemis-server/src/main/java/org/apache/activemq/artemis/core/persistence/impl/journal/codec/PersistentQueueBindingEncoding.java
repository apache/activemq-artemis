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
package org.apache.activemq.artemis.core.persistence.impl.journal.codec;

import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.utils.DataConstants;

public class PersistentQueueBindingEncoding implements EncodingSupport, QueueBindingInfo {

   private long id;

   private SimpleString name;

   private SimpleString address;

   private SimpleString filterString;

   private boolean autoCreated;

   private SimpleString user;

   private List<QueueStatusEncoding> queueStatusEncodings;

   private int maxConsumers;

   private boolean purgeOnNoConsumers;

   private boolean enabled;

   private boolean exclusive;

   private boolean lastValue;

   private SimpleString lastValueKey;

   private boolean nonDestructive;

   private int consumersBeforeDispatch;

   private long delayBeforeDispatch;

   private byte routingType;

   private boolean configurationManaged;

   private boolean groupRebalance;

   private boolean groupRebalancePauseDispatch;

   private int groupBuckets;

   private SimpleString groupFirstKey;

   private boolean autoDelete;

   private long autoDeleteDelay;

   private long autoDeleteMessageCount;

   private long ringSize;

   private boolean internal;

   public PersistentQueueBindingEncoding() {
   }

   @Override
   public String toString() {
      return "PersistentQueueBindingEncoding [id=" + id +
         ", name=" + name +
         ", address=" + address +
         ", filterString=" + filterString +
         ", user=" + user +
         ", autoCreated=" + autoCreated +
         ", maxConsumers=" + maxConsumers +
         ", purgeOnNoConsumers=" + purgeOnNoConsumers +
         ", enabled=" + enabled +
         ", exclusive=" + exclusive +
         ", lastValue=" + lastValue +
         ", lastValueKey=" + lastValueKey +
         ", nonDestructive=" + nonDestructive +
         ", consumersBeforeDispatch=" + consumersBeforeDispatch +
         ", delayBeforeDispatch=" + delayBeforeDispatch +
         ", routingType=" + routingType +
         ", configurationManaged=" + configurationManaged +
         ", groupRebalance=" + groupRebalance +
         ", groupRebalancePauseDispatch=" + groupRebalancePauseDispatch +
         ", groupBuckets=" + groupBuckets +
         ", groupFirstKey=" + groupFirstKey +
         ", autoDelete=" + autoDelete +
         ", autoDeleteDelay=" + autoDeleteDelay +
         ", autoDeleteMessageCount=" + autoDeleteMessageCount +
         ", internal=" + internal +
         "]";
   }

   public PersistentQueueBindingEncoding(final SimpleString name,
                                         final SimpleString address,
                                         final SimpleString filterString,
                                         final SimpleString user,
                                         final boolean autoCreated,
                                         final int maxConsumers,
                                         final boolean purgeOnNoConsumers,
                                         final boolean enabled,
                                         final boolean exclusive,
                                         final boolean groupRebalance,
                                         final boolean groupRebalancePauseDispatch,
                                         final int groupBuckets,
                                         final SimpleString groupFirstKey,
                                         final boolean lastValue,
                                         final SimpleString lastValueKey,
                                         final boolean nonDestructive,
                                         final int consumersBeforeDispatch,
                                         final long delayBeforeDispatch,
                                         final boolean autoDelete,
                                         final long autoDeleteDelay,
                                         final long autoDeleteMessageCount,
                                         final byte routingType,
                                         final boolean configurationManaged,
                                         final long ringSize,
                                         final boolean internal) {
      this.name = name;
      this.address = address;
      this.filterString = filterString;
      this.user = user;
      this.autoCreated = autoCreated;
      this.maxConsumers = maxConsumers;
      this.purgeOnNoConsumers = purgeOnNoConsumers;
      this.groupRebalancePauseDispatch = groupRebalancePauseDispatch;
      this.enabled = enabled;
      this.exclusive = exclusive;
      this.groupRebalance = groupRebalance;
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
      this.routingType = routingType;
      this.configurationManaged = configurationManaged;
      this.ringSize = ringSize;
      this.internal = internal;
   }

   @Override
   public long getId() {
      return id;
   }

   public void setId(final long id) {
      this.id = id;
   }

   @Override
   public SimpleString getAddress() {
      return address;
   }

   @Override
   public void replaceQueueName(SimpleString newName) {
      this.name = newName;
   }

   public void replaceAddress(SimpleString address) {
      this.address = address;
   }

   @Override
   public SimpleString getFilterString() {
      return filterString;
   }

   @Override
   public SimpleString getQueueName() {
      return name;
   }

   @Override
   public SimpleString getUser() {
      return user;
   }

   @Override
   public boolean isAutoCreated() {
      return autoCreated;
   }

   @Override
   public boolean isConfigurationManaged() {
      return configurationManaged;
   }

   @Override
   public void setConfigurationManaged(boolean configurationManaged) {
      this.configurationManaged = configurationManaged;
   }

   @Override
   public void addQueueStatusEncoding(QueueStatusEncoding status) {
      if (queueStatusEncodings == null) {
         queueStatusEncodings = new LinkedList<>();
      }
      queueStatusEncodings.add(status);
   }

   @Override
   public List<QueueStatusEncoding> getQueueStatusEncodings() {
      return queueStatusEncodings;
   }

   @Override
   public int getMaxConsumers() {
      return maxConsumers;
   }

   @Override
   public void setMaxConsumers(int maxConsumers) {
      this.maxConsumers = maxConsumers;
   }

   @Override
   public boolean isPurgeOnNoConsumers() {
      return purgeOnNoConsumers;
   }

   @Override
   public void setPurgeOnNoConsumers(boolean purgeOnNoConsumers) {
      this.purgeOnNoConsumers = purgeOnNoConsumers;
   }

   @Override
   public boolean isEnabled() {
      return enabled;
   }

   @Override
   public void setEnabled(boolean enabled) {
      this.enabled = enabled;
   }

   @Override
   public boolean isExclusive() {
      return exclusive;
   }

   @Override
   public void setExclusive(boolean exclusive) {
      this.exclusive = exclusive;
   }

   @Override
   public boolean isLastValue() {
      return lastValue;
   }

   @Override
   public void setLastValue(boolean lastValue) {
      this.lastValue = lastValue;
   }

   @Override
   public SimpleString getLastValueKey() {
      return lastValueKey;
   }

   @Override
   public void setLastValueKey(SimpleString lastValueKey) {
      this.lastValueKey = lastValueKey;
   }

   @Override
   public boolean isNonDestructive() {
      return nonDestructive;
   }

   @Override
   public void setNonDestructive(boolean nonDestructive) {
      this.nonDestructive = nonDestructive;
   }

   @Override
   public int getConsumersBeforeDispatch() {
      return consumersBeforeDispatch;
   }

   @Override
   public void setConsumersBeforeDispatch(int consumersBeforeDispatch) {
      this.consumersBeforeDispatch = consumersBeforeDispatch;
   }

   @Override
   public long getDelayBeforeDispatch() {
      return delayBeforeDispatch;
   }

   @Override
   public void setDelayBeforeDispatch(long delayBeforeDispatch) {
      this.delayBeforeDispatch = delayBeforeDispatch;
   }

   @Override
   public byte getRoutingType() {
      return routingType;
   }

   @Override
   public void setRoutingType(byte routingType) {
      this.routingType = routingType;
   }

   @Override
   public boolean isGroupRebalance() {
      return groupRebalance;
   }

   @Override
   public boolean isGroupRebalancePauseDispatch() {
      return groupRebalancePauseDispatch;
   }

   @Override
   public int getGroupBuckets() {
      return groupBuckets;
   }

   @Override
   public SimpleString getGroupFirstKey() {
      return groupFirstKey;
   }

   @Override
   public boolean isAutoDelete() {
      return autoDelete;
   }

   @Override
   public long getAutoDeleteDelay() {
      return autoDeleteDelay;
   }

   @Override
   public long getAutoDeleteMessageCount() {
      return autoDeleteMessageCount;
   }

   @Override
   public long getRingSize() {
      return ringSize;
   }

   @Override
   public boolean isInternal() {
      return internal;
   }

   @Override
   public void decode(final ActiveMQBuffer buffer) {
      name = buffer.readSimpleString();
      address = buffer.readSimpleString();
      filterString = buffer.readNullableSimpleString();

      String metadata = buffer.readNullableSimpleString().toString();
      if (metadata != null) {
         String[] elements = metadata.split(";");
         for (String element : elements) {
            String[] keyValuePair = element.split("=");
            if (keyValuePair.length == 2) {
               if (keyValuePair[0].equals("user")) {
                  user = SimpleString.of(keyValuePair[1]);
               }
            }
         }
      }

      autoCreated = buffer.readBoolean();

      if (buffer.readableBytes() > 0) {
         maxConsumers = buffer.readInt();
         purgeOnNoConsumers = buffer.readBoolean();
         routingType = buffer.readByte();
      } else {
         maxConsumers = ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers();
         purgeOnNoConsumers = ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers();
         routingType = ActiveMQDefaultConfiguration.getDefaultRoutingType().getType();
      }

      if (buffer.readableBytes() > 0) {
         exclusive = buffer.readBoolean();
      } else {
         exclusive = ActiveMQDefaultConfiguration.getDefaultExclusive();
      }
      if (buffer.readableBytes() > 0) {
         lastValue = buffer.readBoolean();
      } else {
         lastValue = ActiveMQDefaultConfiguration.getDefaultLastValue();
      }
      if (buffer.readableBytes() > 0) {
         configurationManaged = buffer.readBoolean();
      } else {
         configurationManaged = false;
      }
      if (buffer.readableBytes() > 0) {
         consumersBeforeDispatch = buffer.readInt();
      } else {
         consumersBeforeDispatch = ActiveMQDefaultConfiguration.getDefaultConsumersBeforeDispatch();
      }
      if (buffer.readableBytes() > 0) {
         delayBeforeDispatch = buffer.readLong();
      } else {
         delayBeforeDispatch = ActiveMQDefaultConfiguration.getDefaultDelayBeforeDispatch();
      }
      if (buffer.readableBytes() > 0) {
         lastValueKey = buffer.readNullableSimpleString();
      } else {
         lastValueKey = ActiveMQDefaultConfiguration.getDefaultLastValueKey();
      }
      if (buffer.readableBytes() > 0) {
         nonDestructive = buffer.readBoolean();
      } else {
         nonDestructive = ActiveMQDefaultConfiguration.getDefaultNonDestructive();
      }
      if (buffer.readableBytes() > 0) {
         groupRebalance = buffer.readBoolean();
      } else {
         groupRebalance = ActiveMQDefaultConfiguration.getDefaultGroupRebalance();
      }
      if (buffer.readableBytes() > 0) {
         groupBuckets = buffer.readInt();
      } else {
         groupBuckets = ActiveMQDefaultConfiguration.getDefaultGroupBuckets();
      }
      if (buffer.readableBytes() > 0) {
         autoDelete = buffer.readBoolean();
      } else {
         autoDelete = ActiveMQDefaultConfiguration.getDefaultQueueAutoDelete(autoCreated);
      }
      if (buffer.readableBytes() > 0) {
         autoDeleteDelay = buffer.readLong();
      } else {
         autoDeleteDelay = ActiveMQDefaultConfiguration.getDefaultQueueAutoDeleteDelay();
      }
      if (buffer.readableBytes() > 0) {
         autoDeleteMessageCount = buffer.readLong();
      } else {
         autoDeleteMessageCount = ActiveMQDefaultConfiguration.getDefaultQueueAutoDeleteMessageCount();
      }
      if (buffer.readableBytes() > 0) {
         groupFirstKey = buffer.readNullableSimpleString();
      } else {
         groupFirstKey = ActiveMQDefaultConfiguration.getDefaultGroupFirstKey();
      }
      if (buffer.readableBytes() > 0) {
         ringSize = buffer.readLong();
      } else {
         ringSize = ActiveMQDefaultConfiguration.getDefaultRingSize();
      }
      if (buffer.readableBytes() > 0) {
         enabled = buffer.readBoolean();
      } else {
         enabled = ActiveMQDefaultConfiguration.getDefaultEnabled();
      }

      if (buffer.readableBytes() > 0) {
         groupRebalancePauseDispatch = buffer.readBoolean();
      } else {
         groupRebalancePauseDispatch = ActiveMQDefaultConfiguration.getDefaultGroupRebalancePauseDispatch();
      }

      if (buffer.readableBytes() > 0) {
         internal = buffer.readBoolean();
      } else {
         internal = ActiveMQDefaultConfiguration.getDefaultInternal();
      }
   }

   @Override
   public void encode(final ActiveMQBuffer buffer) {
      buffer.writeSimpleString(name);
      buffer.writeSimpleString(address);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeNullableSimpleString(createMetadata());
      buffer.writeBoolean(autoCreated);
      buffer.writeInt(maxConsumers);
      buffer.writeBoolean(purgeOnNoConsumers);
      buffer.writeByte(routingType);
      buffer.writeBoolean(exclusive);
      buffer.writeBoolean(lastValue);
      buffer.writeBoolean(configurationManaged);
      buffer.writeInt(consumersBeforeDispatch);
      buffer.writeLong(delayBeforeDispatch);
      buffer.writeNullableSimpleString(lastValueKey);
      buffer.writeBoolean(nonDestructive);
      buffer.writeBoolean(groupRebalance);
      buffer.writeInt(groupBuckets);
      buffer.writeBoolean(autoDelete);
      buffer.writeLong(autoDeleteDelay);
      buffer.writeLong(autoDeleteMessageCount);
      buffer.writeNullableSimpleString(groupFirstKey);
      buffer.writeLong(ringSize);
      buffer.writeBoolean(enabled);
      buffer.writeBoolean(groupRebalancePauseDispatch);
      buffer.writeBoolean(internal);
   }

   @Override
   public int getEncodeSize() {
      return SimpleString.sizeofString(name) + SimpleString.sizeofString(address) +
         SimpleString.sizeofNullableString(filterString) + DataConstants.SIZE_BOOLEAN +
         SimpleString.sizeofNullableString(createMetadata()) +
         DataConstants.SIZE_INT +
         DataConstants.SIZE_BOOLEAN +
         DataConstants.SIZE_BYTE +
         DataConstants.SIZE_BOOLEAN +
         DataConstants.SIZE_BOOLEAN +
         DataConstants.SIZE_BOOLEAN +
         DataConstants.SIZE_INT +
         DataConstants.SIZE_LONG +
         SimpleString.sizeofNullableString(lastValueKey) +
         DataConstants.SIZE_BOOLEAN +
         DataConstants.SIZE_BOOLEAN +
         DataConstants.SIZE_INT +
         DataConstants.SIZE_BOOLEAN +
         DataConstants.SIZE_LONG +
         DataConstants.SIZE_LONG +
         SimpleString.sizeofNullableString(groupFirstKey) +
         DataConstants.SIZE_LONG +
         DataConstants.SIZE_BOOLEAN +
         DataConstants.SIZE_BOOLEAN +
         DataConstants.SIZE_BOOLEAN;
   }

   private SimpleString createMetadata() {
      StringBuilder metadata = new StringBuilder();
      if (user != null) {
         metadata.append("user=").append(user).append(";");
      }
      return SimpleString.of(metadata.toString());
   }
}
