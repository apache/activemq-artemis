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

import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.utils.BufferHelper;

public class SessionBindingQueryResponseMessage_V4 extends SessionBindingQueryResponseMessage_V3 {

   private boolean defaultPurgeOnNoConsumers;

   private int defaultMaxConsumers;

   private Boolean defaultExclusive;

   private Boolean defaultLastValue;

   private SimpleString defaultLastValueKey;

   private Boolean defaultNonDestructive;

   private Integer defaultConsumersBeforeDispatch;

   private Long defaultDelayBeforeDispatch;

   public SessionBindingQueryResponseMessage_V4(final boolean exists,
                                                final List<SimpleString> queueNames,
                                                final boolean autoCreateQueues,
                                                final boolean autoCreateAddresses,
                                                final boolean defaultPurgeOnNoConsumers,
                                                final int defaultMaxConsumers,
                                                final Boolean defaultExclusive,
                                                final Boolean defaultLastValue,
                                                final SimpleString defaultLastValueKey,
                                                final Boolean defaultNonDestructive,
                                                final Integer defaultConsumersBeforeDispatch,
                                                final Long defaultDelayBeforeDispatch) {
      super(SESS_BINDINGQUERY_RESP_V4);

      this.exists = exists;

      this.queueNames = queueNames;

      this.autoCreateQueues = autoCreateQueues;

      this.autoCreateAddresses = autoCreateAddresses;

      this.defaultPurgeOnNoConsumers = defaultPurgeOnNoConsumers;

      this.defaultMaxConsumers = defaultMaxConsumers;

      this.defaultExclusive = defaultExclusive;

      this.defaultLastValue = defaultLastValue;

      this.defaultLastValueKey = defaultLastValueKey;

      this.defaultNonDestructive = defaultNonDestructive;

      this.defaultConsumersBeforeDispatch = defaultConsumersBeforeDispatch;

      this.defaultDelayBeforeDispatch = defaultDelayBeforeDispatch;
   }

   public SessionBindingQueryResponseMessage_V4() {
      super(SESS_BINDINGQUERY_RESP_V4);
   }

   public boolean isDefaultPurgeOnNoConsumers() {
      return defaultPurgeOnNoConsumers;
   }

   public int getDefaultMaxConsumers() {
      return defaultMaxConsumers;
   }

   public Boolean isDefaultExclusive() {
      return defaultExclusive;
   }

   public Boolean isDefaultLastValue() {
      return defaultLastValue;
   }

   public SimpleString getDefaultLastValueKey() {
      return defaultLastValueKey;
   }

   public Boolean isDefaultNonDestructive() {
      return defaultNonDestructive;
   }

   public Integer getDefaultConsumersBeforeDispatch() {
      return defaultConsumersBeforeDispatch;
   }

   public Long getDefaultDelayBeforeDispatch() {
      return defaultDelayBeforeDispatch;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      super.encodeRest(buffer);
      buffer.writeBoolean(defaultPurgeOnNoConsumers);
      buffer.writeInt(defaultMaxConsumers);
      BufferHelper.writeNullableBoolean(buffer, defaultExclusive);
      BufferHelper.writeNullableBoolean(buffer, defaultLastValue);
      buffer.writeNullableSimpleString(defaultLastValueKey);
      BufferHelper.writeNullableBoolean(buffer, defaultNonDestructive);
      BufferHelper.writeNullableInteger(buffer, defaultConsumersBeforeDispatch);
      BufferHelper.writeNullableLong(buffer, defaultDelayBeforeDispatch);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      super.decodeRest(buffer);
      defaultPurgeOnNoConsumers = buffer.readBoolean();
      defaultMaxConsumers = buffer.readInt();
      if (buffer.readableBytes() > 0) {
         defaultExclusive = BufferHelper.readNullableBoolean(buffer);
         defaultLastValue = BufferHelper.readNullableBoolean(buffer);
      }
      if (buffer.readableBytes() > 0) {
         defaultLastValueKey = buffer.readNullableSimpleString();
         defaultNonDestructive = BufferHelper.readNullableBoolean(buffer);
         defaultConsumersBeforeDispatch = BufferHelper.readNullableInteger(buffer);
         defaultDelayBeforeDispatch = BufferHelper.readNullableLong(buffer);
      }
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (defaultPurgeOnNoConsumers ? 1231 : 1237);
      result = prime * result + defaultMaxConsumers;
      result = prime * result + (defaultExclusive == null ? 0 : defaultExclusive ? 1231 : 1237);
      result = prime * result + (defaultLastValue == null ? 0 : defaultLastValue ? 1231 : 1237);
      result = prime * result + (defaultLastValueKey == null ? 0 : defaultLastValueKey.hashCode());
      result = prime * result + (defaultNonDestructive == null ? 0 : defaultNonDestructive ? 1231 : 1237);
      result = prime * result + (defaultConsumersBeforeDispatch == null ? 0 : defaultConsumersBeforeDispatch.hashCode());
      result = prime * result + (defaultDelayBeforeDispatch == null ? 0 : defaultDelayBeforeDispatch.hashCode());
      return result;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append("]");
      return buff.toString();
   }

   @Override
   public String getParentString() {
      StringBuffer buff = new StringBuffer(super.getParentString());
      buff.append(", defaultPurgeOnNoConsumers=" + defaultPurgeOnNoConsumers);
      buff.append(", defaultMaxConsumers=" + defaultMaxConsumers);
      buff.append(", defaultExclusive=" + defaultExclusive);
      buff.append(", defaultLastValue=" + defaultLastValue);
      buff.append(", defaultLastValueKey=" + defaultLastValueKey);
      buff.append(", defaultNonDestructive=" + defaultNonDestructive);
      buff.append(", defaultConsumersBeforeDispatch=" + defaultConsumersBeforeDispatch);
      buff.append(", defaultDelayBeforeDispatch=" + defaultDelayBeforeDispatch);
      return buff.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionBindingQueryResponseMessage_V4))
         return false;
      SessionBindingQueryResponseMessage_V4 other = (SessionBindingQueryResponseMessage_V4) obj;
      if (defaultPurgeOnNoConsumers != other.defaultPurgeOnNoConsumers)
         return false;
      if (defaultMaxConsumers != other.defaultMaxConsumers)
         return false;
      if (defaultExclusive == null) {
         if (other.defaultExclusive != null)
            return false;
      } else if (!defaultExclusive.equals(other.defaultExclusive))
         return false;
      if (defaultLastValue == null) {
         if (other.defaultLastValue != null)
            return false;
      } else if (!defaultLastValue.equals(other.defaultLastValue))
         return false;
      if (defaultLastValueKey == null) {
         if (other.defaultLastValueKey != null)
            return false;
      } else if (!defaultLastValueKey.equals(other.defaultLastValueKey))
         return false;
      if (defaultNonDestructive == null) {
         if (other.defaultNonDestructive != null)
            return false;
      } else if (!defaultNonDestructive.equals(other.defaultNonDestructive))
         return false;
      if (defaultConsumersBeforeDispatch == null) {
         if (other.defaultConsumersBeforeDispatch != null)
            return false;
      } else if (!defaultConsumersBeforeDispatch.equals(other.defaultConsumersBeforeDispatch))
         return false;
      if (defaultDelayBeforeDispatch == null) {
         if (other.defaultDelayBeforeDispatch != null)
            return false;
      } else if (!defaultDelayBeforeDispatch.equals(other.defaultDelayBeforeDispatch))
         return false;
      return true;
   }
}
