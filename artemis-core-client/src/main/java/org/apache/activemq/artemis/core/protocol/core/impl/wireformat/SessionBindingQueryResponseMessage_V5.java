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

public class SessionBindingQueryResponseMessage_V5 extends SessionBindingQueryResponseMessage_V4 {

   protected boolean supportsMulticast;

   protected boolean supportsAnycast;

   public SessionBindingQueryResponseMessage_V5(final boolean exists,
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
                                                final Long defaultDelayBeforeDispatch,
                                                final boolean supportsMulticast,
                                                final boolean supportsAnycast) {
      super(SESS_BINDINGQUERY_RESP_V5);

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

      this.supportsMulticast = supportsMulticast;

      this.supportsAnycast = supportsAnycast;
   }

   public SessionBindingQueryResponseMessage_V5() {
      super(SESS_BINDINGQUERY_RESP_V5);
   }

   public Boolean isSupportsMulticast() {
      return supportsMulticast;
   }

   public Boolean isSupportsAnycast() {
      return supportsAnycast;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      super.encodeRest(buffer);
      buffer.writeBoolean(supportsMulticast);
      buffer.writeBoolean(supportsAnycast);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      super.decodeRest(buffer);
      if (buffer.readableBytes() > 0) {
         supportsMulticast = buffer.readBoolean();
         supportsAnycast = buffer.readBoolean();
      }
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (supportsMulticast ? 1231 : 1237);
      result = prime * result + (supportsAnycast ? 1231 : 1237);
      return result;
   }

   @Override
   protected String getPacketString() {
      StringBuffer buff = new StringBuffer(super.getPacketString());
      buff.append(", supportsMulticast=" + supportsMulticast);
      buff.append(", supportsAnycast=" + supportsAnycast);
      return buff.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (!(obj instanceof SessionBindingQueryResponseMessage_V5)) {
         return false;
      }
      SessionBindingQueryResponseMessage_V5 other = (SessionBindingQueryResponseMessage_V5) obj;
      if (supportsMulticast != other.supportsMulticast) {
         return false;
      }
      if (supportsAnycast != other.supportsAnycast) {
         return false;
      }
      return true;
   }
}
