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
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.client.impl.QueueQueryImpl;
import org.apache.activemq.artemis.core.server.QueueQueryResult;

public class SessionQueueQueryResponseMessage_V2 extends SessionQueueQueryResponseMessage {

   private boolean autoCreationEnabled;

   public SessionQueueQueryResponseMessage_V2(final QueueQueryResult result) {
      this(result.getName(), result.getAddress(), result.isDurable(), result.isTemporary(), result.getFilterString(), result.getConsumerCount(), result.getMessageCount(), result.isExists(), result.isAutoCreateJmsQueues());
   }

   public SessionQueueQueryResponseMessage_V2() {
      this(null, null, false, false, null, 0, 0, false, false);
   }

   private SessionQueueQueryResponseMessage_V2(final SimpleString name,
                                               final SimpleString address,
                                               final boolean durable,
                                               final boolean temporary,
                                               final SimpleString filterString,
                                               final int consumerCount,
                                               final long messageCount,
                                               final boolean exists,
                                               final boolean autoCreationEnabled) {
      super(SESS_QUEUEQUERY_RESP_V2);

      this.durable = durable;

      this.temporary = temporary;

      this.consumerCount = consumerCount;

      this.messageCount = messageCount;

      this.filterString = filterString;

      this.address = address;

      this.name = name;

      this.exists = exists;

      this.autoCreationEnabled = autoCreationEnabled;
   }

   public boolean isAutoCreationEnabled() {
      return autoCreationEnabled;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      super.encodeRest(buffer);
      buffer.writeBoolean(autoCreationEnabled);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      super.decodeRest(buffer);
      autoCreationEnabled = buffer.readBoolean();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (autoCreationEnabled ? 1231 : 1237);
      return result;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", address=" + address);
      buff.append(", name=" + name);
      buff.append(", consumerCount=" + consumerCount);
      buff.append(", filterString=" + filterString);
      buff.append(", durable=" + durable);
      buff.append(", exists=" + exists);
      buff.append(", temporary=" + temporary);
      buff.append(", messageCount=" + messageCount);
      buff.append(", autoCreationEnabled=" + autoCreationEnabled);
      buff.append("]");
      return buff.toString();
   }

   @Override
   public ClientSession.QueueQuery toQueueQuery() {
      return new QueueQueryImpl(isDurable(), isTemporary(), getConsumerCount(), getMessageCount(), getFilterString(), getAddress(), getName(), isExists(), isAutoCreationEnabled());
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionQueueQueryResponseMessage_V2))
         return false;
      SessionQueueQueryResponseMessage_V2 other = (SessionQueueQueryResponseMessage_V2) obj;
      if (autoCreationEnabled != other.autoCreationEnabled)
         return false;
      return true;
   }
}
