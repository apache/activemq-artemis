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
import org.apache.activemq.artemis.api.core.RoutingType;

public class SessionQueueQueryResponseMessage_V3 extends SessionQueueQueryResponseMessage_V2 {

   protected boolean autoCreated;

   protected boolean purgeOnNoConsumers;

   protected RoutingType routingType;

   protected int maxConsumers;

   public SessionQueueQueryResponseMessage_V3(final QueueQueryResult result) {
      this(result.getName(), result.getAddress(), result.isDurable(), result.isTemporary(), result.getFilterString(), result.getConsumerCount(), result.getMessageCount(), result.isExists(), result.isAutoCreateQueues(), result.isAutoCreated(), result.isPurgeOnNoConsumers(), result.getRoutingType(), result.getMaxConsumers());
   }

   public SessionQueueQueryResponseMessage_V3() {
      this(null, null, false, false, null, 0, 0, false, false, false, false, RoutingType.MULTICAST, -1);
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
                                               final int maxConsumers) {
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

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      super.encodeRest(buffer);
      buffer.writeBoolean(autoCreated);
      buffer.writeBoolean(purgeOnNoConsumers);
      buffer.writeByte(routingType.getType());
      buffer.writeInt(maxConsumers);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      super.decodeRest(buffer);
      autoCreated = buffer.readBoolean();
      purgeOnNoConsumers = buffer.readBoolean();
      routingType = RoutingType.getType(buffer.readByte());
      maxConsumers = buffer.readInt();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (autoCreated ? 1231 : 1237);
      result = prime * result + (purgeOnNoConsumers ? 1231 : 1237);
      result = prime * result + routingType.hashCode();
      result = prime * result + maxConsumers;
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
      buff.append(", autoCreated=" + autoCreated);
      buff.append(", purgeOnNoConsumers=" + purgeOnNoConsumers);
      buff.append(", routingType=" + routingType);
      buff.append(", maxConsumers=" + maxConsumers);
      return buff.toString();
   }

   @Override
   public ClientSession.QueueQuery toQueueQuery() {
      return new QueueQueryImpl(isDurable(), isTemporary(), getConsumerCount(), getMessageCount(), getFilterString(), getAddress(), getName(), isExists(), isAutoCreateQueues(), getMaxConsumers(), isAutoCreated(), isPurgeOnNoConsumers(), getRoutingType());
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionQueueQueryResponseMessage_V3))
         return false;
      SessionQueueQueryResponseMessage_V3 other = (SessionQueueQueryResponseMessage_V3) obj;
      if (autoCreated != other.autoCreated)
         return false;
      if (purgeOnNoConsumers != other.purgeOnNoConsumers)
         return false;
      if (routingType == null) {
         if (other.routingType != null)
            return false;
      } else if (!routingType.equals(other.routingType))
         return false;
      if (maxConsumers != other.maxConsumers)
         return false;
      return true;
   }
}
