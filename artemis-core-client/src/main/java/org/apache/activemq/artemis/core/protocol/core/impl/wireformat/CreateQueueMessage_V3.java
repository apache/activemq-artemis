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
import org.apache.activemq.artemis.core.server.RoutingType;

public class CreateQueueMessage_V3 extends CreateQueueMessage_V2 {

   private RoutingType routingType;

   private int maxConsumers;

   private boolean deleteOnNoConsumers;

   public CreateQueueMessage_V3(final SimpleString address,
                                final SimpleString queueName,
                                final RoutingType routingType,
                                final SimpleString filterString,
                                final boolean durable,
                                final boolean temporary,
                                final int maxConsumers,
                                final boolean deleteOnNoConsumers,
                                final boolean autoCreated,
                                final boolean requiresResponse) {
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
      this.deleteOnNoConsumers = deleteOnNoConsumers;
   }

   public CreateQueueMessage_V3() {
      super(CREATE_QUEUE_V3);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(super.getParentString());
      buff.append(", routingType=" + routingType);
      buff.append(", maxConsumers=" + maxConsumers);
      buff.append(", deleteOnNoConsumers=" + deleteOnNoConsumers);
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

   public boolean isDeleteOnNoConsumers() {
      return deleteOnNoConsumers;
   }

   public void setDeleteOnNoConsumers(boolean deleteOnNoConsumers) {
      this.deleteOnNoConsumers = deleteOnNoConsumers;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      super.encodeRest(buffer);
      buffer.writeByte(routingType.getType());
      buffer.writeInt(maxConsumers);
      buffer.writeBoolean(deleteOnNoConsumers);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      super.decodeRest(buffer);
      routingType = RoutingType.getType(buffer.readByte());
      maxConsumers = buffer.readInt();
      deleteOnNoConsumers = buffer.readBoolean();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (routingType.getType());
      result = prime * result + (maxConsumers);
      result = prime * result + (deleteOnNoConsumers ? 1231 : 1237);
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof CreateQueueMessage_V3))
         return false;
      CreateQueueMessage_V3 other = (CreateQueueMessage_V3) obj;
      if (autoCreated != other.autoCreated)
         return false;
      return true;
   }
}
