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

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.api.core.RoutingType;

public class CreateAddressMessage extends PacketImpl {

   private SimpleString address;

   private Set<RoutingType> routingTypes;

   private boolean autoCreated;

   private boolean requiresResponse;

   public CreateAddressMessage(final SimpleString address,
                               Set<RoutingType> routingTypes,
                               final boolean autoCreated,
                               final boolean requiresResponse) {
      this();

      this.address = address;
      this.routingTypes = routingTypes;
      this.autoCreated = autoCreated;
      this.requiresResponse = requiresResponse;
   }

   public CreateAddressMessage() {
      super(CREATE_ADDRESS);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", address=" + address);
      buff.append(", routingTypes=" + routingTypes);
      buff.append(", autoCreated=" + autoCreated);
      buff.append("]");
      return buff.toString();
   }

   public SimpleString getAddress() {
      return address;
   }

   public boolean isRequiresResponse() {
      return requiresResponse;
   }

   public boolean isAutoCreated() {
      return autoCreated;
   }

   public void setAddress(SimpleString address) {
      this.address = address;
   }

   public Set<RoutingType> getRoutingTypes() {
      return routingTypes;
   }

   public void setRoutingTypes(Set<RoutingType> routingTypes) {
      this.routingTypes = routingTypes;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeSimpleString(address);
      buffer.writeInt(routingTypes.size());
      for (RoutingType routingType : routingTypes) {
         buffer.writeByte(routingType.getType());
      }
      buffer.writeBoolean(requiresResponse);
      buffer.writeBoolean(autoCreated);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      address = buffer.readSimpleString();
      int routingTypeSetSize = buffer.readInt();
      routingTypes = new HashSet<>(routingTypeSetSize);
      for (int i = 0; i < routingTypeSetSize; i++) {
         routingTypes.add(RoutingType.getType(buffer.readByte()));
      }
      requiresResponse = buffer.readBoolean();
      autoCreated = buffer.readBoolean();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((address == null) ? 0 : address.hashCode());
      result = prime * result + (routingTypes.hashCode());
      result = prime * result + (autoCreated ? 1231 : 1237);
      result = prime * result + (requiresResponse ? 1231 : 1237);
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof CreateAddressMessage))
         return false;
      CreateAddressMessage other = (CreateAddressMessage) obj;
      if (address == null) {
         if (other.address != null)
            return false;
      } else if (!address.equals(other.address))
         return false;
      if (routingTypes == null) {
         if (other.routingTypes != null)
            return false;
      } else if (!routingTypes.equals(other.routingTypes))
         return false;
      if (autoCreated != other.autoCreated)
         return false;
      if (requiresResponse != other.requiresResponse)
         return false;
      return true;
   }
}
