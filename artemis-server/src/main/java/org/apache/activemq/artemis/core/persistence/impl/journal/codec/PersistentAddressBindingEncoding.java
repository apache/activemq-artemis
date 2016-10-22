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

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.utils.DataConstants;

public class PersistentAddressBindingEncoding implements EncodingSupport, AddressBindingInfo {

   public long id;

   public SimpleString name;

   public AddressInfo.RoutingType routingType;

   public PersistentAddressBindingEncoding() {
   }

   @Override
   public String toString() {
      return "PersistentAddressBindingEncoding [id=" + id +
         ", name=" +
         name +
         ", routingType=" +
         routingType +
         "]";
   }

   public PersistentAddressBindingEncoding(final SimpleString name,
                                           final AddressInfo.RoutingType routingType) {
      this.name = name;
      this.routingType = routingType;
   }

   @Override
   public long getId() {
      return id;
   }

   public void setId(final long id) {
      this.id = id;
   }

   @Override
   public SimpleString getName() {
      return name;
   }

   @Override
   public AddressInfo.RoutingType getRoutingType() {
      return routingType;
   }

   @Override
   public void decode(final ActiveMQBuffer buffer) {
      name = buffer.readSimpleString();
      routingType = AddressInfo.RoutingType.getType(buffer.readByte());
   }

   @Override
   public void encode(final ActiveMQBuffer buffer) {
      buffer.writeSimpleString(name);
      buffer.writeByte(routingType.getType());
   }

   @Override
   public int getEncodeSize() {
      return SimpleString.sizeofString(name) + DataConstants.SIZE_BYTE;
   }
}
