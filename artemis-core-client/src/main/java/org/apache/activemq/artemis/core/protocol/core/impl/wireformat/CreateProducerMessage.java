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
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;

import java.util.Objects;

public class CreateProducerMessage  extends PacketImpl {
   protected int id;

   protected SimpleString address;

   public CreateProducerMessage() {
      super(PacketImpl.CREATE_PRODUCER);
   }

   public CreateProducerMessage(int id, SimpleString address) {
      super(PacketImpl.CREATE_PRODUCER);
      this.id = id;
      this.address = address;
   }

   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   public SimpleString getAddress() {
      return address;
   }

   public void setAddress(SimpleString address) {
      this.address = address;
   }



   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeInt(id);
      buffer.writeNullableSimpleString(address);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      id = buffer.readInt();
      address = buffer.readNullableSimpleString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!(obj instanceof CreateProducerMessage other)) {
         return false;
      }
      if (!super.equals(obj)) {
         return false;
      }

      return Objects.equals(id, other.id) &&
             Objects.equals(address, other.address);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), id, address);
   }
}
