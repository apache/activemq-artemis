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

   public long id;

   public SimpleString name;

   public SimpleString address;

   public SimpleString filterString;

   public boolean autoCreated;

   public SimpleString user;

   public List<QueueStatusEncoding> queueStatusEncodings;

   public int maxConsumers;

   public boolean purgeOnNoConsumers;

   public boolean exclusive;

   public byte routingType;

   public PersistentQueueBindingEncoding() {
   }

   @Override
   public String toString() {
      return "PersistentQueueBindingEncoding [id=" + id +
         ", name=" +
         name +
         ", address=" +
         address +
         ", filterString=" +
         filterString +
         ", user=" +
         user +
         ", autoCreated=" +
         autoCreated +
         ", maxConsumers=" +
         maxConsumers +
         ", purgeOnNoConsumers=" +
         purgeOnNoConsumers +
          ", exclusive=" +
          exclusive +
         ", routingType=" +
         routingType +
         "]";
   }

   public PersistentQueueBindingEncoding(final SimpleString name,
                                         final SimpleString address,
                                         final SimpleString filterString,
                                         final SimpleString user,
                                         final boolean autoCreated,
                                         final int maxConsumers,
                                         final boolean purgeOnNoConsumers,
                                         final boolean exclusive,
                                         final byte routingType) {
      this.name = name;
      this.address = address;
      this.filterString = filterString;
      this.user = user;
      this.autoCreated = autoCreated;
      this.maxConsumers = maxConsumers;
      this.purgeOnNoConsumers = purgeOnNoConsumers;
      this.exclusive = exclusive;
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
   public SimpleString getAddress() {
      return address;
   }

   @Override
   public void replaceQueueName(SimpleString newName) {
      this.name = newName;
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
   public boolean isExclusive() {
      return exclusive;
   }

   @Override
   public void setExclusive(boolean exclusive) {
      this.exclusive = exclusive;
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
                  user = SimpleString.toSimpleString(keyValuePair[1]);
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
   }

   @Override
   public int getEncodeSize() {
      return SimpleString.sizeofString(name) + SimpleString.sizeofString(address) +
         SimpleString.sizeofNullableString(filterString) + DataConstants.SIZE_BOOLEAN +
         SimpleString.sizeofNullableString(createMetadata()) +
         DataConstants.SIZE_INT +
         DataConstants.SIZE_BOOLEAN +
         DataConstants.SIZE_BYTE +
         DataConstants.SIZE_BOOLEAN;
   }

   private SimpleString createMetadata() {
      StringBuilder metadata = new StringBuilder();
      metadata.append("user=").append(user).append(";");
      return SimpleString.toSimpleString(metadata.toString());
   }
}
