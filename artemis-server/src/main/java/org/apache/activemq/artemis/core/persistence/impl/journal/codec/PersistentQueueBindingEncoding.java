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
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.utils.DataConstants;

public class PersistentQueueBindingEncoding implements EncodingSupport, QueueBindingInfo {

   public long id;

   public SimpleString name;

   public SimpleString address;

   public SimpleString filterString;

   public boolean autoCreated;

   public SimpleString user;

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
         "]";
   }

   public PersistentQueueBindingEncoding(final SimpleString name,
                                         final SimpleString address,
                                         final SimpleString filterString,
                                         final SimpleString user,
                                         final boolean autoCreated) {
      this.name = name;
      this.address = address;
      this.filterString = filterString;
      this.user = user;
      this.autoCreated = autoCreated;
   }

   public long getId() {
      return id;
   }

   public void setId(final long id) {
      this.id = id;
   }

   public SimpleString getAddress() {
      return address;
   }

   public void replaceQueueName(SimpleString newName) {
      this.name = newName;
   }

   public SimpleString getFilterString() {
      return filterString;
   }

   public SimpleString getQueueName() {
      return name;
   }

   public SimpleString getUser() {
      return user;
   }

   public boolean isAutoCreated() {
      return autoCreated;
   }

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
   }

   public void encode(final ActiveMQBuffer buffer) {
      buffer.writeSimpleString(name);
      buffer.writeSimpleString(address);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeNullableSimpleString(createMetadata());
      buffer.writeBoolean(autoCreated);
   }

   public int getEncodeSize() {
      return SimpleString.sizeofString(name) + SimpleString.sizeofString(address) +
         SimpleString.sizeofNullableString(filterString) + DataConstants.SIZE_BOOLEAN +
         SimpleString.sizeofNullableString(createMetadata());
   }

   private SimpleString createMetadata() {
      StringBuilder metadata = new StringBuilder();
      metadata.append("user=").append(user).append(";");
      return SimpleString.toSimpleString(metadata.toString());
   }
}
