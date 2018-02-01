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
package org.apache.activemq.artemis.core.server;

import java.util.List;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.AutoCreatedQueuesDurability;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;

public class BindingQueryResult {

   private boolean exists;

   private List<SimpleString> queueNames;

   private boolean autoCreateQueues;

   private boolean autoCreateQueuesDurable;

   private boolean autoCreateAddresses;

   private boolean defaultPurgeOnNoConsumers;

   private int defaultMaxConsumers;

   private final AddressInfo addressInfo;

   public BindingQueryResult(final boolean exists,
                             final AddressInfo addressInfo,
                             final List<SimpleString> queueNames,
                             final boolean autoCreateQueues,
                             final boolean autoCreateQueuesDurable,
                             final boolean autoCreateAddresses,
                             final boolean defaultPurgeOnNoConsumers,
                             final int defaultMaxConsumers) {
      this.addressInfo = addressInfo;

      this.exists = exists;

      this.queueNames = queueNames;

      this.autoCreateQueues = autoCreateQueues;

      this.autoCreateQueuesDurable = autoCreateQueuesDurable;

      this.autoCreateAddresses = autoCreateAddresses;

      this.defaultPurgeOnNoConsumers = defaultPurgeOnNoConsumers;

      this.defaultMaxConsumers = defaultMaxConsumers;
   }

   public boolean isExists() {
      return exists;
   }

   public AddressInfo getAddressInfo() {
      return addressInfo;
   }

   public boolean isAutoCreateQueues() {
      return autoCreateQueues;
   }

   public AutoCreatedQueuesDurability isAutoCreateQueuesDurable() {
      if (autoCreateQueuesDurable) {
         return AutoCreatedQueuesDurability.DURABLE;
      } else {
         return AutoCreatedQueuesDurability.NON_DURABLE;
      }
   }

   public boolean isAutoCreateAddresses() {
      return autoCreateAddresses;
   }

   public List<SimpleString> getQueueNames() {
      return queueNames;
   }

   public boolean isDefaultPurgeOnNoConsumers() {
      return defaultPurgeOnNoConsumers;
   }

   public int getDefaultMaxConsumers() {
      return defaultMaxConsumers;
   }
}
