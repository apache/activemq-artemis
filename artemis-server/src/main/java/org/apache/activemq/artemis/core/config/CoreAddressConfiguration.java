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
package org.apache.activemq.artemis.core.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.activemq.artemis.core.server.RoutingType;

public class CoreAddressConfiguration implements Serializable {

   private String name = null;

   private Set<RoutingType> routingTypes = new HashSet<>();

   private List<CoreQueueConfiguration> queueConfigurations = new ArrayList<>();

   public CoreAddressConfiguration() {
   }

   public String getName() {
      return name;
   }

   public CoreAddressConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public Set<RoutingType> getRoutingTypes() {
      return routingTypes;
   }

   public CoreAddressConfiguration addDeliveryMode(RoutingType routingType) {
      routingTypes.add(routingType);
      return this;
   }

   public CoreAddressConfiguration setQueueConfigurations(List<CoreQueueConfiguration> queueConfigurations) {
      this.queueConfigurations = queueConfigurations;
      return this;
   }

   public CoreAddressConfiguration addQueueConfiguration(CoreQueueConfiguration queueConfiguration) {
      this.queueConfigurations.add(queueConfiguration);
      return this;
   }

   public List<CoreQueueConfiguration> getQueueConfigurations() {
      return queueConfigurations;
   }
}
