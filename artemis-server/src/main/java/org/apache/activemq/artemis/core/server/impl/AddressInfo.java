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
package org.apache.activemq.artemis.core.server.impl;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;

public class AddressInfo {

   public enum RoutingType {
      MULTICAST, ANYCAST
   }

   private final SimpleString name;

   private RoutingType routingType = RoutingType.MULTICAST;

   private boolean defaultDeleteOnNoConsumers;

   private int defaultMaxConsumers;

   public AddressInfo(SimpleString name) {
      this.name = name;
   }

   public RoutingType getRoutingType() {
      return routingType;
   }

   public void setRoutingType(RoutingType routingType) {
      this.routingType = routingType;
   }

   public boolean isDefaultDeleteOnNoConsumers() {
      return defaultDeleteOnNoConsumers;
   }

   public void setDefaultDeleteOnNoConsumers(boolean defaultDeleteOnNoConsumers) {
      this.defaultDeleteOnNoConsumers = defaultDeleteOnNoConsumers;
   }

   public int getDefaultMaxConsumers() {
      return defaultMaxConsumers;
   }

   public void setDefaultMaxConsumers(int defaultMaxConsumers) {
      this.defaultMaxConsumers = defaultMaxConsumers;
   }

   public SimpleString getName() {
      return name;
   }
}
