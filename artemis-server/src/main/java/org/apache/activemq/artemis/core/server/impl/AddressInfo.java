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

   private final SimpleString name;

   private RoutingType routingType = RoutingType.MULTICAST;

   private boolean defaultDeleteOnNoConsumers = ActiveMQDefaultConfiguration.getDefaultDeleteQueueOnNoConsumers();

   private int defaultMaxQueueConsumers = ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers();

   private boolean autoCreated = false;

   public AddressInfo(SimpleString name) {
      this.name = name;
   }

   public AddressInfo(SimpleString name, RoutingType routingType,  boolean defaultDeleteOnNoConsumers, int defaultMaxConsumers) {
      this(name);
      this.routingType = routingType;
      this.defaultDeleteOnNoConsumers = defaultDeleteOnNoConsumers;
      this.defaultMaxQueueConsumers = defaultMaxConsumers;
   }

   public RoutingType getRoutingType() {
      return routingType;
   }

   public AddressInfo setRoutingType(RoutingType routingType) {
      this.routingType = routingType;
      return this;
   }

   public boolean isDefaultDeleteOnNoConsumers() {
      return defaultDeleteOnNoConsumers;
   }

   public AddressInfo setDefaultDeleteOnNoConsumers(boolean defaultDeleteOnNoConsumers) {
      this.defaultDeleteOnNoConsumers = defaultDeleteOnNoConsumers;
      return this;
   }

   public int getDefaultMaxQueueConsumers() {
      return defaultMaxQueueConsumers;
   }

   public AddressInfo setDefaultMaxQueueConsumers(int defaultMaxQueueConsumers) {
      this.defaultMaxQueueConsumers = defaultMaxQueueConsumers;
      return this;
   }

   public boolean isAutoCreated() {
      return autoCreated;
   }

   public AddressInfo setAutoCreated(boolean autoCreated) {
      this.autoCreated = autoCreated;
      return this;
   }

   public SimpleString getName() {
      return name;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("AddressInfo [name=" + name);
      buff.append(", routingType=" + routingType);
      buff.append(", defaultMaxQueueConsumers=" + defaultMaxQueueConsumers);
      buff.append(", defaultDeleteOnNoConsumers=" + defaultDeleteOnNoConsumers);
      buff.append(", autoCreated=" + autoCreated);
      buff.append("]");
      return buff.toString();
   }

   public enum RoutingType {
      MULTICAST, ANYCAST;

      public byte getType() {
         switch (this) {
            case MULTICAST:
               return 0;
            case ANYCAST:
               return 1;
            default:
               return -1;
         }
      }

      public static RoutingType getType(byte type) {
         switch (type) {
            case 0:
               return MULTICAST;
            case 1:
               return ANYCAST;
            default:
               return null;
         }
      }
   }
}
