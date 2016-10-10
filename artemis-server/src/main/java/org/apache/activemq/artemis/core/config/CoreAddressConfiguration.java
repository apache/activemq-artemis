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
import java.util.List;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;

public class CoreAddressConfiguration implements Serializable {

   private String name = null;

   private AddressInfo.RoutingType routingType = null;

   private Integer defaultMaxConsumers = ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers();

   private Boolean defaultDeleteOnNoConsumers = ActiveMQDefaultConfiguration.getDefaultDeleteQueueOnNoConsumers();

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

   public AddressInfo.RoutingType getRoutingType() {
      return routingType;
   }

   public CoreAddressConfiguration setRoutingType(AddressInfo.RoutingType routingType) {
      this.routingType = routingType;
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

   public Boolean getDefaultDeleteOnNoConsumers() {
      return defaultDeleteOnNoConsumers;
   }

   public CoreAddressConfiguration setDefaultDeleteOnNoConsumers(Boolean defaultDeleteOnNoConsumers) {
      this.defaultDeleteOnNoConsumers = defaultDeleteOnNoConsumers;
      return this;
   }

   public Integer getDefaultMaxConsumers() {
      return defaultMaxConsumers;
   }

   public CoreAddressConfiguration setDefaultMaxConsumers(Integer defaultMaxConsumers) {
      this.defaultMaxConsumers = defaultMaxConsumers;
      return this;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((routingType == null) ? 0 : routingType.hashCode());
      result = prime * result + ((queueConfigurations == null) ? 0 : queueConfigurations.hashCode());
      result = prime * result + ((defaultMaxConsumers == null) ? 0 : defaultMaxConsumers.hashCode());
      result = prime * result + ((defaultDeleteOnNoConsumers == null) ? 0 : defaultDeleteOnNoConsumers.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      CoreAddressConfiguration other = (CoreAddressConfiguration) obj;
      if (name == null) {
         if (other.name != null)
            return false;
      } else if (!name.equals(other.name))
         return false;
      if (routingType == null) {
         if (other.routingType != null)
            return false;
      } else if (!routingType.equals(other.routingType))
         return false;
      if (queueConfigurations == null) {
         if (other.queueConfigurations != null)
            return false;
      } else if (!queueConfigurations.equals(other.queueConfigurations))
         return false;
      if (defaultMaxConsumers == null) {
         if (other.defaultMaxConsumers != null)
            return false;
      } else if (!defaultMaxConsumers.equals(other.defaultMaxConsumers))
         return false;
      if (defaultDeleteOnNoConsumers == null) {
         if (other.defaultDeleteOnNoConsumers != null)
            return false;
      } else if (!defaultDeleteOnNoConsumers.equals(other.defaultDeleteOnNoConsumers)) {
         return false;
      }

      return true;
   }
}
