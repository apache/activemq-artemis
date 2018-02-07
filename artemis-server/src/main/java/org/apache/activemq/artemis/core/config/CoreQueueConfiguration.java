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

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;

public class CoreQueueConfiguration implements Serializable {

   private static final long serialVersionUID = 650404974977490254L;

   private String address = null;

   private String name = null;

   private String filterString = null;

   private boolean durable = true;

   private String user = null;

   private Boolean exclusive;

   private Boolean lastValue;

   private Integer maxConsumers = ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers();

   private Boolean purgeOnNoConsumers = ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers();

   private RoutingType routingType = ActiveMQDefaultConfiguration.getDefaultRoutingType();

   public CoreQueueConfiguration() {
   }

   public String getAddress() {
      return address;
   }

   public String getName() {
      return name;
   }

   public String getFilterString() {
      return filterString;
   }

   public boolean isDurable() {
      return durable;
   }

   public String getUser() {
      return user;
   }

   public Boolean isExclusive() {
      return exclusive;
   }

   public Boolean isLastValue() {
      return lastValue;
   }

   /**
    * @param address the address to set
    */
   public CoreQueueConfiguration setAddress(final String address) {
      this.address = address;
      return this;
   }

   /**
    * @param name the name to set
    */
   public CoreQueueConfiguration setName(final String name) {
      this.name = name;
      return this;
   }

   /**
    * @param filterString the filterString to set
    */
   public CoreQueueConfiguration setFilterString(final String filterString) {
      this.filterString = filterString;
      return this;
   }

   /**
    * @param durable the durable to set; default value is true
    */
   public CoreQueueConfiguration setDurable(final boolean durable) {
      this.durable = durable;
      return this;
   }

   /**
    * @param maxConsumers for this queue, default is -1 (unlimited)
    */
   public CoreQueueConfiguration setMaxConsumers(Integer maxConsumers) {
      this.maxConsumers = maxConsumers;
      return this;
   }

   /**
    * @param purgeOnNoConsumers delete this queue when consumer count reaches 0, default is false
    */
   public CoreQueueConfiguration setPurgeOnNoConsumers(Boolean purgeOnNoConsumers) {
      this.purgeOnNoConsumers = purgeOnNoConsumers;
      return this;
   }

   /**
    * @param user the use you want to associate with creating the queue
    */
   public CoreQueueConfiguration setUser(String user) {
      this.user = user;
      return this;
   }

   public CoreQueueConfiguration setExclusive(Boolean exclusive) {
      this.exclusive = exclusive;
      return this;
   }

   public CoreQueueConfiguration setLastValue(Boolean lastValue) {
      this.lastValue = lastValue;
      return this;
   }

   public boolean getPurgeOnNoConsumers() {
      return purgeOnNoConsumers;
   }

   public int getMaxConsumers() {
      return maxConsumers;
   }

   public RoutingType getRoutingType() {
      return routingType;
   }

   public CoreQueueConfiguration setRoutingType(RoutingType routingType) {
      this.routingType = routingType;
      return this;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((address == null) ? 0 : address.hashCode());
      result = prime * result + (durable ? 1231 : 1237);
      result = prime * result + ((filterString == null) ? 0 : filterString.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((maxConsumers == null) ? 0 : maxConsumers.hashCode());
      result = prime * result + ((purgeOnNoConsumers == null) ? 0 : purgeOnNoConsumers.hashCode());
      result = prime * result + ((exclusive == null) ? 0 : exclusive.hashCode());
      result = prime * result + ((lastValue == null) ? 0 : lastValue.hashCode());
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
      CoreQueueConfiguration other = (CoreQueueConfiguration) obj;
      if (address == null) {
         if (other.address != null)
            return false;
      } else if (!address.equals(other.address))
         return false;
      if (durable != other.durable)
         return false;
      if (filterString == null) {
         if (other.filterString != null)
            return false;
      } else if (!filterString.equals(other.filterString))
         return false;
      if (name == null) {
         if (other.name != null)
            return false;
      } else if (!name.equals(other.name))
         return false;
      if (maxConsumers == null) {
         if (other.maxConsumers != null)
            return false;
      } else if (!maxConsumers.equals(other.maxConsumers))
         return false;
      if (purgeOnNoConsumers == null) {
         if (other.purgeOnNoConsumers != null)
            return false;
      } else if (!purgeOnNoConsumers.equals(other.purgeOnNoConsumers)) {
         return false;
      }
      if (exclusive == null) {
         if (other.exclusive != null)
            return false;
      } else if (!exclusive.equals(other.exclusive)) {
         return false;
      }
      if (lastValue == null) {
         if (other.lastValue != null)
            return false;
      } else if (!lastValue.equals(other.lastValue)) {
         return false;
      }
      return true;
   }
}
