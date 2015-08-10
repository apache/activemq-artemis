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
import org.apache.activemq.artemis.utils.UUIDGenerator;

public class DivertConfiguration implements Serializable {

   private static final long serialVersionUID = 6910543740464269629L;

   private String name = null;

   private String routingName = UUIDGenerator.getInstance().generateStringUUID();

   private String address = null;

   private String forwardingAddress = null;

   private boolean exclusive = ActiveMQDefaultConfiguration.isDefaultDivertExclusive();

   private String filterString = null;

   private String transformerClassName = null;

   public DivertConfiguration() {
   }

   public String getName() {
      return name;
   }

   public String getRoutingName() {
      return routingName;
   }

   public String getAddress() {
      return address;
   }

   public String getForwardingAddress() {
      return forwardingAddress;
   }

   public boolean isExclusive() {
      return exclusive;
   }

   public String getFilterString() {
      return filterString;
   }

   public String getTransformerClassName() {
      return transformerClassName;
   }

   /**
    * @param name the name to set
    */
   public DivertConfiguration setName(final String name) {
      this.name = name;
      return this;
   }

   /**
    * @param routingName the routingName to set
    */
   public DivertConfiguration setRoutingName(final String routingName) {
      if (routingName == null) {
         this.routingName = UUIDGenerator.getInstance().generateStringUUID();
      }
      else {
         this.routingName = routingName;
      }
      return this;
   }

   /**
    * @param address the address to set
    */
   public DivertConfiguration setAddress(final String address) {
      this.address = address;
      return this;
   }

   /**
    * @param forwardingAddress the forwardingAddress to set
    */
   public DivertConfiguration setForwardingAddress(final String forwardingAddress) {
      this.forwardingAddress = forwardingAddress;
      return this;
   }

   /**
    * @param exclusive the exclusive to set
    */
   public DivertConfiguration setExclusive(final boolean exclusive) {
      this.exclusive = exclusive;
      return this;
   }

   /**
    * @param filterString the filterString to set
    */
   public DivertConfiguration setFilterString(final String filterString) {
      this.filterString = filterString;
      return this;
   }

   /**
    * @param transformerClassName the transformerClassName to set
    */
   public DivertConfiguration setTransformerClassName(final String transformerClassName) {
      this.transformerClassName = transformerClassName;
      return this;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((address == null) ? 0 : address.hashCode());
      result = prime * result + (exclusive ? 1231 : 1237);
      result = prime * result + ((filterString == null) ? 0 : filterString.hashCode());
      result = prime * result + ((forwardingAddress == null) ? 0 : forwardingAddress.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((routingName == null) ? 0 : routingName.hashCode());
      result = prime * result + ((transformerClassName == null) ? 0 : transformerClassName.hashCode());
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
      DivertConfiguration other = (DivertConfiguration) obj;
      if (address == null) {
         if (other.address != null)
            return false;
      }
      else if (!address.equals(other.address))
         return false;
      if (exclusive != other.exclusive)
         return false;
      if (filterString == null) {
         if (other.filterString != null)
            return false;
      }
      else if (!filterString.equals(other.filterString))
         return false;
      if (forwardingAddress == null) {
         if (other.forwardingAddress != null)
            return false;
      }
      else if (!forwardingAddress.equals(other.forwardingAddress))
         return false;
      if (name == null) {
         if (other.name != null)
            return false;
      }
      else if (!name.equals(other.name))
         return false;
      if (routingName == null) {
         if (other.routingName != null)
            return false;
      }
      else if (!routingName.equals(other.routingName))
         return false;
      if (transformerClassName == null) {
         if (other.transformerClassName != null)
            return false;
      }
      else if (!transformerClassName.equals(other.transformerClassName))
         return false;
      return true;
   }
}
