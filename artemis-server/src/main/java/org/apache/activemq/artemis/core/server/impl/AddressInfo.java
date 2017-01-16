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

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.RoutingType;

public class AddressInfo {

   private long id;

   private final SimpleString name;

   private boolean autoCreated = false;

   private Set<RoutingType> routingTypes;

   public AddressInfo(SimpleString name) {
      this.name = name;
      routingTypes = new HashSet<>();
   }

   /**
    * Creates an AddressInfo object with a Set of routing types
    * @param name
    * @param routingTypes
    */
   public AddressInfo(SimpleString name, Set<RoutingType> routingTypes) {
      this.name = name;
      this.routingTypes = routingTypes;
   }

   /**
    * Creates an AddressInfo object with a single RoutingType associated with it.
    * @param name
    * @param routingType
    */
   public AddressInfo(SimpleString name, RoutingType routingType) {
      this.name = name;
      this.routingTypes = new HashSet<>();
      routingTypes.add(routingType);
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

   public void setId(long id) {
      this.id = id;
   }

   public long getId() {
      return id;
   }

   public Set<RoutingType> getRoutingTypes() {
      return routingTypes;
   }

   public AddressInfo setRoutingTypes(Set<RoutingType> routingTypes) {
      this.routingTypes = routingTypes;
      return this;
   }

   public AddressInfo addRoutingType(RoutingType routingType) {
      if (routingTypes == null) {
         routingTypes = new HashSet<>();
      }
      routingTypes.add(routingType);
      return this;
   }

   public RoutingType getRoutingType() {
      /* We want to use a Set to guarantee only a single entry for ANYCAST, MULTICAST can be added to routing types.
         There are cases where we also want to get any routing type (when a queue doesn't specifyc it's routing type for
         example.  For this reason we return the first element in the Set.
         */
      // TODO There must be a better way of doing this.  This creates an iterator on each lookup.
      for (RoutingType routingType : routingTypes) {
         return routingType;
      }
      return null;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("Address [name=" + name);
      buff.append(", id=" + id);
      buff.append(", routingTypes={");
      for (RoutingType routingType : routingTypes) {
         buff.append(routingType.toString() + ",");
      }
      // delete hanging comma
      if (buff.charAt(buff.length() - 1) == ',') {
         buff.deleteCharAt(buff.length() - 1);
      }
      buff.append("}");
      buff.append(", autoCreated=" + autoCreated);
      buff.append("]");
      return buff.toString();
   }

}
