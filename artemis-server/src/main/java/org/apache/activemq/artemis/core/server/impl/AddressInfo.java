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

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.utils.PrefixUtil;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class AddressInfo {

   private long id;

   private final SimpleString name;

   private boolean autoCreated = false;

   private static final EnumSet<RoutingType> EMPTY_ROUTING_TYPES = EnumSet.noneOf(RoutingType.class);
   private EnumSet<RoutingType> routingTypes;
   private RoutingType firstSeen;

   private boolean internal = false;

   private volatile long routedMessageCount = 0;

   private static final AtomicLongFieldUpdater<AddressInfo> routedMessageCountUpdater = AtomicLongFieldUpdater.newUpdater(AddressInfo.class, "routedMessageCount");

   private volatile long unRoutedMessageCount = 0;

   private static final AtomicLongFieldUpdater<AddressInfo> unRoutedMessageCountUpdater = AtomicLongFieldUpdater.newUpdater(AddressInfo.class, "unRoutedMessageCount");

   private long bindingRemovedTimestamp = -1;

   public AddressInfo(SimpleString name) {
      this(name, EnumSet.noneOf(RoutingType.class));
   }

   /**
    * Creates an AddressInfo object with a Set of routing types
    * @param name
    * @param routingTypes
    */
   public AddressInfo(SimpleString name, EnumSet<RoutingType> routingTypes) {
      this.name = name;
      setRoutingTypes(routingTypes);
   }

   /**
    * Creates an AddressInfo object with a single RoutingType associated with it.
    * @param name
    * @param routingType
    */
   public AddressInfo(SimpleString name, RoutingType routingType) {
      this.name = name;
      addRoutingType(routingType);
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

   public EnumSet<RoutingType> getRoutingTypes() {
      return routingTypes == null ? EMPTY_ROUTING_TYPES : routingTypes;
   }

   public AddressInfo setRoutingTypes(final EnumSet<RoutingType> routingTypes) {
      this.routingTypes = routingTypes;
      if (routingTypes != null && !routingTypes.isEmpty()) {
         this.firstSeen = routingTypes.iterator().next();
      } else {
         this.firstSeen = null;
      }
      return this;
   }

   public AddressInfo addRoutingType(final RoutingType routingType) {
      if (routingType != null) {
         if (routingTypes == null || routingTypes.isEmpty()) {
            routingTypes = EnumSet.of(routingType);
            firstSeen = routingType;
         } else {
            routingTypes.add(routingType);
         }
      }
      return this;
   }

   public RoutingType getRoutingType() {
      return firstSeen;
   }

   public long getBindingRemovedTimestamp() {
      return bindingRemovedTimestamp;
   }

   public void setBindingRemovedTimestamp(long bindingRemovedTimestamp) {
      this.bindingRemovedTimestamp = bindingRemovedTimestamp;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("Address [name=" + name);
      buff.append(", id=" + id);
      buff.append(", routingTypes={");
      for (RoutingType routingType : getRoutingTypes()) {
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

   public boolean isInternal() {
      return this.internal;
   }

   public void setInternal(boolean internal) {
      this.internal = internal;
   }

   public AddressInfo create(SimpleString name, RoutingType routingType) {
      AddressInfo info = new AddressInfo(name, routingType);
      info.setInternal(this.internal);
      return info;
   }

   public AddressInfo getAddressAndRoutingType(Map<SimpleString, RoutingType> prefixes) {
      for (Map.Entry<SimpleString, RoutingType> entry : prefixes.entrySet()) {
         if (this.getName().startsWith(entry.getKey())) {
            AddressInfo newAddressInfo = this.create(PrefixUtil.removePrefix(this.getName(), entry.getKey()), entry.getValue());
            return newAddressInfo;
         }
      }
      return this;
   }

   public long incrementRoutedMessageCount() {
      return routedMessageCountUpdater.incrementAndGet(this);
   }

   public long incrementUnRoutedMessageCount() {
      return unRoutedMessageCountUpdater.incrementAndGet(this);
   }

   public long getRoutedMessageCount() {
      return routedMessageCountUpdater.get(this);
   }

   public long getUnRoutedMessageCount() {
      return unRoutedMessageCountUpdater.get(this);
   }

}
