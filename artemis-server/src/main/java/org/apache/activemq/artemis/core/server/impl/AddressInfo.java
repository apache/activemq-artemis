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

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.io.StringReader;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.AddressQueueStatus;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.settings.HierarchicalRepositoryChangeListener;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.apache.activemq.artemis.utils.JsonLoader;
import org.apache.activemq.artemis.utils.PrefixUtil;

public class AddressInfo {

   private long id;
   private long pauseStatusRecord = -1;

   private SimpleString name;

   private boolean autoCreated = false;

   private volatile boolean temporary = false;

   private static final EnumSet<RoutingType> EMPTY_ROUTING_TYPES = EnumSet.noneOf(RoutingType.class);
   private EnumSet<RoutingType> routingTypes;
   private RoutingType firstSeen;

   private boolean internal = false;

   private volatile long routedMessageCount = 0;

   private static final AtomicLongFieldUpdater<AddressInfo> routedMessageCountUpdater = AtomicLongFieldUpdater.newUpdater(AddressInfo.class, "routedMessageCount");

   private volatile long unRoutedMessageCount = 0;

   private static final AtomicLongFieldUpdater<AddressInfo> unRoutedMessageCountUpdater = AtomicLongFieldUpdater.newUpdater(AddressInfo.class, "unRoutedMessageCount");

   private long bindingRemovedTimestamp = -1;

   private volatile boolean paused = false;

   private PostOffice postOffice;
   private StorageManager storageManager;
   private HierarchicalRepositoryChangeListener repositoryChangeListener;

   /**
    * Private constructor used on JSON decoding.
    */
   private AddressInfo() {
   }

   public AddressInfo(String name) {
      this(SimpleString.toSimpleString(name), EnumSet.noneOf(RoutingType.class));
   }

   public AddressInfo(SimpleString name) {
      this(name, EnumSet.noneOf(RoutingType.class));
   }

   /**
    * Creates an AddressInfo object with a Set of routing types
    * @param name
    * @param routingTypes
    */
   public AddressInfo(SimpleString name, EnumSet<RoutingType> routingTypes) {
      this.name = CompositeAddress.extractAddressName(name);
      setRoutingTypes(routingTypes);
   }

   /**
    * Creates an AddressInfo object with a single RoutingType associated with it.
    * @param name
    * @param routingType
    */
   public AddressInfo(SimpleString name, RoutingType routingType) {
      this.name = CompositeAddress.extractAddressName(name);
      addRoutingType(routingType);
   }

   public boolean isAutoCreated() {
      return autoCreated;
   }

   public AddressInfo setAutoCreated(boolean autoCreated) {
      this.autoCreated = autoCreated;
      return this;
   }

   public boolean isTemporary() {
      return temporary;
   }

   public AddressInfo setTemporary(boolean temporary) {
      this.temporary = temporary;
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


   public synchronized void reloadPause(long recordID) {

      if (pauseStatusRecord >= 0) {
         try {
            storageManager.deleteAddressStatus(pauseStatusRecord);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.unableToDeleteQueueStatus(e);
         }
      }

      this.pauseStatusRecord = recordID;

      try {
         Bindings bindings = postOffice.lookupBindingsForAddress(this.getName());
         if (bindings != null) {
            for (Binding binding : bindings.getBindings()) {
               if (binding instanceof QueueBinding) {
                  ((QueueBinding) binding).getQueue().pause(false);
               }
            }
         }
      } catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
      }

      this.paused = true;
   }

   public synchronized void pause(boolean persist) {
      if (postOffice == null) {
         throw new IllegalStateException("");
      }
      if (storageManager == null && persist) {
         throw new IllegalStateException("");
      }
      try {
         if (persist) {
            if (pauseStatusRecord >= 0) {
               try {
                  storageManager.deleteAddressStatus(pauseStatusRecord);
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.unableToDeleteQueueStatus(e);
               }
            }

            this.pauseStatusRecord = storageManager.storeAddressStatus(this.getId(), AddressQueueStatus.PAUSED);
         }
         Bindings bindings = postOffice.lookupBindingsForAddress(this.getName());
         if (bindings != null) {
            for (Binding binding : bindings.getBindings()) {
               if (binding instanceof QueueBinding) {
                  ((QueueBinding) binding).getQueue().pause(false);
               }
            }
         }
         this.paused = true;
      } catch (Exception ex) {
         throw new RuntimeException(ex);
      }
   }

   public synchronized void resume() {
      if (postOffice == null) {
         throw new IllegalStateException("");
      }
      if (storageManager == null && this.pauseStatusRecord > 0) {
         throw new IllegalStateException("");
      }
      if (!this.paused) {
         return;
      }
      try {
         if (this.pauseStatusRecord > 0) {
            storageManager.deleteAddressStatus(this.pauseStatusRecord);
         }
         Bindings bindings = postOffice.lookupBindingsForAddress(this.getName());
         if (bindings != null) {
            for (Binding binding : bindings.getBindings()) {
               if (binding instanceof QueueBinding) {
                  ((QueueBinding) binding).getQueue().resume();
               }
            }
         }
         this.paused = false;
      } catch (Exception ex) {
         throw new RuntimeException(ex);
      }
   }

   boolean isPersisted() {
      return this.paused && this.pauseStatusRecord > 0;
   }

   public boolean isPaused() {
      return this.paused;
   }

   public void setPostOffice(PostOffice postOffice) {
      this.postOffice = postOffice;
   }

   public void setStorageManager(StorageManager storageManager) {
      this.storageManager = storageManager;
   }

   @Override
   public String toString() {
      StringBuilder buff = new StringBuilder();
      buff.append("Address [name=").append(name);
      buff.append(", id=").append(id);
      buff.append(", routingTypes={");
      for (RoutingType routingType : getRoutingTypes()) {
         buff.append(routingType.toString()).append(",");
      }
      // delete hanging comma
      if (buff.charAt(buff.length() - 1) == ',') {
         buff.deleteCharAt(buff.length() - 1);
      }
      buff.append("}");
      buff.append(", autoCreated=").append(autoCreated);
      buff.append(", paused=").append(paused);
      buff.append("]");
      return buff.toString();
   }

   public boolean isInternal() {
      return this.internal;
   }

   public AddressInfo setInternal(boolean internal) {
      this.internal = internal;
      return this;
   }

   public AddressInfo create(SimpleString name, RoutingType routingType) {
      AddressInfo info = new AddressInfo(name, routingType);
      info.setInternal(this.internal);
      if (paused) {
         info.pause(this.pauseStatusRecord > 0);
      }
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

   public HierarchicalRepositoryChangeListener getRepositoryChangeListener() {
      return repositoryChangeListener;
   }

   public AddressInfo setRepositoryChangeListener(HierarchicalRepositoryChangeListener repositoryChangeListener) {
      this.repositoryChangeListener = repositoryChangeListener;
      return this;
   }

   public String toJSON() {
      JsonObjectBuilder builder = JsonLoader.createObjectBuilder();

      builder.add("id", id);
      builder.add("name", "" + name);
      builder.add("auto-created", autoCreated);
      builder.add("temporary", temporary);
      builder.add("internal", internal);
      if (firstSeen != null) {
         builder.add("firstSeen", firstSeen.getType());
      }
      if (routingTypes != null) {
         JsonArrayBuilder arrayBuilder = JsonLoader.createArrayBuilder();
         for (RoutingType rt : routingTypes) {
            arrayBuilder.add(rt.getType());
         }
         builder.add("routingTypes", arrayBuilder);
      }

      return builder.build().toString();
   }

   protected void setJson(String key, Object value) {
      if (key.equals("id")) {
         JsonNumber jsonLong = (JsonNumber) value;
         this.id = jsonLong.longValue();
      } else if (key.equals("name")) {
         JsonString jasonString = (JsonString) value;
         this.name = SimpleString.toSimpleString(jasonString.getString());
      } else if (key.equals("auto-created")) {
         this.autoCreated = Boolean.valueOf(value.toString());
      } else if (key.equals("temporary")) {
         this.temporary = Boolean.valueOf(value.toString());
      } else if (key.equals("firstSeen")) {
         JsonNumber jsonNumber = (JsonNumber)value;
         this.firstSeen = RoutingType.getType((byte)jsonNumber.intValue());
      } else if (key.equals("routingTypes")) {
         JsonArray routingTypes = (JsonArray) value;
         for (JsonValue rtValue : routingTypes) {
            JsonNumber jsonNumber = (JsonNumber)rtValue;
            this.addRoutingType(RoutingType.getType((byte)jsonNumber.intValue()));
         }
      }
   }

   public static AddressInfo fromJSON(String jsonString) {
      JsonObject json = JsonLoader.readObject(new StringReader(jsonString));

      AddressInfo result = new AddressInfo();

      for (Map.Entry<String, JsonValue> entry : json.entrySet()) {
         result.setJson(entry.getKey(), entry.getValue());
      }

      return result;
   }

}
