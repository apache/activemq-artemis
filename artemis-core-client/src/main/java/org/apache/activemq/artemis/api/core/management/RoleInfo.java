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
package org.apache.activemq.artemis.api.core.management;

import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonObject;

import org.apache.activemq.artemis.api.core.JsonUtil;

/**
 * Helper class to create Java Objects from the JSON serialization returned by {@link AddressControl#getRolesAsJSON()}.
 */
public final class RoleInfo {

   private final String name;

   private final boolean send;

   private final boolean consume;

   private final boolean createDurableQueue;

   private final boolean deleteDurableQueue;

   private final boolean createNonDurableQueue;

   private final boolean deleteNonDurableQueue;

   private final boolean manage;

   private final boolean browse;

   private final boolean createAddress;

   private final boolean deleteAddress;

   /**
    * {@return an array of RoleInfo corresponding to the JSON serialization returned by {@link
    * AddressControl#getRolesAsJSON()}}
    */
   public static RoleInfo[] from(final String jsonString) throws Exception {
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      RoleInfo[] roles = new RoleInfo[array.size()];
      for (int i = 0; i < array.size(); i++) {
         JsonObject r = array.getJsonObject(i);
         RoleInfo role = new RoleInfo(
                 r.getString("name"),
                 r.getBoolean("send"),
                 r.getBoolean("consume"),
                 r.getBoolean("createDurableQueue"),
                 r.getBoolean("deleteDurableQueue"),
                 r.getBoolean("createNonDurableQueue"),
                 r.getBoolean("deleteNonDurableQueue"),
                 r.getBoolean("manage"),
                 r.getBoolean("browse"),
                 r.getBoolean("createAddress"),
                 r.getBoolean("deleteAddress"));
         roles[i] = role;
      }
      return roles;
   }

   private RoleInfo(final String name,
                    final boolean send,
                    final boolean consume,
                    final boolean createDurableQueue,
                    final boolean deleteDurableQueue,
                    final boolean createNonDurableQueue,
                    final boolean deleteNonDurableQueue,
                    final boolean manage,
                    final boolean browse,
                    final boolean createAddress,
                    final boolean deleteAddress) {
      this.name = name;
      this.send = send;
      this.consume = consume;
      this.createDurableQueue = createDurableQueue;
      this.deleteDurableQueue = deleteDurableQueue;
      this.createNonDurableQueue = createNonDurableQueue;
      this.deleteNonDurableQueue = deleteNonDurableQueue;
      this.manage = manage;
      this.browse = browse;
      this.createAddress = createAddress;
      this.deleteAddress = deleteAddress;
   }

   public String getName() {
      return name;
   }

   /**
    * {@return whether this role can send messages to the address}
    */
   public boolean isSend() {
      return send;
   }

   /**
    * {@return whether this role can consume messages from queues bound to the address}
    */
   public boolean isConsume() {
      return consume;
   }

   /**
    * {@return whether this role can create durable queues bound to the address}
    */
   public boolean isCreateDurableQueue() {
      return createDurableQueue;
   }

   /**
    * {@return whether this role can delete durable queues bound to the address}
    */
   public boolean isDeleteDurableQueue() {
      return deleteDurableQueue;
   }

   /**
    * {@return whether this role can create non-durable queues bound to the address}
    */
   public boolean isCreateNonDurableQueue() {
      return createNonDurableQueue;
   }

   /**
    * {@return whether this role can delete non-durable queues bound to the address}
    */
   public boolean isDeleteNonDurableQueue() {
      return deleteNonDurableQueue;
   }

   /**
    * {@return whether this role can send management messages to the address}
    */
   public boolean isManage() {
      return manage;
   }

   /**
    * {@return whether this role can browse queues bound to the address}
    */
   public boolean isBrowse() {
      return browse;
   }

   /**
    * {@return whether this role can create addresses}
    */
   public boolean isCreateAddress() {
      return createAddress;
   }

   /**
    * {@return whether this role can delete addresses}
    */
   public boolean isDeleteAddress() {
      return deleteAddress;
   }
}
