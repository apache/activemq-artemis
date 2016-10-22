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

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.apache.activemq.artemis.api.core.JsonUtil;

/**
 * Helper class to create Java Objects from the
 * JSON serialization returned by {@link AddressControl#getRolesAsJSON()}.
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

   /**
    * Returns an array of RoleInfo corresponding to the JSON serialization returned
    * by {@link AddressControl#getRolesAsJSON()}.
    */
   public static RoleInfo[] from(final String jsonString) throws Exception {
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      RoleInfo[] roles = new RoleInfo[array.size()];
      for (int i = 0; i < array.size(); i++) {
         JsonObject r = array.getJsonObject(i);
         RoleInfo role = new RoleInfo(r.getString("name"), r.getBoolean("send"), r.getBoolean("consume"), r.getBoolean("createDurableQueue"), r.getBoolean("deleteDurableQueue"), r.getBoolean("createNonDurableQueue"), r.getBoolean("deleteNonDurableQueue"), r.getBoolean("manage"), r.getBoolean("browse"), r.getBoolean("createAddress"));
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
                    final boolean createAddress) {
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
   }

   /**
    * Returns the name of the role.
    */
   public String getName() {
      return name;
   }

   /**
    * Returns whether this role can send messages to the address.
    */
   public boolean isSend() {
      return send;
   }

   /**
    * Returns whether this role can consume messages from queues bound to the address.
    */
   public boolean isConsume() {
      return consume;
   }

   /**
    * Returns whether this role can create durable queues bound to the address.
    */
   public boolean isCreateDurableQueue() {
      return createDurableQueue;
   }

   /**
    * Returns whether this role can delete durable queues bound to the address.
    */
   public boolean isDeleteDurableQueue() {
      return deleteDurableQueue;
   }

   /**
    * Returns whether this role can create non-durable queues bound to the address.
    */
   public boolean isCreateNonDurableQueue() {
      return createNonDurableQueue;
   }

   /**
    * Returns whether this role can delete non-durable queues bound to the address.
    */
   public boolean isDeleteNonDurableQueue() {
      return deleteNonDurableQueue;
   }

   /**
    * Returns whether this role can send management messages to the address.
    */
   public boolean isManage() {
      return manage;
   }

   /**
    * Returns whether this role can browse queues bound to the address.
    */
   public boolean isBrowse() {
      return browse;
   }

   /**
    * Returns whether this role can create addresses.
    */
   public boolean isCreateAddress() {
      return createAddress;
   }
}
