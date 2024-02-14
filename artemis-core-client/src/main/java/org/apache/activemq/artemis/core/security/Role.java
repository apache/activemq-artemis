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
package org.apache.activemq.artemis.core.security;

import org.apache.activemq.artemis.json.JsonObject;
import java.io.Serializable;

import org.apache.activemq.artemis.utils.JsonLoader;

/**
 * A role is used by the security store to define access rights and is configured on a connection factory or an address.
 */
public class Role implements Serializable {

   private static final long serialVersionUID = 3560097227776448872L;

   private String name;

   private boolean send;

   private boolean consume;

   private boolean createAddress;

   private boolean deleteAddress;

   private boolean createDurableQueue;

   private boolean deleteDurableQueue;

   private boolean createNonDurableQueue;

   private boolean deleteNonDurableQueue;

   private boolean manage;

   private boolean browse;

   private boolean view;

   private boolean edit;

   public JsonObject toJson() {
      return JsonLoader.createObjectBuilder().add("name", name).add("send", send).add("consume", consume).add("createDurableQueue", createDurableQueue).add("deleteDurableQueue", deleteDurableQueue).add("createNonDurableQueue", createNonDurableQueue).add("deleteNonDurableQueue", deleteNonDurableQueue).add("manage", manage)
         .add("browse", browse).add("createAddress", createAddress).add("deleteAddress", deleteAddress).add("view", view).add("edit", edit).build();
   }

   public Role() {
      // for properties config
   }

   /**
    * @param name
    * @param send
    * @param consume
    * @param createDurableQueue
    * @param deleteDurableQueue
    * @param createNonDurableQueue
    * @param deleteNonDurableQueue
    * @param manage
    * @deprecated Use {@link #Role(String, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean)}
    */
   @Deprecated
   public Role(final String name,
               final boolean send,
               final boolean consume,
               final boolean createDurableQueue,
               final boolean deleteDurableQueue,
               final boolean createNonDurableQueue,
               final boolean deleteNonDurableQueue,
               final boolean manage) {
      // This constructor exists for version compatibility on the API.
      // it will pass consume as browse
      this(name, send, consume, createDurableQueue, deleteDurableQueue, createNonDurableQueue, deleteNonDurableQueue, manage, consume);
   }

   @Deprecated
   public Role(final String name,
               final boolean send,
               final boolean consume,
               final boolean createDurableQueue,
               final boolean deleteDurableQueue,
               final boolean createNonDurableQueue,
               final boolean deleteNonDurableQueue,
               final boolean manage,
               final boolean browse) {
      // This constructor exists for version compatibility on the API. If either createDurableQueue or createNonDurableQueue
      // is true then createAddress will be true. If either deleteDurableQueue or deleteNonDurableQueue is true then deleteAddress will be true.
      this(name, send, consume, createDurableQueue, deleteDurableQueue, createNonDurableQueue, deleteNonDurableQueue, manage, browse, createDurableQueue || createNonDurableQueue, deleteDurableQueue || deleteNonDurableQueue, false, false);
   }

   @Deprecated
   public Role(final String name,
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
      this(name, send, consume, createDurableQueue, deleteDurableQueue, createNonDurableQueue, deleteNonDurableQueue, manage, browse, createAddress, deleteAddress, false, false);
   }

   public Role(final String name,
               final boolean send,
               final boolean consume,
               final boolean createDurableQueue,
               final boolean deleteDurableQueue,
               final boolean createNonDurableQueue,
               final boolean deleteNonDurableQueue,
               final boolean manage,
               final boolean browse,
               final boolean createAddress,
               final boolean deleteAddress,
               final boolean view,
               final boolean edit) {
      if (name == null) {
         throw new NullPointerException("name is null");
      }
      this.name = name;
      this.send = send;
      this.consume = consume;
      this.createAddress = createAddress;
      this.deleteAddress = deleteAddress;
      this.createDurableQueue = createDurableQueue;
      this.deleteDurableQueue = deleteDurableQueue;
      this.createNonDurableQueue = createNonDurableQueue;
      this.deleteNonDurableQueue = deleteNonDurableQueue;
      this.manage = manage;
      this.browse = browse;
      this.view = view;
      this.edit = edit;
   }

   public String getName() {
      return name;
   }

   public boolean isSend() {
      return send;
   }

   public boolean isConsume() {
      return consume;
   }

   public boolean isCreateAddress() {
      return createAddress;
   }

   public boolean isDeleteAddress() {
      return deleteAddress;
   }

   public boolean isCreateDurableQueue() {
      return createDurableQueue;
   }

   public boolean isDeleteDurableQueue() {
      return deleteDurableQueue;
   }

   public boolean isCreateNonDurableQueue() {
      return createNonDurableQueue;
   }

   public boolean isDeleteNonDurableQueue() {
      return deleteNonDurableQueue;
   }

   public boolean isManage() {
      return manage;
   }

   public boolean isBrowse() {
      return browse;
   }

   public void setName(String name) {
      this.name = name;
   }

   public void setSend(boolean send) {
      this.send = send;
   }

   public void setConsume(boolean consume) {
      this.consume = consume;
   }

   public void setCreateAddress(boolean createAddress) {
      this.createAddress = createAddress;
   }

   public void setDeleteAddress(boolean deleteAddress) {
      this.deleteAddress = deleteAddress;
   }

   public void setCreateDurableQueue(boolean createDurableQueue) {
      this.createDurableQueue = createDurableQueue;
   }

   public void setDeleteDurableQueue(boolean deleteDurableQueue) {
      this.deleteDurableQueue = deleteDurableQueue;
   }

   public void setCreateNonDurableQueue(boolean createNonDurableQueue) {
      this.createNonDurableQueue = createNonDurableQueue;
   }

   public void setDeleteNonDurableQueue(boolean deleteNonDurableQueue) {
      this.deleteNonDurableQueue = deleteNonDurableQueue;
   }

   public void setManage(boolean manage) {
      this.manage = manage;
   }

   public void setBrowse(boolean browse) {
      this.browse = browse;
   }

   @Override
   public String toString() {
      StringBuffer stringReturn = new StringBuffer("Role {name=" + name + "; allows=[");

      if (send) {
         stringReturn.append(" send ");
      }
      if (consume) {
         stringReturn.append(" consume ");
      }
      if (createAddress) {
         stringReturn.append(" createAddress ");
      }
      if (deleteAddress) {
         stringReturn.append(" deleteAddress ");
      }
      if (createDurableQueue) {
         stringReturn.append(" createDurableQueue ");
      }
      if (deleteDurableQueue) {
         stringReturn.append(" deleteDurableQueue ");
      }
      if (createNonDurableQueue) {
         stringReturn.append(" createNonDurableQueue ");
      }
      if (deleteNonDurableQueue) {
         stringReturn.append(" deleteNonDurableQueue ");
      }
      if (manage) {
         stringReturn.append(" manage ");
      }
      if (browse) {
         stringReturn.append(" browse ");
      }
      if (view) {
         stringReturn.append(" view ");
      }
      if (edit) {
         stringReturn.append(" edit ");
      }
      stringReturn.append("]}");

      return stringReturn.toString();
   }

   @Override
   public boolean equals(final Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      Role role = (Role) o;

      if (consume != role.consume) {
         return false;
      }
      if (createAddress != role.createAddress) {
         return false;
      }
      if (deleteAddress != role.deleteAddress) {
         return false;
      }
      if (createDurableQueue != role.createDurableQueue) {
         return false;
      }
      if (createNonDurableQueue != role.createNonDurableQueue) {
         return false;
      }
      if (deleteDurableQueue != role.deleteDurableQueue) {
         return false;
      }
      if (deleteNonDurableQueue != role.deleteNonDurableQueue) {
         return false;
      }
      if (send != role.send) {
         return false;
      }
      if (manage != role.manage) {
         return false;
      }
      if (browse != role.browse) {
         return false;
      }
      if (!name.equals(role.name)) {
         return false;
      }
      if (view != role.view) {
         return false;
      }
      if (edit != role.edit) {
         return false;
      }

      return true;
   }

   @Override
   public int hashCode() {
      int result;
      result = name.hashCode();
      result = 31 * result + (send ? 1 : 0);
      result = 31 * result + (consume ? 1 : 0);
      result = 31 * result + (createAddress ? 1 : 0);
      result = 31 * result + (deleteAddress ? 1 : 0);
      result = 31 * result + (createDurableQueue ? 1 : 0);
      result = 31 * result + (deleteDurableQueue ? 1 : 0);
      result = 31 * result + (createNonDurableQueue ? 1 : 0);
      result = 31 * result + (deleteNonDurableQueue ? 1 : 0);
      result = 31 * result + (manage ? 1 : 0);
      result = 31 * result + (browse ? 1 : 0);
      result = 31 * result + (view ? 1 : 0);
      result = 31 * result + (edit ? 1 : 0);
      return result;
   }

   public void merge(Role other) {
      send = send || other.send;
      consume = consume || other.consume;
      createAddress = createAddress || other.createAddress;
      deleteAddress = deleteAddress || other.deleteAddress;
      createDurableQueue = createDurableQueue || other.createDurableQueue;
      deleteDurableQueue = deleteDurableQueue || other.deleteDurableQueue;
      createNonDurableQueue = createNonDurableQueue || other.createNonDurableQueue;
      deleteNonDurableQueue = deleteNonDurableQueue || other.deleteNonDurableQueue;
      manage = manage || other.manage;
      browse = browse || other.browse;
      view = view || other.view;
      edit = edit || other.edit;
   }

   public boolean isEdit() {
      return edit;
   }

   public void setEdit(boolean edit) {
      this.edit = edit;
   }

   public boolean isView() {
      return view;
   }

   public void setView(boolean view) {
      this.view = view;
   }
}
