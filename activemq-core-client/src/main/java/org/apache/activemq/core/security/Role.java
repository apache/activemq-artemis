/**
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
package org.apache.activemq.core.security;

import java.io.Serializable;

/**
 * A role is used by the security store to define access rights and is configured on a connection factory or an address.
 */
public class Role implements Serializable
{
   private static final long serialVersionUID = 3560097227776448872L;

   private final String name;

   private final boolean send;

   private final boolean consume;

   private final boolean createDurableQueue;

   private final boolean deleteDurableQueue;

   private final boolean createNonDurableQueue;

   private final boolean deleteNonDurableQueue;

   private final boolean manage;

   public Role(final String name,
               final boolean send,
               final boolean consume,
               final boolean createDurableQueue,
               final boolean deleteDurableQueue,
               final boolean createNonDurableQueue,
               final boolean deleteNonDurableQueue,
               final boolean manage)
   {
      if (name == null)
      {
         throw new NullPointerException("name is null");
      }
      this.name = name;
      this.send = send;
      this.consume = consume;
      this.createDurableQueue = createDurableQueue;
      this.deleteDurableQueue = deleteDurableQueue;
      this.createNonDurableQueue = createNonDurableQueue;
      this.deleteNonDurableQueue = deleteNonDurableQueue;
      this.manage = manage;
   }

   public String getName()
   {
      return name;
   }

   public boolean isSend()
   {
      return send;
   }

   public boolean isConsume()
   {
      return consume;
   }

   public boolean isCreateDurableQueue()
   {
      return createDurableQueue;
   }

   public boolean isDeleteDurableQueue()
   {
      return deleteDurableQueue;
   }

   public boolean isCreateNonDurableQueue()
   {
      return createNonDurableQueue;
   }

   public boolean isDeleteNonDurableQueue()
   {
      return deleteNonDurableQueue;
   }

   @Override
   public String toString()
   {
      StringBuffer stringReturn = new StringBuffer("Role {name=" + name + "; allows=[");

      if (send)
      {
         stringReturn.append(" send ");
      }
      if (consume)
      {
         stringReturn.append(" consume ");
      }
      if (createDurableQueue)
      {
         stringReturn.append(" createDurableQueue ");
      }
      if (deleteDurableQueue)
      {
         stringReturn.append(" deleteDurableQueue ");
      }
      if (createNonDurableQueue)
      {
         stringReturn.append(" createNonDurableQueue ");
      }
      if (deleteNonDurableQueue)
      {
         stringReturn.append(" deleteNonDurableQueue ");
      }

      stringReturn.append("]}");

      return stringReturn.toString();
   }

   @Override
   public boolean equals(final Object o)
   {
      if (this == o)
      {
         return true;
      }
      if (o == null || getClass() != o.getClass())
      {
         return false;
      }

      Role role = (Role)o;

      if (consume != role.consume)
      {
         return false;
      }
      if (createDurableQueue != role.createDurableQueue)
      {
         return false;
      }
      if (createNonDurableQueue != role.createNonDurableQueue)
      {
         return false;
      }
      if (deleteDurableQueue != role.deleteDurableQueue)
      {
         return false;
      }
      if (deleteNonDurableQueue != role.deleteNonDurableQueue)
      {
         return false;
      }
      if (send != role.send)
      {
         return false;
      }
      if (!name.equals(role.name))
      {
         return false;
      }

      return true;
   }

   @Override
   public int hashCode()
   {
      int result;
      result = name.hashCode();
      result = 31 * result + (send ? 1 : 0);
      result = 31 * result + (consume ? 1 : 0);
      result = 31 * result + (createDurableQueue ? 1 : 0);
      result = 31 * result + (deleteDurableQueue ? 1 : 0);
      result = 31 * result + (createNonDurableQueue ? 1 : 0);
      result = 31 * result + (deleteNonDurableQueue ? 1 : 0);
      return result;
   }

   public boolean isManage()
   {
      return manage;
   }
}
