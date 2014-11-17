/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.core.security;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 * @version $Revision: 2925 $
 *          <p/>
 *          $Id: $
 */
public enum CheckType
{
   SEND
   {
      @Override
      public boolean hasRole(final Role role)
      {
         return role.isSend();
      }
   },
   CONSUME
   {
      @Override
      public boolean hasRole(final Role role)
      {
         return role.isConsume();
      }
   },
   CREATE_DURABLE_QUEUE
   {
      @Override
      public boolean hasRole(final Role role)
      {
         return role.isCreateDurableQueue();
      }
   },
   DELETE_DURABLE_QUEUE
   {
      @Override
      public boolean hasRole(final Role role)
      {
         return role.isDeleteDurableQueue();
      }
   },
   CREATE_NON_DURABLE_QUEUE
   {
      @Override
      public boolean hasRole(final Role role)
      {
         return role.isCreateNonDurableQueue();
      }
   },
   DELETE_NON_DURABLE_QUEUE
   {
      @Override
      public boolean hasRole(final Role role)
      {
         return role.isDeleteNonDurableQueue();
      }
   },
   MANAGE
   {
      @Override
      public boolean hasRole(final Role role)
      {
         return role.isManage();
      }
   };

   public abstract boolean hasRole(final Role role);
}
