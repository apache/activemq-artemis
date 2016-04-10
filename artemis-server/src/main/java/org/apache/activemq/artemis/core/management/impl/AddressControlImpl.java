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
package org.apache.activemq.artemis.core.management.impl;

import javax.management.MBeanOperationInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.utils.json.JSONArray;
import org.apache.activemq.artemis.utils.json.JSONObject;

public class AddressControlImpl extends AbstractControl implements AddressControl {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final SimpleString address;

   private final PostOffice postOffice;

   private final PagingManager pagingManager;

   private final HierarchicalRepository<Set<Role>> securityRepository;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AddressControlImpl(final SimpleString address,
                             final PostOffice postOffice,
                             final PagingManager pagingManager,
                             final StorageManager storageManager,
                             final HierarchicalRepository<Set<Role>> securityRepository) throws Exception {
      super(AddressControl.class, storageManager);
      this.address = address;
      this.postOffice = postOffice;
      this.pagingManager = pagingManager;
      this.securityRepository = securityRepository;
   }

   // Public --------------------------------------------------------

   // AddressControlMBean implementation ----------------------------

   @Override
   public String getAddress() {
      return address.toString();
   }

   @Override
   public String[] getQueueNames() throws Exception {
      clearIO();
      try {
         Bindings bindings = postOffice.getBindingsForAddress(address);
         List<String> queueNames = new ArrayList<>();
         for (Binding binding : bindings.getBindings()) {
            if (binding instanceof QueueBinding) {
               queueNames.add(binding.getUniqueName().toString());
            }
         }
         return queueNames.toArray(new String[queueNames.size()]);
      }
      catch (Throwable t) {
         throw new IllegalStateException(t.getMessage());
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getBindingNames() throws Exception {
      clearIO();
      try {
         Bindings bindings = postOffice.getBindingsForAddress(address);
         String[] bindingNames = new String[bindings.getBindings().size()];
         int i = 0;
         for (Binding binding : bindings.getBindings()) {
            bindingNames[i++] = binding.getUniqueName().toString();
         }
         return bindingNames;
      }
      catch (Throwable t) {
         throw new IllegalStateException(t.getMessage());
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   public Object[] getRoles() throws Exception {
      clearIO();
      try {
         Set<Role> roles = securityRepository.getMatch(address.toString());

         Object[] objRoles = new Object[roles.size()];

         int i = 0;
         for (Role role : roles) {
            objRoles[i++] = new Object[]{role.getName(), CheckType.SEND.hasRole(role), CheckType.CONSUME.hasRole(role), CheckType.CREATE_DURABLE_QUEUE.hasRole(role), CheckType.DELETE_DURABLE_QUEUE.hasRole(role), CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role), CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role), CheckType.MANAGE.hasRole(role)};
         }
         return objRoles;
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   public String getRolesAsJSON() throws Exception {
      clearIO();
      try {
         JSONArray json = new JSONArray();
         Set<Role> roles = securityRepository.getMatch(address.toString());

         for (Role role : roles) {
            json.put(new JSONObject(role));
         }
         return json.toString();
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   public long getNumberOfBytesPerPage() throws Exception {
      clearIO();
      try {
         return pagingManager.getPageStore(address).getPageSizeBytes();
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   public long getAddressSize() throws Exception {
      clearIO();
      try {
         return pagingManager.getPageStore(address).getAddressSize();
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   public long getNumberOfMessages() throws Exception {
      clearIO();
      long totalMsgs = 0;
      try {
         Bindings bindings = postOffice.getBindingsForAddress(address);
         for (Binding binding : bindings.getBindings()) {
            if (binding instanceof QueueBinding) {
               totalMsgs += ((QueueBinding) binding).getQueue().getMessageCount();
            }
         }
         return totalMsgs;
      }
      catch (Throwable t) {
         throw new IllegalStateException(t.getMessage());
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isPaging() throws Exception {
      clearIO();
      try {
         return pagingManager.getPageStore(address).isPaging();
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   public int getNumberOfPages() throws Exception {
      clearIO();
      try {
         PagingStore pageStore = pagingManager.getPageStore(address);

         if (!pageStore.isPaging()) {
            return 0;
         }
         else {
            return pagingManager.getPageStore(address).getNumberOfPages();
         }
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(AddressControl.class);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
