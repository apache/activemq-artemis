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

import javax.json.JsonArrayBuilder;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.utils.JsonLoader;

public class AddressControlImpl extends AbstractControl implements AddressControl {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private AddressInfo addressInfo;

   private final ActiveMQServer server;

   private final PagingManager pagingManager;

   private final HierarchicalRepository<Set<Role>> securityRepository;

   private final SecurityStore securityStore;

   private final ManagementService managementService;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AddressControlImpl(AddressInfo addressInfo,
                             final ActiveMQServer server,
                             final PagingManager pagingManager,
                             final StorageManager storageManager,
                             final HierarchicalRepository<Set<Role>> securityRepository,
                             final SecurityStore securityStore,
                             final ManagementService managementService)throws Exception {
      super(AddressControl.class, storageManager);
      this.server = server;
      this.addressInfo = addressInfo;
      this.pagingManager = pagingManager;
      this.securityRepository = securityRepository;
      this.securityStore = securityStore;
      this.managementService = managementService;
   }

   // Public --------------------------------------------------------

   // AddressControlMBean implementation ----------------------------

   @Override
   public String getAddress() {
      return addressInfo.getName().toString();
   }

   @Override
   public String[] getRoutingTypes() {
      EnumSet<RoutingType> routingTypes = addressInfo.getRoutingTypes();
      String[] result = new String[routingTypes.size()];
      int i = 0;
      for (RoutingType routingType : routingTypes) {
         result[i++] = routingType.toString();
      }
      return result;
   }

   @Override
   public String getRoutingTypesAsJSON() throws Exception {
      clearIO();
      try {
         JsonArrayBuilder json = JsonLoader.createArrayBuilder();
         String[] routingTypes = getRoutingTypes();

         for (String routingType : routingTypes) {
            json.add(routingType);
         }
         return json.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getQueueNames() throws Exception {
      clearIO();
      try {
         Bindings bindings = server.getPostOffice().lookupBindingsForAddress(addressInfo.getName());
         if (bindings != null) {
            List<String> queueNames = new ArrayList<>();
            for (Binding binding : bindings.getBindings()) {
               if (binding instanceof QueueBinding) {
                  queueNames.add(binding.getUniqueName().toString());
               }
            }
            return queueNames.toArray(new String[queueNames.size()]);
         } else {
            return new String[0];
         }
      } catch (Throwable t) {
         throw new IllegalStateException(t.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getBindingNames() throws Exception {
      clearIO();
      try {
         Bindings bindings = server.getPostOffice().lookupBindingsForAddress(addressInfo.getName());
         if (bindings != null) {
            String[] bindingNames = new String[bindings.getBindings().size()];
            int i = 0;
            for (Binding binding : bindings.getBindings()) {
               bindingNames[i++] = binding.getUniqueName().toString();
            }
            return bindingNames;
         } else {
            return new String[0];
         }
      } catch (Throwable t) {
         throw new IllegalStateException(t.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Object[] getRoles() throws Exception {
      clearIO();
      try {
         Set<Role> roles = securityRepository.getMatch(addressInfo.getName().toString());

         Object[] objRoles = new Object[roles.size()];

         int i = 0;
         for (Role role : roles) {
            objRoles[i++] = new Object[]{role.getName(), CheckType.SEND.hasRole(role), CheckType.CONSUME.hasRole(role), CheckType.CREATE_DURABLE_QUEUE.hasRole(role), CheckType.DELETE_DURABLE_QUEUE.hasRole(role), CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role), CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role), CheckType.MANAGE.hasRole(role)};
         }
         return objRoles;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getRolesAsJSON() throws Exception {
      clearIO();
      try {
         JsonArrayBuilder json = JsonLoader.createArrayBuilder();
         Set<Role> roles = securityRepository.getMatch(addressInfo.getName().toString());

         for (Role role : roles) {
            json.add(role.toJson());
         }
         return json.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getNumberOfBytesPerPage() throws Exception {
      clearIO();
      try {
         final PagingStore pagingStore = getPagingStore();
         if (pagingStore == null) {
            return 0;
         }
         return pagingStore.getPageSizeBytes();
      } finally {
         blockOnIO();
      }
   }

   private PagingStore getPagingStore() throws Exception {
      return pagingManager.getPageStore(addressInfo.getName());
   }

   @Override
   public long getAddressSize() throws Exception {
      clearIO();
      try {
         final PagingStore pagingStore = getPagingStore();
         if (pagingStore == null) {
            return 0;
         }
         return pagingStore.getAddressSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getNumberOfMessages() throws Exception {
      clearIO();
      long totalMsgs = 0;
      try {
         Bindings bindings = server.getPostOffice().lookupBindingsForAddress(addressInfo.getName());
         if (bindings != null) {
            for (Binding binding : bindings.getBindings()) {
               if (binding instanceof QueueBinding) {
                  totalMsgs += ((QueueBinding) binding).getQueue().getMessageCount();
               }
            }
         }
         return totalMsgs;
      } catch (Throwable t) {
         throw new IllegalStateException(t.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isPaging() throws Exception {
      clearIO();
      try {
         final PagingStore pagingStore = getPagingStore();
         if (pagingStore == null) {
            return false;
         }
         return pagingStore.isPaging();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getNumberOfPages() throws Exception {
      clearIO();
      try {
         final PagingStore pageStore = getPagingStore();

         if (pageStore == null || !pageStore.isPaging()) {
            return 0;
         } else {
            return pageStore.getNumberOfPages();
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessageCount() {
      return getMessageCount(DurabilityType.ALL);
   }

   @Override
   public long getRoutedMessageCount() {
      return addressInfo.getRoutedMessageCount();
   }

   @Override
   public long getUnRoutedMessageCount() {
      return addressInfo.getUnRoutedMessageCount();
   }


   @Override
   public String sendMessage(final Map<String, String> headers,
                             final int type,
                             final String body,
                             boolean durable,
                             final String user,
                             final String password) throws Exception {
      try {
         return sendMessage(addressInfo.getName(), server, headers, type, body, durable, user, password);
      } catch (Exception e) {
         throw new IllegalStateException(e.getMessage());
      }
   }


   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(AddressControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(AddressControl.class);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private long getMessageCount(final DurabilityType durability) {
      List<QueueControl> queues = getQueues(durability);
      long count = 0;
      for (QueueControl queue : queues) {
         count += queue.getMessageCount();
      }
      return count;
   }

   private List<QueueControl> getQueues(final DurabilityType durability) {
      try {
         List<QueueControl> matchingQueues = new ArrayList<>();
         String[] queues = getQueueNames();
         for (String queue : queues) {
            QueueControl coreQueueControl = (QueueControl) managementService.getResource(ResourceNames.QUEUE + queue);

            // Ignore the "special" subscription
            if (coreQueueControl != null) {
               if (durability == DurabilityType.ALL || durability == DurabilityType.DURABLE && coreQueueControl.isDurable() ||
                     durability == DurabilityType.NON_DURABLE && !coreQueueControl.isDurable()) {
                  matchingQueues.add(coreQueueControl);
               }
            }
         }
         return matchingQueues;
      } catch (Exception e) {
         return Collections.emptyList();
      }
   }

   // Inner classes -------------------------------------------------

   private enum DurabilityType {
      ALL, DURABLE, NON_DURABLE
   }
}
