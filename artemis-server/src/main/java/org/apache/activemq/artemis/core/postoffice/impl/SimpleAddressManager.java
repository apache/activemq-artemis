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
package org.apache.activemq.artemis.core.postoffice.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Address;
import org.apache.activemq.artemis.core.postoffice.AddressManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.BindingsFactory;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.jboss.logging.Logger;

/**
 * A simple address manager that maintains the addresses and bindings.
 */
public class SimpleAddressManager implements AddressManager {

   private static final Logger logger = Logger.getLogger(SimpleAddressManager.class);

   private final ConcurrentMap<SimpleString, AddressInfo> addressInfoMap = new ConcurrentHashMap<>();

   private final StorageManager storageManager;

   /**
    * HashMap<Address, Binding>
    */
   protected final ConcurrentMap<SimpleString, Bindings> mappings = new ConcurrentHashMap<>();

   /**
    * HashMap<QueueName, Binding>
    */
   private final ConcurrentMap<SimpleString, Binding> nameMap = new ConcurrentHashMap<>();

   private final BindingsFactory bindingsFactory;

   protected final WildcardConfiguration wildcardConfiguration;

   public SimpleAddressManager(final BindingsFactory bindingsFactory, final StorageManager storageManager) {
      this(bindingsFactory, new WildcardConfiguration(), storageManager);
   }

   public SimpleAddressManager(final BindingsFactory bindingsFactory,
                               final WildcardConfiguration wildcardConfiguration,
                               final StorageManager storageManager) {
      this.wildcardConfiguration = wildcardConfiguration;
      this.bindingsFactory = bindingsFactory;
      this.storageManager = storageManager;
   }

   @Override
   public boolean addBinding(final Binding binding) throws Exception {
      if (nameMap.putIfAbsent(binding.getUniqueName(), binding) != null) {
         throw ActiveMQMessageBundle.BUNDLE.bindingAlreadyExists(binding);
      }

      if (logger.isTraceEnabled()) {
         logger.trace("Adding binding " + binding + " with address = " + binding.getUniqueName(), new Exception("trace"));
      }

      return addMappingInternal(binding.getAddress(), binding);
   }

   @Override
   public Binding removeBinding(final SimpleString uniqueName, Transaction tx) throws Exception {
      final Binding binding = nameMap.remove(uniqueName);

      if (binding == null) {
         return null;
      }

      removeBindingInternal(binding.getAddress(), uniqueName);

      return binding;
   }

   @Override
   public Bindings getBindingsForRoutingAddress(final SimpleString address) throws Exception {
      return mappings.get(address);
   }

   @Override
   public Binding getBinding(final SimpleString bindableName) {
      return nameMap.get(CompositeAddress.extractQueueName(bindableName));
   }

   @Override
   public Map<SimpleString, Binding> getBindings() {
      return nameMap;
   }

   @Override
   public Bindings getMatchingBindings(final SimpleString address) throws Exception {
      Address add = new AddressImpl(address, wildcardConfiguration);

      Bindings bindings = bindingsFactory.createBindings(address);

      for (Binding binding : nameMap.values()) {
         Address addCheck = new AddressImpl(binding.getAddress(), wildcardConfiguration);

         if (addCheck.matches(add)) {
            bindings.addBinding(binding);
         }
      }

      return bindings;
   }

   @Override
   public Bindings getDirectBindings(final SimpleString address) throws Exception {
      Bindings bindings = bindingsFactory.createBindings(address);

      for (Binding binding : nameMap.values()) {
         if (binding.getAddress().equals(address)) {
            bindings.addBinding(binding);
         }
      }

      return bindings;
   }

   @Override
   public SimpleString getMatchingQueue(final SimpleString address, RoutingType routingType) throws Exception {

      Binding binding = getBinding(address);

      if (binding == null || !(binding instanceof LocalQueueBinding) || !binding.getAddress().equals(address)) {
         Bindings bindings = mappings.get(address);
         if (bindings != null) {
            for (Binding theBinding : bindings.getBindings()) {
               if (theBinding instanceof LocalQueueBinding) {
                  binding = theBinding;
                  break;
               }
            }
         }
      }

      return binding != null ? binding.getUniqueName() : null;
   }

   @Override
   public SimpleString getMatchingQueue(final SimpleString address,
                                        final SimpleString queueName,
                                        RoutingType routingType) throws Exception {
      Binding binding = getBinding(queueName);

      if (binding != null && !binding.getAddress().equals(address) && !address.toString().isEmpty()) {
         throw new IllegalStateException("queue belongs to address" + binding.getAddress());
      }
      return binding != null ? binding.getUniqueName() : null;
   }

   @Override
   public void clear() {
      nameMap.clear();
      mappings.clear();
   }

   @Override
   public Set<SimpleString> getAddresses() {
      Set<SimpleString> addresses = new HashSet<>();
      addresses.addAll(addressInfoMap.keySet());
      return addresses;
   }

   protected void removeBindingInternal(final SimpleString address, final SimpleString bindableName) {
      Bindings bindings = mappings.get(address);

      if (bindings != null) {
         removeMapping(bindableName, bindings);

         if (bindings.getBindings().isEmpty()) {
            mappings.remove(address);
         }
      }
   }

   protected Binding removeMapping(final SimpleString bindableName, final Bindings bindings) {
      Binding theBinding = null;

      for (Binding binding : bindings.getBindings()) {
         if (binding.getUniqueName().equals(CompositeAddress.extractQueueName(bindableName))) {
            theBinding = binding;
            break;
         }
      }

      if (theBinding == null) {
         throw new IllegalStateException("Cannot find binding " + bindableName);
      }

      bindings.removeBinding(theBinding);

      return theBinding;
   }

   protected boolean addMappingInternal(final SimpleString address, final Binding binding) throws Exception {
      Bindings bindings = mappings.get(address);

      Bindings prevBindings = null;

      if (bindings == null) {
         bindings = bindingsFactory.createBindings(address);

         prevBindings = mappings.putIfAbsent(address, bindings);

         if (prevBindings != null) {
            bindings = prevBindings;
         }
      }

      bindings.addBinding(binding);

      return prevBindings != null;
   }

   @Override
   public boolean reloadAddressInfo(AddressInfo addressInfo) throws Exception {
      return addressInfoMap.putIfAbsent(addressInfo.getName(), addressInfo) == null;
   }

   @Override
   public boolean addAddressInfo(AddressInfo addressInfo) throws Exception {
      boolean added = reloadAddressInfo(addressInfo);
      if (added && storageManager != null) {
         long txID = storageManager.generateID();
         try {
            storageManager.addAddressBinding(txID, addressInfo);
            storageManager.commitBindings(txID);
         } catch (Exception e) {
            try {
               storageManager.rollbackBindings(txID);
            } catch (Exception ignored) {
            }
            throw e;
         }
      }
      return added;
   }

   @Override
   public AddressInfo updateAddressInfo(SimpleString addressName,
                                        EnumSet<RoutingType> routingTypes) throws Exception {

      AddressInfo info = addressInfoMap.get(addressName);

      if (info == null) {
         throw ActiveMQMessageBundle.BUNDLE.addressDoesNotExist(addressName);
      }

      if (routingTypes == null || isEquals(routingTypes, info.getRoutingTypes())) {
         // there are no changes.. we just give up now
         return info;
      }

      validateRoutingTypes(addressName, routingTypes);
      final EnumSet<RoutingType> updatedRoutingTypes = EnumSet.copyOf(routingTypes);
      info.setRoutingTypes(updatedRoutingTypes);


      if (storageManager != null) {
         //it change the address info without any lock!
         final long txID = storageManager.generateID();
         try {
            storageManager.deleteAddressBinding(txID, info.getId());
            storageManager.addAddressBinding(txID, info);
            storageManager.commitBindings(txID);
         } catch (Exception e) {
            try {
               storageManager.rollbackBindings(txID);
            } catch (Throwable ignored) {
            }
            throw e;
         }
      }
      return info;
   }

   private boolean isEquals(Collection<RoutingType> set1, EnumSet<RoutingType> set2) {
      Set<RoutingType> eset1 = set1 == null || set1.isEmpty() ? Collections.emptySet() : EnumSet.copyOf(set1);
      Set<RoutingType> eset2 = set2 == null || set2.isEmpty() ? Collections.emptySet() : EnumSet.copyOf(set2);

      if (eset1.size() == 0 && eset2.size() == 0) {
         return true;
      }

      if (eset1.size() != eset2.size()) {
         return false;
      }

      return eset2.containsAll(eset1);
   }

   private void validateRoutingTypes(SimpleString addressName, EnumSet<RoutingType> routingTypes) {
      final Bindings bindings = this.mappings.get(addressName);
      if (bindings != null) {
         for (Binding binding : bindings.getBindings()) {
            if (binding instanceof QueueBinding) {
               final QueueBinding queueBinding = (QueueBinding) binding;
               final RoutingType routingType = queueBinding.getQueue().getRoutingType();
               if (!routingTypes.contains(routingType) && binding.getAddress().equals(addressName)) {
                  throw ActiveMQMessageBundle.BUNDLE.invalidRoutingTypeDelete(routingType, addressName.toString());
               }
            }
         }
      }
   }

   @Override
   public AddressInfo removeAddressInfo(SimpleString address) throws Exception {
      return addressInfoMap.remove(address);
   }

   @Override
   public AddressInfo getAddressInfo(SimpleString addressName) {
      return addressInfoMap.get(addressName);
   }

   @Override
   public void updateMessageLoadBalancingTypeForAddress(SimpleString  address, MessageLoadBalancingType messageLoadBalancingType) throws Exception {
      getBindingsForRoutingAddress(address).setMessageLoadBalancingType(messageLoadBalancingType);
   }
}
