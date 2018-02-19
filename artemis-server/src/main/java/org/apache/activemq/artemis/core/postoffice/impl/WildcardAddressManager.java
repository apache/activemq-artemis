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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Address;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.BindingsFactory;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.transaction.Transaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * extends the simple manager to allow wildcard addresses to be used.
 */
public class WildcardAddressManager extends SimpleAddressManager {

   /**
    * These are all the addresses, we use this so we can link back from the actual address to its linked wildcard addresses
    * or vice versa
    */
   private final Map<SimpleString, Address> addresses = new ConcurrentHashMap<>();

   private final Map<SimpleString, Address> wildCardAddresses = new ConcurrentHashMap<>();

   public WildcardAddressManager(final BindingsFactory bindingsFactory, final WildcardConfiguration wildcardConfiguration, final
                                 StorageManager storageManager) {
      super(bindingsFactory, wildcardConfiguration, storageManager);
   }

   public WildcardAddressManager(final BindingsFactory bindingsFactory, StorageManager storageManager) {
      super(bindingsFactory, storageManager);
   }

   @Override
   public Bindings getBindingsForRoutingAddress(final SimpleString address) throws Exception {
      Bindings bindings = super.getBindingsForRoutingAddress(address);

      // this should only happen if we're routing to an address that has no mappings when we're running checkAllowable
      if (bindings == null && !wildCardAddresses.isEmpty()) {
         Address add = addAndUpdateAddressMap(address);
         if (!add.containsWildCard()) {
            for (Address destAdd : add.getLinkedAddresses()) {
               Bindings b = super.getBindingsForRoutingAddress(destAdd.getAddress());
               if (b != null) {
                  Collection<Binding> theBindings = b.getBindings();
                  for (Binding theBinding : theBindings) {
                     super.addMappingInternal(address, theBinding);
                  }
                  super.getBindingsForRoutingAddress(address).setMessageLoadBalancingType(b.getMessageLoadBalancingType());
               }
            }
         }
         bindings = super.getBindingsForRoutingAddress(address);
      }
      return bindings;
   }

   /**
    * If the address to add the binding to contains a wildcard then a copy of the binding (with the same underlying queue)
    * will be added to the actual mappings. Otherwise the binding is added as normal.
    *
    * @param binding the binding to add
    * @return true if the address was a new mapping
    */
   @Override
   public boolean addBinding(final Binding binding) throws Exception {
      boolean exists = super.addBinding(binding);
      if (!exists) {
         Address add = addAndUpdateAddressMap(binding.getAddress());
         if (add.containsWildCard()) {
            for (Address destAdd : add.getLinkedAddresses()) {
               super.addMappingInternal(destAdd.getAddress(), binding);
            }
         } else {
            for (Address destAdd : add.getLinkedAddresses()) {
               Bindings bindings = super.getBindingsForRoutingAddress(destAdd.getAddress());
               if (bindings != null) {
                  for (Binding b : bindings.getBindings()) {
                     super.addMappingInternal(binding.getAddress(), b);
                  }
               }
            }
         }
      }
      return exists;
   }

   @Override
   public void updateMessageLoadBalancingTypeForAddress(SimpleString address, MessageLoadBalancingType messageLoadBalancingType) throws Exception {
      Address add = addAndUpdateAddressMap(address);
      Bindings bindingsForRoutingAddress = super.getBindingsForRoutingAddress(address);
      if (bindingsForRoutingAddress != null) {
         bindingsForRoutingAddress.setMessageLoadBalancingType(messageLoadBalancingType);
      }
      if (add.containsWildCard()) {
         for (Address destAdd : add.getLinkedAddresses()) {
            getBindingsForRoutingAddress(destAdd.getAddress()).setMessageLoadBalancingType(messageLoadBalancingType);
         }
      } else {
         for (Address destAdd : add.getLinkedAddresses()) {
            super.getBindingsForRoutingAddress(destAdd.getAddress()).setMessageLoadBalancingType(messageLoadBalancingType);
         }
      }
   }

   /**
    * If the address is a wild card then the binding will be removed from the actual mappings for any linked address.
    * otherwise it will be removed as normal.
    *
    * @param uniqueName the name of the binding to remove
    * @return true if this was the last mapping for a specific address
    */
   @Override
   public Binding removeBinding(final SimpleString uniqueName, Transaction tx) throws Exception {
      Binding binding = super.removeBinding(uniqueName, tx);
      if (binding != null) {
         Address add = getAddress(binding.getAddress());
         if (add.containsWildCard()) {
            for (Address theAddress : add.getLinkedAddresses()) {
               super.removeBindingInternal(theAddress.getAddress(), uniqueName);
            }
         }
         removeAndUpdateAddressMap(add);
      }
      return binding;
   }

   @Override
   public AddressInfo removeAddressInfo(SimpleString address) throws Exception {
      final AddressInfo removed = super.removeAddressInfo(address);
      if (removed != null) {
         //Remove from mappings so removeAndUpdateAddressMap processes and cleanup
         mappings.remove(address);
         removeAndUpdateAddressMap(new AddressImpl(removed.getName(), wildcardConfiguration));
      }
      return removed;
   }

   @Override
   public void clear() {
      super.clear();
      addresses.clear();
      wildCardAddresses.clear();
   }

   private Address getAddress(final SimpleString address) {
      Address add = new AddressImpl(address, wildcardConfiguration);
      Address actualAddress;
      if (add.containsWildCard()) {
         actualAddress = wildCardAddresses.get(address);
      } else {
         actualAddress = addresses.get(address);
      }
      return actualAddress != null ? actualAddress : add;
   }

   private synchronized Address addAndUpdateAddressMap(final SimpleString address) {
      Address add = new AddressImpl(address, wildcardConfiguration);
      Address actualAddress;
      if (add.containsWildCard()) {
         actualAddress = wildCardAddresses.get(address);
      } else {
         actualAddress = addresses.get(address);
      }
      if (actualAddress == null) {
         actualAddress = add;
         addAddress(address, actualAddress);
      }
      if (actualAddress.containsWildCard()) {
         for (Address destAdd : addresses.values()) {
            if (destAdd.matches(actualAddress)) {
               destAdd.addLinkedAddress(actualAddress);
               actualAddress.addLinkedAddress(destAdd);
            }
         }
      } else {
         for (Address destAdd : wildCardAddresses.values()) {
            if (actualAddress.matches(destAdd)) {
               destAdd.addLinkedAddress(actualAddress);
               actualAddress.addLinkedAddress(destAdd);
            }
         }
      }
      return actualAddress;
   }

   private void addAddress(final SimpleString address, final Address actualAddress) {
      if (actualAddress.containsWildCard()) {
         wildCardAddresses.put(address, actualAddress);
      } else {
         addresses.put(address, actualAddress);
      }
   }

   private synchronized void removeAndUpdateAddressMap(final Address address) throws Exception {
      // we only remove if there are no bindings left
      Bindings bindings = super.getBindingsForRoutingAddress(address.getAddress());
      if (bindings == null || bindings.getBindings().size() == 0) {
         List<Address> addresses = address.getLinkedAddresses();
         for (Address address1 : addresses) {
            address1.removeLinkedAddress(address);
            Bindings linkedBindings = super.getBindingsForRoutingAddress(address1.getAddress());
            if (linkedBindings == null || linkedBindings.getBindings().size() == 0) {
               removeAddress(address1);
            }
         }
         removeAddress(address);
      }
   }

   private void removeAddress(final Address add) {
      if (add.containsWildCard()) {
         wildCardAddresses.remove(add.getAddress());
      } else {
         addresses.remove(add.getAddress());
      }
   }
}
