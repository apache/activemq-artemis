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
import java.util.List;
import java.util.Map;
import org.jctools.maps.NonBlockingHashMap;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.postoffice.Address;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.BindingsFactory;
import org.apache.activemq.artemis.core.transaction.Transaction;

/**
 * extends the simple manager to allow wildcard addresses to be used.
 */
public class WildcardAddressManager extends SimpleAddressManager {

   static final char SINGLE_WORD = '*';

   static final char ANY_WORDS = '#';

   static final char DELIM = '.';

   static final SimpleString SINGLE_WORD_SIMPLESTRING = new SimpleString("*");

   static final SimpleString ANY_WORDS_SIMPLESTRING = new SimpleString("#");

   /**
    * These are all the addresses, we use this so we can link back from the actual address to its linked wilcard addresses
    * or vice versa
    */
   private final Map<SimpleString, Address> addresses = new NonBlockingHashMap<>();

   private final Map<SimpleString, Address> wildCardAddresses = new NonBlockingHashMap<>();

   public WildcardAddressManager(final BindingsFactory bindingsFactory, final WildcardConfiguration wildcardConfiguration) {
      super(bindingsFactory, wildcardConfiguration);
   }

   public WildcardAddressManager(final BindingsFactory bindingsFactory) {
      super(bindingsFactory);
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
               for (Binding b : bindings.getBindings()) {
                  super.addMappingInternal(binding.getAddress(), b);
               }
            }
         }
      }
      return exists;
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
