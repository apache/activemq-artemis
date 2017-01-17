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

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.postoffice.Address;

/**
 * splits an address string into its hierarchical parts split by '.'
 */
public class AddressImpl implements Address {

   private final SimpleString address;

   private final SimpleString[] addressParts;

   private final boolean containsWildCard;

   private final List<Address> linkedAddresses = new ArrayList<>();

   private final WildcardConfiguration wildcardConfiguration;

   public AddressImpl(final SimpleString address) {
      this(address, new WildcardConfiguration());
   }

   public AddressImpl(final SimpleString address, WildcardConfiguration wildcardConfiguration) {
      this.address = address;
      this.wildcardConfiguration = wildcardConfiguration;
      addressParts = address.split(wildcardConfiguration.getDelimiter());
      containsWildCard = address.contains(wildcardConfiguration.getSingleWord()) || address.contains(wildcardConfiguration.getAnyWords());
   }

   @Override
   public SimpleString getAddress() {
      return address;
   }

   @Override
   public SimpleString[] getAddressParts() {
      return addressParts;
   }

   @Override
   public boolean containsWildCard() {
      return containsWildCard;
   }

   @Override
   public List<Address> getLinkedAddresses() {
      return linkedAddresses;
   }

   @Override
   public void addLinkedAddress(final Address address) {
      if (!linkedAddresses.contains(address)) {
         linkedAddresses.add(address);
      }
   }

   @Override
   public void removeLinkedAddress(final Address actualAddress) {
      linkedAddresses.remove(actualAddress);
   }

   @Override
   public boolean matches(final Address add) {
      if (containsWildCard == add.containsWildCard()) {
         return address.equals(add.getAddress());
      }
      int pos = 0;
      int matchPos = 0;

      SimpleString nextToMatch;
      for (; matchPos < add.getAddressParts().length; ) {
         if (pos >= addressParts.length) {
            // test for # as last address part
            return pos + 1 == add.getAddressParts().length && add.getAddressParts()[pos].equals(new SimpleString(wildcardConfiguration.getAnyWords()));
         }
         SimpleString curr = addressParts[pos];
         SimpleString next = addressParts.length > pos + 1 ? addressParts[pos + 1] : null;
         SimpleString currMatch = add.getAddressParts()[matchPos];
         if (currMatch.equals(new SimpleString(wildcardConfiguration.getSingleWord()))) {
            pos++;
            matchPos++;
         } else if (currMatch.equals(new SimpleString(wildcardConfiguration.getAnyWords()))) {
            if (matchPos == addressParts.length - 1) {
               pos++;
               matchPos++;
            } else if (next == null) {
               return false;
            } else if (matchPos == add.getAddressParts().length - 1) {
               return true;
            } else {
               nextToMatch = add.getAddressParts()[matchPos + 1];
               while (curr != null) {
                  if (curr.equals(nextToMatch)) {
                     break;
                  }
                  pos++;
                  curr = next;
                  next = addressParts.length > pos + 1 ? addressParts[pos + 1] : null;
               }
               if (curr == null) {
                  return false;
               }
               matchPos++;
            }
         } else {
            if (!curr.equals(currMatch)) {
               return false;
            }
            pos++;
            matchPos++;
         }
      }
      return pos == addressParts.length;
   }

   @Override
   public boolean equals(final Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      AddressImpl address1 = (AddressImpl) o;

      if (!address.equals(address1.address)) {
         return false;
      }

      return true;
   }

   @Override
   public int hashCode() {
      return address.hashCode();
   }
}
