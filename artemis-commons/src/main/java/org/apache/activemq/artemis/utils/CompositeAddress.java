/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.utils;

import org.apache.activemq.artemis.api.core.SimpleString;

public class CompositeAddress {

   public static final String SEPARATOR = "::";

   public static final int SEPARATOR_LENGTH = SEPARATOR.length();

   public static String toFullyQualified(String address, String qName) {
      return toFullyQualified(SimpleString.toSimpleString(address), SimpleString.toSimpleString(qName)).toString();
   }

   public static SimpleString toFullyQualified(SimpleString address, SimpleString qName) {
      SimpleString result;
      if (address == null && qName == null) {
         result = null;
      } else if (address != null && qName == null) {
         result = address;
      } else if (address == null && qName != null) {
         result = qName;
      } else {
         result = address.concat(SEPARATOR).concat(qName);
      }

      return result;
   }

   /**
    * Checks whether a given string complies the fully qualified queue name (FQQN) standard or not.
    *
    * @param address The string to check for FQQN standard compliance.
    * @return boolean
    */
   public static boolean isFullyQualified(String address) {
      if (address == null) {
         return false;
      }

      // If address length is incapable of holding the separator and two more characters
      if (address.length() < SEPARATOR_LENGTH + 2) {
         return false;
      }

      // If separator is not found or is at the start or end of string then this is not a FQQN
      int index = address.indexOf(SEPARATOR, 1);
      return index != -1 && index != address.length() - SEPARATOR_LENGTH;
   }

   /**
    * Checks whether a given SimpleString complies the fully qualified queue name (FQQN) standard or not.
    *
    * @param address The string to check for FQQN standard compliance.
    * @return boolean
    */
   public static boolean isFullyQualified(SimpleString address) {
      return address != null && isFullyQualified(address.toString());
   }

   public static SimpleString extractQueueName(SimpleString name) {
      if (name == null) {
         return null;
      }
      final String nameString = name.toString();
      final String queueName = extractQueueName(nameString);
      if (queueName.equals(nameString)) {
         return name;
      }
      return new SimpleString(queueName);
   }

   public static String extractQueueName(String queue) {
      if (queue == null) {
         return null;
      }
      int index = queue.indexOf(SEPARATOR);
      if (index != -1) {
         return queue.substring(index + SEPARATOR.length());
      }
      return queue;
   }

   public static SimpleString extractAddressName(SimpleString address) {
      if (address == null) {
         return null;
      }
      final String addrString = address.toString();
      final String addressName = extractAddressName(addrString);
      if (addressName.equals(addrString)) {
         return address;
      }
      return new SimpleString(addressName);
   }

   public static String extractAddressName(String address) {
      if (address == null) {
         return null;
      }
      int index = address.indexOf(SEPARATOR);
      if (index != -1) {
         return address.substring(0, index);
      }
      return address;
   }
}
