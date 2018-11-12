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

   public static String SEPARATOR = "::";

   public static String toFullyQualified(String address, String qName) {
      return new StringBuilder().append(address).append(SEPARATOR).append(qName).toString();
   }

   public static SimpleString toFullyQualified(SimpleString address, SimpleString qName) {
      return address.concat(SEPARATOR).concat(qName);
   }

   public static boolean isFullyQualified(String address) {
      return address == null ? false : address.contains(SEPARATOR);
   }

   public static SimpleString extractQueueName(SimpleString name) {
      return name == null ? null : new SimpleString(extractQueueName(name.toString()));
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
      return address == null ? null : new SimpleString(extractAddressName(address.toString()));
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
