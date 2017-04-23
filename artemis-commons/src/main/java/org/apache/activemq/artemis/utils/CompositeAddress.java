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

   public static SimpleString toFullQN(SimpleString address, SimpleString qName) {
      return address.concat(SEPARATOR).concat(qName);
   }

   public static String toFullQN(String address, String qName) {
      return address + SEPARATOR + qName;
   }

   public static String SEPARATOR = "::";
   private final String address;
   private final String queueName;
   private final boolean fqqn;

   public String getAddress() {
      return address;
   }

   public String getQueueName() {
      return queueName;
   }

   public CompositeAddress(String address, String queueName) {

      this.address = address;
      this.queueName = queueName;
      this.fqqn = address != null && !address.isEmpty();
   }

   public CompositeAddress(String singleName) {
      int index = singleName.indexOf(SEPARATOR);
      if (index == -1) {
         this.fqqn = false;
         this.address = null;
         this.queueName = singleName;
      } else {
         this.fqqn = true;
         this.address = singleName.substring(0, index);
         this.queueName = singleName.substring(index + 2);
      }
   }

   public boolean isFqqn() {
      return fqqn;
   }

   public static boolean isFullyQualified(String address) {
      return address.contains(SEPARATOR);
   }

   public static CompositeAddress getQueueName(String address) {

      int index = address.indexOf(SEPARATOR);
      if (index == -1) {
         throw new IllegalStateException("Not A Fully Qualified Name");
      }
      return new CompositeAddress(address.substring(0, index), address.substring(index + 2));
   }

   public static String extractQueueName(String name) {
      int index = name.indexOf(SEPARATOR);
      if (index != -1) {
         return name.substring(index + 2);
      }
      return name;
   }

   public static SimpleString extractQueueName(SimpleString name) {
      return new SimpleString(extractQueueName(name.toString()));
   }

   public static String extractAddressName(String address) {
      String[] split = address.split(SEPARATOR);
      return split[0];
   }
}
