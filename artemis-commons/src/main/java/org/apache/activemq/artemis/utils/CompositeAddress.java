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
      this.fqqn = address != null;
   }

   public CompositeAddress(String singleName) {
      String[] split = singleName.split(SEPARATOR);
      if (split.length == 1) {
         this.fqqn = false;
         this.address = null;
         this.queueName = split[0];
      } else {
         this.fqqn = true;
         this.address = split[0];
         this.queueName = split[1];
      }
   }

   public boolean isFqqn() {
      return fqqn;
   }

   public static boolean isFullyQualified(String address) {
      return address.contains(SEPARATOR);
   }

   public static CompositeAddress getQueueName(String address) {
      String[] split = address.split(SEPARATOR);
      if (split.length <= 0) {
         throw new IllegalStateException("Not A Fully Qualified Name");
      }
      if (split.length == 1) {
         return new CompositeAddress(null, split[0]);
      }
      return new CompositeAddress(split[0], split[1]);
   }

   public static String extractQueueName(String name) {
      String[] split = name.split(SEPARATOR);
      return split[split.length - 1];
   }

   public static SimpleString extractQueueName(SimpleString name) {
      return new SimpleString(extractQueueName(name.toString()));
   }

   public static String extractAddressName(String address) {
      String[] split = address.split(SEPARATOR);
      return split[0];
   }
}
