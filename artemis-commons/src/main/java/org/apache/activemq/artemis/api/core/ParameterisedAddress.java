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
package org.apache.activemq.artemis.api.core;

import static org.apache.activemq.artemis.utils.uri.URISupport.appendParameters;
import static org.apache.activemq.artemis.utils.uri.URISupport.parseQuery;

import java.net.URISyntaxException;
import java.util.Map;

import org.apache.activemq.artemis.utils.uri.URISupport;

public class ParameterisedAddress {

   public static SimpleString toParameterisedAddress(SimpleString address, Map<String, String> parameters) throws URISyntaxException {
      if (parameters != null && !parameters.isEmpty()) {
         return SimpleString.toSimpleString(toParameterisedAddress(address.toString(), parameters));
      } else {
         return address;
      }
   }

   public static String toParameterisedAddress(String address, Map<String, String> parameters) throws URISyntaxException {
      if (parameters != null && !parameters.isEmpty()) {
         return appendParameters(new StringBuilder(address), parameters).toString();
      } else {
         return address;
      }
   }

   private final SimpleString address;
   private final QueueAttributes queueAttributes;

   public SimpleString getAddress() {
      return address;
   }

   public QueueAttributes getQueueAttributes() {
      return queueAttributes;
   }

   public ParameterisedAddress(SimpleString address, QueueAttributes queueAttributes) {
      this.address = address;
      this.queueAttributes = queueAttributes;
   }

   public ParameterisedAddress(String address, QueueAttributes queueAttributes) {
      this(SimpleString.toSimpleString(address), queueAttributes);
   }

   public ParameterisedAddress(SimpleString address) {
      this(address.toString());
   }

   public ParameterisedAddress(String address) {
      int index = address.indexOf('?');
      if (index == -1) {
         this.address = SimpleString.toSimpleString(address);
         this.queueAttributes = null;
      } else {
         this.address = SimpleString.toSimpleString(address.substring(0, index));
         QueueAttributes queueAttributes = new QueueAttributes();
         try {
            parseQuery(address).forEach(queueAttributes::set);
         } catch (URISyntaxException use) {
            throw new IllegalArgumentException("Malformed parameters in address " + address);
         }
         this.queueAttributes = queueAttributes;
      }
   }

   public boolean isParameterised() {
      return this.queueAttributes != null;
   }

   public static boolean isParameterised(String address) {
      return URISupport.containsQuery(address);
   }

   public static boolean isParameterised(SimpleString address) {
      return URISupport.containsQuery(address);
   }

}
