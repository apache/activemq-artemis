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

import java.util.Map;

public class ParameterisedAddress {

   public static SimpleString toParameterisedAddress(SimpleString address, Map<String, String> parameters) {
      if (parameters != null && parameters.size() > 0) {
         return SimpleString.toSimpleString(toParameterisedAddress(address.toString(), parameters));
      } else {
         return address;
      }
   }

   public static String toParameterisedAddress(String address, Map<String, String> parameters) {
      if (parameters != null && parameters.size() > 0) {
         StringBuilder stringBuilder = new StringBuilder(address).append(PARAMETER_MARKER);
         return toParameterString(stringBuilder, parameters).toString();
      } else {
         return address;
      }
   }

   private static StringBuilder toParameterString(StringBuilder stringBuilder, Map<String, String> parameters) {
      boolean first = true;
      for (Map.Entry<String, String> entry : parameters.entrySet()) {
         if (first) {
            first = false;
         } else {
            stringBuilder.append(PARAMETER_SEPERATOR);
         }
         stringBuilder.append(entry.getKey()).append(PARAMETER_KEY_VALUE_SEPERATOR).append(entry.getValue());
      }
      return stringBuilder;
   }

   public static char PARAMETER_SEPERATOR = '&';
   public static char PARAMETER_KEY_VALUE_SEPERATOR = '=';
   public static char PARAMETER_MARKER = '?';
   public static String PARAMETER_SEPERATOR_STRING = Character.toString(PARAMETER_SEPERATOR);
   public static String PARAMETER_KEY_VALUE_SEPERATOR_STRING = Character.toString(PARAMETER_KEY_VALUE_SEPERATOR);
   public static String PARAMETER_MARKER_STRING = Character.toString(PARAMETER_MARKER);
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
      int index = address.indexOf(PARAMETER_MARKER);
      if (index == -1) {
         this.address = SimpleString.toSimpleString(address);
         this.queueAttributes = null;
      } else {
         this.address = SimpleString.toSimpleString(address.substring(0, index));
         String parametersString = address.substring(index + 1, address.length());
         String[] parameterPairs = parametersString.split(PARAMETER_SEPERATOR_STRING);
         QueueAttributes queueAttributes = new QueueAttributes();
         for (String param : parameterPairs) {
            String[] keyValue = param.split(PARAMETER_KEY_VALUE_SEPERATOR_STRING);
            if (keyValue.length != 2) {
               throw new IllegalArgumentException("Malformed parameter section " + param);
            } else {
               queueAttributes.set(keyValue[0], keyValue[1]);
            }
         }
         this.queueAttributes = queueAttributes;
      }
   }

   public boolean isParameterised() {
      return this.queueAttributes != null;
   }

   public static boolean isParameterised(String address) {
      return address.contains(PARAMETER_MARKER_STRING);
   }

   public static boolean isParameterised(SimpleString address) {
      return address.contains(PARAMETER_MARKER);
   }

}
