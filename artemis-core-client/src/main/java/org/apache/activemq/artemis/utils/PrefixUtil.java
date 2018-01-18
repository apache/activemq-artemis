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
package org.apache.activemq.artemis.utils;

import java.util.EnumSet;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.RoutingType;

public class PrefixUtil {

   public static Pair<SimpleString, EnumSet<RoutingType>> getAddressAndRoutingTypes(SimpleString address,
                                                                                    EnumSet<RoutingType> defaultRoutingTypes,
                                                                                    Map<SimpleString, RoutingType> prefixes) {
      for (Map.Entry<SimpleString, RoutingType> entry : prefixes.entrySet()) {
         if (address.startsWith(entry.getKey())) {
            return new Pair<>(removePrefix(address, entry.getKey()), EnumSet.of(entry.getValue()));
         }
      }
      return new Pair<>(address, defaultRoutingTypes);
   }

   public static SimpleString getAddress(SimpleString address, Map<SimpleString, RoutingType> prefixes) {
      for (Map.Entry<SimpleString, RoutingType> entry : prefixes.entrySet()) {
         if (address.startsWith(entry.getKey())) {
            return removePrefix(address, entry.getKey());
         }
      }
      return address;
   }

   public static SimpleString removePrefix(SimpleString string, SimpleString prefix) {
      return string.subSeq(prefix.length(), string.length());
   }
}
