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

package org.apache.activemq.artemis.protocol.amqp.sasl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public class MechanismFinder {

   private static final Map<String, List<ServerSASLFactory>> FACTORY_MAP = new HashMap<>();
   private static final Comparator<? super ServerSASLFactory> PRECEDENCE_COMPARATOR =
      (f1, f2) -> f1.getPrecedence() - f2.getPrecedence();

   static {
      ServiceLoader<ServerSASLFactory> serviceLoader =
               ServiceLoader.load(ServerSASLFactory.class, MechanismFinder.class.getClassLoader());
      for (ServerSASLFactory factory : serviceLoader) {
         List<ServerSASLFactory> list = FACTORY_MAP.computeIfAbsent(factory.getMechanism(), k -> new ArrayList<>());
         list.add(factory);
      }
      for (List<ServerSASLFactory> factories : FACTORY_MAP.values()) {
         Collections.sort(factories, PRECEDENCE_COMPARATOR);
      }
   }

   public static String[] getDefaultMechanisms() {
      return FACTORY_MAP.values()
            .stream()
            .flatMap(List<ServerSASLFactory>::stream)
            .sorted(PRECEDENCE_COMPARATOR)
            .map(ServerSASLFactory::getMechanism)
            .distinct()
            .toArray(String[]::new);
   }

   public static ServerSASLFactory getFactory(String mechanism) {
      List<ServerSASLFactory> list = FACTORY_MAP.get(mechanism);
      if (list == null || list.isEmpty()) {
         return null;
      }
      // return mechanism with highest precedence if multiple are registered
      return list.get(0);
   }
}
