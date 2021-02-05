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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

public class MechanismFinder {

   private static final String[] DEFAULT_MECHANISMS = new String[] {PlainSASL.NAME, AnonymousServerSASL.NAME};

   private static final Map<String, ServerSASLFactory> FACTORY_MAP = new HashMap<>();

   static {
      ServiceLoader<ServerSASLFactory> serviceLoader =
               ServiceLoader.load(ServerSASLFactory.class, MechanismFinder.class.getClassLoader());
      for (ServerSASLFactory factory : serviceLoader) {
         FACTORY_MAP.put(factory.getMechanism(), factory);
      }
   }

   public static String[] getKnownMechanisms() {
      return Stream.concat(Arrays.stream(DEFAULT_MECHANISMS), FACTORY_MAP.keySet().stream()).toArray(String[]::new);
   }

   public static ServerSASLFactory getFactory(String mechanism) {
      return FACTORY_MAP.get(mechanism);
   }
}
