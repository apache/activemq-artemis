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
package org.apache.activemq.artemis.core.management.impl.view;

import java.util.Map;
import java.util.TreeMap;

public enum ProducerField {
   ID("id"),
   SESSION("session"),
   CONNECTION_ID("connectionID"),
   ADDRESS("address"), USER("user"),
   PROTOCOL("protocol"),
   CLIENT_ID("clientID"),
   LOCAL_ADDRESS("localAddress"),
   REMOTE_ADDRESS("remoteAddress"),
   CREATION_TIME("creationTime");

   private static final Map<String, ProducerField> lookup = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

   static {
      for (ProducerField e: values()) {
         lookup.put(e.name, e);
      }
   }

   private final String name;

   public String getName() {
      return name;
   }

   ProducerField(String name) {
      this.name = name;
   }

   public static ProducerField valueOfName(String name) {
      return lookup.get(name);
   }
}
