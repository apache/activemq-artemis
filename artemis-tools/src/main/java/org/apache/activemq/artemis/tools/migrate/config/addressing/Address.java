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
package org.apache.activemq.artemis.tools.migrate.config.addressing;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Address {

   private String name;

   private Set<String> routingTypes = new HashSet<>();

   private String defaultMaxConsumers = "-1";

   private String defaultPurgeOnNoConsumers = "false";

   private List<Queue> queues = new ArrayList<>();

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public Set<String> getRoutingTypes() {
      return routingTypes;
   }

   public void setRoutingTypes(Set<String> routingTypes) {
      this.routingTypes = routingTypes;
   }

   public String getDefaultMaxConsumers() {
      return defaultMaxConsumers;
   }

   public List<Queue> getQueues() {
      return queues;
   }

   public void setQueues(List<Queue> queues) {
      this.queues = queues;
   }
}
