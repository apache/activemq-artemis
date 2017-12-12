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

import javax.json.JsonObjectBuilder;
import org.apache.activemq.artemis.core.management.impl.view.predicate.AddressFilterPredicate;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.utils.JsonLoader;

public class AddressView extends ActiveMQAbstractView<AddressInfo> {

   private static final String defaultSortColumn = "id";

   private final ActiveMQServer server;

   public AddressView(ActiveMQServer server) {
      super();
      this.server = server;
      this.predicate = new AddressFilterPredicate(server);
   }

   @Override
   public Class getClassT() {
      return AddressInfo.class;
   }

   @Override
   public JsonObjectBuilder toJson(AddressInfo address) {
      if (address == null) {
         return null;
      }

      JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("id", toString(address.getId())).add("name", toString(address.getName())).add("routingTypes", toString(address.getRoutingTypes()));

      try {
         obj.add("queueCount", toString(server.bindingQuery(address.getName()).getQueueNames().size()));
         return obj;
      } catch (Exception e) {
         obj.add("queueCount", 0);
      }
      return obj;
   }

   @Override
   public Object getField(AddressInfo address, String fieldName) {
      if (address == null) {
         return null;
      }

      switch (fieldName) {
         case "id":
            return address.getId();
         case "name":
            return address.getName();
         case "routingTypes":
            return address.getRoutingTypes();
         case "queueCount":
            try {
               return server.bindingQuery(address.getName()).getQueueNames().size();
            } catch (Exception e) {
               return 0;
            }
         default:
            throw new IllegalArgumentException("Unsupported field, " + fieldName);
      }
   }

   @Override
   public String getDefaultOrderColumn() {
      return defaultSortColumn;
   }
}
