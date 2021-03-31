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
package org.apache.activemq.artemis.core.management.impl.view.predicate;

import org.apache.activemq.artemis.core.management.impl.view.AddressField;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;

public class AddressFilterPredicate extends ActiveMQFilterPredicate<AddressInfo> {

   private AddressField f;

   private final ActiveMQServer server;

   public AddressFilterPredicate(ActiveMQServer server) {
      super();
      this.server = server;
   }

   @Override
   public boolean test(AddressInfo address) {
      if (f == null)
         return true;
      try {
         switch (f) {
            case ID:
               return matches(address.getId());
            case NAME:
               return matches(address.getName());
            case ROUTING_TYPES:
               return matchAny(address.getRoutingTypes());
            case PRODUCER_ID:
               return matches("TODO");
            case QUEUE_COUNT:
               return matches(server.bindingQuery(address.getName()).getQueueNames().size());
         }
      } catch (Exception e) {
         return false;
      }

      return true;
   }

   @Override
   public void setField(String field) {
      if (field != null && !field.equals("")) {
         this.f = AddressField.valueOfName(field);

         //for backward compatibility
         if (this.f == null) {
            this.f = AddressField.valueOf(field);
         }
      }
   }
}
