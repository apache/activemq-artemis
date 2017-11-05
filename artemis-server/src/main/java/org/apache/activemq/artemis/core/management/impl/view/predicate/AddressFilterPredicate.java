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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;

public class AddressFilterPredicate extends ActiveMQFilterPredicate<AddressControl> {

   enum Field {
      ID, NAME, ROUTING_TYPES, PRODUCER_ID, QUEUE_COUNT, ADDRESS_MEMORY_SIZE, PAGING, NUMBER_OF_PAGES, NUMBER_OF_BYTES_PER_PAGE
   }

   private Field f;

   private final ActiveMQServer server;

   public AddressFilterPredicate(ActiveMQServer server) {
      super();
      this.server = server;
   }

   @Override
   public boolean apply(AddressControl addressControl) {
      AddressInfo address = server.getAddressInfo(new SimpleString(addressControl.getAddress()));

      //should not happen but just in case...
      if (address == null) {
         return false;
      }

      if (f == null)
         return true;
      try {
         switch (f) {
            case ID:
               return matches(address.getId());
            case NAME:
               return matches(addressControl.getAddress());
            case ROUTING_TYPES:
               return matchAny(address.getRoutingTypes());
            case PRODUCER_ID:
               return matches("TODO");
            case QUEUE_COUNT:
               return matches(addressControl.getQueueNames().length);
            case ADDRESS_MEMORY_SIZE:
               return matches(addressControl.getAddressSize());
            case PAGING:
               return matches(addressControl.isPaging());
            case NUMBER_OF_PAGES:
               return matches(addressControl.getNumberOfPages());
            case NUMBER_OF_BYTES_PER_PAGE:
               return matches(addressControl.getNumberOfBytesPerPage());
         }
      } catch (Exception e) {
         return false;
      }

      return true;
   }

   @Override
   public void setField(String field) {
      if (field != null && !field.equals("")) {
         this.f = Field.valueOf(field.toUpperCase());
      }
   }
}
