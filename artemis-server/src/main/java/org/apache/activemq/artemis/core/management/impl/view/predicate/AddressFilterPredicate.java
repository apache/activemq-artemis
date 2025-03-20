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

import java.util.Arrays;

import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.core.management.impl.view.AddressField;
import org.apache.activemq.artemis.core.server.ActiveMQServer;

public class AddressFilterPredicate extends ActiveMQFilterPredicate<AddressControl> {

   private AddressField f;

   private final ActiveMQServer server;

   public AddressFilterPredicate(ActiveMQServer server) {
      super();
      this.server = server;
   }

   @Override
   public boolean test(AddressControl address) {
      if (f == null)
         return true;
      try {
         return switch (f) {
            case ID -> matches(address.getId());
            case NAME -> matches(address.getAddress());
            case ROUTING_TYPES -> matchAny(Arrays.asList(address.getRoutingTypes()));
            case QUEUE_COUNT -> matches(address.getQueueCount());
            case INTERNAL -> matches(address.isInternal());
            case TEMPORARY -> matches(address.isTemporary());
            case AUTO_CREATED -> matches(address.isAutoCreated());
            case PAUSED -> matches(address.isPaused());
            case CURRENT_DUPLICATE_ID_CACHE_SIZE -> matches(address.getCurrentDuplicateIdCacheSize());
            case RETROACTIVE_RESOURCE -> matches(address.isRetroactiveResource());
            case UNROUTED_MESSAGE_COUNT -> matches(address.getUnRoutedMessageCount());
            case ROUTED_MESSAGE_COUNT -> matches(address.getRoutedMessageCount());
            case MESSAGE_COUNT -> matches(address.getMessageCount());
            case NUMBER_OF_BYTES_PER_PAGE -> matches(address.getNumberOfBytesPerPage());
            case ADDRESS_LIMIT_PERCENT -> matches(address.getAddressLimitPercent());
            case PAGING -> matches(address.isPaging());
            case NUMBER_OF_PAGES -> matches(address.getNumberOfPages());
            case ADDRESS_SIZE -> matches(address.getAddressSize());
            case MAX_PAGE_READ_BYTES -> matches(address.getMaxPageReadBytes());
            case MAX_PAGE_READ_MESSAGES -> matches(address.getMaxPageReadMessages());
            case PREFETCH_PAGE_BYTES -> matches(address.getPrefetchPageBytes());
            case PREFETCH_PAGE_MESSAGES -> matches(address.getPrefetchPageBytes());
         };
      } catch (Exception e) {
         return false;
      }
   }

   @Override
   public void setField(String field) {
      if (field != null && !field.isEmpty()) {
         this.f = AddressField.valueOfName(field);

         //for backward compatibility
         if (this.f == null) {
            this.f = AddressField.valueOf(field);
         }
      }
   }
}
