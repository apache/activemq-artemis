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

package org.apache.activemq.artemis.protocol.amqp.federation.internal;

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.core.postoffice.Binding;

/**
 * An entry type class used to hold a {@link FederationConsumerInternal} and
 * any other state data needed by the manager that is creating them based on the
 * policy configuration for the federation instance.  The entry can be extended
 * by federation implementation to hold additional state data for the federation
 * consumer and the managing of its lifetime.
 *
 * This entry type provides a reference counter that can be used to register demand
 * on a federation resource such that it is not torn down until all demand has been
 * removed from the local resource.
 */
public class FederationAddressEntry {

   private final FederationConsumerInternal consumer;

   private final Set<Binding> demandBindings = new HashSet<>();

   /**
    * Creates a new address entry with a single reference
    *
    * @param consumer
    *    The federation consumer that will be carried in this entry.
    */
   public FederationAddressEntry(FederationConsumerInternal consumer) {
      this.consumer = consumer;
   }

   /**
    * @return the address that this entry is acting to federate.
    */
   public String getAddress() {
      return consumer.getConsumerInfo().getAddress();
   }

   /**
    * @return the consumer managed by this entry
    */
   public FederationConsumerInternal getConsumer() {
      return consumer;
   }

   /**
    * @return true if there are bindings that are mapped to this federation entry.
    */
   public boolean hasDemand() {
      return !demandBindings.isEmpty();
   }

   /**
    * Add demand on this federation address consumer from the given binding.
    *
    * @return this federation address consumer entry.
    */
   public FederationAddressEntry addDemand(Binding binding) {
      demandBindings.add(binding);
      return this;
   }

   /**
    * Reduce demand on this federation address consumer from the given binding.
    *
    * @return this federation address consumer entry.
    */
   public FederationAddressEntry removeDenamd(Binding binding) {
      demandBindings.remove(binding);
      return this;
   }
}
