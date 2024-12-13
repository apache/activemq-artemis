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

package org.apache.activemq.artemis.protocol.amqp.connect.federation;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;

/**
 * An entry type class used to hold a {@link AMQPFederationConsumer} and
 * any other state data needed by the manager that is creating them based on the
 * policy configuration for the federation instance. The entry can be extended
 * by federation implementation to hold additional state data for the federation
 * consumer and the managing active demand.
 *
 * This entry type provides reference tracking state for current demand (bindings)
 * on a federation resource such that it is not torn down until all demand has been
 * removed from the local resource.
 */
public class AMQPFederationAddressEntry {

   private final AddressInfo addressInfo;
   private final Set<Binding> demandBindings = new HashSet<>();

   private AMQPFederationConsumer consumer;

   /**
    * Creates a new address entry for tracking demand on a federated address
    *
    * @param addressInfo
    *    The address information object that this entry hold demand state for.
    */
   public AMQPFederationAddressEntry(AddressInfo addressInfo) {
      this.addressInfo = addressInfo;
   }

   /**
    * @return the address information that this entry is acting to federate.
    */
   public AddressInfo getAddressInfo() {
      return addressInfo;
   }

   /**
    * @return the address that this entry is acting to federate.
    */
   public String getAddress() {
      return addressInfo.getName().toString();
   }

   /**
    * @return <code>true</code> if a consumer is currently set on this entry.
    */
   public boolean hasConsumer() {
      return consumer != null;
   }

   /**
    * @return the consumer managed by this entry
    */
   public AMQPFederationConsumer getConsumer() {
      return consumer;
   }

   /**
    * Sets the consumer assigned to this entry to the given instance.
    *
    * @param consumer
    *    The federation consumer that is currently active for this entry.
    *
    * @return this federation address consumer entry.
    */
   public AMQPFederationAddressEntry setConsumer(AMQPFederationConsumer consumer) {
      Objects.requireNonNull(consumer, "Cannot assign a null consumer to this entry, call clear to unset");
      this.consumer = consumer;
      return this;
   }

   /**
    * Clears the currently assigned consumer from this entry.
    *
    * @return the consumer that was stored here previously or null if none was set
    */
   public AMQPFederationConsumer clearConsumer() {
      final AMQPFederationConsumer taken = consumer;

      this.consumer = null;

      return taken;
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
   public AMQPFederationAddressEntry addDemand(Binding binding) {
      demandBindings.add(binding);
      return this;
   }

   /**
    * Reduce demand on this federation address consumer from the given binding.
    *
    * @return this federation address consumer entry.
    */
   public AMQPFederationAddressEntry removeDemand(Binding binding) {
      demandBindings.remove(binding);
      return this;
   }

   /**
    * Remove demand on this federation address consumer from all previous bindings.
    *
    * @return this federation address consumer entry.
    */
   public AMQPFederationAddressEntry removeAllDemand() {
      demandBindings.clear();
      return this;
   }
}
