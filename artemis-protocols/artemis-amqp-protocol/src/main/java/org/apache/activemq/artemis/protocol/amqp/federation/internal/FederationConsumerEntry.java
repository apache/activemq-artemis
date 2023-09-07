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

/**
 * Am entry type class used to hold a {@link FederationConsumerInternal} and
 * any other state data needed by the manager that is creating them based on the
 * policy configuration for the federation instance.  The entry can be extended
 * by federation implementation to hold additional state data for the federation
 * consumer and the managing of its lifetime.
 *
 * This entry type provides a reference counter that can be used to register demand
 * on a federation resource such that it is not torn down until all demand has been
 * removed from the local resource.
 */
public class FederationConsumerEntry {

   private final FederationConsumerInternal consumer;

   private int references = 1;

   /**
    * Creates a new consumer entry with a single reference
    *
    * @param consumer
    *    The federation consumer that will be carried in this entry.
    */
   public FederationConsumerEntry(FederationConsumerInternal consumer) {
      this.consumer = consumer;
   }

   /**
    * @return the consumer managed by this entry
    */
   public FederationConsumerInternal getConsumer() {
      return consumer;
   }

   /**
    * Add additional demand on the resource associated with this entries consumer.
    */
   public void addDemand() {
      references++;
   }

   /**
    * Reduce the known demand on the resource this entries consumer is associated with
    * and returns true when demand reaches zero which indicates the consumer should be
    * closed and the entry cleaned up.
    *
    * @return true if demand has fallen to zero on the resource associated with the consumer.
    */
   public boolean reduceDemand() {
      return --references == 0;
   }
}
