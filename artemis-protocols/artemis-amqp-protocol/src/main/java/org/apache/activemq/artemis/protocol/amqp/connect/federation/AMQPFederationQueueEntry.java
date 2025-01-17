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

import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;

/**
 * An entry type class used to hold a {@link AMQPFederationConsumer} and
 * any other state data needed by the manager that is creating them based on the
 * policy configuration for the federation instance.  The entry can be extended
 * by federation implementation to hold additional state data for the federation
 * consumer and the managing of its lifetime.
 *
 * This entry type provides reference tracking state for current demand (bindings)
 * on a federation resource such that it is not torn down until all demand has been
 * removed from the local resource.
 */
public class AMQPFederationQueueEntry {

   private final Queue queue;
   private final FederationConsumerInfo consumerInfo;
   private final Set<String> consumerDemand = new HashSet<>();

   private AMQPFederationConsumer consumer;

   /**
    * Creates a new queue entry with a single reference
    *
    * @param consumerInfo
    *       Consumer information object used to define the federation queue consumer.
    * @param queue
    *       The {@link Queue} on the server where that the federation is tracking.
    */
   public AMQPFederationQueueEntry(Queue queue, FederationConsumerInfo consumerInfo) {
      this.consumerInfo = consumerInfo;
      this.queue = queue;
   }

   /**
    * @return the {@link Queue} on the server this federation entry is tracking a consumer for.
    */
   public Queue getQueue() {
      return queue;
   }

   /**
    * @return the name of the queue that this entry tracks demand for.
    */
   public String getQueueName() {
      return consumerInfo.getQueueName();
   }

   /**
    * @return the consumer information that defines the properties of federation queue consumers
    */
   public FederationConsumerInfo getConsumerInfo() {
      return consumerInfo;
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
    * @return this federation queue consumer entry.
    */
   public AMQPFederationQueueEntry setConsumer(AMQPFederationConsumer consumer) {
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
      return !consumerDemand.isEmpty();
   }

   /**
    * Add additional demand on the resource associated with this entries consumer.
    *
    * @param consumer
    *    The {@link ServerConsumer} that generated the demand on federated resource.
    *
    * @return this federation queue entry instance.
    */
   public AMQPFederationQueueEntry addDemand(ServerConsumer consumer) {
      consumerDemand.add(identifyConsumer(consumer));
      return this;
   }

   /**
    * Remove the known demand on the resource from the given {@link ServerConsumer}.
    *
    * @param consumer
    *    The {@link ServerConsumer} that generated the demand on federated resource.
    *
    * @return this federation queue entry instance.
    */
   public AMQPFederationQueueEntry removeDemand(ServerConsumer consumer) {
      consumerDemand.remove(identifyConsumer(consumer));
      return this;
   }

   /**
    * Remove all known demand on the resource from any previously added {@link ServerConsumer}s.
    *
    * @return this federation queue entry instance.
    */
   public AMQPFederationQueueEntry removeAllDemand() {
      consumerDemand.clear();
      return this;
   }

   private static String identifyConsumer(ServerConsumer consumer) {
      return consumer.getConnectionID().toString() + ":" +
             consumer.getSessionID() + ":" +
             consumer.getID();
   }
}
