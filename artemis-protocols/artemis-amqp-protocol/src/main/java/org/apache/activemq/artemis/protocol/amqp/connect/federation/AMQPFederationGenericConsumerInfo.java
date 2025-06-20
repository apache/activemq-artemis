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

import java.util.Objects;
import java.util.UUID;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.protocol.amqp.federation.Federation;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromAddressPolicy;
import org.apache.activemq.artemis.utils.CompositeAddress;

/**
 * Information and identification class for Federation consumers created to federate queues and addresses. Instances of
 * this class should be usable in Collections classes where equality and hashing support is needed.
 */
public class AMQPFederationGenericConsumerInfo implements FederationConsumerInfo {

   public static final String FEDERATED_QUEUE_PREFIX = "federated";
   public static final String QUEUE_NAME_FORMAT_STRING = "${address}::${routeType}";

   private final Role role;
   private final String address;
   private final String queueName;
   private final RoutingType routingType;
   private final String filterString;
   private final String fqqn;
   private final int priority;
   private final String id;

   public AMQPFederationGenericConsumerInfo(Role role, String address, String queueName, RoutingType routingType,
                                        String filterString, String fqqn, int priority) {
      this.role = role;
      this.address = address;
      this.queueName = queueName;
      this.routingType = routingType;
      this.filterString = filterString;
      this.fqqn = fqqn;
      this.priority = priority;
      this.id = UUID.randomUUID().toString();
   }

   /**
    * Factory for creating federation address consumer information objects from server resources.
    *
    * @param address      The address being federated, the remote consumer will be created under this address.
    * @param queueName    The name of the remote queue that will be created in order to route messages here.
    * @param routingType  The routing type to assign the remote consumer.
    * @param filterString A filter string used by the federation instance to limit what enters the remote queue.
    * @param federation   The parent {@link Federation} that this federation consumer is created for
    * @param policy       The {@link FederationReceiveFromAddressPolicy} that triggered this information object to be
    *                     created.
    * @return a newly created and configured {@link FederationConsumerInfo} instance
    */
   public static AMQPFederationGenericConsumerInfo build(String address, String queueName, RoutingType routingType, String filterString, Federation federation, FederationReceiveFromAddressPolicy policy) {
      return new AMQPFederationGenericConsumerInfo(Role.ADDRESS_CONSUMER,
                                               address,
                                               queueName,
                                               routingType,
                                               filterString,
                                               CompositeAddress.toFullyQualified(address, queueName),
                                               ActiveMQDefaultConfiguration.getDefaultConsumerPriority());
   }

   @Override
   public String getId() {
      return id;
   }

   @Override
   public Role getRole() {
      return role;
   }

   @Override
   public String getQueueName() {
      return queueName;
   }

   @Override
   public String getAddress() {
      return address;
   }

   @Override
   public String getFqqn() {
      return fqqn;
   }

   @Override
   public RoutingType getRoutingType() {
      return routingType;
   }

   @Override
   public String getFilterString() {
      return filterString;
   }

   @Override
   public int getPriority() {
      return priority;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!(obj instanceof AMQPFederationGenericConsumerInfo other)) {
         return false;
      }

      return role == other.role &&
             priority == other.priority &&
             Objects.equals(address, other.address) &&
             Objects.equals(queueName, other.queueName) &&
             routingType == other.routingType &&
             Objects.equals(filterString, other.filterString) &&
             Objects.equals(fqqn, other.fqqn);
   }

   @Override
   public int hashCode() {
      return Objects.hash(role, address, queueName, routingType, filterString, fqqn, priority);
   }

   @Override
   public String toString() {
      return "FederationConsumerInfo: { " + getRole() + ", " + getFqqn() + "}";
   }
}
