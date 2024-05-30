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

import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.IGNORE_QUEUE_CONSUMER_FILTERS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.IGNORE_QUEUE_CONSUMER_PRIORITIES;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.LARGE_MESSAGE_THRESHOLD;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.LINK_ATTACH_TIMEOUT;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.PULL_RECEIVER_BATCH_SIZE;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS_LOW;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.qpid.proton.engine.Receiver;

/**
 * Configuration options applied to a consumer created from federation policies
 * for address or queue federation. The options first check the policy properties
 * for matching configuration settings before looking at the federation's own
 * configuration for the options managed here.
 */
public final class AMQPFederationConsumerConfiguration {

   private final Map<String, Object> properties;
   private final AMQPFederationConfiguration configuration;

   @SuppressWarnings("unchecked")
   public AMQPFederationConsumerConfiguration(AMQPFederationConfiguration configuration, Map<String, ?> properties) {
      Objects.requireNonNull(configuration, "Federation configuration cannot be null");

      this.configuration = configuration;

      if (properties == null || properties.isEmpty()) {
         this.properties = Collections.EMPTY_MAP;
      } else {
         this.properties = (Map<String, Object>) Collections.unmodifiableMap(new HashMap<>(properties));
      }
   }

   public int getReceiverCredits() {
      final Object property = properties.get(RECEIVER_CREDITS);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return configuration.getReceiverCredits();
      }
   }

   public int getReceiverCreditsLow() {
      final Object property = properties.get(RECEIVER_CREDITS_LOW);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return configuration.getReceiverCreditsLow();
      }
   }

   /**
    * @return the credit batch size offered to a {@link Receiver} link that is in pull mode.
    */
   public int getPullReceiverBatchSize() {
      final Object property = properties.get(PULL_RECEIVER_BATCH_SIZE);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return configuration.getPullReceiverBatchSize();
      }
   }

   public int getLargeMessageThreshold() {
      final Object property = properties.get(LARGE_MESSAGE_THRESHOLD);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return configuration.getLargeMessageThreshold();
      }
   }

   public int getLinkAttachTimeout() {
      final Object property = properties.get(LINK_ATTACH_TIMEOUT);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return configuration.getLinkAttachTimeout();
      }
   }

   public boolean isCoreMessageTunnelingEnabled() {
      final Object property = properties.get(AmqpSupport.TUNNEL_CORE_MESSAGES);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return configuration.isCoreMessageTunnelingEnabled();
      }
   }

   public boolean isIgnoreSubscriptionFilters() {
      final Object property = properties.get(IGNORE_QUEUE_CONSUMER_FILTERS);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return configuration.isIgnoreSubscriptionFilters();
      }
   }

   public boolean isIgnoreSubscriptionPriorities() {
      final Object property = properties.get(IGNORE_QUEUE_CONSUMER_PRIORITIES);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return configuration.isIgnoreSubscriptionPriorities();
      }
   }
}
