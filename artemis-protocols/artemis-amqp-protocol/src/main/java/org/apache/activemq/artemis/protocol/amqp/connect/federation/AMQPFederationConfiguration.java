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

import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.LARGE_MESSAGE_THRESHOLD;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.LINK_ATTACH_TIMEOUT;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.PULL_RECEIVER_BATCH_SIZE;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS_LOW;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.IGNORE_QUEUE_CONSUMER_FILTERS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.IGNORE_QUEUE_CONSUMER_PRIORITIES;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.qpid.proton.engine.Receiver;

/**
 * A configuration class that contains API for getting federation specific
 * configuration either from a {@link Map} of configuration elements or from
 * the connection associated with the federation instance, or possibly from a
 * set default value.
 */
public final class AMQPFederationConfiguration {

   /**
    * Default timeout value (in seconds) used to control when a link attach is considered to have
    * failed due to not responding to an attach request.
    */
   public static final int DEFAULT_LINK_ATTACH_TIMEOUT = 30;

   /**
    * Default credits granted to a receiver that is in pull mode.
    */
   public static final int DEFAULT_PULL_CREDIT_BATCH_SIZE = 100;

   /**
    * Default value for the core message tunneling feature that indicates if core protocol messages
    * should be streamed as binary blobs as the payload of an custom AMQP message which avoids any
    * conversions of the messages to / from AMQP.
    */
   public static final boolean DEFAULT_CORE_MESSAGE_TUNNELING_ENABLED = true;

   /**
    * Default value for the filtering applied to federation queue consumers that controls if
    * the filter specified by a consumer subscription is used or if the higher level queue
    * filter only is applied when creating a federation queue consumer.
    */
   private static final boolean DEFAULT_IGNNORE_QUEUE_CONSUMER_FILTERS = false;

   /**
    * Default value for the priority applied to federation queue consumers that controls if
    * the priority specified by a consumer subscription is used or if the policy priority
    * offset value is simply applied to the default consumer priority value.
    */
   private static final boolean DEFAULT_IGNNORE_QUEUE_CONSUMER_PRIORITIES = false;

   private final Map<String, Object> properties;
   private final AMQPConnectionContext connection;

   @SuppressWarnings("unchecked")
   public AMQPFederationConfiguration(AMQPConnectionContext connection, Map<String, Object> properties) {
      Objects.requireNonNull(connection, "Connection provided cannot be null");

      this.connection = connection;

      if (properties != null && !properties.isEmpty()) {
         this.properties = new HashMap<>(properties);
      } else {
         this.properties = Collections.EMPTY_MAP;
      }
   }

   /**
    * @return the credit batch size offered to a {@link Receiver} link.
    */
   public int getReceiverCredits() {
      final Object property = properties.get(RECEIVER_CREDITS);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return connection.getAmqpCredits();
      }
   }

   /**
    * @return the number of remaining credits on a {@link Receiver} before the batch is replenished.
    */
   public int getReceiverCreditsLow() {
      final Object property = properties.get(RECEIVER_CREDITS_LOW);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return connection.getAmqpLowCredits();
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
         return DEFAULT_PULL_CREDIT_BATCH_SIZE;
      }
   }

   /**
    * @return the size in bytes of an incoming message after which the {@link Receiver} treats it as large.
    */
   public int getLargeMessageThreshold() {
      final Object property = properties.get(LARGE_MESSAGE_THRESHOLD);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return connection.getProtocolManager().getAmqpMinLargeMessageSize();
      }
   }

   /**
    * @return the size in bytes of an incoming message after which the {@link Receiver} treats it as large.
    */
   public int getLinkAttachTimeout() {
      final Object property = properties.get(LINK_ATTACH_TIMEOUT);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return DEFAULT_LINK_ATTACH_TIMEOUT;
      }
   }

   /**
    * @return true if the federation is configured to tunnel core messages as AMQP custom messages.
    */
   public boolean isCoreMessageTunnelingEnabled() {
      final Object property = properties.get(AmqpSupport.TUNNEL_CORE_MESSAGES);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return DEFAULT_CORE_MESSAGE_TUNNELING_ENABLED;
      }
   }

   /**
    * @return <code>true</code> if federation is configured to ignore filters on individual queue consumers
    */
   public boolean isIgnoreSubscriptionFilters() {
      final Object property = properties.get(IGNORE_QUEUE_CONSUMER_FILTERS);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return DEFAULT_IGNNORE_QUEUE_CONSUMER_FILTERS;
      }
   }

   /**
    * @return <code>true</code> if federation is configured to ignore priorities on individual queue consumers
    */
   public boolean isIgnoreSubscriptionPriorities() {
      final Object property = properties.get(IGNORE_QUEUE_CONSUMER_PRIORITIES);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return DEFAULT_IGNNORE_QUEUE_CONSUMER_PRIORITIES;
      }
   }

   /**
    * Enumerate the configuration options in this configuration object and return a {@link Map} that
    * contains the values which can be sent to a remote peer
    *
    * @return a Map that contains the values of each configuration option.
    */
   public Map<String, Object> toConfigurationMap() {
      final Map<String, Object> configMap = new HashMap<>();

      configMap.put(RECEIVER_CREDITS, getReceiverCredits());
      configMap.put(RECEIVER_CREDITS_LOW, getReceiverCreditsLow());
      configMap.put(PULL_RECEIVER_BATCH_SIZE, getPullReceiverBatchSize());
      configMap.put(LARGE_MESSAGE_THRESHOLD, getLargeMessageThreshold());
      configMap.put(LINK_ATTACH_TIMEOUT, getLinkAttachTimeout());
      configMap.put(IGNORE_QUEUE_CONSUMER_FILTERS, isIgnoreSubscriptionFilters());
      configMap.put(IGNORE_QUEUE_CONSUMER_PRIORITIES, isIgnoreSubscriptionPriorities());
      configMap.put(AmqpSupport.TUNNEL_CORE_MESSAGES, isCoreMessageTunnelingEnabled());

      return configMap;
   }
}
