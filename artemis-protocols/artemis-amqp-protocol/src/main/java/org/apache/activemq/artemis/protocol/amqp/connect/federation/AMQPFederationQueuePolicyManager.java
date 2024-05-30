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

import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumer;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromQueuePolicy;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo.Role;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationConsumerInternal;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationGenericConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationQueuePolicyManager;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AMQP Federation implementation of an federation queue policy manager.
 */
public class AMQPFederationQueuePolicyManager extends FederationQueuePolicyManager {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected final AMQPFederation federation;

   protected volatile AMQPFederationConsumerConfiguration configuration;
   protected volatile AMQPSessionContext session;

   public AMQPFederationQueuePolicyManager(AMQPFederation federation, FederationReceiveFromQueuePolicy queuePolicy) throws ActiveMQException {
      super(federation, queuePolicy);

      this.federation = federation;
   }

   @Override
   protected void handlePolicyManagerStarted(FederationReceiveFromQueuePolicy policy) {
      // Capture state for the current connection on each start of the policy manager.
      configuration = new AMQPFederationConsumerConfiguration(federation.getConfiguration(), policy.getProperties());
      session = federation.getSessionContext();
   }

   @Override
   protected FederationConsumerInfo createConsumerInfo(ServerConsumer consumer) {
      final Queue queue = consumer.getQueue();
      final String queueName = queue.getName().toString();
      final String address = queue.getAddress().toString();

      final int priority = configuration.isIgnoreSubscriptionPriorities() ?
         ActiveMQDefaultConfiguration.getDefaultConsumerPriority() + policy.getPriorityAjustment() :
         consumer.getPriority() + policy.getPriorityAjustment();

      final String filterString =
         selectFilter(queue.getFilter(), configuration.isIgnoreSubscriptionFilters() ? null : consumer.getFilter());

      return new FederationGenericConsumerInfo(Role.QUEUE_CONSUMER,
                                               address,
                                               queueName,
                                               queue.getRoutingType(),
                                               filterString,
                                               CompositeAddress.toFullyQualified(address, queueName),
                                               priority);
   }

   @Override
   protected FederationConsumerInternal createFederationConsumer(FederationConsumerInfo consumerInfo) {
      Objects.requireNonNull(consumerInfo, "Federation Queue consumer information object was null");

      if (logger.isTraceEnabled()) {
         logger.trace("AMQP Federation {} creating queue consumer: {} for policy: {}", federation.getName(), consumerInfo, policy.getPolicyName());
      }

      // Don't initiate anything yet as the caller might need to register error handlers etc
      // before the attach is sent otherwise they could miss the failure case.
      return new AMQPFederationQueueConsumer(federation, configuration, session, consumerInfo, policy);
   }

   @Override
   protected void signalBeforeCreateFederationConsumer(FederationConsumerInfo info) {
      try {
         server.callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               ((ActiveMQServerAMQPFederationPlugin) plugin).beforeCreateFederationConsumer(info);
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("beforeCreateFederationConsumer", t);
      }
   }

   @Override
   protected void signalAfterCreateFederationConsumer(FederationConsumer consumer) {
      try {
         server.callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               ((ActiveMQServerAMQPFederationPlugin) plugin).afterCreateFederationConsumer(consumer);
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("afterCreateFederationConsumer", t);
      }
   }

   @Override
   protected void signalBeforeCloseFederationConsumer(FederationConsumer consumer) {
      try {
         server.callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               ((ActiveMQServerAMQPFederationPlugin) plugin).beforeCloseFederationConsumer(consumer);
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("beforeCloseFederationConsumer", t);
      }
   }

   @Override
   protected void signalAfterCloseFederationConsumer(FederationConsumer consumer) {
      try {
         server.callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               ((ActiveMQServerAMQPFederationPlugin) plugin).afterCloseFederationConsumer(consumer);
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("afterCloseFederationConsumer", t);
      }
   }

   @Override
   protected final boolean isPluginBlockingFederationConsumerCreate(Queue queue) {
      final AtomicBoolean canCreate = new AtomicBoolean(true);

      try {
         server.callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               if (canCreate.get()) {
                  canCreate.set(((ActiveMQServerAMQPFederationPlugin) plugin).shouldCreateFederationConsumerForQueue(queue));
               }
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("shouldCreateFederationConsumerForQueue", t);
      }

      return !canCreate.get();
   }

   private static String selectFilter(Filter queueFilter, Filter consumerFilter) {
      if (consumerFilter != null) {
         return consumerFilter.getFilterString().toString();
      } else {
         return queueFilter != null ? queueFilter.getFilterString().toString() : null;
      }
   }
}
