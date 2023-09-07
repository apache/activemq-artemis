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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Divert;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumer;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromAddressPolicy;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationAddressPolicyManager;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationConsumerInternal;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationGenericConsumerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AMQP Federation implementation of an federation address policy manager.
 */
public class AMQPFederationAddressPolicyManager extends FederationAddressPolicyManager {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected final AMQPFederation federation;
   protected final AMQPFederationConsumerConfiguration configuration;

   protected final String remoteQueueFilter;

   public AMQPFederationAddressPolicyManager(AMQPFederation federation, FederationReceiveFromAddressPolicy addressPolicy) throws ActiveMQException {
      super(federation, addressPolicy);

      this.federation = federation;

      if (policy.getMaxHops() > 0) {
         this.remoteQueueFilter = "\"m." + AMQPFederationPolicySupport.MESSAGE_HOPS_ANNOTATION.toString() +
                                  "\" IS NULL OR \"m." + AMQPFederationPolicySupport.MESSAGE_HOPS_ANNOTATION.toString() +
                                  "\"<" + policy.getMaxHops();
      } else {
         this.remoteQueueFilter = null;
      }

      this.configuration = new AMQPFederationConsumerConfiguration(federation, policy.getProperties());
   }

   @Override
   protected FederationGenericConsumerInfo createConsumerInfo(AddressInfo address) {
      return FederationGenericConsumerInfo.build(address.getName().toString(),
                                                 generateQueueName(address),
                                                 address.getRoutingType(),
                                                 remoteQueueFilter,
                                                 federation,
                                                 policy);
   }

   protected String generateQueueName(AddressInfo address) {
      return "federation." + federation.getName() + ".address." + address.getName() + ".node." + server.getNodeID();
   }

   @Override
   protected FederationConsumerInternal createFederationConsumer(FederationConsumerInfo consumerInfo) {
      Objects.requireNonNull(consumerInfo, "Federation Address consumer information object was null");

      if (logger.isTraceEnabled()) {
         logger.trace("AMQP Federation {} creating address consumer: {} for policy: {}", federation.getName(), consumerInfo, policy.getPolicyName());
      }

      // Don't initiate anything yet as the caller might need to register error handlers etc
      // before the attach is sent otherwise they could miss the failure case.
      return new AMQPFederationAddressConsumer(federation, configuration, federation.getSessionContext(), consumerInfo, policy);
   }

   @Override
   protected boolean testIfAddressMatchesPolicy(AddressInfo addressInfo) {
      if (!policy.test(addressInfo)) {
         return false;
      }

      // Address consumers can't pull as we have no real metric to indicate when / how much
      // we should pull so instead we refuse to match if credit set to zero.
      if (federation.getReceiverCredits() <= 0) {
         logger.debug("Federation address policy rejecting match on {} because credit is set to zero:", addressInfo.getName());
         return false;
      } else {
         return true;
      }
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
   protected final boolean isPluginBlockingFederationConsumerCreate(AddressInfo address) {
      final AtomicBoolean canCreate = new AtomicBoolean(true);

      try {
         server.callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               if (canCreate.get()) {
                  canCreate.set(((ActiveMQServerAMQPFederationPlugin) plugin).shouldCreateFederationConsumerForAddress(address));
               }
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("shouldCreateFederationConsumerForAddress", t);
      }

      return !canCreate.get();
   }

   @Override
   protected final boolean isPluginBlockingFederationConsumerCreate(Divert divert, Queue queue) {
      final AtomicBoolean canCreate = new AtomicBoolean(true);

      try {
         server.callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               if (canCreate.get()) {
                  canCreate.set(((ActiveMQServerAMQPFederationPlugin) plugin).shouldCreateFederationConsumerForDivert(divert, queue));
               }
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("shouldCreateFederationConsumerForDivert", t);
      }

      return !canCreate.get();
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
}
