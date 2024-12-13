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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Divert;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBindingPlugin;
import org.apache.activemq.artemis.protocol.amqp.federation.Federation;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumer;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromResourcePolicy;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationType;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base Federation policy manager that declares some common APIs that address or queue policy
 * managers must provide implementations for.
 */
public abstract class AMQPFederationPolicyManager implements ActiveMQServerBindingPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected final AtomicLong messageCount = new AtomicLong();
   protected final BiConsumer<FederationConsumerInfo, Message> messageObserver = (i, m) -> messageCount.incrementAndGet();
   protected final ActiveMQServer server;
   protected final AMQPFederation federation;

   protected volatile boolean started;
   protected volatile AMQPFederationConsumerConfiguration configuration;
   protected volatile AMQPSessionContext session;

   public AMQPFederationPolicyManager(AMQPFederation federation) throws ActiveMQException {
      Objects.requireNonNull(federation, "The Federation instance cannot be null");

      this.federation = federation;
      this.server = federation.getServer();
   }

   /**
    * @return the number of messages that all federation consumer of this policy have received from the remote.
    */
   public long getMessagesReceived() {
      return messageCount.get();
   }

   /**
    * @return the immutable federation policy configuration that backs this manager.
    */
   public abstract FederationReceiveFromResourcePolicy getPolicy();

   /**
    * @return the federation type this policy manager implements.
    */
   public final FederationType getPolicyType() {
      return getPolicy().getPolicyType();
   }

   /**
    * @return the assigned name of the policy that is being managed.
    */
   public final String getPolicyName() {
      return getPolicy().getPolicyName();
   }

   /**
    * @return <code>true</code> if the policy is started at the time this method was called.
    */
   public final boolean isStarted() {
      return started;
   }

   /**
    * @return the {@link AMQPFederation} instance that owns this policy manager.
    */
   public AMQPFederation getFederation() {
      return federation;
   }

   /**
    * Start the federation policy manager which will initiate a scan of all broker
    * bindings and create and matching remote receivers. Start on a policy manager
    * should only be called after its parent {@link Federation} is started and the
    * federation connection has been established.
    */
   public synchronized void start() {
      if (!federation.isStarted()) {
         throw new IllegalStateException("Cannot start a federation policy manager when the federation is stopped.");
      }

      if (!started) {
         started = true;
         // Capture state for the current connection on each start of the policy manager.
         configuration = new AMQPFederationConsumerConfiguration(federation.getConfiguration(), getPolicy().getProperties());
         session = federation.getSessionContext();
         server.registerBrokerPlugin(this);
         scanAllBindings(); // Create federation consumers for any matches to the policy at startup.
      }
   }

   /**
    * On start a federation policy manager needs to scan all the current bindings for matches
    * to its configuration among the domain it manages (address or queue).
    */
   protected abstract void scanAllBindings();

   /**
    * Create a new {@link AMQPFederationConsumer} instance using the consumer information
    * given. This is called when local demand for a matched resource requires a new consumer to
    * be created. This method by default will call the configured consumer factory function that
    * was provided when the manager was created, a subclass can override this to perform additional
    * actions for the create operation.
    *
    * @param consumerInfo
    *    The {@link FederationConsumerInfo} that defines the consumer to be created.
    *
    * @return a new {@link AMQPFederationConsumer} instance that will reside in this manager.
    */
   protected abstract AMQPFederationConsumer createFederationConsumer(FederationConsumerInfo consumerInfo);

   /**
    * Attempts to close a federation consumer and signals the installed federation plugin
    * of the impending and post closed state. The method will not double close a consumer
    * as it checks the closed state. The method is synchronized to allow for use in asynchronous
    * call backs from federation consumers.
    *
    * @param federationConsuner
    *    A federation consumer to close, or null in which case no action is taken.
    */
   protected synchronized void tryCloseFederationConsumer(AMQPFederationConsumer federationConsuner) {
      if (federationConsuner != null) {
         try {
            if (!federationConsuner.isClosed()) {
               signalPluginBeforeCloseFederationConsumer(federationConsuner);
               federationConsuner.close();
               signalPluginAfterCloseFederationConsumer(federationConsuner);
            }
         } catch (Exception ignore) {
            logger.trace("Caught error on attempted close of existing federation consumer", ignore);
         }
      }
   }

   /**
    * Signal any registered plugins for this federation instance that a remote consumer
    * is being created.
    *
    * @param info
    *    The {@link FederationConsumerInfo} that describes the remote federation consumer
    */
   protected final void signalPluginBeforeCreateFederationConsumer(FederationConsumerInfo info) {
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

   /**
    * Signal any registered plugins for this federation instance that a remote consumer
    * has been created.
    *
    * @param consumer
    *    The {@link FederationConsumerInfo} that describes the remote consumer
    */
   protected final void signalPluginAfterCreateFederationConsumer(FederationConsumer consumer) {
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

   /**
    * Signal any registered plugins for this federation instance that a remote consumer
    * is about to be closed.
    *
    * @param consumer
    *    The {@link FederationConsumer} that that is about to be closed.
    */
   protected final void signalPluginBeforeCloseFederationConsumer(FederationConsumer consumer) {
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

   /**
    * Signal any registered plugins for this federation instance that a remote consumer
    * has now been closed.
    *
    * @param consumer
    *    The {@link FederationConsumer} that that has been closed.
    */
   protected final void signalPluginAfterCloseFederationConsumer(FederationConsumer consumer) {
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

   /**
    * Query all registered plugins for this federation instance to determine if any wish to
    * prevent a federation consumer from being created for the given resource.
    *
    * @param address
    *    The address on which the manager is intending to create a remote consumer for.
    *
    * @return true if any registered plugin signaled that creation should be suppressed.
    */
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

   /**
    * Query all registered plugins for this federation instance to determine if any wish to
    * prevent a federation consumer from being created for the given Queue.
    *
    * @param divert
    *    The {@link Divert} that triggered the manager to attempt to create a remote consumer.
    * @param queue
    *    The {@link Queue} that triggered the manager to attempt to create a remote consumer.
    *
    * @return true if any registered plugin signaled that creation should be suppressed.
    */
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

   /**
    * Query all registered plugins for this federation instance to determine if any wish to
    * prevent a federation consumer from being created for the given Queue.
    *
    * @param queue
    *    The {@link Queue} that triggered the manager to attempt to create a remote consumer.
    *
    * @return true if any registered plugin signaled that creation should be suppressed.
    */
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
