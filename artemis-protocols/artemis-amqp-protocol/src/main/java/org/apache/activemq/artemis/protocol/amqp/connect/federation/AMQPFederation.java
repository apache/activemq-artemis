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

import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONTROL_LINK_PREFIX;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_BASE_VALIDATION_ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_EVENTS_LINK_PREFIX;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromAddressPolicy;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromQueuePolicy;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationAddressPolicyManager;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationInternal;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationQueuePolicyManager;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.qpid.proton.engine.Link;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single AMQP Federation instance that can be tied to an AMQP broker connection or
 * used on a remote peer to control the reverse case of when the remote configures the
 * target side of the connection.
 */
public abstract class AMQPFederation implements FederationInternal {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   /**
    * Value used to store the federation instance used by an AMQP connection that
    * is performing remote command and control operations or is the target of said
    * operations. Only one federation instance is allowed per connection and will
    * be checked.
    */
   public static final String FEDERATION_INSTANCE_RECORD = "FEDERATION_INSTANCE_RECORD";

   private static final WildcardConfiguration DEFAULT_WILDCARD_CONFIGURATION = new WildcardConfiguration();

   // Local policies that should be matched against demand on local addresses and queues.
   protected final Map<String, FederationQueuePolicyManager> queueMatchPolicies = new ConcurrentHashMap<>();
   protected final Map<String, FederationAddressPolicyManager> addressMatchPolicies = new ConcurrentHashMap<>();
   protected final Map<String, Predicate<Link>> linkClosedinterceptors = new ConcurrentHashMap<>();

   protected final WildcardConfiguration wildcardConfiguration;
   protected final ScheduledExecutorService scheduler;

   protected final String name;
   protected final ActiveMQServer server;

   protected AMQPFederationEventDispatcher eventDispatcher;
   protected AMQPFederationEventProcessor eventProcessor;

   // Connection and Session are updated after each reconnect.
   protected volatile AMQPConnectionContext connection;
   protected volatile AMQPSessionContext session;

   protected boolean started;
   protected volatile boolean connected;

   public AMQPFederation(String name, ActiveMQServer server) {
      Objects.requireNonNull(name, "Federation name cannot be null");
      Objects.requireNonNull(server, "Provided server instance cannot be null");

      this.name = name;
      this.server = server;
      this.scheduler = server.getScheduledPool();

      if (server.getConfiguration().getWildcardConfiguration() != null) {
         this.wildcardConfiguration = server.getConfiguration().getWildcardConfiguration();
      } else {
         this.wildcardConfiguration = DEFAULT_WILDCARD_CONFIGURATION;
      }
   }

   /**
    * @return the {@link WildcardConfiguration} that is in use by this server federation.
    */
   public WildcardConfiguration getWildcardConfiguration() {
      return wildcardConfiguration;
   }

   public ScheduledExecutorService getScheduler() {
      return scheduler;
   }

   @Override
   public ActiveMQServer getServer() {
      return server;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public synchronized boolean isStarted() {
      return started;
   }

   /**
    * @return the session context assigned to this federation instance
    */
   public abstract AMQPConnectionContext getConnectionContext();

   /**
    * @return the session context assigned to this federation instance
    */
   public abstract AMQPSessionContext getSessionContext();

   /**
    * @return the federation configuration that is in effect.
    */
   public abstract AMQPFederationConfiguration getConfiguration();

   @Override
   public final synchronized void start() throws ActiveMQException {
      if (!started) {
         handleFederationStarted();
         signalFederationStarted();
         started = true;
      }
   }

   @Override
   public final synchronized void stop() throws ActiveMQException {
      if (started) {
         handleFederationStopped();
         signalFederationStopped();
         started = false;

         try {
            if (eventDispatcher != null) {
               eventDispatcher.close();
            }

            if (eventProcessor != null) {
               eventProcessor.close(false);
            }
         } catch (ActiveMQException amqEx) {
            throw amqEx;
         } catch (Exception ex) {
            throw (ActiveMQException) new ActiveMQException(ex.getMessage()).initCause(ex);
         } finally {
            eventDispatcher = null;
            eventProcessor = null;
         }
      }
   }

   /**
    * Performs the prefixing for federation events queues that places the events queues into
    * the name-space of federation related internal queues.
    *
    * @param suffix
    *    A suffix to append to the federation events link (normally the AMQP link name).
    *
    * @return the full internal queue name to use for the given suffix.
    */
   String prefixEventsLinkQueueName(String suffix) {
      final StringBuilder builder = new StringBuilder();
      final char delimiter = getWildcardConfiguration().getDelimiter();

      builder.append(FEDERATION_BASE_VALIDATION_ADDRESS)
             .append(delimiter)
             .append(FEDERATION_EVENTS_LINK_PREFIX)
             .append(delimiter)
             .append(suffix);

      return builder.toString();
   }

   /**
    * Performs the prefixing for federation control queue name that places the queues
    * into the name-space of federation related internal queues.
    *
    * @param suffix
    *    A suffix to append to the federation control link (normally the AMQP link name).
    *
    * @return the full internal queue name to use for the given suffix.
    */
   String prefixControlLinkQueueName(String suffix) {
      final StringBuilder builder = new StringBuilder();
      final char delimiter = getWildcardConfiguration().getDelimiter();

      builder.append(FEDERATION_BASE_VALIDATION_ADDRESS)
             .append(delimiter)
             .append(FEDERATION_CONTROL_LINK_PREFIX)
             .append(delimiter)
             .append(suffix);

      return builder.toString();
   }

   /**
    * Adds a remote linked closed event interceptor that can intercept the closed event and
    * if it returns true indicate that the close has been handled and that no further action
    * need to be taken for this event.
    *
    * @param id
    *    A unique Id value that identifies the interceptor for later removal.
    * @param interceptor
    *    The predicate that will be called for any link close.
    *
    * @return this {@link AMQPFederation} instance.
    */
   public AMQPFederation addLinkClosedInterceptor(String id, Predicate<Link> interceptor) {
      linkClosedinterceptors.put(id, interceptor);
      return this;
   }

   /**
    * Remove a previously registered link close interceptor from the list of close interceptor bindings.
    *
    * @param id
    *   The id of the interceptor to remove
    *
    * @return this {@link AMQPFederation} instance.
    */
   public AMQPFederation removeLinkClosedInterceptor(String id) {
      linkClosedinterceptors.remove(id);
      return this;
   }

   /**
    * Adds a new {@link FederationReceiveFromQueuePolicy} entry to the set of policies that this
    * federation will use to create demand on the remote when local demand is present.
    *
    * @param queuePolicy
    *    The policy to add to the set of configured {@link FederationReceiveFromQueuePolicy} instance.
    *
    * @return this {@link AMQPFederation} instance.
    *
    * @throws ActiveMQException if an error occurs processing the added policy
    */
   public synchronized AMQPFederation addQueueMatchPolicy(FederationReceiveFromQueuePolicy queuePolicy) throws ActiveMQException {
      final FederationQueuePolicyManager manager = new AMQPFederationQueuePolicyManager(this, queuePolicy);

      queueMatchPolicies.put(queuePolicy.getPolicyName(), manager);

      logger.debug("AMQP Federation {} adding queue match policy: {}", getName(), queuePolicy.getPolicyName());

      if (isStarted()) {
         // This is a heavy operation in some cases so move off the IO thread
         scheduler.execute(() -> manager.start());
      }

      return this;
   }

   /**
    * Adds a new {@link FederationReceiveFromAddressPolicy} entry to the set of policies that this
    * federation will use to create demand on the remote when local demand is present.
    *
    * @param addressPolicy
    *    The policy to add to the set of configured {@link FederationReceiveFromAddressPolicy} instance.
    *
    * @return this {@link AMQPFederation} instance.
    *
    * @throws ActiveMQException if an error occurs processing the added policy
    */
   public synchronized AMQPFederation addAddressMatchPolicy(FederationReceiveFromAddressPolicy addressPolicy) throws ActiveMQException {
      final FederationAddressPolicyManager manager = new AMQPFederationAddressPolicyManager(this, addressPolicy);

      addressMatchPolicies.put(addressPolicy.getPolicyName(), manager);

      logger.debug("AMQP Federation {} adding address match policy: {}", getName(), addressPolicy.getPolicyName());

      if (isStarted()) {
         // This is a heavy operation in some cases so move off the IO thread
         scheduler.execute(() -> manager.start());
      }

      return this;
   }

   /**
    * Register an event sender instance with this federation for use in sending federation level
    * events from this federation instance to the remote peer.
    *
    * @param dispatcher
    *    The event sender instance to be registered.
    */
   synchronized void registerEventSender(AMQPFederationEventDispatcher dispatcher) {
      if (eventDispatcher != null) {
         throw new IllegalStateException("Federation event dipsatcher already registered on this federation instance.");
      }

      eventDispatcher = dispatcher;
   }

   /**
    * Register an event receiver instance with this federation for use in receiving federation level
    * events sent to this federation instance from the remote peer.
    *
    * @param dispatcher
    *    The event receiver instance to be registered.
    */
   synchronized void registerEventReceiver(AMQPFederationEventProcessor processor) {
      if (eventProcessor != null) {
         throw new IllegalStateException("Federation event processor already registered on this federation instance.");
      }

      eventProcessor = processor;
   }

   /**
    * Register an address by name that was either not present when an address federation consumer
    * was initiated or was removed and the active address federation consumer was force closed.
    * Upon (re)creation of the registered address a one time event will be sent to the remote
    * federation instance which allows it to check if demand still exists and make another attempt
    * at creating a consumer to federate messages from that address.
    *
    * @param address
    *     The address that is currently missing which should be watched for creation.
    */
   synchronized void registerMissingAddress(String address) {
      if (eventDispatcher != null) {
         eventDispatcher.addAddressWatch(address);
      }
   }

   /**
    * Register a queue by name that was either not present when an queue federation consumer was
    * initiated or was removed and the active queue federation consumer was force closed. Upon
    * (re)creation of the registered address and queue a one time event will be sent to the remote
    * federation instance which allows it to check if demand still exists and make another attempt
    * at creating a consumer to federate messages from that queue.
    *
    * @param queue
    *     The queue that is currently missing which should be watched for creation.
    */
   synchronized void registerMissingQueue(String queue) {
      if (eventDispatcher != null) {
         eventDispatcher.addQueueWatch(queue);
      }
   }

   /**
    * Triggers scan of federation address policies for local address demand on the given address
    * that was added on the remote peer which was previously absent and could not be auto created
    * or was removed while a federation receiver was attached and caused an existing federation
    * receiver to be closed.
    *
    * @param addressName
    *       The address that has been added on the remote peer.
    */
   synchronized void processRemoteAddressAdded(String addressName) {
      addressMatchPolicies.values().forEach(policy -> {
         try {
            policy.afterRemoteAddressAdded(addressName);
         } catch (Exception e) {
            logger.warn("Error processing remote address added event: ", e);
            signalError(e);
         }
      });
   }

   /**
    * Triggers scan of federation queue policies for local queue demand on the given queue
    * that was added on the remote peer which was previously absent at the time of a federation
    * receiver attach or was removed and caused an existing federation receiver to be closed.
    *
    * @param addressName
    *       The address that has been added on the remote peer.
    * @param queueName
    *       The queue that has been added on the remote peer.
    */
   synchronized void processRemoteQueueAdded(String addressName, String queueName) {
      queueMatchPolicies.values().forEach(policy -> {
         try {
            policy.afterRemoteQueueAdded(addressName, queueName);
         } catch (Exception e) {
            logger.warn("Error processing remote queue added event: ", e);
            signalError(e);
         }
      });
   }

   /**
    * Error signaling API that must be implemented by the specific federation implementation
    * to handle error when creating a federation resource such as an outgoing receiver link.
    *
    * @param cause
    *    The error that caused the resource creation to fail.
    */
   protected abstract void signalResourceCreateError(Exception cause);

   /**
    * Error signaling API that must be implemented by the specific federation implementation
    * to handle errors encountered during normal operations.
    *
    * @param cause
    *    The error that caused the operation to fail.
    */
   protected abstract void signalError(Exception cause);

   /**
    * Provides an entry point for the concrete federation implementation to respond
    * to being started.
    *
    * @throws ActiveMQException if an error is thrown during policy start.
    */
   protected void handleFederationStarted() throws ActiveMQException {
      if (connected) {
         queueMatchPolicies.forEach((k, v) -> v.start());
         addressMatchPolicies.forEach((k, v) -> v.start());
      }
   }

   /**
    * Provides an entry point for the concrete federation implementation to respond
    * to being stopped.
    *
    * @throws ActiveMQException if an error is thrown during policy stop.
    */
   protected void handleFederationStopped() throws ActiveMQException {
      queueMatchPolicies.forEach((k, v) -> v.stop());
      addressMatchPolicies.forEach((k, v) -> v.stop());
   }

   protected boolean invokeLinkClosedInterceptors(Link link) {
      for (Map.Entry<String, Predicate<Link>> interceptor : linkClosedinterceptors.entrySet()) {
         if (interceptor.getValue().test(link)) {
            logger.trace("Remote link[{}] close intercepted and handled by interceptor: {}", link.getName(), interceptor.getKey());
            return true;
         }
      }

      return false;
   }

   protected void signalFederationStarted() {
      try {
         server.callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               ((ActiveMQServerAMQPFederationPlugin) plugin).federationStarted(this);
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("federationStarted", t);
      }
   }

   protected void signalFederationStopped() {
      try {
         server.callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               ((ActiveMQServerAMQPFederationPlugin) plugin).federationStopped(this);
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("federationStopped", t);
      }
   }
}
