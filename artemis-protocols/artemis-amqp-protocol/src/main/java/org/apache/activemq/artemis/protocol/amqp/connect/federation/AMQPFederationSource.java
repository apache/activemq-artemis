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

import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONTROL_LINK;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONFIGURATION;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.AMQP_LINK_INITIALIZER_KEY;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.protocol.amqp.connect.AMQPBrokerConnection;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPIllegalStateException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConstants;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromAddressPolicy;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromQueuePolicy;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.DeleteOnClose;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the initiating side of a broker federation that occurs over an AMQP
 * broker connection.
 * <p>
 * This endpoint will create a control link to the remote peer that is a sender
 * of federation commands which can be used to instruct the remote to initiate
 * federation operations back to this peer over the same connection and without
 * the need for local configuration.
 */
public class AMQPFederationSource extends AMQPFederation {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   // Capabilities set on the sender link used to send policies or other control messages to
   // the remote federation target.
   private static final Symbol[] CONTROL_LINK_CAPABILITIES = new Symbol[] {FEDERATION_CONTROL_LINK};

   private final AMQPBrokerConnection brokerConnection;

   // Remote policies that should be conveyed to the remote server for reciprocal federation operations.
   private final Map<String, FederationReceiveFromQueuePolicy> remoteQueueMatchPolicies = new HashMap<>();
   private final Map<String, FederationReceiveFromAddressPolicy> remoteAddressMatchPolicies = new HashMap<>();

   private final Map<String, Object> properties;

   private volatile AMQPFederationConfiguration configuration;

   /**
    * Creates a new AMQP Federation instance that will manage the state of a single AMQP
    * broker federation instance using an AMQP broker connection as the IO channel.
    *
    * @param name
    *    The name of this federation instance.
    * @param properties
    *    A set of optional properties that provide additional configuration.
    * @param connection
    *    The broker connection over which this federation will occur.
    */
   @SuppressWarnings("unchecked")
   public AMQPFederationSource(String name, Map<String, Object> properties, AMQPBrokerConnection connection) {
      super(name, connection.getServer());

      if (properties == null || properties.isEmpty()) {
         this.properties = Collections.EMPTY_MAP;
      } else {
         this.properties = (Map<String, Object>) Collections.unmodifiableMap(new HashMap<>(properties));
      }

      this.brokerConnection = connection;
      this.brokerConnection.addLinkClosedInterceptor(getName(), this::invokeLinkClosedInterceptors);
   }

   /**
    * @return the {@link AMQPBrokerConnection} that this federation is attached to.
    */
   public AMQPBrokerConnection getBrokerConnection() {
      return brokerConnection;
   }

   @Override
   public int getLinkAttachTimeout() {
      return configuration.getLinkAttachTimeout();
   }

   @Override
   public synchronized AMQPSessionContext getSessionContext() {
      if (!connected) {
         throw new IllegalStateException("Cannot access session while federation is not connected");
      }

      return session;
   }

   @Override
   public synchronized AMQPConnectionContext getConnectionContext() {
      if (!connected) {
         throw new IllegalStateException("Cannot access connection while federation is not connected");
      }

      return connection;
   }

   @Override
   public synchronized int getReceiverCredits() {
      if (!connected) {
         throw new IllegalStateException("Cannot access connection configuration, federation is not connected");
      }

      return configuration.getReceiverCredits();
   }

   @Override
   public synchronized int getReceiverCreditsLow() {
      if (!connected) {
         throw new IllegalStateException("Cannot access connection configuration, federation is not connected");
      }

      return configuration.getReceiverCreditsLow();
   }

   @Override
   public synchronized int getLargeMessageThreshold() {
      if (!connected) {
         throw new IllegalStateException("Cannot access connection configuration, federation is not connected");
      }

      return configuration.getLargeMessageThreshold();
   }

   @Override
   public boolean isCoreMessageTunnelingEnabled() {
      if (!connected) {
         throw new IllegalStateException("Cannot access connection configuration, federation is not connected");
      }

      return configuration.isCoreMessageTunnelingEnabled();
   }

   /**
    * Adds a new {@link FederationReceiveFromQueuePolicy} entry to the set of policies that the
    * remote end of this federation will use to create demand on the this server when local
    * demand is present.
    *
    * @param queuePolicy
    *    The policy to add to the set of configured {@link FederationReceiveFromQueuePolicy} instance.
    *
    * @return this {@link AMQPFederationSource} instance.
    */
   public synchronized AMQPFederationSource addRemoteQueueMatchPolicy(FederationReceiveFromQueuePolicy queuePolicy) {
      remoteQueueMatchPolicies.putIfAbsent(queuePolicy.getPolicyName(), queuePolicy);
      return this;
   }

   /**
    * Adds a new {@link FederationReceiveFromAddressPolicy} entry to the set of policies that the
    * remote end of this federation will use to create demand on the this server when local
    * demand is present.
    *
    * @param addressPolicy
    *    The policy to add to the set of configured {@link FederationReceiveFromAddressPolicy} instance.
    *
    * @return this {@link AMQPFederationSource} instance.
    */
   public synchronized AMQPFederationSource addRemoteAddressMatchPolicy(FederationReceiveFromAddressPolicy addressPolicy) {
      remoteAddressMatchPolicies.putIfAbsent(addressPolicy.getPolicyName(), addressPolicy);
      return this;
   }

   /**
    * Called by the parent broker connection when the connection has failed and this federation
    * should tear down any active resources and await a reconnect if one is allowed.
    *
    * @throws ActiveMQException if an error occurs processing the connection dropped event
    */
   public synchronized void handleConnectionDropped() throws ActiveMQException {
      connected = false;

      final AtomicReference<Exception> errorCaught = new AtomicReference<>();

      queueMatchPolicies.forEach((k, v) -> {
         try {
            v.stop();
         } catch (Exception ex) {
            errorCaught.compareAndExchange(null, ex);
         }
      });

      addressMatchPolicies.forEach((k, v) -> {
         try {
            v.stop();
         } catch (Exception ex) {
            errorCaught.compareAndExchange(null, ex);
         }
      });

      connection = null;
      session = null;

      if (errorCaught.get() != null) {
         final Exception error = errorCaught.get();
         if (error instanceof ActiveMQException) {
            throw (ActiveMQException) error;
         } else {
            throw (ActiveMQException) new ActiveMQException(error.getMessage()).initCause(error);
         }
      }
   }

   /**
    * Called by the parent broker connection when the connection has been established and this
    * federation should build up its active state based on the configuration.
    *
    * @param connection
    *    The new {@link Connection} that represents the currently active connection.
    * @param session
    *    The new {@link Session} that was created for use by broker connection resources.
    *
    * @throws ActiveMQException if an error occurs processing the connection restored event
    */
   public synchronized void handleConnectionRestored(AMQPConnectionContext connection, AMQPSessionContext session) throws ActiveMQException {
      final Connection protonConnection = session.getSession().getConnection();
      final org.apache.qpid.proton.engine.Record attachments = protonConnection.attachments();

      if (attachments.get(FEDERATION_INSTANCE_RECORD, AMQPFederation.class) != null) {
         throw new ActiveMQAMQPIllegalStateException("An existing federation instance was found on the connection");
      }

      this.connection = connection;
      this.session = session;
      this.configuration = new AMQPFederationConfiguration(connection, properties);

      // Assign an federation instance to the connection which incoming federation links can look for
      // to indicate this is a valid AMQP federation endpoint.
      attachments.set(FEDERATION_INSTANCE_RECORD, AMQPFederationSource.class, this);

      // Create the control link and the outcome will then dictate if the configured
      // policy managers are started or not.
      asyncCreateControlLink();
   }

   @Override
   protected void signalResourceCreateError(Exception cause) {
      brokerConnection.connectError(cause);
   }

   @Override
   protected void signalError(Exception cause) {
      brokerConnection.runtimeError(cause);
   }

   protected boolean interceptLinkClosedEvent(Link link) {
      return false;
   }

   private void asyncCreateControlLink() {
      // Schedule the control link creation on the connection event loop thread
      // Eventual establishment of the control link indicates successful connection
      // to a remote peer that can support AMQP federation requirements.
      connection.runLater(() -> {
         try {
            final Sender sender = session.getSession().sender("Federation:" + getName() + ":" + UUIDGenerator.getInstance().generateStringUUID());
            final AMQPFederationCommandDispatcher commandLink = new AMQPFederationCommandDispatcher(sender, getServer(), session.getSessionSPI());
            final Target target = new Target();

            // The control link should be dynamic and the node is destroyed if the connection drops
            target.setDynamic(true);
            target.setCapabilities(new Symbol[] {Symbol.valueOf("temporary-topic")});
            target.setDurable(TerminusDurability.NONE);
            target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            // Set the dynamic node lifetime-policy to indicate this needs to be destroyed on close
            // we don't want control links remaining once a federation connection is closed.
            final Map<Symbol, Object> dynamicNodeProperties = new HashMap<>();
            dynamicNodeProperties.put(AmqpSupport.LIFETIME_POLICY, DeleteOnClose.getInstance());
            target.setDynamicNodeProperties(dynamicNodeProperties);

            // Send our local configuration data to the remote side of the control link
            // for use when creating remote federation resources.
            final Map<Symbol, Object> senderProperties = new HashMap<>();
            senderProperties.put(FEDERATION_CONFIGURATION, configuration.toConfigurationMap());

            sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
            sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
            sender.setDesiredCapabilities(CONTROL_LINK_CAPABILITIES);
            sender.setProperties(senderProperties);
            sender.setTarget(target);
            sender.setSource(new Source());
            sender.open();

            final ScheduledFuture<?> futureTimeout;
            final AtomicBoolean cancelled = new AtomicBoolean(false);

            if (brokerConnection.getConnectionTimeout() > 0) {
               futureTimeout = brokerConnection.getServer().getScheduledPool().schedule(() -> {
                  cancelled.set(true);
                  brokerConnection.connectError(ActiveMQAMQPProtocolMessageBundle.BUNDLE.brokerConnectionTimeout());
               }, brokerConnection.getConnectionTimeout(), TimeUnit.MILLISECONDS);
            } else {
               futureTimeout = null;
            }

            // Using attachments to set up a Runnable that will be executed inside the remote link opened handler
            sender.attachments().set(AMQP_LINK_INITIALIZER_KEY, Runnable.class, () -> {
               try {
                  if (cancelled.get()) {
                     return;
                  }

                  if (futureTimeout != null) {
                     futureTimeout.cancel(false);
                  }

                  if (sender.getRemoteTarget() == null) {
                     brokerConnection.connectError(
                        ActiveMQAMQPProtocolMessageBundle.BUNDLE.federationControlLinkRefused(sender.getName()));
                     return;
                  }

                  if (!AmqpSupport.verifyOfferedCapabilities(sender)) {
                     brokerConnection.connectError(
                        ActiveMQAMQPProtocolMessageBundle.BUNDLE.missingOfferedCapability(Arrays.toString(CONTROL_LINK_CAPABILITIES)));
                     return;
                  }

                  // We tag the session with the Federation marker as there could be incoming receivers created
                  // under it from a remote federation target if remote federation policies are configured. This
                  // allows the policy managers to then determine if local demand is from a federation target or
                  // not and based on configuration choose when to create remote receivers.
                  //
                  // This currently is a session global tag which means any consumer created from this session in
                  // response to remote attach of said receiver is going to get caught by the filtering but as of
                  // now we shouldn't be creating consumers other than federation consumers but if that were to
                  // change we'd either need single new session for this federation instance or a session per
                  // consumer at the extreme which then requires that the protocol handling code add the metadata
                  // during the receiver attach on the remote.
                  try {
                     session.getSessionSPI().addMetaData(FederationConstants.FEDERATION_NAME, getName());
                  } catch (ActiveMQAMQPException e) {
                     throw e;
                  } catch (Exception e) {
                     logger.trace("Exception on add of federation Metadata: ", e);
                     throw new ActiveMQAMQPInternalErrorException("Error while configuring interal session metadata");
                  }

                  final ProtonServerSenderContext senderContext =
                     new ProtonServerSenderContext(connection, sender, session, session.getSessionSPI(), commandLink);

                  session.addSender(sender, senderContext);

                  connected = true;

                  remoteQueueMatchPolicies.forEach((key, policy) -> {
                     try {
                        commandLink.sendPolicy(policy);
                     } catch (Exception e) {
                        brokerConnection.error(e);
                     }
                  });

                  remoteAddressMatchPolicies.forEach((key, policy) -> {
                     try {
                        commandLink.sendPolicy(policy);
                     } catch (Exception e) {
                        brokerConnection.error(e);
                     }
                  });

                  // Attempt to start the policy managers in another thread to avoid blocking the IO thread
                  scheduler.execute(() -> {
                     // Sync action with federation start / stop otherwise we could get out of sync
                     synchronized (AMQPFederationSource.this) {
                        if (isStarted()) {
                           queueMatchPolicies.forEach((k, v) -> v.start());
                           addressMatchPolicies.forEach((k, v) -> v.start());
                        }
                     }
                  });

               } catch (Exception e) {
                  brokerConnection.error(e);
               }
            });
         } catch (Exception e) {
            brokerConnection.error(e);
         }

         connection.flush();
      });
   }
}
