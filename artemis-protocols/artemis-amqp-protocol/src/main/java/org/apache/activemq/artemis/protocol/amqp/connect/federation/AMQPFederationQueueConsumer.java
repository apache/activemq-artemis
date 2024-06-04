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

import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_QUEUE_RECEIVER;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_RECEIVER_PRIORITY;
import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPTunneledMessageConstants.AMQP_TUNNELED_CORE_LARGE_MESSAGE_FORMAT;
import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPTunneledMessageConstants.AMQP_TUNNELED_CORE_MESSAGE_FORMAT;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.AMQP_LINK_INITIALIZER_KEY;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.DETACH_FORCED;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.NOT_FOUND;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.RESOURCE_DELETED;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.transformer.Transformer;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPNotFoundException;
import org.apache.activemq.artemis.protocol.amqp.federation.Federation;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromQueuePolicy;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationConsumerInternal;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpJmsSelectorFilter;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPTunneledCoreLargeMessageReader;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPTunneledCoreMessageReader;
import org.apache.activemq.artemis.protocol.amqp.proton.MessageReader;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerReceiverContext;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer implementation for Federated Queues that receives from a remote
 * AMQP peer and forwards those messages onto the internal broker Queue for
 * consumption by an attached resource.
 */
public class AMQPFederationQueueConsumer implements FederationConsumerInternal {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final int DEFAULT_PENDING_MSG_CHECK_BACKOFF_MULTIPLIER = 2;
   public static final int DEFAULT_PENDING_MSG_CHECK_MAX_DELAY = 30;

   private static final Symbol[] DEFAULT_OUTCOMES = new Symbol[]{Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL,
                                                                 Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL};


   // Sequence ID value used to keep links that would otherwise have the same name from overlapping
   // this generally occurs when consumers on the same queue have differing filters.
   private static final AtomicLong LINK_SEQUENCE_ID = new AtomicLong();

   private final AMQPFederation federation;
   private final AMQPFederationConsumerConfiguration configuration;
   private final FederationConsumerInfo consumerInfo;
   private final FederationReceiveFromQueuePolicy policy;
   private final AMQPConnectionContext connection;
   private final AMQPSessionContext session;
   private final Predicate<Link> remoteCloseIntercepter = this::remoteLinkClosedIntercepter;
   private final Transformer transformer;

   private AMQPFederatedQueueDeliveryReceiver receiver;
   private Receiver protonReceiver;
   private boolean started;
   private volatile boolean closed;
   private Consumer<FederationConsumerInternal> remoteCloseHandler;

   public AMQPFederationQueueConsumer(AMQPFederation federation, AMQPFederationConsumerConfiguration configuration,
                                      AMQPSessionContext session, FederationConsumerInfo consumerInfo, FederationReceiveFromQueuePolicy policy) {
      this.federation = federation;
      this.consumerInfo = consumerInfo;
      this.policy = policy;
      this.connection = session.getAMQPConnectionContext();
      this.session = session;
      this.configuration = configuration;

      final TransformerConfiguration transformerConfiguration = policy.getTransformerConfiguration();
      if (transformerConfiguration != null) {
         this.transformer = federation.getServer().getServiceRegistry().getFederationTransformer(policy.getPolicyName(), transformerConfiguration);
      } else {
         this.transformer = (m) -> m;
      }
   }

   @Override
   public Federation getFederation() {
      return federation;
   }

   @Override
   public FederationConsumerInfo getConsumerInfo() {
      return consumerInfo;
   }

   /**
    * @return the {@link FederationReceiveFromQueuePolicy} that initiated this consumer.
    */
   public FederationReceiveFromQueuePolicy getPolicy() {
      return policy;
   }

   @Override
   public synchronized void start() {
      if (!started && !closed) {
         started = true;
         asyncCreateReceiver();
      }
   }

   @Override
   public synchronized void close() {
      if (!closed) {
         closed = true;
         if (started) {
            started = false;
            connection.runLater(() -> {
               federation.removeLinkClosedInterceptor(consumerInfo.getId());

               if (receiver != null) {
                  try {
                     receiver.close(false);
                  } catch (ActiveMQAMQPException e) {
                  } finally {
                     receiver = null;
                  }
               }

               // Need to track the proton receiver and close it here as the default
               // context implementation doesn't do that and could result in no detach
               // being sent in some cases and possible resources leaks.
               if (protonReceiver != null) {
                  try {
                     protonReceiver.close();
                  } finally {
                     protonReceiver = null;
                  }
               }

               connection.flush();
            });
         }
      }
   }

   @Override
   public synchronized AMQPFederationQueueConsumer setRemoteClosedHandler(Consumer<FederationConsumerInternal> handler) {
      if (started) {
         throw new IllegalStateException("Cannot set a remote close handler after the consumer is started");
      }

      this.remoteCloseHandler = handler;
      return this;
   }

   protected boolean remoteLinkClosedIntercepter(Link link) {
      if (link == protonReceiver && link.getRemoteCondition() != null && link.getRemoteCondition().getCondition() != null) {
         final Symbol errorCondition = link.getRemoteCondition().getCondition();

         // Cases where remote link close is not considered terminal, additional checks
         // should be added as needed for cases where the remote has closed the link either
         // during the attach or at some point later.

         if (RESOURCE_DELETED.equals(errorCondition)) {
            // Remote side manually deleted this queue.
            return true;
         } else if (NOT_FOUND.equals(errorCondition)) {
            // Remote did not have a queue that matched.
            return true;
         } else if (DETACH_FORCED.equals(errorCondition)) {
            // Remote operator forced the link to detach.
            return true;
         }
      }

      return false;
   }

   private void signalBeforeFederationConsumerMessageHandled(Message message) throws ActiveMQException {
      try {
         federation.getServer().callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               ((ActiveMQServerAMQPFederationPlugin) plugin).beforeFederationConsumerMessageHandled(this, message);
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("beforeFederationConsumerMessageHandled", t);
      }
   }

   private void signalAfterFederationConsumerMessageHandled(Message message) throws ActiveMQException {
      try {
         federation.getServer().callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               ((ActiveMQServerAMQPFederationPlugin) plugin).afterFederationConsumerMessageHandled(this, message);
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("afterFederationConsumerMessageHandled", t);
      }
   }

   private String generateLinkName() {
      return "federation-" + federation.getName() +
             "-queue-receiver-" + consumerInfo.getFqqn() +
             "-" + federation.getServer().getNodeID() + ":" +
             LINK_SEQUENCE_ID.getAndIncrement();
   }

   private void asyncCreateReceiver() {
      connection.runLater(() -> {
         if (closed) {
            return;
         }

         try {
            final Receiver protonReceiver = session.getSession().receiver(generateLinkName());
            final Target target = new Target();
            final Source source = new Source();
            final String address = consumerInfo.getFqqn();
            final Queue localQueue = federation.getServer().locateQueue(consumerInfo.getQueueName());

            if (RoutingType.ANYCAST.equals(consumerInfo.getRoutingType())) {
               source.setCapabilities(AmqpSupport.QUEUE_CAPABILITY);
            } else {
               source.setCapabilities(AmqpSupport.TOPIC_CAPABILITY);
            }

            source.setOutcomes(Arrays.copyOf(DEFAULT_OUTCOMES, DEFAULT_OUTCOMES.length));
            source.setDurable(TerminusDurability.NONE);
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            source.setAddress(address);

            if (consumerInfo.getFilterString() != null && !consumerInfo.getFilterString().isEmpty()) {
               final AmqpJmsSelectorFilter jmsFilter = new AmqpJmsSelectorFilter(consumerInfo.getFilterString());
               final Map<Symbol, Object> filtersMap = new HashMap<>();
               filtersMap.put(AmqpSupport.JMS_SELECTOR_KEY, jmsFilter);

               source.setFilter(filtersMap);
            }

            target.setAddress(address);

            final Map<Symbol, Object> receiverProperties = new HashMap<>();
            receiverProperties.put(FEDERATION_RECEIVER_PRIORITY, consumerInfo.getPriority());

            protonReceiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
            protonReceiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
            protonReceiver.setDesiredCapabilities(new Symbol[] {FEDERATION_QUEUE_RECEIVER});
            // If enabled offer core tunneling which we prefer to AMQP conversions of core as
            // the large ones will be converted to standard AMQP messages in memory. When not
            // offered the remote must not use core tunneling and AMQP conversion will be the
            // fallback.
            if (configuration.isCoreMessageTunnelingEnabled()) {
               protonReceiver.setOfferedCapabilities(new Symbol[] {AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT});
            }
            protonReceiver.setProperties(receiverProperties);
            protonReceiver.setTarget(target);
            protonReceiver.setSource(source);
            protonReceiver.open();

            final ScheduledFuture<?> openTimeoutTask;
            final AtomicBoolean openTimedOut = new AtomicBoolean(false);

            if (configuration.getLinkAttachTimeout() > 0) {
               openTimeoutTask = federation.getServer().getScheduledPool().schedule(() -> {
                  openTimedOut.set(true);
                  federation.signalResourceCreateError(ActiveMQAMQPProtocolMessageBundle.BUNDLE.brokerConnectionTimeout());
               }, configuration.getLinkAttachTimeout(), TimeUnit.SECONDS);
            } else {
               openTimeoutTask = null;
            }

            this.protonReceiver = protonReceiver;

            protonReceiver.attachments().set(AMQP_LINK_INITIALIZER_KEY, Runnable.class, () -> {
               try {
                  if (openTimeoutTask != null) {
                     openTimeoutTask.cancel(false);
                  }

                  if (openTimedOut.get()) {
                     return;
                  }

                  // Remote must support federation receivers otherwise we fail the connection unless the
                  // Attach indicates that a detach is incoming in which case we just allow the normal handling
                  // to occur.
                  if (protonReceiver.getRemoteSource() != null && !AmqpSupport.verifyOfferedCapabilities(protonReceiver, FEDERATION_QUEUE_RECEIVER)) {
                     federation.signalResourceCreateError(
                        ActiveMQAMQPProtocolMessageBundle.BUNDLE.missingOfferedCapability(FEDERATION_QUEUE_RECEIVER.toString()));
                     return;
                  }

                  // Intercept remote close and check for valid reasons for remote closure such as
                  // the remote peer not having a matching queue for this subscription or from an
                  // operator manually closing the link.
                  federation.addLinkClosedInterceptor(consumerInfo.getId(), remoteCloseIntercepter);

                  receiver = new AMQPFederatedQueueDeliveryReceiver(localQueue, protonReceiver);

                  if (protonReceiver.getRemoteSource() != null) {
                     logger.debug("AMQP Federation {} queue consumer {} completed open", federation.getName(), consumerInfo);
                  } else {
                     logger.debug("AMQP Federation {} queue consumer {} rejected by remote", federation.getName(), consumerInfo);
                  }

                  session.addReceiver(protonReceiver, (session, protonRcvr) -> {
                     return this.receiver;
                  });
               } catch (Exception e) {
                  federation.signalError(e);
               }
            });
         } catch (Exception e) {
            federation.signalError(e);
         }

         connection.flush();
      });
   }

   private static int caclulateNextDelay(int lastDelay, int backoffMultiplier, int maxDelay) {
      final int nextDelay;

      if (lastDelay == 0) {
         nextDelay = 1;
      } else {
         nextDelay = Math.min(lastDelay * backoffMultiplier, maxDelay);
      }

      return nextDelay;
   }

   /**
    * Wrapper around the standard receiver context that provides federation specific entry
    * points and customizes inbound delivery handling for this Queue receiver.
    */
   private class AMQPFederatedQueueDeliveryReceiver extends ProtonServerReceiverContext {

      private final SimpleString cachedFqqn;

      private final Queue localQueue;

      private MessageReader coreMessageReader;

      private MessageReader coreLargeMessageReader;

      /**
       * Creates the federation receiver instance.
       *
       * @param session
       *    The server session context bound to the receiver instance.
       * @param consumerInfo
       *     The {@link FederationConsumerInfo} that defines the consumer being created.
       * @param receiver
       *    The proton receiver that will be wrapped in this server context instance.
       * @param creditRunnable
       *    The {@link Runnable} to provide to the base class for managing link credit.
       */
      AMQPFederatedQueueDeliveryReceiver(Queue localQueue, Receiver receiver) {
         super(session.getSessionSPI(), session.getAMQPConnectionContext(), session, receiver);

         this.localQueue = localQueue;
         this.cachedFqqn = SimpleString.of(consumerInfo.getFqqn());
      }

      @Override
      public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
         super.close(remoteLinkClose);

         if (remoteLinkClose && remoteCloseHandler != null) {
            try {
               remoteCloseHandler.accept(AMQPFederationQueueConsumer.this);
            } catch (Exception e) {
               logger.debug("User remote closed handler threw error: ", e);
            } finally {
               remoteCloseHandler = null;
            }
         }
      }

      @Override
      public void initialize() throws Exception {
         initialized = true;

         final Target target = (Target) receiver.getRemoteTarget();

         // Match the settlement mode of the remote instead of relying on the default of MIXED.
         receiver.setSenderSettleMode(receiver.getRemoteSenderSettleMode());

         // We don't currently support SECOND so enforce that the answer is always FIRST
         receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

         // the target will have an address and it will naturally have a Target otherwise
         // the remote is misbehaving and we close it.
         if (target == null || target.getAddress() == null || target.getAddress().isEmpty()) {
            throw new ActiveMQAMQPInternalErrorException("Remote should have sent an valid Target but we got: " + target);
         }

         address = SimpleString.of(target.getAddress());
         defRoutingType = getRoutingType(target.getCapabilities(), address);

         try {
            final QueueQueryResult result = sessionSPI.queueQuery(address, defRoutingType, false);

            // We initiated this link so the target should refer to an queue that definitely exists
            // however there is a chance the queue was removed in the interim.
            if (!result.isExists()) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist(address.toString());
            }
         } catch (ActiveMQAMQPNotFoundException e) {
            throw e;
         } catch (Exception e) {
            logger.debug(e.getMessage(), e);
            throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
         }

         flow();
      }

      @Override
      protected MessageReader trySelectMessageReader(Receiver receiver, Delivery delivery) {
         if (delivery.getMessageFormat() == AMQP_TUNNELED_CORE_MESSAGE_FORMAT) {
            return coreMessageReader != null ?
               coreMessageReader : (coreMessageReader = new AMQPTunneledCoreMessageReader(this));
         } else if (delivery.getMessageFormat() == AMQP_TUNNELED_CORE_LARGE_MESSAGE_FORMAT) {
            return coreLargeMessageReader != null ?
               coreLargeMessageReader : (coreLargeMessageReader = new AMQPTunneledCoreLargeMessageReader(this));
         } else {
            return super.trySelectMessageReader(receiver, delivery);
         }
      }

      @Override
      protected void actualDelivery(Message message, Delivery delivery, DeliveryAnnotations deliveryAnnotations, Receiver receiver, Transaction tx) {
         try {
            if (logger.isTraceEnabled()) {
               logger.trace("AMQP Federation {} queue consumer {} dispatching incoming message: {}",
                            federation.getName(), consumerInfo, message);
            }

            final Message theMessage = transformer.transform(message);

            if (theMessage != message && logger.isTraceEnabled()) {
               logger.trace("The transformer {} replaced the original message {} with a new instance {}",
                            transformer, message, theMessage);
            }

            signalBeforeFederationConsumerMessageHandled(theMessage);
            sessionSPI.serverSend(this, tx, receiver, delivery, cachedFqqn, routingContext, theMessage);
            signalAfterFederationConsumerMessageHandled(theMessage);
         } catch (Exception e) {
            logger.warn("Inbound delivery for {} encountered an error: {}", consumerInfo, e.getMessage(), e);
            deliveryFailed(delivery, receiver, e);
         }
      }

      @Override
      protected Runnable createCreditRunnable(AMQPConnectionContext connection) {
         // We defer to the configuration instance as opposed to the base class version that reads
         // from the connection this allows us to defer to configured policy properties that specify
         // credit. This also allows consumers created on the remote side of a federation connection
         // to read from properties sent from the federation source that indicate the values that are
         // configured on the local side.
         if (configuration.getReceiverCredits() > 0) {
            return createCreditRunnable(configuration.getReceiverCredits(), configuration.getReceiverCreditsLow(), receiver, connection, this);
         } else {
            return this::checkIfCreditTopUpNeeded;
         }
      }

      @Override
      protected int getConfiguredMinLargeMessageSize(AMQPConnectionContext connection) {
         // Looks at policy properties first before looking at federation configuration and finally
         // going to the base connection context to read the URI configuration.
         return configuration.getLargeMessageThreshold();
      }

      // Credit handling here kicks in when the connection is configured for zero link credit and
      // we want to then batch credit to the remote only when there is no local pending messages
      // which implies the local consumers are keeping up and we can pull more across.

      private final AtomicBoolean creditTopUpInProgress = new AtomicBoolean();

      private final Runnable checkForNoBacklogRunnable = this::checkForNoBacklogOnQueue;
      private final Runnable performCreditTopUpRunnable = this::performCreditTopUp;

      private int lastBacklogCheckDelay;

      private void checkIfCreditTopUpNeeded() {
         if (!connection.isHandler()) {
            connection.runLater(creditRunnable);
            return;
         }

         if (receiver.getCredit() + AMQPFederatedQueueDeliveryReceiver.this.pendingSettles <= 0 && !creditTopUpInProgress.get()) {
            // We don't need more scheduled tasks stacking up trying to issue a new
            // batch of credit so lets gate this now so they give up.
            creditTopUpInProgress.set(true);

            // Move to the Queue executor to ensure we get a proper read on the state of pending messages.
            localQueue.getExecutor().execute(checkForNoBacklogRunnable);
         }
      }

      private void checkForNoBacklogOnQueue() {
         // Only when there is no backlog do we grant new credit, otherwise we must wait until
         // the local backlog is zero. The top up must be run from the connection executor.
         if (localQueue.getPendingMessageCount() == 0) {
            lastBacklogCheckDelay = 0;
            connection.runLater(performCreditTopUpRunnable);
         } else {
            lastBacklogCheckDelay = caclulateNextDelay(lastBacklogCheckDelay, DEFAULT_PENDING_MSG_CHECK_BACKOFF_MULTIPLIER, DEFAULT_PENDING_MSG_CHECK_MAX_DELAY);

            federation.getScheduler().schedule(() -> {
               localQueue.getExecutor().execute(checkForNoBacklogRunnable);
            }, lastBacklogCheckDelay, TimeUnit.SECONDS);
         }
      }

      private void performCreditTopUp() {
         connection.requireInHandler();

         if (receiver.getLocalState() != EndpointState.ACTIVE) {
            return; // Closed before this was triggered.
         }

         receiver.flow(configuration.getPullReceiverBatchSize());
         connection.instantFlush();
         lastBacklogCheckDelay = 0;
         creditTopUpInProgress.set(false);
      }
   }
}
