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
package org.apache.activemq.artemis.protocol.amqp.proton;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.server.AddressQueryResult;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.converter.CoreAmqpConverter;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPIllegalStateException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPNotFoundException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPResourceLimitExceededException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.proton.transaction.ProtonTransactionImpl;
import org.apache.activemq.artemis.protocol.amqp.util.NettyReadable;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.selector.filter.FilterException;
import org.apache.activemq.artemis.selector.impl.SelectorParser;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;
import org.jboss.logging.Logger;

/**
 * TODO: Merge {@link ProtonServerSenderContext} and {@link org.apache.activemq.artemis.protocol.amqp.client.ProtonClientSenderContext} once we support 'global' link names. The split is a workaround for outgoing links
 */
public class ProtonServerSenderContext extends ProtonInitializable implements ProtonDeliveryHandler {

   private static final Logger log = Logger.getLogger(ProtonServerSenderContext.class);

   private static final Symbol COPY = Symbol.valueOf("copy");
   private static final Symbol TOPIC = Symbol.valueOf("topic");
   private static final Symbol QUEUE = Symbol.valueOf("queue");
   private static final Symbol SHARED = Symbol.valueOf("shared");
   private static final Symbol GLOBAL = Symbol.valueOf("global");

   private final ConnectionFlushIOCallback connectionFlusher = new ConnectionFlushIOCallback();

   private Consumer brokerConsumer;

   protected final AMQPSessionContext protonSession;
   protected final Sender sender;
   protected final AMQPConnectionContext connection;
   protected boolean closed = false;
   protected final AMQPSessionCallback sessionSPI;
   private boolean multicast;
   //todo get this from somewhere
   private RoutingType defaultRoutingType = RoutingType.ANYCAST;
   private RoutingType routingTypeToUse = defaultRoutingType;
   private boolean shared = false;
   private boolean global = false;
   private boolean isVolatile = false;
   private boolean preSettle;
   private SimpleString tempQueueName;

   public ProtonServerSenderContext(AMQPConnectionContext connection,
                                    Sender sender,
                                    AMQPSessionContext protonSession,
                                    AMQPSessionCallback server) {
      super();
      this.connection = connection;
      this.sender = sender;
      this.protonSession = protonSession;
      this.sessionSPI = server;
   }

   public Object getBrokerConsumer() {
      return brokerConsumer;
   }

   @Override
   public void onFlow(int currentCredits, boolean drain) {
      sessionSPI.onFlowConsumer(brokerConsumer, currentCredits, drain);
   }

   public Sender getSender() {
      return sender;
   }

   /*
    * start the session
    */
   public void start() throws ActiveMQAMQPException {
      sessionSPI.start();
      // protonSession.getServerSession().start();

      // todo add flow control
      try {
         // to do whatever you need to make the broker start sending messages to the consumer
         // this could be null if a link reattach has happened
         if (brokerConsumer != null) {
            sessionSPI.startSender(brokerConsumer);
         }
         // protonSession.getServerSession().receiveConsumerCredits(consumerID, -1);
      } catch (Exception e) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorStartingConsumer(e.getMessage());
      }
   }

   /**
    * create the actual underlying ActiveMQ Artemis Server Consumer
    */
   @SuppressWarnings("unchecked")
   @Override
   public void initialise() throws Exception {
      super.initialise();

      Source source = (Source) sender.getRemoteSource();
      SimpleString queue = null;
      String selector = null;
      final Map<Symbol, Object> supportedFilters = new HashMap<>();

      // Match the settlement mode of the remote instead of relying on the default of MIXED.
      sender.setSenderSettleMode(sender.getRemoteSenderSettleMode());

      // We don't currently support SECOND so enforce that the answer is anlways FIRST
      sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);

      if (source != null) {
         // We look for message selectors on every receiver, while in other cases we might only
         // consume the filter depending on the subscription type.
         Map.Entry<Symbol, DescribedType> filter = AmqpSupport.findFilter(source.getFilter(), AmqpSupport.JMS_SELECTOR_FILTER_IDS);
         if (filter != null) {
            selector = filter.getValue().getDescribed().toString();
            // Validate the Selector.
            try {
               SelectorParser.parse(selector);
            } catch (FilterException e) {
               throw new ActiveMQAMQPException(AmqpError.INVALID_FIELD, "Invalid filter", ActiveMQExceptionType.INVALID_FILTER_EXPRESSION);
            }

            supportedFilters.put(filter.getKey(), filter.getValue());
         }
      }

      if (source == null) {
         // Attempt to recover a previous subscription happens when a link reattach happens on a
         // subscription queue
         String clientId = getClientId();
         String pubId = sender.getName();
         global = hasRemoteDesiredCapability(sender, GLOBAL);
         queue = createQueueName(connection.isUseCoreSubscriptionNaming(), clientId, pubId, true, global, false);
         QueueQueryResult result = sessionSPI.queueQuery(queue, RoutingType.MULTICAST, false);
         multicast = true;
         routingTypeToUse = RoutingType.MULTICAST;

         // Once confirmed that the address exists we need to return a Source that reflects
         // the lifetime policy and capabilities of the new subscription.
         if (result.isExists()) {
            source = new org.apache.qpid.proton.amqp.messaging.Source();
            source.setAddress(queue.toString());
            source.setDurable(TerminusDurability.UNSETTLED_STATE);
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setDistributionMode(COPY);
            source.setCapabilities(TOPIC);

            SimpleString filterString = result.getFilterString();
            if (filterString != null) {
               selector = filterString.toString();
               boolean noLocal = false;

               String remoteContainerId = sender.getSession().getConnection().getRemoteContainer();
               String noLocalFilter = MessageUtil.CONNECTION_ID_PROPERTY_NAME.toString() + "<>'" + remoteContainerId + "'";

               if (selector.endsWith(noLocalFilter)) {
                  if (selector.length() > noLocalFilter.length()) {
                     noLocalFilter = " AND " + noLocalFilter;
                     selector = selector.substring(0, selector.length() - noLocalFilter.length());
                  } else {
                     selector = null;
                  }

                  noLocal = true;
               }

               if (noLocal) {
                  supportedFilters.put(AmqpSupport.NO_LOCAL_NAME, AmqpNoLocalFilter.NO_LOCAL);
               }

               if (selector != null && !selector.trim().isEmpty()) {
                  supportedFilters.put(AmqpSupport.JMS_SELECTOR_NAME, new AmqpJmsSelectorFilter(selector));
               }
            }

            sender.setSource(source);
         } else {
            throw new ActiveMQAMQPNotFoundException("Unknown subscription link: " + sender.getName());
         }
      } else if (source.getDynamic()) {
         // if dynamic we have to create the node (queue) and set the address on the target, the
         // node is temporary and  will be deleted on closing of the session
         queue = SimpleString.toSimpleString(java.util.UUID.randomUUID().toString());
         tempQueueName = queue;
         try {
            sessionSPI.createTemporaryQueue(queue, RoutingType.ANYCAST);
            // protonSession.getServerSession().createQueue(queue, queue, null, true, false);
         } catch (Exception e) {
            throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingTemporaryQueue(e.getMessage());
         }
         source.setAddress(queue.toString());
      } else {
         SimpleString addressToUse;
         SimpleString queueNameToUse = null;
         shared = hasCapabilities(SHARED, source);
         global = hasCapabilities(GLOBAL, source);

         //find out if we have an address made up of the address and queue name, if yes then set queue name
         if (CompositeAddress.isFullyQualified(source.getAddress())) {
            CompositeAddress compositeAddress = CompositeAddress.getQueueName(source.getAddress());
            addressToUse = new SimpleString(compositeAddress.getAddress());
            queueNameToUse = new SimpleString(compositeAddress.getQueueName());
         } else {
            addressToUse = new SimpleString(source.getAddress());
         }
         //check to see if the client has defined how we act
         boolean clientDefined = hasCapabilities(TOPIC, source) || hasCapabilities(QUEUE, source);
         if (clientDefined) {
            multicast = hasCapabilities(TOPIC, source);
            AddressQueryResult addressQueryResult = null;
            try {
               addressQueryResult = sessionSPI.addressQuery(addressToUse, multicast ? RoutingType.MULTICAST : RoutingType.ANYCAST, true);
            } catch (ActiveMQSecurityException e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingConsumer(e.getMessage());
            } catch (ActiveMQAMQPException e) {
               throw e;
            } catch (Exception e) {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }

            if (!addressQueryResult.isExists()) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.sourceAddressDoesntExist();
            }

            Set<RoutingType> routingTypes = addressQueryResult.getRoutingTypes();

            //if the client defines 1 routing type and the broker another then throw an exception
            if (multicast && !routingTypes.contains(RoutingType.MULTICAST)) {
               throw new ActiveMQAMQPIllegalStateException("Address " + addressToUse + " is not configured for topic support");
            } else if (!multicast && !routingTypes.contains(RoutingType.ANYCAST)) {
               throw new ActiveMQAMQPIllegalStateException("Address " + addressToUse + " is not configured for queue support");
            }
         } else {
            // if not we look up the address
            AddressQueryResult addressQueryResult = null;
            try {
               addressQueryResult = sessionSPI.addressQuery(addressToUse, defaultRoutingType, true);
            } catch (ActiveMQSecurityException e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingConsumer(e.getMessage());
            } catch (ActiveMQAMQPException e) {
               throw e;
            } catch (Exception e) {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }

            if (!addressQueryResult.isExists()) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.sourceAddressDoesntExist();
            }

            Set<RoutingType> routingTypes = addressQueryResult.getRoutingTypes();
            if (routingTypes.contains(RoutingType.MULTICAST) && routingTypes.size() == 1) {
               multicast = true;
            } else {
               //todo add some checks if both routing types are supported
               multicast = false;
            }
         }
         routingTypeToUse = multicast ? RoutingType.MULTICAST : RoutingType.ANYCAST;
         // if not dynamic then we use the target's address as the address to forward the
         // messages to, however there has to be a queue bound to it so we need to check this.
         if (multicast) {
            Map.Entry<Symbol, DescribedType> filter = AmqpSupport.findFilter(source.getFilter(), AmqpSupport.NO_LOCAL_FILTER_IDS);
            if (filter != null) {
               String remoteContainerId = sender.getSession().getConnection().getRemoteContainer();
               String noLocalFilter = MessageUtil.CONNECTION_ID_PROPERTY_NAME.toString() + "<>'" + remoteContainerId + "'";
               if (selector != null) {
                  selector += " AND " + noLocalFilter;
               } else {
                  selector = noLocalFilter;
               }

               supportedFilters.put(filter.getKey(), filter.getValue());
            }

            queue = getMatchingQueue(queueNameToUse, addressToUse, RoutingType.MULTICAST);
            SimpleString simpleStringSelector = SimpleString.toSimpleString(selector);

            //if the address specifies a broker configured queue then we always use this, treat it as a queue
            if (queue != null) {
               multicast = false;
            } else if (TerminusDurability.UNSETTLED_STATE.equals(source.getDurable()) || TerminusDurability.CONFIGURATION.equals(source.getDurable())) {

               // if we are a subscription and durable create a durable queue using the container
               // id and link name
               String clientId = getClientId();
               String pubId = sender.getName();
               queue = createQueueName(connection.isUseCoreSubscriptionNaming(), clientId, pubId, shared, global, false);
               QueueQueryResult result = sessionSPI.queueQuery(queue, routingTypeToUse, false);
               if (result.isExists()) {
                  // If a client reattaches to a durable subscription with a different no-local
                  // filter value, selector or address then we must recreate the queue (JMS semantics).
                  if (!Objects.equals(result.getFilterString(), simpleStringSelector) || (sender.getSource() != null && !sender.getSource().getAddress().equals(result.getAddress().toString()))) {

                     if (result.getConsumerCount() == 0) {
                        sessionSPI.deleteQueue(queue);
                        sessionSPI.createUnsharedDurableQueue(addressToUse, RoutingType.MULTICAST, queue, simpleStringSelector);
                     } else {
                        throw new ActiveMQAMQPIllegalStateException("Unable to recreate subscription, consumers already exist");
                     }
                  }
               } else {
                  if (shared) {
                     sessionSPI.createSharedDurableQueue(addressToUse, RoutingType.MULTICAST, queue, simpleStringSelector);
                  } else {
                     sessionSPI.createUnsharedDurableQueue(addressToUse, RoutingType.MULTICAST, queue, simpleStringSelector);
                  }
               }
            } else {
               // otherwise we are a volatile subscription
               isVolatile = true;
               if (shared && sender.getName() != null) {
                  queue = createQueueName(connection.isUseCoreSubscriptionNaming(), getClientId(), sender.getName(), shared, global, isVolatile);
                  QueueQueryResult result = sessionSPI.queueQuery(queue, routingTypeToUse, false);
                  if (!(result.isExists() && Objects.equals(result.getAddress(), addressToUse) && Objects.equals(result.getFilterString(), simpleStringSelector))) {
                     sessionSPI.createSharedVolatileQueue(addressToUse, RoutingType.MULTICAST, queue, simpleStringSelector);
                  }
               } else {
                  queue = SimpleString.toSimpleString(java.util.UUID.randomUUID().toString());
                  tempQueueName = queue;
                  try {
                     sessionSPI.createTemporaryQueue(addressToUse, queue, RoutingType.MULTICAST, simpleStringSelector);
                  } catch (Exception e) {
                     throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingTemporaryQueue(e.getMessage());
                  }
               }
            }
         } else {
            if (queueNameToUse != null) {
               SimpleString matchingAnycastQueue = getMatchingQueue(queueNameToUse, addressToUse, RoutingType.ANYCAST);
               if (matchingAnycastQueue != null) {
                  queue = matchingAnycastQueue;
               } else {
                  throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.sourceAddressDoesntExist();
               }
            } else {
               SimpleString matchingAnycastQueue = sessionSPI.getMatchingQueue(addressToUse, RoutingType.ANYCAST);
               if (matchingAnycastQueue != null) {
                  queue = matchingAnycastQueue;
               } else {
                  queue = addressToUse;
               }
            }

         }

         if (queue == null) {
            throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.sourceAddressNotSet();
         }

         try {
            if (!sessionSPI.queueQuery(queue, routingTypeToUse, !multicast).isExists()) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.sourceAddressDoesntExist();
            }
         } catch (ActiveMQAMQPNotFoundException e) {
            throw e;
         } catch (Exception e) {
            throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
         }
      }

      // Detect if sender is in pre-settle mode.
      preSettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;

      // We need to update the source with any filters we support otherwise the client
      // is free to consider the attach as having failed if we don't send back what we
      // do support or if we send something we don't support the client won't know we
      // have not honored what it asked for.
      source.setFilter(supportedFilters.isEmpty() ? null : supportedFilters);

      boolean browseOnly = !multicast && source.getDistributionMode() != null && source.getDistributionMode().equals(COPY);
      try {
         brokerConsumer = (Consumer) sessionSPI.createSender(this, queue, multicast ? null : selector, browseOnly);
      } catch (ActiveMQAMQPResourceLimitExceededException e1) {
         throw new ActiveMQAMQPResourceLimitExceededException(e1.getMessage());
      } catch (ActiveMQSecurityException e) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingConsumer(e.getMessage());
      } catch (Exception e) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingConsumer(e.getMessage());
      }
   }

   private SimpleString getMatchingQueue(SimpleString queueName, SimpleString address, RoutingType routingType) throws Exception {
      if (queueName != null) {
         QueueQueryResult result = sessionSPI.queueQuery(queueName, routingType, false);
         if (!result.isExists()) {
            throw new ActiveMQAMQPNotFoundException("Queue: '" + queueName + "' does not exist");
         } else {
            if (!result.getAddress().equals(address)) {
               throw new ActiveMQAMQPNotFoundException("Queue: '" + queueName + "' does not exist for address '" + address + "'");
            }
            return sessionSPI.getMatchingQueue(address, queueName, routingType);
         }
      }
      return null;
   }

   protected String getClientId() {
      return connection.getRemoteContainer();
   }

   /*
    * close the session
    */
   @Override
   public void close(ErrorCondition condition) throws ActiveMQAMQPException {
      closed = true;
      if (condition != null) {
         sender.setCondition(condition);
      }
      protonSession.removeSender(sender);
      connection.lock();
      try {
         sender.close();
      } finally {
         connection.unlock();
      }
      connection.flush();

      try {
         sessionSPI.closeSender(brokerConsumer);
      } catch (Exception e) {
         log.warn(e.getMessage(), e);
         throw new ActiveMQAMQPInternalErrorException(e.getMessage());
      }
   }

   /*
    * close the session
    */
   @Override
   public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
      try {
         closed = true;
         sessionSPI.closeSender(brokerConsumer);
         // if this is a link close rather than a connection close or detach, we need to delete
         // any durable resources for say pub subs
         if (remoteLinkClose) {
            Source source = (Source) sender.getSource();
            if (source != null && source.getAddress() != null && multicast) {
               SimpleString queueName = SimpleString.toSimpleString(source.getAddress());
               QueueQueryResult result = sessionSPI.queueQuery(queueName, routingTypeToUse, false);
               if (result.isExists() && source.getDynamic()) {
                  sessionSPI.deleteQueue(queueName);
               } else {
                  if (source.getDurable() == TerminusDurability.NONE && tempQueueName != null && (source.getExpiryPolicy() == TerminusExpiryPolicy.LINK_DETACH || source.getExpiryPolicy() == TerminusExpiryPolicy.SESSION_END)) {
                     sessionSPI.removeTemporaryQueue(tempQueueName);
                  } else {
                     String clientId = getClientId();
                     String pubId = sender.getName();
                     if (pubId.contains("|")) {
                        pubId = pubId.split("\\|")[0];
                     }
                     SimpleString queue = createQueueName(connection.isUseCoreSubscriptionNaming(), clientId, pubId, shared, global, isVolatile);
                     result = sessionSPI.queueQuery(queue, multicast ? RoutingType.MULTICAST : RoutingType.ANYCAST, false);
                     //only delete if it isn't volatile and has no consumers
                     if (result.isExists() && !isVolatile && result.getConsumerCount() == 0) {
                        sessionSPI.deleteQueue(queue);
                     }
                  }
               }
            } else if (source != null && source.getDynamic() && (source.getExpiryPolicy() == TerminusExpiryPolicy.LINK_DETACH || source.getExpiryPolicy() == TerminusExpiryPolicy.SESSION_END)) {
               try {
                  sessionSPI.removeTemporaryQueue(SimpleString.toSimpleString(source.getAddress()));
               } catch (Exception e) {
                  //ignore on close, its temp anyway and will be removed later
               }
            }
         }
      } catch (Exception e) {
         log.warn(e.getMessage(), e);
         throw new ActiveMQAMQPInternalErrorException(e.getMessage());
      }
   }

   @Override
   public void onMessage(Delivery delivery) throws ActiveMQAMQPException {
      if (closed) {
         return;
      }

      OperationContext oldContext = sessionSPI.recoverContext();

      try {
         Message message = ((MessageReference) delivery.getContext()).getMessage();
         DeliveryState remoteState = delivery.getRemoteState();

         boolean settleImmediate = true;
         if (remoteState instanceof Accepted) {
            // this can happen in the twice ack mode, that is the receiver accepts and settles separately
            // acking again would show an exception but would have no negative effect but best to handle anyway.
            if (delivery.isSettled()) {
               return;
            }
            // we have to individual ack as we can't guarantee we will get the delivery updates
            // (including acks) in order from dealer, a performance hit but a must
            try {
               sessionSPI.ack(null, brokerConsumer, message);
            } catch (Exception e) {
               log.warn(e.toString(), e);
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorAcknowledgingMessage(message.toString(), e.getMessage());
            }
         } else if (remoteState instanceof TransactionalState) {
            // When the message arrives with a TransactionState disposition the ack should
            // enlist the message into the transaction associated with the given txn ID.
            TransactionalState txState = (TransactionalState) remoteState;
            ProtonTransactionImpl tx = (ProtonTransactionImpl) this.sessionSPI.getTransaction(txState.getTxnId(), false);

            if (txState.getOutcome() != null) {
               settleImmediate = false;
               Outcome outcome = txState.getOutcome();
               if (outcome instanceof Accepted) {
                  if (!delivery.remotelySettled()) {
                     TransactionalState txAccepted = new TransactionalState();
                     txAccepted.setOutcome(Accepted.getInstance());
                     txAccepted.setTxnId(txState.getTxnId());
                     delivery.disposition(txAccepted);
                  }
                  // we have to individual ack as we can't guarantee we will get the delivery
                  // (including acks) in order from dealer, a performance hit but a must
                  try {
                     sessionSPI.ack(tx, brokerConsumer, message);
                     tx.addDelivery(delivery, this);
                  } catch (Exception e) {
                     throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorAcknowledgingMessage(message.toString(), e.getMessage());
                  }
               }
            }
         } else if (remoteState instanceof Released) {
            try {
               sessionSPI.cancel(brokerConsumer, message, false);
            } catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
         } else if (remoteState instanceof Rejected) {
            try {
               sessionSPI.reject(brokerConsumer, message);
            } catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
         } else if (remoteState instanceof Modified) {
            try {
               Modified modification = (Modified) remoteState;

               if (Boolean.TRUE.equals(modification.getUndeliverableHere())) {
                  message.rejectConsumer(brokerConsumer.sequentialID());
               }

               if (Boolean.TRUE.equals(modification.getDeliveryFailed())) {
                  sessionSPI.cancel(brokerConsumer, message, true);
               } else {
                  sessionSPI.cancel(brokerConsumer, message, false);
               }
            } catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
         } else {
            log.debug("Received null or unknown disposition for delivery update: " + remoteState);
            return;
         }

         if (!preSettle) {
            protonSession.replaceTag(delivery.getTag());
         }

         if (settleImmediate) {
            delivery.settle();
         }

      } finally {
         sessionSPI.afterIO(connectionFlusher);
         sessionSPI.resetContext(oldContext);
      }
   }

   private final class ConnectionFlushIOCallback implements IOCallback {
      @Override
      public void done() {
         connection.flush();
      }

      @Override
      public void onError(int errorCode, String errorMessage) {
         connection.flush();
      }
   }

   public void settle(Delivery delivery) {
      connection.lock();
      try {
         delivery.settle();
      } finally {
         connection.unlock();
      }
   }

   public synchronized void checkState() {
      sessionSPI.resumeDelivery(brokerConsumer);
   }

   /**
    * handle an out going message from ActiveMQ Artemis, send via the Proton Sender
    */
   public int deliverMessage(MessageReference messageReference, int deliveryCount, Connection transportConnection) throws Exception {

      if (closed) {
         return 0;
      }

      AMQPMessage message = CoreAmqpConverter.checkAMQP(messageReference.getMessage());
      sessionSPI.invokeOutgoing(message, (ActiveMQProtonRemotingConnection) transportConnection.getProtocolConnection());

      // we only need a tag if we are going to settle later
      byte[] tag = preSettle ? new byte[0] : protonSession.getTag();

      // Let the Message decide how to present the message bytes
      ReadableBuffer sendBuffer = message.getSendBuffer(deliveryCount);
      boolean releaseRequired = sendBuffer instanceof NettyReadable;

      try {
         int size = sendBuffer.remaining();

         while (!connection.tryLock(1, TimeUnit.SECONDS)) {
            if (closed || sender.getLocalState() == EndpointState.CLOSED) {
               // If we're waiting on the connection lock, the link might be in the process of closing.  If this happens
               // we return.
               return 0;
            } else {
               if (log.isDebugEnabled()) {
                  log.debug("Couldn't get lock on deliverMessage " + this);
               }
            }
         }

         try {
            final Delivery delivery;
            delivery = sender.delivery(tag, 0, tag.length);
            delivery.setMessageFormat((int) message.getMessageFormat());
            delivery.setContext(messageReference);

            if (releaseRequired) {
               sender.send(sendBuffer);
               // Above send copied, so release now if needed
               releaseRequired = false;
               ((NettyReadable) sendBuffer).getByteBuf().release();
            } else {
               // Don't have pooled content, no need to release or copy.
               sender.sendNoCopy(sendBuffer);
            }

            if (preSettle) {
               // Presettled means the client implicitly accepts any delivery we send it.
               sessionSPI.ack(null, brokerConsumer, messageReference.getMessage());
               delivery.settle();
            } else {
               sender.advance();
            }

            connection.flush();
         } finally {
            connection.unlock();
         }

         return size;
      } finally {
         if (releaseRequired) {
            ((NettyReadable) sendBuffer).getByteBuf().release();
         }
      }
   }

   private static boolean hasCapabilities(Symbol symbol, Source source) {
      if (source != null) {
         if (source.getCapabilities() != null) {
            for (Symbol cap : source.getCapabilities()) {
               if (symbol.equals(cap)) {
                  return true;
               }
            }
         }
      }
      return false;
   }

   private static boolean hasRemoteDesiredCapability(Link link, Symbol capability) {
      Symbol[] remoteDesiredCapabilities = link.getRemoteDesiredCapabilities();
      if (remoteDesiredCapabilities != null) {
         for (Symbol cap : remoteDesiredCapabilities) {
            if (capability.equals(cap)) {
               return true;
            }
         }
      }
      return false;
   }

   private static SimpleString createQueueName(boolean useCoreSubscriptionNaming,
                                         String clientId,
                                         String pubId,
                                         boolean shared,
                                         boolean global,
                                         boolean isVolatile) {
      if (useCoreSubscriptionNaming) {
         final boolean durable = !isVolatile;
         final String subscriptionName = pubId.contains("|") ? pubId.split("\\|")[0] : pubId;
         final String clientID = clientId == null || clientId.isEmpty() || global ? null : clientId;
         return ActiveMQDestination.createQueueNameForSubscription(durable, clientID, subscriptionName);
      } else {
         String queue = clientId == null || clientId.isEmpty() || global ? pubId : clientId + "." + pubId;
         if (shared) {
            if (queue.contains("|")) {
               queue = queue.split("\\|")[0];
            }
            if (isVolatile) {
               queue += ":shared-volatile";
            }
            if (global) {
               queue += ":global";
            }
         }
         return SimpleString.toSimpleString(queue);
      }
   }

   /**
    * Update link state to reflect that the previous drain attempt has completed.
    */
   public void reportDrained() {
      connection.lock();
      try {
         sender.drained();
      } finally {
         connection.unlock();
      }

      connection.flush();
   }
}
