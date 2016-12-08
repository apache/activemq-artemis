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

import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.impl.CompositeAddress;
import org.apache.activemq.artemis.core.server.AddressQueryResult;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPIllegalStateException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPNotFoundException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPResourceLimitExceededException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.util.CreditsSemaphore;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.selector.filter.FilterException;
import org.apache.activemq.artemis.selector.impl.SelectorParser;
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
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sender;
import org.jboss.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

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

   private Object brokerConsumer;

   protected final AMQPSessionContext protonSession;
   protected final Sender sender;
   protected final AMQPConnectionContext connection;
   protected boolean closed = false;
   protected final AMQPSessionCallback sessionSPI;
   private boolean multicast;
   //todo get this from somewhere
   private RoutingType defaultRoutingType = RoutingType.ANYCAST;
   protected CreditsSemaphore creditsSemaphore = new CreditsSemaphore(0);
   private RoutingType routingTypeToUse = defaultRoutingType;
   private boolean shared = false;
   private boolean global = false;
   private boolean isVolatile = false;

   public ProtonServerSenderContext(AMQPConnectionContext connection, Sender sender, AMQPSessionContext protonSession, AMQPSessionCallback server) {
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
      this.creditsSemaphore.setCredits(currentCredits);
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
      String queue = null;
      String selector = null;
      final Map<Symbol, Object> supportedFilters = new HashMap<>();

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
         queue = createQueueName(clientId, pubId, true, global, false);
         QueueQueryResult result = sessionSPI.queueQuery(queue, RoutingType.MULTICAST, false);
         multicast = true;
         routingTypeToUse = RoutingType.MULTICAST;

         // Once confirmed that the address exists we need to return a Source that reflects
         // the lifetime policy and capabilities of the new subscription.
         if (result.isExists()) {
            source = new org.apache.qpid.proton.amqp.messaging.Source();
            source.setAddress(queue);
            source.setDurable(TerminusDurability.UNSETTLED_STATE);
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setDistributionMode(COPY);
            source.setCapabilities(TOPIC);

            SimpleString filterString = result.getFilterString();
            if (filterString != null) {
               selector = filterString.toString();
               boolean noLocal = false;

               String remoteContainerId = sender.getSession().getConnection().getRemoteContainer();
               String noLocalFilter = ActiveMQConnection.CONNECTION_ID_PROPERTY_NAME.toString() + "<>'" + remoteContainerId + "'";

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
         queue = java.util.UUID.randomUUID().toString();
         try {
            sessionSPI.createTemporaryQueue(queue, RoutingType.ANYCAST);
            // protonSession.getServerSession().createQueue(queue, queue, null, true, false);
         } catch (Exception e) {
            throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingTemporaryQueue(e.getMessage());
         }
         source.setAddress(queue);
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
         if (clientDefined)  {
            multicast = hasCapabilities(TOPIC, source);
            AddressInfo addressInfo = sessionSPI.getAddress(addressToUse);
            Set<RoutingType> routingTypes = addressInfo.getRoutingTypes();
            //if the client defines 1 routing type and the broker another then throw an exception
            if (multicast && !routingTypes.contains(RoutingType.MULTICAST)) {
               throw new ActiveMQAMQPIllegalStateException("Address is not configured for topic support");
            } else if (!multicast && !routingTypes.contains(RoutingType.ANYCAST)) {
               throw new ActiveMQAMQPIllegalStateException("Address is not configured for queue support");
            }
         } else {
            //if not we look up the address
            AddressQueryResult addressQueryResult = sessionSPI.addressQuery(addressToUse.toString(), defaultRoutingType, true);
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
               String noLocalFilter = ActiveMQConnection.CONNECTION_ID_PROPERTY_NAME.toString() + "<>'" + remoteContainerId + "'";
               if (selector != null) {
                  selector += " AND " + noLocalFilter;
               } else {
                  selector = noLocalFilter;
               }

               supportedFilters.put(filter.getKey(), filter.getValue());
            }


            if (queueNameToUse != null) {
               SimpleString matchingAnycastQueue = sessionSPI.getMatchingQueue(addressToUse, queueNameToUse, RoutingType.MULTICAST  );
               queue = matchingAnycastQueue.toString();
            }
            //if the address specifies a broker configured queue then we always use this, treat it as a queue
            if (queue != null) {
               multicast = false;
            } else if (TerminusDurability.UNSETTLED_STATE.equals(source.getDurable()) || TerminusDurability.CONFIGURATION.equals(source.getDurable())) {

               // if we are a subscription and durable create a durable queue using the container
               // id and link name
               String clientId = getClientId();
               String pubId = sender.getName();
               queue = createQueueName(clientId, pubId, shared, global, false);
               QueueQueryResult result = sessionSPI.queueQuery(queue, routingTypeToUse, false);

               if (result.isExists()) {
                  // If a client reattaches to a durable subscription with a different no-local
                  // filter value, selector or address then we must recreate the queue (JMS semantics).
                  if (!Objects.equals(result.getFilterString(), SimpleString.toSimpleString(selector)) ||
                     (sender.getSource() != null && !sender.getSource().getAddress().equals(result.getAddress().toString()))) {

                     if (result.getConsumerCount() == 0) {
                        sessionSPI.deleteQueue(queue);
                        sessionSPI.createUnsharedDurableQueue(source.getAddress(), RoutingType.MULTICAST, queue, selector);
                     } else {
                        throw new ActiveMQAMQPIllegalStateException("Unable to recreate subscription, consumers already exist");
                     }
                  }
               } else {
                  if (shared) {
                     sessionSPI.createSharedDurableQueue(source.getAddress(), RoutingType.MULTICAST, queue, selector);
                  } else {
                     sessionSPI.createUnsharedDurableQueue(source.getAddress(), RoutingType.MULTICAST, queue, selector);
                  }
               }
            } else {
               // otherwise we are a volatile subscription
               isVolatile = true;
               if (shared && sender.getName() != null) {
                  queue = createQueueName(getClientId(), sender.getName(), shared, global, isVolatile);
                  try {
                     sessionSPI.createSharedVolatileQueue(source.getAddress(), RoutingType.MULTICAST, queue, selector);
                  } catch (ActiveMQQueueExistsException e) {
                     //this is ok, just means its shared
                  }
               } else {
                  queue = java.util.UUID.randomUUID().toString();
                  try {
                     sessionSPI.createTemporaryQueue(source.getAddress(), queue, RoutingType.MULTICAST, selector);
                  } catch (Exception e) {
                     throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingTemporaryQueue(e.getMessage());
                  }
               }
            }
         } else {
            if (queueNameToUse != null) {
               SimpleString matchingAnycastQueue = sessionSPI.getMatchingQueue(addressToUse, queueNameToUse, RoutingType.ANYCAST);
               if (matchingAnycastQueue != null) {
                  queue = matchingAnycastQueue.toString();
               } else {
                  throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.sourceAddressDoesntExist();
               }
            } else {
               SimpleString matchingAnycastQueue = sessionSPI.getMatchingQueue(addressToUse, RoutingType.ANYCAST);
               if (matchingAnycastQueue != null) {
                  queue = matchingAnycastQueue.toString();
               } else {
                  queue = addressToUse.toString();
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

      // We need to update the source with any filters we support otherwise the client
      // is free to consider the attach as having failed if we don't send back what we
      // do support or if we send something we don't support the client won't know we
      // have not honored what it asked for.
      source.setFilter(supportedFilters.isEmpty() ? null : supportedFilters);

      boolean browseOnly = !multicast && source.getDistributionMode() != null && source.getDistributionMode().equals(COPY);
      try {
         brokerConsumer = sessionSPI.createSender(this, queue, multicast ? null : selector, browseOnly);
      } catch (ActiveMQAMQPResourceLimitExceededException e1) {
         throw new ActiveMQAMQPResourceLimitExceededException(e1.getMessage());
      } catch (Exception e) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingConsumer(e.getMessage());
      }
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
      synchronized (connection.getLock()) {
         sender.close();
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
         sessionSPI.closeSender(brokerConsumer);
         // if this is a link close rather than a connection close or detach, we need to delete
         // any durable resources for say pub subs
         if (remoteLinkClose) {
            Source source = (Source) sender.getSource();
            if (source != null && source.getAddress() != null && multicast) {
               String queueName = source.getAddress();
               QueueQueryResult result = sessionSPI.queueQuery(queueName, routingTypeToUse,  false);
               if (result.isExists() && source.getDynamic()) {
                  sessionSPI.deleteQueue(queueName);
               } else {
                  String clientId = getClientId();
                  String pubId = sender.getName();
                  if (pubId.contains("|")) {
                     pubId = pubId.split("\\|")[0];
                  }
                  String queue = createQueueName(clientId, pubId, shared, global, isVolatile);
                  result = sessionSPI.queueQuery(queue, multicast ? RoutingType.MULTICAST : RoutingType.ANYCAST, false);
                  //only delete if it isn't volatile and has no consumers
                  if (result.isExists() && !isVolatile && result.getConsumerCount() == 0) {
                     sessionSPI.deleteQueue(queue);
                  }
               }
            } else if (source != null && source.getDynamic() && (source.getExpiryPolicy() == TerminusExpiryPolicy.LINK_DETACH || source.getExpiryPolicy() == TerminusExpiryPolicy.SESSION_END)) {
               try {
                  sessionSPI.removeTemporaryQueue(source.getAddress());
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
      Object message = delivery.getContext();

      boolean preSettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;

      DeliveryState remoteState = delivery.getRemoteState();

      if (remoteState != null) {
         // If we are transactional then we need ack if the msg has been accepted
         if (remoteState instanceof TransactionalState) {

            TransactionalState txState = (TransactionalState) remoteState;
            Transaction tx = this.sessionSPI.getTransaction(txState.getTxnId());
            if (txState.getOutcome() != null) {
               Outcome outcome = txState.getOutcome();
               if (outcome instanceof Accepted) {
                  if (!delivery.remotelySettled()) {
                     TransactionalState txAccepted = new TransactionalState();
                     txAccepted.setOutcome(Accepted.getInstance());
                     txAccepted.setTxnId(txState.getTxnId());

                     delivery.disposition(txAccepted);
                  }
                  // we have to individual ack as we can't guarantee we will get the delivery
                  // updates (including acks) in order
                  // from dealer, a perf hit but a must
                  try {
                     sessionSPI.ack(tx, brokerConsumer, message);
                  } catch (Exception e) {
                     throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorAcknowledgingMessage(message.toString(), e.getMessage());
                  }
               }
            }
         } else if (remoteState instanceof Accepted) {
            // we have to individual ack as we can't guarantee we will get the delivery updates
            // (including acks) in order
            // from dealer, a perf hit but a must
            try {
               sessionSPI.ack(null, brokerConsumer, message);
            } catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorAcknowledgingMessage(message.toString(), e.getMessage());
            }
         } else if (remoteState instanceof Released) {
            try {
               sessionSPI.cancel(brokerConsumer, message, false);
            } catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
         } else if (remoteState instanceof Rejected || remoteState instanceof Modified) {
            try {
               sessionSPI.cancel(brokerConsumer, message, true);
            } catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
         }
         // todo add tag caching
         if (!preSettle) {
            protonSession.replaceTag(delivery.getTag());
         }

         synchronized (connection.getLock()) {
            delivery.settle();
            sender.offer(1);
         }

      } else {
         // todo not sure if we need to do anything here
      }
   }

   public synchronized void checkState() {
      sessionSPI.resumeDelivery(brokerConsumer);
   }

   /**
    * handle an out going message from ActiveMQ Artemis, send via the Proton Sender
    */
   public int deliverMessage(Object message, int deliveryCount) throws Exception {
      if (closed) {
         System.err.println("Message can't be delivered as it's closed");
         return 0;
      }

      if (!creditsSemaphore.tryAcquire()) {
         try {
            creditsSemaphore.acquire();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // nothing to be done here.. we just keep going
            throw new IllegalStateException(e.getMessage(), e);
         }
      }

      // presettle means we can settle the message on the dealer side before we send it, i.e.
      // for browsers
      boolean preSettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;

      // we only need a tag if we are going to settle later
      byte[] tag = preSettle ? new byte[0] : protonSession.getTag();

      ByteBuf nettyBuffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1024);
      try {
         long messageFormat = 0;

         // Encode the Server Message into the given Netty Buffer as an AMQP
         // Message transformed from the internal message model.
         try {
            messageFormat = sessionSPI.encodeMessage(message, deliveryCount, new NettyWritable(nettyBuffer));
         } catch (Throwable e) {
            log.warn(e.getMessage(), e);
            throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
         }

         int size = nettyBuffer.writerIndex();

         synchronized (connection.getLock()) {
            if (sender.getLocalState() == EndpointState.CLOSED) {
               return 0;
            }
            final Delivery delivery;
            delivery = sender.delivery(tag, 0, tag.length);
            delivery.setMessageFormat((int) messageFormat);
            delivery.setContext(message);

            // this will avoid a copy.. patch provided by Norman using buffer.array()
            sender.send(nettyBuffer.array(), nettyBuffer.arrayOffset() + nettyBuffer.readerIndex(), nettyBuffer.readableBytes());

            if (preSettle) {
               // Presettled means the client implicitly accepts any delivery we send it.
               sessionSPI.ack(null, brokerConsumer, message);
               delivery.settle();
            } else {
               sender.advance();
            }
         }

         connection.flush();

         return size;
      } finally {
         nettyBuffer.release();
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

   private static String createQueueName(String clientId, String pubId, boolean shared, boolean global, boolean isVolatile) {
      String queue = clientId == null || clientId.isEmpty() ? pubId : clientId + "." + pubId;
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
      return queue;
   }
}
