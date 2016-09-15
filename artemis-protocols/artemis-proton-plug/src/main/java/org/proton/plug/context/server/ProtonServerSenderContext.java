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
package org.proton.plug.context.server;

import java.util.Map;
import java.util.Objects;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
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
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.jboss.logging.Logger;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.AmqpSupport;
import org.proton.plug.context.AbstractConnectionContext;
import org.proton.plug.context.AbstractProtonContextSender;
import org.proton.plug.context.AbstractProtonSessionContext;
import org.proton.plug.context.ProtonPlugSender;
import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.exceptions.ActiveMQAMQPIllegalStateException;
import org.proton.plug.exceptions.ActiveMQAMQPInternalErrorException;
import org.proton.plug.exceptions.ActiveMQAMQPNotFoundException;
import org.proton.plug.logger.ActiveMQAMQPProtocolMessageBundle;

import static org.proton.plug.AmqpSupport.JMS_SELECTOR_FILTER_IDS;
import static org.proton.plug.AmqpSupport.findFilter;

public class ProtonServerSenderContext extends AbstractProtonContextSender implements ProtonPlugSender {

   private static final Logger log = Logger.getLogger(ProtonServerSenderContext.class);

   private static final Symbol SELECTOR = Symbol.getSymbol("jms-selector");
   private static final Symbol COPY = Symbol.valueOf("copy");
   private static final Symbol TOPIC = Symbol.valueOf("topic");

   private Object brokerConsumer;

   public ProtonServerSenderContext(AbstractConnectionContext connection,
                                    Sender sender,
                                    AbstractProtonSessionContext protonSession,
                                    AMQPSessionCallback server) {
      super(connection, sender, protonSession, server);
   }

   public Object getBrokerConsumer() {
      return brokerConsumer;
   }

   @Override
   public void onFlow(int currentCredits, boolean drain) {
      super.onFlow(currentCredits, drain);
      sessionSPI.onFlowConsumer(brokerConsumer, currentCredits, drain);
   }

   /*
* start the session
* */
   @Override
   public void start() throws ActiveMQAMQPException {
      super.start();
      // protonSession.getServerSession().start();

      //todo add flow control
      try {
         // to do whatever you need to make the broker start sending messages to the consumer
         //this could be null if a link reattach has happened
         if (brokerConsumer != null) {
            sessionSPI.startSender(brokerConsumer);
         }
         //protonSession.getServerSession().receiveConsumerCredits(consumerID, -1);
      }
      catch (Exception e) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorStartingConsumer(e.getMessage());
      }
   }

   /**
    * create the actual underlying ActiveMQ Artemis Server Consumer
    */
   @Override
   public void initialise() throws Exception {
      super.initialise();

      Source source = (Source) sender.getRemoteSource();

      String queue;

      String selector = null;

      /*
      * even tho the filter is a map it will only return a single filter unless a nolocal is also provided
      * */
      if (source != null) {
         Map.Entry<Symbol, DescribedType> filter = findFilter(source.getFilter(), JMS_SELECTOR_FILTER_IDS);
         if (filter != null) {
            selector = filter.getValue().getDescribed().toString();
            // Validate the Selector.
            try {
               SelectorParser.parse(selector);
            }
            catch (FilterException e) {
               close(new ErrorCondition(AmqpError.INVALID_FIELD, e.getMessage()));
               return;
            }
         }
      }

      /*
      * if we have a capability for a topic (qpid-jms) or we are configured on this address to act like a topic then act
      * like a subscription.
      * */
      boolean isPubSub = hasCapabilities(TOPIC, source) || isPubSub(source);

      if (isPubSub) {
         if (findFilter(source.getFilter(), AmqpSupport.NO_LOCAL_FILTER_IDS) != null) {
            String remoteContainerId = sender.getSession().getConnection().getRemoteContainer();
            String noLocalFilter = ActiveMQConnection.CONNECTION_ID_PROPERTY_NAME.toString() + "<>'" + remoteContainerId + "'";
            if (selector != null) {
               selector += " AND " + noLocalFilter;
            }
            else {
               selector = noLocalFilter;
            }
         }
      }

      if (source == null) {
         // Attempt to recover a previous subscription happens when a link reattach happens on a subscription queue
         String clientId = connection.getRemoteContainer();
         String pubId = sender.getName();
         queue = clientId + ":" + pubId;
         boolean exists = sessionSPI.queueQuery(queue, false).isExists();

         /*
         * If it exists then we know it is a subscription so we set the capabilities on the source so we can delete on a
         * link remote close.
         * */
         if (exists) {
            source = new org.apache.qpid.proton.amqp.messaging.Source();
            source.setAddress(queue);
            source.setDurable(TerminusDurability.UNSETTLED_STATE);
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setDistributionMode(COPY);
            source.setCapabilities(TOPIC);
            sender.setSource(source);
         }
         else {
            throw new ActiveMQAMQPNotFoundException("Unknown subscription link: " + sender.getName());
         }
      }
      else {
         if (source.getDynamic()) {
            //if dynamic we have to create the node (queue) and set the address on the target, the node is temporary and
            // will be deleted on closing of the session
            queue = java.util.UUID.randomUUID().toString();
            try {
               sessionSPI.createTemporaryQueue(queue);
               //protonSession.getServerSession().createQueue(queue, queue, null, true, false);
            }
            catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingTemporaryQueue(e.getMessage());
            }
            source.setAddress(queue);
         }
         else {
            //if not dynamic then we use the targets address as the address to forward the messages to, however there has to
            //be a queue bound to it so we nee to check this.
            if (isPubSub) {
               // if we are a subscription and durable create a durable queue using the container id and link name
               if (TerminusDurability.UNSETTLED_STATE.equals(source.getDurable()) ||
                                TerminusDurability.CONFIGURATION.equals(source.getDurable())) {
                  String clientId = connection.getRemoteContainer();
                  String pubId = sender.getName();
                  queue = clientId + ":" + pubId;
                  QueueQueryResult result = sessionSPI.queueQuery(queue, false);

                  if (result.isExists()) {
                     // If a client reattaches to a durable subscription with a different no-local filter value, selector
                     // or address then we must recreate the queue (JMS semantics).

                     if (!Objects.equals(result.getFilterString(), SimpleString.toSimpleString(selector)) ||
                        (sender.getSource() != null && !sender.getSource().getAddress().equals(result.getAddress().toString()))) {
                        if (result.getConsumerCount() == 0) {
                           sessionSPI.deleteQueue(queue);
                           sessionSPI.createDurableQueue(source.getAddress(), queue, selector);
                        }
                        else {
                           throw new ActiveMQAMQPIllegalStateException("Unable to recreate subscription, consumers already exist");
                        }
                     }
                  }
                  else {
                     sessionSPI.createDurableQueue(source.getAddress(), queue, selector);
                  }
                  source.setAddress(queue);
               }
               //otherwise we are a volatile subscription
               else {
                  queue = java.util.UUID.randomUUID().toString();
                  try {
                     sessionSPI.createTemporaryQueue(source.getAddress(), queue, selector);
                  }
                  catch (Exception e) {
                     throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingTemporaryQueue(e.getMessage());
                  }
                  source.setAddress(queue);
               }
            }
            else {
               queue = source.getAddress();
            }
            if (queue == null) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.sourceAddressNotSet();
            }

            try {
               if (!sessionSPI.queueQuery(queue, !isPubSub).isExists()) {
                  throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.sourceAddressDoesntExist();
               }
            }
            catch (ActiveMQAMQPNotFoundException e) {
               throw e;
            }
            catch (Exception e) {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }
         }

         boolean browseOnly = !isPubSub && source.getDistributionMode() != null && source.getDistributionMode().equals(COPY);
         try {
            brokerConsumer = sessionSPI.createSender(this, queue, isPubSub ? null : selector, browseOnly);
         }
         catch (Exception e) {
            throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingConsumer(e.getMessage());
         }
      }
   }

   private boolean isPubSub(Source source) {
      String pubSubPrefix = sessionSPI.getPubSubPrefix();
      return source != null && pubSubPrefix != null && source.getAddress() != null && source.getAddress().startsWith(pubSubPrefix);
   }

   /*
   * close the session
   * */
   @Override
   public void close(ErrorCondition condition) throws ActiveMQAMQPException {
      super.close(condition);
      try {
         sessionSPI.closeSender(brokerConsumer);
      }
      catch (Exception e) {
         log.warn(e.getMessage(), e);
         throw new ActiveMQAMQPInternalErrorException(e.getMessage());
      }
   }

   /*
   * close the session
   * */
   @Override
   public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
      super.close(remoteLinkClose);
      try {
         sessionSPI.closeSender(brokerConsumer);
         //if this is a link close rather than a connection close or detach, we need to delete any durable resources for
         // say pub subs
         if (remoteLinkClose) {
            Source source = (Source) sender.getSource();
            if (source != null && source.getAddress() != null && hasCapabilities(TOPIC, source)) {
               String queueName = source.getAddress();
               QueueQueryResult result = sessionSPI.queueQuery(queueName, false);
               if (result.isExists() && source.getDynamic()) {
                  sessionSPI.deleteQueue(queueName);
               }
               else {
                  String clientId = connection.getRemoteContainer();
                  String pubId = sender.getName();
                  String queue = clientId + ":" + pubId;
                  result = sessionSPI.queueQuery(queue, false);
                  if (result.isExists()) {
                     if (result.getConsumerCount() > 0) {
                        System.out.println("error");
                     }
                     sessionSPI.deleteQueue(queue);
                  }
               }
            }
         }
      }
      catch (Exception e) {
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
                  //we have to individual ack as we can't guarantee we will get the delivery updates (including acks) in order
                  // from dealer, a perf hit but a must
                  try {
                     sessionSPI.ack(tx, brokerConsumer, message);
                  }
                  catch (Exception e) {
                     throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorAcknowledgingMessage(message.toString(), e.getMessage());
                  }
               }
            }
         }
         else if (remoteState instanceof Accepted) {
            //we have to individual ack as we can't guarantee we will get the delivery updates (including acks) in order
            // from dealer, a perf hit but a must
            try {
               sessionSPI.ack(null, brokerConsumer, message);
            }
            catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorAcknowledgingMessage(message.toString(), e.getMessage());
            }
         }
         else if (remoteState instanceof Released) {
            try {
               sessionSPI.cancel(brokerConsumer, message, false);
            }
            catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
         }
         else if (remoteState instanceof Rejected || remoteState instanceof Modified) {
            try {
               sessionSPI.cancel(brokerConsumer, message, true);
            }
            catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
         }
         //todo add tag caching
         if (!preSettle) {
            protonSession.replaceTag(delivery.getTag());
         }

         synchronized (connection.getLock()) {
            delivery.settle();
            sender.offer(1);
         }

      }
      else {
         //todo not sure if we need to do anything here
      }
   }

   @Override
   public synchronized void checkState() {
      super.checkState();
      sessionSPI.resumeDelivery(brokerConsumer);
   }

   /**
    * handle an out going message from ActiveMQ Artemis, send via the Proton Sender
    */
   @Override
   public int deliverMessage(Object message, int deliveryCount) throws Exception {
      if (closed) {
         System.err.println("Message can't be delivered as it's closed");
         return 0;
      }

      //encode the message
      ProtonJMessage serverMessage;
      try {
         // This can be done a lot better here
         serverMessage = sessionSPI.encodeMessage(message, deliveryCount);
      }
      catch (Throwable e) {
         log.warn(e.getMessage(), e);
         throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
      }

      return performSend(serverMessage, message);
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

}
