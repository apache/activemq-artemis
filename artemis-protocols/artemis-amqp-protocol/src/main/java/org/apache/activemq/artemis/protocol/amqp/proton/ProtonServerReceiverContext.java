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

import java.util.Arrays;
import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPNotFoundException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.sasl.PlainSASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.jboss.logging.Logger;

public class ProtonServerReceiverContext extends ProtonInitializable implements ProtonDeliveryHandler {

   private static final Logger log = Logger.getLogger(ProtonServerReceiverContext.class);

   protected final AMQPConnectionContext connection;

   protected final AMQPSessionContext protonSession;

   protected final Receiver receiver;

   protected SimpleString address;

   protected final AMQPSessionCallback sessionSPI;

   /*
    The maximum number of credits we will allocate to clients.
    This number is also used by the broker when refresh client credits.
    */
   private final int amqpCredits;

   // Used by the broker to decide when to refresh clients credit.  This is not used when client requests credit.
   private final int minCreditRefresh;
   private TerminusExpiryPolicy expiryPolicy;

   public ProtonServerReceiverContext(AMQPSessionCallback sessionSPI,
                                      AMQPConnectionContext connection,
                                      AMQPSessionContext protonSession,
                                      Receiver receiver) {
      this.connection = connection;
      this.protonSession = protonSession;
      this.receiver = receiver;
      this.sessionSPI = sessionSPI;
      this.amqpCredits = connection.getAmqpCredits();
      this.minCreditRefresh = connection.getAmqpLowCredits();
   }

   @Override
   public void onFlow(int credits, boolean drain) {
      flow(Math.min(credits, amqpCredits), amqpCredits);
   }

   @Override
   public void initialise() throws Exception {
      super.initialise();
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();

      // Match the settlement mode of the remote instead of relying on the default of MIXED.
      receiver.setSenderSettleMode(receiver.getRemoteSenderSettleMode());

      // We don't currently support SECOND so enforce that the answer is anlways FIRST
      receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

      RoutingType defRoutingType;

      if (target != null) {
         if (target.getDynamic()) {
            // if dynamic we have to create the node (queue) and set the address on the target, the node is temporary and
            // will be deleted on closing of the session
            address = SimpleString.toSimpleString(sessionSPI.tempQueueName());
            defRoutingType = getRoutingType(target.getCapabilities(), address);

            try {
               sessionSPI.createTemporaryQueue(address, defRoutingType);
            } catch (ActiveMQSecurityException e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingTempDestination(e.getMessage());
            } catch (Exception e) {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }
            expiryPolicy = target.getExpiryPolicy() != null ? target.getExpiryPolicy() : TerminusExpiryPolicy.LINK_DETACH;
            target.setAddress(address.toString());
         } else {
            // the target will have an address unless the remote is requesting an anonymous
            // relay in which case the address in the incoming message's to field will be
            // matched on receive of the message.
            address = SimpleString.toSimpleString(target.getAddress());

            if (address != null && !address.isEmpty()) {
               defRoutingType = getRoutingType(target.getCapabilities(), address);
               try {
                  if (!sessionSPI.bindingQuery(address, defRoutingType)) {
                     throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist();
                  }
               } catch (ActiveMQAMQPNotFoundException e) {
                  throw e;
               } catch (Exception e) {
                  log.debug(e.getMessage(), e);
                  throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
               }

               try {
                  sessionSPI.check(address, CheckType.SEND, new SecurityAuth() {
                     @Override
                     public String getUsername() {
                        String username = null;
                        SASLResult saslResult = connection.getSASLResult();
                        if (saslResult != null) {
                           username = saslResult.getUser();
                        }

                        return username;
                     }

                     @Override
                     public String getPassword() {
                        String password = null;
                        SASLResult saslResult = connection.getSASLResult();
                        if (saslResult != null) {
                           if (saslResult instanceof PlainSASLResult) {
                              password = ((PlainSASLResult) saslResult).getPassword();
                           }
                        }

                        return password;
                     }

                     @Override
                     public RemotingConnection getRemotingConnection() {
                        return connection.connectionCallback.getProtonConnectionDelegate();
                     }
                  });
               } catch (ActiveMQSecurityException e) {
                  throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingProducer(e.getMessage());
               }
            }
         }

         Symbol[] remoteDesiredCapabilities = receiver.getRemoteDesiredCapabilities();
         if (remoteDesiredCapabilities != null) {
            List<Symbol> list = Arrays.asList(remoteDesiredCapabilities);
            if (list.contains(AmqpSupport.DELAYED_DELIVERY)) {
               receiver.setOfferedCapabilities(new Symbol[]{AmqpSupport.DELAYED_DELIVERY});
            }
         }
      }
      flow(amqpCredits, minCreditRefresh);
   }

   public RoutingType getRoutingType(Receiver receiver, SimpleString address) {
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();
      return target != null ? getRoutingType(target.getCapabilities(), address) : getRoutingType((Symbol[]) null, address);
   }

   private RoutingType getRoutingType(Symbol[] symbols, SimpleString address) {
      if (symbols != null) {
         for (Symbol symbol : symbols) {
            if (AmqpSupport.TEMP_TOPIC_CAPABILITY.equals(symbol) || AmqpSupport.TOPIC_CAPABILITY.equals(symbol)) {
               return RoutingType.MULTICAST;
            } else if (AmqpSupport.TEMP_QUEUE_CAPABILITY.equals(symbol) || AmqpSupport.QUEUE_CAPABILITY.equals(symbol)) {
               return RoutingType.ANYCAST;
            }
         }
      }

      return sessionSPI.getDefaultRoutingType(address);
   }

   /*
   * called when Proton receives a message to be delivered via a Delivery.
   *
   * This may be called more than once per deliver so we have to cache the buffer until we have received it all.
   *
   * */
   @Override
   public void onMessage(Delivery delivery) throws ActiveMQAMQPException {
      Receiver receiver;
      try {

         if (!delivery.isReadable()) {
            return;
         }
         if (delivery.isPartial()) {
            return;
         }

         receiver = ((Receiver) delivery.getLink());

         Transaction tx = null;
         byte[] data;

         data = new byte[delivery.available()];
         receiver.recv(data, 0, data.length);
         receiver.advance();

         if (delivery.getRemoteState() instanceof TransactionalState) {
            TransactionalState txState = (TransactionalState) delivery.getRemoteState();
            tx = this.sessionSPI.getTransaction(txState.getTxnId(), false);
         }

         sessionSPI.serverSend(this, tx, receiver, delivery, address, delivery.getMessageFormat(), data);

         flow(amqpCredits, minCreditRefresh);
      } catch (Exception e) {
         log.warn(e.getMessage(), e);
         Rejected rejected = new Rejected();
         ErrorCondition condition = new ErrorCondition();

         if (e instanceof ActiveMQSecurityException) {
            condition.setCondition(AmqpError.UNAUTHORIZED_ACCESS);
         } else {
            condition.setCondition(Symbol.valueOf("failed"));
         }

         condition.setDescription(e.getMessage());
         rejected.setError(condition);
         connection.lock();
         try {
            delivery.disposition(rejected);
            delivery.settle();
         } finally {
            connection.unlock();
         }
      }
   }

   @Override
   public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
      protonSession.removeReceiver(receiver);
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();
      if (target != null && target.getDynamic() && (target.getExpiryPolicy() == TerminusExpiryPolicy.LINK_DETACH || target.getExpiryPolicy() == TerminusExpiryPolicy.SESSION_END)) {
         try {
            sessionSPI.removeTemporaryQueue(SimpleString.toSimpleString(target.getAddress()));
         } catch (Exception e) {
            //ignore on close, its temp anyway and will be removed later
         }
      }
   }

   @Override
   public void close(ErrorCondition condition) throws ActiveMQAMQPException {
      receiver.setCondition(condition);
      close(false);
   }

   public void flow(int credits, int threshold) {
      // Use the SessionSPI to allocate producer credits, or default, always allocate credit.
      if (sessionSPI != null) {
         sessionSPI.offerProducerCredit(address, credits, threshold, receiver);
      } else {
         connection.lock();
         try {
            receiver.flow(credits);
         } finally {
            connection.unlock();
         }
         connection.flush();
      }
   }

   public void drain(int credits) {
      connection.lock();
      try {
         receiver.drain(credits);
      } finally {
         connection.unlock();
      }
      connection.flush();
   }

   public int drained() {
      return receiver.drained();
   }

   public boolean isDraining() {
      return receiver.draining();
   }
}
