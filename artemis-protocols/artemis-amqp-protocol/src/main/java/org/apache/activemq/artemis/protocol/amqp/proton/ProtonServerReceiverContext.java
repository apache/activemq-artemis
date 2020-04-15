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

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPLargeMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPNotFoundException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPSecurityException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.sasl.PlainSASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.runnables.AtomicRunnable;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.jboss.logging.Logger;

/**
 * This is the equivalent for the ServerProducer
 */
public class ProtonServerReceiverContext extends ProtonInitializable implements ProtonDeliveryHandler {

   private static final Logger log = Logger.getLogger(ProtonServerReceiverContext.class);

   protected final AMQPConnectionContext connection;

   protected final AMQPSessionContext protonSession;

   protected final Receiver receiver;

   protected SimpleString address;

   protected final AMQPSessionCallback sessionSPI;

   final RoutingContext routingContext;

   /**
    * We create this AtomicRunnable with setRan.
    * This is because we always reuse the same instance.
    * In case the creditRunnable was run, we reset and send it over.
    * We set it as ran as the first one should always go through
    */
   protected final AtomicRunnable creditRunnable;
   private final boolean useModified;

   /**
    * This Credit Runnable may be used in Mock tests to simulate the credit semantic here
    */
   public static AtomicRunnable createCreditRunnable(int refill,
                                                     int threshold,
                                                     Receiver receiver,
                                                     AMQPConnectionContext connection) {
      Runnable creditRunnable = () -> {

         connection.requireInHandler();
         if (receiver.getCredit() <= threshold) {
            int topUp = refill - receiver.getCredit();
            if (topUp > 0) {
               // System.out.println("Sending " + topUp + " towards client");
               receiver.flow(topUp);
               connection.flush();
            }
         }
      };
      return new AtomicRunnable() {
         @Override
         public void atomicRun() {
            connection.runNow(creditRunnable);
         }
      };
   }

   /*
    The maximum number of credits we will allocate to clients.
    This number is also used by the broker when refresh client credits.
    */
   private final int amqpCredits;

   // Used by the broker to decide when to refresh clients credit.  This is not used when client requests credit.
   private final int minCreditRefresh;

   private final int minLargeMessageSize;

   public ProtonServerReceiverContext(AMQPSessionCallback sessionSPI,
                                      AMQPConnectionContext connection,
                                      AMQPSessionContext protonSession,
                                      Receiver receiver) {
      this.connection = connection;
      this.routingContext = new RoutingContextImpl(null).setDuplicateDetection(connection.getProtocolManager().isAmqpDuplicateDetection());
      this.protonSession = protonSession;
      this.receiver = receiver;
      this.sessionSPI = sessionSPI;
      this.amqpCredits = connection.getAmqpCredits();
      this.minCreditRefresh = connection.getAmqpLowCredits();
      this.creditRunnable = createCreditRunnable(amqpCredits, minCreditRefresh, receiver, connection).setRan();
      useModified = this.connection.getProtocolManager().isUseModifiedForTransientDeliveryErrors();
      this.minLargeMessageSize = connection.getProtocolManager().getAmqpMinLargeMessageSize();

      if (sessionSPI != null) {
         sessionSPI.addCloseable((boolean failed) -> clearLargeMessage());
      }
   }

   protected void clearLargeMessage() {
      connection.runNow(() -> {
         if (currentLargeMessage != null) {
            try {
               currentLargeMessage.deleteFile();
            } catch (Throwable error) {
               ActiveMQServerLogger.LOGGER.errorDeletingLargeMessageFile(error);
            } finally {
               currentLargeMessage = null;
            }
         }
      });
   }

   @Override
   public void onFlow(int credits, boolean drain) {
      flow();
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
            } catch (ActiveMQAMQPSecurityException e) {
               throw e;
            } catch (ActiveMQSecurityException e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingTempDestination(e.getMessage());
            } catch (Exception e) {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }
            target.setAddress(address.toString());
         } else {
            // the target will have an address unless the remote is requesting an anonymous
            // relay in which case the address in the incoming message's to field will be
            // matched on receive of the message.
            address = SimpleString.toSimpleString(target.getAddress());

            if (address != null && !address.isEmpty()) {
               defRoutingType = getRoutingType(target.getCapabilities(), address);
               try {
                  if (!sessionSPI.checkAddressAndAutocreateIfPossible(address, defRoutingType)) {
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
      flow();
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
      final AddressInfo addressInfo = sessionSPI.getAddress(address);
      if (addressInfo != null && !addressInfo.getRoutingTypes().isEmpty()) {
         if (addressInfo.getRoutingTypes().size() == 1 && addressInfo.getRoutingType() == RoutingType.MULTICAST) {
            return RoutingType.MULTICAST;
         }
      }
      RoutingType defaultRoutingType = sessionSPI.getDefaultRoutingType(address);
      defaultRoutingType = defaultRoutingType == null ? ActiveMQDefaultConfiguration.getDefaultRoutingType() : defaultRoutingType;
      return defaultRoutingType;
   }

   volatile AMQPLargeMessage currentLargeMessage;

   /*
    * called when Proton receives a message to be delivered via a Delivery.
    *
    * This may be called more than once per deliver so we have to cache the buffer until we have received it all.
    */
   @Override
   public void onMessage(Delivery delivery) throws ActiveMQAMQPException {
      connection.requireInHandler();
      Receiver receiver = ((Receiver) delivery.getLink());

      if (receiver.current() != delivery) {
         return;
      }

      try {
         if (delivery.isAborted()) {
            clearLargeMessage();

            // Aborting implicitly remotely settles, so advance
            // receiver to the next delivery and settle locally.
            receiver.advance();
            delivery.settle();

            // Replenish the credit if not doing a drain
            if (!receiver.getDrain()) {
               receiver.flow(1);
            }

            return;
         } else if (delivery.isPartial()) {
            if (sessionSPI.getStorageManager() instanceof NullStorageManager) {
               // if we are dealing with the NullStorageManager we should just make it a regular message anyways
               return;
            }

            if (currentLargeMessage == null) {
               // minLargeMessageSize < 0 means no large message treatment, make it disabled
               if (minLargeMessageSize > 0 && delivery.available() >= minLargeMessageSize) {
                  initializeCurrentLargeMessage(delivery, receiver);
               }
            } else {
               currentLargeMessage.addBytes(receiver.recv());
            }

            return;
         }

         AMQPMessage message;

         // this is treating the case where the frameSize > minLargeMessage and the message is still large enough
         if (!(sessionSPI.getStorageManager() instanceof NullStorageManager) && currentLargeMessage == null && minLargeMessageSize > 0 && delivery.available() >= minLargeMessageSize) {
            initializeCurrentLargeMessage(delivery, receiver);
         }

         if (currentLargeMessage != null) {
            currentLargeMessage.addBytes(receiver.recv());
            receiver.advance();
            currentLargeMessage.finishParse();
            message = currentLargeMessage;
            currentLargeMessage = null;
         } else {
            ReadableBuffer data = receiver.recv();
            receiver.advance();
            message = sessionSPI.createStandardMessage(delivery, data);
         }

         Transaction tx = null;
         if (delivery.getRemoteState() instanceof TransactionalState) {
            TransactionalState txState = (TransactionalState) delivery.getRemoteState();
            tx = this.sessionSPI.getTransaction(txState.getTxnId(), false);
         }

         actualDelivery(message, delivery, receiver, tx);
      } catch (Exception e) {
         throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
      }

   }

   private void initializeCurrentLargeMessage(Delivery delivery, Receiver receiver) throws Exception {
      long id = sessionSPI.getStorageManager().generateID();
      currentLargeMessage = new AMQPLargeMessage(id, delivery.getMessageFormat(), null, sessionSPI.getCoreMessageObjectPools(), sessionSPI.getStorageManager());

      ReadableBuffer dataBuffer = receiver.recv();
      currentLargeMessage.parseHeader(dataBuffer);

      sessionSPI.getStorageManager().largeMessageCreated(id, currentLargeMessage);
      currentLargeMessage.addBytes(dataBuffer);
   }

   private void actualDelivery(AMQPMessage message, Delivery delivery, Receiver receiver, Transaction tx) {
      try {
         sessionSPI.serverSend(this, tx, receiver, delivery, address, routingContext, message);
      } catch (Exception e) {
         log.warn(e.getMessage(), e);

         deliveryFailed(delivery, receiver, e);

      }
   }

   public void deliveryFailed(Delivery delivery, Receiver receiver, Exception e) {
      connection.runNow(() -> {
         DeliveryState deliveryState = determineDeliveryState(((Source) receiver.getSource()),
                                                              useModified,
                                                              e);
         delivery.disposition(deliveryState);
         delivery.settle();
         flow();
         connection.flush();
      });
   }

   private DeliveryState determineDeliveryState(final Source source, final boolean useModified, final Exception e) {
      Outcome defaultOutcome = getEffectiveDefaultOutcome(source);

      if (isAddressFull(e) && useModified &&
          (outcomeSupported(source, Modified.DESCRIPTOR_SYMBOL) || defaultOutcome instanceof Modified)) {
         Modified modified = new Modified();
         modified.setDeliveryFailed(true);
         return modified;
      } else {
         if (outcomeSupported(source, Rejected.DESCRIPTOR_SYMBOL) || defaultOutcome instanceof Rejected) {
            return createRejected(e);
         } else if (source.getDefaultOutcome() instanceof DeliveryState) {
            return ((DeliveryState) source.getDefaultOutcome());
         } else {
            // The AMQP specification requires that Accepted is returned for this case. However there exist
            // implementations that set neither outcomes/default-outcome but use/expect for full range of outcomes.
            // To maintain compatibility with these implementations, we maintain previous behaviour.
            return createRejected(e);
         }
      }
   }

   private boolean isAddressFull(final Exception e) {
      return e instanceof ActiveMQException && ActiveMQExceptionType.ADDRESS_FULL.equals(((ActiveMQException) e).getType());
   }

   private Rejected createRejected(final Exception e) {
      ErrorCondition condition = new ErrorCondition();

      // Set condition
      if (e instanceof ActiveMQSecurityException) {
         condition.setCondition(AmqpError.UNAUTHORIZED_ACCESS);
      } else if (isAddressFull(e)) {
         condition.setCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED);
      } else {
         condition.setCondition(Symbol.valueOf("failed"));
      }
      condition.setDescription(e.getMessage());

      Rejected rejected = new Rejected();
      rejected.setError(condition);
      return rejected;
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
      clearLargeMessage();
   }

   public void flow() {
      connection.requireInHandler();
      if (!creditRunnable.isRun()) {
         return; // nothing to be done as the previous one did not run yet
      }

      creditRunnable.reset();

      // Use the SessionSPI to allocate producer credits, or default, always allocate credit.
      if (sessionSPI != null) {
         sessionSPI.flow(address, creditRunnable);
      } else {
         creditRunnable.run();
      }
   }

   public void drain(int credits) {
      connection.runNow(() -> {
         receiver.drain(credits);
         connection.flush();
      });
   }

   public int drained() {
      return receiver.drained();
   }

   public boolean isDraining() {
      return receiver.draining();
   }

   private boolean outcomeSupported(final Source source, final Symbol outcome) {
      if (source != null && source.getOutcomes() != null) {
         return Arrays.asList(( source).getOutcomes()).contains(outcome);
      }
      return false;
   }

   private Outcome getEffectiveDefaultOutcome(final Source source) {
      return (source.getOutcomes() == null || source.getOutcomes().length == 0) ? source.getDefaultOutcome() : null;
   }
}
