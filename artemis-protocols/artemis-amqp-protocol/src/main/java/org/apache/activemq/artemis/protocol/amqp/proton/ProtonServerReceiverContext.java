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
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPNotFoundException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPSecurityException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolLogger;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.jboss.logging.Logger;

/**
 * This is the equivalent for the ServerProducer
 */
public class ProtonServerReceiverContext extends ProtonAbstractReceiver {

   private static final Logger log = Logger.getLogger(ProtonServerReceiverContext.class);

   protected SimpleString address;
   protected SimpleString lastAddress;
   protected AddressFullMessagePolicy lastAddressPolicy;
   protected boolean addressAlreadyClashed = false;


   protected final Runnable spiFlow = this::sessionSPIFlow;

   private RoutingType defRoutingType;

   public ProtonServerReceiverContext(AMQPSessionCallback sessionSPI,
                                      AMQPConnectionContext connection,
                                      AMQPSessionContext protonSession,
                                      Receiver receiver) {
      super(sessionSPI, connection, protonSession, receiver);

   }

   @Override
   public void initialize() throws Exception {
      super.initialize();
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();

      // Match the settlement mode of the remote instead of relying on the default of MIXED.
      receiver.setSenderSettleMode(receiver.getRemoteSenderSettleMode());

      // We don't currently support SECOND so enforce that the answer is anlways FIRST
      receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

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
            String targetAddress = target.getAddress();
            if (targetAddress != null && !targetAddress.isEmpty()) {
               address = SimpleString.toSimpleString(targetAddress);
            }

            if (address != null) {
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
                  sessionSPI.check(address, CheckType.SEND, connection.getSecurityAuth());
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

   public RoutingType getDefRoutingType() {
      return defRoutingType;
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

   @Override
   protected void actualDelivery(AMQPMessage message, Delivery delivery, Receiver receiver, Transaction tx) {
      try {
         if (sessionSPI != null) {
            // message could be null on unit tests (Mocking from ProtonServerReceiverContextTest).
            if (address == null && message != null) {
               validateAddressOnAnonymousLink(message);
            }
            sessionSPI.serverSend(this, tx, receiver, delivery, address, routingContext, message);
         }
      } catch (Exception e) {
         log.warn(e.getMessage(), e);

         deliveryFailed(delivery, receiver, e);

      }
   }

   private void validateAddressOnAnonymousLink(AMQPMessage message) throws Exception {
      SimpleString newAddress = message.getAddressSimpleString();
      if (newAddress != null && !newAddress.equals(lastAddress)) {
         AddressFullMessagePolicy currentPolicy = sessionSPI.getProtocolManager().getServer().getPagingManager().getPageStore(newAddress).getAddressFullMessagePolicy();
         if (lastAddressPolicy != null && lastAddressPolicy != currentPolicy) {
            if (!addressAlreadyClashed) {
               addressAlreadyClashed = true; // print the warning only once
               ActiveMQAMQPProtocolLogger.LOGGER.incompatibleAddressFullMessagePolicy(lastAddress.toString(), "" + lastAddressPolicy, newAddress.toString(), "" + currentPolicy);
            }

            log.debug("AddressFullPolicy clash between " + lastAddress + "/" + lastAddressPolicy + " and " + newAddress + "/" + lastAddressPolicy);
         }
         this.lastAddress = message.getAddressSimpleString();
         this.lastAddressPolicy = currentPolicy;
      }
   }

   public void deliveryFailed(Delivery delivery, Receiver receiver, Exception e) {
      connection.runNow(() -> {
         DeliveryState deliveryState = determineDeliveryState(((Source) receiver.getSource()),
                                                              useModified,
                                                              e);
         delivery.disposition(deliveryState);
         settle(delivery);
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
      super.close(remoteLinkClose);
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
   public void flow() {
      // this will mark flow control to happen once after the event loop
      connection.afterFlush(spiFlow);
   }

   protected void sessionSPIFlow() {
      connection.requireInHandler();
      // Use the SessionSPI to allocate producer credits, or default, always allocate credit.
      if (sessionSPI != null) {
         sessionSPI.flow(address != null ? address : lastAddress, creditRunnable);
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
