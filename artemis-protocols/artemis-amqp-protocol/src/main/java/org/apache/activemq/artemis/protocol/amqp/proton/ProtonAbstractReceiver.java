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

import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ProtonAbstractReceiver extends ProtonInitializable implements ProtonDeliveryHandler {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected final AMQPConnectionContext connection;

   protected final AMQPSessionContext protonSession;

   protected final Receiver receiver;

   protected final int minLargeMessageSize;

   protected final RoutingContext routingContext;

   protected final AMQPSessionCallback sessionSPI;

   // Cached instances used for this receiver which will be swapped as message of varying types
   // are sent to this receiver from the remote peer.
   protected final MessageReader standardMessageReader = new AMQPMessageReader(this);

   protected final MessageReader largeMessageReader = new AMQPLargeMessageReader(this);

   protected volatile MessageReader messageReader;

   protected final Runnable creditRunnable;

   protected final boolean useModified;

   protected int pendingSettles = 0;

   public static boolean isBellowThreshold(int credit, int pending, int threshold) {
      return credit <= threshold - pending;
   }

   public static int calculatedUpdateRefill(int refill, int credits, int pending) {
      return refill - credits - pending;
   }

   public ProtonAbstractReceiver(AMQPSessionCallback sessionSPI,
                                 AMQPConnectionContext connection,
                                 AMQPSessionContext protonSession,
                                 Receiver receiver) {
      this.sessionSPI = sessionSPI;
      this.connection = connection;
      this.protonSession = protonSession;
      this.receiver = receiver;
      this.minLargeMessageSize = getConfiguredMinLargeMessageSize(connection);
      this.creditRunnable = createCreditRunnable(connection);
      useModified = this.connection.getProtocolManager().isUseModifiedForTransientDeliveryErrors();
      this.routingContext = new RoutingContextImpl(null).setDuplicateDetection(connection.getProtocolManager().isAmqpDuplicateDetection());
   }

   public AMQPSessionContext getSessionContext() {
      return protonSession;
   }

   /** Set the proper operation context in the Thread Local.
    *  Return the old context*/
   protected OperationContext recoverContext() {
      return sessionSPI.recoverContext();
   }

   protected void closeCurrentReader() {
      connection.runNow(() -> {
         if (messageReader != null) {
            messageReader.close();
            messageReader = null;
         }
      });
   }

   /**
    * Subclass can override this to provide a custom credit runnable that performs
    * other checks or applies credit in a manner more fitting that implementation.
    *
    * @param connection
    *       The {@link AMQPConnectionContext} that this resource falls under.
    *
    * @return a {@link Runnable} that will perform the actual credit granting operation.
    */
   protected Runnable createCreditRunnable(AMQPConnectionContext connection) {
      return createCreditRunnable(connection.getAmqpCredits(), connection.getAmqpLowCredits(), receiver, connection, this);
   }

   /**
    * Subclass can override this to provide the minimum large message size that should
    * be used when creating receiver instances.
    *
    * @param connection
    *       The {@link AMQPConnectionContext} that this resource falls under.
    *
    * @return the minimum large message size configuration value for this receiver.
    */
   protected int getConfiguredMinLargeMessageSize(AMQPConnectionContext connection) {
      return connection.getProtocolManager().getAmqpMinLargeMessageSize();
   }

   /**
    * This Credit Runnable can be used to manage the credit replenishment of a target AMQP receiver.
    *
    * @param refill
    *       The number of credit to top off the receiver to
    * @param threshold
    *       The low water mark for credit before refill is done
    * @param receiver
    *       The proton receiver that will have its credit refilled
    * @param connection
    *       The connection that own the receiver
    * @param context
    *       The context that will be associated with the receiver
    *
    * @return A new Runnable that can be used to keep receiver credit replenished.
    */
   public static Runnable createCreditRunnable(int refill,
                                               int threshold,
                                               Receiver receiver,
                                               AMQPConnectionContext connection,
                                               ProtonAbstractReceiver context) {
      return new FlowControlRunner(refill, threshold, receiver, connection, context);
   }

   /**
    * This Credit Runnable can be used to manage the credit replenishment of a target AMQP receiver.
    * <p>
    * This method is generally used for tests as it does not account for the receiver context that is
    * assigned to the given receiver instance which does not allow for tracking pending settles.
    *
    * @param refill
    *       The number of credit to top off the receiver to
    * @param threshold
    *       The low water mark for credit before refill is done
    * @param receiver
    *       The proton receiver that will have its credit refilled
    * @param connection
    *       The connection that own the receiver
    *
    * @return A new Runnable that can be used to keep receiver credit replenished.
    */
   public static Runnable createCreditRunnable(int refill,
                                               int threshold,
                                               Receiver receiver,
                                               AMQPConnectionContext connection) {
      return new FlowControlRunner(refill, threshold, receiver, connection, null);
   }

   /**
    * The reason why we use the AtomicRunnable here
    * is because PagingManager will call Runnable in case it was blocked.
    * however it could call many Runnable
    *  and this serves as a control to avoid duplicated calls
    * */
   static class FlowControlRunner implements Runnable {

      /*
       * The number of credits sent to the remote when the runnable decides that a top off is needed.
       */
      final int refill;

      /*
       * The low water mark before the runnable considers performing a credit top off.
       */
      final int threshold;

      final Receiver receiver;
      final AMQPConnectionContext connection;
      final ProtonAbstractReceiver context;

      FlowControlRunner(int refill, int threshold, Receiver receiver, AMQPConnectionContext connection, ProtonAbstractReceiver context) {
         this.refill = refill;
         this.threshold = threshold;
         this.receiver = receiver;
         this.connection = connection;
         this.context = context;
      }

      @Override
      public void run() {
         if (!connection.isHandler()) {
            // for the case where the paging manager is resuming flow due to blockage
            // this should then move back to the connection thread.
            connection.runLater(this);
            return;
         }

         connection.requireInHandler();
         int pending = context != null ? context.pendingSettles : 0;
         if (isBellowThreshold(receiver.getCredit(), pending, threshold)) {
            int topUp = calculatedUpdateRefill(refill, receiver.getCredit(), pending);
            if (topUp > 0) {
               receiver.flow(topUp);
               connection.instantFlush();
            }
         }
      }
   }

   public void incrementSettle() {
      assert pendingSettles >= 0;
      connection.requireInHandler();
      pendingSettles++;
   }

   public void settle(Delivery settlement) {
      connection.requireInHandler();
      pendingSettles--;
      assert pendingSettles >= 0;
      settlement.settle();
      flow();
   }

   @Override
   public void onFlow(int credits, boolean drain) {
      flow();
   }

   private void handleAbortedDelivery(Delivery delivery) {
      Receiver receiver = ((Receiver) delivery.getLink());

      closeCurrentReader();

      // Aborting implicitly remotely settles, so advance
      // receiver to the next delivery and settle locally.
      receiver.advance();
      delivery.settle();

      // Replenish the credit if not doing a drain
      if (!receiver.getDrain()) {
         receiver.flow(1);
      }
   }

   private MessageReader getOrSelectMessageReader(Receiver receiver, Delivery delivery) {
      // The reader will be nulled once a message has been read, otherwise a large message
      // is being read in chunks from the remote.
      if (messageReader != null) {
         return messageReader;
      } else {
         final MessageReader selected = trySelectMessageReader(receiver, delivery);

         if (selected != null) {
            return messageReader = selected.open();
         } else {
            return null;
         }
      }
   }

   protected MessageReader trySelectMessageReader(Receiver receiver, Delivery delivery) {
      if (sessionSPI.getStorageManager() instanceof NullStorageManager) {
         // if we are dealing with the NullStorageManager we should just make it a regular message anyways
         return standardMessageReader;
      } else if (delivery.isPartial()) {
         if (minLargeMessageSize > 0 && delivery.available() >= minLargeMessageSize) {
            return largeMessageReader;
         } else {
            return null; // Not enough context to decide yet.
         }
      } else if (minLargeMessageSize > 0 && delivery.available() >= minLargeMessageSize) {
         // this is treating the case where the frameSize > minLargeMessage and the message is still large enough
         return largeMessageReader;
      } else {
         // Either minLargeMessageSize < 0 which means disable or the entire message has
         // arrived and is under the threshold so use the standard variant.
         return standardMessageReader;
      }
   }

   /*
    * called when Proton receives a message to be delivered via a Delivery.
    *
    * This may be called more than once per deliver so we have to cache the buffer until we have received it all.
    */
   @Override
   public final void onMessage(Delivery delivery) throws ActiveMQAMQPException {
      connection.requireInHandler();

      if (receiver.current() != delivery) {
         return;
      }

      if (delivery.isAborted()) {
         handleAbortedDelivery(delivery);
         return;
      }

      try {
         final MessageReader messageReader = getOrSelectMessageReader(receiver, delivery);

         if (messageReader == null) {
            return;
         }

         Message completeMessage;
         if ((completeMessage = messageReader.readBytes(delivery)) != null) {
            // notice the AMQP Large Message Reader will always return null
            // and call the onMessageComplete directly
            // since that happens asynchronously
            onMessageComplete(delivery, completeMessage, messageReader.getDeliveryAnnotations());
         }
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
      }
   }

   public void onMessageComplete(Delivery delivery,
                          Message message, DeliveryAnnotations deliveryAnnotations) {
      connection.requireInHandler();

      try {
         receiver.advance();

         Transaction tx = null;
         if (delivery.getRemoteState() instanceof TransactionalState) {
            TransactionalState txState = (TransactionalState) delivery.getRemoteState();
            try {
               tx = this.sessionSPI.getTransaction(txState.getTxnId(), false);
            } catch (Exception e) {
               this.onExceptionWhileReading(e);
            }
         }

         actualDelivery(message, delivery, deliveryAnnotations, receiver, tx);
      } finally {
         // reader is complete, we give it up now
         this.messageReader.close();
         this.messageReader = null;
      }
   }

   @Override
   public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
      protonSession.removeReceiver(receiver);
      closeCurrentReader();
   }

   public void onExceptionWhileReading(Throwable e) {
      logger.warn(e.getMessage(), e);
      connection.runNow(() -> {
         // setting it enabled just in case a large message reader disabled it
         connection.enableAutoRead();
         ErrorCondition ec = new ErrorCondition(AmqpError.INTERNAL_ERROR, e.getMessage());
         connection.close(ec);
         connection.flush();
      });
   }

   @Override
   public void close(ErrorCondition condition) throws ActiveMQAMQPException {
      receiver.setCondition(condition);
      close(false);
   }

   public AMQPConnectionContext getConnection() {
      return connection;
   }

   protected abstract void actualDelivery(Message message, Delivery delivery, DeliveryAnnotations deliveryAnnotations, Receiver receiver, Transaction tx);

   // TODO: how to implement flow here?
   public abstract void flow();

}
