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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQQueueMaxConsumerLimitReached;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.message.LargeBodyReader;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.server.AddressQueryResult;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPLargeMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessageBrokerAccessor;
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
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.selector.filter.FilterException;
import org.apache.activemq.artemis.selector.impl.SelectorParser;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.apache.activemq.artemis.utils.DestinationUtil;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.DeliveryState.DeliveryStateType;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;
import org.jboss.logging.Logger;

/**
 * This is the Equivalent for the ServerConsumer
 */
public class ProtonServerSenderContext extends ProtonInitializable implements ProtonDeliveryHandler {

   private static final Logger log = Logger.getLogger(ProtonServerSenderContext.class);

   private static final Symbol COPY = Symbol.valueOf("copy");
   private static final Symbol TOPIC = Symbol.valueOf("topic");
   private static final Symbol QUEUE = Symbol.valueOf("queue");
   private static final Symbol SHARED = Symbol.valueOf("shared");
   private static final Symbol GLOBAL = Symbol.valueOf("global");

   SenderController controller;

   private final ConnectionFlushIOCallback connectionFlusher = new ConnectionFlushIOCallback();

   private Consumer brokerConsumer;
   private ReadyListener onflowControlReady;
   protected final AMQPSessionContext protonSession;
   protected final Sender sender;
   protected final AMQPConnectionContext connection;
   protected boolean closed = false;
   protected final AMQPSessionCallback sessionSPI;

   private boolean preSettle;

   private final AtomicBoolean draining = new AtomicBoolean(false);

   // once a large message is accepted, we shouldn't accept any further messages
   // as large message could be interrupted due to flow control and resumed at the same message
   volatile boolean hasLarge = false;
   volatile LargeMessageDeliveryContext pendingLargeMessage = null;


   private int credits = 0;

   private AtomicInteger pending = new AtomicInteger(0);
   /**
    * The model proton uses requires us to hold a lock in certain times
    * to sync the credits we have versus the credits that are being held in proton
    * */
   private final Object creditsLock = new Object();
   private final java.util.function.Consumer<? super MessageReference> executeDelivery;
   private java.util.function.Consumer<? super MessageReference> beforeDelivery;
   private final boolean amqpTreatRejectAsUnmodifiedDeliveryFailed;

   public ProtonServerSenderContext(AMQPConnectionContext connection,
                                    Sender sender,
                                    AMQPSessionContext protonSession,
                                    AMQPSessionCallback server) {
      this(connection, sender, protonSession, server, null);
   }

   public ProtonServerSenderContext(AMQPConnectionContext connection,
                                    Sender sender,
                                    AMQPSessionContext protonSession,
                                    AMQPSessionCallback server,
                                    SenderController senderController) {
      super();
      this.controller = senderController;
      this.connection = connection;
      this.sender = sender;
      this.protonSession = protonSession;
      this.sessionSPI = server;
      this.executeDelivery = this::executeDelivery;
      amqpTreatRejectAsUnmodifiedDeliveryFailed = this.connection.getProtocolManager()
                                                                 .isAmqpTreatRejectAsUnmodifiedDeliveryFailed();
   }

   public ProtonServerSenderContext setBeforeDelivery(java.util.function.Consumer<? super MessageReference> beforeDelivery) {
      this.beforeDelivery = beforeDelivery;
      return this;
   }

   public Object getBrokerConsumer() {
      return brokerConsumer;
   }

   @Override
   public void onFlow(int currentCredits, boolean drain) {
      connection.requireInHandler();

      setupCredit();

      ServerConsumerImpl serverConsumer = (ServerConsumerImpl) brokerConsumer;
      if (drain) {
         // If the draining is already running, then don't do anything
         if (draining.compareAndSet(false, true)) {
            final ProtonServerSenderContext plugSender = (ProtonServerSenderContext) serverConsumer.getProtocolContext();
            serverConsumer.forceDelivery(1, new Runnable() {
               @Override
               public void run() {
                  try {
                     connection.runNow(() -> {
                        plugSender.reportDrained();
                        setupCredit();
                     });
                  } finally {
                     draining.set(false);
                  }
               }
            });
         }
      } else {
         serverConsumer.receiveCredits(-1);
      }
   }

   public boolean hasCredits() {
      if (hasLarge) {
         // we will resume accepting once the large message is finished
         return false;
      }

      if (!connection.flowControl(onflowControlReady)) {
         return false;
      }

      synchronized (creditsLock) {
         return credits > 0 && sender.getLocalState() != EndpointState.CLOSED;
      }
   }

   private void setupCredit() {
      synchronized (creditsLock) {
         this.credits = sender.getCredit() - pending.get();
         if (credits < 0) {
            credits = 0;
         }
      }
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
   @Override
   public void initialize() throws Exception {
      super.initialize();

      if (controller == null) {
         controller = new DefaultController(sessionSPI);
      }

      try {
         brokerConsumer = controller.init(this);
         onflowControlReady = brokerConsumer::promptDelivery;
      } catch (ActiveMQAMQPResourceLimitExceededException e1) {
         throw e1;
      } catch (ActiveMQSecurityException e) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingConsumer(e.getMessage());
      } catch (ActiveMQQueueMaxConsumerLimitReached e) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingConsumer(e.getMessage());
      } catch (ActiveMQException e) {
         throw e;
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

      connection.runLater(() -> {
         sender.close();
         try {
            sessionSPI.closeSender(brokerConsumer);
         } catch (Exception e) {
            log.warn(e.getMessage(), e);
         }
         sender.close();
         connection.flush();
      });
   }

   /*
    * close the session
    */
   @Override
   public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
      // we need to mark closed first to make sure no more adds are accepted
      closed = true;

      // MessageReferences are sent to the Connection executor (Netty Loop)
      // as a result the returning references have to be done later after they
      // had their chance to finish and clear the runnable
      connection.runLater(() -> {
         try {
            internalClose(remoteLinkClose);
         } catch (Exception e) {
            log.warn(e.getMessage(), e);
         }
      });
   }

   private void internalClose(boolean remoteLinkClose) throws ActiveMQAMQPException {
      try {
         protonSession.removeSender(sender);
         sessionSPI.closeSender(brokerConsumer);
         // if this is a link close rather than a connection close or detach, we need to delete
         // any durable resources for say pub subs
         if (remoteLinkClose) {
            controller.close();

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

         if (remoteState != null && remoteState.getType() == DeliveryStateType.Accepted) {
            // this can happen in the twice ack mode, that is the receiver accepts and settles separately
            // acking again would show an exception but would have no negative effect but best to handle anyway.
            if (!delivery.isSettled()) {
               doAck(message);

               delivery.settle();
            }
         } else {
            handleExtendedDeliveryOutcomes(message, delivery, remoteState);
         }

         if (!preSettle) {
            protonSession.replaceTag(delivery.getTag());
         }
      } finally {
         sessionSPI.afterIO(connectionFlusher);
         sessionSPI.resetContext(oldContext);
      }
   }

   protected void doAck(Message message) throws ActiveMQAMQPIllegalStateException {
      // we have to individual ack as we can't guarantee we will get the delivery updates
      // (including acks) in order from dealer, a performance hit but a must
      try {
         sessionSPI.ack(null, brokerConsumer, message);
      } catch (Exception e) {
         log.warn(e.toString(), e);
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorAcknowledgingMessage(message.toString(), e.getMessage());
      }
   }

   private boolean handleExtendedDeliveryOutcomes(Message message, Delivery delivery, DeliveryState remoteState) throws ActiveMQAMQPException {
      boolean settleImmediate = true;
      boolean handled = true;

      if (remoteState == null) {
         log.debug("Received null disposition for delivery update: " + remoteState);
         return true;
      }

      switch (remoteState.getType()) {
         case Transactional:
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
            break;
         case Released:
            try {
               sessionSPI.cancel(brokerConsumer, message, false);
            } catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
            break;
         case Rejected:
            try {
               if (amqpTreatRejectAsUnmodifiedDeliveryFailed) {
                  // We could be more discriminating - for instance check for AmqpError#RESOURCE_LIMIT_EXCEEDED
                  sessionSPI.cancel(brokerConsumer, message, true);
               } else {
                  sessionSPI.reject(brokerConsumer, message);
               }
            } catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
            break;
         case Modified:
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
            break;
         default:
            log.debug("Received null or unknown disposition for delivery update: " + remoteState);
            handled = false;
      }

      if (settleImmediate) {
         delivery.settle();
      }

      return handled;
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
      connection.requireInHandler();
      delivery.settle();
   }

   public synchronized void checkState() {
      sessionSPI.resumeDelivery(brokerConsumer);
   }

   /**
    * handle an out going message from ActiveMQ Artemis, send via the Proton Sender
    */
   public int deliverMessage(final MessageReference messageReference, final ServerConsumer consumer) throws Exception {

      if (closed) {
         return 0;
      }

      if (beforeDelivery != null) {
         beforeDelivery.accept(messageReference);
      }

      try {
         synchronized (creditsLock) {
            if (sender.getLocalState() == EndpointState.CLOSED) {
               return 0;
            }
            pending.incrementAndGet();
            credits--;
         }

         if (messageReference.getMessage() instanceof AMQPLargeMessage) {
            hasLarge = true;
         }

         if (messageReference instanceof Runnable && consumer.allowReferenceCallback()) {
            messageReference.onDelivery(executeDelivery);
            connection.runNow((Runnable) messageReference);
         } else {
            connection.runNow(() -> executeDelivery(messageReference));
         }

         // This is because on AMQP we only send messages based in credits, not bytes
         return 1;
      } finally {

      }
   }

   private void executeDelivery(MessageReference messageReference) {

      try {
         if (sender.getLocalState() == EndpointState.CLOSED) {
            log.debug("Not delivering message " + messageReference + " as the sender is closed and credits were available, if you see too many of these it means clients are issuing credits and closing the connection with pending credits a lot of times");
            return;
         }
         AMQPMessage message = CoreAmqpConverter.checkAMQP(messageReference.getMessage(), sessionSPI.getStorageManager());

         if (sessionSPI.invokeOutgoing(message, (ActiveMQProtonRemotingConnection) sessionSPI.getTransportConnection().getProtocolConnection()) != null) {
            return;
         }
         if (message instanceof AMQPLargeMessage) {
            deliverLarge(messageReference, (AMQPLargeMessage) message);
         } else {
            deliverStandard(messageReference, message);
         }

      } catch (Exception e) {
         log.warn(e.getMessage(), e);
         brokerConsumer.errorProcessing(e, messageReference);
      }
   }

   private class LargeMessageDeliveryContext {

      LargeMessageDeliveryContext(MessageReference reference, AMQPLargeMessage message, Delivery delivery) {
         this.position = 0L;
         this.reference = reference;
         this.message = message;
         this.delivery = delivery;
      }

      long position;
      final MessageReference reference;
      final AMQPLargeMessage message;
      final Delivery delivery;

      void resume() {
         connection.runNow(this::deliver);
      }

      void deliver() {

         // This is discounting some bytes due to Transfer payload
         int frameSize = protonSession.session.getConnection().getTransport().getOutboundFrameSizeLimit() - 50 - (delivery.getTag() != null ? delivery.getTag().length : 0);

         DeliveryAnnotations deliveryAnnotationsToEncode;

         message.checkReference(reference);

         if (reference.getProtocolData() != null && reference.getProtocolData() instanceof DeliveryAnnotations) {
            deliveryAnnotationsToEncode = (DeliveryAnnotations)reference.getProtocolData();
         } else {
            deliveryAnnotationsToEncode = null;
         }

         LargeBodyReader context = message.getLargeBodyReader();
         try {
            context.open();
            try {
               context.position(position);
               long bodySize = context.getSize();

               ByteBuffer buf = ByteBuffer.allocate(frameSize);

               for (; position < bodySize; ) {
                  if (!connection.flowControl(this::resume)) {
                     context.close();
                     return;
                  }
                  buf.clear();
                  int size = 0;

                  try {
                     if (position == 0) {
                        replaceInitialHeader(deliveryAnnotationsToEncode, context, WritableBuffer.ByteBufferWrapper.wrap(buf));
                     }
                     size = context.readInto(buf);

                     sender.send(new ReadableBuffer.ByteBufferReader(buf));
                     position += size;
                  } catch (java.nio.BufferOverflowException overflowException) {
                     if (position == 0) {
                        if (log.isDebugEnabled()) {
                           log.debug("Delivery of message failed with an overFlowException, retrying again with expandable buffer");
                        }
                        // on the very first packet, if the initial header was replaced with a much bigger header (re-encoding)
                        // we could recover the situation with a retry using an expandable buffer.
                        // this is tested on org.apache.activemq.artemis.tests.integration.amqp.AmqpMessageDivertsTest
                        size = retryInitialPacketWithExpandableBuffer(deliveryAnnotationsToEncode, context, buf);
                     } else {
                        // if this is not the position 0, something is going on
                        // we just forward the exception as this is not supposed to happen
                        throw overflowException;
                     }
                  }

                  if (size > 0) {

                     if (position < bodySize) {
                        connection.instantFlush();
                     }
                  }
               }
            } finally {
               context.close();
            }

            if (preSettle) {
               // Presettled means the client implicitly accepts any delivery we send it.
               try {
                  sessionSPI.ack(null, brokerConsumer, reference.getMessage());
               } catch (Exception e) {
                  log.debug(e.getMessage(), e);
               }
               delivery.settle();
            } else {
               sender.advance();
            }

            connection.instantFlush();

            synchronized (creditsLock) {
               pending.decrementAndGet();
            }

            finishLargeMessage();
         } catch (Exception e) {
            log.warn(e.getMessage(), e);
            brokerConsumer.errorProcessing(e, reference);
         }
      }

      /**
       * This is a retry logic when either the delivery annotations or re-encoded buffer is bigger than the frame size
       * This will create one expandable buffer.
       * It will then let Proton to do the framing correctly
       */
      private int retryInitialPacketWithExpandableBuffer(DeliveryAnnotations deliveryAnnotationsToEncode,
                                                         LargeBodyReader context,
                                                         ByteBuffer buf) throws Exception {
         int size;
         buf.clear();
         // if the buffer overflow happened during the initial position
         // this means the replaced headers are bigger then the frame size
         // on this case we do with an expandable netty buffer
         ByteBuf nettyBuffer = PooledByteBufAllocator.DEFAULT.buffer(AMQPMessageBrokerAccessor.getRemainingBodyPosition(message) * 2);
         try {
            replaceInitialHeader(deliveryAnnotationsToEncode, context, new NettyWritable(nettyBuffer));
            size = context.readInto(buf);
            position += size;

            nettyBuffer.writeBytes(buf);

            ByteBuffer nioBuffer = nettyBuffer.nioBuffer();
            nioBuffer.position(nettyBuffer.writerIndex());
            nioBuffer = (ByteBuffer) nioBuffer.flip();
            sender.send(new ReadableBuffer.ByteBufferReader(nioBuffer));
         } finally {
            nettyBuffer.release();
         }
         return size;
      }

      private int replaceInitialHeader(DeliveryAnnotations deliveryAnnotationsToEncode,
                                        LargeBodyReader context,
                                        WritableBuffer buf) throws Exception {
         TLSEncode.getEncoder().setByteBuffer(buf);
         try {
            int proposedPosition = writeHeaderAndAnnotations(context, deliveryAnnotationsToEncode);
            if (message.isReencoded()) {
               proposedPosition = writePropertiesAndApplicationProperties(context, message);
            }

            context.position(proposedPosition);
            position = proposedPosition;
            return (int)position;
         } finally {

            TLSEncode.getEncoder().setByteBuffer((WritableBuffer)null);
         }
      }

      /**
       * Write properties and application properties when the message is flagged as re-encoded.
       */
      private int writePropertiesAndApplicationProperties(LargeBodyReader context, AMQPLargeMessage message) throws Exception {
         int bodyPosition = AMQPMessageBrokerAccessor.getRemainingBodyPosition(message);
         assert bodyPosition > 0;
         writePropertiesAndApplicationPropertiesInternal(message);
         return bodyPosition;
      }

      private void writePropertiesAndApplicationPropertiesInternal(AMQPLargeMessage message) {
         Properties amqpProperties = AMQPMessageBrokerAccessor.getCurrentProperties(message);
         if (amqpProperties != null) {
            TLSEncode.getEncoder().writeObject(amqpProperties);
         }

         ApplicationProperties applicationProperties = AMQPMessageBrokerAccessor.getDecodedApplicationProperties(message);

         if (applicationProperties != null) {
            TLSEncode.getEncoder().writeObject(applicationProperties);
         }
      }

      private int writeHeaderAndAnnotations(LargeBodyReader context,
                                             DeliveryAnnotations deliveryAnnotationsToEncode) throws ActiveMQException {
         Header header = AMQPMessageBrokerAccessor.getCurrentHeader(message);
         if (header != null) {
            TLSEncode.getEncoder().writeObject(header);
         }
         if (deliveryAnnotationsToEncode != null) {
            TLSEncode.getEncoder().writeObject(deliveryAnnotationsToEncode);
         }
         return message.getPositionAfterDeliveryAnnotations();
      }
   }

   private void finishLargeMessage() {
      pendingLargeMessage = null;
      hasLarge = false;
      brokerConsumer.promptDelivery();
   }

   private void deliverLarge(MessageReference messageReference, AMQPLargeMessage message) {

      // we only need a tag if we are going to settle later
      byte[] tag = preSettle ? new byte[0] : protonSession.getTag();

      final Delivery delivery;
      delivery = sender.delivery(tag, 0, tag.length);
      delivery.setMessageFormat((int) message.getMessageFormat());
      delivery.setContext(messageReference);

      pendingLargeMessage = new LargeMessageDeliveryContext(messageReference, message, delivery);
      pendingLargeMessage.deliver();

   }

   private void deliverStandard(MessageReference messageReference, AMQPMessage message) {
      // Let the Message decide how to present the message bytes
      ReadableBuffer sendBuffer = message.getSendBuffer(messageReference.getDeliveryCount(), messageReference);
      // we only need a tag if we are going to settle later
      byte[] tag = preSettle ? new byte[0] : protonSession.getTag();

      boolean releaseRequired = sendBuffer instanceof NettyReadable;
      final Delivery delivery;
      delivery = sender.delivery(tag, 0, tag.length);
      delivery.setMessageFormat((int) message.getMessageFormat());
      delivery.setContext(messageReference);

      try {

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
            try {
               sessionSPI.ack(null, brokerConsumer, messageReference.getMessage());
            } catch (Exception e) {
               log.debug(e.getMessage(), e);
            }
            delivery.settle();
         } else {
            sender.advance();
         }

         connection.flush();
      } finally {
         synchronized (creditsLock) {
            pending.decrementAndGet();
         }
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
         return DestinationUtil.createQueueNameForSubscription(durable, clientID, subscriptionName);
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
      connection.requireInHandler();
      sender.drained();
      connection.instantFlush();
   }

   public AMQPSessionContext getSessionContext() {
      return protonSession;
   }

   class DefaultController implements SenderController {


      private boolean shared = false;
      boolean global = false;
      boolean multicast;
      final AMQPSessionCallback sessionSPI;
      SimpleString queue = null;
      SimpleString tempQueueName;
      String selector;

      private final RoutingType defaultRoutingType = RoutingType.ANYCAST;
      private RoutingType routingTypeToUse = RoutingType.ANYCAST;

      private boolean isVolatile = false;

      DefaultController(AMQPSessionCallback sessionSPI) {
         this.sessionSPI = sessionSPI;

      }

      @Override
      public Consumer init(ProtonServerSenderContext senderContext) throws Exception {
         Source source = (Source) sender.getRemoteSource();
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
            shared = hasRemoteDesiredCapability(sender, SHARED);
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
               addressToUse = SimpleString.toSimpleString(CompositeAddress.extractAddressName(source.getAddress()));
               queueNameToUse = SimpleString.toSimpleString(CompositeAddress.extractQueueName(source.getAddress()));
            } else {
               addressToUse = SimpleString.toSimpleString(source.getAddress());
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
                  //if client specifies fully qualified name that's allowed, don't throw exception.
                  if (queueNameToUse == null) {
                     throw new ActiveMQAMQPIllegalStateException("Address " + addressToUse + " is not configured for queue support");
                  }
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
                  //a queue consumer can receive from a multicast queue if it uses a fully qualified name
                  //setting routingType to null means do not check the routingType against the Queue's routing type.
                  routingTypeToUse = null;
                  SimpleString matchingAnycastQueue = getMatchingQueue(queueNameToUse, addressToUse, null);
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

         return (Consumer) sessionSPI.createSender(senderContext, queue, multicast ? null : selector, browseOnly);
      }


      private SimpleString getMatchingQueue(SimpleString queueName, SimpleString address, RoutingType routingType) throws Exception {
         if (queueName != null) {
            QueueQueryResult result = sessionSPI.queueQuery(CompositeAddress.toFullyQualified(address, queueName), routingType, true);
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


      @Override
      public void close() throws Exception {
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

   }
}
