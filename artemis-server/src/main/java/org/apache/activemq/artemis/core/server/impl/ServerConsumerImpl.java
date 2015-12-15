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
package org.apache.activemq.artemis.core.server.impl;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.client.impl.ClientConsumerImpl;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.message.BodyEncoder;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.HandleStatus;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.FutureLatch;
import org.apache.activemq.artemis.utils.LinkedListIterator;
import org.apache.activemq.artemis.utils.TypedProperties;

/**
 * Concrete implementation of a ClientConsumer.
 */
public class ServerConsumerImpl implements ServerConsumer, ReadyListener {
   // Constants ------------------------------------------------------------------------------------

   private static boolean isTrace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final long id;

   protected final Queue messageQueue;

   private final Filter filter;

   private final int minLargeMessageSize;

   private final ServerSession session;

   private final Object lock = new Object();

   private final boolean supportLargeMessage;

   private Object protocolContext;

   /**
    * We get a readLock when a message is handled, and return the readLock when the message is finally delivered
    * When stopping the consumer we need to get a writeLock to make sure we had all delivery finished
    * otherwise a rollback may get message sneaking in
    */
   private final ReadWriteLock lockDelivery = new ReentrantReadWriteLock();

   private volatile AtomicInteger availableCredits = new AtomicInteger(0);

   private boolean started;

   private volatile LargeMessageDeliverer largeMessageDeliverer = null;

   @Override
   public String debug() {
      return toString() + "::Delivering " + this.deliveringRefs.size();
   }

   /**
    * if we are a browse only consumer we don't need to worry about acknowledgements or being
    * started/stopped by the session.
    */
   private final boolean browseOnly;

   protected BrowserDeliverer browserDeliverer;

   private final boolean strictUpdateDeliveryCount;

   private final StorageManager storageManager;

   protected final java.util.Queue<MessageReference> deliveringRefs = new ConcurrentLinkedQueue<>();

   private final SessionCallback callback;

   private final boolean preAcknowledge;

   private final ManagementService managementService;

   private final Binding binding;

   private boolean transferring = false;

   private final long creationTime;

   private AtomicLong consumerRateCheckTime = new AtomicLong(System.currentTimeMillis());

   private AtomicLong messageConsumedSnapshot = new AtomicLong(0);

   private long acks;

   // Constructors ---------------------------------------------------------------------------------

   public ServerConsumerImpl(final long id,
                             final ServerSession session,
                             final QueueBinding binding,
                             final Filter filter,
                             final boolean started,
                             final boolean browseOnly,
                             final StorageManager storageManager,
                             final SessionCallback callback,
                             final boolean preAcknowledge,
                             final boolean strictUpdateDeliveryCount,
                             final ManagementService managementService) throws Exception {
      this(id, session, binding, filter, started, browseOnly, storageManager, callback, preAcknowledge, strictUpdateDeliveryCount, managementService, true, null);
   }

   public ServerConsumerImpl(final long id,
                             final ServerSession session,
                             final QueueBinding binding,
                             final Filter filter,
                             final boolean started,
                             final boolean browseOnly,
                             final StorageManager storageManager,
                             final SessionCallback callback,
                             final boolean preAcknowledge,
                             final boolean strictUpdateDeliveryCount,
                             final ManagementService managementService,
                             final boolean supportLargeMessage,
                             final Integer credits) throws Exception {
      this.id = id;

      this.filter = filter;

      this.session = session;

      this.binding = binding;

      messageQueue = binding.getQueue();

      this.started = browseOnly || started;

      this.browseOnly = browseOnly;

      this.storageManager = storageManager;

      this.callback = callback;

      this.preAcknowledge = preAcknowledge;

      this.managementService = managementService;

      minLargeMessageSize = session.getMinLargeMessageSize();

      this.strictUpdateDeliveryCount = strictUpdateDeliveryCount;

      this.creationTime = System.currentTimeMillis();

      if (browseOnly) {
         browserDeliverer = new BrowserDeliverer(messageQueue.totalIterator());
      }
      else {
         messageQueue.addConsumer(this);
      }
      this.supportLargeMessage = supportLargeMessage;

      if (credits != null) {
         if (credits == -1) {
            availableCredits = null;
         }
         else {
            availableCredits.set(credits);
         }
      }
   }

   @Override
   public void readyForWriting() {
      promptDelivery();
   }

   // ServerConsumer implementation
   // ----------------------------------------------------------------------

   @Override
   public Object getProtocolContext() {
      return protocolContext;
   }

   @Override
   public void setProtocolContext(Object protocolContext) {
      this.protocolContext = protocolContext;
   }

   @Override
   public long getID() {
      return id;
   }

   @Override
   public boolean isBrowseOnly() {
      return browseOnly;
   }

   @Override
   public long getCreationTime() {
      return creationTime;
   }

   @Override
   public String getConnectionID() {
      return this.session.getConnectionID().toString();
   }

   @Override
   public String getSessionID() {
      return this.session.getName();
   }

   @Override
   public List<MessageReference> getDeliveringMessages() {
      List<MessageReference> refs = new LinkedList<>();
      synchronized (lock) {
         List<MessageReference> refsOnConsumer = session.getInTXMessagesForConsumer(this.id);
         if (refsOnConsumer != null) {
            refs.addAll(refsOnConsumer);
         }
         refs.addAll(deliveringRefs);
      }

      return refs;
   }

   @Override
   public HandleStatus handle(final MessageReference ref) throws Exception {
      if (callback != null && !callback.hasCredits(this) || availableCredits != null && availableCredits.get() <= 0) {
         if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
            ActiveMQServerLogger.LOGGER.debug(this + " is busy for the lack of credits. Current credits = " +
                                                 availableCredits +
                                                 " Can't receive reference " +
                                                 ref);
         }

         return HandleStatus.BUSY;
      }

      synchronized (lock) {
         // If the consumer is stopped then we don't accept the message, it
         // should go back into the
         // queue for delivery later.
         // TCP-flow control has to be done first than everything else otherwise we may lose notifications
         if (!callback.isWritable(this) || !started || transferring ) {
            return HandleStatus.BUSY;
         }

         // If there is a pendingLargeMessage we can't take another message
         // This has to be checked inside the lock as the set to null is done inside the lock
         if (largeMessageDeliverer != null) {
            if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
               ActiveMQServerLogger.LOGGER.debug(this + " is busy delivering large message " +
                                                    largeMessageDeliverer +
                                                    ", can't deliver reference " +
                                                    ref);
            }
            return HandleStatus.BUSY;
         }
         final ServerMessage message = ref.getMessage();

         if (filter != null && !filter.match(message)) {
            if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
               ActiveMQServerLogger.LOGGER.trace("Reference " + ref + " is a noMatch on consumer " + this);
            }
            return HandleStatus.NO_MATCH;
         }

         if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
            ActiveMQServerLogger.LOGGER.trace("ServerConsumerImpl::" + this + " Handling reference " + ref);
         }
         if (!browseOnly) {
            if (!preAcknowledge) {
               deliveringRefs.add(ref);
            }

            ref.handled();

            ref.setConsumerId(this.id);

            ref.incrementDeliveryCount();

            // If updateDeliveries = false (set by strict-update),
            // the updateDeliveryCount would still be updated after c
            if (strictUpdateDeliveryCount && !ref.isPaged()) {
               if (ref.getMessage().isDurable() && ref.getQueue().isDurable() &&
                  !ref.getQueue().isInternalQueue() &&
                  !ref.isPaged()) {
                  storageManager.updateDeliveryCount(ref);
               }
            }

            if (preAcknowledge) {
               if (message.isLargeMessage()) {
                  // we must hold one reference, or the file will be deleted before it could be delivered
                  ((LargeServerMessage) message).incrementDelayDeletionCount();
               }

               // With pre-ack, we ack *before* sending to the client
               ref.getQueue().acknowledge(ref);
            }

         }

         if (message.isLargeMessage() && this.supportLargeMessage) {
            largeMessageDeliverer = new LargeMessageDeliverer((LargeServerMessage) message, ref);
         }

         lockDelivery.readLock().lock();

         return HandleStatus.HANDLED;
      }
   }

   @Override
   public void proceedDeliver(MessageReference reference) throws Exception {
      try {
         ServerMessage message = reference.getMessage();

         if (message.isLargeMessage() && supportLargeMessage) {
            if (largeMessageDeliverer == null) {
               // This can't really happen as handle had already crated the deliverer
               // instead of throwing an exception in weird cases there is no problem on just go ahead and create it
               // again here
               largeMessageDeliverer = new LargeMessageDeliverer((LargeServerMessage) message, reference);
            }
            // The deliverer was prepared during handle, as we can't have more than one pending large message
            // as it would return busy if there is anything pending
            largeMessageDeliverer.deliver();
         }
         else {
            deliverStandardMessage(reference, message);
         }
      }
      finally {
         lockDelivery.readLock().unlock();
      }
   }

   @Override
   public Filter getFilter() {
      return filter;
   }

   @Override
   public void close(final boolean failed) throws Exception {
      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("ServerConsumerImpl::" + this + " being closed with failed=" + failed, new Exception("trace"));
      }

      setStarted(false);

      LargeMessageDeliverer del = largeMessageDeliverer;

      if (del != null) {
         del.finish();
      }

      removeItself();

      LinkedList<MessageReference> refs = cancelRefs(failed, false, null);

      Iterator<MessageReference> iter = refs.iterator();

      Transaction tx = new TransactionImpl(storageManager);

      while (iter.hasNext()) {
         MessageReference ref = iter.next();

         if (isTrace) {
            ActiveMQServerLogger.LOGGER.trace("ServerConsumerImpl::" + this + " cancelling reference " + ref);
         }

         ref.getQueue().cancel(tx, ref, true);
      }

      tx.rollback();

      if (!browseOnly) {
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

         props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

         props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

         props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filter == null ? null : filter.getFilterString());

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

         props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, messageQueue.getConsumerCount());

         // HORNETQ-946
         props.putSimpleStringProperty(ManagementHelper.HDR_USER, SimpleString.toSimpleString(session.getUsername()));

         props.putSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS, SimpleString.toSimpleString(((ServerSessionImpl) session).getRemotingConnection().getRemoteAddress()));

         props.putSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME, SimpleString.toSimpleString(session.getName()));

         Notification notification = new Notification(null, CoreNotificationType.CONSUMER_CLOSED, props);

         managementService.sendNotification(notification);
      }
   }

   @Override
   public void removeItself() throws Exception {
      if (browseOnly) {
         browserDeliverer.close();
      }
      else {
         messageQueue.removeConsumer(this);
      }

      session.removeConsumer(id);
   }

   /**
    * Prompt delivery and send a "forced delivery" message to the consumer.
    * <p>
    * When the consumer receives such a "forced delivery" message, it discards it and knows that
    * there are no other messages to be delivered.
    */
   @Override
   public synchronized void forceDelivery(final long sequence) {
      promptDelivery();

      // JBPAPP-6030 - Using the executor to avoid distributed dead locks
      messageQueue.getExecutor().execute(new Runnable() {
         @Override
         public void run() {
            try {
               // We execute this on the same executor to make sure the force delivery message is written after
               // any delivery is completed

               synchronized (lock) {
                  if (transferring) {
                     // Case it's transferring (reattach), we will retry later
                     messageQueue.getExecutor().execute(new Runnable() {
                        @Override
                        public void run() {
                           forceDelivery(sequence);
                        }
                     });
                  }
                  else {
                     ServerMessage forcedDeliveryMessage = new ServerMessageImpl(storageManager.generateID(), 50);

                     forcedDeliveryMessage.putLongProperty(ClientConsumerImpl.FORCED_DELIVERY_MESSAGE, sequence);
                     forcedDeliveryMessage.setAddress(messageQueue.getName());

                     callback.sendMessage(forcedDeliveryMessage, ServerConsumerImpl.this, 0);
                  }
               }
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorSendingForcedDelivery(e);
            }
         }
      });

   }

   @Override
   public LinkedList<MessageReference> cancelRefs(final boolean failed,
                                                  final boolean lastConsumedAsDelivered,
                                                  final Transaction tx) throws Exception {
      boolean performACK = lastConsumedAsDelivered;

      try {
         if (largeMessageDeliverer != null) {
            largeMessageDeliverer.finish();
         }
      }
      catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.errorResttingLargeMessage(e, largeMessageDeliverer);
      }
      finally {
         largeMessageDeliverer = null;
      }

      LinkedList<MessageReference> refs = new LinkedList<>();

      synchronized (lock) {
         if (!deliveringRefs.isEmpty()) {
            for (MessageReference ref : deliveringRefs) {
               if (performACK) {
                  ackReference(tx, ref);

                  performACK = false;
               }
               else {
                  refs.add(ref);
                  if (!failed) {
                     // We don't decrement delivery count if the client failed, since there's a possibility that refs
                     // were actually delivered but we just didn't get any acks for them
                     // before failure
                     ref.decrementDeliveryCount();
                  }
               }

               if (isTrace) {
                  ActiveMQServerLogger.LOGGER.trace("ServerConsumerImpl::" + this + " Preparing Cancelling list for messageID = " + ref.getMessage().getMessageID() + ", ref = " + ref);
               }
            }

            deliveringRefs.clear();
         }
      }

      return refs;
   }

   @Override
   public void setStarted(final boolean started) {
      synchronized (lock) {
         // This is to make sure that the delivery process has finished any pending delivery
         // otherwise a message may sneak in on the client while we are trying to stop the consumer
         lockDelivery.writeLock().lock();
         try {
            this.started = browseOnly || started;
         }
         finally {
            lockDelivery.writeLock().unlock();
         }
      }

      // Outside the lock
      if (started) {
         promptDelivery();
      }
   }

   @Override
   public void setTransferring(final boolean transferring) {
      synchronized (lock) {
         // This is to make sure that the delivery process has finished any pending delivery
         // otherwise a message may sneak in on the client while we are trying to stop the consumer
         lockDelivery.writeLock().lock();
         try {
            this.transferring = transferring;
         }
         finally {
            lockDelivery.writeLock().unlock();
         }
      }

      // Outside the lock
      if (transferring) {
         // And we must wait for any force delivery to be executed - this is executed async so we add a future to the
         // executor and
         // wait for it to complete

         FutureLatch future = new FutureLatch();

         messageQueue.getExecutor().execute(future);

         boolean ok = future.await(10000);

         if (!ok) {
            ActiveMQServerLogger.LOGGER.errorTransferringConsumer();
         }
      }

      if (!transferring) {
         promptDelivery();
      }
   }

   @Override
   public void receiveCredits(final int credits) {
      if (credits == -1) {
         if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
            ActiveMQServerLogger.LOGGER.debug(this + ":: FlowControl::Received disable flow control message");
         }
         // No flow control
         availableCredits = null;

         // There may be messages already in the queue
         promptDelivery();
      }
      else if (credits == 0) {
         // reset, used on slow consumers
         ActiveMQServerLogger.LOGGER.debug(this + ":: FlowControl::Received reset flow control message");
         availableCredits.set(0);
      }
      else {
         int previous = availableCredits.getAndAdd(credits);

         if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
            ActiveMQServerLogger.LOGGER.debug(this + "::FlowControl::Received " +
                                                 credits +
                                                 " credits, previous value = " +
                                                 previous +
                                                 " currentValue = " +
                                                 availableCredits.get());
         }

         if (previous <= 0 && previous + credits > 0) {
            if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
               ActiveMQServerLogger.LOGGER.trace(this + "::calling promptDelivery from receiving credits");
            }
            promptDelivery();
         }
      }
   }

   @Override
   public Queue getQueue() {
      return messageQueue;
   }

   @Override
   public void acknowledge(Transaction tx, final long messageID) throws Exception {
      if (browseOnly) {
         return;
      }

      // Acknowledge acknowledges all refs delivered by the consumer up to and including the one explicitly
      // acknowledged

      // We use a transaction here as if the message is not found, we should rollback anything done
      // This could eventually happen on retries during transactions, and we need to make sure we don't ACK things we are not supposed to acknowledge

      boolean startedTransaction = false;

      if (tx == null) {
         startedTransaction = true;
         tx = new TransactionImpl(storageManager);
      }

      try {

         MessageReference ref;
         do {
            synchronized (lock) {
               ref = deliveringRefs.poll();
            }

            if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
               ActiveMQServerLogger.LOGGER.trace("ACKing ref " + ref + " on tx= " + tx + ", consumer=" + this);
            }

            if (ref == null) {
               ActiveMQIllegalStateException ils = ActiveMQMessageBundle.BUNDLE.consumerNoReference(id, messageID, messageQueue.getName());
               if (tx != null) {
                  tx.markAsRollbackOnly(ils);
               }
               throw ils;
            }

            ackReference(tx, ref);
            acks++;
         } while (ref.getMessage().getMessageID() != messageID);

         if (startedTransaction) {
            tx.commit();
         }
      }
      catch (ActiveMQException e) {
         if (startedTransaction) {
            tx.rollback();
         }
         else {
            tx.markAsRollbackOnly(e);
         }
         throw e;
      }
      catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.errorAckingMessage((Exception) e);
         ActiveMQException activeMQIllegalStateException = new ActiveMQIllegalStateException(e.getMessage());
         if (startedTransaction) {
            tx.rollback();
         }
         else {
            tx.markAsRollbackOnly(activeMQIllegalStateException);
         }
         throw activeMQIllegalStateException;
      }
   }

   private void ackReference(Transaction tx, MessageReference ref) throws Exception {
      if (tx == null) {
         ref.getQueue().acknowledge(ref);
      }
      else {
         ref.getQueue().acknowledge(tx, ref);
      }
   }

   @Override
   public void individualAcknowledge(final Transaction tx, final long messageID) throws Exception {
      if (browseOnly) {
         return;
      }

      MessageReference ref = removeReferenceByID(messageID);

      if (ref == null) {
         ActiveMQIllegalStateException ils = ActiveMQMessageBundle.BUNDLE.consumerNoReference(id, messageID, messageQueue.getName());
         if (tx != null) {
            tx.markAsRollbackOnly(ils);
         }
         throw ils;
      }

      ackReference(tx, ref);

      acks++;
   }

   @Override
   public void individualCancel(final long messageID, boolean failed) throws Exception {
      if (browseOnly) {
         return;
      }

      MessageReference ref = removeReferenceByID(messageID);

      if (ref == null) {
         throw new IllegalStateException("Cannot find ref to ack " + messageID);
      }

      if (!failed) {
         ref.decrementDeliveryCount();
      }

      ref.getQueue().cancel(ref, System.currentTimeMillis());
   }

   @Override
   public MessageReference removeReferenceByID(final long messageID) throws Exception {
      if (browseOnly) {
         return null;
      }

      // Expiries can come in out of sequence with respect to delivery order

      synchronized (lock) {
         // This is an optimization, if the reference is the first one, we just poll it.
         if (deliveringRefs.peek().getMessage().getMessageID() == messageID) {
            return deliveringRefs.poll();
         }

         Iterator<MessageReference> iter = deliveringRefs.iterator();

         MessageReference ref = null;

         while (iter.hasNext()) {
            MessageReference theRef = iter.next();

            if (theRef.getMessage().getMessageID() == messageID) {
               iter.remove();

               ref = theRef;

               break;
            }
         }
         return ref;
      }
   }

   /**
    * To be used on tests only
    */
   public AtomicInteger getAvailableCredits() {
      return availableCredits;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      return "ServerConsumerImpl [id=" + id + ", filter=" + filter + ", binding=" + binding + "]";
   }

   @Override
   public String toManagementString() {
      return "ServerConsumer [id=" + getConnectionID() + ":" + getSessionID() + ":" + id + ", filter=" + filter + ", binding=" + binding.toManagementString() + "]";
   }

   @Override
   public void disconnect() {
      callback.disconnect(this, getQueue().getName().toString());
   }

   public float getRate() {
      float timeSlice = ((System.currentTimeMillis() - consumerRateCheckTime.getAndSet(System.currentTimeMillis())) / 1000.0f);
      if (timeSlice == 0) {
         messageConsumedSnapshot.getAndSet(acks);
         return 0.0f;
      }
      return BigDecimal.valueOf((acks - messageConsumedSnapshot.getAndSet(acks)) / timeSlice).setScale(2, BigDecimal.ROUND_UP).floatValue();
   }

   // Private --------------------------------------------------------------------------------------

   @Override
   public void promptDelivery() {
      // largeMessageDeliverer is always set inside a lock
      // if we don't acquire a lock, we will have NPE eventually
      if (largeMessageDeliverer != null) {
         resumeLargeMessage();
      }
      else {
         forceDelivery();
      }
   }

   private void forceDelivery() {
      if (browseOnly) {
         messageQueue.getExecutor().execute(browserDeliverer);
      }
      else {
         messageQueue.deliverAsync();
      }
   }

   private void resumeLargeMessage() {
      messageQueue.getExecutor().execute(resumeLargeMessageRunnable);
   }

   /**
    * @param ref
    * @param message
    */
   private void deliverStandardMessage(final MessageReference ref, final ServerMessage message) {
      int packetSize = callback.sendMessage(message, ServerConsumerImpl.this, ref.getDeliveryCount());

      if (availableCredits != null) {
         availableCredits.addAndGet(-packetSize);

         if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
            ActiveMQServerLogger.LOGGER.trace(this + "::FlowControl::delivery standard taking " +
                                                 packetSize +
                                                 " from credits, available now is " +
                                                 availableCredits);
         }
      }
   }

   // Inner classes
   // ------------------------------------------------------------------------

   private final Runnable resumeLargeMessageRunnable = new Runnable() {
      @Override
      public void run() {
         synchronized (lock) {
            try {
               if (largeMessageDeliverer == null || largeMessageDeliverer.deliver()) {
                  forceDelivery();
               }
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorRunningLargeMessageDeliverer(e);
            }
         }
      }
   };

   /**
    * Internal encapsulation of the logic on sending LargeMessages.
    * This Inner class was created to avoid a bunch of loose properties about the current LargeMessage being sent
    */
   private final class LargeMessageDeliverer {

      private long sizePendingLargeMessage;

      private LargeServerMessage largeMessage;

      private final MessageReference ref;

      private boolean sentInitialPacket = false;

      /**
       * The current position on the message being processed
       */
      private long positionPendingLargeMessage;

      private BodyEncoder context;

      public LargeMessageDeliverer(final LargeServerMessage message, final MessageReference ref) throws Exception {
         largeMessage = message;

         largeMessage.incrementDelayDeletionCount();

         this.ref = ref;
      }

      public boolean deliver() throws Exception {
         lockDelivery.readLock().lock();
         try {
            if (largeMessage == null) {
               return true;
            }

            if (availableCredits != null && availableCredits.get() <= 0) {
               if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
                  ActiveMQServerLogger.LOGGER.trace(this + "::FlowControl::delivery largeMessage interrupting as there are no more credits, available=" +
                                                       availableCredits);
               }

               return false;
            }

            if (!sentInitialPacket) {
               context = largeMessage.getBodyEncoder();

               sizePendingLargeMessage = context.getLargeBodySize();

               context.open();

               sentInitialPacket = true;

               int packetSize = callback.sendLargeMessage(largeMessage, ServerConsumerImpl.this, context.getLargeBodySize(), ref.getDeliveryCount());

               if (availableCredits != null) {
                  availableCredits.addAndGet(-packetSize);

                  if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
                     ActiveMQServerLogger.LOGGER.trace(this + "::FlowControl::" +
                                                          " deliver initialpackage with " +
                                                          packetSize +
                                                          " delivered, available now = " +
                                                          availableCredits);
                  }
               }

               // Execute the rest of the large message on a different thread so as not to tie up the delivery thread
               // for too long

               resumeLargeMessage();

               return false;
            }
            else {
               if (availableCredits != null && availableCredits.get() <= 0) {
                  if (ServerConsumerImpl.isTrace) {
                     ActiveMQServerLogger.LOGGER.trace(this + "::FlowControl::deliverLargeMessage Leaving loop of send LargeMessage because of credits, available=" +
                                                          availableCredits);
                  }

                  return false;
               }

               int localChunkLen = 0;

               localChunkLen = (int) Math.min(sizePendingLargeMessage - positionPendingLargeMessage, minLargeMessageSize);

               ActiveMQBuffer bodyBuffer = ActiveMQBuffers.fixedBuffer(localChunkLen);

               context.encode(bodyBuffer, localChunkLen);

               byte[] body = bodyBuffer.toByteBuffer().array();

               int packetSize = callback.sendLargeMessageContinuation(ServerConsumerImpl.this, body, positionPendingLargeMessage + localChunkLen < sizePendingLargeMessage, false);

               int chunkLen = body.length;

               if (availableCredits != null) {
                  availableCredits.addAndGet(-packetSize);

                  if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
                     ActiveMQServerLogger.LOGGER.trace(this + "::FlowControl::largeMessage deliver continuation, packetSize=" +
                                                          packetSize +
                                                          " available now=" +
                                                          availableCredits);
                  }
               }

               positionPendingLargeMessage += chunkLen;

               if (positionPendingLargeMessage < sizePendingLargeMessage) {
                  resumeLargeMessage();

                  return false;
               }
            }

            if (ServerConsumerImpl.isTrace) {
               ActiveMQServerLogger.LOGGER.trace("Finished deliverLargeMessage");
            }

            finish();

            return true;
         }
         finally {
            lockDelivery.readLock().unlock();
         }
      }

      public void finish() throws Exception {
         synchronized (lock) {
            if (largeMessage == null) {
               // handleClose could be calling close while handle is also calling finish.
               // As a result one of them could get here after the largeMessage is already gone.
               // On that case we just ignore this call
               return;
            }
            if (context != null) {
               context.close();
            }

            largeMessage.releaseResources();

            largeMessage.decrementDelayDeletionCount();

            if (preAcknowledge && !browseOnly) {
               // PreAck will have an extra reference
               largeMessage.decrementDelayDeletionCount();
            }

            largeMessageDeliverer = null;

            largeMessage = null;
         }
      }
   }

   protected class BrowserDeliverer implements Runnable {

      protected MessageReference current = null;

      public BrowserDeliverer(final LinkedListIterator<MessageReference> iterator) {
         this.iterator = iterator;
      }

      public final LinkedListIterator<MessageReference> iterator;

      public synchronized void close() {
         iterator.close();
      }

      @Override
      public synchronized void run() {
         // if the reference was busy during the previous iteration, handle it now
         if (current != null) {
            try {
               HandleStatus status = handle(current);

               if (status == HandleStatus.BUSY) {
                  return;
               }

               if (status == HandleStatus.HANDLED) {
                  proceedDeliver(current);
               }

               current = null;
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorBrowserHandlingMessage(e, current);
               return;
            }
         }

         MessageReference ref = null;
         HandleStatus status;

         while (true) {
            try {
               ref = null;
               synchronized (messageQueue) {
                  if (!iterator.hasNext()) {
                     break;
                  }

                  ref = iterator.next();

                  status = handle(ref);
               }

               if (status == HandleStatus.HANDLED) {
                  proceedDeliver(ref);
               }
               else if (status == HandleStatus.BUSY) {
                  // keep a reference on the current message reference
                  // to handle it next time the browser deliverer is executed
                  current = ref;
                  break;
               }
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorBrowserHandlingMessage(e, ref);
               break;
            }
         }
      }

      public boolean isBrowsed() {
         messageQueue.deliverAsync();
         boolean b = !iterator.hasNext();
         return b;
      }
   }
}
