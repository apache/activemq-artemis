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
package org.apache.activemq.artemis.protocol.amqp.connect.mirror;

import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.ToIntFunction;

import org.apache.activemq.artemis.api.core.ActiveMQAddressDoesNotExistException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.RunnableCallback;
import org.apache.activemq.artemis.core.paging.cursor.PagedReference;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.server.mirror.MirrorController;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessageBrokerAccessor;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonAbstractReceiver;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.pools.MpscPool;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.ADD_ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.BROKER_ID;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.CREATE_QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.DELETE_ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.DELETE_QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.EVENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_BROKER_ID_EXTRA_PROPERTY;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_DESTINATION;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_ID;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.POST_ACK;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_ID_EXTRA_PROPERTY;

public class AMQPMirrorControllerTarget extends ProtonAbstractReceiver implements MirrorController {

   private static final Logger logger = LoggerFactory.getLogger(AMQPMirrorControllerTarget.class);

   private static final ThreadLocal<MirrorController> CONTROLLER_THREAD_LOCAL = new ThreadLocal<>();

   public static void setControllerInUse(MirrorController controller) {
      CONTROLLER_THREAD_LOCAL.set(controller);
   }

   public static MirrorController getControllerInUse() {
      return CONTROLLER_THREAD_LOCAL.get();
   }

   /**
    * Objects of this class can be used by either transaction or by OperationContext.
    * It is important that when you're using the transactions you clear any references to
    * the operation context. Don't use transaction and OperationContext at the same time
    * as that would generate duplicates on the objects cache.
    */
   class ACKMessageOperation implements IOCallback, Runnable {

      Delivery delivery;

      /**
       * notice that when you use the Transaction, you need to make sure you don't use the IO
       */
      public TransactionOperationAbstract tx = new TransactionOperationAbstract() {
         @Override
         public void afterCommit(Transaction tx) {
            connectionRun();
         }
      };

      void reset() {
         this.delivery = null;
      }

      ACKMessageOperation setDelivery(Delivery delivery) {
         this.delivery = delivery;
         return this;
      }

      @Override
      public void run() {
         if (logger.isTraceEnabled()) {
            logger.trace("Delivery settling for " + delivery + ", context=" + delivery.getContext());
         }
         delivery.disposition(Accepted.getInstance());
         settle(delivery);
         connection.flush();
         AMQPMirrorControllerTarget.this.ackMessageMpscPool.release(ACKMessageOperation.this);
      }

      @Override
      public void done() {
         connectionRun();
      }

      public void connectionRun() {
         connection.runNow(ACKMessageOperation.this);
      }

      @Override
      public void onError(int errorCode, String errorMessage) {
         logger.warn("{}-{}", errorCode, errorMessage);
      }
   }

   // in a regular case we should not have more than amqpCredits on the pool, that's the max we would need
   private final MpscPool<ACKMessageOperation> ackMessageMpscPool = new MpscPool<>(amqpCredits, ACKMessageOperation::reset, ACKMessageOperation::new);

   final RoutingContextImpl routingContext = new RoutingContextImpl(null);

   final BasicMirrorController<Receiver> basicController;

   final ActiveMQServer server;

   DuplicateIDCache lruduplicateIDCache;
   String lruDuplicateIDKey;

   private final ReferenceNodeStore referenceNodeStore;

   OperationContext mirrorContext;

   public AMQPMirrorControllerTarget(AMQPSessionCallback sessionSPI,
                                     AMQPConnectionContext connection,
                                     AMQPSessionContext protonSession,
                                     Receiver receiver,
                                     ActiveMQServer server) {
      super(sessionSPI, connection, protonSession, receiver);
      this.basicController = new BasicMirrorController(server);
      this.basicController.setLink(receiver);
      this.server = server;
      this.referenceNodeStore = sessionSPI.getProtocolManager().getReferenceIDSupplier();
      mirrorContext = protonSession.getSessionSPI().getSessionContext();
   }

   @Override
   public String getRemoteMirrorId() {
      return basicController.getRemoteMirrorId();
   }

   @Override
   public void flow() {
      creditRunnable.run();
   }

   @Override
   protected void actualDelivery(AMQPMessage message, Delivery delivery, Receiver receiver, Transaction tx) {
      recoverContext();
      incrementSettle();

      if (logger.isTraceEnabled()) {
         logger.trace(server + "::actualdelivery call for " + message);
      }
      setControllerInUse(this);

      delivery.setContext(message);

      ACKMessageOperation messageAckOperation = this.ackMessageMpscPool.borrow().setDelivery(delivery);

      try {
         /** We use message annotations, because on the same link we will receive control messages
          *  coming from mirror events,
          *  and the actual messages that need to be replicated.
          *  Using anything from the body would force us to parse the body on regular messages.
          *  The body of the message may still be used on control messages, on cases where a JSON string is sent. */
         Object eventType = AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, EVENT_TYPE);
         if (eventType != null) {
            if (eventType.equals(ADD_ADDRESS)) {
               AddressInfo addressInfo = parseAddress(message);

               addAddress(addressInfo);
            } else if (eventType.equals(DELETE_ADDRESS)) {
               AddressInfo addressInfo = parseAddress(message);

               deleteAddress(addressInfo);
            } else if (eventType.equals(CREATE_QUEUE)) {
               QueueConfiguration queueConfiguration = parseQueue(message);

               createQueue(queueConfiguration);
            } else if (eventType.equals(DELETE_QUEUE)) {
               String address = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, ADDRESS);
               String queueName = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, QUEUE);

               deleteQueue(SimpleString.toSimpleString(address), SimpleString.toSimpleString(queueName));
            } else if (eventType.equals(POST_ACK)) {
               String nodeID = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, BROKER_ID);

               AckReason ackReason = AMQPMessageBrokerAccessor.getMessageAnnotationAckReason(message);

               if (nodeID == null) {
                  nodeID = getRemoteMirrorId(); // not sending the nodeID means it's data generated on that broker
               }
               String queueName = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, QUEUE);
               AmqpValue value = (AmqpValue) message.getBody();
               Long messageID = (Long) value.getValue();
               if (postAcknowledge(queueName, nodeID, messageID, messageAckOperation, ackReason)) {
                  messageAckOperation = null;
               }
            }
         } else {
            if (sendMessage(message, messageAckOperation)) {
               // since the send was successful, we give up the reference here,
               // so there won't be any call on afterCompleteOperations
               messageAckOperation = null;
            }
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      } finally {
         setControllerInUse(null);
         if (messageAckOperation != null) {
            server.getStorageManager().afterCompleteOperations(messageAckOperation);
         }
      }
   }

   @Override
   public void initialize() throws Exception {
      super.initialize();

      // Match the settlement mode of the remote instead of relying on the default of MIXED.
      receiver.setSenderSettleMode(receiver.getRemoteSenderSettleMode());

      // We don't currently support SECOND so enforce that the answer is anlways FIRST
      receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
      flow();
   }

   private QueueConfiguration parseQueue(AMQPMessage message) {
      AmqpValue bodyValue = (AmqpValue) message.getBody();
      String body = (String) bodyValue.getValue();
      return QueueConfiguration.fromJSON(body);
   }

   private AddressInfo parseAddress(AMQPMessage message) {
      AmqpValue bodyValue = (AmqpValue) message.getBody();
      String body = (String) bodyValue.getValue();
      return AddressInfo.fromJSON(body);
   }

   @Override
   public void addAddress(AddressInfo addressInfo) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug(server + " adding address " + addressInfo);
      }
      server.addAddressInfo(addressInfo);
   }

   @Override
   public void deleteAddress(AddressInfo addressInfo) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug(server + " delete address " + addressInfo);
      }
      try {
         server.removeAddressInfo(addressInfo.getName(), null, true);
      } catch (ActiveMQAddressDoesNotExistException expected) {
         // it was removed from somewhere else, which is fine
         logger.debug(expected.getMessage(), expected);
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

   @Override
   public void createQueue(QueueConfiguration queueConfiguration) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug(server + " adding queue " + queueConfiguration);
      }
      try {
         server.createQueue(queueConfiguration, true);
      } catch (Exception e) {
         logger.debug("Queue could not be created, already existed " + queueConfiguration, e);
      }
   }

   @Override
   public void deleteQueue(SimpleString addressName, SimpleString queueName) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug(server + " destroy queue " + queueName + " on address = " + addressName + " server " + server.getIdentity());
      }
      try {
         server.destroyQueue(queueName, null, false, true, false, false);
      } catch (ActiveMQNonExistentQueueException expected) {
         logger.debug(server + " queue " + queueName + " was previously removed", expected);
      }
   }

   public boolean postAcknowledge(String queue,
                                  String nodeID,
                                  long messageID,
                                  ACKMessageOperation ackMessage,
                                  AckReason reason) throws Exception {
      final Queue targetQueue = server.locateQueue(queue);

      if (targetQueue == null) {
         logger.warn("Queue " + queue + " not found on mirror target, ignoring ack for queue=" + queue + ", messageID=" + messageID + ", nodeID=" + nodeID);
         return false;
      }

      if (logger.isDebugEnabled()) {
         // we only do the following check if debug
         if (targetQueue.getConsumerCount() > 0) {
            logger.debug("server " + server.getIdentity() + ", queue " + targetQueue.getName() + " has consumers while delivering ack for " + messageID);
         }
      }

      if (logger.isTraceEnabled()) {
         logger.trace("Server " + server.getIdentity() + " with queue = " + queue + " being acked for " + messageID + " coming from " + messageID + " targetQueue = " + targetQueue);
      }

      performAck(nodeID, messageID, targetQueue, ackMessage, reason, (short)0);
      return true;
   }

   public void performAckOnPage(String nodeID, long messageID, Queue targetQueue, IOCallback ackMessageOperation) {
      PageAck pageAck = new PageAck(targetQueue, nodeID, messageID, ackMessageOperation);
      targetQueue.getPageSubscription().scanAck(pageAck, pageAck, pageAck, pageAck);
   }

   private void performAck(String nodeID, long messageID, Queue targetQueue, ACKMessageOperation ackMessageOperation, AckReason reason, final short retry) {
      if (logger.isTraceEnabled()) {
         logger.trace("performAck (nodeID=" + nodeID + ", messageID=" + messageID + ")" + ", targetQueue=" + targetQueue.getName());
      }
      MessageReference reference = targetQueue.removeWithSuppliedID(nodeID, messageID, referenceNodeStore);

      if (reference == null) {
         if (logger.isDebugEnabled()) {
            logger.debug("Retrying Reference not found on messageID=" + messageID + " nodeID=" + nodeID + ", currentRetry=" + retry);
         }
         switch (retry) {
            case 0:
               // first retry, after IO Operations
               sessionSPI.getSessionContext().executeOnCompletion(new RunnableCallback(() -> performAck(nodeID, messageID, targetQueue, ackMessageOperation, reason, (short) 1)));
               return;
            case 1:
               // second retry after the queue is flushed the temporary adds
               targetQueue.flushOnIntermediate(() -> {
                  recoverContext();
                  performAck(nodeID, messageID, targetQueue, ackMessageOperation, reason, (short)2);
               });
               return;
            case 2:
               // third retry, on paging
               if (reason != AckReason.EXPIRED) {
                  // if expired, we don't need to check on paging
                  // as the message will expire again when depaged (if on paging)
                  performAckOnPage(nodeID, messageID, targetQueue, ackMessageOperation);
                  return;
               } else {
                  ackMessageOperation.run();
               }
         }
      }

      if (reference != null) {
         if (logger.isTraceEnabled()) {
            logger.trace("Post ack Server " + server + " worked well for messageID=" + messageID + " nodeID=" + nodeID);
         }
         try {
            switch (reason) {
               case EXPIRED:
                  targetQueue.expire(reference, null, false);
                  break;
               default:
                  targetQueue.acknowledge(null, reference, reason, null, false);
                  break;
            }
            OperationContextImpl.getContext().executeOnCompletion(ackMessageOperation);
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
         }
      }
   }

   /**
    * this method returning true means the sendMessage was successful, and the IOContext should no longer be used.
    * as the sendMessage was successful the OperationContext of the transaction will take care of the completion.
    * The caller of this method should give up any reference to messageCompletionAck when this method returns true.
    * */
   private boolean sendMessage(AMQPMessage message, ACKMessageOperation messageCompletionAck) throws Exception {

      if (message.getMessageID() <= 0) {
         message.setMessageID(server.getStorageManager().generateID());
      }

      String internalMirrorID = (String)AMQPMessageBrokerAccessor.getDeliveryAnnotationProperty(message, BROKER_ID);
      if (internalMirrorID == null) {
         internalMirrorID = getRemoteMirrorId(); // not pasisng the ID means the data was generated on the remote broker
      }
      Long internalIDLong = (Long) AMQPMessageBrokerAccessor.getDeliveryAnnotationProperty(message, INTERNAL_ID);
      String internalAddress = (String) AMQPMessageBrokerAccessor.getDeliveryAnnotationProperty(message, INTERNAL_DESTINATION);

      long internalID = 0;

      if (internalIDLong != null) {
         internalID = internalIDLong;
      }

      if (logger.isTraceEnabled()) {
         logger.trace("sendMessage on server " + server + " for message " + message + " with internalID = " + internalIDLong + " mirror id " + internalMirrorID);
      }

      routingContext.setDuplicateDetection(false); // we do our own duplicate detection here

      DuplicateIDCache duplicateIDCache;
      if (lruDuplicateIDKey != null && lruDuplicateIDKey.equals(internalMirrorID)) {
         duplicateIDCache = lruduplicateIDCache;
      } else {
         // we use the number of credits for the duplicate detection, as that means the maximum number of elements you can have pending
         logger.trace("Setting up duplicate detection cache on {}, ServerID={} with {} elements, being the number of credits", ProtonProtocolManager.MIRROR_ADDRESS, internalMirrorID, connection.getAmqpCredits());

         lruDuplicateIDKey = internalMirrorID;
         lruduplicateIDCache = server.getPostOffice().getDuplicateIDCache(SimpleString.toSimpleString(ProtonProtocolManager.MIRROR_ADDRESS + "_" + internalMirrorID), connection.getAmqpCredits());
         duplicateIDCache = lruduplicateIDCache;
      }

      byte[] duplicateIDBytes = ByteUtil.longToBytes(internalIDLong);

      if (duplicateIDCache.contains(duplicateIDBytes)) {
         flow();
         return false;
      }

      message.setBrokerProperty(INTERNAL_ID_EXTRA_PROPERTY, internalID);
      message.setBrokerProperty(INTERNAL_BROKER_ID_EXTRA_PROPERTY, internalMirrorID);

      if (internalAddress != null) {
         message.setAddress(internalAddress);
      }

      final TransactionImpl transaction = new MirrorTransaction(server.getStorageManager());
      transaction.addOperation(messageCompletionAck.tx);
      routingContext.setTransaction(transaction);
      duplicateIDCache.addToCache(duplicateIDBytes, transaction);

      routingContext.clear().setMirrorSource(this).setLoadBalancingType(MessageLoadBalancingType.OFF);
      server.getPostOffice().route(message, routingContext, false);
      // We use this as part of a transaction because of the duplicate detection cache that needs to be done atomically
      transaction.commit();
      flow();

      // return true here will instruct the caller to ignore any references to messageCompletionAck
      return true;
   }

   @Override
   public void postAcknowledge(MessageReference ref, AckReason reason) {
      // Do nothing
   }

   @Override
   public void sendMessage(Message message, RoutingContext context, List<MessageReference> refs) {
      // Do nothing
   }

   class PageAck implements ToIntFunction<PagedReference>, BooleanSupplier, Runnable {

      final Queue targetQueue;
      final String nodeID;
      final long messageID;
      final IOCallback operation;

      PageAck(Queue targetQueue, String nodeID, long messageID, IOCallback operation) {
         this.targetQueue = targetQueue;
         this.nodeID = nodeID;
         this.messageID = messageID;
         this.operation = operation;
      }

      /**
       * Method to retry the ack before a scan
       */
      @Override
      public boolean getAsBoolean() {
         try {
            recoverContext();
            MessageReference reference = targetQueue.removeWithSuppliedID(nodeID, messageID, referenceNodeStore);
            if (reference == null) {
               return false;
            } else {
               targetQueue.acknowledge(null, reference, AckReason.NORMAL, null, false);
               OperationContextImpl.getContext().executeOnCompletion(operation);
               return true;
            }
         } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
            return false;
         }
      }

      @Override
      public int applyAsInt(PagedReference reference) {
         String refNodeID = referenceNodeStore.getServerID(reference);
         long refMessageID = referenceNodeStore.getID(reference);
         if (refNodeID == null) {
            refNodeID = referenceNodeStore.getDefaultNodeID();
         }

         if (refNodeID.equals(nodeID)) {
            long diff = refMessageID - messageID;
            if (diff == 0) {
               return 0;
            } else if (diff > 0) {
               return 1;
            } else {
               return -1;
            }
         } else {
            return -1;
         }
      }

      @Override
      public void run() {
         operation.done();
      }

   }

}
