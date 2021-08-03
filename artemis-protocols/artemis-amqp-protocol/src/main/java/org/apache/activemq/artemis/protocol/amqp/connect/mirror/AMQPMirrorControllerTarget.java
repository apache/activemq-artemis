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

import org.apache.activemq.artemis.api.core.ActiveMQAddressDoesNotExistException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
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
import org.apache.activemq.artemis.utils.collections.NodeStore;
import org.apache.activemq.artemis.utils.pools.MpscPool;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.jboss.logging.Logger;

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

   private static final Logger logger = Logger.getLogger(AMQPMirrorControllerTarget.class);

   private static ThreadLocal<MirrorController> controllerThreadLocal = new ThreadLocal<>();

   public static void setControllerInUse(MirrorController controller) {
      controllerThreadLocal.set(controller);
   }

   public static MirrorController getControllerInUse() {
      return controllerThreadLocal.get();
   }

   /** Objects of this class can be used by either transaction or by OperationContext.
    *  It is important that when you're using the transactions you clear any references to
    *  the operation context. Don't use transaction and OperationContext at the same time
    *  as that would generate duplicates on the objects cache.
    */
   class ACKMessageOperation implements IOCallback, Runnable {

      Delivery delivery;

      /** notice that when you use the Transaction, you need to make sure you don't use the IO*/
      public TransactionOperationAbstract tx = new TransactionOperationAbstract() {
         @Override
         public void afterCommit(Transaction tx) {
            completeOperation();
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
         completeOperation();
      }

      private void completeOperation() {
         connection.runNow(ACKMessageOperation.this);
      }

      @Override
      public void onError(int errorCode, String errorMessage) {
         logger.warn(errorMessage + "-"  + errorMessage);
      }
   }

   // in a regular case we should not have more than amqpCredits on the pool, that's the max we would need
   private final MpscPool<ACKMessageOperation> ackMessageMpscPool = new MpscPool<>(amqpCredits, ACKMessageOperation::reset, () -> new ACKMessageOperation());

   final RoutingContextImpl routingContext = new RoutingContextImpl(null);

   final BasicMirrorController<Receiver> basicController;

   final ActiveMQServer server;

   DuplicateIDCache lruduplicateIDCache;
   String lruDuplicateIDKey;

   private final NodeStore<MessageReference> referenceNodeStore;

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
               if (logger.isDebugEnabled()) {
                  logger.debug(server + " Adding Address " + addressInfo);
               }
               addAddress(addressInfo);
            } else if (eventType.equals(DELETE_ADDRESS)) {
               AddressInfo addressInfo = parseAddress(message);
               if (logger.isDebugEnabled()) {
                  logger.debug(server + " Removing Address " + addressInfo);
               }
               deleteAddress(addressInfo);
            } else if (eventType.equals(CREATE_QUEUE)) {
               QueueConfiguration queueConfiguration = parseQueue(message);
               if (logger.isDebugEnabled()) {
                  logger.debug(server + " Creating queue " + queueConfiguration);
               }
               createQueue(queueConfiguration);
            } else if (eventType.equals(DELETE_QUEUE)) {

               String address = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, ADDRESS);
               String queueName = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, QUEUE);
               if (logger.isDebugEnabled()) {
                  logger.debug(server + " Deleting queue " + queueName + " on address " + address);
               }
               deleteQueue(SimpleString.toSimpleString(address), SimpleString.toSimpleString(queueName));
            } else if (eventType.equals(POST_ACK)) {
               String address = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, ADDRESS);
               String nodeID = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, BROKER_ID);
               if (nodeID == null) {
                  nodeID = getRemoteMirrorId(); // not sending the nodeID means it's data generated on that broker
               }
               String queueName = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, QUEUE);
               AmqpValue value = (AmqpValue) message.getBody();
               Long messageID = (Long) value.getValue();
               if (logger.isDebugEnabled()) {
                  logger.debug(server + " Post ack address=" + address + " queueName = " + queueName + " messageID=" + messageID + ", nodeID=" + nodeID);
               }
               if (postAcknowledge(address, queueName, nodeID, messageID, messageAckOperation)) {
                  messageAckOperation = null;
               }
            }
         } else {
            if (logger.isDebugEnabled()) {
               logger.debug(server + " Sending message " + message);
            }
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
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();

      // Match the settlement mode of the remote instead of relying on the default of MIXED.
      receiver.setSenderSettleMode(receiver.getRemoteSenderSettleMode());

      // We don't currently support SECOND so enforce that the answer is anlways FIRST
      receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
      flow();
   }

   private QueueConfiguration parseQueue(AMQPMessage message) throws Exception {
      AmqpValue bodyvalue = (AmqpValue) message.getBody();
      String body = (String) bodyvalue.getValue();
      QueueConfiguration queueConfiguration = QueueConfiguration.fromJSON(body);
      return queueConfiguration;
   }

   private AddressInfo parseAddress(AMQPMessage message) throws Exception {
      AmqpValue bodyvalue = (AmqpValue) message.getBody();
      String body = (String) bodyvalue.getValue();
      AddressInfo addressInfo = AddressInfo.fromJSON(body);
      return addressInfo;
   }

   @Override
   public void addAddress(AddressInfo addressInfo) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug(server + " Adding address " + addressInfo);
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
         logger.debug(server + " Adding queue " + queueConfiguration);
      }
      try {
         server.createQueue(queueConfiguration, true);
      } catch (Exception ignored) {
         logger.debug("Queue could not be created, already existed " + queueConfiguration, ignored);
      }
   }

   @Override
   public void deleteQueue(SimpleString addressName, SimpleString queueName) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug(server + " destroy queue " + queueName + " on address = " + addressName + " server " + server.getIdentity());
      }
      try {
         server.destroyQueue(queueName,null, false, true, false, false);
      } catch (ActiveMQNonExistentQueueException expected) {
         logger.debug(server + " queue " + queueName + " was previously removed", expected);
      }
   }

   public boolean postAcknowledge(String address, String queue, String nodeID, long messageID, ACKMessageOperation ackMessage) throws Exception {
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

      performAck(nodeID, messageID, targetQueue, ackMessage, true);
      return true;

   }

   private void performAck(String nodeID, long messageID, Queue targetQueue, ACKMessageOperation ackMessageOperation, boolean retry) {
      if (logger.isTraceEnabled()) {
         logger.trace("performAck (nodeID=" + nodeID + ", messageID=" + messageID + ")" + ", targetQueue=" + targetQueue.getName());
      }
      MessageReference reference = targetQueue.removeWithSuppliedID(nodeID, messageID, referenceNodeStore);
      if (reference == null && retry) {
         if (logger.isDebugEnabled()) {
            logger.debug("Retrying Reference not found on messageID=" + messageID + " nodeID=" + nodeID);
         }
         targetQueue.flushOnIntermediate(() -> {
            recoverContext();
            performAck(nodeID, messageID, targetQueue, ackMessageOperation, false);
         });
         return;
      }
      if (reference != null) {
         if (logger.isTraceEnabled()) {
            logger.trace("Post ack Server " + server + " worked well for messageID=" + messageID + " nodeID=" + nodeID);
         }
         try {
            targetQueue.acknowledge(reference);
            OperationContextImpl.getContext().executeOnCompletion(ackMessageOperation);
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
         }
      } else {
         if (logger.isDebugEnabled()) {
            logger.debug("Post ack Server " + server + " could not find messageID = " + messageID +
                            " representing nodeID=" + nodeID);
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
         logger.trace("sendMessage on server " + server + " for message " + message +
                         " with internalID = " + internalIDLong + " mirror id " + internalMirrorID);
      }


      routingContext.setDuplicateDetection(false); // we do our own duplicate detection here

      DuplicateIDCache duplicateIDCache;
      if (lruDuplicateIDKey != null && lruDuplicateIDKey.equals(internalMirrorID)) {
         duplicateIDCache = lruduplicateIDCache;
      } else {
         // we use the number of credits for the duplicate detection, as that means the maximum number of elements you can have pending
         if (logger.isDebugEnabled()) {
            logger.trace("Setting up duplicate detection cache on " + ProtonProtocolManager.MIRROR_ADDRESS + ", ServerID=" + internalMirrorID + " with " + connection.getAmqpCredits() + " elements, being the number of credits");
         }

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

      routingContext.clear().setMirrorSource(this);
      server.getPostOffice().route(message, routingContext, false);
      // We use this as part of a transaction because of the duplicate detection cache that needs to be done atomically
      transaction.commit();
      flow();

      // return true here will instruct the caller to ignore any references to messageCompletionAck
      return true;
   }

   /**
    * @param ref
    * @param reason
    */
   @Override
   public void postAcknowledge(MessageReference ref, AckReason reason) {
   }

   @Override
   public void sendMessage(Message message, RoutingContext context, List<MessageReference> refs) {
   }

}
