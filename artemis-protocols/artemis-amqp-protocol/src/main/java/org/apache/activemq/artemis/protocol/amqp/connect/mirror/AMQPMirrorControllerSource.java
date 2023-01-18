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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
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
import org.apache.activemq.artemis.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessageBrokerAccessor;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.connect.AMQPBrokerConnection;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.engine.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerTarget.getControllerInUse;

public class AMQPMirrorControllerSource extends BasicMirrorController<Sender> implements MirrorController, ActiveMQComponent {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final Symbol EVENT_TYPE = Symbol.getSymbol("x-opt-amq-mr-ev-type");
   public static final Symbol ACK_REASON = Symbol.getSymbol("x-opt-amq-mr-ack-reason");
   public static final Symbol ADDRESS = Symbol.getSymbol("x-opt-amq-mr-adr");
   public static final Symbol QUEUE = Symbol.getSymbol("x-opt-amq-mr-qu");
   public static final Symbol BROKER_ID = Symbol.getSymbol("x-opt-amq-bkr-id");

   // Events:
   public static final Symbol ADD_ADDRESS = Symbol.getSymbol("addAddress");
   public static final Symbol DELETE_ADDRESS = Symbol.getSymbol("deleteAddress");
   public static final Symbol CREATE_QUEUE = Symbol.getSymbol("createQueue");
   public static final Symbol DELETE_QUEUE = Symbol.getSymbol("deleteQueue");
   public static final Symbol POST_ACK = Symbol.getSymbol("postAck");

   // Delivery annotation property used on mirror control routing and Ack
   public static final Symbol INTERNAL_ID = Symbol.getSymbol("x-opt-amq-mr-id");
   public static final Symbol INTERNAL_DESTINATION = Symbol.getSymbol("x-opt-amq-mr-dst");

   // Capabilities
   public static final Symbol MIRROR_CAPABILITY = Symbol.getSymbol("amq.mirror");
   public static final Symbol QPID_DISPATCH_WAYPOINT_CAPABILITY = Symbol.valueOf("qd.waypoint");

   public static final SimpleString INTERNAL_ID_EXTRA_PROPERTY = SimpleString.toSimpleString(INTERNAL_ID.toString());
   public static final SimpleString INTERNAL_BROKER_ID_EXTRA_PROPERTY = SimpleString.toSimpleString(BROKER_ID.toString());

   private static final ThreadLocal<RoutingContext> mirrorControlRouting = ThreadLocal.withInitial(() -> new RoutingContextImpl(null).setMirrorDisabled(true));

   final Queue snfQueue;
   final ActiveMQServer server;
   final ReferenceNodeStore idSupplier;
   final boolean acks;
   final boolean addQueues;
   final boolean deleteQueues;
   final MirrorAddressFilter addressFilter;
   private final AMQPBrokerConnection brokerConnection;
   private final boolean sync;

   final AMQPMirrorBrokerConnectionElement replicaConfig;

   boolean started;

   @Override
   public void start() throws Exception {
   }

   @Override
   public void stop() throws Exception {
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   public AMQPMirrorControllerSource(ProtonProtocolManager protonProtocolManager, Queue snfQueue, ActiveMQServer server, AMQPMirrorBrokerConnectionElement replicaConfig,
                                     AMQPBrokerConnection brokerConnection) {
      super(server);
      this.replicaConfig = replicaConfig;
      this.snfQueue = snfQueue;
      this.server = server;
      this.idSupplier = protonProtocolManager.getReferenceIDSupplier();
      this.addQueues = replicaConfig.isQueueCreation();
      this.deleteQueues = replicaConfig.isQueueRemoval();
      this.addressFilter = new MirrorAddressFilter(replicaConfig.getAddressFilter());
      this.acks = replicaConfig.isMessageAcknowledgements();
      this.brokerConnection = brokerConnection;
      this.sync = replicaConfig.isSync();
   }

   public Queue getSnfQueue() {
      return snfQueue;
   }

   public AMQPBrokerConnection getBrokerConnection() {
      return brokerConnection;
   }

   @Override
   public void addAddress(AddressInfo addressInfo) throws Exception {
      logger.trace("{} addAddress {}", server, addressInfo);

      if (getControllerInUse() != null && !addressInfo.isInternal()) {
         return;
      }

      if (ignoreAddress(addressInfo.getName())) {
         return;
      }

      if (addQueues) {
         Message message = createMessage(addressInfo.getName(), null, ADD_ADDRESS, null, addressInfo.toJSON());
         route(server, message);
      }
   }

   @Override
   public void deleteAddress(AddressInfo addressInfo) throws Exception {
      logger.trace("{} deleteAddress {}", server, addressInfo);

      if (invalidTarget(getControllerInUse()) || addressInfo.isInternal()) {
         return;
      }
      if (ignoreAddress(addressInfo.getName())) {
         return;
      }
      if (deleteQueues) {
         Message message = createMessage(addressInfo.getName(), null, DELETE_ADDRESS, null, addressInfo.toJSON());
         route(server, message);
      }
   }

   @Override
   public void createQueue(QueueConfiguration queueConfiguration) throws Exception {
      logger.trace("{} createQueue {}", server, queueConfiguration);

      if (invalidTarget(getControllerInUse()) || queueConfiguration.isInternal()) {
         if (logger.isTraceEnabled()) {
            logger.trace("Rejecting ping pong on create {} as isInternal={} and mirror target = {}", queueConfiguration, queueConfiguration.isInternal(), getControllerInUse());
         }

         return;
      }
      if (ignoreAddress(queueConfiguration.getAddress())) {
         if (logger.isTraceEnabled()) {
            logger.trace("Skipping create {}, queue address {} doesn't match filter", queueConfiguration, queueConfiguration.getAddress());
         }
         return;
      }
      if (addQueues) {
         Message message = createMessage(queueConfiguration.getAddress(), queueConfiguration.getName(), CREATE_QUEUE, null, queueConfiguration.toJSON());
         route(server, message);
      }
   }

   @Override
   public void deleteQueue(SimpleString address, SimpleString queue) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("{} deleteQueue {}/{}", server, address, queue);
      }

      if (invalidTarget(getControllerInUse())) {
         return;
      }

      if (ignoreAddress(address)) {
         return;
      }

      if (deleteQueues) {
         Message message = createMessage(address, queue, DELETE_QUEUE, null, queue.toString());
         route(server, message);
      }
   }

   private boolean invalidTarget(MirrorController controller) {
      return controller != null && sameNode(getRemoteMirrorId(), controller.getRemoteMirrorId());
   }

   private boolean ignoreAddress(SimpleString address) {
      return !addressFilter.match(address);
   }

   private boolean sameNode(String remoteID, String sourceID) {
      return (remoteID != null && sourceID != null && remoteID.equals(sourceID));
   }

   @Override
   public void sendMessage(Transaction tx, Message message, RoutingContext context) {
      SimpleString address = context.getAddress(message);

      if (invalidTarget(context.getMirrorSource())) {
         logger.trace("sendMessage::server {} is discarding send to avoid infinite loop (reflection with the mirror)", server);
         return;
      }

      if (context.isInternal()) {
         logger.trace("sendMessage::server {} is discarding send to avoid sending to internal queue", server);
         return;
      }

      if (ignoreAddress(address)) {
         logger.trace("sendMessage::server {} is discarding send to address {}, address doesn't match filter", server, address);
         return;
      }

      logger.trace("sendMessage::{} send message {}", server, message);

      try {
         context.setReusable(false);

         String nodeID = idSupplier.getServerID(message);

         if (nodeID != null && nodeID.equals(getRemoteMirrorId())) {
            logger.trace("sendMessage::Message {} already belonged to the node, {}, it won't circle send", message, getRemoteMirrorId());
            return;
         }

         MessageReference ref = MessageReference.Factory.createReference(message, snfQueue);
         setProtocolData(ref, nodeID, idSupplier.getID(ref));

         snfQueue.refUp(ref);

         if (tx != null) {
            logger.debug("sendMessage::Mirroring Message {} with TX", message);
            getSendOperation(tx).addRef(ref);
         } // if non transactional the afterStoreOperations will use the ref directly and call processReferences

         if (sync) {
            OperationContext operContext = OperationContextImpl.getContext(server.getExecutorFactory());
            if (tx == null) {
               // notice that if transactional, the context is lined up on beforeCommit as part of the transaction operation
               operContext.replicationLineUp();
            }
            if (logger.isDebugEnabled()) {
               logger.debug("sendMessage::mirror syncUp context={}, ref={}", operContext, ref);
            }
            ref.setProtocolData(OperationContext.class, operContext);
         }

         if (message.isDurable() && snfQueue.isDurable()) {
            PostOfficeImpl.storeDurableReference(server.getStorageManager(), message, context.getTransaction(), snfQueue, true);
         }

         if (tx == null) {
            server.getStorageManager().afterStoreOperations(new IOCallback() {
               @Override
               public void done() {
                  PostOfficeImpl.processReference(ref, false);
               }

               @Override
               public void onError(int errorCode, String errorMessage) {
               }
            });
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      }
   }

   private void syncDone(MessageReference reference) {
      OperationContext ctx = reference.getProtocolData(OperationContext.class);
      if (ctx != null) {
         ctx.replicationDone();
         logger.debug("syncDone::replicationDone::ctx={},ref={}", ctx, reference);
      }  else {
         Message message = reference.getMessage();
         if (message != null) {
            ctx = (OperationContext) message.getUserContext(OperationContext.class);
            if (ctx != null) {
               ctx.replicationDone();
               logger.debug("syncDone::replicationDone message={}", message);
            } else {
               logger.trace("syncDone::No operationContext set on message {}", message);
            }
         } else {
            logger.debug("syncDone::no message set on reference {}", reference);
         }
      }
   }

   public static void validateProtocolData(ReferenceNodeStore referenceIDSupplier, MessageReference ref, SimpleString snfAddress) {
      if (ref.getProtocolData(DeliveryAnnotations.class) == null && !ref.getMessage().getAddressSimpleString().equals(snfAddress)) {
         setProtocolData(referenceIDSupplier, ref);
      }
   }

   /** This method will return the brokerID used by the message */
   private static String setProtocolData(ReferenceNodeStore referenceIDSupplier, MessageReference ref) {
      String brokerID = referenceIDSupplier.getServerID(ref);
      long id = referenceIDSupplier.getID(ref);

      setProtocolData(ref, brokerID, id);

      return brokerID;
   }

   private static void setProtocolData(MessageReference ref, String brokerID, long id) {
      Map<Symbol, Object> daMap = new HashMap<>();
      DeliveryAnnotations deliveryAnnotations = new DeliveryAnnotations(daMap);

      // getListID will return null when the message was generated on this broker.
      // on this case we do not send the brokerID, and the ControllerTarget will get the information from the link.
      // this is just to safe a few bytes and some processing on the wire.
      if (brokerID != null) {
         // not sending the brokerID, will make the other side to get the brokerID from the remote link's property
         daMap.put(BROKER_ID, brokerID);
      }

      daMap.put(INTERNAL_ID, id);
      String address = ref.getMessage().getAddress();
      if (address != null) { // this is the message that was set through routing
         Properties amqpProperties = getProperties(ref.getMessage());
         if (amqpProperties == null || !address.equals(amqpProperties.getTo())) {
            // We set the internal destination property only if we need to
            // otherwise we just use the one already set over Properties
            daMap.put(INTERNAL_DESTINATION, ref.getMessage().getAddress());
         }
      }
      ref.setProtocolData(DeliveryAnnotations.class, deliveryAnnotations);
   }

   private static Properties getProperties(Message message) {
      if (message instanceof AMQPMessage) {
         return AMQPMessageBrokerAccessor.getCurrentProperties((AMQPMessage)message);
      } else {
         return null;
      }
   }

   private void postACKInternalMessage(MessageReference reference) {
      logger.debug("postACKInternalMessage::server={}, ref={}", server, reference);
      if (sync) {
         syncDone(reference);
      }
   }

   @Override
   public void postAcknowledge(MessageReference ref, AckReason reason) throws Exception {
      if (!acks || ref.getQueue().isMirrorController()) {
         postACKInternalMessage(ref);
         return;
      }
   }

   @Override
   public void preAcknowledge(final Transaction tx, final MessageReference ref, final AckReason reason) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("postACKInternalMessage::tx={}, ref={}, reason={}", tx, ref, reason);
      }

      MirrorController controllerInUse = getControllerInUse();

      if (!acks || ref.getQueue().isMirrorController()) { // we don't call preAcknowledge on snfqueues, otherwise we would get infinite loop because of this feedback/
         return;
      }

      if (invalidTarget(controllerInUse)) {
         return;
      }

      if ((ref.getQueue() != null && (ref.getQueue().isInternalQueue() || ref.getQueue().isMirrorController()))) {
         if (logger.isDebugEnabled()) {
            logger.debug("preAcknowledge::{} rejecting preAcknowledge queue={}, ref={} to avoid infinite loop with the mirror (reflection)", server, ref.getQueue().getName(), ref);
         }
         return;
      }

      if (ignoreAddress(ref.getQueue().getAddress())) {
         if (logger.isTraceEnabled()) {
            logger.trace("preAcknowledge::{} rejecting preAcknowledge queue={}, ref={}, queue address is excluded", server, ref.getQueue().getName(), ref);
         }
         return;
      }

      logger.trace("preAcknowledge::{} preAcknowledge {}", server, ref);

      String nodeID = idSupplier.getServerID(ref); // notice the brokerID will be null for any message generated on this broker.
      long internalID = idSupplier.getID(ref);
      Message messageCommand = createMessage(ref.getQueue().getAddress(), ref.getQueue().getName(), POST_ACK, nodeID, internalID, reason);
      if (sync) {
         OperationContext operationContext;
         operationContext = OperationContextImpl.getContext(server.getExecutorFactory());
         messageCommand.setUserContext(OperationContext.class, operationContext);
         if (tx == null) {
            // notice that if transactional, the context is lined up on beforeCommit as part of the transaction operation
            operationContext.replicationLineUp();
         }
      }

      if (tx != null) {
         MirrorACKOperation operation = getAckOperation(tx);
         // notice the operationContext.replicationLineUp is done on beforeCommit as part of the TX
         operation.addMessage(messageCommand, ref);
      } else {
         server.getStorageManager().afterStoreOperations(new IOCallback() {
            @Override
            public void done() {
               try {
                  logger.debug("preAcknowledge::afterStoreOperation for messageReference {}", ref);
                  route(server, messageCommand);
               } catch (Exception e) {
                  logger.warn(e.getMessage(), e);
               }
            }

            @Override
            public void onError(int errorCode, String errorMessage) {
            }
         });
      }
   }

   private MirrorACKOperation getAckOperation(Transaction tx) {
      MirrorACKOperation ackOperation = (MirrorACKOperation) tx.getProperty(TransactionPropertyIndexes.MIRROR_ACK_OPERATION);
      if (ackOperation == null) {
         logger.trace("getAckOperation::setting operation on transaction {}", tx);
         ackOperation = new MirrorACKOperation(server);
         tx.putProperty(TransactionPropertyIndexes.MIRROR_ACK_OPERATION, ackOperation);
         tx.afterStore(ackOperation);
      }

      return ackOperation;
   }

   private MirrorSendOperation getSendOperation(Transaction tx) {
      if (tx == null) {
         return null;
      }
      MirrorSendOperation sendOperation = (MirrorSendOperation) tx.getProperty(TransactionPropertyIndexes.MIRROR_SEND_OPERATION);
      if (sendOperation == null) {
         logger.trace("getSendOperation::setting operation on transaction {}", tx);
         sendOperation = new MirrorSendOperation();
         tx.putProperty(TransactionPropertyIndexes.MIRROR_SEND_OPERATION, sendOperation);
         tx.afterStore(sendOperation);
      }

      return sendOperation;
   }

   private static class MirrorACKOperation extends TransactionOperationAbstract {

      final ActiveMQServer server;

      // This map contains the Message used to generate the command towards the target, the reference being acked
      final HashMap<Message, MessageReference> acks = new HashMap<>();

      MirrorACKOperation(ActiveMQServer server) {
         this.server = server;
      }

      /**
       *
       * @param message the message with the instruction to ack on the target node. Notice this is not the message owned by the reference.
       * @param ref the reference being acked
       */
      public void addMessage(Message message, MessageReference ref) {
         acks.put(message, ref);
      }

      @Override
      public void beforeCommit(Transaction tx) {
         logger.debug("MirrorACKOperation::beforeCommit processing {}", acks);
         acks.forEach(this::doBeforeCommit);
      }

      // callback to be used on forEach
      private void doBeforeCommit(Message ack, MessageReference ref) {
         OperationContext context = (OperationContext) ack.getUserContext(OperationContext.class);
         if (context != null) {
            context.replicationLineUp();
         }
      }

      @Override
      public void afterCommit(Transaction tx) {
         logger.debug("MirrorACKOperation::afterCommit processing {}", acks);
         acks.forEach(this::doAfterCommit);
      }

      // callback to be used on forEach
      private void doAfterCommit(Message ack, MessageReference ref) {
         try {
            route(server, ack);
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
         }
         ref.getMessage().usageDown();
      }

      @Override
      public void afterRollback(Transaction tx) {
         acks.forEach(this::doAfterRollback);
      }

      // callback to be used on forEach
      private void doAfterRollback(Message ack, MessageReference ref) {
         OperationContext context = (OperationContext) ack.getUserContext(OperationContext.class);
         if (context != null) {
            context.replicationDone();
         }
      }

   }

   private static final class MirrorSendOperation extends TransactionOperationAbstract {
      final List<MessageReference> refs = new ArrayList<>();

      public void addRef(MessageReference ref) {
         refs.add(ref);
      }

      @Override
      public void beforeCommit(Transaction tx) {
         refs.forEach(this::doBeforeCommit);
      }

      // callback to be used on forEach
      private void doBeforeCommit(MessageReference ref) {
         OperationContext context = ref.getProtocolData(OperationContext.class);
         if (context != null) {
            context.replicationLineUp();
         }
      }

      @Override
      public void afterRollback(Transaction tx) {
         logger.debug("MirrorSendOperation::afterRollback, refs:{}", refs);
         refs.forEach(this::doBeforeRollback);
      }

      // forEach callback
      private void doBeforeRollback(MessageReference ref) {
         OperationContext localCTX = ref.getProtocolData(OperationContext.class);
         if (localCTX != null) {
            localCTX.replicationDone();
         }
      }

      @Override
      public void afterCommit(Transaction tx) {
         logger.debug("MirrorSendOperation::afterCommit refs:{}", refs);
         refs.forEach(this::doAfterCommit);
      }

      // forEach callback
      private void doAfterCommit(MessageReference ref) {
         PostOfficeImpl.processReference(ref, false);
      }
   }

   private Message createMessage(SimpleString address, SimpleString queue, Object event, String brokerID, Object body) {
      return AMQPMirrorMessageFactory.createMessage(snfQueue.getAddress().toString(), address, queue, event, brokerID, body, null);
   }

   private Message createMessage(SimpleString address, SimpleString queue, Object event, String brokerID, Object body, AckReason ackReason) {
      return AMQPMirrorMessageFactory.createMessage(snfQueue.getAddress().toString(), address, queue, event, brokerID, body, ackReason);
   }

   public static void route(ActiveMQServer server, Message message) throws Exception {
      message.setMessageID(server.getStorageManager().generateID());
      RoutingContext ctx = mirrorControlRouting.get();
      ctx.clear();
      server.getPostOffice().route(message, ctx, false);
   }

}
