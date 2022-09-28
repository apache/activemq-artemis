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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
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

import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerTarget.getControllerInUse;

public class AMQPMirrorControllerSource extends BasicMirrorController<Sender> implements MirrorController, ActiveMQComponent {

   private static final Logger logger = LoggerFactory.getLogger(AMQPMirrorControllerSource.class);

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
   }

   public Queue getSnfQueue() {
      return snfQueue;
   }

   public AMQPBrokerConnection getBrokerConnection() {
      return brokerConnection;
   }

   @Override
   public void addAddress(AddressInfo addressInfo) throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace(server + " addAddress " + addressInfo);
      }

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
      if (logger.isTraceEnabled()) {
         logger.trace(server + " deleteAddress " + addressInfo);
      }
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
      if (logger.isTraceEnabled()) {
         logger.trace(server + " createQueue " + queueConfiguration);
      }
      if (invalidTarget(getControllerInUse()) || queueConfiguration.isInternal()) {
         if (logger.isTraceEnabled()) {
            logger.trace("Rejecting ping pong on create " + queueConfiguration + " as isInternal=" + queueConfiguration.isInternal() + " and mirror target = " + getControllerInUse());
         }
         return;
      }
      if (ignoreAddress(queueConfiguration.getAddress())) {
         if (logger.isTraceEnabled()) {
            logger.trace("Skipping create " + queueConfiguration + ", queue address " + queueConfiguration.getAddress() + " doesn't match filter");
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
         logger.trace(server + " deleteQueue " + address + "/" + queue);
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
   public void sendMessage(Message message, RoutingContext context, List<MessageReference> refs) {
      SimpleString address = context.getAddress(message);

      if (invalidTarget(context.getMirrorSource())) {
         if (logger.isTraceEnabled()) {
            logger.trace("server " + server + " is discarding send to avoid infinite loop (reflection with the mirror)");
         }
         return;
      }

      if (context.isInternal()) {
         if (logger.isTraceEnabled()) {
            logger.trace("server " + server + " is discarding send to avoid sending to internal queue");
         }
         return;
      }

      if (ignoreAddress(address)) {
         if (logger.isTraceEnabled()) {
            logger.trace("server " + server + " is discarding send to address " + address + ", address doesn't match filter");
         }
         return;
      }

      if (logger.isTraceEnabled()) {
         logger.trace(server + " send message " + message);
      }

      try {
         context.setReusable(false);

         MessageReference ref = MessageReference.Factory.createReference(message, snfQueue);
         String nodeID = setProtocolData(idSupplier, ref);
         if (nodeID != null && nodeID.equals(getRemoteMirrorId())) {
            if (logger.isTraceEnabled()) {
               logger.trace("Message " + message + "already belonged to the node, " + getRemoteMirrorId() + ", it won't circle send");
            }
            return;
         }
         snfQueue.refUp(ref);
         refs.add(ref);

         if (message.isDurable() && snfQueue.isDurable()) {
            PostOfficeImpl.storeDurableReference(server.getStorageManager(), message, context.getTransaction(), snfQueue, true);
         }

      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      }
   }

   public static void validateProtocolData(ReferenceNodeStore referenceIDSupplier, MessageReference ref, SimpleString snfAddress) {
      if (ref.getProtocolData() == null && !ref.getMessage().getAddressSimpleString().equals(snfAddress)) {
         setProtocolData(referenceIDSupplier, ref);
      }
   }

   /** This method will return the brokerID used by the message */
   private static String setProtocolData(ReferenceNodeStore referenceIDSupplier, MessageReference ref) {
      Map<Symbol, Object> daMap = new HashMap<>();
      DeliveryAnnotations deliveryAnnotations = new DeliveryAnnotations(daMap);

      String brokerID = referenceIDSupplier.getServerID(ref);

      // getListID will return null when the message was generated on this broker.
      // on this case we do not send the brokerID, and the ControllerTarget will get the information from the link.
      // this is just to safe a few bytes and some processing on the wire.
      if (brokerID != null) {
         // not sending the brokerID, will make the other side to get the brokerID from the remote link's property
         daMap.put(BROKER_ID, brokerID);
      }

      long id = referenceIDSupplier.getID(ref);

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
      ref.setProtocolData(deliveryAnnotations);

      return brokerID;
   }

   private static Properties getProperties(Message message) {
      if (message instanceof AMQPMessage) {
         return AMQPMessageBrokerAccessor.getCurrentProperties((AMQPMessage)message);
      } else {
         return null;
      }
   }

   @Override
   public void postAcknowledge(MessageReference ref, AckReason reason) throws Exception {

      MirrorController controllerInUse = getControllerInUse();

      if (!acks || ref.getQueue().isMirrorController()) { // we don't call postACK on snfqueues, otherwise we would get infinite loop because of this feedback/
         return;
      }

      if (invalidTarget(controllerInUse)) {
         return;
      }

      if ((ref.getQueue() != null && (ref.getQueue().isInternalQueue() || ref.getQueue().isMirrorController()))) {
         if (logger.isDebugEnabled()) {
            logger.debug(server + " rejecting postAcknowledge queue=" + ref.getQueue().getName() + ", ref=" + ref + " to avoid infinite loop with the mirror (reflection)");
         }
         return;
      }

      if (ignoreAddress(ref.getQueue().getAddress())) {
         if (logger.isTraceEnabled()) {
            logger.trace(server + " rejecting postAcknowledge queue=" + ref.getQueue().getName() + ", ref=" + ref + ", queue address is excluded");
         }
         return;
      }

      if (logger.isTraceEnabled()) {
         logger.trace(server + " postAcknowledge " + ref);
      }

      String nodeID = idSupplier.getServerID(ref); // notice the brokerID will be null for any message generated on this broker.
      long internalID = idSupplier.getID(ref);
      if (logger.isTraceEnabled()) {
         logger.trace(server + " sending ack message from server " + nodeID + " with messageID=" + internalID);
      }
      Message message = createMessage(ref.getQueue().getAddress(), ref.getQueue().getName(), POST_ACK, nodeID, internalID, reason);
      route(server, message);
      ref.getMessage().usageDown();
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
