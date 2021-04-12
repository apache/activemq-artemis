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
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessageBrokerAccessor;
import org.apache.activemq.artemis.protocol.amqp.connect.AMQPBrokerConnection;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.jboss.logging.Logger;

public class AMQPMirrorControllerSource implements MirrorController, ActiveMQComponent {

   private static final Logger logger = Logger.getLogger(AMQPMirrorControllerSource.class);

   public static final Symbol EVENT_TYPE = Symbol.getSymbol("x-opt-amq-mr-ev-type");
   public static final Symbol ADDRESS = Symbol.getSymbol("x-opt-amq-mr-adr");
   public static final Symbol QUEUE = Symbol.getSymbol("x-opt-amq-mr-qu");

   // Events:
   public static final Symbol ADD_ADDRESS = Symbol.getSymbol("addAddress");
   public static final Symbol DELETE_ADDRESS = Symbol.getSymbol("deleteAddress");
   public static final Symbol CREATE_QUEUE = Symbol.getSymbol("createQueue");
   public static final Symbol DELETE_QUEUE = Symbol.getSymbol("deleteQueue");
   public static final Symbol ADDRESS_SCAN_START = Symbol.getSymbol("addressCanStart");
   public static final Symbol ADDRESS_SCAN_END = Symbol.getSymbol("addressScanEnd");
   public static final Symbol POST_ACK = Symbol.getSymbol("postAck");

   // Delivery annotation property used on mirror control routing and Ack
   public static final Symbol INTERNAL_ID = Symbol.getSymbol("x-opt-amq-mr-id");
   public static final Symbol INTERNAL_DESTINATION = Symbol.getSymbol("x-opt-amq-mr-dst");

   private static final ThreadLocal<MirrorControlRouting> mirrorControlRouting = ThreadLocal.withInitial(() -> new MirrorControlRouting(null));

   final Queue snfQueue;
   final ActiveMQServer server;
   final boolean acks;
   final boolean addQueues;
   final boolean deleteQueues;
   private final AMQPBrokerConnection brokerConnection;

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

   public AMQPMirrorControllerSource(Queue snfQueue, ActiveMQServer server, boolean acks, boolean addQueues, boolean deleteQueues, AMQPBrokerConnection brokerConnection) {
      this.snfQueue = snfQueue;
      this.server = server;
      this.acks = acks;
      this.addQueues = addQueues;
      this.deleteQueues = deleteQueues;
      this.brokerConnection = brokerConnection;
   }

   public Queue getSnfQueue() {
      return snfQueue;
   }

   public AMQPBrokerConnection getBrokerConnection() {
      return brokerConnection;
   }

   @Override
   public void startAddressScan() throws Exception {
      Message message = createMessage(null, null, ADDRESS_SCAN_START, null);
      route(server, message);
   }

   @Override
   public void endAddressScan() throws Exception {
      Message message = createMessage(null, null, ADDRESS_SCAN_END, null);
      route(server, message);
   }

   @Override
   public void addAddress(AddressInfo addressInfo) throws Exception {
      if (addQueues) {
         Message message = createMessage(addressInfo.getName(), null, ADD_ADDRESS, addressInfo.toJSON());
         route(server, message);
      }
   }

   @Override
   public void deleteAddress(AddressInfo addressInfo) throws Exception {
      if (deleteQueues) {
         Message message = createMessage(addressInfo.getName(), null, DELETE_ADDRESS, addressInfo.toJSON());
         route(server, message);
      }
   }

   @Override
   public void createQueue(QueueConfiguration queueConfiguration) throws Exception {
      if (addQueues) {
         Message message = createMessage(queueConfiguration.getAddress(), queueConfiguration.getName(), CREATE_QUEUE, queueConfiguration.toJSON());
         route(server, message);
      }
   }

   @Override
   public void deleteQueue(SimpleString address, SimpleString queue) throws Exception {
      if (deleteQueues) {
         Message message = createMessage(address, queue, DELETE_QUEUE, queue.toString());
         route(server, message);
      }
   }

   @Override
   public void sendMessage(Message message, RoutingContext context, List<MessageReference> refs) {

      try {
         context.setReusable(false);

         MessageReference ref = MessageReference.Factory.createReference(message, snfQueue);
         snfQueue.refUp(ref);
         refs.add(ref);
         message.usageUp();

         setProtocolData(ref);

         if (message.isDurable() && snfQueue.isDurable()) {
            PostOfficeImpl.storeDurableReference(server.getStorageManager(), message, context.getTransaction(), snfQueue, true);
         }

      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      }
   }

   public static void validateProtocolData(MessageReference ref, SimpleString snfAddress) {
      if (ref.getProtocolData() == null && !ref.getMessage().getAddressSimpleString().equals(snfAddress)) {
         setProtocolData(ref);
      }
   }

   private static void setProtocolData(MessageReference ref) {
      Map<Symbol, Object> daMap = new HashMap<>();
      DeliveryAnnotations deliveryAnnotations = new DeliveryAnnotations(daMap);
      daMap.put(INTERNAL_ID, ref.getMessage().getMessageID());
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
      if (acks && !ref.getQueue().isMirrorController()) { // we don't call postACK on snfqueues, otherwise we would get infinite loop because of this feedback
         Message message = createMessage(ref.getQueue().getAddress(), ref.getQueue().getName(), POST_ACK, ref.getMessage().getMessageID());
         route(server, message);
         ref.getMessage().usageDown();
      }
   }

   private Message createMessage(SimpleString address, SimpleString queue, Object event, Object body) {
      return AMQPMirrorMessageFactory.createMessage(snfQueue.getAddress().toString(), address, queue, event, body);
   }
   public static void route(ActiveMQServer server, Message message) throws Exception {
      message.setMessageID(server.getStorageManager().generateID());
      MirrorControlRouting ctx = mirrorControlRouting.get();
      ctx.clear();
      server.getPostOffice().route(message, ctx, false);
   }

   private static class MirrorControlRouting extends RoutingContextImpl {

      MirrorControlRouting(Transaction transaction) {
         super(transaction);
      }

      @Override
      public boolean isMirrorController() {
         return true;
      }
   }
}
