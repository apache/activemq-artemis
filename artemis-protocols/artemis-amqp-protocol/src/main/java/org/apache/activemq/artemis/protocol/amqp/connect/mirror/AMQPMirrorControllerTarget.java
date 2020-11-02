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
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import org.apache.activemq.artemis.api.core.ActiveMQAddressDoesNotExistException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
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
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonAbstractReceiver;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.EVENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_DESTINATION;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.POST_ACK;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.ADD_ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.DELETE_ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.CREATE_QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.DELETE_QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_ID;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.ADDRESS_SCAN_START;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.ADDRESS_SCAN_END;

public class AMQPMirrorControllerTarget extends ProtonAbstractReceiver implements MirrorController {

   public static final SimpleString INTERNAL_ID_EXTRA_PROPERTY = SimpleString.toSimpleString(INTERNAL_ID.toString());

   private static final Logger logger = Logger.getLogger(AMQPMirrorControllerTarget.class);

   final ActiveMQServer server;

   final RoutingContextImpl routingContext = new RoutingContextImpl(null);

   Map<SimpleString, Map<SimpleString, QueueConfiguration>> scanAddresses;

   public AMQPMirrorControllerTarget(AMQPSessionCallback sessionSPI,
                                     AMQPConnectionContext connection,
                                     AMQPSessionContext protonSession,
                                     Receiver receiver,
                                     ActiveMQServer server) {
      super(sessionSPI, connection, protonSession, receiver);
      this.server = server;
   }

   @Override
   public void flow() {
      creditRunnable.run();
   }

   @Override
   protected void actualDelivery(AMQPMessage message, Delivery delivery, Receiver receiver, Transaction tx) {
      incrementSettle();


      if (logger.isDebugEnabled()) {
         logger.debug(server.getIdentity() + "::Received " + message);
      }
      try {
         /** We use message annotations, because on the same link we will receive control messages
          *  coming from mirror events,
          *  and the actual messages that need to be replicated.
          *  Using anything from the body would force us to parse the body on regular messages.
          *  The body of the message may still be used on control messages, on cases where a JSON string is sent. */
         Object eventType = AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, EVENT_TYPE);
         if (eventType != null) {
            if (eventType.equals(ADDRESS_SCAN_START)) {
               logger.debug("Starting scan for removed queues");
               startAddressScan();
            } else if (eventType.equals(ADDRESS_SCAN_END)) {
               logger.debug("Ending scan for removed queues");
               endAddressScan();
            } else if (eventType.equals(ADD_ADDRESS)) {
               AddressInfo addressInfo = parseAddress(message);
               if (logger.isDebugEnabled()) {
                  logger.debug("Adding Address " + addressInfo);
               }
               addAddress(addressInfo);
            } else if (eventType.equals(DELETE_ADDRESS)) {
               AddressInfo addressInfo = parseAddress(message);
               if (logger.isDebugEnabled()) {
                  logger.debug("Removing Address " + addressInfo);
               }
               deleteAddress(addressInfo);
            } else if (eventType.equals(CREATE_QUEUE)) {
               QueueConfiguration queueConfiguration = parseQueue(message);
               if (logger.isDebugEnabled()) {
                  logger.debug("Creating queue " + queueConfiguration);
               }
               createQueue(queueConfiguration);
            } else if (eventType.equals(DELETE_QUEUE)) {

               String address = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, ADDRESS);
               String queueName = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, QUEUE);
               if (logger.isDebugEnabled()) {
                  logger.debug("Deleting queue " + queueName + " on address " + address);
               }
               deleteQueue(SimpleString.toSimpleString(address), SimpleString.toSimpleString(queueName));
            } else if (eventType.equals(POST_ACK)) {
               String address = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, ADDRESS);
               String queueName = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, QUEUE);
               AmqpValue value = (AmqpValue) message.getBody();
               Long messageID = (Long) value.getValue();
               if (logger.isDebugEnabled()) {
                  logger.debug("Post ack address=" + address + " queueName = " + queueName + " messageID=" + messageID);
               }
               postAcknowledge(address, queueName, messageID);
            }
         } else {
            if (logger.isDebugEnabled()) {
               logger.debug("Sending message " + message);
            }
            sendMessage(message);
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      } finally {
         delivery.disposition(Accepted.getInstance());
         settle(delivery);
         connection.flush();
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
   public void startAddressScan() throws Exception {
      scanAddresses = new HashMap<>();
   }

   @Override
   public void endAddressScan() throws Exception {
      Map<SimpleString, Map<SimpleString, QueueConfiguration>> scannedAddresses = scanAddresses;
      this.scanAddresses = null;
      Stream<Binding> bindings = server.getPostOffice().getAllBindings();
      bindings.forEach((binding) -> {
         if (binding instanceof LocalQueueBinding) {
            LocalQueueBinding localQueueBinding = (LocalQueueBinding) binding;
            Map<SimpleString, QueueConfiguration> scannedQueues = scannedAddresses.get(localQueueBinding.getQueue().getAddress());

            if (scannedQueues == null) {
               if (logger.isDebugEnabled()) {
                  logger.debug("There's no address " + localQueueBinding.getQueue().getAddress() + " so, removing queue");
               }
               try {
                  deleteQueue(localQueueBinding.getQueue().getAddress(), localQueueBinding.getQueue().getName());
               } catch (Exception e) {
                  logger.warn(e.getMessage(), e);
               }
            } else {
               QueueConfiguration queueConfg = scannedQueues.get(localQueueBinding.getQueue().getName());
               if (queueConfg == null) {
                  if (logger.isDebugEnabled()) {
                     logger.debug("There no queue for " + localQueueBinding.getQueue().getName() + " so, removing queue");
                  }
                  try {
                     deleteQueue(localQueueBinding.getQueue().getAddress(), localQueueBinding.getQueue().getName());
                  } catch (Exception e) {
                     logger.warn(e.getMessage(), e);
                  }
               }
            }
         }
      });
   }

   private Map<SimpleString, QueueConfiguration> getQueueScanMap(SimpleString address) {
      Map<SimpleString, QueueConfiguration> queueMap = scanAddresses.get(address);
      if (queueMap == null) {
         queueMap = new HashMap<>();
         scanAddresses.put(address, queueMap);
      }
      return queueMap;
   }

   @Override
   public void addAddress(AddressInfo addressInfo) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug("Adding address " + addressInfo);
      }
      server.addAddressInfo(addressInfo);
   }

   @Override
   public void deleteAddress(AddressInfo addressInfo) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug("delete address " + addressInfo);
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
         logger.debug("Adding queue " + queueConfiguration);
      }
      server.createQueue(queueConfiguration, true);

      if (scanAddresses != null) {
         getQueueScanMap(queueConfiguration.getAddress()).put(queueConfiguration.getName(), queueConfiguration);
      }
   }

   @Override
   public void deleteQueue(SimpleString addressName, SimpleString queueName) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug("destroy queue " + queueName + " on address = " + addressName);
      }
      try {
         server.destroyQueue(queueName);
      } catch (ActiveMQNonExistentQueueException expected) {
         logger.debug("queue " + queueName + " was previously removed", expected);
      }
   }

   private static ToLongFunction<MessageReference> referenceIDSupplier = (source) -> {
      Long id = (Long) source.getMessage().getBrokerProperty(INTERNAL_ID_EXTRA_PROPERTY);
      if (id == null) {
         return -1;
      } else {
         return id;
      }
   };

   public void postAcknowledge(String address, String queue, long messageID) {
      if (logger.isDebugEnabled()) {
         logger.debug("post acking " + address + ", queue = " + queue + ", messageID = " + messageID);
      }

      Queue targetQueue = server.locateQueue(queue);
      if (targetQueue != null) {
         MessageReference reference = targetQueue.removeWithSuppliedID(messageID, referenceIDSupplier);
         if (reference != null) {
            if (logger.isDebugEnabled()) {
               logger.debug("Acking reference " + reference);
            }
            try {
               targetQueue.acknowledge(reference);
            } catch (Exception e) {
               // TODO anything else I can do here?
               // such as close the connection with error?
               logger.warn(e.getMessage(), e);
            }
         } else {
            if (logger.isTraceEnabled()) {
               logger.trace("There is no reference to ack on " + messageID);
            }
         }
      }

   }

   private void sendMessage(AMQPMessage message) throws Exception {
      if (message.getMessageID() <= 0) {
         message.setMessageID(server.getStorageManager().generateID());
      }

      Long internalID = (Long) AMQPMessageBrokerAccessor.getDeliveryAnnotationProperty(message, INTERNAL_ID);
      String internalAddress = (String) AMQPMessageBrokerAccessor.getDeliveryAnnotationProperty(message, INTERNAL_DESTINATION);

      if (internalID != null) {
         message.setBrokerProperty(INTERNAL_ID_EXTRA_PROPERTY, internalID);
      }

      if (internalAddress != null) {
         message.setAddress(internalAddress);
      }

      routingContext.clear();
      server.getPostOffice().route(message, routingContext, false);
      flow();
   }

   /**
    * not implemented on the target, treated at {@link #postAcknowledge(String, String, long)}
    *
    * @param ref
    * @param reason
    */
   @Override
   public void postAcknowledge(MessageReference ref, AckReason reason) {
   }

   /**
    * not implemented on the target, treated at {@link #sendMessage(AMQPMessage)}
    *
    * @param message
    * @param context
    * @param refs
    */
   @Override
   public void sendMessage(Message message, RoutingContext context, List<MessageReference> refs) {
   }

}
