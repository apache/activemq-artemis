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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

import javax.jms.InvalidDestinationException;
import javax.jms.ResourceAllocationException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireConnection;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageConverter;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireProtocolManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.SlowConsumerDetectionListener;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.activemq.artemis.utils.SimpleIDGenerator;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.wireformat.WireFormat;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.core.protocol.openwire.util.OpenWireUtil.OPENWIRE_WILDCARD;

public class AMQSession implements SessionCallback {
   private final Logger logger = Logger.getLogger(AMQSession.class);

   // ConsumerID is generated inside the session, 0, 1, 2, ... as many consumers as you have on the session
   protected final IDGenerator consumerIDGenerator = new SimpleIDGenerator(0);

   private ConnectionInfo connInfo;
   private ServerSession coreSession;
   private SessionInfo sessInfo;
   private ActiveMQServer server;
   private OpenWireConnection connection;

   private AtomicBoolean started = new AtomicBoolean(false);

   private final ScheduledExecutorService scheduledPool;

   // The sessionWireformat used by the session
   // this object is meant to be used per thread / session
   // so we make a new one per AMQSession
   private final OpenWireMessageConverter converter;

   private final OpenWireProtocolManager protocolManager;

   public AMQSession(ConnectionInfo connInfo,
                     SessionInfo sessInfo,
                     ActiveMQServer server,
                     OpenWireConnection connection,
                     OpenWireProtocolManager protocolManager) {
      this.connInfo = connInfo;
      this.sessInfo = sessInfo;

      this.server = server;
      this.connection = connection;
      this.protocolManager = protocolManager;
      this.scheduledPool = protocolManager.getScheduledPool();
      OpenWireFormat marshaller = (OpenWireFormat) connection.getMarshaller();

      this.converter = new OpenWireMessageConverter(marshaller.copy());
   }

   public boolean isClosed() {
      return coreSession.isClosed();
   }

   public OpenWireMessageConverter getConverter() {
      return converter;
   }

   public void initialize() {
      String name = sessInfo.getSessionId().toString();
      String username = connInfo.getUserName();
      String password = connInfo.getPassword();

      int minLargeMessageSize = Integer.MAX_VALUE; // disable
      // minLargeMessageSize for
      // now

      try {
         coreSession = server.createSession(name, username, password, minLargeMessageSize, connection, true, false, false, false, null, this, true, connection.getOperationContext(), protocolManager.getPrefixes());

         long sessionId = sessInfo.getSessionId().getValue();
         if (sessionId == -1) {
            this.connection.setAdvisorySession(this);
         }
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.error("error init session", e);
      }

   }

   @Override
   public boolean updateDeliveryCountAfterCancel(ServerConsumer consumer, MessageReference ref, boolean failed) {
      if (consumer.getProtocolData() != null) {
         return ((AMQConsumer) consumer.getProtocolData()).updateDeliveryCountAfterCancel(ref);
      } else {
         return false;
      }

   }

   public List<AMQConsumer> createConsumer(ConsumerInfo info,
                                           SlowConsumerDetectionListener slowConsumerDetectionListener) throws Exception {
      //check destination
      ActiveMQDestination dest = info.getDestination();
      ActiveMQDestination[] dests = null;
      if (dest.isComposite()) {
         dests = dest.getCompositeDestinations();
      } else {
         dests = new ActiveMQDestination[]{dest};
      }

      List<AMQConsumer> consumersList = new java.util.LinkedList<>();

      for (ActiveMQDestination openWireDest : dests) {
         if (openWireDest.isQueue()) {
            SimpleString queueName = new SimpleString(convertWildcard(openWireDest.getPhysicalName()));

            if (!checkAutoCreateQueue(queueName, openWireDest.isTemporary())) {
               throw new InvalidDestinationException("Destination doesn't exist: " + queueName);
            }
         }
         AMQConsumer consumer = new AMQConsumer(this, openWireDest, info, scheduledPool);

         long nativeID = consumerIDGenerator.generateID();
         consumer.init(slowConsumerDetectionListener, nativeID);
         consumersList.add(consumer);
      }

      return consumersList;
   }

   private boolean checkAutoCreateQueue(SimpleString queueName, boolean isTemporary) throws Exception {
      boolean hasQueue = true;
      if (!connection.containsKnownDestination(queueName)) {

         BindingQueryResult bindingQuery = server.bindingQuery(queueName);
         QueueQueryResult queueBinding = server.queueQuery(queueName);

         boolean isAutoCreate = bindingQuery.isExists() ? true : bindingQuery.isAutoCreateQueues();

         if (!queueBinding.isExists()) {
            if (isAutoCreate) {
               server.createQueue(queueName, RoutingType.ANYCAST, queueName, null, true, isTemporary);
               connection.addKnownDestination(queueName);
            } else {
               hasQueue = false;
            }
         }
      }
      return hasQueue;
   }

   public void start() {

      coreSession.start();
      started.set(true);

   }

   // rename actualDest to destination
   @Override
   public void afterDelivery() throws Exception {

   }

   @Override
   public void browserFinished(ServerConsumer consumer) {
      AMQConsumer theConsumer = (AMQConsumer) consumer.getProtocolData();
      if (theConsumer != null) {
         theConsumer.browseFinished();
      }
   }

   @Override
   public boolean isWritable(ReadyListener callback, Object protocolContext) {
      return connection.isWritable(callback);
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address) {
      // TODO Auto-generated method stub

   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address) {
      // TODO Auto-generated method stub

   }

   @Override
   public int sendMessage(MessageReference reference,
                          org.apache.activemq.artemis.api.core.Message message,
                          ServerConsumer consumer,
                          int deliveryCount) {
      AMQConsumer theConsumer = (AMQConsumer) consumer.getProtocolData();
      return theConsumer.handleDeliver(reference, message, deliveryCount);
   }

   @Override
   public int sendLargeMessage(MessageReference reference,
                               org.apache.activemq.artemis.api.core.Message message,
                               ServerConsumer consumerID,
                               long bodySize,
                               int deliveryCount) {
      // TODO Auto-generated method stub
      return 0;
   }

   @Override
   public int sendLargeMessageContinuation(ServerConsumer consumerID,
                                           byte[] body,
                                           boolean continues,
                                           boolean requiresResponse) {
      // TODO Auto-generated method stub
      return 0;
   }

   @Override
   public void closed() {
      // TODO Auto-generated method stub

   }

   @Override
   public boolean hasCredits(ServerConsumer consumer) {

      AMQConsumer amqConsumer = null;

      if (consumer.getProtocolData() != null) {
         amqConsumer = (AMQConsumer) consumer.getProtocolData();
      }

      return amqConsumer != null && amqConsumer.hasCredits();
   }

   @Override
   public void disconnect(ServerConsumer consumerId, String queueName) {
      // TODO Auto-generated method stub

   }

   public void send(final ProducerInfo producerInfo,
                    final Message messageSend,
                    boolean sendProducerAck) throws Exception {
      messageSend.setBrokerInTime(System.currentTimeMillis());

      ActiveMQDestination destination = messageSend.getDestination();

      ActiveMQDestination[] actualDestinations = null;
      if (destination.isComposite()) {
         actualDestinations = destination.getCompositeDestinations();
         messageSend.setOriginalDestination(destination);
      } else {
         actualDestinations = new ActiveMQDestination[]{destination};
      }

      org.apache.activemq.artemis.api.core.Message originalCoreMsg = getConverter().inbound(messageSend);

      if (connection.isNoLocal()) {
         //Note: advisory messages are dealt with in
         //OpenWireProtocolManager#fireAdvisory
         originalCoreMsg.putStringProperty(MessageUtil.CONNECTION_ID_PROPERTY_NAME.toString(), this.connection.getState().getInfo().getConnectionId().getValue());
      }

      /* ActiveMQ failover transport will attempt to reconnect after connection failure.  Any sent messages that did
      * not receive acks will be resent.  (ActiveMQ broker handles this by returning a last sequence id received to
      * the client).  To handle this in Artemis we use a duplicate ID cache.  To do this we check to see if the
      * message comes from failover connection.  If so we add a DUPLICATE_ID to handle duplicates after a resend. */
      if (connection.getContext().isFaultTolerant() && !messageSend.getProperties().containsKey(org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString())) {
         originalCoreMsg.putStringProperty(org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), messageSend.getMessageId().toString());
      }

      boolean shouldBlockProducer = producerInfo.getWindowSize() > 0 || messageSend.isResponseRequired();

      final AtomicInteger count = new AtomicInteger(actualDestinations.length);


      if (shouldBlockProducer) {
         connection.getContext().setDontSendReponse(true);
      }

      for (int i = 0; i < actualDestinations.length; i++) {
         ActiveMQDestination dest = actualDestinations[i];
         SimpleString address = new SimpleString(dest.getPhysicalName());
         org.apache.activemq.artemis.api.core.Message coreMsg = originalCoreMsg.copy();
         coreMsg.setAddress(address);

         if (actualDestinations[i].isQueue()) {
            checkAutoCreateQueue(new SimpleString(actualDestinations[i].getPhysicalName()), actualDestinations[i].isTemporary());
            coreMsg.putByteProperty(org.apache.activemq.artemis.api.core.Message.HDR_ROUTING_TYPE, RoutingType.ANYCAST.getType());
         } else {
            coreMsg.putByteProperty(org.apache.activemq.artemis.api.core.Message.HDR_ROUTING_TYPE, RoutingType.MULTICAST.getType());
         }
         PagingStore store = server.getPagingManager().getPageStore(address);


         this.connection.disableTtl();
         if (shouldBlockProducer) {
            if (!store.checkMemory(() -> {
               Exception exceptionToSend = null;

               try {
                  RoutingStatus result = getCoreSession().send(coreMsg, false, dest.isTemporary());

                  if (result == RoutingStatus.NO_BINDINGS && dest.isQueue()) {
                     throw new InvalidDestinationException("Cannot publish to a non-existent Destination: " + dest);
                  }
               } catch (Exception e) {

                  logger.warn(e.getMessage(), e);
                  exceptionToSend = e;
               }
               connection.enableTtl();
               if (count.decrementAndGet() == 0) {
                  if (exceptionToSend != null) {
                     this.connection.getContext().setDontSendReponse(false);
                     connection.sendException(exceptionToSend);
                  } else {
                     if (sendProducerAck) {
                        try {
                           ProducerAck ack = new ProducerAck(producerInfo.getProducerId(), messageSend.getSize());
                           connection.dispatchAsync(ack);
                        } catch (Exception e) {
                           this.connection.getContext().setDontSendReponse(false);
                           ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
                           connection.sendException(e);
                        }
                     } else {
                        connection.getContext().setDontSendReponse(false);
                        try {
                           Response response = new Response();
                           response.setCorrelationId(messageSend.getCommandId());
                           connection.dispatchAsync(response);
                        } catch (Exception e) {
                           ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
                           connection.sendException(e);
                        }
                     }
                  }
               }
            })) {
               this.connection.getContext().setDontSendReponse(false);
               connection.enableTtl();
               throw new ResourceAllocationException("Queue is full " + address);
            }
         } else {
            //non-persistent messages goes here, by default we stop reading from
            //transport
            connection.getTransportConnection().setAutoRead(false);
            if (!store.checkMemory(() -> {
               connection.getTransportConnection().setAutoRead(true);
               connection.enableTtl();
            })) {
               connection.getTransportConnection().setAutoRead(true);
               connection.enableTtl();
               throw new ResourceAllocationException("Queue is full " + address);
            }

            RoutingStatus result = getCoreSession().send(coreMsg, false, dest.isTemporary());
            if (result == RoutingStatus.NO_BINDINGS && dest.isQueue()) {
               throw new InvalidDestinationException("Cannot publish to a non-existent Destination: " + dest);
            }

            if (count.decrementAndGet() == 0) {
               if (sendProducerAck) {
                  ProducerAck ack = new ProducerAck(producerInfo.getProducerId(), messageSend.getSize());
                  connection.dispatchAsync(ack);
               }
            }
         }
      }
   }

   public String convertWildcard(String physicalName) {
      return OPENWIRE_WILDCARD.convert(physicalName, server.getConfiguration().getWildcardConfiguration());
   }

   public ServerSession getCoreSession() {
      return this.coreSession;
   }

   public ActiveMQServer getCoreServer() {
      return this.server;
   }

   public WireFormat getMarshaller() {
      return this.connection.getMarshaller();
   }

   public ConnectionInfo getConnectionInfo() {
      return this.connInfo;
   }

   public void disableSecurity() {
      this.coreSession.disableSecurity();
   }

   public void deliverMessage(MessageDispatch dispatch) {
      this.connection.deliverMessage(dispatch);
   }

   public void close() throws Exception {
      this.coreSession.close(false);
   }

   public OpenWireConnection getConnection() {
      return connection;
   }

   public boolean isInternal() {
      return sessInfo.getSessionId().getValue() == -1;
   }
}
