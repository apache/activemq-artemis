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
package org.apache.activemq.artemis.core.protocol.stomp;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.PendingTask;
import org.apache.activemq.artemis.utils.UUIDGenerator;

import static org.apache.activemq.artemis.core.protocol.stomp.ActiveMQStompProtocolMessageBundle.BUNDLE;

public class StompSession implements SessionCallback {

   private final StompProtocolManager manager;

   private final StompConnection connection;

   private ServerSession session;

   private final OperationContext sessionContext;

   private final BlockingDeque<PendingTask> afterDeliveryTasks = new LinkedBlockingDeque<>();

   private final Map<Long, StompSubscription> subscriptions = new ConcurrentHashMap<>();

   // key = message ID, value = consumer ID
   private final Map<Long, Pair<Long, Integer>> messagesToAck = new ConcurrentHashMap<>();

   private volatile boolean noLocal = false;

   private final int consumerCredits;

   StompSession(final StompConnection connection, final StompProtocolManager manager, OperationContext sessionContext) {
      this.connection = connection;
      this.manager = manager;
      this.sessionContext = sessionContext;
      this.consumerCredits = ConfigurationHelper.getIntProperty(TransportConstants.STOMP_CONSUMERS_CREDIT, TransportConstants.STOMP_DEFAULT_CONSUMERS_CREDIT, connection.getAcceptorUsed().getConfiguration());
   }

   @Override
   public boolean isWritable(ReadyListener callback, Object protocolContext) {
      return connection.isWritable(callback);
   }

   void setServerSession(ServerSession session) {
      this.session = session;
   }

   public ServerSession getCoreSession() {
      return session;
   }

   @Override
   public boolean hasCredits(ServerConsumer consumerID) {
      return true;
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address) {
   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address) {
   }

   @Override
   public void afterDelivery() throws Exception {
      PendingTask task;
      while ((task = afterDeliveryTasks.poll()) != null) {
         task.run();
      }
   }

   @Override
   public void browserFinished(ServerConsumer consumer) {

   }

   @Override
   public boolean updateDeliveryCountAfterCancel(ServerConsumer consumer, MessageReference ref, boolean failed) {
      return false;
   }

   @Override
   public int sendMessage(MessageReference ref,
                          Message serverMessage,
                          final ServerConsumer consumer,
                          int deliveryCount) {

      ICoreMessage  coreMessage = serverMessage.toCore();

      LargeServerMessageImpl largeMessage = null;
      ICoreMessage newServerMessage = serverMessage.toCore();
      try {
         StompSubscription subscription = subscriptions.get(consumer.getID());
         // subscription might be null if the consumer was closed
         if (subscription == null)
            return 0;
         StompFrame frame;
         ActiveMQBuffer buffer = coreMessage.getDataBuffer();

         frame = connection.createStompMessage(newServerMessage, buffer, subscription, deliveryCount);

         int length = frame.getEncodedSize();

         if (subscription.getAck().equals(Stomp.Headers.Subscribe.AckModeValues.AUTO)) {
            if (manager.send(connection, frame)) {
               final long messageID = newServerMessage.getMessageID();
               final long consumerID = consumer.getID();

               // this will be called after the delivery is complete
               // we can't call session.ack within the delivery
               // as it could dead lock.
               afterDeliveryTasks.offer(new PendingTask() {
                  @Override
                  public void run() throws Exception {
                     //we ack and commit only if the send is successful
                     session.acknowledge(consumerID, messageID);
                     session.commit();
                  }
               });
            }
         } else {
            messagesToAck.put(newServerMessage.getMessageID(), new Pair<>(consumer.getID(), length));
            // Must send AFTER adding to messagesToAck - or could get acked from client BEFORE it's been added!
            manager.send(connection, frame);
         }

         return length;
      } catch (Exception e) {
         if (ActiveMQStompProtocolLogger.LOGGER.isDebugEnabled()) {
            ActiveMQStompProtocolLogger.LOGGER.debug(e);
         }
         return 0;
      } finally {
         if (largeMessage != null) {
            largeMessage.releaseResources();
            largeMessage = null;
         }
      }

   }

   @Override
   public int sendLargeMessageContinuation(ServerConsumer consumer,
                                           byte[] body,
                                           boolean continues,
                                           boolean requiresResponse) {
      return 0;
   }

   @Override
   public int sendLargeMessage(MessageReference ref,
                               Message msg,
                               ServerConsumer consumer,
                               long bodySize,
                               int deliveryCount) {
      return 0;
   }

   @Override
   public void closed() {
   }

   @Override
   public void disconnect(ServerConsumer consumerId, SimpleString queueName) {
      StompSubscription stompSubscription = subscriptions.remove(consumerId.getID());
      if (stompSubscription != null) {
         StompFrame frame = connection.getFrameHandler().createStompFrame(Stomp.Responses.ERROR);
         frame.addHeader(Stomp.Headers.CONTENT_TYPE, "text/plain");
         frame.setBody("consumer with ID " + consumerId + " disconnected by server");
         connection.sendFrame(frame, null);
      }
   }

   public void acknowledge(String messageID, String subscriptionID) throws Exception {
      long id = Long.parseLong(messageID);
      Pair<Long, Integer> pair = messagesToAck.remove(id);

      if (pair == null) {
         throw BUNDLE.failToAckMissingID(id).setHandler(connection.getFrameHandler());
      }

      long consumerID = pair.getA();
      int credits = pair.getB();

      StompSubscription sub = subscriptions.get(consumerID);

      if (subscriptionID != null) {
         if (!sub.getID().equals(subscriptionID)) {
            throw BUNDLE.subscriptionIDMismatch(subscriptionID, sub.getID()).setHandler(connection.getFrameHandler());
         }
      }

      if (this.consumerCredits != -1) {
         session.receiveConsumerCredits(consumerID, credits);
      }

      if (sub.getAck().equals(Stomp.Headers.Subscribe.AckModeValues.CLIENT_INDIVIDUAL)) {
         session.individualAcknowledge(consumerID, id);
      } else {
         session.acknowledge(consumerID, id);
      }

      session.commit();
   }

   public StompPostReceiptFunction addSubscription(long consumerID,
                               String subscriptionID,
                               String clientID,
                               String durableSubscriptionName,
                               String destination,
                               String selector,
                               String ack) throws Exception {
      SimpleString address = SimpleString.toSimpleString(destination);
      SimpleString queueName = SimpleString.toSimpleString(destination);
      SimpleString selectorSimple = SimpleString.toSimpleString(selector);
      boolean pubSub = false;
      final int receiveCredits = ack.equals(Stomp.Headers.Subscribe.AckModeValues.AUTO) ? -1 : consumerCredits;

      Set<RoutingType> routingTypes = manager.getServer().getAddressInfo(getCoreSession().removePrefix(address)).getRoutingTypes();
      boolean topic = routingTypes.size() == 1 && routingTypes.contains(RoutingType.MULTICAST);
      if (topic) {
         // subscribes to a topic
         pubSub = true;
         if (durableSubscriptionName != null) {
            if (clientID == null) {
               throw BUNDLE.missingClientID();
            }
            queueName = SimpleString.toSimpleString(clientID + "." + durableSubscriptionName);
            if (manager.getServer().locateQueue(queueName) == null) {
               session.createQueue(address, queueName, selectorSimple, false, true);
            }
         } else {
            queueName = UUIDGenerator.getInstance().generateSimpleStringUUID();
            session.createQueue(address, queueName, selectorSimple, true, false);
         }
      }
      final ServerConsumer consumer = session.createConsumer(consumerID, queueName, topic ? null : selectorSimple, false, false, 0);
      StompSubscription subscription = new StompSubscription(subscriptionID, ack, queueName, pubSub);
      subscriptions.put(consumerID, subscription);
      session.start();
      return () -> consumer.receiveCredits(receiveCredits);
   }

   public boolean unsubscribe(String id, String durableSubscriptionName, String clientID) throws Exception {
      boolean result = false;
      Iterator<Entry<Long, StompSubscription>> iterator = subscriptions.entrySet().iterator();

      while (iterator.hasNext()) {
         Map.Entry<Long, StompSubscription> entry = iterator.next();
         long consumerID = entry.getKey();
         StompSubscription sub = entry.getValue();
         if (id != null && id.equals(sub.getID())) {
            iterator.remove();
            SimpleString queueName = sub.getQueueName();
            session.closeConsumer(consumerID);
            if (sub.isPubSub() && manager.getServer().locateQueue(queueName) != null) {
               session.deleteQueue(queueName);
            }
            result = true;
         }
      }

      if (!result && durableSubscriptionName != null && clientID != null) {
         SimpleString queueName = SimpleString.toSimpleString(clientID + "." + durableSubscriptionName);
         if (manager.getServer().locateQueue(queueName) != null) {
            session.deleteQueue(queueName);
         }
         result = true;
      }

      return result;
   }

   boolean containsSubscription(String subscriptionID) {
      for (Entry<Long, StompSubscription> entry : subscriptions.entrySet()) {
         StompSubscription sub = entry.getValue();
         if (sub.getID().equals(subscriptionID)) {
            return true;
         }
      }
      return false;
   }

   public RemotingConnection getConnection() {
      return connection;
   }

   public OperationContext getContext() {
      return sessionContext;
   }

   public boolean isNoLocal() {
      return noLocal;
   }

   public void setNoLocal(boolean noLocal) {
      this.noLocal = noLocal;
   }

   public void sendInternal(Message message, boolean direct) throws Exception {
      session.send(message, direct);
   }

   public void sendInternalLarge(CoreMessage message, boolean direct) throws Exception {
      int headerSize = message.getHeadersAndPropertiesEncodeSize();
      if (headerSize >= connection.getMinLargeMessageSize()) {
         throw BUNDLE.headerTooBig();
      }

      StorageManager storageManager = ((ServerSessionImpl) session).getStorageManager();
      long id = storageManager.generateID();
      LargeServerMessage largeMessage = storageManager.createLargeMessage(id, message);

      ActiveMQBuffer body = message.getReadOnlyBodyBuffer();
      byte[] bytes = new byte[body.readableBytes()];
      body.readBytes(bytes);

      largeMessage.addBytes(bytes);

      largeMessage.releaseResources();

      largeMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, bytes.length);

      session.send(largeMessage, direct);

      largeMessage = null;
   }

}
