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
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.Inflater;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.BodyEncoder;
import org.apache.activemq.artemis.core.message.impl.MessageImpl;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.UUIDGenerator;

import static org.apache.activemq.artemis.core.protocol.stomp.ActiveMQStompProtocolMessageBundle.BUNDLE;

public class StompSession implements SessionCallback {

   private final StompProtocolManager manager;

   private final StompConnection connection;

   private ServerSession session;

   private final OperationContext sessionContext;

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

   void setServerSession(ServerSession session) {
      this.session = session;
   }

   public ServerSession getSession() {
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
   public int sendMessage(ServerMessage serverMessage, ServerConsumer consumer, int deliveryCount) {
      LargeServerMessageImpl largeMessage = null;
      ServerMessage newServerMessage = serverMessage;
      try {
         StompSubscription subscription = subscriptions.get(consumer.getID());
         StompFrame frame = null;
         if (serverMessage.isLargeMessage()) {
            newServerMessage = serverMessage.copy();

            largeMessage = (LargeServerMessageImpl) serverMessage;
            BodyEncoder encoder = largeMessage.getBodyEncoder();
            encoder.open();
            int bodySize = (int) encoder.getLargeBodySize();

            //large message doesn't have a body.
            ((ServerMessageImpl) newServerMessage).createBody(bodySize);
            encoder.encode(newServerMessage.getBodyBuffer(), bodySize);
            encoder.close();
         }

         if (serverMessage.getBooleanProperty(Message.HDR_LARGE_COMPRESSED)) {
            //decompress
            ActiveMQBuffer qbuff = newServerMessage.getBodyBuffer();
            int bytesToRead = qbuff.writerIndex() - MessageImpl.BODY_OFFSET;
            Inflater inflater = new Inflater();
            inflater.setInput(qbuff.readBytes(bytesToRead).toByteBuffer().array());

            //get the real size of large message
            long sizeBody = newServerMessage.getLongProperty(Message.HDR_LARGE_BODY_SIZE);

            byte[] data = new byte[(int) sizeBody];
            inflater.inflate(data);
            inflater.end();
            qbuff.resetReaderIndex();
            qbuff.resetWriterIndex();
            qbuff.writeBytes(data);
         }

         frame = connection.createStompMessage(newServerMessage, subscription, deliveryCount);

         int length = frame.getEncodedSize();

         if (subscription.getAck().equals(Stomp.Headers.Subscribe.AckModeValues.AUTO)) {
            if (manager.send(connection, frame)) {
               //we ack and commit only if the send is successful
               session.acknowledge(consumer.getID(), newServerMessage.getMessageID());
               session.commit();
            }
         }
         else {
            messagesToAck.put(newServerMessage.getMessageID(), new Pair<>(consumer.getID(), length));
            // Must send AFTER adding to messagesToAck - or could get acked from client BEFORE it's been added!
            manager.send(connection, frame);
         }

         return length;
      }
      catch (Exception e) {
         return 0;
      }
      finally {
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
   public int sendLargeMessage(ServerMessage msg, ServerConsumer consumer, long bodySize, int deliveryCount) {
      return 0;
   }

   @Override
   public void closed() {
   }

   @Override
   public void addReadyListener(final ReadyListener listener) {
      connection.getTransportConnection().addReadyListener(listener);
   }

   @Override
   public void removeReadyListener(final ReadyListener listener) {
      connection.getTransportConnection().removeReadyListener(listener);
   }

   @Override
   public void disconnect(ServerConsumer consumerId, String queueName) {
      StompSubscription stompSubscription = subscriptions.remove(consumerId.getID());
      if (stompSubscription != null) {
         StompFrame frame = connection.getFrameHandler().createStompFrame(Stomp.Responses.ERROR);
         frame.addHeader(Stomp.Headers.CONTENT_TYPE, "text/plain");
         frame.setBody("consumer with ID " + consumerId + " disconnected by server");
         connection.sendFrame(frame);
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
      }
      else {
         session.acknowledge(consumerID, id);
      }

      session.commit();
   }

   public void addSubscription(long consumerID,
                               String subscriptionID,
                               String clientID,
                               String durableSubscriptionName,
                               String destination,
                               String selector,
                               String ack) throws Exception {
      SimpleString queue = SimpleString.toSimpleString(destination);
      int receiveCredits = consumerCredits;
      if (ack.equals(Stomp.Headers.Subscribe.AckModeValues.AUTO)) {
         receiveCredits = -1;
      }

      if (destination.startsWith("jms.topic")) {
         // subscribes to a topic
         if (durableSubscriptionName != null) {
            if (clientID == null) {
               throw BUNDLE.missingClientID();
            }
            queue = SimpleString.toSimpleString(clientID + "." + durableSubscriptionName);
            QueueQueryResult query = session.executeQueueQuery(queue);
            if (!query.isExists()) {
               session.createQueue(SimpleString.toSimpleString(destination), queue, SimpleString.toSimpleString(selector), false, true);
            }
         }
         else {
            queue = UUIDGenerator.getInstance().generateSimpleStringUUID();
            session.createQueue(SimpleString.toSimpleString(destination), queue, SimpleString.toSimpleString(selector), true, false);
         }
         ((ServerSessionImpl) session).createConsumer(consumerID, queue, null, false, false, receiveCredits);
      }
      else {
         ((ServerSessionImpl) session).createConsumer(consumerID, queue, SimpleString.toSimpleString(selector), false, false, receiveCredits);
      }

      StompSubscription subscription = new StompSubscription(subscriptionID, ack);
      subscriptions.put(consumerID, subscription);

      session.start();
   }

   public boolean unsubscribe(String id, String durableSubscriptionName) throws Exception {
      Iterator<Entry<Long, StompSubscription>> iterator = subscriptions.entrySet().iterator();
      while (iterator.hasNext()) {
         Map.Entry<Long, StompSubscription> entry = iterator.next();
         long consumerID = entry.getKey();
         StompSubscription sub = entry.getValue();
         if (id != null && id.equals(sub.getID())) {
            iterator.remove();
            session.closeConsumer(consumerID);
            SimpleString queueName;
            if (durableSubscriptionName != null && durableSubscriptionName.trim().length() != 0) {
               queueName = SimpleString.toSimpleString(id + "." + durableSubscriptionName);
            }
            else {
               queueName = SimpleString.toSimpleString(id);
            }
            QueueQueryResult query = session.executeQueueQuery(queueName);
            if (query.isExists()) {
               session.deleteQueue(queueName);
            }
            return true;
         }
      }
      return false;
   }

   boolean containsSubscription(String subscriptionID) {
      Iterator<Entry<Long, StompSubscription>> iterator = subscriptions.entrySet().iterator();
      while (iterator.hasNext()) {
         Map.Entry<Long, StompSubscription> entry = iterator.next();
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

   public void sendInternal(ServerMessageImpl message, boolean direct) throws Exception {
      session.send(message, direct);
   }

   public void sendInternalLarge(ServerMessageImpl message, boolean direct) throws Exception {
      int headerSize = message.getHeadersAndPropertiesEncodeSize();
      if (headerSize >= connection.getMinLargeMessageSize()) {
         throw BUNDLE.headerTooBig();
      }

      StorageManager storageManager = ((ServerSessionImpl) session).getStorageManager();
      long id = storageManager.generateID();
      LargeServerMessage largeMessage = storageManager.createLargeMessage(id, message);

      byte[] bytes = new byte[message.getBodyBuffer().writerIndex() - MessageImpl.BODY_OFFSET];
      message.getBodyBuffer().readBytes(bytes);

      largeMessage.addBytes(bytes);

      largeMessage.releaseResources();

      largeMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, bytes.length);

      session.send(largeMessage, direct);

      largeMessage = null;
   }

}
