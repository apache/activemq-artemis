/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.mqtt;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTStateManager {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
   private ActiveMQServer server;
   private final Map<String, MQTTSessionState> sessionStates = new ConcurrentHashMap<>();
   private final Queue sessionStore;
   private static Map<Integer, MQTTStateManager> INSTANCES = new HashMap<>();
   private final Map<String, MQTTConnection> connectedClients  = new ConcurrentHashMap<>();
   private final long timeout;

   /*
    * Even though there may be multiple instances of MQTTProtocolManager (e.g. for MQTT on different ports) we only want
    * one instance of MQTTSessionStateManager per-broker with the understanding that there can be multiple brokers in
    * the same JVM.
    */
   public static synchronized MQTTStateManager getInstance(ActiveMQServer server) throws Exception {
      MQTTStateManager instance = INSTANCES.get(System.identityHashCode(server));
      if (instance == null) {
         instance = new MQTTStateManager(server);
         INSTANCES.put(System.identityHashCode(server), instance);
      }

      return instance;
   }

   public static synchronized void removeInstance(ActiveMQServer server) {
      INSTANCES.remove(System.identityHashCode(server));
   }

   private MQTTStateManager(ActiveMQServer server) throws Exception {
      this.server = server;
      this.timeout = server.getConfiguration().getMqttSessionStatePersistenceTimeout();
      this.sessionStore = server.createQueue(QueueConfiguration.of(MQTTUtil.MQTT_SESSION_STORE).setRoutingType(RoutingType.ANYCAST).setLastValue(true).setDurable(true).setInternal(true).setAutoCreateAddress(true), true);

      // load session data from queue
      try (LinkedListIterator<MessageReference> iterator = sessionStore.browserIterator()) {
         while (iterator.hasNext()) {
            Message message = iterator.next().getMessage();
            if (!(message instanceof CoreMessage)) {
               MQTTLogger.LOGGER.sessionStateMessageIncorrectType(message.getClass().getName().toString());
               continue;
            }
            String clientId = message.getStringProperty(Message.HDR_LAST_VALUE_NAME);
            if (clientId == null || clientId.length() == 0) {
               MQTTLogger.LOGGER.sessionStateMessageBadClientId();
               continue;
            }
            MQTTSessionState sessionState;
            try {
               sessionState = new MQTTSessionState((CoreMessage) message);
            } catch (Exception e) {
               MQTTLogger.LOGGER.errorDeserializingStateMessage(e);
               continue;
            }
            sessionStates.put(clientId, sessionState);
         }
      } catch (NoSuchElementException ignored) {
         // this could happen through paging browsing
      }
   }

   public void scanSessions() {
      List<String> toRemove = new ArrayList();
      for (Map.Entry<String, MQTTSessionState> entry : sessionStates.entrySet()) {
         MQTTSessionState state = entry.getValue();
         logger.debug("Inspecting session: {}", state);
         int sessionExpiryInterval = state.getClientSessionExpiryInterval();
         if (!state.isAttached() && sessionExpiryInterval > 0 && state.getDisconnectedTime() + (sessionExpiryInterval * 1000) < System.currentTimeMillis()) {
            toRemove.add(entry.getKey());
         }
         if (state.isWill() && !state.isAttached() && state.isFailed() && state.getWillDelayInterval() > 0 && state.getDisconnectedTime() + (state.getWillDelayInterval() * 1000) < System.currentTimeMillis()) {
            state.getSession().sendWillMessage();
         }
      }

      for (String key : toRemove) {
         try {
            MQTTSessionState state = removeSessionState(key);
            if (state != null) {
               if (state.isWill() && !state.isAttached() && state.isFailed()) {
                  state.getSession().sendWillMessage();
               }
               state.getSession().clean(false);
            }
         } catch (Exception e) {
            MQTTLogger.LOGGER.failedToRemoveSessionState(key, e);
         }
      }
   }

   public MQTTSessionState getSessionState(String clientId) throws Exception {
      /* [MQTT-3.1.2-4] Attach an existing session if one exists otherwise create a new one. */
      if (sessionStates.containsKey(clientId)) {
         return sessionStates.get(clientId);
      } else {
         MQTTSessionState sessionState = new MQTTSessionState(clientId);
         logger.debug("Adding MQTT session state for: {}", clientId);
         sessionStates.put(clientId, sessionState);
         return sessionState;
      }
   }

   public MQTTSessionState removeSessionState(String clientId) throws Exception {
      logger.debug("Removing MQTT session state for: {}", clientId);
      if (clientId == null) {
         return null;
      }
      removeDurableSessionState(clientId);
      return sessionStates.remove(clientId);
   }

   public void removeDurableSessionState(String clientId) throws Exception {
      int deletedCount = sessionStore.deleteMatchingReferences(FilterImpl.createFilter(new StringBuilder(Message.HDR_LAST_VALUE_NAME).append(" = '").append(clientId).append("'").toString()));
      logger.debug("Removed {} durable MQTT state records for: {}", deletedCount, clientId);
   }

   public Map<String, MQTTSessionState> getSessionStates() {
      return new HashMap<>(sessionStates);
   }

   @Override
   public String toString() {
      return "MQTTSessionStateManager@" + Integer.toHexString(System.identityHashCode(this));
   }

   public void storeSessionState(MQTTSessionState state) throws Exception {
      logger.debug("Adding durable MQTT state record for: {}", state.getClientId());

      /*
       * It is imperative to ensure the routed message is actually *all the way* on the queue before proceeding
       * otherwise there can be a race with removing it.
       */
      CountDownLatch latch = new CountDownLatch(1);
      Transaction tx = new TransactionImpl(server.getStorageManager());
      server.getPostOffice().route(serializeState(state, server.getStorageManager().generateID()), tx, false);
      tx.addOperation(new TransactionOperationAbstract() {
         @Override
         public void afterCommit(Transaction tx) {
            latch.countDown();
         }
      });
      tx.commit();
      if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
         throw MQTTBundle.BUNDLE.unableToStoreMqttState(timeout);
      }
   }

   public static CoreMessage serializeState(MQTTSessionState state, long messageID) {
      CoreMessage message = new CoreMessage().initBuffer(50).setMessageID(messageID);
      message.setAddress(MQTTUtil.MQTT_SESSION_STORE);
      message.setDurable(true);
      message.putStringProperty(Message.HDR_LAST_VALUE_NAME, state.getClientId());
      Collection<Pair<MqttTopicSubscription, Integer>> subscriptions = state.getSubscriptionsPlusID();
      ActiveMQBuffer buf = message.getBodyBuffer();

      /*
       * This byte represents the "version". If the payload changes at any point in the future then we can detect that
       * and adjust so that when users are upgrading we can still read the old data format.
       */
      buf.writeByte((byte) 0);

      buf.writeInt(subscriptions.size());
      logger.debug("Serializing {} subscriptions", subscriptions.size());
      for (Pair<MqttTopicSubscription, Integer> pair : subscriptions) {
         MqttTopicSubscription sub = pair.getA();
         buf.writeString(sub.topicName());
         buf.writeInt(sub.option().qos().value());
         buf.writeBoolean(sub.option().isNoLocal());
         buf.writeBoolean(sub.option().isRetainAsPublished());
         buf.writeInt(sub.option().retainHandling().value());
         buf.writeNullableInt(pair.getB());
      }

      return message;
   }

   public boolean isClientConnected(String clientId, MQTTConnection connection) {
      MQTTConnection connectedConn = connectedClients.get(clientId);

      if (connectedConn != null) {
         return connectedConn.equals(connection);
      }

      return false;
   }

   public boolean isClientConnected(String clientId) {
      return connectedClients.containsKey(clientId);
   }

   public void removeConnectedClient(String clientId) {
      connectedClients.remove(clientId);
   }

   /**
    * @param clientId
    * @param connection
    * @return the {@code MQTTConnection} that the added connection replaced or null if there was no previous entry for
    * the {@code clientId}
    */
   public MQTTConnection addConnectedClient(String clientId, MQTTConnection connection) {
      return connectedClients.put(clientId, connection);
   }

   public MQTTConnection getConnectedClient(String clientId) {
      return connectedClients.get(clientId);
   }

   /** For DEBUG only */
   public Map<String, MQTTConnection> getConnectedClients() {
      return connectedClients;
   }
}