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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.MessageConverter;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
import org.apache.activemq.artemis.utils.TypedProperties;
import org.apache.activemq.artemis.utils.UUIDGenerator;

import static org.apache.activemq.artemis.core.protocol.stomp.ActiveMQStompProtocolMessageBundle.BUNDLE;

/**
 * StompProtocolManager
 */
class StompProtocolManager implements ProtocolManager<StompFrameInterceptor>, NotificationListener {
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ActiveMQServer server;

   private final StompProtocolManagerFactory factory;

   private final Executor executor;

   private final Map<String, StompSession> transactedSessions = new HashMap<String, StompSession>();

   // key => connection ID, value => Stomp session
   private final Map<Object, StompSession> sessions = new HashMap<Object, StompSession>();

   private final Set<String> destinations = new ConcurrentHashSet<String>();

   private final List<StompFrameInterceptor> incomingInterceptors;
   private final List<StompFrameInterceptor> outgoingInterceptors;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public StompProtocolManager(final StompProtocolManagerFactory factory,
                               final ActiveMQServer server,
                               final List<StompFrameInterceptor> incomingInterceptors,
                               final List<StompFrameInterceptor> outgoingInterceptors) {
      this.factory = factory;
      this.server = server;
      this.executor = server.getExecutorFactory().getExecutor();
      ManagementService service = server.getManagementService();
      if (service != null) {
         //allow management message to pass
         destinations.add(service.getManagementAddress().toString());
         service.addNotificationListener(this);
      }
      this.incomingInterceptors = incomingInterceptors;
      this.outgoingInterceptors = outgoingInterceptors;
   }

   @Override
   public boolean acceptsNoHandshake() {
      return false;
   }

   @Override
   public ProtocolManagerFactory<StompFrameInterceptor> getFactory() {
      return factory;
   }

   @Override
   public void updateInterceptors(List<BaseInterceptor> incoming, List<BaseInterceptor> outgoing) {
      this.incomingInterceptors.clear();
      this.incomingInterceptors.addAll(getFactory().filterInterceptors(incoming));

      this.outgoingInterceptors.clear();
      this.outgoingInterceptors.addAll(getFactory().filterInterceptors(outgoing));
   }

   @Override
   public MessageConverter getConverter() {
      return null;
   }

   // ProtocolManager implementation --------------------------------

   @Override
   public ConnectionEntry createConnectionEntry(final Acceptor acceptorUsed, final Connection connection) {
      StompConnection conn = new StompConnection(acceptorUsed, connection, this);

      // Note that STOMP 1.0 has no heartbeat, so if connection ttl is non zero, data must continue to be sent or connection
      // will be timed out and closed!

      String ttlStr = (String) acceptorUsed.getConfiguration().get(TransportConstants.CONNECTION_TTL);
      Long ttl = ttlStr == null ? null : Long.valueOf(ttlStr);

      if (ttl != null) {
         if (ttl > 0) {
            return new ConnectionEntry(conn, null, System.currentTimeMillis(), ttl);
         }
         throw BUNDLE.negativeConnectionTTL(ttl);
      }

      ttl = server.getConfiguration().getConnectionTTLOverride();

      if (ttl != -1) {
         return new ConnectionEntry(conn, null, System.currentTimeMillis(), ttl);
      }
      else {
         // Default to 1 minute - which is same as core protocol
         return new ConnectionEntry(conn, null, System.currentTimeMillis(), 1 * 60 * 1000);
      }
   }

   @Override
   public void removeHandler(String name) {
   }

   @Override
   public void handleBuffer(final RemotingConnection connection, final ActiveMQBuffer buffer) {
      StompConnection conn = (StompConnection) connection;

      conn.setDataReceived();

      do {
         StompFrame request;
         try {
            request = conn.decode(buffer);
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorDecodingPacket(e);
            return;
         }

         if (request == null) {
            break;
         }

         try {
            invokeInterceptors(this.incomingInterceptors, request, conn);
            conn.handleFrame(request);
         }
         finally {
            server.getStorageManager().clearContext();
         }
      } while (conn.hasBytes());
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline) {
   }

   @Override
   public boolean isProtocol(byte[] array) {
      String frameStart = new String(array, StandardCharsets.US_ASCII);
      return frameStart.startsWith(Stomp.Commands.CONNECT) || frameStart.startsWith(Stomp.Commands.STOMP);
   }

   @Override
   public void handshake(NettyServerConnection connection, ActiveMQBuffer buffer) {
      //Todo move handshake to here
   }

   // Public --------------------------------------------------------

   public boolean send(final StompConnection connection, final StompFrame frame) {
      if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
         ActiveMQServerLogger.LOGGER.trace("sent " + frame);
      }

      invokeInterceptors(this.outgoingInterceptors, frame, connection);

      synchronized (connection) {
         if (connection.isDestroyed()) {
            ActiveMQStompProtocolLogger.LOGGER.connectionClosed(connection);
            return false;
         }

         try {
            connection.physicalSend(frame);
         }
         catch (Exception e) {
            ActiveMQStompProtocolLogger.LOGGER.errorSendingFrame(e, frame);
            return false;
         }
         return true;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   public StompSession getSession(StompConnection connection) throws Exception {
      StompSession stompSession = sessions.get(connection.getID());
      if (stompSession == null) {
         stompSession = new StompSession(connection, this, server.getStorageManager().newContext(server.getExecutorFactory().getExecutor()));
         String name = UUIDGenerator.getInstance().generateStringUUID();
         ServerSession session = server.createSession(name, connection.getLogin(), connection.getPasscode(), ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, connection, true, false, false, false, null, stompSession, null, true);
         stompSession.setServerSession(session);
         sessions.put(connection.getID(), stompSession);
      }
      server.getStorageManager().setContext(stompSession.getContext());
      return stompSession;
   }

   public StompSession getTransactedSession(StompConnection connection, String txID) throws Exception {
      StompSession stompSession = transactedSessions.get(txID);
      if (stompSession == null) {
         stompSession = new StompSession(connection, this, server.getStorageManager().newContext(executor));
         String name = UUIDGenerator.getInstance().generateStringUUID();
         ServerSession session = server.createSession(name, connection.getLogin(), connection.getPasscode(), ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, connection, false, false, false, false, null, stompSession, null, true);
         stompSession.setServerSession(session);
         transactedSessions.put(txID, stompSession);
      }
      server.getStorageManager().setContext(stompSession.getContext());
      return stompSession;
   }

   public void cleanup(final StompConnection connection) {
      connection.setValid(false);

      // Close the session outside of the lock on the StompConnection, otherwise it could dead lock
      this.executor.execute(new Runnable() {
         @Override
         public void run() {
            StompSession session = sessions.remove(connection.getID());
            if (session != null) {
               try {
                  session.getSession().stop();
                  session.getSession().rollback(true);
                  session.getSession().close(false);
               }
               catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.errorCleaningStompConn(e);
               }
            }

            // removed the transacted session belonging to the connection
            Iterator<Entry<String, StompSession>> iterator = transactedSessions.entrySet().iterator();
            while (iterator.hasNext()) {
               Map.Entry<String, StompSession> entry = iterator.next();
               if (entry.getValue().getConnection() == connection) {
                  ServerSession serverSession = entry.getValue().getSession();
                  try {
                     serverSession.rollback(true);
                     serverSession.close(false);
                  }
                  catch (Exception e) {
                     ActiveMQServerLogger.LOGGER.errorCleaningStompConn(e);
                  }
                  iterator.remove();
               }
            }
         }
      });
   }

   public void sendReply(final StompConnection connection, final StompFrame frame) {
      server.getStorageManager().afterCompleteOperations(new IOCallback() {
         @Override
         public void onError(final int errorCode, final String errorMessage) {
            ActiveMQServerLogger.LOGGER.errorProcessingIOCallback(errorCode, errorMessage);

            ActiveMQStompException e = new ActiveMQStompException("Error sending reply", ActiveMQExceptionType.createException(errorCode, errorMessage)).setHandler(connection.getFrameHandler());

            StompFrame error = e.getFrame();
            send(connection, error);
         }

         @Override
         public void done() {
            send(connection, frame);
         }
      });
   }

   public String getSupportedVersionsAsString() {
      String versions = "";
      for (StompVersions version : StompVersions.values()) {
         versions += " v" + version;
      }
      return versions.substring(1);
   }

   public String getSupportedVersionsAsErrorVersion() {
      String versions = "";
      for (StompVersions version : StompVersions.values()) {
         versions += "," + version;
      }
      return versions.substring(1);
   }

   public String getVirtualHostName() {
      return "activemq";
   }

   public boolean validateUser(String login, String passcode) {
      boolean validated = true;

      ActiveMQSecurityManager sm = server.getSecurityManager();

      if (sm != null && server.getConfiguration().isSecurityEnabled()) {
         validated = sm.validateUser(login, passcode);
      }

      return validated;
   }

   public ServerMessageImpl createServerMessage() {
      return new ServerMessageImpl(server.getStorageManager().generateID(), 512);
   }

   public void commitTransaction(StompConnection connection, String txID) throws Exception {
      StompSession session = getTransactedSession(connection, txID);
      if (session == null) {
         throw new ActiveMQStompException(connection, "No transaction started: " + txID);
      }
      transactedSessions.remove(txID);
      session.getSession().commit();
   }

   public void abortTransaction(StompConnection connection, String txID) throws Exception {
      StompSession session = getTransactedSession(connection, txID);
      if (session == null) {
         throw new ActiveMQStompException(connection, "No transaction started: " + txID);
      }
      transactedSessions.remove(txID);
      session.getSession().rollback(false);
   }
   // Inner classes -------------------------------------------------

   public void createSubscription(StompConnection connection,
                                  String subscriptionID,
                                  String durableSubscriptionName,
                                  String destination,
                                  String selector,
                                  String ack,
                                  boolean noLocal) throws Exception {
      StompSession stompSession = getSession(connection);
      stompSession.setNoLocal(noLocal);
      if (stompSession.containsSubscription(subscriptionID)) {
         throw new ActiveMQStompException(connection, "There already is a subscription for: " + subscriptionID +
            ". Either use unique subscription IDs or do not create multiple subscriptions for the same destination");
      }
      long consumerID = server.getStorageManager().generateID();
      String clientID = (connection.getClientID() != null) ? connection.getClientID() : null;
      stompSession.addSubscription(consumerID, subscriptionID, clientID, durableSubscriptionName, destination, selector, ack);
   }

   public void unsubscribe(StompConnection connection,
                           String subscriptionID,
                           String durableSubscriberName) throws Exception {
      StompSession stompSession = getSession(connection);
      boolean unsubscribed = stompSession.unsubscribe(subscriptionID, durableSubscriberName);
      if (!unsubscribed) {
         throw new ActiveMQStompException(connection, "Cannot unsubscribe as no subscription exists for id: " + subscriptionID);
      }
   }

   public void acknowledge(StompConnection connection, String messageID, String subscriptionID) throws Exception {
      StompSession stompSession = getSession(connection);
      stompSession.acknowledge(messageID, subscriptionID);
   }

   public void beginTransaction(StompConnection connection, String txID) throws Exception {
      ActiveMQServerLogger.LOGGER.stompBeginTX(txID);
      if (transactedSessions.containsKey(txID)) {
         ActiveMQServerLogger.LOGGER.stompErrorTXExists(txID);
         throw new ActiveMQStompException(connection, "Transaction already started: " + txID);
      }
      // create the transacted session
      getTransactedSession(connection, txID);
   }

   public boolean destinationExists(String destination) {
      return destinations.contains(destination);
   }

   @Override
   public void onNotification(Notification notification) {
      if (!(notification.getType() instanceof CoreNotificationType))
         return;

      CoreNotificationType type = (CoreNotificationType) notification.getType();

      TypedProperties props = notification.getProperties();

      switch (type) {
         case BINDING_ADDED: {
            if (!props.containsProperty(ManagementHelper.HDR_BINDING_TYPE)) {
               throw ActiveMQMessageBundle.BUNDLE.bindingTypeNotSpecified();
            }

            Integer bindingType = props.getIntProperty(ManagementHelper.HDR_BINDING_TYPE);

            if (bindingType == BindingType.DIVERT_INDEX) {
               return;
            }

            SimpleString address = props.getSimpleStringProperty(ManagementHelper.HDR_ADDRESS);

            destinations.add(address.toString());

            break;
         }
         case BINDING_REMOVED: {
            SimpleString address = props.getSimpleStringProperty(ManagementHelper.HDR_ADDRESS);
            destinations.remove(address.toString());
            break;
         }
         default:
            //ignore all others
            break;
      }
   }

   public ActiveMQServer getServer() {
      return server;
   }

   private void invokeInterceptors(List<StompFrameInterceptor> interceptors,
                                   final StompFrame frame,
                                   final StompConnection connection) {
      if (interceptors != null && !interceptors.isEmpty()) {
         for (StompFrameInterceptor interceptor : interceptors) {
            try {
               if (!interceptor.intercept(frame, connection)) {
                  break;
               }
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.error(e);
            }
         }
      }
   }
}
