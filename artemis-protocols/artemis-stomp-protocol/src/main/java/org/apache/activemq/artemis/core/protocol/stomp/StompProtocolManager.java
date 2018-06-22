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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.spi.core.protocol.AbstractProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.UUIDGenerator;

import static org.apache.activemq.artemis.core.protocol.stomp.ActiveMQStompProtocolMessageBundle.BUNDLE;

/**
 * StompProtocolManager
 */
public class StompProtocolManager extends AbstractProtocolManager<StompFrame, StompFrameInterceptor, StompConnection> {

   private static final List<String> websocketRegistryNames = Arrays.asList("v10.stomp", "v11.stomp", "v12.stomp");

   private final ActiveMQServer server;

   private final StompProtocolManagerFactory factory;

   private final Executor executor;

   private final Map<Object, StompSession> transactedSessions = new HashMap<>();

   // key => connection ID, value => Stomp session
   private final Map<Object, StompSession> sessions = new HashMap<>();

   private final List<StompFrameInterceptor> incomingInterceptors;
   private final List<StompFrameInterceptor> outgoingInterceptors;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   StompProtocolManager(final StompProtocolManagerFactory factory,
                        final ActiveMQServer server,
                        final List<StompFrameInterceptor> incomingInterceptors,
                        final List<StompFrameInterceptor> outgoingInterceptors) {
      this.factory = factory;
      this.server = server;
      this.executor = server.getExecutorFactory().getExecutor();
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
   public ConnectionEntry createConnectionEntry(final Acceptor acceptorUsed, final Connection connection) {
      StompConnection conn = new StompConnection(acceptorUsed, connection, this, server.getScheduledPool(), server.getExecutorFactory());

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
      } else {
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
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorDecodingPacket(e);
            return;
         }

         if (request == null) {
            break;
         }

         try {
            invokeInterceptors(this.incomingInterceptors, request, conn);
            conn.logFrame(request, true);
            conn.handleFrame(request);
         } finally {
            server.getStorageManager().clearContext();
         }
      }
      while (conn.hasBytes());
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

   @Override
   public List<String> websocketSubprotocolIdentifiers() {
      return websocketRegistryNames;
   }

   // Public --------------------------------------------------------

   public boolean send(final StompConnection connection, final StompFrame frame) {
      invokeInterceptors(this.outgoingInterceptors, frame, connection);
      connection.logFrame(frame, false);

      synchronized (connection) {
         if (connection.isDestroyed()) {
            ActiveMQStompProtocolLogger.LOGGER.connectionClosed(connection);
            return false;
         }

         try {
            connection.physicalSend(frame);
         } catch (Exception e) {
            ActiveMQStompProtocolLogger.LOGGER.errorSendingFrame(e, frame);
            return false;
         }
         return true;
      }
   }

   public StompSession getSession(StompConnection connection) throws Exception {
      return internalGetSession(connection, sessions, connection.getID(), false);
   }

   public StompSession getTransactedSession(StompConnection connection, String txID) throws Exception {
      return internalGetSession(connection, transactedSessions, txID, true);
   }

   private StompSession internalGetSession(StompConnection connection, Map<Object, StompSession> sessions, Object id, boolean transacted) throws Exception {
      StompSession stompSession = sessions.get(id);
      if (stompSession == null) {
         stompSession = new StompSession(connection, this, server.getStorageManager().newContext(server.getExecutorFactory().getExecutor()));
         String name = UUIDGenerator.getInstance().generateStringUUID();
         ServerSession session = server.createSession(name, connection.getLogin(), connection.getPasscode(), ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, connection, !transacted, false, false, false, null, stompSession, true, server.newOperationContext(), getPrefixes());
         stompSession.setServerSession(session);
         sessions.put(id, stompSession);
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
                  session.getCoreSession().stop();
                  session.getCoreSession().rollback(true);
                  session.getCoreSession().close(false);
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.errorCleaningStompConn(e);
               }
            }

            // removed the transacted session belonging to the connection
            Iterator<Entry<Object, StompSession>> iterator = transactedSessions.entrySet().iterator();
            while (iterator.hasNext()) {
               Map.Entry<Object, StompSession> entry = iterator.next();
               if (entry.getValue().getConnection() == connection) {
                  ServerSession serverSession = entry.getValue().getCoreSession();
                  try {
                     serverSession.rollback(true);
                     serverSession.close(false);
                  } catch (Exception e) {
                     ActiveMQServerLogger.LOGGER.errorCleaningStompConn(e);
                  }
                  iterator.remove();
               }
            }
         }
      });
   }

   public void sendReply(final StompConnection connection, final StompFrame frame, final StompPostReceiptFunction function) {
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
            if (frame != null) {
               send(connection, frame);
            }

            if (function != null) {
               function.afterReceipt();
            }
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

   public CoreMessage createServerMessage() {
      return new CoreMessage(server.getStorageManager().generateID(), 512);
   }

   public void commitTransaction(StompConnection connection, String txID) throws Exception {
      StompSession session = getTransactedSession(connection, txID);
      if (session == null) {
         throw new ActiveMQStompException(connection, "No transaction started: " + txID);
      }
      transactedSessions.remove(txID);
      session.getCoreSession().commit();
   }

   public void abortTransaction(StompConnection connection, String txID) throws Exception {
      StompSession session = getTransactedSession(connection, txID);
      if (session == null) {
         throw new ActiveMQStompException(connection, "No transaction started: " + txID);
      }
      transactedSessions.remove(txID);
      session.getCoreSession().rollback(false);
   }
   // Inner classes -------------------------------------------------

   public StompPostReceiptFunction subscribe(StompConnection connection,
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
      return stompSession.addSubscription(consumerID, subscriptionID, connection.getClientID(), durableSubscriptionName, destination, selector, ack);
   }

   public void unsubscribe(StompConnection connection,
                           String subscriptionID,
                           String durableSubscriberName) throws Exception {
      StompSession stompSession = getSession(connection);
      boolean unsubscribed = stompSession.unsubscribe(subscriptionID, durableSubscriberName, connection.getClientID());
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
      if (server.getManagementService().getManagementAddress().toString().equals(destination)) {
         return true;
      }
      return server.getPostOffice().getAddressInfo(SimpleString.toSimpleString(destination)) != null;
   }

   public ActiveMQServer getServer() {
      return server;
   }
}
