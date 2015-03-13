/**
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
package org.apache.activemq.core.protocol.stomp;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;

import io.netty.channel.ChannelPipeline;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQExceptionType;
import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.management.CoreNotificationType;
import org.apache.activemq.api.core.management.ManagementHelper;
import org.apache.activemq.core.journal.IOAsyncTask;
import org.apache.activemq.core.postoffice.BindingType;
import org.apache.activemq.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.core.server.ActiveMQMessageBundle;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServerLogger;
import org.apache.activemq.core.server.ServerSession;
import org.apache.activemq.core.server.impl.ServerMessageImpl;
import org.apache.activemq.core.server.management.ManagementService;
import org.apache.activemq.core.server.management.Notification;
import org.apache.activemq.core.server.management.NotificationListener;
import org.apache.activemq.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.spi.core.protocol.MessageConverter;
import org.apache.activemq.spi.core.protocol.ProtocolManager;
import org.apache.activemq.spi.core.protocol.RemotingConnection;
import org.apache.activemq.spi.core.remoting.Acceptor;
import org.apache.activemq.spi.core.remoting.Connection;
import org.apache.activemq.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.utils.ConcurrentHashSet;
import org.apache.activemq.utils.TypedProperties;
import org.apache.activemq.utils.UUIDGenerator;

import static org.apache.activemq.core.protocol.stomp.ActiveMQStompProtocolMessageBundle.BUNDLE;

/**
 * StompProtocolManager
 */
class StompProtocolManager implements ProtocolManager, NotificationListener
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ActiveMQServer server;

   private final Executor executor;

   private final Map<String, StompSession> transactedSessions = new HashMap<String, StompSession>();

   // key => connection ID, value => Stomp session
   private final Map<Object, StompSession> sessions = new HashMap<Object, StompSession>();

   private final Set<String> destinations = new ConcurrentHashSet<String>();

   private final List<Interceptor> incomingInterceptors;
   private final List<Interceptor> outgoingInterceptors;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public StompProtocolManager(final ActiveMQServer server, final List<Interceptor> incomingInterceptors, final List<Interceptor> outgoingInterceptors)
   {
      this.server = server;
      this.executor = server.getExecutorFactory().getExecutor();
      ManagementService service = server.getManagementService();
      if (service != null)
      {
         //allow management message to pass
         destinations.add(service.getManagementAddress().toString());
         service.addNotificationListener(this);
      }
      this.incomingInterceptors = incomingInterceptors;
      this.outgoingInterceptors = outgoingInterceptors;
   }

   @Override
   public MessageConverter getConverter()
   {
      return null;
   }

   // ProtocolManager implementation --------------------------------

   public ConnectionEntry createConnectionEntry(final Acceptor acceptorUsed, final Connection connection)
   {
      StompConnection conn = new StompConnection(acceptorUsed, connection, this);

      // Note that STOMP 1.0 has no heartbeat, so if connection ttl is non zero, data must continue to be sent or connection
      // will be timed out and closed!

      String ttlStr = (String) acceptorUsed.getConfiguration().get(TransportConstants.CONNECTION_TTL);
      Long ttl = ttlStr == null ? null : Long.valueOf(ttlStr);

      if (ttl != null)
      {
         if (ttl > 0)
         {
            return new ConnectionEntry(conn, null, System.currentTimeMillis(), ttl);
         }
         throw BUNDLE.negativeConnectionTTL(ttl);
      }

      ttl = server.getConfiguration().getConnectionTTLOverride();

      if (ttl != -1)
      {
         return new ConnectionEntry(conn, null, System.currentTimeMillis(), ttl);
      }
      else
      {
         // Default to 1 minute - which is same as core protocol
         return new ConnectionEntry(conn, null, System.currentTimeMillis(), 1 * 60 * 1000);
      }
   }

   public void removeHandler(String name)
   {
   }

   public void handleBuffer(final RemotingConnection connection, final ActiveMQBuffer buffer)
   {
      StompConnection conn = (StompConnection) connection;

      conn.setDataReceived();

      do
      {
         StompFrame request;
         try
         {
            request = conn.decode(buffer);
         }
         catch (Exception e)
         {
            ActiveMQServerLogger.LOGGER.errorDecodingPacket(e);
            return;
         }

         if (request == null)
         {
            break;
         }

         try
         {
            invokeInterceptors(this.incomingInterceptors, request, conn);
            conn.handleFrame(request);
         }
         finally
         {
            server.getStorageManager().clearContext();
         }
      } while (conn.hasBytes());
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline)
   {
   }

   @Override
   public boolean isProtocol(byte[] array)
   {
      String frameStart = new String(array, StandardCharsets.US_ASCII);
      return frameStart.startsWith(StompCommands.CONNECT.name()) || frameStart.startsWith(StompCommands.STOMP.name());
   }

   @Override
   public void handshake(NettyServerConnection connection, ActiveMQBuffer buffer)
   {
      //Todo move handshake to here
   }

   // Public --------------------------------------------------------

   public boolean send(final StompConnection connection, final StompFrame frame)
   {
      if (ActiveMQServerLogger.LOGGER.isTraceEnabled())
      {
         ActiveMQServerLogger.LOGGER.trace("sent " + frame);
      }

      invokeInterceptors(this.outgoingInterceptors, frame, connection);

      synchronized (connection)
      {
         if (connection.isDestroyed())
         {
            ActiveMQStompProtocolLogger.LOGGER.connectionClosed(connection);
            return false;
         }

         try
         {
            connection.physicalSend(frame);
         }
         catch (Exception e)
         {
            ActiveMQStompProtocolLogger.LOGGER.errorSendingFrame(e, frame);
            return false;
         }
         return true;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   public StompSession getSession(StompConnection connection) throws Exception
   {
      StompSession stompSession = sessions.get(connection.getID());
      if (stompSession == null)
      {
         stompSession = new StompSession(connection, this, server.getStorageManager()
            .newContext(server.getExecutorFactory().getExecutor()));
         String name = UUIDGenerator.getInstance().generateStringUUID();
         ServerSession session = server.createSession(name,
                                                      connection.getLogin(),
                                                      connection.getPasscode(),
                                                      ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                      connection,
                                                      true,
                                                      false,
                                                      false,
                                                      false,
                                                      null,
                                                      stompSession, null);
         stompSession.setServerSession(session);
         sessions.put(connection.getID(), stompSession);
      }
      server.getStorageManager().setContext(stompSession.getContext());
      return stompSession;
   }

   public StompSession getTransactedSession(StompConnection connection, String txID) throws Exception
   {
      StompSession stompSession = transactedSessions.get(txID);
      if (stompSession == null)
      {
         stompSession = new StompSession(connection, this, server.getStorageManager().newContext(executor));
         String name = UUIDGenerator.getInstance().generateStringUUID();
         ServerSession session = server.createSession(name,
                                                      connection.getLogin(),
                                                      connection.getPasscode(),
                                                      ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                      connection,
                                                      false,
                                                      false,
                                                      false,
                                                      false,
                                                      null,
                                                      stompSession, null);
         stompSession.setServerSession(session);
         transactedSessions.put(txID, stompSession);
      }
      server.getStorageManager().setContext(stompSession.getContext());
      return stompSession;
   }

   public void cleanup(final StompConnection connection)
   {
      connection.setValid(false);

      // Close the session outside of the lock on the StompConnection, otherwise it could dead lock
      this.executor.execute(new Runnable()
      {
         public void run()
         {
            StompSession session = sessions.remove(connection.getID());
            if (session != null)
            {
               try
               {
                  session.getSession().stop();
                  session.getSession().rollback(true);
                  session.getSession().close(false);
               }
               catch (Exception e)
               {
                  ActiveMQServerLogger.LOGGER.errorCleaningStompConn(e);
               }
            }

            // removed the transacted session belonging to the connection
            Iterator<Entry<String, StompSession>> iterator = transactedSessions.entrySet().iterator();
            while (iterator.hasNext())
            {
               Map.Entry<String, StompSession> entry = iterator.next();
               if (entry.getValue().getConnection() == connection)
               {
                  ServerSession serverSession = entry.getValue().getSession();
                  try
                  {
                     serverSession.rollback(true);
                     serverSession.close(false);
                  }
                  catch (Exception e)
                  {
                     ActiveMQServerLogger.LOGGER.errorCleaningStompConn(e);
                  }
                  iterator.remove();
               }
            }
         }
      });
   }

   public void sendReply(final StompConnection connection, final StompFrame frame)
   {
      server.getStorageManager().afterCompleteOperations(new IOAsyncTask()
      {
         public void onError(final int errorCode, final String errorMessage)
         {
            ActiveMQServerLogger.LOGGER.errorProcessingIOCallback(errorCode, errorMessage);

            ActiveMQStompException e = new ActiveMQStompException("Error sending reply",
                                                                ActiveMQExceptionType.createException(errorCode, errorMessage));

            StompFrame error = e.getFrame();
            send(connection, error);
         }

         public void done()
         {
            send(connection, frame);
         }
      });
   }

   public String getSupportedVersionsAsString()
   {
      return "v1.0 v1.1 v1.2";
   }

   public String getVirtualHostName()
   {
      return "activemq";
   }

   public boolean validateUser(String login, String passcode)
   {
      boolean validated = true;

      ActiveMQSecurityManager sm = server.getSecurityManager();

      if (sm != null && server.getConfiguration().isSecurityEnabled())
      {
         validated = sm.validateUser(login, passcode);
      }

      return validated;
   }

   public ServerMessageImpl createServerMessage()
   {
      return new ServerMessageImpl(server.getStorageManager().generateID(), 512);
   }

   public void commitTransaction(StompConnection connection, String txID) throws Exception
   {
      StompSession session = getTransactedSession(connection, txID);
      if (session == null)
      {
         throw new ActiveMQStompException("No transaction started: " + txID);
      }
      transactedSessions.remove(txID);
      session.getSession().commit();
   }

   public void abortTransaction(StompConnection connection, String txID) throws Exception
   {
      StompSession session = getTransactedSession(connection, txID);
      if (session == null)
      {
         throw new ActiveMQStompException("No transaction started: " + txID);
      }
      transactedSessions.remove(txID);
      session.getSession().rollback(false);
   }
   // Inner classes -------------------------------------------------

   public void createSubscription(StompConnection connection,
                                  String subscriptionID, String durableSubscriptionName,
                                  String destination, String selector, String ack, boolean noLocal) throws Exception
   {
      StompSession stompSession = getSession(connection);
      stompSession.setNoLocal(noLocal);
      if (stompSession.containsSubscription(subscriptionID))
      {
         throw new ActiveMQStompException("There already is a subscription for: " + subscriptionID +
                                            ". Either use unique subscription IDs or do not create multiple subscriptions for the same destination");
      }
      long consumerID = server.getStorageManager().generateID();
      String clientID = (connection.getClientID() != null) ? connection.getClientID() : null;
      stompSession.addSubscription(consumerID,
                                   subscriptionID,
                                   clientID,
                                   durableSubscriptionName,
                                   destination,
                                   selector,
                                   ack);
   }

   public void unsubscribe(StompConnection connection,
                           String subscriptionID, String durableSubscriberName) throws Exception
   {
      StompSession stompSession = getSession(connection);
      boolean unsubscribed = stompSession.unsubscribe(subscriptionID, durableSubscriberName);
      if (!unsubscribed)
      {
         throw new ActiveMQStompException("Cannot unsubscribe as no subscription exists for id: " + subscriptionID);
      }
   }

   public void acknowledge(StompConnection connection, String messageID, String subscriptionID) throws Exception
   {
      StompSession stompSession = getSession(connection);
      stompSession.acknowledge(messageID, subscriptionID);
   }

   public void beginTransaction(StompConnection connection, String txID) throws Exception
   {
      ActiveMQServerLogger.LOGGER.stompBeginTX(txID);
      if (transactedSessions.containsKey(txID))
      {
         ActiveMQServerLogger.LOGGER.stompErrorTXExists(txID);
         throw new ActiveMQStompException(connection, "Transaction already started: " + txID);
      }
      // create the transacted session
      getTransactedSession(connection, txID);
   }

   public boolean destinationExists(String destination)
   {
      return destinations.contains(destination);
   }

   @Override
   public void onNotification(Notification notification)
   {
      if (!(notification.getType() instanceof CoreNotificationType)) return;

      CoreNotificationType type = (CoreNotificationType) notification.getType();

      TypedProperties props = notification.getProperties();

      switch (type)
      {
         case BINDING_ADDED:
         {
            if (!props.containsProperty(ManagementHelper.HDR_BINDING_TYPE))
            {
               throw ActiveMQMessageBundle.BUNDLE.bindingTypeNotSpecified();
            }

            Integer bindingType = props.getIntProperty(ManagementHelper.HDR_BINDING_TYPE);

            if (bindingType == BindingType.DIVERT_INDEX)
            {
               return;
            }

            SimpleString address = props.getSimpleStringProperty(ManagementHelper.HDR_ADDRESS);

            destinations.add(address.toString());

            break;
         }
         case BINDING_REMOVED:
         {
            SimpleString address = props.getSimpleStringProperty(ManagementHelper.HDR_ADDRESS);
            destinations.remove(address.toString());
            break;
         }
         default:
            //ignore all others
            break;
      }
   }

   public ActiveMQServer getServer()
   {
      return server;
   }

   private void invokeInterceptors(List<Interceptor> interceptors, final StompFrame frame, final StompConnection connection)
   {
      if (interceptors != null && !interceptors.isEmpty())
      {
         for (Interceptor interceptor : interceptors)
         {
            if (interceptor instanceof StompFrameInterceptor)
            {
               try
               {
                  if (!((StompFrameInterceptor)interceptor).intercept(frame, connection))
                  {
                     break;
                  }
               }
               catch (Exception e)
               {
                  ActiveMQServerLogger.LOGGER.error(e);
               }
            }
         }
      }
   }
}
