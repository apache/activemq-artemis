/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.client.impl;

import java.lang.ref.WeakReference;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.FailoverEventListener;
import org.hornetq.api.core.client.FailoverEventType;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.client.HornetQClientMessageBundle;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.impl.HornetQClientProtocolManager;
import org.hornetq.core.protocol.core.impl.PacketDecoder;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.ClientProtocolManager;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.spi.core.remoting.Connector;
import org.hornetq.spi.core.remoting.ConnectorFactory;
import org.hornetq.spi.core.remoting.ProtocolResponseHandler;
import org.hornetq.spi.core.remoting.SessionContext;
import org.hornetq.utils.ClassloadingUtil;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.ConfirmationWindowWarning;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.UUIDGenerator;

/**
 * @author Tim Fox
 * @author Clebert Suconic
 */

public class ClientSessionFactoryImpl implements ClientSessionFactoryInternal, ConnectionLifeCycleListener
{


   // TODO use the factory here
   protected ClientProtocolManager clientProtocolManager = new HornetQClientProtocolManager(this);

   // Constants
   // ------------------------------------------------------------------------------------

   private static final boolean isTrace = HornetQClientLogger.LOGGER.isTraceEnabled();

   private static final boolean isDebug = HornetQClientLogger.LOGGER.isDebugEnabled();

   // Attributes
   // -----------------------------------------------------------------------------------

   private final ServerLocatorInternal serverLocator;

   private TransportConfiguration connectorConfig;

   private TransportConfiguration backupConfig;

   private ConnectorFactory connectorFactory;

   private transient boolean finalizeCheck = true;

   private final long callTimeout;

   private final long callFailoverTimeout;

   private final long clientFailureCheckPeriod;

   private final long connectionTTL;

   private final Set<ClientSessionInternal> sessions = new HashSet<ClientSessionInternal>();

   private final Object createSessionLock = new Object();

   private final Lock newFailoverLock = new ReentrantLock();


   private final Object connectionLock = new Object();

   private final ExecutorFactory orderedExecutorFactory;

   private final Executor threadPool;

   private final ScheduledExecutorService scheduledThreadPool;

   private final Executor closeExecutor;

   private RemotingConnection connection;

   private final long retryInterval;

   private final double retryIntervalMultiplier; // For exponential backoff

   private final long maxRetryInterval;

   private int reconnectAttempts;

   private final Set<SessionFailureListener> listeners = new ConcurrentHashSet<SessionFailureListener>();

   private final Set<FailoverEventListener> failoverListeners = new ConcurrentHashSet<FailoverEventListener>();

   private Connector connector;

   private Future<?> pingerFuture;
   private PingRunnable pingRunnable;


   private final List<Interceptor> incomingInterceptors;

   private final List<Interceptor> outgoingInterceptors;

   private volatile boolean stopPingingAfterOne;

   private volatile boolean closed;

   public final Exception createTrace;

   public static final Set<CloseRunnable> CLOSE_RUNNABLES = Collections.synchronizedSet(new HashSet<CloseRunnable>());

   private final ConfirmationWindowWarning confirmationWindowWarning;

   private String liveNodeID;


   public ClientSessionFactoryImpl(final ServerLocatorInternal serverLocator,
                                   final TransportConfiguration connectorConfig,
                                   final long callTimeout,
                                   final long callFailoverTimeout,
                                   final long clientFailureCheckPeriod,
                                   final long connectionTTL,
                                   final long retryInterval,
                                   final double retryIntervalMultiplier,
                                   final long maxRetryInterval,
                                   final int reconnectAttempts,
                                   final Executor threadPool,
                                   final ScheduledExecutorService scheduledThreadPool,
                                   final List<Interceptor> incomingInterceptors,
                                   final List<Interceptor> outgoingInterceptors,
                                   PacketDecoder packetDecoder)
   {
      createTrace = new Exception();

      this.serverLocator = serverLocator;

      this.connectorConfig = connectorConfig;

      connectorFactory = instantiateConnectorFactory(connectorConfig.getFactoryClassName());

      checkTransportKeys(connectorFactory, connectorConfig.getParams());

      this.callTimeout = callTimeout;

      this.callFailoverTimeout = callFailoverTimeout;

      // HORNETQ-1314 - if this in an in-vm connection then disable connection monitoring
      if (connectorFactory.isReliable() &&
         clientFailureCheckPeriod == HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD &&
         connectionTTL == HornetQClient.DEFAULT_CONNECTION_TTL)
      {
         this.clientFailureCheckPeriod = HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD_INVM;
         this.connectionTTL = HornetQClient.DEFAULT_CONNECTION_TTL_INVM;
      }
      else
      {
         this.clientFailureCheckPeriod = clientFailureCheckPeriod;

         this.connectionTTL = connectionTTL;
      }

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetryInterval = maxRetryInterval;

      this.reconnectAttempts = reconnectAttempts;

      this.scheduledThreadPool = scheduledThreadPool;

      this.threadPool = threadPool;

      orderedExecutorFactory = new OrderedExecutorFactory(threadPool);

      closeExecutor = orderedExecutorFactory.getExecutor();

      this.incomingInterceptors = incomingInterceptors;

      this.outgoingInterceptors = outgoingInterceptors;

      confirmationWindowWarning = new ConfirmationWindowWarning(serverLocator.getConfirmationWindowSize() < 0);


      // TODO : Get rid of this / encapsulate it through the ClientProtocolManager (create a ExchangeServerProtocol for instance)
      ((HornetQClientProtocolManager) clientProtocolManager).replacePacketDecoder(packetDecoder);

   }

   public void disableFinalizeCheck()
   {
      finalizeCheck = false;
   }

   public Lock lockFailover()
   {
      newFailoverLock.lock();
      return newFailoverLock;
   }

   public void connect(final int initialConnectAttempts, final boolean failoverOnInitialConnection) throws HornetQException
   {
      // Get the connection
      getConnectionWithRetry(initialConnectAttempts);

      if (connection == null)
      {
         StringBuilder msg =
            new StringBuilder("Unable to connect to server using configuration ").append(connectorConfig);
         if (backupConfig != null)
         {
            msg.append(" and backup configuration ").append(backupConfig);
         }
         throw new HornetQNotConnectedException(msg.toString());
      }

   }

   public TransportConfiguration getConnectorConfiguration()
   {
      return connectorConfig;
   }

   public void setBackupConnector(final TransportConfiguration live, final TransportConfiguration backUp)
   {
      Connector localConnector = connector;

      // if the connector has never been used (i.e. the getConnection hasn't been called yet), we will need
      // to create a connector just to validate if the parameters are ok.
      // so this will create the instance to be used on the isEquivalent check
      if (localConnector == null)
      {
         localConnector = connectorFactory.createConnector(connectorConfig.getParams(),
                                                           new DelegatingBufferHandler(),
                                                           this,
                                                           closeExecutor,
                                                           threadPool,
                                                           scheduledThreadPool);
      }

      if (localConnector.isEquivalent(live.getParams()) && backUp != null && !localConnector.isEquivalent(backUp.getParams()))
      {
         if (ClientSessionFactoryImpl.isDebug)
         {
            HornetQClientLogger.LOGGER.debug("Setting up backup config = " + backUp + " for live = " + live);
         }
         backupConfig = backUp;
      }
      else
      {
         if (ClientSessionFactoryImpl.isDebug)
         {
            HornetQClientLogger.LOGGER.debug("ClientSessionFactoryImpl received backup update for live/backup pair = " + live +
                                                " / " +
                                                backUp +
                                                " but it didn't belong to " +
                                                connectorConfig);
         }
      }
   }

   public Object getBackupConnector()
   {
      return backupConfig;
   }

   public ClientSession createSession(final String username,
                                      final String password,
                                      final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge,
                                      final int ackBatchSize) throws HornetQException
   {
      return createSessionInternal(username,
                                   password,
                                   xa,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   preAcknowledge,
                                   ackBatchSize);
   }

   public ClientSession createSession(final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final int ackBatchSize) throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   false,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   serverLocator.isPreAcknowledge(),
                                   ackBatchSize);
   }

   public ClientSession createXASession() throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   true,
                                   false,
                                   false,
                                   serverLocator.isPreAcknowledge(),
                                   serverLocator.getAckBatchSize());
   }

   public ClientSession createTransactedSession() throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   false,
                                   false,
                                   false,
                                   serverLocator.isPreAcknowledge(),
                                   serverLocator.getAckBatchSize());
   }

   public ClientSession createSession() throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   false,
                                   true,
                                   true,
                                   serverLocator.isPreAcknowledge(),
                                   serverLocator.getAckBatchSize());
   }

   public ClientSession createSession(final boolean autoCommitSends, final boolean autoCommitAcks) throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   false,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   serverLocator.isPreAcknowledge(),
                                   serverLocator.getAckBatchSize());
   }

   public ClientSession createSession(final boolean xa, final boolean autoCommitSends, final boolean autoCommitAcks) throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   xa,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   serverLocator.isPreAcknowledge(),
                                   serverLocator.getAckBatchSize());
   }

   public ClientSession createSession(final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge) throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   xa,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   preAcknowledge,
                                   serverLocator.getAckBatchSize());
   }

   // ConnectionLifeCycleListener implementation --------------------------------------------------

   public void connectionCreated(final HornetQComponent component, final Connection connection, final String protocol)
   {
   }

   public void connectionDestroyed(final Object connectionID)
   {
      // The exception has to be created in the same thread where it's being called
      // as to avoid a different stack trace cause
      final HornetQException ex = HornetQClientMessageBundle.BUNDLE.channelDisconnected();

      // It has to use the same executor as the disconnect message is being sent through

      closeExecutor.execute(new Runnable()
      {
         public void run()
         {
            handleConnectionFailure(connectionID, ex);
         }
      });

   }

   public void connectionException(final Object connectionID, final HornetQException me)
   {
      handleConnectionFailure(connectionID, me);
   }

   // Must be synchronized to prevent it happening concurrently with failover which can lead to
   // inconsistencies
   public void removeSession(final ClientSessionInternal session, final boolean failingOver)
   {
      synchronized (sessions)
      {
         sessions.remove(session);
      }
   }

   public void connectionReadyForWrites(final Object connectionID, final boolean ready)
   {
   }

   public synchronized int numConnections()
   {
      return connection != null ? 1 : 0;
   }

   public int numSessions()
   {
      return sessions.size();
   }

   public void addFailureListener(final SessionFailureListener listener)
   {
      listeners.add(listener);
   }

   public boolean removeFailureListener(final SessionFailureListener listener)
   {
      return listeners.remove(listener);
   }

   public void addFailoverListener(FailoverEventListener listener)
   {
      failoverListeners.add(listener);
   }

   public boolean removeFailoverListener(FailoverEventListener listener)
   {
      return failoverListeners.remove(listener);
   }

   public void causeExit()
   {
      clientProtocolManager.stop();
   }

   private void interruptConnectAndCloseAllSessions(boolean close)
   {
      clientProtocolManager.stop();

      synchronized (createSessionLock)
      {
         closeCleanSessions(close);
         closed = true;
      }
   }

   /**
    * @param close
    */
   private void closeCleanSessions(boolean close)
   {
      HashSet<ClientSessionInternal> sessionsToClose;
      synchronized (sessions)
      {
         sessionsToClose = new HashSet<ClientSessionInternal>(sessions);
      }
      // work on a copied set. the session will be removed from sessions when session.close() is
      // called
      for (ClientSessionInternal session : sessionsToClose)
      {
         try
         {
            if (close)
               session.close();
            else
               session.cleanUp(false);
         }
         catch (Exception e1)
         {
            HornetQClientLogger.LOGGER.unableToCloseSession(e1);
         }
      }
      checkCloseConnection();
   }

   public void close()
   {
      if (closed)
      {
         return;
      }
      interruptConnectAndCloseAllSessions(true);

      serverLocator.factoryClosed(this);
   }

   public void cleanup()
   {
      if (closed)
      {
         return;
      }

      interruptConnectAndCloseAllSessions(false);
   }

   public boolean isClosed()
   {
      return closed || serverLocator.isClosed();
   }

   @Override
   public ServerLocator getServerLocator()
   {
      return serverLocator;
   }

   public void stopPingingAfterOne()
   {
      stopPingingAfterOne = true;
   }

   private void handleConnectionFailure(final Object connectionID, final HornetQException me)
   {
      handleConnectionFailure(connectionID, me, null);
   }

   private void handleConnectionFailure(final Object connectionID, final HornetQException me, String scaleDownTargetNodeID)
   {
      try
      {
         failoverOrReconnect(connectionID, me, scaleDownTargetNodeID);
      }
      catch (HornetQInterruptedException e1)
      {
         // this is just a debug, since an interrupt is an expected event (in case of a shutdown)
         HornetQClientLogger.LOGGER.debug(e1.getMessage(), e1);
      }
   }

   /**
    * TODO: Maybe this belongs to HornetQClientProtocolManager
    * @param connectionID
    * @param me
    */
   private void failoverOrReconnect(final Object connectionID, final HornetQException me, String scaleDownTargetNodeID)
   {
      Set<ClientSessionInternal> sessionsToClose = null;
      if (!clientProtocolManager.isAlive())
         return;
      Lock localFailoverLock = lockFailover();
      try
      {
         if (connection == null || !connection.getID().equals(connectionID) || !clientProtocolManager.isAlive())
         {
            // We already failed over/reconnected - probably the first failure came in, all the connections were failed
            // over then a async connection exception or disconnect
            // came in for one of the already exitLoop connections, so we return true - we don't want to call the
            // listeners again

            return;
         }

         if (ClientSessionFactoryImpl.isTrace)
         {
            HornetQClientLogger.LOGGER.trace("Client Connection failed, calling failure listeners and trying to reconnect, reconnectAttempts=" + reconnectAttempts);
         }

         callFailoverListeners(FailoverEventType.FAILURE_DETECTED);
         // We call before reconnection occurs to give the user a chance to do cleanup, like cancel messages
         callSessionFailureListeners(me, false, false, scaleDownTargetNodeID);

         // Now get locks on all channel 1s, whilst holding the failoverLock - this makes sure
         // There are either no threads executing in createSession, or one is blocking on a createSession
         // result.

         // Then interrupt the channel 1 that is blocking (could just interrupt them all)

         // Then release all channel 1 locks - this allows the createSession to exit the monitor

         // Then get all channel 1 locks again - this ensures the any createSession thread has executed the section and
         // returned all its connections to the connection manager (the code to return connections to connection manager
         // must be inside the lock

         // Then perform failover

         // Then release failoverLock

         // The other side of the bargain - during createSession:
         // The calling thread must get the failoverLock and get its' connections when this is
         // locked.
         // While this is still locked it must then get the channel1 lock
         // It can then release the failoverLock
         // It should catch HornetQException.INTERRUPTED in the call to channel.sendBlocking
         // It should then return its connections, with channel 1 lock still held
         // It can then release the channel 1 lock, and retry (which will cause locking on
         // failoverLock
         // until failover is complete

         if (reconnectAttempts != 0)
         {


            if (clientProtocolManager.cleanupBeforeFailover())
            {


               // Now we absolutely know that no threads are executing in or blocked in
               // createSession,
               // and no
               // more will execute it until failover is complete

               // So.. do failover / reconnection

               RemotingConnection oldConnection = connection;

               connection = null;

               Connector localConnector = connector;
               if (localConnector != null)
               {
                  try
                  {
                     localConnector.close();
                  }
                  catch (Exception ignore)
                  {
                     // no-op
                  }
               }

               cancelScheduledTasks();

               connector = null;

               reconnectSessions(oldConnection, reconnectAttempts);

               if (oldConnection != null)
               {
                  oldConnection.destroy();
               }

               if (connection != null)
               {
                  callFailoverListeners(FailoverEventType.FAILOVER_COMPLETED);
               }
            }
         }
         else
         {
            RemotingConnection connectionToDestory = connection;
            if (connectionToDestory != null)
            {
               connectionToDestory.destroy();
            }
            connection = null;
         }

         if (connection == null)
         {
            synchronized (sessions)
            {
               sessionsToClose = new HashSet<ClientSessionInternal>(sessions);
            }
            callFailoverListeners(FailoverEventType.FAILOVER_FAILED);
            callSessionFailureListeners(me, true, false);
         }
      }
      finally
      {
         localFailoverLock.unlock();
      }

      // This needs to be outside the failover lock to prevent deadlock
      if (connection != null)
      {
         callSessionFailureListeners(me, true, true);
      }
      if (sessionsToClose != null)
      {
         // If connection is null it means we didn't succeed in failing over or reconnecting
         // so we close all the sessions, so they will throw exceptions when attempted to be used

         for (ClientSessionInternal session : sessionsToClose)
         {
            try
            {
               session.cleanUp(true);
            }
            catch (Exception cause)
            {
               HornetQClientLogger.LOGGER.failedToCleanupSession(cause);
            }
         }
      }
   }

   private ClientSession createSessionInternal(final String username,
                                               final String password,
                                               final boolean xa,
                                               final boolean autoCommitSends,
                                               final boolean autoCommitAcks,
                                               final boolean preAcknowledge,
                                               final int ackBatchSize) throws HornetQException
   {
      String name = UUIDGenerator.getInstance().generateStringUUID();


      SessionContext context = createSessionChannel(name,
                                                    username,
                                                    password,
                                                    xa,
                                                    autoCommitSends,
                                                    autoCommitAcks,
                                                    preAcknowledge);

      ClientSessionInternal session = new ClientSessionImpl(this,
                                                            name,
                                                            username,
                                                            password,
                                                            xa,
                                                            autoCommitSends,
                                                            autoCommitAcks,
                                                            preAcknowledge,
                                                            serverLocator.isBlockOnAcknowledge(),
                                                            serverLocator.isAutoGroup(),
                                                            ackBatchSize,
                                                            serverLocator.getConsumerWindowSize(),
                                                            serverLocator.getConsumerMaxRate(),
                                                            serverLocator.getConfirmationWindowSize(),
                                                            serverLocator.getProducerWindowSize(),
                                                            serverLocator.getProducerMaxRate(),
                                                            serverLocator.isBlockOnNonDurableSend(),
                                                            serverLocator.isBlockOnDurableSend(),
                                                            serverLocator.isCacheLargeMessagesClient(),
                                                            serverLocator.getMinLargeMessageSize(),
                                                            serverLocator.isCompressLargeMessage(),
                                                            serverLocator.getInitialMessagePacketSize(),
                                                            serverLocator.getGroupID(),
                                                            context,
                                                            orderedExecutorFactory.getExecutor(),
                                                            orderedExecutorFactory.getExecutor());

      context.setSession(session);

      synchronized (sessions)
      {
         if (closed || !clientProtocolManager.isAlive())
         {
            session.close();
            return null;
         }
         sessions.add(session);
      }

      return new DelegatingSession(session);

   }


   private void callSessionFailureListeners(final HornetQException me, final boolean afterReconnect,
                                            final boolean failedOver)
   {
      callSessionFailureListeners(me, afterReconnect, failedOver, null);
   }

   private void callSessionFailureListeners(final HornetQException me, final boolean afterReconnect,
                                            final boolean failedOver, final String scaleDownTargetNodeID)
   {
      final List<SessionFailureListener> listenersClone = new ArrayList<SessionFailureListener>(listeners);

      for (final SessionFailureListener listener : listenersClone)
      {
         try
         {
            if (afterReconnect)
            {
               listener.connectionFailed(me, failedOver, scaleDownTargetNodeID);
            }
            else
            {
               listener.beforeReconnect(me);
            }
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            HornetQClientLogger.LOGGER.failedToExecuteListener(t);
         }
      }
   }

   private void callFailoverListeners(FailoverEventType type)
   {
      final List<FailoverEventListener> listenersClone = new ArrayList<>(failoverListeners);

      for (final FailoverEventListener listener : listenersClone)
      {
         try
         {
            listener.failoverEvent(type);
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            HornetQClientLogger.LOGGER.failedToExecuteListener(t);
         }
      }
   }

   /*
    * Re-attach sessions all pre-existing sessions to the new remoting connection
    */
   private void reconnectSessions(final RemotingConnection oldConnection, final int reconnectAttempts)
   {
      HashSet<ClientSessionInternal> sessionsToFailover;
      synchronized (sessions)
      {
         sessionsToFailover = new HashSet<ClientSessionInternal>(sessions);
      }

      for (ClientSessionInternal session : sessionsToFailover)
      {
         session.preHandleFailover(connection);
      }

      getConnectionWithRetry(reconnectAttempts);

      if (connection == null)
      {
         if (!clientProtocolManager.isAlive())
            HornetQClientLogger.LOGGER.failedToConnectToServer();

         return;
      }




      List<FailureListener> oldListeners = oldConnection.getFailureListeners();

      List<FailureListener> newListeners = new ArrayList<>(connection.getFailureListeners());

      for (FailureListener listener : oldListeners)
      {
         // Add all apart from the old DelegatingFailureListener
         if (listener instanceof DelegatingFailureListener == false)
         {
            newListeners.add(listener);
         }
      }

      connection.setFailureListeners(newListeners);

      // This used to be done inside failover
      // it needs to be done on the protocol
      ((CoreRemotingConnection)connection).syncIDGeneratorSequence(((CoreRemotingConnection) oldConnection).getIDGeneratorSequence());

      for (ClientSessionInternal session : sessionsToFailover)
      {
         session.handleFailover(connection);
      }
   }

   private void getConnectionWithRetry(final int reconnectAttempts)
   {
      if (!clientProtocolManager.isAlive())
         return;
      if (HornetQClientLogger.LOGGER.isTraceEnabled())
      {
         HornetQClientLogger.LOGGER.trace("getConnectionWithRetry::" + reconnectAttempts +
                                             " with retryInterval = " +
                                             retryInterval +
                                             " multiplier = " +
                                             retryIntervalMultiplier, new Exception("trace"));
      }

      long interval = retryInterval;

      int count = 0;

      while (clientProtocolManager.isAlive())
      {
         if (ClientSessionFactoryImpl.isDebug)
         {
            HornetQClientLogger.LOGGER.debug("Trying reconnection attempt " + count + "/" + reconnectAttempts);
         }

         if (getConnection() != null)
         {
            if (HornetQClientLogger.LOGGER.isDebugEnabled())
            {
               HornetQClientLogger.LOGGER.debug("Reconnection successful");
            }
            return;
         }
         else
         {
            // Failed to get connection

            if (reconnectAttempts != 0)
            {
               count++;

               if (reconnectAttempts != -1 && count == reconnectAttempts)
               {
                  if (reconnectAttempts != 1)
                  {
                     HornetQClientLogger.LOGGER.failedToConnectToServer(reconnectAttempts);
                  }

                  return;
               }

               if (ClientSessionFactoryImpl.isTrace)
               {
                  HornetQClientLogger.LOGGER.waitingForRetry(interval, retryInterval, retryIntervalMultiplier);
               }

               try
               {
                  if (clientProtocolManager.waitOnLatch(interval))
                  {
                     return;
                  }
               }
               catch (InterruptedException ignore)
               {
                  throw new HornetQInterruptedException(createTrace);
               }

               // Exponential back-off
               long newInterval = (long) (interval * retryIntervalMultiplier);

               if (newInterval > maxRetryInterval)
               {
                  newInterval = maxRetryInterval;
               }

               interval = newInterval;
            }
            else
            {
               HornetQClientLogger.LOGGER.debug("Could not connect to any server. Didn't have reconnection configured on the ClientSessionFactory");
               return;
            }
         }
      }
   }

   private void cancelScheduledTasks()
   {
      Future<?> pingerFutureLocal = pingerFuture;
      if (pingerFutureLocal != null)
      {
         pingerFutureLocal.cancel(false);
      }
      PingRunnable pingRunnableLocal = pingRunnable;
      if (pingRunnableLocal != null)
      {
         pingRunnableLocal.cancel();
      }
      pingerFuture = null;
      pingRunnable = null;
   }

   private void checkCloseConnection()
   {
      if (connection != null && sessions.size() == 0)
      {
         cancelScheduledTasks();

         try
         {
            connection.destroy();
         }
         catch (Throwable ignore)
         {
         }

         connection = null;

         try
         {
            if (connector != null)
            {
               connector.close();
            }
         }
         catch (Throwable ignore)
         {
         }

         connector = null;
      }
   }

   public RemotingConnection getConnection()
   {
      if (closed)
         throw new IllegalStateException("ClientSessionFactory is closed!");
      if (!clientProtocolManager.isAlive())
         return null;
      synchronized (connectionLock)
      {
         if (connection != null)
         {
            // a connection already exists, so returning the same one
            return connection;
         }
         else
         {
            connection = establishNewConnection();

            //we check if we can actually connect.
            // we do it here as to receive the reply connection has to be not null
            if (connection != null && liveNodeID != null)
            {
               try
               {
                  if (!clientProtocolManager.checkForFailover(liveNodeID))
                  {
                     connection.destroy();
                     connection = null;
                  }
               }
               catch (HornetQException e)
               {
                  if (connection != null)
                  {
                     connection.destroy();
                     connection = null;
                  }
               }
            }

            if (connection != null && serverLocator.getAfterConnectInternalListener() != null)
            {
               serverLocator.getAfterConnectInternalListener().onConnection(this);
            }

            if (serverLocator.getTopology() != null)
            {
               if (connection != null)
               {
                  if (ClientSessionFactoryImpl.isTrace)
                  {
                     HornetQClientLogger.LOGGER.trace(this + "::Subscribing Topology");
                  }
                  clientProtocolManager.sendSubscribeTopology(serverLocator.isClusterConnection());
               }
            }
            else
            {
               HornetQClientLogger.LOGGER.debug("serverLocator@" + System.identityHashCode(serverLocator + " had no topology"));
            }

            return connection;
         }
      }
   }


   protected void schedulePing()
   {
      if (clientFailureCheckPeriod != -1)
      {
         if (pingerFuture == null)
         {
            pingRunnable = new ClientSessionFactoryImpl.PingRunnable();

            pingerFuture = scheduledThreadPool.scheduleWithFixedDelay(new ClientSessionFactoryImpl.ActualScheduledPinger(pingRunnable),
                                                                      0,
                                                                      clientFailureCheckPeriod,
                                                                      TimeUnit.MILLISECONDS);
            // To make sure the first ping will be sent
            pingRunnable.send();
         }
         // send a ping every time we create a new remoting connection
         // to set up its TTL on the server side
         else
         {
            pingRunnable.run();
         }
      }
   }


   @Override
   protected void finalize() throws Throwable
   {
      if (!closed && finalizeCheck)
      {
         HornetQClientLogger.LOGGER.factoryLeftOpen(createTrace, System.identityHashCode(this));

         close();
      }

      super.finalize();
   }

   protected ConnectorFactory instantiateConnectorFactory(final String connectorFactoryClassName)
   {

      // Will set the instance here to avoid races where cachedFactory is set to null
      ConnectorFactory cachedFactory = connectorFactory;

      // First if cachedFactory had been used already, we take it from the cache.
      if (cachedFactory != null && cachedFactory.getClass().getName().equals(connectorFactoryClassName))
      {
         return cachedFactory;
      }
      // else... we will try to instantiate a new one

      return AccessController.doPrivileged(new PrivilegedAction<ConnectorFactory>()
      {
         public ConnectorFactory run()
         {
            return (ConnectorFactory) ClassloadingUtil.newInstanceFromClassLoader(connectorFactoryClassName);
         }
      });
   }


   public class CloseRunnable implements Runnable
   {
      private final RemotingConnection conn;
      private final String scaleDownTargetNodeID;

      public CloseRunnable(RemotingConnection conn, String scaleDownTargetNodeID)
      {
         this.conn = conn;
         this.scaleDownTargetNodeID = scaleDownTargetNodeID;
      }

      // Must be executed on new thread since cannot block the Netty thread for a long time and fail
      // can cause reconnect loop
      public void run()
      {
         try
         {
            CLOSE_RUNNABLES.add(this);
            if (scaleDownTargetNodeID == null)
            {
               conn.fail(HornetQClientMessageBundle.BUNDLE.disconnected());
            }
            else
            {
               conn.fail(HornetQClientMessageBundle.BUNDLE.disconnected(), scaleDownTargetNodeID);
            }
         }
         finally
         {
            CLOSE_RUNNABLES.remove(this);
         }

      }

      public ClientSessionFactoryImpl stop()
      {
         causeExit();
         CLOSE_RUNNABLES.remove(this);
         return ClientSessionFactoryImpl.this;
      }

   }


   public void setReconnectAttempts(final int attempts)
   {
      reconnectAttempts = attempts;
   }

   public Object getConnector()
   {
      return connector;
   }

   @Override
   public ConfirmationWindowWarning getConfirmationWindowWarning()
   {
      return confirmationWindowWarning;
   }

   protected Connection openTransportConnection(final Connector connector)
   {
      connector.start();

      Connection transportConnection = connector.createConnection();

      if (transportConnection == null)
      {
         if (ClientSessionFactoryImpl.isDebug)
         {
            HornetQClientLogger.LOGGER.debug("Connector towards " + connector + " failed");
         }

         try
         {
            connector.close();
         }
         catch (Throwable t)
         {
         }
      }

      return transportConnection;
   }

   protected Connector createConnector(ConnectorFactory connectorFactory, TransportConfiguration configuration)
   {
      return connectorFactory.createConnector(configuration.getParams(),
                                              new DelegatingBufferHandler(),
                                              this,
                                              closeExecutor,
                                              threadPool,
                                              scheduledThreadPool);
   }

   private void checkTransportKeys(final ConnectorFactory factory, final Map<String, Object> params)
   {
      if (params != null)
      {
         Set<String> invalid = ConfigurationHelper.checkKeys(factory.getAllowableProperties(), params.keySet());

         if (!invalid.isEmpty())
         {
            String msg = "The following keys are invalid for configuring a connector: " +
               ConfigurationHelper.stringSetToCommaListString(invalid);

            throw new IllegalStateException(msg);

         }
      }
   }


   /**
    * It will connect to either live or backup accordingly to the current configurations
    * it will also switch to backup case it can't connect to live and there's a backup configured
    *
    * @return
    */
   protected Connection createTransportConnection()
   {
      Connection transportConnection = null;

      try
      {
         if (ClientSessionFactoryImpl.isDebug)
         {
            HornetQClientLogger.LOGGER.debug("Trying to connect with connector = " + connectorFactory +
                                                ", parameters = " +
                                                connectorConfig.getParams() +
                                                " connector = " +
                                                connector);
         }


         Connector liveConnector = createConnector(connectorFactory, connectorConfig);

         if ((transportConnection = openTransportConnection(liveConnector)) != null)
         {
            // if we can't connect the connect method will return null, hence we have to try the backup
            connector = liveConnector;
         }
         else if (backupConfig != null)
         {
            if (ClientSessionFactoryImpl.isDebug)
            {
               HornetQClientLogger.LOGGER.debug("Trying backup config = " + backupConfig);
            }

            ConnectorFactory backupConnectorFactory = instantiateConnectorFactory(backupConfig.getFactoryClassName());

            Connector backupConnector = createConnector(backupConnectorFactory, backupConfig);

            transportConnection = openTransportConnection(backupConnector);

            if ((transportConnection = openTransportConnection(backupConnector)) != null)
            {
            /*looks like the backup is now live, let's use that*/

               if (ClientSessionFactoryImpl.isDebug)
               {
                  HornetQClientLogger.LOGGER.debug("Connected to the backup at " + backupConfig);
               }

               // Switching backup as live
               connector = backupConnector;
               connectorConfig = backupConfig;
               backupConfig = null;
               connectorFactory = backupConnectorFactory;
            }
            else
            {
               if (ClientSessionFactoryImpl.isDebug)
               {
                  HornetQClientLogger.LOGGER.debug("Backup is not active yet");
               }
            }

         }
      }
      catch (Exception cause)
      {
         // Sanity catch for badly behaved remoting plugins

         HornetQClientLogger.LOGGER.createConnectorException(cause);

         if (transportConnection != null)
         {
            try
            {
               transportConnection.close();
            }
            catch (Throwable t)
            {
            }
         }

         if (connector != null)
         {
            try
            {
               connector.close();
            }
            catch (Throwable t)
            {
            }
         }

         transportConnection = null;

         connector = null;
      }

      return transportConnection;
   }

   private class DelegatingBufferHandler implements BufferHandler
   {
      public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
      {
         RemotingConnection theConn = connection;

         if (theConn != null && connectionID.equals(theConn.getID()))
         {
            theConn.bufferReceived(connectionID, buffer);
         }
         else
         {
            HornetQClientLogger.LOGGER.debug("TheConn == null on ClientSessionFactoryImpl::DelegatingBufferHandler, ignoring packet");
         }
      }
   }

   private final class DelegatingFailureListener implements FailureListener
   {
      private final Object connectionID;

      DelegatingFailureListener(final Object connectionID)
      {
         this.connectionID = connectionID;
      }

      @Override
      public void connectionFailed(final HornetQException me, final boolean failedOver)
      {
         connectionFailed(me, failedOver, null);
      }

      @Override
      public void connectionFailed(final HornetQException me, final boolean failedOver, String scaleDownTargetNodeID)
      {
         handleConnectionFailure(connectionID, me, scaleDownTargetNodeID);
      }

      @Override
      public String toString()
      {
         return DelegatingFailureListener.class.getSimpleName() + "('reconnectsOrFailover', hash=" +
            super.hashCode() + ")";
      }
   }

   private static final class ActualScheduledPinger implements Runnable
   {
      private final WeakReference<PingRunnable> pingRunnable;

      ActualScheduledPinger(final PingRunnable runnable)
      {
         pingRunnable = new WeakReference<PingRunnable>(runnable);
      }

      public void run()
      {
         PingRunnable runnable = pingRunnable.get();

         if (runnable != null)
         {
            runnable.run();
         }
      }

   }

   private final class PingRunnable implements Runnable
   {
      private boolean cancelled;

      private boolean first;

      private long lastCheck = System.currentTimeMillis();

      public synchronized void run()
      {
         if (cancelled || stopPingingAfterOne && !first)
         {
            return;
         }

         first = false;

         long now = System.currentTimeMillis();

         if (clientFailureCheckPeriod != -1 && connectionTTL != -1 && now >= lastCheck + connectionTTL)
         {
            if (!connection.checkDataReceived())
            {

               // We use a different thread to send the fail
               // but the exception has to be created here to preserve the stack trace
               final HornetQException me = HornetQClientMessageBundle.BUNDLE.connectionTimedOut(connection.getTransportConnection());

               cancelled = true;

               threadPool.execute(new Runnable()
               {
                  // Must be executed on different thread
                  public void run()
                  {
                     connection.fail(me);
                  }
               });

               return;
            }
            else
            {
               lastCheck = now;
            }
         }

         send();
      }

      /**
       *
       */
      public void send()
      {

         clientProtocolManager.ping(connectionTTL);
      }

      public synchronized void cancel()
      {
         cancelled = true;
      }
   }


   protected RemotingConnection establishNewConnection()
   {
      Connection transportConnection = createTransportConnection();

      if (transportConnection == null)
      {
         if (ClientSessionFactoryImpl.isTrace)
         {
            HornetQClientLogger.LOGGER.trace("Neither backup or live were active, will just give up now");
         }
         return null;
      }

      RemotingConnection newConnection = clientProtocolManager.connect(transportConnection, callTimeout,
                                                                       callFailoverTimeout, incomingInterceptors,
                                                                       outgoingInterceptors,
                                                                       new SessionFactoryProtocolHandler());

      newConnection.addFailureListener(new DelegatingFailureListener(newConnection.getID()));

      schedulePing();

      if (HornetQClientLogger.LOGGER.isTraceEnabled())
      {
         HornetQClientLogger.LOGGER.trace("returning " + connection);
      }

      return newConnection;
   }


   protected SessionContext createSessionChannel(final String name,
                                                 final String username,
                                                 final String password,
                                                 final boolean xa,
                                                 final boolean autoCommitSends,
                                                 final boolean autoCommitAcks,
                                                 final boolean preAcknowledge) throws HornetQException
   {
      synchronized (createSessionLock)
      {
         return clientProtocolManager.createSessionContext(name, username,
                                                           password, xa, autoCommitSends, autoCommitAcks, preAcknowledge,
                                                           serverLocator.getMinLargeMessageSize(), serverLocator.getConfirmationWindowSize());
      }
   }


   class SessionFactoryProtocolHandler implements ProtocolResponseHandler
   {

      @Override
      public void nodeDisconnected(RemotingConnection conn, String nodeID, String scaleDownTargetNodeID)
      {

         if (HornetQClientLogger.LOGGER.isTraceEnabled())
         {
            HornetQClientLogger.LOGGER.trace("Disconnect being called on client:" +
                                                " server locator = " +
                                                serverLocator +
                                                " notifying node " +
                                                nodeID +
                                                " as down", new Exception("trace"));
         }

         serverLocator.notifyNodeDown(System.currentTimeMillis(), nodeID);

         closeExecutor.execute(new CloseRunnable(conn, scaleDownTargetNodeID));

      }

      @Override
      public void notifyNodeUp(long uniqueEventID, String nodeID, String backupGroupName, String scaleDownGroupName, Pair<TransportConfiguration, TransportConfiguration> connectorPair, boolean isLast)
      {
         // if it is our connector then set the live id used for failover
         if (connectorPair.getA() != null && connectorPair.getA().equals(connectorConfig))
         {
            liveNodeID = nodeID;
         }
         serverLocator.notifyNodeUp(uniqueEventID, nodeID, backupGroupName, scaleDownGroupName, connectorPair, isLast);
      }

      @Override
      public void notifyNodeDown(long eventTime, String nodeID)
      {
         serverLocator.notifyNodeDown(eventTime, nodeID);
      }
   }

}
