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
package org.apache.activemq.artemis.core.remoting.server.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import org.jctools.maps.NonBlockingHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.core.ActiveMQRemoteDisconnectException;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.impl.CoreProtocolManagerFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.security.ActiveMQPrincipal;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ServiceRegistry;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.AcceptorFactory;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ServerConnectionLifeCycleListener;
import org.apache.activemq.artemis.uri.AcceptorTransportConfigurationParser;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.jboss.logging.Logger;

public class RemotingServiceImpl implements RemotingService, ServerConnectionLifeCycleListener {
   // Constants -----------------------------------------------------

   private static final Logger logger = Logger.getLogger(RemotingServiceImpl.class);

   // Attributes ----------------------------------------------------

   private volatile boolean started = false;

   private final Set<TransportConfiguration> acceptorsConfig;

   private final List<BaseInterceptor> incomingInterceptors = new CopyOnWriteArrayList<>();

   private final List<BaseInterceptor> outgoingInterceptors = new CopyOnWriteArrayList<>();

   private final Map<String, Acceptor> acceptors = new HashMap<>();

   private final Map<Object, ConnectionEntry> connections = new NonBlockingHashMap<>();

   private final ReusableLatch connectionCountLatch = new ReusableLatch(0);

   private final ActiveMQServer server;

   private final ManagementService managementService;

   private ExecutorService threadPool;

   private final Executor flushExecutor;

   private final ScheduledExecutorService scheduledThreadPool;

   private FailureCheckAndFlushThread failureCheckAndFlushThread;

   private final ClusterManager clusterManager;

   private final Map<String, ProtocolManagerFactory> protocolMap = new NonBlockingHashMap<>();

   private ActiveMQPrincipal defaultInvmSecurityPrincipal;

   private ServiceRegistry serviceRegistry;

   private boolean paused = false;

   private AtomicLong totalConnectionCount = new AtomicLong(0);

   private long connectionTtlCheckInterval;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RemotingServiceImpl(final ClusterManager clusterManager,
                              final Configuration config,
                              final ActiveMQServer server,
                              final ManagementService managementService,
                              final ScheduledExecutorService scheduledThreadPool,
                              List<ProtocolManagerFactory> protocolManagerFactories,
                              final Executor flushExecutor,
                              final ServiceRegistry serviceRegistry) {
      this.serviceRegistry = serviceRegistry;

      acceptorsConfig = config.getAcceptorConfigurations();

      this.server = server;

      this.clusterManager = clusterManager;

      setInterceptors(config);

      this.managementService = managementService;

      this.scheduledThreadPool = scheduledThreadPool;

      CoreProtocolManagerFactory coreProtocolManagerFactory = new CoreProtocolManagerFactory();
      //i know there is only 1
      this.flushExecutor = flushExecutor;

      ActiveMQServerLogger.LOGGER.addingProtocolSupport(coreProtocolManagerFactory.getProtocols()[0], coreProtocolManagerFactory.getModuleName());
      this.protocolMap.put(coreProtocolManagerFactory.getProtocols()[0], coreProtocolManagerFactory);

      if (config.isResolveProtocols()) {
         resolveProtocols(this.getClass().getClassLoader());

         if (this.getClass().getClassLoader() != Thread.currentThread().getContextClassLoader()) {
            resolveProtocols(Thread.currentThread().getContextClassLoader());
         }
      }

      if (protocolManagerFactories != null) {
         loadProtocolManagerFactories(protocolManagerFactories);
      }

      this.connectionTtlCheckInterval = config.getConnectionTtlCheckInterval();
   }

   private void setInterceptors(Configuration configuration) {
      incomingInterceptors.addAll(serviceRegistry.getIncomingInterceptors(configuration.getIncomingInterceptorClassNames()));
      outgoingInterceptors.addAll(serviceRegistry.getOutgoingInterceptors(configuration.getOutgoingInterceptorClassNames()));
   }

   @Override
   public synchronized void start() throws Exception {
      if (started) {
         return;
      }

      logger.tracef("Starting remoting service %s", this);

      paused = false;

      // The remoting service maintains it's own thread pool for handling remoting traffic
      // If OIO each connection will have it's own thread
      // If NIO these are capped at nio-remoting-threads which defaults to num cores * 3
      // This needs to be a different thread pool to the main thread pool especially for OIO where we may need
      // to support many hundreds of connections, but the main thread pool must be kept small for better performance

      ThreadFactory tFactory = AccessController.doPrivileged(new PrivilegedAction<ThreadFactory>() {
         @Override
         public ThreadFactory run() {
            return new ActiveMQThreadFactory("ActiveMQ-remoting-threads-" + server.toString() +
                                                "-" +
                                                System.identityHashCode(this), false, Thread.currentThread().getContextClassLoader());
         }
      });

      threadPool = Executors.newCachedThreadPool(tFactory);

      for (TransportConfiguration info : acceptorsConfig) {
         createAcceptor(info);
      }

      /**
       * Don't start the acceptors here.  Only start the acceptors at the every end of the start-up process to avoid
       * race conditions. See {@link #startAcceptors()}.
       */

      // This thread checks connections that need to be closed, and also flushes confirmations
      failureCheckAndFlushThread = new FailureCheckAndFlushThread(connectionTtlCheckInterval);

      failureCheckAndFlushThread.start();

      started = true;
   }

   @Override
   public Acceptor createAcceptor(String name, String uri) throws Exception {
      AcceptorTransportConfigurationParser parser = new AcceptorTransportConfigurationParser();

      List<TransportConfiguration> configurations = parser.newObject(parser.expandURI(uri), name);

      return createAcceptor(configurations.get(0));
   }

   @Override
   public Acceptor createAcceptor(TransportConfiguration info) {
      Acceptor acceptor = null;

      try {
         AcceptorFactory factory = server.getServiceRegistry().getAcceptorFactory(info.getName(), info.getFactoryClassName());

         Map<String, ProtocolManagerFactory> selectedProtocolFactories = new NonBlockingHashMap<>();

         @SuppressWarnings("deprecation")
         String protocol = ConfigurationHelper.getStringProperty(TransportConstants.PROTOCOL_PROP_NAME, null, info.getParams());
         if (protocol != null) {
            ActiveMQServerLogger.LOGGER.warnDeprecatedProtocol();
            locateProtocols(protocol, info, selectedProtocolFactories);
         }

         String protocols = ConfigurationHelper.getStringProperty(TransportConstants.PROTOCOLS_PROP_NAME, null, info.getParams());

         if (protocols != null) {
            locateProtocols(protocols, info, selectedProtocolFactories);
         }

         ClusterConnection clusterConnection = lookupClusterConnection(info);

         // If empty: we get the default list
         if (selectedProtocolFactories.isEmpty()) {
            selectedProtocolFactories = protocolMap;
         }

         Map<String, ProtocolManager> selectedProtocols = new NonBlockingHashMap<>();
         for (Entry<String, ProtocolManagerFactory> entry : selectedProtocolFactories.entrySet()) {
            selectedProtocols.put(entry.getKey(), entry.getValue().createProtocolManager(server, info.getExtraParams(), incomingInterceptors, outgoingInterceptors));
         }

         acceptor = factory.createAcceptor(info.getName(), clusterConnection, info.getParams(), new DelegatingBufferHandler(), this, threadPool, scheduledThreadPool, selectedProtocols);

         if (defaultInvmSecurityPrincipal != null && acceptor.isUnsecurable()) {
            acceptor.setDefaultActiveMQPrincipal(defaultInvmSecurityPrincipal);
         }

         acceptors.put(info.getName(), acceptor);

         if (managementService != null) {
            acceptor.setNotificationService(managementService);

            managementService.registerAcceptor(acceptor, info);
         }
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorCreatingAcceptor(e, info.getFactoryClassName());
      }

      return acceptor;
   }

   @Override
   public void destroyAcceptor(String name) throws Exception {
      Acceptor acceptor = acceptors.get(name);
      if (acceptor != null) {
         acceptor.stop();
         acceptors.remove(name);
      }
   }

   @Override
   public synchronized void startAcceptors() throws Exception {
      if (isStarted()) {
         for (Acceptor a : acceptors.values()) {
            a.start();
         }
      }
   }

   @Override
   public synchronized void allowInvmSecurityOverride(ActiveMQPrincipal principal) {
      defaultInvmSecurityPrincipal = principal;
      for (Acceptor acceptor : acceptors.values()) {
         if (acceptor.isUnsecurable()) {
            acceptor.setDefaultActiveMQPrincipal(principal);
         }
      }
   }

   @Override
   public synchronized void pauseAcceptors() {
      if (!started)
         return;

      paused = true;

      for (Acceptor acceptor : acceptors.values()) {
         try {
            acceptor.pause();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorStoppingAcceptor(acceptor.getName());
         }
      }
   }

   @Override
   public synchronized boolean isPaused() {
      return paused;
   }

   @Override
   public synchronized void freeze(final String scaleDownNodeID, final CoreRemotingConnection connectionToKeepOpen) {
      if (!started)
         return;
      failureCheckAndFlushThread.close(false);
      HashMap<Object, ConnectionEntry> connectionEntries = new HashMap<>(connections);

      // Now we ensure that no connections will process any more packets after this method is
      // complete then send a disconnect packet
      for (Entry<Object, ConnectionEntry> entry : connectionEntries.entrySet()) {
         RemotingConnection conn = entry.getValue().connection;

         if (conn.equals(connectionToKeepOpen))
            continue;

         if (logger.isTraceEnabled()) {
            logger.trace("Sending connection.disconnection packet to " + conn);
         }

         if (!conn.isClient()) {
            conn.disconnect(scaleDownNodeID, false);
            removeConnection(entry.getKey());
         }
      }
   }

   @Override
   public void stop(final boolean criticalError) throws Exception {
      if (!started) {
         return;
      }

      failureCheckAndFlushThread.close(criticalError);

      // We need to stop them accepting first so no new connections are accepted after we send the disconnect message
      for (Acceptor acceptor : acceptors.values()) {
         if (logger.isDebugEnabled()) {
            logger.debug("Pausing acceptor " + acceptor);
         }

         try {
            acceptor.pause();
         } catch (Throwable t) {
            ActiveMQServerLogger.LOGGER.errorStoppingAcceptor(acceptor.getName());
         }

      }

      if (logger.isDebugEnabled()) {
         logger.debug("Sending disconnect on live connections");
      }

      HashSet<ConnectionEntry> connectionEntries = new HashSet<>(connections.values());

      // Now we ensure that no connections will process any more packets after this method is complete
      // then send a disconnect packet
      for (ConnectionEntry entry : connectionEntries) {
         RemotingConnection conn = entry.connection;

         if (logger.isTraceEnabled()) {
            logger.trace("Sending connection.disconnection packet to " + conn);
         }

         conn.disconnect(criticalError);
      }

      for (Acceptor acceptor : acceptors.values()) {
         try {
            acceptor.stop();
         } catch (Throwable t) {
            ActiveMQServerLogger.LOGGER.errorStoppingAcceptor(acceptor.getName());
         }
      }

      acceptors.clear();

      connections.clear();
      connectionCountLatch.setCount(0);

      if (managementService != null) {
         managementService.unregisterAcceptors();
      }

      threadPool.shutdown();

      if (!criticalError) {
         boolean ok = threadPool.awaitTermination(10000, TimeUnit.MILLISECONDS);

         if (!ok) {
            ActiveMQServerLogger.LOGGER.timeoutRemotingThreadPool();
         }
      }

      started = false;
   }

   @Override
   public Acceptor getAcceptor(String name) {
      return acceptors.get(name);
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   private RemotingConnection getConnection(final Object remotingConnectionID) {
      ConnectionEntry entry = connections.get(remotingConnectionID);

      if (entry != null) {
         return entry.connection;
      } else {
         ActiveMQServerLogger.LOGGER.errorRemovingConnection();

         return null;
      }
   }

   public ConnectionEntry getConnectionEntry(final Object remotingConnectionID) {
      ConnectionEntry entry = connections.get(remotingConnectionID);

      if (entry != null) {
         return entry;
      } else {
         return null;
      }
   }

   @Override
   public RemotingConnection removeConnection(final Object remotingConnectionID) {
      ConnectionEntry entry = connections.remove(remotingConnectionID);

      if (entry != null) {
         logger.debug("RemotingServiceImpl::removing connection ID " + remotingConnectionID);
         connectionCountLatch.countDown();
         return entry.connection;
      } else {
         logger.debug("The connectionID::" + remotingConnectionID + " was already removed by some other module");

         return null;
      }
   }

   @Override
   public synchronized Set<RemotingConnection> getConnections() {
      Set<RemotingConnection> conns = new HashSet<>(connections.size());

      for (ConnectionEntry entry : connections.values()) {
         conns.add(entry.connection);
      }

      return conns;
   }

   @Override
   public long getTotalConnectionCount() {
      return totalConnectionCount.get();
   }

   @Override
   public synchronized ReusableLatch getConnectionCountLatch() {
      return connectionCountLatch;
   }

   // ConnectionLifeCycleListener implementation -----------------------------------

   private ProtocolManagerFactory getProtocolManager(String protocol) {
      return protocolMap.get(protocol);
   }

   @Override
   public void connectionCreated(final ActiveMQComponent component,
                                 final Connection connection,
                                 final ProtocolManager protocol) {
      if (server == null) {
         throw new IllegalStateException("Unable to create connection, server hasn't finished starting up");
      }

      ConnectionEntry entry = protocol.createConnectionEntry((Acceptor) component, connection);

      if (logger.isTraceEnabled()) {
         logger.trace("Connection created " + connection);
      }

      connections.put(connection.getID(), entry);
      connectionCountLatch.countUp();
      totalConnectionCount.incrementAndGet();
   }

   @Override
   public void connectionDestroyed(final Object connectionID) {

      if (logger.isTraceEnabled()) {
         logger.trace("Connection removed " + connectionID + " from server " + this.server, new Exception("trace"));
      }

      ConnectionEntry conn = connections.get(connectionID);

      if (conn != null && !conn.connection.isSupportReconnect()) {
         removeConnection(connectionID);

         conn.connection.fail(new ActiveMQRemoteDisconnectException());
      }
   }

   @Override
   public void connectionException(final Object connectionID, final ActiveMQException me) {
      // We DO NOT call fail on connection exception, otherwise in event of real connection failure, the
      // connection will be failed, the session will be closed and won't be able to reconnect

      // E.g. if live server fails, then this handler wil be called on backup server for the server
      // side replicating connection.
      // If the connection fail() is called then the sessions on the backup will get closed.

      // Connections should only fail when TTL is exceeded
   }

   @Override
   public void connectionReadyForWrites(final Object connectionID, final boolean ready) {
   }

   @Override
   public void addIncomingInterceptor(final BaseInterceptor interceptor) {
      incomingInterceptors.add(interceptor);

      updateProtocols();
   }

   @Override
   public List<BaseInterceptor> getIncomingInterceptors() {
      return Collections.unmodifiableList(incomingInterceptors);
   }

   @Override
   public boolean removeIncomingInterceptor(final BaseInterceptor interceptor) {
      if (incomingInterceptors.remove(interceptor)) {
         updateProtocols();
         return true;
      } else {
         return false;
      }
   }

   @Override
   public void addOutgoingInterceptor(final BaseInterceptor interceptor) {
      outgoingInterceptors.add(interceptor);
      updateProtocols();
   }

   @Override
   public List<BaseInterceptor> getOutgoinInterceptors() {
      return Collections.unmodifiableList(outgoingInterceptors);
   }

   @Override
   public boolean removeOutgoingInterceptor(final BaseInterceptor interceptor) {
      if (outgoingInterceptors.remove(interceptor)) {
         updateProtocols();
         return true;
      } else {
         return false;
      }
   }

   private ClusterConnection lookupClusterConnection(TransportConfiguration acceptorConfig) {
      String clusterConnectionName = (String) acceptorConfig.getParams().get(org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.CLUSTER_CONNECTION);

      ClusterConnection clusterConnection = null;
      if (clusterConnectionName != null) {
         clusterConnection = clusterManager.getClusterConnection(clusterConnectionName);
      }

      // if not found we will still use the default name, even if a name was provided
      if (clusterConnection == null) {
         clusterConnection = clusterManager.getDefaultConnection(acceptorConfig);
      }

      return clusterConnection;
   }

   // Inner classes -------------------------------------------------

   private final class DelegatingBufferHandler implements BufferHandler {

      @Override
      public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
         ConnectionEntry conn = connections.get(connectionID);

         if (conn != null) {
            conn.connection.bufferReceived(connectionID, buffer);
         } else {
            if (logger.isTraceEnabled()) {
               logger.trace("ConnectionID = " + connectionID + " was already closed, so ignoring packet");
            }
         }
      }
   }

   private final class FailureCheckAndFlushThread extends Thread {

      private final long pauseInterval;

      private volatile boolean closed;
      private final CountDownLatch latch = new CountDownLatch(1);

      FailureCheckAndFlushThread(final long pauseInterval) {
         super("activemq-failure-check-thread");

         this.pauseInterval = pauseInterval;
      }

      public void close(final boolean criticalError) {
         closed = true;

         latch.countDown();

         if (!criticalError) {
            try {
               join();
            } catch (InterruptedException e) {
               throw new ActiveMQInterruptedException(e);
            }
         }
      }

      @Override
      public void run() {
         while (!closed) {
            try {
               long now = System.currentTimeMillis();

               Set<Pair<Object, Long>> toRemove = new HashSet<>();

               for (ConnectionEntry entry : connections.values()) {
                  final RemotingConnection conn = entry.connection;

                  boolean flush = true;

                  if (entry.ttl != -1) {
                     if (!conn.checkDataReceived()) {
                        if (now >= entry.lastCheck + entry.ttl) {
                           toRemove.add(new Pair<>(conn.getID(), entry.ttl));

                           flush = false;
                        }
                     } else {
                        entry.lastCheck = now;
                     }
                  }

                  if (flush) {
                     flushExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                           try {
                              // this is using a different thread
                              // as if anything wrong happens on flush
                              // failure detection could be affected
                              conn.flush();
                           } catch (Throwable e) {
                              ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
                           }

                        }
                     });
                  }
               }

               for (final Pair<Object, Long> pair : toRemove) {
                  final RemotingConnection conn = getConnection(pair.getA());
                  if (conn != null) {
                     // In certain cases (replicationManager for instance) calling fail could take some time
                     // We can't pause the FailureCheckAndFlushThread as that would lead other clients to fail for
                     // missing pings
                     flushExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                           conn.fail(ActiveMQMessageBundle.BUNDLE.clientExited(conn.getRemoteAddress(), pair.getB()));
                        }
                     });
                     removeConnection(pair.getA());
                  }
               }

               if (latch.await(pauseInterval, TimeUnit.MILLISECONDS))
                  return;
            } catch (Throwable e) {
               ActiveMQServerLogger.LOGGER.errorOnFailureCheck(e);
            }
         }
      }
   }

   protected void updateProtocols() {
      for (Acceptor acceptor : this.acceptors.values()) {
         acceptor.updateInterceptors(incomingInterceptors, outgoingInterceptors);
      }
   }

   /**
    * Locates protocols from the internal default map and moves them into the input protocol map.
    *
    * @param protocolList
    * @param transportConfig
    * @param protocolMap
    */
   private void locateProtocols(String protocolList,
                                Object transportConfig,
                                Map<String, ProtocolManagerFactory> protocolMap) {
      String[] protocolsSplit = protocolList.split(",");

      for (String protocolItem : protocolsSplit) {
         ProtocolManagerFactory protocolManagerFactory = this.protocolMap.get(protocolItem);

         if (protocolManagerFactory == null) {
            ActiveMQServerLogger.LOGGER.noProtocolManagerFound(protocolItem, transportConfig.toString());
         } else {
            protocolMap.put(protocolItem, protocolManagerFactory);
         }
      }
   }

   /**
    * Finds protocol support from a given classloader.
    *
    * @param loader
    */
   private void resolveProtocols(ClassLoader loader) {
      ServiceLoader<ProtocolManagerFactory> serviceLoader = ServiceLoader.load(ProtocolManagerFactory.class, loader);
      loadProtocolManagerFactories(serviceLoader);
   }

   /**
    * Loads the protocols found into a map.
    *
    * @param protocolManagerFactoryCollection
    */
   private void loadProtocolManagerFactories(Iterable<ProtocolManagerFactory> protocolManagerFactoryCollection) {
      for (ProtocolManagerFactory next : protocolManagerFactoryCollection) {
         String[] protocols = next.getProtocols();
         for (String protocol : protocols) {
            ActiveMQServerLogger.LOGGER.addingProtocolSupport(protocol, next.getModuleName());
            protocolMap.put(protocol, next);
         }
      }
   }

}
